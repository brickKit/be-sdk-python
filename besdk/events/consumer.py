"""Durable consumers (be-protocol P12.5–P12.9, P12.14; repro r1-07).

The durable is looked up and created only when absent, never updated (nats-py's ``add_consumer`` is
create-or-update, so it is called only after ``consumer_info`` said the durable does not exist). Its
server-side configuration is the protocol's constants: ack wait 30 s, max ack pending 256, max deliver −1,
no backoff. The runtime counts deliveries and applies ``EVENTS_BACKOFF`` itself with a delayed nak; a
delivery above ``EVENTS_MAX_DELIVER`` and a permanent error go to the dead letters, then ``Term``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError
from opentelemetry import trace
from opentelemetry.trace import Link

from besdk import context
from besdk.events import envelope as E
from besdk.events.model import Event, Permanent, StartFrom, Subscription

if TYPE_CHECKING:
    from besdk.events.bus import Bus
    from besdk.events.contract import Contract
    from besdk.metrics import BeMetrics
    from besdk.store.store import Store

ACK_WAIT = 30.0
HANDLER_MARGIN = 5.0  # a handler's deadline is ack_wait − 5 s (P12.9)
MAX_ACK_PENDING = 256
INACTIVE = 30 * 86400.0
FETCH_WAIT = 5.0
CURSOR_UPSERT = (
    "INSERT INTO besdk_event_cursor (consumer, aggregate_type, aggregate_id, version, event_id) "
    "VALUES ($1, $2, $3, $4, $5) ON CONFLICT (consumer, aggregate_type, aggregate_id) DO UPDATE "
    "SET version = EXCLUDED.version, event_id = EXCLUDED.event_id, seen_at = now() "
    "WHERE besdk_event_cursor.version < EXCLUDED.version RETURNING 1")
CURSOR_GET = ("SELECT version FROM besdk_event_cursor WHERE consumer = $1 AND aggregate_type = $2 "
              "AND aggregate_id = $3")


def durable_config(durable: str, subject: str, start: StartFrom) -> ConsumerConfig:
    return ConsumerConfig(durable_name=durable, filter_subject=subject, ack_policy=AckPolicy.EXPLICIT,
                          ack_wait=ACK_WAIT, max_ack_pending=MAX_ACK_PENDING, max_deliver=-1,
                          deliver_policy=DeliverPolicy.NEW if start == StartFrom.NEW else DeliverPolicy.ALL,
                          inactive_threshold=INACTIVE)


class Consumer:
    def __init__(self, *, member: str, sub: Subscription, store: "Store", bus: "Bus", contract: "Contract",
                 max_deliver: int, backoff_ns: list[int], metrics: "BeMetrics", logger: logging.Logger,
                 tracer: Any, propagator: Any):
        self.member, self.sub, self.store, self.bus = member, sub, store, bus
        self.contract, self.max_deliver, self.backoff_ns = contract, max_deliver, backoff_ns
        self.metrics, self.logger, self.tracer, self.propagator = metrics, logger, tracer, propagator
        self.durable = E.durable_name(member, sub.subject)
        self.stream = E.stream_of(sub.subject)
        self._tasks: set[asyncio.Task] = set()

    async def ensure(self) -> None:
        """Stream and durable exist afterwards; an existing durable is left as it is (P12.5)."""
        await self.bus.ensure_stream_for(self.sub.subject)
        await self.bus.ensure_dlq()
        want = durable_config(self.durable, self.sub.subject, self.sub.start_from)
        try:
            info = await self.bus.js.consumer_info(self.stream, self.durable)
        except NotFoundError:
            await self.bus.js.add_consumer(self.stream, want)
            self.logger.info("durable_created", extra={"durable": self.durable})
            return
        c = info.config
        if (c.ack_wait, c.max_ack_pending, c.max_deliver, bool(c.backoff)) != (ACK_WAIT, MAX_ACK_PENDING, -1, False):
            self.logger.warning("durable_config_differs", extra={"durable": self.durable, "ack_wait": c.ack_wait,
                                                                 "max_ack_pending": c.max_ack_pending,
                                                                 "max_deliver": c.max_deliver})

    async def run(self) -> None:
        await self.ensure()
        psub = await self.bus.js.pull_subscribe_bind(durable=self.durable, stream=self.stream)
        conc = max(1, self.sub.concurrency)
        freed = asyncio.Event()
        try:
            while True:
                while len(self._tasks) >= conc:
                    freed.clear()
                    await freed.wait()
                try:
                    msgs = await psub.fetch(batch=conc - len(self._tasks), timeout=FETCH_WAIT)
                except (TimeoutError, asyncio.TimeoutError):
                    continue
                for m in msgs:
                    t = asyncio.create_task(self._one(m))
                    self._tasks.add(t)
                    t.add_done_callback(lambda t: (self._tasks.discard(t), freed.set()))
        finally:
            await self.drain()
            try:
                await psub.unsubscribe()
            except Exception:  # noqa: BLE001
                pass

    async def drain(self, timeout: float = 5.0) -> None:
        if self._tasks:
            await asyncio.wait(list(self._tasks), timeout=timeout)

    async def _one(self, msg: Any) -> None:
        try:
            await self.handle(msg)
        except Exception as e:  # noqa: BLE001 - the message is redelivered after the ack wait
            self.logger.error("consumer_failed", extra={"durable": self.durable, "error": f"{type(e).__name__}: {e}"})

    def _inbound(self, headers: dict) -> E.Inbound:
        decl = self.contract.get(self.sub.subject)
        agg = decl.aggregate_type if decl else (self.sub.aggregate_type or headers.get("ce-aggregatetype", ""))
        return E.Inbound(self.member, self.sub.subject, agg, bool(decl and decl.transaction_document))

    async def handle(self, msg: Any) -> None:
        md = msg.metadata
        delivery, seq = md.num_delivered, md.sequence.stream
        headers = dict(msg.headers or {})
        inbound = self._inbound(headers)
        if delivery > self.max_deliver:
            await self._dead(msg, headers, "MAX_DELIVER", delivery, seq)
            return
        acc = E.accept(inbound, headers, msg.data, delivery)
        if acc.dlq_reason:
            await self._dead(msg, headers, acc.dlq_reason, delivery, seq)
            return
        ev = acc.event
        try:
            result = await self._run_handler(msg, ev)
        except asyncio.CancelledError:
            await msg.nak()
            raise
        except Permanent:
            await self._dead(msg, headers, "PERMANENT", delivery, seq)
            return
        except Exception as e:  # noqa: BLE001 - handler errors are redelivered (P12.7)
            d = E.redelivery(delivery, self.max_deliver, self.backoff_ns, "error", self.durable, seq)
            self.logger.warning("event_handler_failed", extra={"subject": ev.subject, "event_id": ev.id,
                                                               "delivery": delivery, "error": f"{type(e).__name__}: {e}"})
            await msg.nak(delay=d.delay_ns / 1e9)
            self.metrics.consumer_handled.labels(subject=ev.subject, result="nak").inc()
            return
        await msg.ack()
        self.metrics.consumer_handled.labels(subject=ev.subject, result=result).inc()
        if ev.occurred_at:
            self.metrics.consumer_lag.labels(subject=ev.subject).set(
                (datetime.now(timezone.utc) - ev.occurred_at).total_seconds())

    async def _run_handler(self, msg: Any, ev: Event) -> str:
        """Run Apply (in the cursor's transaction) or Run (outside, cursor after); InProgress meanwhile."""
        links = []
        if ev.traceparent:
            sc = trace.get_current_span(self.propagator.extract({"traceparent": ev.traceparent})).get_span_context()
            if sc.is_valid:
                links.append(Link(sc))
        progress = asyncio.create_task(_progress(msg))
        handled = context.HandledEvent(ev.id, ev.hop_count, ev.subject, ev.delivery)
        try:
            with self.tracer.start_as_current_span(f"consume {ev.subject}", links=links,
                                                   context=trace.set_span_in_context(trace.INVALID_SPAN),
                                                   kind=trace.SpanKind.CONSUMER):
                with context.scope(event=handled, member=self.member, deadline=context.deadline_in(ACK_WAIT - HANDLER_MARGIN)):
                    async with asyncio.timeout(ACK_WAIT - HANDLER_MARGIN):
                        return await (self._apply(ev) if self.sub.apply else self._run(ev))
        finally:
            progress.cancel()

    async def _apply(self, ev: Event) -> str:
        sub = self.sub

        async def body(tx):
            if await tx.fetchval(CURSOR_UPSERT, sub.consumer, ev.aggregate_type, ev.aggregate_id, ev.version,
                                 ev.id) is None:
                return "skipped"
            await sub.apply(tx, ev)
            return "applied"

        return await self.store.tx(body)

    async def _run(self, ev: Event) -> str:
        sub = self.sub
        cur = await self.store.tx(lambda tx: tx.fetchval(CURSOR_GET, sub.consumer, ev.aggregate_type, ev.aggregate_id))
        if not E.cursor_applies(cur, ev.version):
            return "skipped"
        await sub.run(ev)
        await self.store.tx(lambda tx: tx.fetchval(CURSOR_UPSERT, sub.consumer, ev.aggregate_type, ev.aggregate_id,
                                                   ev.version, ev.id))
        return "applied"

    async def _dead(self, msg: Any, headers: dict, reason: str, delivery: int, seq: int) -> None:
        """Publish to ``dlq.<durable>.<subject>`` with message ID ``dlq:<durable>:<seq>``, then Term (P12.7)."""
        h = {k: v for k, v in headers.items() if k.startswith("ce-") or k in ("traceparent", "tracestate",
                                                                             "content-type")}
        h.update({"be-dlq-reason": reason, "be-dlq-consumer": self.durable, "be-dlq-delivery": str(delivery),
                  "Nats-Msg-Id": f"dlq:{self.durable}:{seq}"})
        await self.bus.ensure_dlq()
        await self.bus.publish(E.dlq_subject(self.durable, self.sub.subject), msg.data, h)
        await msg.term()
        self.metrics.dlq_messages.labels(subject=self.sub.subject).inc()
        self.metrics.consumer_handled.labels(subject=self.sub.subject, result="dlq").inc()
        self.logger.warning("event_dead_lettered", extra={"subject": self.sub.subject, "reason": reason,
                                                          "delivery": delivery, "durable": self.durable})


async def _progress(msg: Any) -> None:
    """``InProgress`` every ack_wait / 3 while the handler runs (P12.9)."""
    while True:
        await asyncio.sleep(ACK_WAIT / 3)
        try:
            await msg.in_progress()
        except Exception:  # noqa: BLE001
            return

