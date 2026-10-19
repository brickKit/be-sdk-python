"""The consumer side of chapter 06: the inbox written with the business change (P6.25), the hop limit (P6.15),
claim checks (P6.14) and dead letters (P6.20)."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from opentelemetry import context as otel_context

from besdk.config import Key, Type
from besdk.db.pool import DB, Tx
from besdk.events.core import (H_EPOCH, H_HOPS, H_ID, H_IMPL, H_PARTITION, H_PROJECTION, H_SEQ, H_SOURCE, H_STREAM,
                               H_SUBJECT, H_TYPE, H_VERSION, MAX_HOPS, Delivery, Outcome, SubscriptionSpec, durable_name)
from besdk.events.objectstore import HTTPClaims, resolve_claim
from besdk.events.outbox import caused_by
from besdk.obs import PROPAGATOR, Logger


@dataclass
class Received:
    """One event as a handler sees it; data is the payload with claim checks already resolved."""

    id: str
    type: str
    source: str
    record_id: str
    version: int
    projection: str
    stream: str
    partition: int
    seq: int
    epoch: str
    impl: str
    hops: int
    data: bytes
    headers: dict[str, str]

    def decode(self) -> Any:
        return json.loads(self.data)


HandleFunc = Callable[[Tx, Received], Awaitable[None]]


@dataclass
class Sub:
    """One subscription of a module."""

    name: str  # unique within the component
    streams: list[str]
    handle: HandleFunc
    types: list[str] = field(default_factory=list)  # empty: every type of the streams
    max_deliver: int = 0
    concurrency: int = 0  # the deployment's BUS_SUBSCRIPTION_CONCURRENCY overrides it (P6.31)
    # depends on arrival order: always one message at a time, whatever the deployment sets
    ordered: bool = False


CONCURRENCY_KEY = "BUS_SUBSCRIPTION_CONCURRENCY"


def consumer_keys() -> list[Key]:
    """The keys of a member that subscribes (P6.31)."""
    return [Key(CONCURRENCY_KEY, Type.INTEGER, min=1, max=64)]


def concurrency(sub: Sub, configured: int) -> int:
    """A subscription's effective concurrency (P6.31): 1 when ordered, else the deployment's setting (configured, 0
    when unset), else the declaration, else 1."""
    if sub.ordered:
        return 1
    return configured or sub.concurrency or 1


class Subscriptions:
    def __init__(self) -> None:
        self.list: list[Sub] = []

    def add(self, sub: Sub) -> None:
        self.list.append(sub)


class Consumer:
    """Runs one subscription of a member."""

    def __init__(self, member: str, sub: Sub, db: DB, log: Logger, claims: HTTPClaims | None = None,
                 configured: int = 0):
        self.member, self.sub, self.db, self.log, self.claims = member, sub, db, log, claims
        self.configured = configured  # the member's BUS_SUBSCRIPTION_CONCURRENCY, 0 when unset (P6.31)

    def spec(self) -> SubscriptionSpec:
        filters = [f"be.{st}.*.{t}" for st in self.sub.streams for t in self.sub.types]
        return SubscriptionSpec(durable_name(self.member, self.sub.name), list(self.sub.streams), filters,
                                self.sub.max_deliver, concurrency=concurrency(self.sub, self.configured)).defaults()

    async def handle(self, d: Delivery) -> Outcome:
        """Claim check, hop limit, inbox and business write in one transaction; a failure on the last delivery
        becomes a dead letter."""
        try:
            await self._process(d)
            return Outcome.ACK
        except Exception as e:  # noqa: BLE001 - every failure is a nak or a dead letter
            err = e
        if d.attempt < self.spec().max_deliver:
            self.log.warn("an event failed; it will be delivered again", subscription=self.sub.name, event_id=d.id,
                          attempt=d.attempt, error=repr(err))
            return Outcome.NAK
        try:
            await self._dead_letter(d, err)
        except Exception as e:  # noqa: BLE001
            self.log.error("an event failed for the last time and its dead letter could not be written",
                           subscription=self.sub.name, event_id=d.id, error=repr(e))
            return Outcome.NAK  # not lost: the transport keeps it until the limit stops it
        self.log.error("an event failed for the last time; recorded as a dead letter", subscription=self.sub.name,
                       event_id=d.id, attempts=d.attempt, error=repr(err))
        return Outcome.TERM

    async def _process(self, d: Delivery) -> None:
        r = received(d)
        if r.hops > MAX_HOPS:
            raise ValueError(f"event {r.id}: {r.hops} hops exceed the limit of {MAX_HOPS}")
        r.data = await resolve_claim(d, self.claims)
        carrier = {k: d.headers["ce-" + k] for k in ("traceparent", "tracestate") if "ce-" + k in d.headers}
        token = otel_context.attach(PROPAGATOR.extract(carrier=carrier))
        try:
            with caused_by(r.id, r.hops):
                async def apply(tx: Tx) -> None:
                    status = await tx.execute("INSERT INTO be_inbox (subscription, event_id) VALUES ($1, $2) "
                                              "ON CONFLICT DO NOTHING", self.sub.name, uuid.UUID(r.id))
                    if status.endswith(" 0"):
                        return  # already applied: a redelivery takes effect once
                    await self.sub.handle(tx, r)

                await self.db.background_tx(apply)
        finally:
            otel_context.detach(token)

    async def _dead_letter(self, d: Delivery, cause: BaseException) -> None:
        await self.db.background_tx(lambda tx: tx.execute(
            """INSERT INTO be_dead_letter (subscription, event_id, type, headers, payload, error, attempts)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7) ON CONFLICT DO NOTHING""", self.sub.name, d.id,
            d.headers.get(H_TYPE, ""), json.dumps(d.headers), d.data, repr(cause), d.attempt))


def received(d: Delivery) -> Received:
    h = d.headers
    if not h.get(H_ID) or not h.get(H_TYPE):
        raise ValueError("the envelope has no ce-id or ce-type")

    def num(key: str) -> int:
        v = h.get(key, "0")
        if not v.lstrip("-").isdigit():
            raise ValueError(f"envelope header {key}: {v!r}")
        return int(v)

    return Received(h[H_ID], h[H_TYPE], h.get(H_SOURCE, ""), h.get(H_SUBJECT, ""), num(H_VERSION),
                    h.get(H_PROJECTION, ""), h.get(H_STREAM, ""), num(H_PARTITION), num(H_SEQ), h.get(H_EPOCH, ""),
                    h.get(H_IMPL, ""), num(H_HOPS), d.data, h)
