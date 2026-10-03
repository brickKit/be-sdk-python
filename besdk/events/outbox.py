"""Publishing (be-protocol P12.1, P12.2, P12.8): ``tx.publish`` writes a ``besdk_outbox`` row in the business
transaction; the pump claims rows (``FOR UPDATE SKIP LOCKED``), publishes them outside any transaction and
marks a row ``PUBLISHED`` only after the broker's PubAck. Rows are never dropped."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from besdk import context, errors, ids
from besdk.events.contract import Contract, EventDecl
from besdk.events.envelope import OutboxRow, check_subject, derive, headers_of, legal_entity_of
from besdk.events.model import Event

if TYPE_CHECKING:
    from besdk.events.bus import Bus
    from besdk.metrics import BeMetrics
    from besdk.store.store import Store
    from besdk.store.tx import Tx

PAYLOAD_LIMIT = 64 * 1024  # P12.2: above it the event carries a claim check (stage-B ruling: rejected)
BATCH = 256
BUSY, IDLE = 0.2, 2.0

_INSERT = ("INSERT INTO besdk_outbox (id, created_at, subject, aggregate_type, aggregate_id, aggregate_version, "
           "occurred_at, traceparent, causation_id, hop_count, headers, payload) "
           "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb)")
_COLS = ("id, created_at, subject, aggregate_type, aggregate_id, aggregate_version, occurred_at, traceparent, "
         "causation_id, hop_count, payload::text AS payload, attempts")
_CLAIM = (f"UPDATE besdk_outbox SET status = 'SENDING', claimed_until = now() + interval '30 seconds', "
          f"attempts = attempts + 1 WHERE (id, created_at) IN (SELECT id, created_at FROM besdk_outbox "
          f"WHERE (status = 'PENDING' AND next_attempt_at <= now()) OR (status = 'SENDING' AND claimed_until < now()) "
          f"ORDER BY created_at, id LIMIT {BATCH} FOR UPDATE SKIP LOCKED) RETURNING {_COLS}")
_DONE = ("UPDATE besdk_outbox SET status = 'PUBLISHED', published_at = now(), claimed_until = NULL "
         "WHERE id = ANY($1::uuid[]) AND status = 'SENDING'")
_RETRY = ("UPDATE besdk_outbox SET status = 'PENDING', claimed_until = NULL, last_error = $3, "
          "next_attempt_at = now() + make_interval(secs => least(60, power(2, greatest(attempts - 1, 0))))"
          " WHERE id = $1 AND created_at = $2")
_GAUGE = ("SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0) "
          "FROM besdk_outbox WHERE status <> 'PUBLISHED'")


class Publisher:
    """What ``tx.publish`` needs of the member: its ID, version, contract and declared subjects."""

    def __init__(self, member: str, version: str, contract: Contract, publishes: list[str], propagator: Any,
                 logger: logging.Logger):
        self.member, self.version, self.contract = member, version, contract
        self.publishes = set(publishes)
        self.propagator, self.logger = propagator, logger

    def decl(self, ev: Event) -> EventDecl:
        def bad(reason: str, detail: str) -> errors.Error:
            return errors.internal(f"{reason}: {ev.subject}: {detail}")

        try:
            check_subject(ev.subject)
        except errors.ProtocolError as e:
            raise bad("SUBJECT_INVALID", e.detail) from None
        decl = self.contract.get(ev.subject)
        if ev.subject not in self.publishes or decl is None:
            raise bad("SUBJECT_NOT_DECLARED", "not in Events.publishes and the event contract")
        if not isinstance(ev.payload, dict):
            raise bad("PAYLOAD_INVALID", "a payload is a JSON object")
        if decl.transaction_document and not legal_entity_of(ev.payload):
            raise bad("LEGAL_ENTITY_MISSING", "legal_entity_id is required on a transaction document")
        problems = self.contract.problems(ev.subject, ev.payload)
        if problems:
            raise bad("PAYLOAD_INVALID", "; ".join(problems[:3]))
        if ev.version < 1:
            raise bad("ENVELOPE_INVALID", "aggregate version starts at 1")
        return decl


def causation_now() -> tuple[str, int]:
    u = context.current()
    if u.event is not None:
        return derive("event", handled=(u.event.id, u.event.hop_count))
    return derive("request")


async def write(tx: "Tx", ev: Event) -> None:
    pub: Publisher | None = getattr(tx.store, "publisher", None)
    if pub is None:
        raise errors.internal("SUBJECT_NOT_DECLARED: this member declares no events")
    decl = pub.decl(ev)
    data = json.dumps(ev.payload, ensure_ascii=False, separators=(",", ":"))
    size = len(data.encode())
    if size > PAYLOAD_LIMIT:
        raise errors.internal(f"PAYLOAD_TOO_LARGE: {ev.subject} is {size} bytes, above 64 KiB; use a claim check")
    eid = ids.new_id()
    carrier: dict[str, str] = {}
    pub.propagator.inject(carrier)
    causation, hop = causation_now()
    le = legal_entity_of(ev.payload)
    await tx.execute(_INSERT, eid, ids.id_time(eid), ev.subject, decl.aggregate_type, ev.aggregate_id, ev.version,
                     datetime.now(timezone.utc), carrier.get("traceparent", ""), causation, hop,
                     json.dumps({"ce-legalentity": le} if le else {}), data)
    ev.id, ev.aggregate_type, ev.source = str(eid), decl.aggregate_type, pub.member


class OutboxPump:
    """Claims, publishes with PubAck, marks; backs off failed rows from 1 s to 1 min (P12.1)."""

    def __init__(self, store: "Store", bus: "Bus", pub: Publisher, metrics: "BeMetrics", logger: logging.Logger):
        self.store, self.bus, self.pub, self.metrics, self.logger = store, bus, pub, metrics, logger
        self._kick = asyncio.Event()

    def kick(self) -> None:
        self._kick.set()

    async def run(self) -> None:
        idle = BUSY
        while True:
            n = await self.round()
            idle = BUSY if n else min(IDLE, idle * 2)
            try:
                await asyncio.wait_for(self._kick.wait(), idle)
            except TimeoutError:
                pass
            self._kick.clear()

    async def round(self) -> int:
        rows = await self.store.tx(lambda tx: tx.fetch(_CLAIM))
        if rows:
            results = await asyncio.gather(*(self._publish(r) for r in rows), return_exceptions=True)
            ok = [r["id"] for r, res in zip(rows, results) if res is None]
            failed = [(r, res) for r, res in zip(rows, results) if res is not None]

            async def mark(tx):
                if ok:
                    await tx.execute(_DONE, ok)
                for r, e in failed:
                    await tx.execute(_RETRY, r["id"], r["created_at"], f"{type(e).__name__}: {e}"[:200])

            await self.store.tx(mark)
            for r, res in zip(rows, results):
                if res is None:
                    self.metrics.events_published.labels(subject=r["subject"]).inc()
            if failed:
                self.logger.warning("outbox_publish_failed", extra={"rows": len(failed),
                                                                     "error": type(failed[0][1]).__name__})
        await self._gauges()
        return len(rows)

    async def _publish(self, r: Any) -> None:
        decl = self.pub.contract.get(r["subject"])
        row = OutboxRow(id=str(r["id"]), subject=r["subject"], aggregate_type=r["aggregate_type"],
                        aggregate_id=r["aggregate_id"], aggregate_version=r["aggregate_version"],
                        occurred_at=r["occurred_at"], traceparent=r["traceparent"], causation_id=r["causation_id"],
                        hop_count=r["hop_count"], payload_json=r["payload"])
        h = headers_of(row, component_id=self.pub.member, version=self.pub.version,
                       events_file=decl.file if decl else "", transaction_document=False)
        await self.bus.ensure_stream_for(r["subject"])
        await self.bus.publish(r["subject"], r["payload"].encode(), h)

    async def _gauges(self) -> None:
        n, age = await self.store.tx(lambda tx: tx.fetchrow(_GAUGE))
        self.metrics.outbox_pending.set(n)
        self.metrics.outbox_oldest_age.set(float(age))


