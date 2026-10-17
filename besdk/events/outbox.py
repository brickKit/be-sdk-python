"""The producer side of chapter 06: the outbox written in the business transaction (P6.2), numbering after commit
under the partition counter's lock with the database epoch (P6.5-P6.8), and the forwarder (P6.10-P6.12)."""
from __future__ import annotations

import asyncio
import contextlib
import contextvars
import json
import uuid
from datetime import datetime, timezone
from typing import Iterator, Protocol

from besdk import errs
from besdk.db.batch import delete_in_batches
from besdk.db.pool import DB, Tx, TxOptions
from besdk.events.core import (MAX_HOPS, MAX_PAYLOAD, MAX_SUBJECTS, REDELIVERY_RETENTION, Event, Message, Projection,
                               Row, Stream, Transport, digest, subject)
from besdk.events.objectstore import sha256_hex
from besdk.obs import PROPAGATOR, Logger

NUMBER_LOCK_WAIT, NUMBER_TX_LIMIT = 2.0, 5.0  # P6.5
_cause: contextvars.ContextVar[tuple[str, int] | None] = contextvars.ContextVar("be_event_cause", default=None)


@contextlib.contextmanager
def caused_by(event_id: str, hops: int) -> Iterator[None]:
    """Mark the processing of an event: events emitted inside carry the causation and one more hop (P6.15)."""
    token = _cause.set((event_id, hops))
    try:
        yield
    finally:
        _cause.reset(token)


class ClaimStore(Protocol):
    """Puts a large payload into object storage and returns its reference URI (called outside any transaction)."""

    async def put(self, event_id: str, data: bytes) -> str: ...


def _internal(msg: str) -> errs.Error:
    return errs.new("INTERNAL").wrap(ValueError(msg))


class Producer:
    """Writes a member's events into its outbox."""

    def __init__(self, source: str, streams: list[Stream], claims: ClaimStore | None = None):
        for s in streams:
            s.validate()
        self.source, self.streams, self.claims = source, {s.name: s for s in streams}, claims

    async def emit(self, tx: Tx, stream: str, ev: Event) -> str:
        """One event into the outbox inside the business transaction (P6.2); returns its event ID."""
        s = self.streams.get(stream)
        if s is None:
            raise _internal(f"events: stream {stream!r} is not declared")
        if not ev.type or not ev.record_id:
            raise _internal("events: an event needs a type and a record ID")
        if ev.projection not in tuple(Projection):
            raise _internal(f"events: projection {ev.projection!r}")
        if ev.subjects is not None and len(ev.subjects) > MAX_SUBJECTS:
            raise _internal(f"events: {len(ev.subjects)} subjects, at most {MAX_SUBJECTS}")
        payload = b"{}" if ev.data is None else json.dumps(ev.data, ensure_ascii=False, separators=(",", ":")).encode()
        if len(payload) > MAX_PAYLOAD and self.claims is None:
            raise _internal(f"events: a {len(payload)}-byte payload needs object storage for a claim check")
        hops, causation = 0, None
        if (c := _cause.get()) is not None:
            hops, causation = c[1] + 1, c[0]
        if hops > MAX_HOPS:
            raise _internal(f"events: {hops} hops exceed the limit of {MAX_HOPS} (a causal loop?)")
        trace: dict[str, str] = {}
        PROPAGATOR.inject(trace)
        eid = str(uuid.uuid7())
        await tx.execute("""INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
            projection, legal_entity, subjects, causation, hops, trace, payload, digest)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13::jsonb, $14, $15)""",
                         uuid.UUID(eid), s.name, s.partition_of(ev.record_id), ev.type, datetime.now(timezone.utc),
                         ev.record_id, ev.version, str(ev.projection), ev.legal_entity or None,
                         None if ev.subjects is None else json.dumps(ev.subjects),
                         uuid.UUID(causation) if causation else None, hops, json.dumps(trace) if trace else None,
                         payload, digest(eid, payload))
        return eid


async def number(db: DB, stream: str, partition: int, batch: int) -> int:
    """Give the visible, unnumbered rows of one partition their positions (P6.5-P6.8): lock the counter, read the
    epoch, start a new epoch with a history entry when it changed, then number in insertion order with a fresh
    statement snapshot. Returns how many rows were numbered."""

    async def run(tx: Tx) -> int:
        await tx.execute("INSERT INTO be_outbox_partition (stream, partition) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                         stream, partition)
        row = await tx.fetchrow("""SELECT epoch_generation, epoch_timeline, last_seq FROM be_outbox_partition
            WHERE stream = $1 AND partition = $2 FOR UPDATE""", stream, partition)
        try:
            now = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
        except Exception as e:  # noqa: BLE001 - a standby or no recovery generation: numbering is refused
            raise RuntimeError("events: the database epoch cannot be read; numbering refused") from e
        last = row["last_seq"]
        if row["epoch_generation"] != now["generation"] or row["epoch_timeline"] != now["timeline"]:
            if row["epoch_generation"] is not None:
                await tx.execute("""INSERT INTO be_epoch_history (stream, partition, old_generation, old_timeline,
                    fork_seq, new_generation, new_timeline) VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                                 stream, partition, row["epoch_generation"], row["epoch_timeline"], last,
                                 now["generation"], now["timeline"])
            last = 0
        # a new statement, so a new snapshot taken after the lock was granted (READ COMMITTED)
        status = await tx.execute("""WITH picked AS (
                SELECT ord, row_number() OVER (ORDER BY ord) AS rn FROM (
                    SELECT ord FROM be_outbox WHERE stream = $1 AND partition = $2 AND seq IS NULL ORDER BY ord LIMIT $3
                ) p
            )
            UPDATE be_outbox o SET seq = $4 + picked.rn, epoch_generation = $5, epoch_timeline = $6
            FROM picked WHERE o.ord = picked.ord""", stream, partition, batch, last, now["generation"], now["timeline"])
        n = int(status.rsplit(" ", 1)[-1])
        await tx.execute("""UPDATE be_outbox_partition SET last_seq = $3, epoch_generation = $4, epoch_timeline = $5
            WHERE stream = $1 AND partition = $2""", stream, partition, last + n, now["generation"], now["timeline"])
        return n

    return await db.background_tx(run, TxOptions(lock_timeout=NUMBER_LOCK_WAIT, transaction_timeout=NUMBER_TX_LIMIT))


DEFAULT_LEASE, DEFAULT_POLL, DEFAULT_BATCH = 10.0, 0.25, 500


class Forwarder:
    """Numbers and publishes a member's outbox (P6.10, P6.11)."""

    def __init__(self, db: DB, producer: Producer, transport: Transport, source: str, impl: str, log: Logger,
                 lease: float = DEFAULT_LEASE, poll: float = DEFAULT_POLL, batch: int = DEFAULT_BATCH):
        self.db, self.producer, self.transport, self.source, self.impl, self.log = db, producer, transport, source, impl, log
        self.lease, self.poll, self.batch, self.owner = lease, poll, batch, str(uuid.uuid7())
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Create the streams' storage, then forward in the background until stop."""
        for s in self.producer.streams.values():
            await self.transport.ensure_stream(s)
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        while True:
            try:
                n = await self.once()
            except Exception as e:  # noqa: BLE001 - retried; the rows stay in the outbox
                self.log.warn("forwarding events failed; retrying", error=repr(e))
                n = 0
            if n == 0:
                await asyncio.sleep(self.poll)

    async def once(self) -> int:
        """Forward every partition once; returns how many messages were published."""
        total, first = 0, None
        for s in self.producer.streams.values():
            for p in range(s.partitions):
                try:
                    total += await self._partition(s, p)
                except Exception as e:  # noqa: BLE001
                    first = first or e
        if first is not None:
            raise first
        return total

    async def _partition(self, s: Stream, p: int) -> int:
        async def lease(tx: Tx) -> bool:
            await tx.execute("INSERT INTO be_outbox_partition (stream, partition) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                             s.name, p)
            status = await tx.execute("""UPDATE be_outbox_partition SET lease_owner = $3,
                lease_until = clock_timestamp() + make_interval(secs => $4)
                WHERE stream = $1 AND partition = $2
                  AND (lease_until IS NULL OR lease_until < clock_timestamp() OR lease_owner = $3)""",
                                      s.name, p, self.owner, self.lease)
            return status.endswith(" 1")

        if not await self.db.background_tx(lease):
            return 0  # another replica holds the lease: it only reduces contention (P6.11)
        await number(self.db, s.name, p, self.batch)
        rows = await self._unpublished(s.name, p)
        done: list[int] = []
        claimed: list[int] = []
        try:
            for r in rows:
                m, is_claim = await self._message(r)
                await self.transport.publish(m)  # published again later if this fails: at least once (P6.10)
                done.append(r.ord)
                if is_claim:
                    claimed.append(r.ord)
        finally:
            if done:
                await self.db.background_tx(lambda tx: tx.execute(
                    """UPDATE be_outbox SET published_at = now(), claim = coalesce(ord = ANY($2), false)
                    WHERE ord = ANY($1) AND published_at IS NULL""", done, claimed))
        return len(done)

    async def _message(self, r: Row) -> tuple[Message, bool]:
        payload, claim = r.payload, False
        if len(payload) > MAX_PAYLOAD:
            if self.producer.claims is None:
                raise RuntimeError(f"event {r.event_id}: {len(payload)} bytes and no object storage for a claim check")
            uri = await self.producer.claims.put(r.event_id, payload)
            payload = json.dumps({"claim": {"uri": uri, "size": len(r.payload), "sha256": sha256_hex(r.payload)}},
                                 separators=(",", ":")).encode()
            claim = True
        r.claim = claim
        return Message(subject(r.stream, r.partition, r.type), r.headers(self.source, self.impl), payload), claim

    async def _unpublished(self, stream: str, p: int) -> list[Row]:
        rows = await self.db.background_tx(lambda tx: tx.fetch(
            """SELECT ord, event_id::text AS event_id, type, occurred_at, record_id, version, projection,
            coalesce(legal_entity, '') AS legal_entity, subjects, coalesce(causation::text, '') AS causation, hops,
            trace, payload, digest, seq, epoch_generation, epoch_timeline
            FROM be_outbox WHERE stream = $1 AND partition = $2 AND seq IS NOT NULL AND published_at IS NULL
            ORDER BY epoch_generation, epoch_timeline, seq LIMIT $3""", stream, p, self.batch))
        return [Row(r["ord"], r["event_id"], stream, p, r["type"], r["occurred_at"], r["record_id"], r["version"],
                    r["projection"], bytes(r["payload"]), r["digest"], r["seq"],
                    (r["epoch_generation"], r["epoch_timeline"]), r["legal_entity"],
                    json.loads(r["subjects"]) if r["subjects"] else None, r["causation"], r["hops"],
                    json.loads(r["trace"]) if r["trace"] else {}) for r in rows]


async def purge(db: DB, retention: float = REDELIVERY_RETENTION) -> int:
    """Published rows past the redelivery retention, in batches (P6.12); unpublished rows are never deleted."""
    return await delete_in_batches(db, """DELETE FROM be_outbox WHERE ord IN (SELECT ord FROM be_outbox
        WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => $1)
        LIMIT $2 FOR UPDATE SKIP LOCKED)""", retention)
