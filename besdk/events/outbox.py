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
# the claim-check uploads of one batch in flight at once (P6.14): one at a time would cap a partition at
# 1/upload-time events a second whatever the machine
CLAIM_CONCURRENCY = 8
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
        self.written = asyncio.Event()  # a commit wrote outbox rows: the member's forwarder starts at once (P6.29)
        self._dirty: set[tuple[str, int]] = set()  # the partitions committed rows went to since the forwarder took them

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
        part = (s.name, s.partition_of(ev.record_id))
        await tx.execute("""INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
            projection, legal_entity, subjects, causation, hops, trace, payload, digest)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13::jsonb, $14, $15)""",
                         uuid.UUID(eid), part[0], part[1], ev.type, datetime.now(timezone.utc),
                         ev.record_id, ev.version, str(ev.projection), ev.legal_entity or None,
                         None if ev.subjects is None else json.dumps(ev.subjects),
                         uuid.UUID(causation) if causation else None, hops, json.dumps(trace) if trace else None,
                         payload, digest(eid, payload))
        tx.after_commit(lambda: self._wake(part))
        return eid

    def _wake(self, part: tuple[str, int]) -> None:
        """Records the partition a commit wrote to and tells the forwarder."""
        self._dirty.add(part)
        self.written.set()

    def take_written(self) -> list[tuple[str, int]]:
        """The partitions committed rows went to since the last call, forgotten."""
        out, self._dirty = list(self._dirty), set()
        self.written.clear()
        return out


NUMBER_LIMITS = TxOptions(lock_timeout=NUMBER_LOCK_WAIT, transaction_timeout=NUMBER_TX_LIMIT)


async def number(db: DB, stream: str, partition: int, batch: int) -> int:
    """Give the visible, unnumbered rows of one partition their positions (P6.5-P6.8): lock the counter, read the
    epoch, start a new epoch with a history entry when it changed, then number in insertion order with a fresh
    statement snapshot. Returns how many rows were numbered."""
    return await db.background_tx(lambda tx: number_in(tx, stream, partition, batch), NUMBER_LIMITS)


async def number_in(tx: Tx, stream: str, partition: int, batch: int) -> int:
    """number's work inside a transaction the caller opened with NUMBER_LIMITS."""
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


DEFAULT_LEASE, DEFAULT_POLL, DEFAULT_BATCH = 10.0, 0.25, 500


class Forwarder:
    """Numbers and publishes a member's outbox (P6.10, P6.11)."""

    def __init__(self, db: DB, producer: Producer, transport: Transport, source: str, impl: str, log: Logger,
                 lease: float = DEFAULT_LEASE, poll: float = DEFAULT_POLL, batch: int = DEFAULT_BATCH):
        self.db, self.producer, self.transport, self.source, self.impl, self.log = db, producer, transport, source, impl, log
        self.lease, self.poll, self.batch, self.owner = lease, poll, batch, str(uuid.uuid7())
        self._task: asyncio.Task | None = None
        self._held: dict[tuple[str, int], float] = {}  # until when this forwarder holds each lease, by its own clock

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
        """Forwards on two signals (P6.29): a commit of this member wakes it for the partitions that commit wrote to,
        and the poll takes every partition at its own pace, for other replicas' rows and retries. A wake never
        postpones the poll: under a steady stream of commits it would never come."""
        loop = asyncio.get_running_loop()
        next_poll = loop.time()
        while True:
            if (wait := next_poll - loop.time()) > 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.producer.written.wait(), wait)
            poll = loop.time() >= next_poll
            written = self.producer.take_written()
            try:
                await self._forward(self._all() if poll else written)
            except Exception as e:  # noqa: BLE001 - retried; the rows stay in the outbox
                self.log.warn("forwarding events failed; retrying", error=repr(e))
            if poll:
                next_poll = loop.time() + self.poll

    async def once(self) -> int:
        """Forward every partition, again while one had a full batch; returns how many messages were published."""
        return await self._forward(self._all())

    def _all(self) -> list[tuple[str, int]]:
        return [(s.name, p) for s in self.producer.streams.values() for p in range(s.partitions)]

    async def _forward(self, parts: list[tuple[str, int]]) -> int:
        """Forwards parts, then again the ones whose batch was full, until none is; raises the first error after
        the others were tried."""
        total, first = 0, None
        while parts:
            full = []
            for part in parts:
                try:
                    n = await self._partition(*part)
                except Exception as e:  # noqa: BLE001
                    first = first or e
                    continue
                total += n
                if n == self.batch:
                    full.append(part)
            parts = full
        if first is not None:
            raise first
        return total

    async def _lease(self, stream: str, p: int) -> bool:
        """Takes or renews the partition's lease (P6.11), but only once half of it has passed: within its first
        half this forwarder still holds it, and the lease only reduces contention."""
        loop = asyncio.get_running_loop()
        if self._held.get((stream, p), 0.0) - loop.time() > self.lease / 2:
            return True
        asked = loop.time()  # the database's lease ends lease after it granted it, so no sooner than this plus lease

        async def take(tx: Tx) -> bool:
            await tx.execute("INSERT INTO be_outbox_partition (stream, partition) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                             stream, p)
            status = await tx.execute("""UPDATE be_outbox_partition SET lease_owner = $3,
                lease_until = clock_timestamp() + make_interval(secs => $4)
                WHERE stream = $1 AND partition = $2
                  AND (lease_until IS NULL OR lease_until < clock_timestamp() OR lease_owner = $3)""",
                                      stream, p, self.owner, self.lease)
            return status.endswith(" 1")

        held = False
        try:
            held = await self.db.background_tx(take)
        finally:
            if held:
                self._held[(stream, p)] = asked + self.lease
            else:
                self._held.pop((stream, p), None)
        return held

    async def _partition(self, stream: str, p: int) -> int:
        if not await self._lease(stream, p):
            return 0  # another replica holds the lease: it only reduces contention (P6.11)

        async def numbered(tx: Tx) -> list[Row]:
            # numbering and reading what is numbered and unpublished in one transaction: one round trip fewer
            await number_in(tx, stream, p, self.batch)
            return await self._unpublished(tx, stream, p)

        rows = await self.db.background_tx(numbered, NUMBER_LIMITS)
        done: list[int] = []
        claimed: list[int] = []
        # the messages are prepared in the background and published in position order as each is ready: the
        # claim-check uploads do not depend on each other, only the publishing order is fixed (P6.10)
        slots = asyncio.Semaphore(CLAIM_CONCURRENCY)

        async def prepared(r: Row) -> tuple[Message, bool]:
            if len(r.payload) <= MAX_PAYLOAD:
                return await self._message(r)
            async with slots:
                return await self._message(r)

        tasks = [asyncio.create_task(prepared(r)) for r in rows]
        try:
            for r, t in zip(rows, tasks):
                m, is_claim = await t
                await self.transport.publish(m)  # published again later if this fails: at least once (P6.10)
                done.append(r.ord)
                if is_claim:
                    claimed.append(r.ord)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
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

    async def _unpublished(self, tx: Tx, stream: str, p: int) -> list[Row]:
        rows = await tx.fetch(
            """SELECT ord, event_id::text AS event_id, type, occurred_at, record_id, version, projection,
            coalesce(legal_entity, '') AS legal_entity, subjects, coalesce(causation::text, '') AS causation, hops,
            trace, payload, digest, seq, epoch_generation, epoch_timeline
            FROM be_outbox WHERE stream = $1 AND partition = $2 AND seq IS NOT NULL AND published_at IS NULL
            ORDER BY epoch_generation, epoch_timeline, seq LIMIT $3""", stream, p, self.batch)
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
