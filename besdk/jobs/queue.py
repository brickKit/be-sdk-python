"""Queue jobs (be-protocol P14, kind ``queue``): ``tx.enqueue`` inserts in the business transaction (a job
of a rolled-back transaction does not exist); workers claim ready rows with ``SKIP LOCKED``, run the
handler outside any transaction, retry with backoff and, past ``max_attempts``, mark the row ``dead`` and
call ``on_dead`` in a transaction. A ``running`` row past its lease is claimable again (at least once)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from besdk import context, errors, ids
from besdk.jobs.model import QueuedJob, Worker

if TYPE_CHECKING:
    from besdk.store.tx import Tx

LEASE_GRACE = 30.0
IDLE_MIN, IDLE_MAX = 0.2, 2.0

_ENQUEUE = ("INSERT INTO besdk_job_queue (id, kind, args, unique_key, run_at, max_attempts, traceparent, "
            "causation_id, hop_count) VALUES ($1, $2, $3::jsonb, $4, coalesce($5, now()), $6, $7, $8, $9) "
            "ON CONFLICT (kind, unique_key) WHERE unique_key IS NOT NULL AND state <> 'done' DO NOTHING "
            "RETURNING 1")
_CLAIM = ("UPDATE besdk_job_queue SET state = 'running', attempts = attempts + 1, "
          "lease_until = now() + make_interval(secs => $3) WHERE id IN (SELECT id FROM besdk_job_queue "
          "WHERE kind = $1 AND ((state = 'ready' AND run_at <= now()) OR (state = 'running' AND lease_until < now())) "
          "ORDER BY run_at, id LIMIT $2 FOR UPDATE SKIP LOCKED) "
          "RETURNING id, kind, args::text AS args, attempts, max_attempts, unique_key, last_error")
_DONE = ("UPDATE besdk_job_queue SET state = 'done', finished_at = now(), lease_until = NULL "
         "WHERE id = $1 AND state = 'running'")
_RETRY = ("UPDATE besdk_job_queue SET state = 'ready', lease_until = NULL, last_error = $2, "
          "run_at = now() + make_interval(secs => $3) WHERE id = $1 AND state = 'running'")
_DEAD = ("UPDATE besdk_job_queue SET state = 'dead', lease_until = NULL, last_error = $2, finished_at = now() "
         "WHERE id = $1 AND state = 'running' RETURNING 1")
_DEPTH = ("SELECT state, count(*) AS n, coalesce(extract(epoch FROM now() - min(run_at) "
          "FILTER (WHERE state = 'ready' AND run_at <= now())), 0) AS age "
          "FROM besdk_job_queue WHERE kind = $1 AND state <> 'done' GROUP BY state")


async def enqueue(tx: "Tx", kind: str, args: Any, *, run_at: datetime | None = None,
                  unique_key: str | None = None) -> bool:
    """Insert a job in this transaction; False when a live job with ``unique_key`` already exists."""
    jobs = getattr(tx.store, "jobs", None)
    worker = jobs.workers.get(kind) if jobs is not None else None
    if worker is None:
        raise errors.internal(f"enqueue: no Worker for kind {kind!r} in this member")
    carrier: dict[str, str] = {}
    jobs.rt.telemetry.propagator.inject(carrier)
    u = context.current()
    causation, hop = (u.event.id, u.event.hop_count + 1) if u.event else ("", 0)
    row = await tx.fetchval(_ENQUEUE, ids.new_id(), kind, json.dumps(args, ensure_ascii=False, default=str),
                            unique_key, run_at, worker.max_attempts, carrier.get("traceparent", ""), causation, hop)
    return row is not None


class WorkerLoop:
    """One worker kind on one replica: claim up to ``concurrency`` rows, run them, record the outcome."""

    def __init__(self, rt: Any, worker: Worker, execute: Callable[..., Awaitable[None]]):
        self.rt, self.w, self.execute = rt, worker, execute
        self.store = rt.store()

    async def run(self) -> None:
        idle = IDLE_MIN
        while True:
            n = await self.round()
            await self._gauges()
            idle = IDLE_MIN if n else min(IDLE_MAX, idle * 2)
            await asyncio.sleep(idle)

    async def round(self) -> int:
        w = self.w
        rows = await self.store.tx(lambda tx: tx.fetch(_CLAIM, w.kind, w.concurrency, w.timeout + LEASE_GRACE))
        await asyncio.gather(*(self._one(r) for r in rows))
        return len(rows)

    async def _one(self, r: Any) -> None:
        j = QueuedJob(id=str(r["id"]), kind=r["kind"], args=json.loads(r["args"]), attempts=r["attempts"],
                      max_attempts=r["max_attempts"], unique_key=r["unique_key"], last_error=r["last_error"])
        try:
            await self.execute(self.w.kind, lambda: self.w.run(j), self.w.timeout)
        except Exception as e:  # noqa: BLE001 - recorded on the row
            await self._failed(r["id"], j, f"{type(e).__name__}: {e}"[:500])
            return
        await self.store.tx(lambda tx: tx.execute(_DONE, r["id"]))

    async def _failed(self, rid: Any, j: QueuedJob, text: str) -> None:
        w = self.w
        if j.attempts < j.max_attempts:
            delay = w.backoff[min(j.attempts - 1, len(w.backoff) - 1)] if w.backoff else 1.0
            await self.store.tx(lambda tx: tx.execute(_RETRY, rid, text, float(delay)))
            return
        dead = QueuedJob(j.id, j.kind, j.args, j.attempts, j.max_attempts, j.unique_key, text)

        async def bury(tx: "Tx") -> None:
            if await tx.fetchval(_DEAD, rid, text) and w.on_dead is not None:
                await w.on_dead(tx, dead)

        await self.store.tx(bury)
        self.rt.logger.error("queue_job_dead", extra={"kind": j.kind, "job_id": j.id, "error": text})

    async def _gauges(self) -> None:
        rows = await self.store.tx(lambda tx: tx.fetch(_DEPTH, self.w.kind))
        m, seen = self.rt.metrics, {r["state"] for r in rows}
        for r in rows:
            m.queue_depth.labels(kind=self.w.kind, state=r["state"]).set(r["n"])
        for s in {"ready", "running", "dead"} - seen:
            m.queue_depth.labels(kind=self.w.kind, state=s).set(0)
        m.queue_oldest_age.labels(kind=self.w.kind).set(max((float(r["age"]) for r in rows), default=0.0))
