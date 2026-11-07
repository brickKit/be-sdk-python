"""A member's background work (protocol chapter 07): one poll timer and a few workers claiming items atomically
(P7.5, P7.22, P7.23), a wake-up per item a commit enqueued (P7.21), the cron scheduler under each job's watermark lock
(P7.16), singletons (P7.17) and every jobs (P7.18). A run renews its lease every third of it and stops when a commit
finds the claim lost (P7.6, P7.7); stopping cancels the handlers and gives their leases and attempts back (P7.8)."""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
import time
import uuid
from typing import Any

from besdk import reqctx
from besdk.db.pool import DB, Tx
from besdk.obs import Logger
from besdk.work.job import (BACKOFF_CAP, BACKOFF_START, CRON, EVERY, NON_IDEMPOTENT_EXTERNAL, QUEUE, SINGLETON, Item,
                            Job, Jobs, Lost, backoff, decode, kind_of)
from besdk.work.run import Run, lost

DEFAULT_POLL = 0.5
DEFAULT_CONCURRENCY = 4
SCHEDULE_EVERY = 1.0

# P7.22: an expired lease first, else the earliest due item, each through its own ordered partial index, so a backlog
# does not slow a claim (an OR over both indexes fetched and sorted the whole backlog).
CLAIM = """WITH expired AS (
        SELECT id FROM be_work_item WHERE job = ANY($1::text[]) AND state = 'running' AND lease_until <= now()
        ORDER BY lease_until LIMIT 1 FOR UPDATE SKIP LOCKED),
    due AS (
        SELECT id FROM be_work_item WHERE job = ANY($1::text[]) AND state IN ('pending', 'retry_wait') AND run_at <= now()
            AND NOT EXISTS (SELECT 1 FROM expired)
        ORDER BY run_at LIMIT 1 FOR UPDATE SKIP LOCKED),
    c AS (SELECT id FROM expired UNION ALL SELECT id FROM due)
    UPDATE be_work_item w SET state = 'running', epoch = w.epoch + 1, attempts = w.attempts + 1, holder = $2,
        lease_until = now() + make_interval(secs => $3), claim_db_generation = e.generation,
        claim_db_timeline = e.timeline
    FROM c, be_current_db_epoch() e WHERE w.id = c.id
    RETURNING w.id, w.job, w.kind, w.epoch, w.claim_db_generation, w.claim_db_timeline, w.attempts, w.max_attempts,
        w.effect_class, w.operation_id, w.outcome_unknown, w.args, w.progress, coalesce(w.slot, '') AS slot,
        w.call_results"""


class Runner:
    """A member's runner; jobs are added before start."""

    def __init__(self, db: DB, member: str, log: Logger, lease: float = 30.0, poll: float = DEFAULT_POLL,
                 concurrency: int = DEFAULT_CONCURRENCY):
        self.db, self.log, self.lease, self.poll, self.concurrency = db, log, lease, poll, max(concurrency, 1)
        self.holder = f"{member}/{uuid.uuid7()}"
        self.jobs: dict[str, Job] = {}
        self.names: list[str] = []  # the jobs with work items (all but every)
        self.claims = 0  # claim statements run: what idle background work costs the database (P7.23)
        self.materialized = 0  # materializing transactions run
        self._ready: asyncio.Queue[bool] | None = None
        self._loops: list[asyncio.Task] = []
        self._workers: list[asyncio.Task] = []
        self._stopping = False
        self._next_slot: dict[str, dt.datetime] = {}  # each cron job's next slot after its watermark (P7.23)
        self._skew = 0.0  # the database's clock minus this host's, seconds, at the last materialization

    def add(self, *jobs: Job) -> None:
        for j in jobs:
            j.prepare()
            if j.name in self.jobs:
                raise ValueError(f"work: job {j.name} declared twice")
            self.jobs[j.name] = j
            if j.kind != EVERY:
                self.names.append(j.name)

    def add_all(self, jobs: Jobs) -> None:
        self.add(*jobs.list())

    @property
    def empty(self) -> bool:
        return not self.jobs

    async def enqueue(self, tx: Tx, job: str, args: Any = None, at: dt.datetime | None = None) -> str:
        """Puts a queue job's item into the business transaction (P7.3) and returns its ID. It is due at once, or at
        an instant by the database clock (P7.24). An item cannot be withdrawn: a handler that finds its business state
        changed since voids it."""
        j = self.jobs.get(job)
        if j is None or j.kind != QUEUE:
            raise ValueError(f"work: {job} is not a declared queue job")
        iid = str(uuid.uuid7())
        await tx.execute("""INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id,
            args, run_at) VALUES ($1, $2, 'queue', 'pending', $3, $4, $5, $6::jsonb, coalesce($7::timestamptz, now()))""",
                         iid, job, j.max_attempts, j.effect, f"{iid}:{job}", json.dumps(args), at)
        if at is None or at <= dt.datetime.now(dt.UTC):  # an item not yet due is found by the poll once it is
            tx.after_commit(lambda: self._wake(False))
        return iid

    def _wake(self, polled: bool) -> None:
        """Wakes one idle worker; a full queue means every worker is already woken (P7.21, P7.23)."""
        if self._ready is not None:
            with contextlib.suppress(asyncio.QueueFull):
                self._ready.put_nowait(polled)

    async def start(self) -> None:
        """Runs the loops and workers until stop."""
        self._ready = asyncio.Queue(maxsize=self.concurrency)
        for name in self.names:
            if (j := self.jobs[name]).kind == SINGLETON:
                await self._ensure_singleton(j)
        if any(j.kind == CRON for j in self.jobs.values()):
            self._loops.append(asyncio.create_task(self._loop(SCHEDULE_EVERY, self._schedule)))
        if self.names:
            # one poll for all the workers (P7.23): idle, a member claims once a poll whatever its concurrency
            self._loops.append(asyncio.create_task(self._loop(self.poll, self._poll)))
            self._workers = [asyncio.create_task(self._work()) for _ in range(self.concurrency)]
        for j in self.jobs.values():
            if j.kind == EVERY:
                self._loops.append(asyncio.create_task(self._loop(float(j.interval), self._every(j))))

    async def stop(self) -> None:
        """Cancels the loops and the running handlers; each handler's lease and attempt are given back (P7.8)."""
        self._stopping = True
        for t in self._loops + self._workers:
            t.cancel()
        await asyncio.gather(*self._loops, *self._workers, return_exceptions=True)
        self._loops, self._workers = [], []

    async def _poll(self) -> bool:
        self._wake(True)
        return False

    async def _loop(self, every: float, fn) -> None:
        """Runs fn at once, then every interval, and at once again while it finds more."""
        while True:
            more = False
            try:
                more = await fn()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - a loop never ends on a failure
                self.log.warn("background work failed", error=str(e))
            if not more:
                await asyncio.sleep(every)

    def _every(self, j: Job):
        async def run() -> bool:
            try:
                with reqctx.deadline_scope(time.monotonic() + float(j.timeout)):
                    async with asyncio.timeout(float(j.timeout)):
                        await j.handle(None)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self.log.warn("an every job failed", job=j.name, error=str(e))
            return False
        return run

    async def _ensure_singleton(self, j: Job) -> None:
        async def body(tx: Tx) -> None:
            await tx.execute("""INSERT INTO be_work_item (id, job, kind, state, max_attempts, effect_class, operation_id)
                VALUES ($1, $1, 'singleton', 'pending', $2, $3, $4) ON CONFLICT DO NOTHING""",
                             j.name, j.max_attempts, j.effect, str(uuid.uuid7()))
        await self.db.background_tx(body)

    # --- cron (P7.15, P7.16, P7.23)

    async def _schedule(self) -> bool:
        """Materializes due cron slots under each job's watermark lock. Before a job's next slot it opens no
        transaction; the slot's time is on the database clock, compared through the skew seen at the last look."""
        for name in self.names:
            j = self.jobs[name]
            if j.kind != CRON:
                continue
            at = self._next_slot.get(name)
            if at is not None and dt.datetime.now(dt.UTC) + dt.timedelta(seconds=self._skew) < at:
                continue
            local = dt.datetime.now(dt.UTC)
            try:
                nxt, db_now = await self.materialize(name)
            except Exception as e:  # noqa: BLE001
                self.log.warn("materializing cron slots failed", job=name, error=str(e))
                continue
            self._skew, self._next_slot[name] = (db_now - local).total_seconds(), nxt
        return False

    async def materialize(self, job: str, now: dt.datetime | None = None) -> tuple[dt.datetime, dt.datetime]:
        """Turns a cron job's due slots into work items under its watermark lock (P7.16); returns the first slot
        after the new watermark (far ahead when the schedule has none) and the time it materialized at. now None:
        the database clock."""
        from besdk.work import cron

        j = self.jobs[job]
        self.materialized += 1
        out: dict[str, dt.datetime] = {}

        async def body(tx: Tx) -> None:
            at = now if now is not None else await tx.fetchval("SELECT now()")
            out["at"] = at
            await tx.execute("INSERT INTO be_cron_watermark (job, watermark_at) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                             j.name, at)
            wm = await tx.fetchval("SELECT watermark_at FROM be_cron_watermark WHERE job = $1 FOR UPDATE", j.name)
            pending, paused, mark = cron.plan(j.schedule, j.backfill, j.max_backlog, wm, at)
            nxt = j.schedule.next_after(mark)
            out["next"] = nxt.at if nxt is not None else wm + dt.timedelta(days=36500)
            for slots, state in ((pending, "pending"), (paused, "paused")):
                for s in slots:
                    await tx.execute("""INSERT INTO be_work_item (id, job, kind, state, pause_reason, max_attempts,
                        effect_class, operation_id, slot, run_at) VALUES ($1, $2, 'cron', $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT DO NOTHING""", f"{j.name}@{s.id}", j.name, state,
                                     "backlog_exceeded" if state == "paused" else None, j.max_attempts, j.effect,
                                     str(uuid.uuid7()), s.id, s.at)
            if mark == wm:
                return
            every = pending + paused
            await tx.execute("""UPDATE be_cron_watermark SET watermark_at = $2, last_slot = coalesce($3, last_slot)
                WHERE job = $1""", j.name, mark, every[-1].id if every else None)

        await self.db.background_tx(body)
        return out["next"], out["at"]

    # --- workers

    async def _work(self) -> None:
        """One worker: woken, it claims (P7.5) and runs items until it finds none. Woken by the poll, it wakes another
        worker for each item it finds, so a backlog the poll found spreads over all of them (P7.23)."""
        assert self._ready is not None
        while True:
            polled = await self._ready.get()
            while True:
                try:
                    r = await self.claim()
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    self.log.warn("claiming work failed", error=str(e))
                    break
                if r is None:
                    break
                if polled:
                    self._wake(True)
                await self.run(r)

    async def claim(self) -> Run | None:
        """Claims one item (P7.5): an expired lease first, else the earliest due one; None when nothing is due. A
        non-idempotent item whose call may have been sent is paused instead (P7.12)."""
        if not self.names:
            return None
        self.claims += 1
        out: list[Run] = []

        async def body(tx: Tx) -> None:
            out.clear()
            row = await tx.fetchrow(CLAIM, self.names, self.holder, float(self.lease))
            if row is None:
                return
            if row["effect_class"] == NON_IDEMPOTENT_EXTERNAL and row["outcome_unknown"]:
                await tx.execute("""UPDATE be_work_item SET state = 'paused', pause_reason = 'outcome_unknown',
                    holder = NULL, lease_until = NULL WHERE id = $1""", row["id"])
                self.log.error("a non-idempotent call's outcome is unknown; the item is paused for an operator",
                               job=row["job"], item=row["id"])
                return
            item = Item(row["id"], row["job"], row["kind"], row["attempts"], row["max_attempts"], row["operation_id"],
                        decode(row["args"]), decode(row["progress"]), row["slot"], decode(row["call_results"]) or {})
            out.append(Run(item, self.jobs[row["job"]], self.db, row["epoch"], row["claim_db_generation"],
                           row["claim_db_timeline"]))

        await self.db.background_tx(body)
        return out[0] if out else None

    async def run(self, r: Run) -> None:
        """Runs one claim: the handler under the job's deadline, the lease renewed every third of it, then complete,
        fail or nothing (lost). Cancelled by stop, it gives the lease and the attempt back (P7.8)."""
        j = r._job
        state = {"lost": False}

        async def handler() -> None:
            with reqctx.deadline_scope(time.monotonic() + float(j.timeout)):
                async with asyncio.timeout(float(j.timeout)):
                    await j.handle(r)

        h = asyncio.create_task(handler())

        def on_lost() -> None:  # taken over or the epoch changed: stop at once (P7.6)
            state["lost"] = True
            h.cancel()

        r.on_lost = on_lost

        async def renew() -> None:
            while True:
                await asyncio.sleep(self.lease / 3)
                try:
                    await r.renew(self.lease)
                except Lost:
                    return
                except Exception as e:  # noqa: BLE001 - try again at the next third
                    self.log.warn("renewing a lease failed", job=r.job, item=r.id, error=str(e))

        renewal = asyncio.create_task(renew())
        err: BaseException | None = None
        try:
            await h
        except asyncio.CancelledError:
            if self._stopping:  # P7.8: give the lease and the attempt back, then let the stop go on
                renewal.cancel()
                with contextlib.suppress(Lost, Exception):
                    await asyncio.shield(r.exec("lease_until = now(), attempts = attempts - 1"))
                raise
            if not state["lost"]:
                raise
        except TimeoutError as e:
            err = TimeoutError(f"the run passed its {j.timeout}s deadline") if not r.completed else None
            if err is not None:
                err.__cause__ = e
        except Lost:
            pass
        except Exception as e:  # noqa: BLE001 - every failure is classified, none takes the process down
            err = e
        finally:
            renewal.cancel()
        if state["lost"] or (err is not None and lost(err)):
            return  # nothing more to commit
        try:
            if err is None:
                if not r.completed:
                    await r.complete()
            else:
                await self._fail(r, err)
        except Lost:
            pass
        except Exception as e:  # noqa: BLE001
            self.log.warn("finishing a work item failed", job=r.job, item=r.id, error=str(e))

    async def _fail(self, r: Run, cause: BaseException) -> None:
        j, msg, kind = r._job, str(cause) or type(cause).__name__, kind_of(cause)
        if kind == "wait":
            if j.max_wait > 0:
                waited = await self.db.background_tx(lambda tx: tx.fetchval(
                    "SELECT coalesce(waiting_since < now() - make_interval(secs => $2), false) FROM be_work_item "
                    "WHERE id = $1", r.id, float(j.max_wait)))
                if waited:
                    await self._dead(r, RuntimeError(f"waited longer than {j.max_wait}s: {msg}"))
                    return
            # a wait never spends an attempt, so its backoff doubles with the wait itself: wait again as long as it
            # has waited, from the backoff's start up to its cap
            await r.exec("""state = 'retry_wait', attempts = attempts - 1, holder = NULL, lease_until = NULL,
                last_error = $5, waiting_since = coalesce(waiting_since, now()), run_at = now() + make_interval(
                secs => least($7::float8, greatest($6::float8,
                    extract(epoch FROM now() - coalesce(waiting_since, now()))::float8)))""",
                         (msg, BACKOFF_START, BACKOFF_CAP))
        elif kind == "permanent":
            await self._dead(r, cause)
        elif j.effect == NON_IDEMPOTENT_EXTERNAL and kind != "not_effective" and await self._unknown(r):
            await r.exec("""state = 'paused', pause_reason = 'outcome_unknown', holder = NULL, lease_until = NULL,
                last_error = $5""", (msg,))
            self.log.error("a non-idempotent call failed without proof it did not take effect; paused", job=r.job,
                           item=r.id)
        elif r.attempts >= r.max_attempts:
            await self._dead(r, cause)
        else:
            self.log.warn("a work item failed; it is retried after a backoff", job=r.job, item=r.id,
                          attempt=r.attempts, error=msg)
            await r.exec("""state = 'retry_wait', outcome_unknown = false, holder = NULL, lease_until = NULL,
                last_error = $5, run_at = now() + make_interval(secs => $6)""", (msg, backoff(r.attempts)))

    async def _unknown(self, r: Run) -> bool:
        return bool(await self.db.background_tx(lambda tx: tx.fetchval(
            "SELECT outcome_unknown FROM be_work_item WHERE id = $1", r.id)))

    async def _dead(self, r: Run, cause: BaseException) -> None:
        j = r._job

        async def letter(tx: Tx) -> None:
            if j.dead_letter is not None:
                await j.dead_letter(tx, r, cause)

        await r.exec("""state = 'dead', outcome_unknown = false, holder = NULL, lease_until = NULL, last_error = $5,
            finished_at = now()""", (str(cause) or type(cause).__name__,), letter)
        self.log.error("a work item is dead; an operator decides", job=r.job, item=r.id, error=str(cause))
