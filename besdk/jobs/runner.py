"""The member's background work (be-protocol P14): the plan of jobs, one supervised loop per enabled job,
worker kind and reconciler, and ``run_once`` for ``job run <name>`` (P14.8).

Every run happens in ``execute``: the job name in the context, the declared timeout (cancelled at it), and
the metrics ``be_job_runs_total{job,result}``, ``be_job_duration_seconds{job}``,
``be_job_last_success_timestamp_seconds{job}`` (P14.3). The holder of leases and slots is
``<member ID>/<instance id>``, or ``<member ID>/job-run:<instance id>`` for a one-shot run.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from zoneinfo import ZoneInfo

from besdk import context
from besdk.jobs import claims
from besdk.jobs.model import (AUTHZ_CHANGES, CLEANUP, LIFECYCLE, OUTBOX, JobKind, JobsConfigError, Planned,
                              plan)
from besdk.jobs.queue import WorkerLoop
from besdk.jobs.reconcile import ReconcilerLoop

if TYPE_CHECKING:
    from besdk.runtime import Module, Runtime

LEASE_TTL = 30.0
UTC = timezone.utc


def _now() -> datetime:
    return datetime.now(UTC)


class JobsRuntime:
    def __init__(self, rt: "Runtime", module: "Module", *, lease_ttl: float = LEASE_TTL,
                 holder_suffix: str | None = None):
        self.rt, self.module, self.ttl = rt, module, lease_ttl
        self.holder = f"{rt.id}/{holder_suffix or rt.instance}"
        self.db = "PG_SCHEMA" in rt.config.declared()
        self.bound: dict[str, Callable[[], Awaitable[None]]] = {}
        self.workers = {w.kind: w for w in module.workers}
        self.reconcilers = {r.name: r for r in module.reconcilers}
        self.status: dict[str, dict[str, Any]] = {}
        zone = ZoneInfo(rt.config.string("BUSINESS_TIMEZONE", "Asia/Shanghai")
                        if "BUSINESS_TIMEZONE" in rt.config.declared() else "Asia/Shanghai")
        overrides = rt.config.json("JOBS_OVERRIDES") if "JOBS_OVERRIDES" in rt.config.declared() else None
        self.plan: dict[str, Planned] = plan(module.jobs, overrides, zone, rt.logger, runtime=self._runtime_jobs())
        self._check_storage()
        self._held: set[str] = set()
        if self.db:
            from besdk.jobs.cleanup import cleanup

            rt.store().jobs = self  # tx.enqueue finds the worker kinds here
            self.bind("be.cleanup", lambda: cleanup(rt.store()))

    def _runtime_jobs(self) -> tuple:
        out = [OUTBOX]
        if self.db:
            out += [CLEANUP, LIFECYCLE, AUTHZ_CHANGES]
        return tuple(out)

    def _check_storage(self) -> None:
        if self.db:
            return
        needs_db = [p.job.name for p in self.plan.values() if p.job.kind is not JobKind.EVERY
                    and not p.job.name.startswith("be.")]
        if needs_db or self.workers or self.reconcilers:
            raise JobsConfigError("singleton, cron, queue and reconciler work needs a database (PG_SCHEMA)")

    def bind(self, name: str, fn: Callable[[], Awaitable[None]]) -> None:
        """Give a runtime-owned job its work (be.cleanup, be.lifecycle, be.authz.changes)."""
        self.bound[name] = fn

    def interval(self, name: str, default: float) -> float:
        p = self.plan.get(name)
        return p.interval if p and p.interval else default

    def enabled(self, name: str) -> bool:
        p = self.plan.get(name)
        return p is None or p.enabled

    def _work(self, p: Planned) -> Callable[[], Awaitable[None]] | None:
        return self.bound.get(p.job.name) if p.job.name.startswith("be.") else p.job.run

    # --- one run -----------------------------------------------------------------------------------

    async def execute(self, name: str, fn: Callable[[], Awaitable[Any]], timeout: float, *, epoch: int = 0) -> Any:
        m, start = self.rt.metrics, time.perf_counter()
        st = self.status.setdefault(name, {"last_success": None, "last_error": ""})
        with context.scope(job=name, job_epoch=epoch, member=self.rt.id, request_id=""):
            try:
                async with asyncio.timeout(timeout):
                    out = await fn()
            except asyncio.CancelledError:
                m.job_runs.labels(job=name, result="cancelled").inc()
                raise
            except Exception as e:
                m.job_runs.labels(job=name, result="error").inc()
                st["last_error"] = f"{type(e).__name__}: {e}"[:300]
                self.rt.logger.error("job_failed", extra={"job": name, "error": st["last_error"]})
                raise
            finally:
                m.job_duration.labels(job=name).observe(time.perf_counter() - start)
        m.job_runs.labels(job=name, result="ok").inc()
        m.job_last_success.labels(job=name).set(time.time())
        st["last_success"] = _now().isoformat()
        return out

    # --- in-process scheduling ---------------------------------------------------------------------

    async def start(self) -> None:
        sup = self.rt.supervisor
        for p in self.plan.values():
            if not p.enabled or p.job.name == "be.outbox" or self._work(p) is None:
                continue
            loop = {JobKind.EVERY: self._every, JobKind.SINGLETON: self._singleton, JobKind.CRON: self._cron}[p.job.kind]
            sup.start(f"job:{p.job.name}", lambda p=p, loop=loop: loop(p))
        for w in self.workers.values():
            if self.enabled(w.kind):
                sup.start(f"worker:{w.kind}", WorkerLoop(self.rt, w, self.execute).run)
        for r in self.reconcilers.values():
            if self.enabled(r.name):
                sup.start(f"reconciler:{r.name}", ReconcilerLoop(self.rt, r, self.execute).run)

    async def stop(self) -> None:
        """Release held leases so another replica takes over at once (P1.6)."""
        for name in list(self._held):
            try:
                await claims.release(self.rt.store(), name, self.holder)
            except Exception:  # noqa: BLE001 - the TTL frees it anyway
                pass
        self._held.clear()

    async def _every(self, p: Planned) -> None:
        while True:
            await asyncio.sleep(p.interval)
            try:
                await self.execute(p.job.name, self._work(p), p.job.timeout)
            except Exception:  # noqa: BLE001 - logged and counted; the next tick runs again
                pass

    async def _cron(self, p: Planned) -> None:
        await self._slot(p, p.schedule.prev_at_or_before(_now()))  # only the most recent missed slot
        while True:
            nxt = p.schedule.next_after(_now())
            while (left := (nxt - _now()).total_seconds()) > 0:
                await asyncio.sleep(min(left, 30.0))
            await self._slot(p, nxt)

    async def _slot(self, p: Planned, slot: datetime, holder: str | None = None) -> bool:
        store = self.rt.store()
        if not await claims.claim_slot(store, p.job.name, slot, holder or self.holder):
            return False
        result = "ok"
        try:
            await self.execute(p.job.name, self._work(p), p.job.timeout)
        except Exception as e:  # noqa: BLE001
            result = f"error: {type(e).__name__}: {e}"
            raise
        finally:
            await claims.finish_slot(store, p.job.name, slot, result)
        return True

    async def _singleton(self, p: Planned) -> None:
        store, name = self.rt.store(), p.job.name
        while True:
            epoch = await claims.lease(store, name, self.holder, self.ttl)
            if epoch is None:
                await asyncio.sleep(self.ttl / 3)
                continue
            self._held.add(name)
            await self._hold(p, epoch)
            self._held.discard(name)

    async def _hold(self, p: Planned, epoch: int) -> None:
        """Run on the interval while the lease is renewed every TTL/3; a lost lease cancels the run."""
        lost = asyncio.Event()
        keeper = asyncio.create_task(self._keep(p.job.name, lost))
        try:
            while not lost.is_set():
                run = asyncio.create_task(self.execute(p.job.name, self._work(p), p.job.timeout, epoch=epoch))
                waiter = asyncio.create_task(lost.wait())
                await asyncio.wait({run, waiter}, return_when=asyncio.FIRST_COMPLETED)
                waiter.cancel()
                if lost.is_set():
                    run.cancel()
                    self.rt.logger.warning("job_lease_lost", extra={"job": p.job.name})
                    await asyncio.gather(run, return_exceptions=True)
                    return
                if not run.cancelled():
                    run.exception()  # a failure is logged and counted by execute; mark it retrieved
                try:
                    await asyncio.wait_for(lost.wait(), p.interval)
                except TimeoutError:
                    pass
        finally:
            keeper.cancel()

    async def _keep(self, name: str, lost: asyncio.Event) -> None:
        while True:
            await asyncio.sleep(self.ttl / 3)
            try:
                held = await claims.lease(self.rt.store(), name, self.holder, self.ttl)
            except Exception:  # noqa: BLE001 - cannot prove we hold it: stop (fencing)
                held = None
            if held is None:
                lost.set()
                return

    # --- job run <name> (P14.8) --------------------------------------------------------------------

    def names(self) -> set[str]:
        return set(self.plan) | set(self.workers) | set(self.reconcilers)

    async def run_once(self, name: str) -> str:
        """One run of ``name``; "ok" or "noop: <reason>"; a failed run raises; unknown name: KeyError."""
        if name in self.plan and self._work(self.plan[name]) is not None:
            return await self._once(self.plan[name])
        if name in self.reconcilers:
            n = await ReconcilerLoop(self.rt, self.reconcilers[name], self.execute).round()
            return "ok" if n else "noop: no candidates"
        if name in self.workers:
            loop, n = WorkerLoop(self.rt, self.workers[name], self.execute), 0
            while (k := await loop.round()) > 0:
                n += k
            return "ok" if n else "noop: no ready jobs"
        raise KeyError(name)

    async def _once(self, p: Planned) -> str:
        if p.job.kind is JobKind.CRON:
            ok = await self._slot(p, p.schedule.prev_at_or_before(_now()))
            return "ok" if ok else "noop: the slot is already taken"
        if p.job.kind is JobKind.SINGLETON:
            store = self.rt.store()
            epoch = await claims.lease(store, p.job.name, self.holder, self.ttl)
            if epoch is None:
                return "noop: the lease is held elsewhere"
            try:
                await self.execute(p.job.name, self._work(p), p.job.timeout, epoch=epoch)
            finally:
                await claims.release(store, p.job.name, self.holder)
            return "ok"
        await self.execute(p.job.name, self._work(p), p.job.timeout)
        return "ok"
