"""One claim of a work item (P7.5-P7.12, P7.17): every commit through it carries the claim check, so a holder that was
taken over, or whose database epoch changed, commits nothing and stops."""
from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from besdk import crash
from besdk.db.pool import DB, Tx, rows_affected
from besdk.work.job import SINGLETON, Item, Job, Lost

# The claim check of every commit (P7.6): the epoch and claim epoch this holder got, that claim epoch still current.
GUARD = """id = $1 AND state = 'running' AND epoch = $2 AND claim_db_generation = $3 AND claim_db_timeline = $4
    AND (claim_db_generation, claim_db_timeline) = (SELECT generation, timeline FROM be_current_db_epoch())"""


def lost(err: BaseException) -> bool:
    """Whether a transaction failed because the claim was lost (the transaction wraps what its function raised)."""
    while err is not None:
        if isinstance(err, Lost):
            return True
        err = err.__cause__  # type: ignore[assignment]
    return False


class Run(Item):
    """A claimed work item: what the handler sees, and the guarded commits."""

    def __init__(self, item: Item, job: Job, db: DB, epoch: int, generation: int, timeline: int):
        super().__init__(**item.__dict__)
        self._job, self._db, self._claim = job, db, (epoch, generation, timeline)
        self.completed = False
        self.on_lost: Callable[[], None] | None = None  # the runner cancels the handler when a commit finds it lost

    @property
    def claim(self) -> tuple[int, int, int]:
        """The epoch and the claim's database epoch (generation, timeline)."""
        return self._claim

    async def exec(self, assignments: str, extra: tuple = (), fn: Callable[[Tx], Awaitable[Any]] | None = None,
                   point: str = "work.update") -> None:
        """One guarded update, with fn's writes in the same transaction; raises Lost when no row matched. point names
        the transition for the simulation's crash points (besdk.crash)."""
        async def body(tx: Tx) -> None:
            status = await tx.execute(f"UPDATE be_work_item SET {assignments} WHERE {GUARD}", self.id, *self._claim,
                                      *extra)
            if rows_affected(status) == 0:
                raise Lost(self.id)
            if fn is not None:
                await fn(tx)
            crash.point(point + "/commit")

        try:
            await self._db.background_tx(body)
            crash.point(point + "/done")
        except Exception as e:
            if lost(e):
                if self.on_lost is not None:
                    self.on_lost()
                raise Lost(self.id) from None
            raise

    async def complete(self, fn: Callable[[Tx], Awaitable[Any]] | None = None) -> None:
        """Commits the item's effect and marks it done in one transaction (P7.10); fn may be None. A non-idempotent
        call's unknown outcome is cleared with it. A singleton goes back to pending for its next run."""
        if self.kind == SINGLETON:
            await self.exec("""state = 'pending', outcome_unknown = false, waiting_since = NULL, holder = NULL,
                lease_until = NULL, attempts = 0, progress = NULL, operation_id = $5,
                run_at = now() + make_interval(secs => $6)""", (str(uuid.uuid7()), float(self._job.interval)), fn,
                            "work.complete")
        else:
            await self.exec("""state = 'done', outcome_unknown = false, waiting_since = NULL, holder = NULL,
                lease_until = NULL, finished_at = now()""", (), fn, "work.complete")
        self.completed = True

    async def before_call(self) -> None:
        """Marks a non-idempotent external call as possibly sent (P7.12): await it, and only then make the call."""
        await self.exec("outcome_unknown = true", point="work.before_call")

    def sub_key(self, name: str) -> str:
        """The idempotency key of one call within this run: the operation ID and a key chosen by meaning (P7.11)."""
        return f"{self.operation_id}:{name}"

    async def record_call(self, key: str, result: str) -> None:
        """Stores a sub-call's result (unknown, effective, not_effective) before and after it is made."""
        await self.exec("call_results = call_results || $5::jsonb", (json.dumps({key: result}),), point="work.record_call")
        self.call_results[key] = result

    async def checkpoint(self, progress: Any, fn: Callable[[Tx], Awaitable[Any]] | None = None) -> None:
        """Commits a singleton's progress with fn's writes and starts the next segment's operation ID (P7.17)."""
        nxt = str(uuid.uuid7())
        await self.exec("progress = $5::jsonb, operation_id = $6, call_results = '{}'", (json.dumps(progress), nxt), fn,
                        "work.checkpoint")
        self.progress, self.operation_id, self.call_results = progress, nxt, {}

    async def renew(self, lease: float) -> None:
        """Extends the lease (P7.7)."""
        await self.exec("lease_until = now() + make_interval(secs => $5)", (float(lease),), point="work.renew")


async def shielded(coro: Awaitable[Any]) -> Any:
    """Runs a commit that must finish even while the caller is being cancelled (the stop's lease release)."""
    return await asyncio.shield(coro)
