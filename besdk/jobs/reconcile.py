"""Reconcilers (be-protocol P14, kind ``reconciler``): candidates from the component's own SQL, each item
claimed with a lease in ``besdk_reconcile`` (due and not leased), handled outside any transaction, the
outcome applied in a short transaction. A failure backs off; past ``max_attempts`` the reconciler gives
up (``give_up`` suspends the item and opens an exception task). Applying or giving up deletes the row."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable

from besdk.jobs.model import Reconciler

LEASE_GRACE = 30.0

_CLAIM = ("INSERT INTO besdk_reconcile (name, item_id, lease_until) VALUES ($1, $2, now() + make_interval(secs => $3)) "
          "ON CONFLICT (name, item_id) DO UPDATE SET lease_until = now() + make_interval(secs => $3) "
          "WHERE (besdk_reconcile.lease_until IS NULL OR besdk_reconcile.lease_until < now()) "
          "AND besdk_reconcile.next_at <= now() RETURNING attempts")
_FAIL = ("UPDATE besdk_reconcile SET attempts = attempts + 1, lease_until = NULL, last_error = $3, "
         "next_at = now() + make_interval(secs => $4) WHERE name = $1 AND item_id = $2")
_DELETE = "DELETE FROM besdk_reconcile WHERE name = $1 AND item_id = $2"
_AGE = ("SELECT count(*) AS n, coalesce(extract(epoch FROM now() - min(next_at)), 0) AS age "
        "FROM besdk_reconcile WHERE name = $1 AND attempts > 0")


class ReconcilerLoop:
    def __init__(self, rt: Any, rec: Reconciler, execute: Callable[..., Awaitable[Any]]):
        self.rt, self.r, self.execute = rt, rec, execute
        self.store = rt.store()

    async def run(self) -> None:
        while True:
            await self.round()
            await asyncio.sleep(self.r.every)

    async def round(self) -> int:
        r = self.r
        items = await self.store.tx(lambda tx: r.candidates(tx, r.batch))
        self.rt.metrics.reconcile_pending.labels(name=r.name).set(len(items))
        handled = 0
        for item in items:
            handled += await self._item(item)
        n, age = await self.store.tx(lambda tx: tx.fetchrow(_AGE, r.name))
        self.rt.metrics.reconcile_oldest_age.labels(name=r.name).set(float(age) if n else 0.0)
        return handled

    async def _item(self, item: Any) -> int:
        r, iid = self.r, self.r.id(item)
        attempts = await self.store.tx(lambda tx: tx.fetchval(_CLAIM, r.name, iid, r.timeout + LEASE_GRACE))
        if attempts is None:
            return 0
        try:
            outcome = await self.execute(r.name, lambda: r.handle(item), r.timeout)

            async def apply(tx: Any) -> None:
                await r.apply(tx, item, outcome)
                await tx.execute(_DELETE, r.name, iid)

            await self.store.tx(apply)
        except Exception as e:  # noqa: BLE001 - recorded, backed off, eventually given up
            await self._failed(item, iid, attempts + 1, f"{type(e).__name__}: {e}"[:500])
        return 1

    async def _failed(self, item: Any, iid: str, attempts: int, text: str) -> None:
        r = self.r
        if attempts < r.max_attempts:
            delay = r.backoff[min(attempts - 1, len(r.backoff) - 1)] if r.backoff else 10.0
            await self.store.tx(lambda tx: tx.execute(_FAIL, r.name, iid, text, float(delay)))
            return

        async def give_up(tx: Any) -> None:
            if r.give_up is not None:
                await r.give_up(tx, item)
            await tx.execute(_DELETE, r.name, iid)

        await self.store.tx(give_up)
        self.rt.metrics.reconcile_giveups.labels(name=r.name).inc()
        self.rt.logger.error("reconcile_gave_up", extra={"reconciler": r.name, "item": iid, "error": text})
