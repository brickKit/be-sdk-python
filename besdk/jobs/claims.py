"""The coordination statements of P14: singleton leases with an epoch fencing token, cron slots claimed
with ``INSERT … ON CONFLICT DO NOTHING``. Each runs in its own short transaction of the member's store, so
replicas, a shell member and a ``job run`` coordinate through the same rows of the member's schema."""

from __future__ import annotations

from datetime import datetime
from typing import Any

# take a free or expired lease, or renew our own; epoch +1 on every takeover
_LEASE = ("INSERT INTO besdk_job_lease (name, holder, epoch, expires_at) "
          "VALUES ($1, $2, 1, now() + make_interval(secs => $3)) "
          "ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, "
          "epoch = CASE WHEN besdk_job_lease.holder = EXCLUDED.holder THEN besdk_job_lease.epoch "
          "ELSE besdk_job_lease.epoch + 1 END, expires_at = EXCLUDED.expires_at "
          "WHERE besdk_job_lease.expires_at < now() OR besdk_job_lease.holder = EXCLUDED.holder RETURNING epoch")
_RELEASE = "UPDATE besdk_job_lease SET expires_at = now() WHERE name = $1 AND holder = $2"
_SLOT = ("INSERT INTO besdk_job_slot (name, slot_at, holder) VALUES ($1, $2, $3) "
         "ON CONFLICT (name, slot_at) DO NOTHING RETURNING 1")
_SLOT_DONE = "UPDATE besdk_job_slot SET done_at = now(), result = $3 WHERE name = $1 AND slot_at = $2"


async def lease(store: Any, name: str, holder: str, ttl: float) -> int | None:
    """The epoch when we hold the lease (taken or renewed), None when another holder has it."""
    return await store.tx(lambda tx: tx.fetchval(_LEASE, name, holder, ttl))


async def release(store: Any, name: str, holder: str) -> None:
    await store.tx(lambda tx: tx.execute(_RELEASE, name, holder))


async def claim_slot(store: Any, name: str, slot_at: datetime, holder: str) -> bool:
    return await store.tx(lambda tx: tx.fetchval(_SLOT, name, slot_at, holder)) is not None


async def finish_slot(store: Any, name: str, slot_at: datetime, result: str) -> None:
    await store.tx(lambda tx: tx.execute(_SLOT_DONE, name, slot_at, result[:500]))
