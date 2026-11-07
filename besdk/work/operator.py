"""What an operator does to dead and paused items (P7.14; protocol 01's transition table) and the cleanup of finished
items (P7.19)."""
from __future__ import annotations

from besdk.db.batch import delete_in_batches
from besdk.db.pool import DB, rows_affected
from besdk.work.job import LOCAL, SINGLETON, UNLIMITED, Job

QUEUE_RETENTION = 7 * 24 * 3600.0
CRON_RETENTION = 30 * 24 * 3600.0
CLEANUP_EVERY = 3600.0


class Refused(Exception):
    """The item's state does not allow this action."""


async def _operate(db: DB, sql: str, *args) -> None:
    status = await db.background_tx(lambda tx: tx.execute(sql, *args))
    if rows_affected(status) == 0:
        raise Refused("work: the item's state does not allow this action")


async def requeue(db: DB, iid: str, verified_not_effective: bool = False) -> None:
    """Puts a dead or paused item back to pending. A paused item with an unknown outcome needs the operator's record
    that the call did not take effect; a dead item never has one."""
    await _operate(db, """UPDATE be_work_item SET state = 'pending', pause_reason = NULL, attempts = 0, run_at = now(),
        outcome_unknown = false, holder = NULL, lease_until = NULL
        WHERE id = $1 AND (
            (state = 'dead' AND NOT outcome_unknown) OR
            (state = 'paused' AND pause_reason IN ('backlog_exceeded', 'business_hold')) OR
            (state = 'paused' AND pause_reason = 'outcome_unknown' AND $2))""", iid, verified_not_effective)


async def resolve(db: DB, iid: str, reason: str) -> None:
    """Marks a dead or paused item resolved with the operator's reason; it never runs again."""
    await _operate(db, """UPDATE be_work_item SET state = 'resolved', pause_reason = NULL, result = to_jsonb($2::text),
        finished_at = now() WHERE id = $1 AND state IN ('dead', 'paused')""", iid, reason)


async def hold(db: DB, iid: str) -> None:
    """Pauses a pending or waiting item for a business check (business_hold)."""
    await _operate(db, """UPDATE be_work_item SET state = 'paused', pause_reason = 'business_hold'
        WHERE id = $1 AND state IN ('pending', 'retry_wait')""", iid)


async def purge(db: DB) -> int:
    """Deletes finished items past their retention, in batches; dead and paused items are never deleted. Cron items
    go only when their slot is behind the watermark, so the slot is never materialized again."""
    return await delete_in_batches(db, """DELETE FROM be_work_item WHERE id IN (SELECT id FROM be_work_item w
        WHERE state IN ('done', 'resolved') AND (
            (kind = 'queue' AND finished_at < now() - make_interval(secs => $1)) OR
            (kind = 'cron' AND finished_at < now() - make_interval(secs => $2)
                AND run_at <= (SELECT watermark_at FROM be_cron_watermark m WHERE m.job = w.job)))
        LIMIT $3 FOR UPDATE SKIP LOCKED)""", QUEUE_RETENTION, CRON_RETENTION)


def cleanup_job(db: DB) -> Job:
    """The hourly cleanup every member with a database runs (P7.19)."""
    async def handle(run) -> None:
        await purge(db)
        await run.complete()

    return Job("be.work.cleanup", SINGLETON, handle, LOCAL, interval=CLEANUP_EVERY, max_attempts=UNLIMITED)
