"""``be.cleanup`` (P13.7, P14.7, P16 platform retention): deletes expired idempotency keys, ``done`` queue rows
after 7 days, slot rows after 30 days and event cursor rows not seen for 30 days. ``dead`` queue rows stay
for the operator. Deletes are batched so one run never holds long locks."""

from __future__ import annotations

from typing import Any

BATCH = 5000
_STEPS = (
    "DELETE FROM besdk_idempotency WHERE ctid IN (SELECT ctid FROM besdk_idempotency WHERE expires_at <= now() "
    "LIMIT $1)",
    "DELETE FROM besdk_job_queue WHERE ctid IN (SELECT ctid FROM besdk_job_queue WHERE state = 'done' "
    "AND finished_at < now() - interval '7 days' LIMIT $1)",
    "DELETE FROM besdk_job_slot WHERE ctid IN (SELECT ctid FROM besdk_job_slot WHERE slot_at < now() - interval '30 days' "
    "LIMIT $1)",
    "DELETE FROM besdk_event_cursor WHERE ctid IN (SELECT ctid FROM besdk_event_cursor "
    "WHERE seen_at < now() - interval '30 days' LIMIT $1)",
)


async def cleanup(store: Any) -> None:
    for sql in _STEPS:
        while True:
            status = await store.tx(lambda tx, sql=sql: tx.execute(sql, BATCH))
            if int(status.rsplit(" ", 1)[-1]) < BATCH:
                break
