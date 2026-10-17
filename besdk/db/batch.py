"""Deletes in batches, each in its own background transaction, so a cleanup never holds many row locks or outlasts
the statement timeout however large the backlog."""
from __future__ import annotations

from typing import Any

from besdk.db.pool import DB, rows_affected

BATCH = 1000


async def delete_in_batches(db: DB, sql: str, *args: Any) -> int:
    """Run a delete whose last parameter is the batch size until a batch deletes fewer rows; returns the total."""
    total = 0
    while True:
        n = rows_affected(await db.background_tx(lambda tx: tx.execute(sql, *args, BATCH)))
        total += n
        if n < BATCH:
            return total
