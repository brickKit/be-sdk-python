"""The statistics upkeep of the SDK's tables (chapter 05, P5.33): the SDK's statements keep cached generic plans, and
one settled while a queue table was small runs until the table is next analyzed; autovacuum does that up to a naptime
(60 s by default) after a backlog arrived. Every process that opens the database calls the migration's
be_analyze_stale() every STALE_CHECK_EVERY seconds, which analyzes the tables whose statistics lag."""
from __future__ import annotations

import asyncio

from besdk.db.pool import DB, TxOptions

STALE_CHECK_EVERY = 2.0


async def analyze_stale(db: DB) -> list[str]:
    """Analyzes the SDK's tables whose statistics lag behind their changes, as the schema owner through the SDK's
    function; returns the tables it analyzed."""
    async def run(tx):
        return [r[0] for r in await tx.fetch("SELECT be_analyze_stale()")]

    # an analyze samples a bounded number of rows; a large table may take seconds
    return await db.background_tx(run, TxOptions(statement_timeout=60.0))


async def keep_statistics(db: DB, logger) -> None:
    """Calls analyze_stale every STALE_CHECK_EVERY seconds until cancelled; a failed call is logged and tried again."""
    while True:
        await asyncio.sleep(STALE_CHECK_EVERY)
        try:
            analyzed = await analyze_stale(db)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - logged, tried again at the next tick
            logger.warn("the statistics of the SDK's tables could not be refreshed", error=str(e))
        else:
            if analyzed:
                logger.debug("analyzed tables whose statistics lagged", tables=analyzed)
