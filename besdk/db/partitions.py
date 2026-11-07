"""The declared tables' monthly partitions (P5.21-P5.23): a component declares a partitioned table in its migration with
be_declare_monthly_partitions; the SDK's hourly job creates the missing ones ahead through the SDK's restricted
function be_maintain_partitions()."""
from __future__ import annotations

from besdk.db.pool import Tx
from besdk.obs import Logger

MAINTAIN_EVERY = 3600.0


async def maintain_partitions(tx: Tx) -> list[str]:
    """Creates the missing partitions from the current month through the declared months ahead, inside tx, and returns
    their names. A mismatched, detached or overlapping partition fails it."""
    return [r[0] for r in await tx.fetch("SELECT be_maintain_partitions()")]


def maintain_job(log: Logger):
    """The hourly maintenance every member with a database runs (P5.22): a besdk.work singleton."""
    from besdk.work.job import LOCAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        created: list[str] = []

        async def body(tx: Tx) -> None:
            created.extend(await maintain_partitions(tx))

        await run.complete(body)
        if created:
            log.info("partitions created", event="partitions_created", partitions=created)

    return Job("be.partitions.maintain", SINGLETON, handle, LOCAL, interval=MAINTAIN_EVERY, max_attempts=UNLIMITED)
