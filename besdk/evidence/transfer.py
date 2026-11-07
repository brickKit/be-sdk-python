"""be.evidence.transfer (P12.15): the evidence outbox's rows go to the evidence store (the audit component's
EvidenceService) in order, a batch at a time; accepted and duplicate rows are deleted (the store never goes back,
P12.14), conflict and invalid rows are stopped and logged, never sent again; a failed call leaves the rest for the
next run. It runs as the SDK's singleton job in normal running and as the host's loop in the recovery phases but
frozen (besdk/sdkjobs.py): a conclusion written during a recovery reaches the store before the replay."""
from __future__ import annotations

import asyncio
import time

from be.audit.v3 import audit_pb2, audit_pb2_grpc

from besdk import reqctx
from besdk.calls.client import options
from besdk.db.pool import DB, Tx
from besdk.evidence.writer import Sealed, Store
from besdk.obs import Logger

BATCH = 500  # the most records one Append carries (P12.12)
TRANSFER_EVERY = 2.0  # seconds between runs
CALL_LIMIT = 30.0  # one run's deadline: calls need one (P4.4)
_STATUS = {audit_pb2.EVIDENCE_STATUS_ACCEPTED: "accepted", audit_pb2.EVIDENCE_STATUS_DUPLICATE: "duplicate",
           audit_pb2.EVIDENCE_STATUS_CONFLICT: "conflict"}


class GRPCStore:
    """The audit component's EvidenceService over the system surface (mTLS, the call policy)."""

    def __init__(self, channel):
        self._stub = audit_pb2_grpc.EvidenceServiceStub(channel)

    async def append(self, recs: list[Sealed]) -> list[str]:
        req = audit_pb2.AppendRequest(records=[audit_pb2.Evidence(record=r.record, digest=r.digest) for r in recs])
        out = await self._stub.Append(req, metadata=options(idempotent=True))
        return [_STATUS.get(r.status, "invalid") for r in out.receipts]


async def transfer(db: DB, store: Store, log: Logger) -> int:
    """Sends the rows not stopped until none is left; returns how many rows were settled. A failed call raises with
    the rest left for the next run."""
    total = 0
    while True:
        rows = await db.background_tx(lambda tx: tx.fetch("""SELECT ord, id, phase, record, digest
            FROM be_evidence_outbox WHERE stopped IS NULL ORDER BY ord LIMIT $1""", BATCH))
        if not rows:
            return total
        st = await store.append([Sealed(r["id"], r["phase"], bytes(r["record"]), r["digest"]) for r in rows])

        async def settle(tx: Tx) -> None:
            for r, s in zip(rows, st):
                if s in ("accepted", "duplicate"):
                    await tx.execute("DELETE FROM be_evidence_outbox WHERE ord = $1", r["ord"])
                else:
                    log.error("the evidence store refused a record: it is kept here and not sent again",
                              event="evidence_refused", id=r["id"], phase=r["phase"], status=s)
                    await tx.execute("UPDATE be_evidence_outbox SET stopped = $2 WHERE ord = $1", r["ord"], s)

        await db.background_tx(settle)
        total += len(rows)
        if len(rows) < BATCH:
            return total


async def transfer_once(db: DB, store: Store | None, log: Logger) -> None:
    """One run of the transfer with its deadline; without a store the records stay local."""
    if store is None:
        return
    try:
        with reqctx.deadline_scope(time.monotonic() + CALL_LIMIT):  # calls need one (P4.4)
            await asyncio.wait_for(transfer(db, store, log), CALL_LIMIT)
    except Exception as e:  # noqa: BLE001 - it stays local and the next run sends it
        log.warn("evidence could not be transferred; it stays local", error=repr(e))


def transfer_job(db: DB, store: Store | None, log: Logger):
    """be.evidence.transfer, the SDK's singleton job (P12.15)."""
    from besdk.work.job import IDEMPOTENT_EXTERNAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        await transfer_once(db, store, log)
        await run.complete()

    return Job("be.evidence.transfer", SINGLETON, handle, IDEMPOTENT_EXTERNAL, interval=TRANSFER_EVERY, max_attempts=UNLIMITED,
               timeout=CALL_LIMIT + 10)


async def loop(db: DB, store: Store, log: Logger) -> None:
    """The transfer as the host's loop in the recovery phases but frozen (besdk/sdkjobs.py, P1.23)."""
    while True:
        await transfer_once(db, store, log)
        await asyncio.sleep(TRANSFER_EVERY)
