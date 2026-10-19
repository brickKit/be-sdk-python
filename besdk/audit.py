"""The component side of reliable audit (protocol P8.19-P8.23; assembly project protocol 03): the local append-only
audit table written with the business change, sensitive reads recorded before the data is released, denials within a
per-minute budget, numbering after commit, transfer to the audit component with receipts, and deletion only once a
receipt is covered by the audit component's backup. The same tables and algorithm as the Go SDK."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from besdk import canon, errs, records, reqctx
from besdk.db.pool import DB, Tx, TxOptions, rows_affected
from besdk.obs import Logger

# Categories (protocol 03, "what needs auditing").
BUSINESS_CHANGE, SECURITY_CHANGE, SENSITIVE_READ = "business_change", "security_change", "sensitive_read"
DELEGATED, SECURITY_DENIAL, WORK_DISPOSITION = "delegated", "security_denial", "work_disposition"
# Outcomes.
SUCCEEDED, DENIED, FAILED = "succeeded", "denied", "failed"
# Parameters of protocol 03.
DENIAL_BUDGET = 20  # details per principal, reason and minute
DEFAULT_CAPACITY = 100_000
MIN_RETENTION = 7 * 24 * 3600.0
BATCH = 500
TRANSFER_EVERY = 5.0
SEND_TIMEOUT = 30.0
SUBMIT_METHOD, WATERMARK_METHOD = "/be.audit.v3.IntakeService/Submit", "/be.audit.v3.IntakeService/Watermark"
_RECEIPT = {1: "received", 2: "conflict", 3: "invalid"}  # be.audit.v3.ReceiptStatus


@dataclass
class Record:
    """One audit record. Restricted values go in as digests or masks, never in clear."""

    category: str
    operation: str
    outcome: str
    reason: str = ""
    actor: str = ""  # the acting principal
    represented: str = ""  # the principal acted for, when delegated
    target: dict | None = None
    changes: dict | None = None
    request: dict | None = None  # request and trace IDs, call mode


def _canon_map(m: dict) -> dict:
    """Only the encoder's types: JSON strings, integers, booleans, objects and arrays. An integer (a version, a count)
    is kept and encoded; a fraction comes back as a float and is refused (decimals travel as strings, protocol 14);
    null members are left out; as in the Go SDK. Other Python types are a module error."""
    try:
        return _drop_nulls(json.loads(json.dumps(m)))
    except TypeError as e:
        raise TypeError(f"audit record values must be JSON strings, booleans, objects or arrays: {e}") from e


def _drop_nulls(v: Any) -> Any:
    """The null members of every object left out: the canonical encoding has no null (security 09a), and an absent
    member says the same as a null one."""
    if isinstance(v, dict):
        return {k: _drop_nulls(e) for k, e in v.items() if e is not None}
    if isinstance(v, list):
        return [_drop_nulls(e) for e in v]
    return v


def _body(r: Record, rid: str, at: datetime) -> dict:
    b: dict[str, Any] = {"id": rid, "occurred_at": canon.time(at), "category": r.category, "operation": r.operation,
                         "outcome": r.outcome}
    for k in ("reason", "actor", "represented"):
        if v := getattr(r, k):
            b[k] = v
    for k in ("target", "changes", "request"):
        if (v := getattr(r, k)) is not None:
            b[k] = _canon_map(v)
    return b


def _json(v: Any) -> str | None:
    return None if v is None else json.dumps(v, separators=(",", ":"))


class Sender(Protocol):
    """Delivers records to the audit component, idempotent by ID."""

    async def send(self, records: list[dict]) -> list[dict]: ...

    async def watermark(self) -> dict: ...


class GRPCSender:
    """Sends to the audit component over its system surface (mTLS, the call policy; infra-audit's contract). A record
    travels as the canonical bytes stored with its row (P8.21)."""

    def __init__(self, channel: Any):
        from be.audit.v3 import audit_pb2_grpc

        self._stub = audit_pb2_grpc.IntakeServiceStub(channel)

    async def send(self, records: list[dict]) -> list[dict]:
        from be.audit.v3 import audit_pb2

        from besdk.calls.client import options

        out = await self._stub.Submit(audit_pb2.SubmitRequest(items=[
            audit_pb2.Item(id=r["id"], seq=r["seq"], epoch=r["epoch"], digest=r["digest"], record=r["record"])
            for r in records]), metadata=options(idempotent=True))
        return [{"id": r.id, "status": _RECEIPT.get(r.status, "conflict"), "epoch": r.epoch, "seq": r.seq,
                 "chain": r.chain} for r in out.receipts]

    async def watermark(self) -> dict:
        from be.audit.v3 import audit_pb2

        from besdk.calls.client import options

        out = await self._stub.Watermark(audit_pb2.WatermarkRequest(), metadata=options(idempotent=True))
        return {"epoch": out.epoch, "seq": out.seq, "ancestors": {a.epoch: a.fork for a in out.ancestors},
                "backed_up_in_epoch": out.backed_up_in_epoch}


@dataclass
class Auditor:
    """Writes a member's audit records."""

    db: DB
    capacity: int = DEFAULT_CAPACITY  # the backlog above which required audited operations are refused
    backlog_seen: int = field(default=0)

    def _capacity_check(self) -> None:
        if self.capacity > 0 and self.backlog_seen >= self.capacity:
            raise errs.new("AUDIT_CAPACITY_EXHAUSTED", "audited operations are paused until the audit backlog drains")

    async def _insert(self, tx: Tx, r: Record) -> str:
        rid, at = str(uuid.uuid7()), datetime.now(timezone.utc)
        body = _body(r, rid, at)
        raw, digest = records.seal("be.audit.record", body)
        records.validate("be.audit.record", body)  # a bad record is refused here, not at transfer
        await tx.execute(
            "INSERT INTO be_audit_log (id, occurred_at, category, operation, outcome, reason, actor, represented, "
            "target, changes, request, digest, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            uuid.UUID(rid), at, r.category, r.operation, r.outcome, r.reason or None,
            _json({"id": r.actor} if r.actor else None), _json({"id": r.represented} if r.represented else None),
            _json(r.target), _json(r.changes), _json(r.request), digest, raw)
        return rid

    async def write(self, tx: Tx, r: Record) -> str:
        """Records a business or security change inside its transaction (P8.19): if the transaction fails, neither
        happened. Over capacity it refuses, so the change does not happen either."""
        self._capacity_check()
        return await self._insert(tx, r)

    async def read(self, r: Record) -> None:
        """Records a sensitive read or export in its own short transaction; call it before releasing the data and
        refuse the read when it fails (P8.19)."""
        self._capacity_check()
        r.category = SENSITIVE_READ
        await self.db.tx(lambda tx: self._insert(tx, r))

    async def denial(self, r: Record) -> Exception | None:
        """Records a security denial within the per-minute budget; past it the window only counts, and flush later
        appends one summary row with the count (P8.23). Its failure never changes the denial: it is returned (for the
        module to log), never raised."""
        r.category, r.outcome = SECURITY_DENIAL, DENIED
        now = datetime.now(timezone.utc)
        minute = now.replace(second=0, microsecond=0)

        async def fn(tx: Tx) -> None:
            detailed = await tx.fetchval(
                "INSERT INTO be_audit_denial_budget (principal, reason, minute, detailed) VALUES ($1, $2, $3, 1) "
                "ON CONFLICT (principal, reason, minute) DO UPDATE SET detailed = be_audit_denial_budget.detailed + 1 "
                "RETURNING detailed", r.actor, r.reason, minute)
            if detailed <= DENIAL_BUDGET:
                await self._insert(tx, r)
                return
            await tx.execute("UPDATE be_audit_denial_budget SET overflow = overflow + 1, first_at = coalesce(first_at, $4), "
                             "last_at = $4 WHERE principal = $1 AND reason = $2 AND minute = $3", r.actor, r.reason,
                             minute, now)

        try:
            await self.db.background_tx(fn)
            return None
        except Exception as e:  # noqa: BLE001 - the denial stands whatever happened to its record
            return e

    async def flush(self) -> int:
        """Appends one summary row for every finished window whose denials went past the budget."""
        async def fn(tx: Tx) -> int:
            rows = await tx.fetch(
                "SELECT principal, reason, minute, overflow, first_at, last_at FROM be_audit_denial_budget "
                "WHERE overflow > 0 AND NOT summarized AND minute < date_trunc('minute', now()) FOR UPDATE SKIP LOCKED")
            for w in rows:
                await self._insert(tx, Record(SECURITY_DENIAL, "be.audit.denials_over_budget", DENIED, w["reason"],
                                              w["principal"], changes={
                                                  "count": str(w["overflow"]), "first": canon.time(w["first_at"]),
                                                  "last": canon.time(w["last_at"]), "window": canon.time(w["minute"])}))
                await tx.execute("UPDATE be_audit_denial_budget SET summarized = true WHERE principal = $1 AND "
                                 "reason = $2 AND minute = $3", w["principal"], w["reason"], w["minute"])
            return len(rows)

        return await self.db.background_tx(fn)

    async def number(self) -> int:
        """Gives committed rows their production sequence under the counter's lock, in insertion order, with the
        database epoch (P8.20; the same algorithm as the outbox, P6.5)."""
        async def fn(tx: Tx) -> int:
            c = await tx.fetchrow("SELECT epoch_generation, epoch_timeline, last_seq FROM be_audit_counter FOR UPDATE")
            try:
                e = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
            except Exception as err:  # noqa: BLE001 - the epoch is required for numbering
                raise RuntimeError("audit: the database epoch cannot be read; numbering refused") from err
            g, t = e["generation"], e["timeline"]
            last = c["last_seq"] if (c["epoch_generation"], c["epoch_timeline"]) == (g, t) else 0
            status = await tx.execute(
                "WITH picked AS (SELECT ord, row_number() OVER (ORDER BY ord) AS rn FROM ("
                "SELECT ord FROM be_audit_log WHERE seq IS NULL ORDER BY ord LIMIT $1) p) "
                "UPDATE be_audit_log l SET seq = $2 + picked.rn, epoch_generation = $3, epoch_timeline = $4 "
                "FROM picked WHERE l.ord = picked.ord", BATCH, last, g, t)
            n = rows_affected(status)
            await tx.execute("UPDATE be_audit_counter SET last_seq = $1, epoch_generation = $2, epoch_timeline = $3",
                             last + n, g, t)
            return n

        return await self.db.background_tx(fn, TxOptions(lock_timeout=2.0, transaction_timeout=5.0))

    async def transfer(self, s: Sender) -> int:
        """Sends one batch of numbered pending rows and stores the receipts (P8.21); returns how many were settled.
        A conflict stops that record only; nothing is retried automatically after a conflict."""
        rows = await self.db.background_tx(lambda tx: tx.fetch(
            "SELECT id::text, seq, epoch_generation, epoch_timeline, digest, record FROM be_audit_log "
            "WHERE transfer_state = 'pending' AND seq IS NOT NULL AND record IS NOT NULL ORDER BY ord LIMIT $1", BATCH))
        if not rows:
            return 0
        batch = [{"id": r["id"], "seq": r["seq"], "epoch": f"{r['epoch_generation']}.{r['epoch_timeline']}",
                  "digest": r["digest"], "record": bytes(r["record"])} for r in rows]
        receipts = await s.send(batch)  # on failure they stay pending and are sent again (idempotent by ID)

        async def store(tx: Tx) -> None:
            for rc in receipts:
                await tx.execute(
                    "UPDATE be_audit_log SET transfer_state = $2, receipt_epoch = $3, receipt_seq = $4, "
                    "receipt_chain = $5, received_at = now() WHERE id = $1 AND transfer_state = 'pending'",
                    # conflict, invalid or a status this SDK does not know: never taken as received (P8.21)
                    uuid.UUID(rc["id"]), "received" if rc.get("status") == "received" else "conflict",
                    rc.get("epoch"), rc.get("seq"), rc.get("chain"))

        await self.db.background_tx(store)
        return len(receipts)

    async def backlog(self) -> int:
        """Counts the rows not yet received and refreshes the capacity check (P8.23)."""
        n = await self.db.background_tx(lambda tx: tx.fetchval(
            "SELECT count(*) FROM be_audit_log WHERE transfer_state = 'pending'"))
        self.backlog_seen = n
        return n

    async def purge(self, w: dict) -> tuple[int, int]:
        """Deletes received rows whose receipts the watermark covers and that are older than the minimum retention
        (P8.22). Receipts on a lost branch or past an ancestor's fork point go back to pending to be sent again.
        w: {"epoch", "seq", "ancestors": {epoch: fork}, "backed_up_in_epoch"} as GRPCSender.watermark gives it."""
        epoch, seq, ancestors = w.get("epoch", ""), int(w.get("seq", 0)), w.get("ancestors") or {}
        delete = ("DELETE FROM be_audit_log WHERE transfer_state = 'received' AND received_at < now() - "
                  "make_interval(secs => $3) AND receipt_epoch = $1 AND receipt_seq <= $2")
        resend = ("UPDATE be_audit_log SET transfer_state = 'pending', receipt_epoch = NULL, receipt_seq = NULL, "
                  "receipt_chain = NULL, received_at = NULL WHERE transfer_state = 'received' ")

        async def fn(tx: Tx) -> tuple[int, int]:
            deleted = rows_affected(await tx.execute(delete, epoch, seq, MIN_RETENTION))
            resent = 0
            for anc, fork in ancestors.items():
                if w.get("backed_up_in_epoch"):
                    deleted += rows_affected(await tx.execute(delete, anc, int(fork), MIN_RETENTION))
                resent += rows_affected(await tx.execute(resend + "AND receipt_epoch = $1 AND receipt_seq > $2",
                                                         anc, int(fork)))
            # receipts of an epoch that is neither current nor an ancestor are on a lost branch: send them again
            resent += rows_affected(await tx.execute(
                resend + "AND NOT (receipt_epoch = ANY($1)) AND receipt_epoch IS NOT NULL AND NOT ("
                "(split_part(receipt_epoch, '.', 1)::bigint, split_part(receipt_epoch, '.', 2)::bigint) > "
                "(split_part($2, '.', 1)::bigint, split_part($2, '.', 2)::bigint))", [epoch, *ancestors], epoch))
            return deleted, resent

        return await self.db.background_tx(fn)

    async def run_once(self, sender: Sender | None, log: Logger) -> None:
        """One transfer run (P8.21): number, summarize finished denial windows, send until nothing is pending,
        delete what the backup covers, refresh the backlog. Without a sender the records stay local."""
        await self.number()
        await self.flush()
        if sender is not None:
            try:
                # calls need a deadline (P4.4): the whole sending of one run gets one
                with reqctx.deadline_scope(time.monotonic() + SEND_TIMEOUT):
                    while await self.transfer(sender) >= BATCH:
                        pass
                    await self.purge(await sender.watermark())
            except (errs.Error, OSError) as e:
                log.warn("audit records could not be transferred; they stay local", error=str(e))
        n = await self.backlog()
        if self.capacity > 0 and n >= self.capacity:
            log.error("the audit backlog reached its capacity; required audited operations are refused", backlog=n)

    async def run(self, sender: Sender | None, log: Logger) -> None:
        """The transfer loop. The Python SDK has no background work items yet, so it runs as the member's task;
        every step is safe when several replicas run it (the counter's lock, receipts idempotent by ID)."""
        while True:
            try:
                await self.run_once(sender, log)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - the next run tries again
                log.warn("the audit transfer failed; it runs again", error=repr(e))
            await asyncio.sleep(TRANSFER_EVERY)
