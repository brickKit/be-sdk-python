"""be.external.v1.ReconcileService (P13.11-P13.13): a full recovery's step 7 for one adapter member, only for the
recovery tool, in the verifying phase. It reads the open commands, asks the external system (one listing, or one query
per reference) outside any transaction, then matches (reconcile.py), applies and keeps the conclusions in one
transaction and answers every conclusion of this recovery. The external system not answering fails the whole call and
writes nothing."""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from besdk import errs, reqctx
from besdk._gen.be.external.v1 import reconcile_pb2, reconcile_pb2_grpc
from besdk.db.pool import Tx
from besdk.extcmd import reconcile as rc
from besdk.extcmd.commands import LIST_TIMEOUT, SEND_TIMEOUT, Command, Commands, _outcome, _Row
from besdk.extcmd.state import MANUAL, Capabilities, Facts


@dataclass
class _Request:
    recovery: str
    t: datetime
    before: timedelta
    failed_at: datetime | None
    issued: list[str]


def _time(s: str, name: str) -> datetime:
    try:
        t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        raise errs.new("REQUEST_INVALID", f"{name} is not an RFC 3339 time") from None
    if t.tzinfo is None:
        raise errs.new("REQUEST_INVALID", f"{name} is not an RFC 3339 time")
    return t


def _parse(req: reconcile_pb2.ReconcileRequest) -> _Request:
    if not req.recovery:
        raise errs.new("REQUEST_INVALID", "recovery is required")
    if req.window_before_seconds < 0:
        raise errs.new("REQUEST_INVALID", "window_before_seconds is negative")
    return _Request(req.recovery, _time(req.t, "t"), timedelta(seconds=req.window_before_seconds),
                    _time(req.failed_at, "failed_at") if req.failed_at else None, list(req.issued))


_OPEN = """SELECT reference, id::text AS id, operation, state, resend_deadline, expires_at, first_reached_at,
        last_reached_at, unknown_since, accepted_since, unrecorded_until,
        (SELECT min(started_at) FROM be_external_attempt a WHERE a.command = c.id AND a.kind = 'send'
            AND a.ended_at IS NULL) AS sending_at
    FROM be_external_command c WHERE state NOT IN ('succeeded', 'failed', 'rejected') ORDER BY reference"""


async def _open(tx: Tx, lock: bool) -> list[rc.Open]:
    rows = await tx.fetch(_OPEN + (" FOR UPDATE" if lock else ""))
    return [rc.Open(r["reference"], r["id"], r["operation"], Facts(
        r["state"], r["resend_deadline"], r["expires_at"], r["first_reached_at"], r["last_reached_at"],
        r["unknown_since"], r["accepted_since"], r["unrecorded_until"]), r["sending_at"]) for r in rows]


async def _mark_local(tx: Tx, local: set[str], refs: list[str]) -> None:
    """Adds to local each of refs this member has a command for, in any state."""
    if refs:
        local.update(r["reference"] for r in await tx.fetch(
            "SELECT DISTINCT reference FROM be_external_command WHERE reference = ANY($1)", refs))


def _rfc3339(t: datetime | None) -> str:
    return "" if t is None else t.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class ReconcileServer(reconcile_pb2_grpc.ReconcileServiceServicer):
    def __init__(self, commands: Commands):
        self.c = commands

    async def Reconcile(self, request, context) -> reconcile_pb2.ReconcileResponse:  # noqa: N802
        r = _parse(request)
        c, local = self.c, set()

        async def read(tx: Tx) -> list[rc.Open]:
            out = await _open(tx, False)
            await _mark_local(tx, local, r.issued)
            return out

        before = await c.db.background_tx(read)
        try:
            records = await self._ask(before, local, r)
        except Exception as e:  # noqa: BLE001 - nothing is written; the recovery tool asks again
            raise errs.new("DEPENDENCY_UNAVAILABLE", f"the external system's records: {e}") from None

        async def apply(tx: Tx) -> list[reconcile_pb2.Conclusion]:
            await self._apply(tx, r, before, records, local)
            rows = await tx.fetch("""SELECT r.reference, coalesce(r.command::text, '') AS command, r.conclusion,
                    coalesce(c.state, r.state, '') AS state, r.resend_deadline, coalesce(r.detail, '') AS detail
                FROM be_external_reconciled r LEFT JOIN be_external_command c ON c.id = r.command
                WHERE r.recovery = $1 ORDER BY r.reference""", r.recovery)
            return [reconcile_pb2.Conclusion(reference=x["reference"], command=x["command"], conclusion=x["conclusion"],
                                             state=x["state"], resend_deadline=_rfc3339(x["resend_deadline"]),
                                             detail=x["detail"]) for x in rows]

        return reconcile_pb2.ReconcileResponse(conclusions=await c.db.background_tx(apply))

    async def _ask(self, open_: list[rc.Open], local: set[str], r: _Request) -> dict[str, rc.Record]:
        """Lists the external records from T minus the window to now when the adapter can (P13.11), and queries by
        reference what the listing does not cover: an open command that may have been sent before the window, every
        open command and issued reference when the adapter cannot list. Commands of an operation without the query
        capability are not asked. An answer it cannot get raises."""
        c, records = self.c, {}
        start = r.t - r.before
        lists = hasattr(c.adapter, "list")
        if lists:
            with reqctx.deadline_scope(time.monotonic() + LIST_TIMEOUT):
                for rec in await asyncio.wait_for(c.adapter.list(start, datetime.now(timezone.utc)), LIST_TIMEOUT):  # type: ignore[union-attr]
                    records[rec.reference] = rec

        async def query(cmd: Command) -> None:
            with reqctx.deadline_scope(time.monotonic() + SEND_TIMEOUT):
                ans = await asyncio.wait_for(c.adapter.query(cmd), SEND_TIMEOUT)  # type: ignore[union-attr]
            if ans.found:
                records[cmd.reference] = rc.Record(cmd.reference, ans.state, ans.external_id, ans.outcome, ans.response)

        for o in open_:
            if (lists and not rc.earliest(o, r.t) < start) or not c.ops.get(o.operation, Capabilities()).query:
                continue
            await query(Command(o.command, c.component, o.reference, o.operation, {}))
        if lists or not any(cp.query for cp in c.ops.values()):
            return records
        for ref in r.issued:
            if ref not in local:
                await query(Command("", c.component, ref, "", {}))
        return records

    async def _apply(self, tx: Tx, r: _Request, before: list[rc.Open], records: dict[str, rc.Record],
                     local: set[str]) -> None:
        """Matches under the lock of the open commands (only those read before asking: one created since was not
        asked about), applies each conclusion and keeps it for this recovery."""
        now = await tx.fetchval("SELECT now()")
        asked = {o.reference for o in before}
        local |= asked  # one final since is still local
        by_ref = {o.reference: o for o in await _open(tx, True) if o.reference in asked}
        await _mark_local(tx, local, list(records))
        until = r.failed_at or now
        caps = lambda op: self.c.ops.get(op, Capabilities())  # noqa: E731
        for cl in rc.match(list(by_ref.values()), {k: v.state for k, v in records.items()}, local, r.issued, caps,
                           r.t, until, now):
            state = ""
            if (o := by_ref.get(cl.reference)) is not None:
                state = await self._conclude(tx, o, cl, records.get(cl.reference), until, now)
            await tx.execute("""INSERT INTO be_external_reconciled (recovery, reference, command, conclusion, state,
                    resend_deadline, detail) VALUES ($1, $2, nullif($3, '')::uuid, $4, nullif($5, ''), $6, nullif($7, ''))
                ON CONFLICT (recovery, reference) DO UPDATE SET command = excluded.command,
                    conclusion = excluded.conclusion, state = excluded.state, resend_deadline = excluded.resend_deadline,
                    detail = excluded.detail, updated_at = now()""",
                             r.recovery, cl.reference, cl.command, cl.conclusion, state, cl.resend_deadline, cl.detail)

    async def _conclude(self, tx: Tx, o: rc.Open, cl: rc.Conclusion, rec: rc.Record | None, until: datetime,
                        now: datetime) -> str:
        """Applies one conclusion to its command; answers the command's state after it."""
        c = self.c
        row = _Row(Command(o.command, "", o.reference, o.operation, {}), o.facts, c.ops.get(o.operation, Capabilities()),
                   0, now)
        cid = uuid.UUID(o.command)
        if cl.conclusion == rc.MATCHED:  # the external record, not sent again
            await tx.execute("""UPDATE be_external_command SET external_id = coalesce(nullif($2, ''), external_id),
                outcome = coalesce($3::jsonb, outcome) WHERE id = $1""", cid, rec.external_id if rec else "",
                             _outcome(rec.outcome if rec else None))
            await c._set(tx, row, cl.state, "reconciled: the external record", c._next_at(row, cl.state, 0))
            return cl.state
        if cl.conclusion == rc.CONCLUSION_MANUAL and o.facts.state == MANUAL:
            return MANUAL
        await tx.execute("""UPDATE be_external_command SET first_reached_at = $2, unrecorded_until = $3,
            resend_deadline = coalesce($4, resend_deadline) WHERE id = $1""", cid, cl.first_reached, until,
                         cl.resend_deadline)
        if cl.conclusion in (rc.CONCLUSION_UNKNOWN, rc.CONCLUSION_MANUAL):
            await c._set(tx, row, cl.conclusion, "reconciled: " + cl.detail, now)
            return cl.conclusion
        return o.facts.state  # deadline: the state unchanged
