"""Reading a class's history (be-protocol P15.26, P15.38): the online part and the units that left the online tables,
under one catalog snapshot and the caller's current authorization, complete within the sync limit or not at all; past
it the read becomes an export the SDK's queue job writes to the cold store."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from besdk import errs
from besdk.authz.segment import Tri
from besdk.db.pool import Tx
from besdk.obs import Logger
from besdk.tiering import reads
from besdk.tiering.catalog import Catalog
from besdk.tiering.store import Exists, Missing

EXPORT_TTL = dt.timedelta(days=7)  # how long a finished export is kept
EXPORT_URL_TTL = dt.timedelta(minutes=15)  # how long its presigned address is valid
EXPORT_JOB = "be.tiering.export"
# the reasons an export fails with, rather than retried: the read's own answers
ANSWERED = frozenset({"QUERY_NOT_COMPLETABLE", "QUERY_PLAN_CHANGED", "NOT_FOUND", "PERMISSION_DENIED",
                      "AUTHORIZATION_UNPROVABLE", "AUTHORIZATION_VIEW_EXPIRED"})

Emit = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class Reader:
    """The owner's code for reading one class's history: never the caller's SQL or object addresses (P15.26).

    op: the authorization operation the read is for (the resource's view operation, or an export operation).
    facts: a cold aggregate's facts for the authorization, from its root's canonical row.
    output: shapes a visible cold aggregate for the caller under its decision (authz.mask on d.fields).
    online: reads the online part under the snapshot: the class's rows the subject may see (its compiled list),
      leaving out the members already off the online tables (Snapshot.online_exclusion), each emitted shaped.
    match: keeps the cold aggregates the request's parameters select (the online part filters in its SQL); None: all.
    """

    op: str
    facts: Callable[[dict[str, Any]], dict]
    output: Callable[[reads.Aggregate, Any], dict[str, Any]]
    online: Callable[[Tx, reads.Snapshot, Any, dict, Emit], Awaitable[None]]
    match: Callable[[dict[str, Any], dict], bool] | None = None


@dataclass(frozen=True)
class Pending:
    """A history read that became an export (the component answers 202 with its id)."""

    export: str


@dataclass
class ExportStatus:
    """An export as its requester sees it: pending, done or failed."""

    state: str
    rows: int = 0
    reason: str = ""  # why it failed
    url: str = ""  # done: a presigned address, valid EXPORT_URL_TTL
    expiry: dt.datetime | None = None  # done: when it goes


class ExportErased(Exception):
    """An erasure came while the export read; it is read again."""


def register(cat: Catalog, cls: str, r: Reader) -> None:
    """Registers the reader of a class's history."""
    if cls not in cat.classes:
        raise ValueError(f"tiering: no class {cls!r} to read")
    if not r.op or r.facts is None or r.output is None or r.online is None:
        raise ValueError(f"tiering: the reader of {cls} needs op, facts, output and online")
    cat.readers[cls] = r


def reader(cat: Catalog, cls: str) -> Reader:
    r = cat.readers.get(cls)
    if r is None:
        raise errs.new("QUERY_NOT_COMPLETABLE", f"the history of {cls} has no reader")
    return r


def _slots(cat: Catalog) -> asyncio.Semaphore:
    """The member's cold-read slots (TIERING_COLD_CONCURRENCY); waiting counts toward the caller's limit."""
    if cat.slots is None:
        cat.slots = asyncio.Semaphore(cat.cold_concurrency if cat.cold_concurrency > 0 else 4)
    return cat.slots


async def _read(cat: Catalog, sub, cls: str, r: Reader, params: dict, purpose: str, emit: Emit) -> None:
    """A class's history under a new snapshot: the online part, then every cold unit by its route, each aggregate
    decided by the subject's current authorization; every visible one is emitted."""
    s = await reads.snapshot(cat, purpose)
    try:
        await cat.db.tx(lambda tx: r.online(tx, s, sub, params, emit))
        decide = sub.decider(r.op)
        for u in await reads.cold_units(cat, s, cls):
            async with _slots(cat):
                aggs = await reads.read_unit(cat, s, u)
            for a in aggs:
                if r.match is not None and not r.match(a.root, params):
                    continue
                d = decide(r.facts(a.root))
                if d.visible is not Tri.TRUE or d.action is not Tri.TRUE:
                    continue
                emit(r.output(a, d))
    finally:
        await asyncio.shield(reads.end(cat, s))


async def history(cat: Catalog, sub, cls: str, params: dict | None = None) -> tuple[list[dict], Pending | None]:
    """Reads a class's history for the subject: complete within the sync limit (TIERING_COLD_SYNC_LIMIT); past it, the
    read is started as an export and Pending names it; never a part (P15.26)."""
    r = reader(cat, cls)
    params = params or {}
    out: list[dict] = []
    try:
        async with asyncio.timeout(cat.cold_sync_limit if cat.cold_sync_limit > 0 else 10.0):
            await _read(cat, sub, cls, r, params, "history", out.append)
    except TimeoutError:
        return [], Pending(await start_export(cat, sub.principal, cls, params))
    return out, None


async def start_export(cat: Catalog, principal: str, cls: str, params: dict | None = None) -> str:
    """Records an export of a class's history for a principal and queues it (P15.38). It cannot be done without a
    cold store, on an object-locked one (an export holds personal data and must go when it expires), or without a
    reader: QUERY_NOT_COMPLETABLE."""
    reader(cat, cls)
    if cat.store is None:
        raise errs.new("QUERY_NOT_COMPLETABLE",
                       "the history cannot be read within the limit and there is no store to export it to")
    if cat.store_locked:
        raise errs.new("QUERY_NOT_COMPLETABLE",
                       "the history cannot be read within the limit and the object-locked store takes no exports")
    if cat.enqueue is None:
        raise errs.new("QUERY_NOT_COMPLETABLE",
                       "the history cannot be read within the limit and exports are not running")
    eid = uuid.uuid7()

    async def body(tx: Tx) -> None:
        await tx.execute("INSERT INTO be_tier_export (id, class, principal, params) VALUES ($1, $2, $3, $4)",
                         eid, cls, principal, json.dumps(params or {}))
        await cat.enqueue(tx, EXPORT_JOB, {"export": str(eid)})

    await cat.db.tx(body)
    return str(eid)


async def export_status(cat: Catalog, eid: str, principal: str) -> ExportStatus:
    """An export's status to its requester; anyone else gets NOT_FOUND (P15.38)."""
    r = await cat.db.tx(lambda tx: tx.fetchrow(
        """SELECT state, object_key, rows, reason, expires_at FROM be_tier_export WHERE id::text = $1
        AND principal = $2 AND (expires_at IS NULL OR expires_at > now())""", eid, principal))
    if r is None:
        raise errs.new("NOT_FOUND", "not found")
    st = ExportStatus(r["state"], r["rows"] or 0, r["reason"] or "")
    if st.state == "done" and r["object_key"] and cat.store is not None:
        st.url = cat.store.presign_get(r["object_key"], int(EXPORT_URL_TTL.total_seconds()))
        st.expiry = r["expires_at"]
    return st


def export_key(cat: Catalog, eid: str) -> str:
    """Where an export's object goes."""
    return f"{cat.store.prefix}/exports/{eid}.ndjson"


async def write_export(cat: Catalog, eid: str) -> tuple[str, int, str]:
    """Writes one pending export's object; answers its key and rows, or the reason it cannot be done."""
    r = await cat.db.tx(lambda tx: tx.fetchrow("SELECT class, principal, params, state FROM be_tier_export WHERE id = $1",
                                               uuid.UUID(eid)))
    if r is None or r["state"] != "pending":
        return "", 0, ""
    rd = cat.readers.get(r["class"])
    if rd is None or cat.store is None or cat.subject is None:
        return "", 0, "QUERY_NOT_COMPLETABLE"
    sub = cat.subject(r["principal"])
    if sub is None:
        return "", 0, "QUERY_NOT_COMPLETABLE"
    key = export_key(cat, eid)
    # an erasure since an earlier attempt read (P15.31): its object goes before the mark, so a crash in between leaves
    # the mark and the object is deleted again
    if await cat.db.tx(lambda tx: tx.fetchval("SELECT erased_at IS NOT NULL FROM be_tier_export WHERE id = $1",
                                              uuid.UUID(eid))):
        try:
            await cat.store.delete(key)
        except Missing:
            pass
        await cat.db.tx(lambda tx: tx.execute("UPDATE be_tier_export SET erased_at = NULL WHERE id = $1",
                                              uuid.UUID(eid)))
    lines: list[bytes] = []
    params = r["params"]
    params = json.loads(params) if isinstance(params, (str, bytes)) else (params or {})
    try:
        await _read(cat, sub, r["class"], rd, params, "export",
                    lambda o: lines.append(json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                                      default=str).encode() + b"\n"))
    except errs.Error as e:
        if e.domain == "be" and e.reason in ANSWERED:
            return "", 0, e.reason
        raise
    try:
        await cat.store.put(key, b"".join(lines))
    except Exists:  # an earlier attempt wrote it whole (a put is all or nothing) and did not record it
        return key, (await cat.store.get(key)).count(b"\n"), ""
    return key, len(lines), ""


def export_job(cat: Catalog, log: Logger):
    """be.tiering.export, the queue job writing the exports (P15.38): a new snapshot, the requester's authorization
    when it runs (Catalog.subject), the history as NDJSON (an output object a line, the JSON a synchronous read
    answers: masked fields null, P8.31) under <prefix>/exports/. A QUERY_* or authorization answer fails the export
    with that reason; anything else is retried."""
    from besdk.work.job import LOCAL, QUEUE, Job, Permanent

    async def handle(run) -> None:
        eid = run.args.get("export") if isinstance(run.args, dict) else None
        if not isinstance(eid, str):
            raise Permanent(f"the export's arguments {run.args!r}")
        try:
            key, rows, reason = await write_export(cat, eid)
        except Exception as e:
            if run.attempts < run.max_attempts:
                raise
            # the last attempt: the requester sees it failed rather than pending for ever
            log.error("an archive export failed on its last attempt", event="tiering_export_failed", export=eid,
                      error=repr(e))
            key, rows, reason = "", 0, "INTERNAL"

        async def record(tx: Tx) -> None:
            if reason:
                log.warn("an archive export failed", event="tiering_export_failed", export=eid, reason=reason)
                await tx.execute("""UPDATE be_tier_export SET state = 'failed', reason = $2, finished_at = now(),
                    expires_at = now() + make_interval(secs => $3) WHERE id = $1 AND state = 'pending'""",
                                 uuid.UUID(eid), reason, EXPORT_TTL.total_seconds())
                return
            if not key:
                return  # finished or gone meanwhile
            status = await tx.execute("""UPDATE be_tier_export SET state = 'done', object_key = $2, rows = $3,
                finished_at = now(), expires_at = now() + make_interval(secs => $4) WHERE id = $1 AND state = 'pending'
                AND erased_at IS NULL""", uuid.UUID(eid), key, rows, EXPORT_TTL.total_seconds())
            if not status.endswith(" 0"):
                return
            erased = await tx.fetchval("SELECT erased_at IS NOT NULL FROM be_tier_export WHERE id = $1 AND "
                                       "state = 'pending'", uuid.UUID(eid))
            if erased:
                raise ExportErased("an erasure came while the export read; it is read again")

        await run.complete(record)

    return Job(EXPORT_JOB, QUEUE, handle, LOCAL, max_attempts=5, timeout=3600.0)


async def clean_exports(cat: Catalog) -> int:
    """Removes the expired exports, their objects then their rows (P15.38); answers how many."""
    got = await cat.db.tx(lambda tx: tx.fetch(
        "SELECT id::text, coalesce(object_key, '') AS key FROM be_tier_export WHERE expires_at <= now()"))
    for g in got:
        if g["key"]:
            if cat.store is None:
                raise RuntimeError(f"tiering: export {g['id']} has an object and there is no store to delete it from")
            await cat.store.delete(g["key"])
        await cat.db.tx(lambda tx, i=g["id"]: tx.execute("DELETE FROM be_tier_export WHERE id::text = $1", i))
    return len(got)


def export_cleanup_job(cat: Catalog, log: Logger):
    """be.tiering.export.cleanup, hourly: the expired exports go (P15.38)."""
    from besdk.work.job import LOCAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        if n := await clean_exports(cat):
            log.info("expired archive exports removed", event="tiering_exports_removed", count=n)
        await run.complete()

    return Job("be.tiering.export.cleanup", SINGLETON, handle, LOCAL, interval=3600.0, max_attempts=UNLIMITED,
               timeout=3600.0)
