"""Erasing one aggregate's personal data (be-protocol P15.31; the assembly protocol 07's erasure): the class's erasable
columns of its root and children become null. Where it is depends on its unit: still online (no unit, or an open one)
the caller erases its own rows; a sealed unit's online rows are erased and its digest computed again in the same
transaction (a transfer under way then fails to publish and starts again, TR27); a published or released unit is
written again as a new manifest version on its route, its state unchanged, the thawed copy dropped. A live hold over
the unit defers it with an alert (TR15). The evidence (category erasure) is written in two phases, as a
destruction's."""
from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from besdk import errs
from besdk.db.pool import Tx
from besdk.evidence.replay import Dangling, Replayed
from besdk.evidence.writer import Object
from besdk.obs import Logger
from besdk.tiering import reroute
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import Class, ident
from besdk.tiering.destroy import read_unit
from besdk.tiering.holds import _declared, holds_on
from besdk.tiering.plan import DESTROYED, OPEN, PUBLISHED, SEALED
from besdk.tiering.transfer import Passthrough, StaleClaim, _tx

CATEGORY = "erasure"
ONLINE = "online"  # not in a unit past open: the caller erases its rows
DEFERRED = "deferred"  # a live hold covers it: alerted, done once lifted
ERASED = "erased"
JOB = "be.tiering.erase"
ATTEMPTS = 5  # how many times an erasure starts again when the unit moved on under it (StaleClaim)

Conclude = Callable[[Tx], Awaitable[None]]


class _Held(Passthrough):
    """A live hold covers the unit."""


async def erase(cat: Catalog, cls: str, root: str, basis: str) -> str:
    """Erases one aggregate's erasable columns for a basis (the request's reference); answers the outcome."""
    return await _erase(cat, cls, root, basis, False)


async def _erase(cat: Catalog, cls: str, root: str, basis: str, redo: bool) -> str:
    """erase; redo is a replayed erasure's re-execution: no hold check, no new evidence (P12.21)."""
    cl = _declared(cat, cls)
    if not cl.erasable:
        raise errs.new("REQUEST_INVALID", cls + " declares no erasable columns")

    async def find(tx: Tx):
        unit = await tx.fetchval("SELECT unit::text FROM be_tier_member WHERE class = $1 AND root_id = $2", cls, root)
        if unit is None:
            return None, None, 0
        return unit, await read_unit(tx, unit, False), await holds_on(tx, unit, cls)

    unit, u, holds = await cat.tx("erase.find", find)
    if unit is None or u is None or u.state == OPEN:
        return ONLINE
    if u.state == DESTROYED:
        return ERASED  # nothing of it is left
    if holds and not redo:
        _deferred(cat, unit, cls, root)
        return DEFERRED
    p = None
    if not redo:
        if cat.evidence is None:
            raise RuntimeError("tiering: no evidence writer: an erasure is never done without its evidence (P15.31)")
        p = await cat.evidence.intent(CATEGORY, "", Object("be.tier.record", f"{cls}/{root}", str(u.manifest)),
                                      {"class": cls, "root": root, "unit": unit, "digest": u.digest, "basis": basis,
                                       "columns": list(cl.erasable)})

    async def conclude(tx: Tx) -> None:
        if redo:
            return
        await tx.execute("LOCK TABLE be_tier_hold IN SHARE MODE")
        if await holds_on(tx, unit, cls):
            raise _Held()
        await p.effective(tx)

    gone = False
    err: BaseException | None = None
    for attempt in range(1, ATTEMPTS + 1):
        try:
            if u.state == SEALED:
                await _tx(cat, "erase.sealed", lambda tx: erase_sealed(cat, tx, cl, unit, root, conclude))
            else:
                await erase_again(cat, cl, unit, root, conclude)
            err = None
            break
        except StaleClaim as e:
            err = e
            if attempt == ATTEMPTS:
                break
        except BaseException as e:  # noqa: BLE001 - concluded below
            err = e
            break
        # the unit moved on meanwhile (published, or written again by a transfer, a route change or another
        # erasure): the erasure starts again from its state now; the intent stays the one written
        u = await cat.tx("erase.find", lambda tx: read_unit(tx, unit, False))
        if u.state == DESTROYED:  # nothing of it is left to erase
            gone, err = True, None
            break
    if p is not None and (gone or err is not None):
        await p.cancelled()
    if isinstance(err, _Held):
        _deferred(cat, unit, cls, root)
        return DEFERRED
    if err is not None:
        raise err
    return ERASED


def _deferred(cat: Catalog, unit: str, cls: str, root: str) -> None:
    if cat.log is not None:
        cat.log.error("an erasure is deferred: a live hold covers its unit", event="tiering_erasure_deferred",
                      unit=unit, **{"class": cls}, root=root)


async def void_exports(tx: Tx, cls: str) -> None:
    """Makes the class's exports that may hold an erased aggregate unreadable (P15.31, assembly project A159): the
    finished ones expire now (their status answers NOT_FOUND, the cleanup deletes their objects); a pending one is
    marked, and its job reads again rather than finishing. It runs in the transaction that erases: the SDK's erasures
    call it; a component erasing its online rows (ONLINE) calls it in its own transaction."""
    await tx.execute("""UPDATE be_tier_export SET expires_at = now() WHERE class = $1 AND state = 'done'
        AND (expires_at IS NULL OR expires_at > now())""", cls)
    await tx.execute("UPDATE be_tier_export SET erased_at = now() WHERE class = $1 AND state = 'pending'", cls)


async def null_online(tx: Tx, cl: Class, unit: str, root: str) -> None:
    """Sets an aggregate's erasable columns null in the online tables, naming its unit for the write barrier, and
    voids the class's exports."""
    await tx.execute("SELECT set_config('be.tier_unit', $1, true)", unit)
    await void_exports(tx, cl.name)
    for t in cl.tables():
        cols = [c for c in cl.erasable if c in t.columns]
        if not cols:
            continue
        ch = cl.child(t.name)
        key = cl.root_expr(ch, "t") if ch is not None else f"t.{ident(cl.root.pk)}::text"
        await tx.execute(f"UPDATE {ident(t.name)} AS t SET {', '.join(ident(c) + ' = NULL' for c in cols)} "
                         f"WHERE {key} = $1", root)


async def erase_sealed(cat: Catalog, tx: Tx, cl: Class, unit: str, root: str, conclude: Conclude) -> None:
    """Erases a sealed unit's aggregate online and computes the unit's digest again, in one transaction."""
    await tx.execute("SELECT 1 FROM be_tier_unit WHERE id = $1 AND state = 'sealed' FOR UPDATE", uuid.UUID(unit))
    await null_online(tx, cl, unit, root)
    roots = [r[0] for r in await tx.fetch("SELECT root_id FROM be_tier_member WHERE unit = $1", uuid.UUID(unit))]
    digest = await cat.digest(tx, cl, roots)
    status = await tx.execute("UPDATE be_tier_unit SET digest = $2 WHERE id = $1 AND state = 'sealed'",
                              uuid.UUID(unit), digest)
    if status.endswith(" 0"):  # published meanwhile: the caller starts again
        raise StaleClaim(f"unit {unit} left sealed during the erasure")
    await conclude(tx)


async def erase_again(cat: Catalog, cl: Class, unit: str, root: str, conclude: Conclude) -> None:
    """Writes a published or released unit again with the aggregate erased, as a new manifest version on its route
    (the publish rule, P15.15-P15.18); the online rows a published unit still has are erased in the publish."""
    erasable = set(cl.erasable)

    def null(r: dict) -> None:
        for c in [c for c in r if c in erasable]:
            del r[c]  # null: absent from the canonical row

    def edit(a) -> None:
        if str(a.root.get(cl.root.pk)) != root:
            return
        null(a.root)
        for rs in a.children.values():
            for r in rs:
                null(r)

    async def also(tx: Tx, claim) -> None:
        await tx.execute("DELETE FROM be_tier_row WHERE unit = $1 AND version = -1", uuid.UUID(unit))
        await tx.execute("DELETE FROM be_tier_thaw WHERE unit = $1", uuid.UUID(unit))
        if claim.state == PUBLISHED:  # its online rows are still there until released
            await null_online(tx, cl, unit, root)
        else:
            await void_exports(tx, cl.name)
        await conclude(tx)

    await reroute.rewrite(cat, unit, "", edit, also)


def erase_job(cat: Catalog, log: Logger):
    """be.tiering.erase: the queue job that redoes an erasure replayed in a full recovery (P15.30) once the member is
    open."""
    from besdk.work.job import LOCAL, QUEUE, UNLIMITED, Job, Permanent

    async def handle(run) -> None:
        a = run.args if isinstance(run.args, dict) else None
        if a is None or not all(isinstance(a.get(k), str) for k in ("class", "root", "basis")):
            raise Permanent(f"the erasure's arguments {run.args!r}")
        try:
            await _erase(cat, a["class"], a["root"], a["basis"], True)
        except Exception as e:
            log.warn("a replayed erasure is not done yet", event="tiering_erasure_redo_failed", **{"class": a["class"]},
                     root=a["root"], error=repr(e))
            raise
        await run.complete()

    return Job(JOB, QUEUE, handle, LOCAL, max_attempts=UNLIMITED, timeout=3600.0)


async def replay(cat: Catalog, tx: Tx, rec: Replayed) -> None:
    """Re-executes a replayed erasure (P15.30): a sealed unit's rows at once, in the replay's transaction; a published
    or released unit's new version by the erase job once open; online, the online rows."""
    cls, root, basis = (str(rec.change.get(k, "")) for k in ("class", "root", "basis"))
    cl = cat.classes.get(cls)
    if cl is None or not root:
        raise Dangling(f"no class {cls!r} or no root")
    r = await tx.fetchrow("""SELECT u.id::text AS id, u.state FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit
        WHERE m.class = $1 AND m.root_id = $2 FOR UPDATE OF u""", cls, root)
    if r is None or r["state"] == OPEN:
        await null_online(tx, cl, "", root)  # in no unit past open: the online tables, no barrier
        return
    if r["state"] == DESTROYED:
        return
    if r["state"] == SEALED:
        async def nothing(tx: Tx) -> None:
            return None

        await erase_sealed(cat, tx, cl, r["id"], root, nothing)
        return
    if cat.enqueue is None:
        raise RuntimeError(f"tiering: the erasure of {cls}/{root} needs the erase job")
    await cat.enqueue(tx, JOB, {"class": cls, "root": root, "basis": basis})
