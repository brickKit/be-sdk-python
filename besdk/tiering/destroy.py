"""Destruction (be-protocol P15.29, P15.30): decided in one local transaction that first takes the hold table in SHARE
mode (a hold set meanwhile waits for it, TR14), then checks the retention is due, no live hold covers the unit, an
approval does, and the retention is not pending. Its evidence is written in two phases: the intent before the decision
(cancelled when the decision does not hold), the conclusion in it. The data goes after the commit and may be deleted
again: the online rows, the warm rows, the thawed copy and the objects. The members stay (a destroyed record read by
key is not found)."""
from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass

from besdk.db.pool import Tx
from besdk.evidence.replay import Dangling, Replayed, Replays
from besdk.evidence.writer import Object
from besdk.obs import Logger
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import Class
from besdk.tiering.holds import approved_for, holds_on
from besdk.tiering.plan import DESTROY, DESTROYED, OPEN, PlanClass, PlanUnit, plan
from besdk.tiering.release import delete_online
from besdk.tiering.transfer import _tx

CATEGORY = "destruction"
NOT_DUE = "retention_not_due"
PURGE_AGAIN = dt.timedelta(days=7)  # how long after a destruction the job deletes its data again


@dataclass
class UnitFacts:
    cls: str
    state: str
    digest: str
    manifest: int
    anchor: dt.datetime | None


async def read_unit(tx: Tx, unit: str, lock: bool) -> UnitFacts | None:
    r = await tx.fetchrow("SELECT class, state, digest, manifest_version, anchor_max FROM be_tier_unit WHERE id = $1"
                          + (" FOR UPDATE" if lock else ""), uuid.UUID(unit))
    if r is None:
        return None
    return UnitFacts(r["class"], r["state"], r["digest"] or "", r["manifest_version"], r["anchor_max"])


async def reasons(cat: Catalog, tx: Tx, unit: str, u: UnitFacts) -> list[str]:
    """What holds a unit's destruction back now (P15.29), with the plan's names."""
    cl = cat.classes[u.cls]
    now = await tx.fetchval("SELECT now()")
    due = u.anchor is not None and u.anchor + dt.timedelta(days=cl.retention.min_days) <= now
    if not cl.retention.pending and not due:  # a pending retention is never due: the plan says why (TR24)
        return [NOT_DUE]
    holds = await holds_on(tx, unit, u.cls)
    approved = await approved_for(tx, unit, u.cls)
    d = plan(now, cat.n, cat.grace, PlanClass(cl.category, cl.residence_days, cl.retention.pending, bool(cl.erasable)),
             PlanUnit(u.state, retention_due=True, approved=approved, holds=holds))
    if d.action == DESTROY:
        return []
    return d.reasons or [NOT_DUE]


async def mark_destroyed(tx: Tx, unit: str, basis: str) -> None:
    """Turns a unit destroyed with its tombstone and moves the catalog version."""
    await tx.execute("""UPDATE be_tier_unit SET state = 'destroyed', destroyed_at = now(), tombstone =
        jsonb_build_object('scope', 'unit', 'unit', id::text, 'class', class, 'digest', digest, 'members', members,
        'basis', $2::text, 'at', now()) WHERE id = $1""", uuid.UUID(unit), basis)
    await tx.execute("UPDATE be_tier_catalog SET version = version + 1")


async def destroy(cat: Catalog, unit: str) -> list[str]:
    """Destroys a unit whose destruction is due and allowed; answers the reasons it did not. A unit already destroyed
    has its data deleted again."""
    async def first(tx: Tx) -> tuple[UnitFacts, list[str]]:
        u = await read_unit(tx, unit, False)
        if u is None:
            raise LookupError(f"tiering: no unit {unit}")
        if u.state in (DESTROYED, OPEN):
            return u, []
        return u, await reasons(cat, tx, unit, u)

    u, why = await cat.tx("destroy.reasons", first)
    if u.state == DESTROYED:
        await purge(cat, unit, cat.classes[u.cls])
        return []
    if u.state == OPEN:
        raise ValueError(f"tiering: unit {unit} is open: nothing to destroy")
    if why:
        return why
    if cat.evidence is None:
        raise RuntimeError("tiering: no evidence writer: a destruction is never done without its evidence (P15.29)")
    cl = cat.classes[u.cls]
    p = await cat.evidence.intent(CATEGORY, "", Object("be.tier.unit", unit, str(u.manifest)),
                                  {"unit": unit, "class": u.cls, "digest": u.digest,
                                   "manifest_version": str(u.manifest), "basis": cl.retention.basis})

    async def decide(tx: Tx) -> list[str]:
        await tx.execute("LOCK TABLE be_tier_hold IN SHARE MODE")
        cur = await read_unit(tx, unit, True)
        if cur is None or cur.state in (DESTROYED, OPEN):
            return [NOT_DUE]
        if got := await reasons(cat, tx, unit, cur):
            return got
        await p.effective(tx)
        await mark_destroyed(tx, unit, cl.retention.basis)
        return []

    try:
        why = await _tx(cat, "destroy.decide", decide)
    except BaseException:
        await p.cancelled()
        raise
    if why:
        await p.cancelled()
        return why
    await purge(cat, unit, cl)
    return []


async def purge(cat: Catalog, unit: str, cl: Class) -> None:
    """Deletes a destroyed unit's data: online rows, warm rows, the thawed copy, the objects. It may run again."""
    from besdk import crash

    await delete_online(cat, unit, cl, None)

    async def rows(tx: Tx) -> None:
        await tx.execute("DELETE FROM be_tier_thaw WHERE unit = $1", uuid.UUID(unit))
        await tx.execute("DELETE FROM be_tier_row WHERE unit = $1", uuid.UUID(unit))

    await cat.tx("destroy.purge", rows)
    if cat.store is None:
        return
    for o in await cat.store.list(f"{cat.store.prefix}/{cl.name}/{unit}/"):
        await cat.store.delete(o.key)
        crash.point("tiering.destroy.object/done")


async def continue_releases(cat: Catalog) -> list[str]:
    """Deletes the online rows a released unit still has (the recovery check's finding), a class at a time."""
    from besdk.tiering.classes import ident

    done = []
    for cl in cat.classes.values():
        units = [r[0] for r in await cat.tx("check.released", lambda tx, cl=cl: tx.fetch(
            f"""SELECT DISTINCT m.unit::text FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit
            JOIN {ident(cl.root.name)} r ON r.{ident(cl.root.pk)}::text = m.root_id
            WHERE u.state = 'released' AND u.class = $1""", cl.name))]
        for u in units:
            await delete_online(cat, u, cl, None)
            done.append(u)
    return done


async def destroy_due(cat: Catalog, log: Logger) -> None:
    """One run of be.tiering.destroy: every unit past open tried (destroy checks), the data of the units destroyed in
    the last PURGE_AGAIN deleted again, and the online rows a released unit still has deleted."""
    units = [r[0] for r in await cat.tx("destroy.due", lambda tx: tx.fetch(
        """SELECT id::text FROM be_tier_unit WHERE state IN ('sealed', 'published', 'released')
        OR (state = 'destroyed' AND destroyed_at > now() - make_interval(secs => $1)) ORDER BY id""",
        PURGE_AGAIN.total_seconds()))]
    try:
        continued = await continue_releases(cat)
    except Exception as e:  # noqa: BLE001
        log.error("a released unit's online rows could not be deleted", event="tiering_release_continue_failed",
                  error=repr(e))
        continued = []
    for u in continued:
        log.warn("a released unit still had online rows: deleted", event="tiering_release_continued", unit=u)
    for u in units:
        try:
            why = await destroy(cat, u)
        except Exception as e:  # noqa: BLE001
            log.error("an archive unit's destruction failed", event="tiering_destroy_failed", unit=u, error=repr(e))
            continue
        if not why:
            log.info("an archive unit is destroyed (or its data deleted again)", event="tiering_destroyed", unit=u)


def destroy_job(cat: Catalog, log: Logger):
    """be.tiering.destroy, daily (P15.29)."""
    from besdk.work.job import LOCAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        await destroy_due(cat, log)
        await run.complete()

    return Job("be.tiering.destroy", SINGLETON, handle, LOCAL, interval=86400.0, max_attempts=UNLIMITED,
               timeout=6 * 3600.0)


def replaying(cat: Catalog) -> bool:
    """Whether the catalog has evidence to replay: a class that can be destroyed (its retention is not pending) or
    erased. The component then declares the replay's operation (Definition.replay)."""
    return any(not cl.retention.pending or cl.erasable for cl in cat.classes.values())


def replays(cat: Catalog, r: Replays) -> None:
    """Registers the re-execution of the destructions and erasures a full recovery finds after its point (P15.30,
    TR16): a destroyed unit turns destroyed without the business preconditions, its data going with the next
    destruction run (PURGE_AGAIN); an erasure as erase.replay says."""
    from besdk.tiering import erase

    async def destruction(tx: Tx, rec: Replayed) -> None:
        unit = rec.object.id
        u = await read_unit(tx, unit, True)
        if u is None:
            raise Dangling(f"no unit {unit}")
        if u.state != DESTROYED:
            await mark_destroyed(tx, unit, str(rec.change.get("basis", "")))

    r.add(erase.CATEGORY, lambda tx, rec: erase.replay(cat, tx, rec))
    r.add(CATEGORY, destruction)
