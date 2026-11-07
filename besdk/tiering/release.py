"""Releasing the online rows of a published unit (be-protocol P15.19, P15.21): only when the grace has passed, the
unit's newest record is older than N days, no read lease taken before the route changed is still running, and a
backup finished after the publish. The rows go in batches, the children first; the SDK names the unit in each
transaction (be.tier_unit) so the write barrier lets this one change through."""
from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Awaitable, Callable

from besdk.db.pool import Tx
from besdk.obs import Logger
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import Class, ident
from besdk.tiering.plan import PUBLISHED, PlanClass, PlanUnit, plan
from besdk.tiering.transfer import StaleClaim, _tx, units_in

BATCH = 10_000  # the rows one release transaction deletes per table (Catalog.batch overrides it)

# when the deployment's latest backup finished (None: none known); the run profile's kind backup
Backups = Callable[[], Awaitable[dt.datetime | None]]


def backups_from(run_profile) -> Backups:
    """The latest backup from the run profile's kind backup: the greatest finished_at among the records whose
    signature holds (P12.4, P15.21)."""
    from besdk.runprofile import BACKUP

    async def latest() -> dt.datetime | None:
        out = None
        for x in await run_profile.list_all(BACKUP):
            if x.err or not x.read or not isinstance(x.record.get("body"), dict):
                continue
            try:
                at = dt.datetime.fromisoformat(str(x.record["body"].get("finished_at", "")).replace("Z", "+00:00"))
            except ValueError:
                continue
            if at.tzinfo is not None and (out is None or at > out):
                out = at
        return out

    return latest


async def release(cat: Catalog, unit: str) -> list[str]:
    """Releases one published unit's online rows if nothing holds it back; answers the reasons it did not. The
    latest backup is read before the transaction (the run profile's store, never inside one)."""
    finished = await cat.backups() if cat.backups is not None else None

    async def decide(tx: Tx) -> tuple[str, list[str]]:
        r = await tx.fetchrow("SELECT class, state, published_at, newest_created, route_version FROM be_tier_unit "
                              "WHERE id = $1", uuid.UUID(unit))
        if r is None or r["state"] != PUBLISHED:
            raise StaleClaim(f"unit {unit} is {r and r['state']}, not published")
        leases = await tx.fetchval("SELECT count(*) FROM be_tier_lease WHERE catalog_version < $1 AND expires_at > now()",
                                   r["route_version"] or 0)
        now = await tx.fetchval("SELECT now()")
        cl = cat.classes[r["class"]]
        d = plan(now, cat.n, cat.grace, PlanClass(cl.category, cl.residence_days),
                 PlanUnit(PUBLISHED, published_at=r["published_at"], newest_created=r["newest_created"],
                          old_leases=leases, backup_after_publish=finished is not None and finished > r["published_at"]))
        return r["class"], d.reasons

    cls, why = await _tx(cat, "release.decide", decide)
    if why:
        return why

    async def last(tx: Tx) -> None:
        await tx.execute("UPDATE be_tier_unit SET state = 'released', released_at = now() WHERE id = $1 "
                         "AND state = 'published'", uuid.UUID(unit))
        await tx.execute("UPDATE be_tier_catalog SET version = version + 1")

    await delete_online(cat, unit, cat.classes[cls], last)
    return []


async def delete_online(cat: Catalog, unit: str, cl: Class, last: Callable[[Tx], Awaitable[None]] | None) -> None:
    """A unit's online rows deleted in batches, the deepest table with rows left first (a parent after its
    children), each transaction naming the unit for the write barrier; last runs in the transaction that finds none
    left."""
    batch = cat.batch or BATCH
    members = "(SELECT m.root_id FROM be_tier_member m WHERE m.unit = $1)"

    async def step(tx: Tx) -> bool:
        await tx.execute("SELECT set_config('be.tier_unit', $1, true)", unit)
        for ch in cl.by_depth(deepest_first=True):
            status = await tx.execute(f"DELETE FROM {ident(ch.name)} WHERE ctid IN (SELECT t.ctid FROM {ident(ch.name)} t "
                                      f"WHERE {cl.root_expr(ch, 't')} IN {members} LIMIT {batch})", uuid.UUID(unit))
            if not status.endswith(" 0"):
                return False
        status = await tx.execute(f"DELETE FROM {ident(cl.root.name)} WHERE ctid IN (SELECT ctid FROM "
                                  f"{ident(cl.root.name)} WHERE {ident(cl.root.pk)}::text IN {members} LIMIT {batch})",
                                  uuid.UUID(unit))
        if not status.endswith(" 0"):
            return False
        if last is not None:
            await last(tx)
        return True

    while not await _tx(cat, "release.delete", step):
        pass


async def release_due(cat: Catalog, log: Logger) -> None:
    """One run of be.tiering.release: the old versions a newer one replaced (P15.39), then every published unit."""
    from besdk.tiering import reroute

    for u in await reroute.old_versions(cat):
        try:
            if not await reroute.release_old(cat, u):
                log.info("an archive unit's old version is released", event="tiering_released_old", unit=u)
        except Exception as e:  # noqa: BLE001
            log.error("an archive unit's old version could not be released", event="tiering_release_old_failed",
                      unit=u, error=repr(e))
    for u in await units_in(cat, PUBLISHED):
        try:
            why = await release(cat, u)
        except StaleClaim as e:  # destroyed or released by another worker meanwhile
            log.info("an archive unit moved on before its release", event="tiering_release_moved_on", unit=u,
                     detail=str(e))
            continue
        except Exception as e:  # noqa: BLE001
            log.error("an archive unit's release failed", event="tiering_release_failed", unit=u, error=repr(e))
            continue
        if not why:
            log.info("an archive unit's online rows are released", event="tiering_released", unit=u)
        elif cat.backups is None:
            log.error("no run profile: archive units are never released", event="tiering_no_backup_record", unit=u)


def release_job(cat: Catalog, log: Logger):
    """be.tiering.release, hourly (P15.19, P15.39)."""
    from besdk.work.job import LOCAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        await release_due(cat, log)
        await run.complete()

    return Job("be.tiering.release", SINGLETON, handle, LOCAL, interval=3600.0, max_attempts=UNLIMITED, timeout=3600.0)
