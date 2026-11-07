"""Writing a published or released unit again (be-protocol P15.31, P15.33, P15.35, P15.39): its content read from its
route (converted when its structure is older), changed or not, copied to a route as a new manifest version, verified
in full and published with its state unchanged. An erasure changes an aggregate; a route change moves the unit to its
class's first route. The old version's data goes once nothing can need it any more."""
from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from besdk import errs
from besdk.db.pool import Tx
from besdk.tiering import rows as canonical
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import OBJECT, Class
from besdk.tiering.manifest import Manifest
from besdk.tiering.plan import GRACE, LEASE, NO_BACKUP, PUBLISHED, RELEASED
from besdk.tiering.transfer import Claim, claim, copy_tables, publish, table_of, verify

NOTHING_OLD = "nothing_old"  # why an old version's data is kept for now (P15.39), besides grace, lease and no_backup

# turns a canonical row of an older structure version of a class's table into the current one (P15.33)
Converter = Callable[[str, dict[str, Any]], dict[str, Any]]


def register(cat: Catalog, cls: str, from_version: str, f: Converter) -> None:
    """Registers the converter from one older structure version of a class."""
    cl = cat.classes.get(cls)
    if cl is None or not from_version or from_version == cl.schema_version or f is None:
        raise ValueError(f"tiering: a converter of {cls} needs an older structure version than "
                         f"{cl.schema_version if cl else '?'}")
    cat.converters[f"{cls}@{from_version}"] = f


def convert(cat: Catalog, cl: Class, from_version: str,
            tables: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    """A unit's canonical rows of an older structure turned into the class's, the result checked (P15.33): every row
    kept, its columns and types the class's, the roots' locate columns there. No converter: QUERY_NOT_COMPLETABLE."""
    f = cat.converters.get(f"{cl.name}@{from_version}")
    if f is None:
        raise errs.new("QUERY_NOT_COMPLETABLE", f"{cl.name} has no converter from structure version "
                                                f"{from_version}").with_("schema_version", from_version)
    out: dict[str, list[dict[str, Any]]] = {}
    for name, rs in tables.items():
        found = table_of(cl, name)
        if found is None:
            raise ValueError(f"tiering: structure version {from_version} of {cl.name} has the table {name} the class "
                             "no longer has")
        t = found[0]
        for r in rs:
            try:
                conv = f(name, r)
            except Exception as e:
                raise ValueError(f"tiering: converting {name} from {from_version}: {e!r}") from e
            if conv is None:
                raise ValueError(f"tiering: the converter of {name} from {from_version} dropped a row")
            try:
                canonical.from_canonical(t.columns, conv)
            except Exception as e:
                raise ValueError(f"tiering: converted {name} from {from_version}: {e}") from e
            if name == cl.root.name:
                for col in (cl.root.pk, *cl.locate):
                    if col not in conv:
                        raise ValueError(f"tiering: a converted {name} from {from_version} has no {col}")
            out.setdefault(name, []).append(conv)
    return out


async def rewrite(cat: Catalog, unit: str, route: str = "", edit: Callable[[Any], None] | None = None,
                  also: Callable[[Tx, Claim], Awaitable[None]] | None = None) -> Manifest:
    """Writes a published or released unit again on a route ("" its own) as a new manifest version, its aggregates
    changed by edit (None: as they are), with also in the publish's transaction."""
    from besdk.tiering import reads

    c = await claim(cat, unit, PUBLISHED, RELEASED)
    cl = cat.classes[c.cls]
    u = await cat.tx("rewrite.route", lambda tx: reads.route_of(tx, unit))
    u.thaw_version = 0  # the route's own copy, not a thawed one
    aggs = await reads.read_unit_route(cat, u)
    tables: dict[str, list[dict[str, Any]]] = {}
    for a in aggs:
        if edit is not None:
            edit(a)
        tables.setdefault(cl.root.name, []).append(canonical.from_canonical(cl.root.columns, a.root))
        for ch in cl.children:
            for r in a.children.get(ch.name, []):
                tables.setdefault(ch.name, []).append(canonical.from_canonical(ch.table.columns, r))
    sums = []
    for t in cl.tables():
        h = canonical.TableHash(t.columns, t.key_columns())
        for r in tables.get(t.name, []):
            h.add(r)
        sums.append((t.name, h.rows, h.hexdigest()))
    c.digest = canonical.unit_digest(cl.name, cl.schema_version, sums)
    m = await copy_tables(cat, c, route or u.route, tables)
    await verify(cat, m)
    await publish(cat, c, m, None if also is None else (lambda tx: also(tx, c)))
    return m


async def reroute(cat: Catalog, unit: str, route: str) -> Manifest:
    """Moves a published or released unit to another route (P15.35, TR20, TR26): a new manifest version there, its
    state unchanged; the old route's data goes by release_old."""
    return await rewrite(cat, unit, route)


async def due(cat: Catalog) -> dict[str, str]:
    """The published and released units on another route than their class's first."""
    got = await cat.tx("reroute.due", lambda tx: tx.fetch(
        "SELECT id::text, class, route FROM be_tier_unit WHERE state IN ('published', 'released')"))
    out = {}
    for r in got:
        cl = cat.classes.get(r["class"])
        if cl is not None and cl.routes and cl.routes[0] != r["route"]:
            out[r["id"]] = cl.routes[0]
    return out


async def release_old(cat: Catalog, unit: str) -> list[str]:
    """Deletes the data of a unit's versions a newer one replaced (P15.39) once the newer one is past the grace, no
    lease from before its route changed is alive, and a backup finished after it; answers why it did not."""
    from besdk import crash

    finished = await cat.backups() if cat.backups is not None else None

    async def decide(tx: Tx) -> tuple[str, list, list[str]]:
        r = await tx.fetchrow("""SELECT u.class, u.manifest_version, u.route_version, m.created_at, now() AS now
            FROM be_tier_unit u JOIN be_tier_manifest m ON m.unit = u.id AND m.version = u.manifest_version
            WHERE u.id = $1 AND u.state IN ('published', 'released')""", uuid.UUID(unit))
        if r is None:
            raise LookupError(f"tiering: unit {unit} is not published or released")
        olds = await tx.fetch("""SELECT version, route, generation FROM be_tier_manifest WHERE unit = $1 AND version < $2
            AND superseded_at IS NULL ORDER BY version""", uuid.UUID(unit), r["manifest_version"])
        why = []
        if not olds:
            return r["class"], [], why
        if r["created_at"] + cat.grace > r["now"]:
            why.append(GRACE)
        leases = await tx.fetchval("SELECT count(*) FROM be_tier_lease WHERE catalog_version < $1 AND expires_at > now()",
                                   r["route_version"] or 0)
        if leases:
            why.append(LEASE)
        if finished is None or not finished > r["created_at"]:
            why.append(NO_BACKUP)
        return r["class"], list(olds), why

    cls, olds, why = await cat.tx("releaseold.decide", decide)
    if not olds:
        return [NOTHING_OLD]
    if why:
        return why
    for o in olds:
        if o["route"] == OBJECT:
            if cat.store is None:
                raise RuntimeError("tiering: an old version on the object route and no store to delete it from")
            for obj in await cat.store.list(f"{cat.store.prefix}/{cls}/{unit}/{o['generation']}.{o['version']}/"):
                await cat.store.delete(obj.key)
                crash.point("tiering.releaseold.object/done")

        async def gone(tx: Tx, v: int = o["version"]) -> None:
            await tx.execute("DELETE FROM be_tier_row WHERE unit = $1 AND version = $2", uuid.UUID(unit), v)
            await tx.execute("UPDATE be_tier_manifest SET superseded_at = now() WHERE unit = $1 AND version = $2",
                             uuid.UUID(unit), v)

        await cat.tx("releaseold.delete", gone)
    return []


async def old_versions(cat: Catalog) -> list[str]:
    """The units with a version whose data is not released yet."""
    return [r[0] for r in await cat.tx("releaseold.list", lambda tx: tx.fetch(
        """SELECT DISTINCT m.unit::text FROM be_tier_manifest m JOIN be_tier_unit u ON u.id = m.unit
        WHERE m.version < u.manifest_version AND m.superseded_at IS NULL AND u.state IN ('published', 'released')"""))]


async def check_readable(cat: Catalog, cls: str) -> dict[str, Exception]:
    """Reads one unit of every older structure version a class still has (P15.34): what each fails with, by structure
    version; empty when every one reads. A component's tests run it before a new structure version ships."""
    from besdk.tiering import reads

    cl = cat.classes.get(cls)
    if cl is None:
        raise ValueError(f"tiering: no class {cls!r}")
    samples = await cat.tx("readable.list", lambda tx: tx.fetch(
        """SELECT DISTINCT ON (v) v, unit FROM (SELECT convert_from(m.manifest, 'UTF8')::jsonb ->> 'schema_version' AS v,
        u.id::text AS unit FROM be_tier_unit u JOIN be_tier_manifest m ON m.unit = u.id AND
        m.version = u.manifest_version WHERE u.class = $1 AND u.state IN ('published', 'released')) s
        WHERE v <> $2 ORDER BY v, unit""", cls, cl.schema_version))
    failed: dict[str, Exception] = {}
    for v, unit in samples:
        u = await cat.tx("readable.read", lambda tx, unit=unit: reads.route_of(tx, unit))
        try:
            await reads.read_unit_route(cat, u)
        except Exception as e:  # noqa: BLE001 - what it fails with is the answer
            failed[v] = e
    return failed

