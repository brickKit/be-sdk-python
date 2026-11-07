"""Reads (be-protocol P15.22-P15.25, P15.27): by a root's key through the locate index, and lists under a catalog
snapshot that reads each unit once by its route at the snapshot. What leaves the online tables is read back as
canonical rows (P15.8: text, decimals and instants as strings, nulls absent), so every route answers the same rows.
A route that cannot answer for a unit the catalog says is there is DEPENDENCY_UNAVAILABLE, never "not found"."""
from __future__ import annotations

import datetime as dt
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from besdk import errs
from besdk.db.pool import Tx
from besdk.tiering import parquet
from besdk.tiering import rows as canonical
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import ONLINE, OBJECT, POSTGRES, Class, Table, ident
from besdk.tiering.manifest import Manifest, parse
from besdk.tiering.plan import DESTROYED

LEASE_TTL = dt.timedelta(minutes=15)  # a snapshot's lease, renewed while the read goes on (protocol 11)
THAW_VERSION = -1  # a version no manifest has

# decides a cold aggregate under the current authorization (P15.26): whether its root is visible and the readable
# fields (None: all)
Visible = Callable[[dict[str, Any]], Awaitable[tuple[bool, list[str] | None]]]


@dataclass
class Aggregate:
    """One aggregate read from a route: its root and its children by table, canonical rows."""

    root: dict[str, Any]
    children: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    unit: str = ""
    route: str = ""


@dataclass
class UnitRoute:
    id: str
    cls: str
    state: str
    route: str
    version: int
    route_version: int
    thaw_version: int


@dataclass(frozen=True)
class Snapshot:
    """A read under one catalog version with its lease (P15.24, P15.25)."""

    version: int
    lease: str

    def online_exclusion(self, cl: Class, alias: str) -> str:
        """The condition an online query over a class's root (alias) adds under the snapshot: leave out the
        aggregates whose unit had left the online tables at its version (they are read from their route) and those of
        a destroyed unit (their online rows are being deleted)."""
        name = cl.name.replace("'", "''")
        return (f"NOT EXISTS (SELECT 1 FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit WHERE m.class = "
                f"'{name}' AND m.root_id = {alias}.{ident(cl.root.pk)}::text AND ((u.route <> 'online' AND "
                f"u.route_version <= {int(self.version)}) OR u.state = 'destroyed'))")


async def route_of(tx: Tx, unit: str) -> UnitRoute:
    r = await tx.fetchrow("""SELECT u.class, u.state, u.route, u.manifest_version, u.route_version, t.version AS thaw
        FROM be_tier_unit u LEFT JOIN be_tier_thaw t ON t.unit = u.id AND t.expires_at > now() WHERE u.id = $1""",
                          uuid.UUID(unit))
    return UnitRoute(unit, r["class"], r["state"], r["route"], r["manifest_version"], r["route_version"] or 0,
                     r["thaw"] or 0)


def unavailable(u: UnitRoute, cause: str) -> errs.Error:
    """A route that cannot answer for a unit the catalog says is there (P15.22, TR09)."""
    return errs.new("DEPENDENCY_UNAVAILABLE", "an archived record cannot be read now").with_(
        "dependency", "db" if u.route == POSTGRES else "blob").with_("unit", u.id).wrap(RuntimeError(cause))


async def get(cat: Catalog, cls: str, root: str) -> Aggregate | None:
    """One aggregate by its root's key (P15.22); None: it has not left the online tables (or does not exist), and the
    caller reads its online tables, after asking here. A destroyed unit's members answer NOT_FOUND from the decision
    on (P15.29)."""
    async def find(tx: Tx) -> UnitRoute | None:
        unit = await tx.fetchval("SELECT unit::text FROM be_tier_member WHERE class = $1 AND root_id = $2", cls, root)
        return None if unit is None else await route_of(tx, unit)

    u = await cat.db.tx(find)
    if u is None:
        return None
    if u.state == DESTROYED:
        raise errs.new("NOT_FOUND", "the record was destroyed at the end of its retention")
    if u.route == ONLINE:
        return None
    aggs = await read_unit_route(cat, u, root)
    if not aggs:
        raise unavailable(u, f"the root {root} is not in its unit's copy")
    return aggs[0]


def manifest_table(mt) -> Table:
    """The structure a manifest's table was written in."""
    cols = {n: typ for n, (typ, _) in mt.columns.items()}
    return Table(mt.name, cols, pk=mt.pk[0]) if len(mt.pk) == 1 else Table(mt.name, cols, key=tuple(mt.pk))


def _canonical(t: Table, got: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [canonical.row(t.columns, r) for r in got]


async def read_unit_route(cat: Catalog, u: UnitRoute, root: str = "") -> list[Aggregate]:
    """A unit's aggregates (only one root when root is set) from its route, or its thawed copy."""
    cl = cat.classes[u.cls]
    raw = await cat.db.tx(lambda tx: tx.fetchval("SELECT manifest FROM be_tier_manifest WHERE unit = $1 AND version = $2",
                                                 uuid.UUID(u.id), u.version))
    m: Manifest = parse(bytes(raw))
    route, version = (POSTGRES, u.thaw_version) if u.thaw_version else (u.route, u.version)
    tables: dict[str, list[dict[str, Any]]] = {}
    if route == POSTGRES:
        for r in await cat.db.tx(lambda tx: tx.fetch("SELECT tbl, row FROM be_tier_row WHERE unit = $1 AND version = $2",
                                                     uuid.UUID(u.id), version)):
            tables.setdefault(r["tbl"], []).append(json.loads(bytes(r["row"])))
        if not tables:
            raise unavailable(u, "no warm rows")
    elif route == OBJECT:
        if cat.store is None:
            raise unavailable(u, "no object store configured")
        for mt in m.tables:
            t = manifest_table(mt)
            if cl.root.name != mt.name and cl.child(mt.name) is None and m.schema_version == cl.schema_version:
                raise unavailable(u, f"the manifest names {mt.name}")
            for o in mt.objects:
                try:
                    data = await cat.store.get(o.key)
                    from besdk.tiering.transfer import sha
                    if sha(data) != o.sha256:
                        raise ValueError("the object is not the manifest's")
                    got = await cat.compute(parquet.read, t, data)
                except errs.Error:
                    raise
                except Exception as e:  # noqa: BLE001 - missing, unreadable or tampered: not "not found"
                    raise unavailable(u, repr(e)) from None
                tables.setdefault(t.name, []).extend(await cat.compute(_canonical, t, got))
    else:
        raise RuntimeError(f"tiering: unit {u.id} has no route to read")
    if m.schema_version != cl.schema_version:  # an older structure: through the component's converter (P15.33)
        from besdk.tiering import reroute
        tables = reroute.convert(cat, cl, m.schema_version, tables)
    return assemble(cl, tables, u, root)


def assemble(cl: Class, tables: dict[str, list[dict[str, Any]]], u: UnitRoute, root: str = "") -> list[Aggregate]:
    """A unit's canonical rows grouped into aggregates by the root's key."""
    def key(t: Table, col: str, r: dict[str, Any]) -> str:
        return str(canonical.value(t.columns[col], r[col])) if r.get(col) is not None else ""

    want = str(canonical.value(cl.root.columns[cl.root.pk], root)) if root else ""
    out: list[Aggregate] = []
    index: dict[str, int] = {}
    for r in tables.get(cl.root.name, []):
        k = key(cl.root, cl.root.pk, r)
        if want and k != want:
            continue
        index[k] = len(out)
        out.append(Aggregate(r, {}, u.id, u.route))
    root_of: dict[str, dict[str, str]] = {}  # a child table's rows: their key -> their root's key
    for ch in cl.by_depth():  # a parent's rows are mapped before its children's
        root_of[ch.name] = {}
        parent = cl.child(ch.parent)
        for r in tables.get(ch.name, []):
            k = key(ch.table, ch.fk, r)
            if parent is not None:
                k = root_of[parent.name].get(k)  # type: ignore[assignment]
                if k is None:
                    continue  # its parent is not in the unit (another root's when one root was asked)
            if ch.table.pk:  # a possible parent: its rows' roots for its children
                root_of[ch.name][key(ch.table, ch.table.pk, r)] = k
            if (i := index.get(k)) is not None:
                out[i].children.setdefault(ch.name, []).append(r)
    return out


async def snapshot(cat: Catalog, purpose: str) -> Snapshot:
    """A lease at the current catalog version: a unit published after it is not released while the lease lives, so
    the read finds its rows online."""
    lease = uuid.uuid7()

    async def body(tx: Tx) -> int:
        v = await tx.fetchval("SELECT version FROM be_tier_catalog")
        await tx.execute("INSERT INTO be_tier_lease (id, catalog_version, purpose, expires_at) VALUES ($1, $2, $3, "
                         "now() + make_interval(secs => $4))", lease, v, purpose, LEASE_TTL.total_seconds())
        return v

    return Snapshot(await cat.db.tx(body), str(lease))


async def renew(cat: Catalog, s: Snapshot) -> None:
    """Extends a snapshot's lease; QUERY_PLAN_CHANGED when it ran out (what it read under may be gone)."""
    status = await cat.db.tx(lambda tx: tx.execute(
        "UPDATE be_tier_lease SET expires_at = now() + make_interval(secs => $2) WHERE id = $1 AND expires_at > now()",
        uuid.UUID(s.lease), LEASE_TTL.total_seconds()))
    if status.endswith(" 0"):
        raise errs.new("QUERY_PLAN_CHANGED", "the archive snapshot of this read has expired; start again")


async def end(cat: Catalog, s: Snapshot) -> None:
    await cat.db.tx(lambda tx: tx.execute("DELETE FROM be_tier_lease WHERE id = $1", uuid.UUID(s.lease)))


async def cold_units(cat: Catalog, s: Snapshot, cls: str) -> list[str]:
    """A class's units the snapshot reads from their routes: off the online tables at its version."""
    return [r[0] for r in await cat.db.tx(lambda tx: tx.fetch(
        """SELECT id::text FROM be_tier_unit WHERE class = $1 AND route <> 'online' AND route_version <= $2
        AND state IN ('published', 'released') ORDER BY id""", cls, s.version))]


async def read_unit(cat: Catalog, s: Snapshot, unit: str, visible: Visible | None = None) -> list[Aggregate]:
    """A cold unit's aggregates under a snapshot, those the caller may see, their fields masked to the readable ones
    (P15.26)."""
    await renew(cat, s)
    u = await cat.db.tx(lambda tx: route_of(tx, unit))
    aggs = await read_unit_route(cat, u)
    if visible is None:
        return aggs
    out = []
    for a in aggs:
        ok, fields = await visible(a.root)
        if not ok:
            continue
        if fields is not None:
            a.root = {k: v for k, v in a.root.items() if k in fields}
        out.append(a)
    return out


async def thaw(cat: Catalog, unit: str, ttl: dt.timedelta) -> None:
    """Copies a unit's rows to a read-only area for ttl (P15.27): reads by key prefer it; nothing reopens writes."""
    u = await cat.db.tx(lambda tx: route_of(tx, unit))
    if u.route == ONLINE:
        raise ValueError(f"tiering: unit {unit} is online")
    u.thaw_version = 0
    aggs = await read_unit_route(cat, u)
    cl = cat.classes[u.cls]
    keys, tbls, raws = [], [], []
    for a in aggs:
        for t, rs in ((cl.root, [a.root]), *((ch.table, a.children.get(ch.name, [])) for ch in cl.children)):
            for r in rs:
                keys.append(canonical.pk_bytes(t.columns, t.key_columns(), r).decode())
                tbls.append(t.name)
                raws.append(canonical.row_bytes(t.columns, r))

    async def body(tx: Tx) -> None:
        await tx.execute("DELETE FROM be_tier_row WHERE unit = $1 AND version = $2", uuid.UUID(unit), THAW_VERSION)
        await tx.execute("""INSERT INTO be_tier_row (unit, version, tbl, pk, row)
            SELECT $1, $2, t, k, r FROM unnest($3::text[], $4::text[], $5::bytea[]) AS x(t, k, r)""",
                         uuid.UUID(unit), THAW_VERSION, tbls, keys, raws)
        await tx.execute("""INSERT INTO be_tier_thaw (unit, version, expires_at) VALUES ($1, $2,
            now() + make_interval(secs => $3)) ON CONFLICT (unit) DO UPDATE SET version = EXCLUDED.version,
            expires_at = EXCLUDED.expires_at""", uuid.UUID(unit), THAW_VERSION, ttl.total_seconds())

    await cat.tx("thaw", body)
