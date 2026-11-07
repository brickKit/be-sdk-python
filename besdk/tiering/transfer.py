"""The transfer (be-protocol P15.14-P15.18, P15.20): a sealed unit is claimed (its generation up, the database epoch
recorded), copied outside any transaction from a read-only snapshot to its route, verified by reading everything
back, and published in one local transaction that checks the state, the generation, the epoch and the digest. What a
worker that lost its claim wrote is never published: the cleanup removes it after the verification window."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from besdk import canon
from besdk.db.pool import Tx, TxOptions
from besdk.obs import Logger
from besdk.tiering import parquet
from besdk.tiering import rows as canonical
from besdk.tiering.catalog import Catalog, select_list
from besdk.tiering.classes import OBJECT, POSTGRES, Class, Table, ident
from besdk.tiering.manifest import PARQUET, PG_ROWS, Manifest, Object
from besdk.tiering.manifest import Table as ManifestTable
from besdk.tiering.manifest import manifest_key, object_key
from besdk.tiering.plan import PUBLISHED, RELEASED, SEALED

PART_ROWS = 200_000  # one Parquet object's bound
ORPHAN_WINDOW = dt.timedelta(days=7)  # an unpublished object or row is kept this long (protocol 11)
SNAPSHOT = TxOptions(snapshot=True)


class Passthrough(Exception):
    """Raised inside a catalog transaction and raised by it as itself (the transaction entry maps what its body
    raises)."""


class Verification(Passthrough):
    """A copy that does not read back as what was sealed (P15.17)."""


class StaleClaim(Passthrough):
    """A publish refused because the unit changed since the claim (P15.18)."""


@dataclass
class Claim:
    """One worker's claim of a unit: a sealed unit's transfer, or a published or released unit written again (an
    erasure's new version, a route change: P15.31, P15.35). digest is the content this claim writes, was the unit's
    digest when claimed (equal for a transfer); state and base the unit's state and manifest version when claimed,
    version the one this claim writes."""

    unit: str
    cls: str
    epoch: str
    generation: int
    digest: str
    was: str
    state: str
    base: int
    version: int


async def epoch_of(tx: Tx) -> str:
    r = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
    return f"{r[0]}.{r[1]}"


async def claim(cat: Catalog, unit: str, *states: str) -> Claim:
    """Raises a unit's generation for a worker, the unit in one of the states (P15.14)."""
    states = states or (SEALED,)

    async def body(tx: Tx) -> Claim:
        r = await tx.fetchrow("SELECT class, state, coalesce(digest, '') AS digest, manifest_version FROM be_tier_unit "
                              "WHERE id = $1 FOR UPDATE", uuid.UUID(unit))
        if r is None or r["state"] not in states:
            raise StaleClaim(f"unit {unit} is {r and r['state']}, not {' or '.join(states)}")
        epoch = await epoch_of(tx)
        gen = await tx.fetchval("UPDATE be_tier_unit SET generation = generation + 1, claim_epoch = $2 WHERE id = $1 "
                                "RETURNING generation", uuid.UUID(unit), epoch)
        return Claim(unit, r["class"], epoch, gen, r["digest"], r["digest"], r["state"], r["manifest_version"],
                     r["manifest_version"] + 1)

    return await _tx(cat, "claim", body)


async def _tx(cat: Catalog, point: str, fn, options=None):
    """The catalog's transaction, a Passthrough raised as itself."""
    try:
        return await cat.tx(point, fn, options)
    except Exception as e:
        if isinstance(e.__cause__, Passthrough):
            raise e.__cause__ from None
        raise


async def read_unit(cat: Catalog, cl: Class, unit: str) -> dict[str, list[dict[str, Any]]]:
    """A unit's tables from one read-only snapshot: the roots (members) and their children."""
    members = "(SELECT m.root_id FROM be_tier_member m WHERE m.unit = $1 AND m.class = $2)"

    async def body(tx: Tx) -> dict[str, list[dict[str, Any]]]:
        out = {}

        async def read(t: Table, where: str) -> None:
            names, cols = select_list(t, "t")
            got = await tx.fetch(f"SELECT {cols} FROM {ident(t.name)} t WHERE {where}", uuid.UUID(unit), cl.name)
            out[t.name] = [{n: r[i] for i, n in enumerate(names)} for r in got]

        await read(cl.root, f"t.{ident(cl.root.pk)}::text IN {members}")
        for ch in cl.children:
            await read(ch.table, f"{cl.root_expr(ch, 't')} IN {members}")
        return out

    return await _tx(cat, "copy.read", body, SNAPSHOT)


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


async def copy(cat: Catalog, c: Claim, route: str) -> Manifest:
    """Writes the claimed unit to a route and answers its manifest (P15.15, P15.16)."""
    return await copy_tables(cat, c, route, await read_unit(cat, cat.classes[c.cls], c.unit))


async def copy_tables(cat: Catalog, c: Claim, route: str, tables: dict[str, list[dict[str, Any]]]) -> Manifest:
    """A unit's content (the driver's values by table) written to a route as the claim's version."""
    from besdk import crash

    cl = cat.classes[c.cls]
    if route == OBJECT:
        if cat.store is None:
            raise RuntimeError("tiering: the object route has no store (TIERING_STORE_URL)")
        fmt = PARQUET
    elif route == POSTGRES:
        fmt = PG_ROWS
    else:
        raise ValueError(f"tiering: unknown route {route!r}")
    m = Manifest(c.unit, c.cls, c.version, route, c.generation, c.epoch, fmt, cl.schema_version, c.digest, [],
                 canon.time(dt.datetime.now(dt.timezone.utc)))
    for t in cl.tables():
        got = tables.get(t.name, [])
        mt = ManifestTable(t.name, t.key_columns(), {n: (t.columns[n], True) for n in t.columns}, len(got))
        if route == OBJECT:
            per = cat.parts or PART_ROWS
            part = 0
            while part == 0 or part * per < len(got):
                chunk = got[part * per:(part + 1) * per]
                data = await cat.compute(parquet.write, t, chunk)
                key = object_key(cat.store.prefix, c.cls, c.unit, c.generation, c.version, t.name, part)
                await cat.store.put(key, data)
                crash.point("tiering.copy.object/done")
                mt.objects.append(Object(key, len(chunk), len(data), sha(data)))
                part += 1
        else:
            await _copy_rows(cat, c, t, got)
        m.tables.append(mt)
    if route == OBJECT:
        await cat.store.put(manifest_key(cat.store.prefix, c.cls, c.unit, c.generation, c.version), m.encode())
        crash.point("tiering.copy.manifest/done")
    return m


def _canonical_rows(t: Table, got: list[dict[str, Any]]) -> list[tuple[str, bytes]]:
    return [(canonical.pk_bytes(t.columns, t.key_columns(), r).decode(), canonical.row_bytes(t.columns, r))
            for r in got]


async def _copy_rows(cat: Catalog, c: Claim, t: Table, got: list[dict[str, Any]]) -> None:
    """A table's canonical rows into the warm route under the claim's version (not read before published)."""
    encoded = await cat.compute(_canonical_rows, t, got)

    async def body(tx: Tx) -> None:
        # a stale worker's rows of the same version would be another generation's: the generation decides (P15.18)
        await tx.execute("DELETE FROM be_tier_row WHERE unit = $1 AND version = $2 AND tbl = $3", uuid.UUID(c.unit),
                         c.version, t.name)
        await tx.execute("""INSERT INTO be_tier_row (unit, version, tbl, pk, row)
            SELECT $1, $2, $3, k, r FROM unnest($4::text[], $5::bytea[]) AS x(k, r)""",
                         uuid.UUID(c.unit), c.version, t.name, [k for k, _ in encoded], [raw for _, raw in encoded])

    await _tx(cat, "copy.rows", body)


def table_of(cl: Class, name: str) -> tuple[Table, str] | None:
    if cl.root.name == name:
        return cl.root, ""
    ch = cl.child(name)
    return (ch.table, ch.fk) if ch is not None else None


def _sum_rows(t: Table, fk: str, root: bool, items: list[tuple[bytes, bytes, Any, Any]]) -> tuple[int, str, set, list]:
    """A table's count and hash from (key, row, pk value, fk value) items, with its keys and its parents' keys."""
    h = canonical.TableHash(t.columns, t.key_columns())
    keys, refs = set(), []
    for k, raw, pk, fkv in items:
        h.add_canonical(k, raw)
        if t.pk:
            keys.add(pk)
        if not root:
            refs.append(fkv)
    return h.rows, h.hexdigest(), keys, refs


def _from_driver(t: Table, fk: str, got: list[dict[str, Any]]) -> list[tuple[bytes, bytes, Any, Any]]:
    out = []
    for r in got:
        pk = canonical.value(t.columns[t.pk], r[t.pk]) if t.pk and r.get(t.pk) is not None else None
        ref = canonical.value(t.columns[fk], r[fk]) if fk and r.get(fk) is not None else None
        out.append((canonical.pk_bytes(t.columns, t.key_columns(), r), canonical.row_bytes(t.columns, r), pk, ref))
    return out


def _read_and_sum(t: Table, fk: str, root: bool, data: list[bytes]) -> tuple[int, str, set, list]:
    items = []
    for raw in data:
        items += _from_driver(t, fk, parquet.read(t, raw))
    return _sum_rows(t, fk, root, items)


async def verify(cat: Catalog, m: Manifest) -> None:
    """Reads the copy back in full and checks it against the manifest and the sealed digest (P15.17); raises
    Verification."""
    cl = cat.classes[m.cls]
    sums, keys, children = [], {}, {}
    for mt in m.tables:
        found = table_of(cl, mt.name)
        if found is None:
            raise Verification(f"table {mt.name} is not the class's")
        t, fk = found
        root = t.name == cl.root.name
        if m.route == OBJECT:
            data = []
            for o in mt.objects:
                raw = await cat.store.get(o.key)
                if len(raw) != o.bytes or sha(raw) != o.sha256:
                    raise Verification(f"object {o.key} is not what the manifest says")
                data.append(raw)
            try:
                n, h, ks, refs = await cat.compute(_read_and_sum, t, fk, root, data)
            except (ValueError, canon.Refused) as e:
                raise Verification(str(e)) from None
        else:
            got = await _tx(cat, "verify.rows", lambda tx, mt=mt: tx.fetch(
                "SELECT pk, row FROM be_tier_row WHERE unit = $1 AND version = $2 AND tbl = $3",
                uuid.UUID(m.unit), m.version, mt.name))
            items = []
            for r in got:
                obj = json.loads(bytes(r["row"]))
                items.append((r["pk"].encode(), bytes(r["row"]), obj.get(t.pk) if t.pk else None,
                              obj.get(fk) if fk else None))
            n, h, ks, refs = _sum_rows(t, fk, root, items)
        if n != mt.rows:
            raise Verification(f"{mt.name} has {n} rows, the manifest {mt.rows}")
        keys[t.name], children[t.name] = ks, refs
        sums.append((mt.name, n, h))
    for name, refs in children.items():  # every child's parent is in the unit, level by level
        ch = cl.child(name)
        if ch is None:
            continue
        parent = cl.parent(ch)
        if any(r not in keys.get(parent.name, set()) for r in refs):  # type: ignore[union-attr]
            raise Verification(f"a row of {name} whose parent is not in the unit")
    for col in cl.locate:  # the columns reads need are in the copy
        if col not in cl.root.columns:
            raise Verification(f"the locate column {col} is missing")
    digest = canonical.unit_digest(cl.name, m.schema_version, sums)
    if digest != m.digest:
        raise Verification(f"the content's digest {digest} is not the sealed {m.digest}")


async def publish(cat: Catalog, c: Claim, m: Manifest, also: Callable[[Tx], Any] | None = None) -> None:
    """Makes a verified copy the unit's authority in one local transaction (P15.18): the unit still in the state and
    at the manifest version it was claimed in, the claim's generation and epoch current, its digest unchanged since
    the claim; otherwise StaleClaim and nothing changes. A sealed unit turns published; a published or released one
    written again keeps its state (P15.31, P15.35). also runs in the same transaction (an erasure's rows)."""
    raw = m.encode()

    async def body(tx: Tx) -> None:
        r = await tx.fetchrow("""SELECT state, coalesce(digest, '') AS digest, generation, claim_epoch, manifest_version,
            route FROM be_tier_unit WHERE id = $1 FOR UPDATE""", uuid.UUID(c.unit))
        now = await epoch_of(tx)
        if r["state"] != c.state or r["manifest_version"] != c.base:
            raise StaleClaim(f"unit {c.unit} is {r['state']} at version {r['manifest_version']}, the claim's "
                             f"{c.state} at {c.base}")
        if r["generation"] != c.generation or r["claim_epoch"] != c.epoch:
            raise StaleClaim(f"unit {c.unit} is at generation {r['generation']}, the claim {c.generation}")
        if c.epoch != now:
            raise StaleClaim(f"the database's epoch is {now}, the claim's {c.epoch}")
        if r["digest"] != c.was or m.digest != c.digest:
            raise StaleClaim("the sealed content changed")
        await tx.execute("INSERT INTO be_tier_manifest (unit, version, route, generation, manifest, digest) "
                         "VALUES ($1, $2, $3, $4, $5, $6)", uuid.UUID(c.unit), m.version, m.route, c.generation, raw,
                         m.digest)
        catalog = await tx.fetchval("UPDATE be_tier_catalog SET version = version + 1 RETURNING version")
        if r["state"] == SEALED:
            await tx.execute("""UPDATE be_tier_unit SET route = $2, manifest_version = $3, state = 'published',
                published_at = now(), route_version = $4 WHERE id = $1""", uuid.UUID(c.unit), m.route, m.version,
                             catalog)
        else:  # written again: the route version moves only when the route does (older leases read the old one)
            await tx.execute("""UPDATE be_tier_unit SET route = $2, manifest_version = $3, digest = $4,
                route_version = CASE WHEN route = $2 THEN route_version ELSE $5 END WHERE id = $1""",
                             uuid.UUID(c.unit), m.route, m.version, m.digest, catalog)
        if also is not None:
            await also(tx)

    await _tx(cat, "publish", body)


async def transfer(cat: Catalog, unit: str) -> Manifest:
    """Claims, copies, verifies and publishes one sealed unit to its class's first route."""
    c = await claim(cat, unit, SEALED)
    m = await copy(cat, c, cat.classes[c.cls].routes[0])
    await verify(cat, m)
    await publish(cat, c, m)
    return m


async def clean_orphans(cat: Catalog, cls: str, unit: str, window: dt.timedelta) -> int:
    """What no manifest of a unit names (P15.20), for one unit or every unit of the class (unit empty): objects under
    the prefix older than the window, warm rows of a version no manifest names that no transfer is writing, and the
    thawed copies whose time is up (P15.27)."""
    from besdk import crash

    named: dict[str, list[str]] = {}
    for r in await _tx(cat, "orphans.list", lambda tx: tx.fetch(
            """SELECT m.unit::text AS unit, m.generation, m.version FROM be_tier_manifest m JOIN be_tier_unit u
            ON u.id = m.unit WHERE u.class = $1 AND ($2 = '' OR m.unit::text = $2)""", cls, unit)):
        named.setdefault(r["unit"], []).append(f"{r['generation']}.{r['version']}/")
    removed = 0
    if cat.store is not None:
        base = f"{cat.store.prefix}/{cls}/"
        now = dt.datetime.now(dt.timezone.utc)
        for o in await cat.store.list(base + (unit + "/" if unit else "")):
            u, _, rest = o.key[len(base):].partition("/")
            if any(rest.startswith(p) for p in named.get(u, [])) or now - o.last_modified < window:
                continue
            await cat.store.delete(o.key)
            crash.point("tiering.orphans.object/done")
            removed += 1

    async def body(tx: Tx) -> int:
        n = 0
        # a warm row is an orphan when no manifest names its version and it is not the version a transfer may be
        # writing now (the next one)
        for q in ("""DELETE FROM be_tier_row r USING be_tier_unit u WHERE u.id = r.unit AND u.class = $1
                AND ($2 = '' OR r.unit::text = $2) AND r.version >= 0 AND r.version <> u.manifest_version + 1
                AND NOT EXISTS (SELECT 1 FROM be_tier_manifest m WHERE m.unit = r.unit AND m.version = r.version)""",
                  """DELETE FROM be_tier_row r USING be_tier_unit u, be_tier_thaw t WHERE u.id = r.unit
                AND u.class = $1 AND ($2 = '' OR r.unit::text = $2) AND r.version = -1 AND t.unit = r.unit
                AND t.expires_at <= now()"""):
            n += int((await tx.execute(q, cls, unit)).split()[-1])
        await tx.execute("""DELETE FROM be_tier_thaw t USING be_tier_unit u WHERE u.id = t.unit AND u.class = $1
            AND ($2 = '' OR t.unit::text = $2) AND t.expires_at <= now()""", cls, unit)
        return n

    return removed + await _tx(cat, "orphans.delete", body)


async def units_in(cat: Catalog, state: str) -> list[str]:
    return [r[0] for r in await _tx(cat, "units", lambda tx: tx.fetch(
        "SELECT id::text FROM be_tier_unit WHERE state = $1 ORDER BY sealed_at LIMIT 100", state))]


async def transfer_due(cat: Catalog, log: Logger) -> None:
    """One run of be.tiering.transfer: every sealed unit transferred, then the units whose class's first route
    changed moved there (P15.35)."""
    from besdk.tiering import reroute

    for u in await units_in(cat, SEALED):
        try:
            m = await transfer(cat, u)
        except StaleClaim as e:  # destroyed, or published by another worker, meanwhile
            log.info("an archive unit moved on before its transfer", event="tiering_transfer_moved_on", unit=u,
                     detail=str(e))
            continue
        except Exception as e:  # noqa: BLE001 - its online rows stay; the next run tries again
            log.error("an archive unit's transfer failed; its online rows stay", event="tiering_transfer_failed",
                      unit=u, error=repr(e))
            continue
        log.info("an archive unit is published", event="tiering_published", unit=u, route=m.route, version=m.version)
    for u, route in (await reroute.due(cat)).items():
        try:
            m = await reroute.reroute(cat, u, route)
        except StaleClaim as e:
            log.info("an archive unit moved on before its route change", event="tiering_reroute_moved_on", unit=u,
                     detail=str(e))
            continue
        except Exception as e:  # noqa: BLE001
            log.error("an archive unit's route change failed; it stays on its route", event="tiering_reroute_failed",
                      unit=u, route=route, error=repr(e))
            continue
        log.info("an archive unit moved to another route", event="tiering_rerouted", unit=u, route=m.route,
                 version=m.version)


def transfer_job(cat: Catalog, log: Logger):
    """be.tiering.transfer (P15.14-P15.18, P15.35)."""
    from besdk.work.job import IDEMPOTENT_EXTERNAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        await transfer_due(cat, log)
        await run.complete()

    return Job("be.tiering.transfer", SINGLETON, handle, IDEMPOTENT_EXTERNAL, interval=60.0, max_attempts=UNLIMITED,
               timeout=3600.0)


def cleanup_job(cat: Catalog, log: Logger):
    """be.tiering.cleanup, daily: every class's orphans and expired thawed copies (P15.20, P15.27)."""
    from besdk.work.job import IDEMPOTENT_EXTERNAL, SINGLETON, UNLIMITED, Job

    async def handle(run) -> None:
        window = cat.orphan_age or ORPHAN_WINDOW
        for name, cl in cat.classes.items():
            if cl.tierable() and (n := await clean_orphans(cat, name, "", window)):
                log.info("archive orphans removed", event="tiering_orphans_removed", **{"class": name}, removed=n)
        await run.complete()

    return Job("be.tiering.cleanup", SINGLETON, handle, IDEMPOTENT_EXTERNAL, interval=86400.0, max_attempts=UNLIMITED,
               timeout=6 * 3600.0)


__all__ = ["Claim", "StaleClaim", "Verification", "claim", "copy", "verify", "publish", "transfer", "clean_orphans",
           "transfer_job", "cleanup_job", "PUBLISHED", "RELEASED"]
