"""The recovery check be.tiering (be-protocol P15.36; the assembly protocol 15's RC11, protocol 11's TR17): after a full
recovery the catalog, the manifests, the warm rows and the objects are compared. One read-only transaction reads the
catalog, then the component's prefix in the cold store is listed once, outside it."""
from __future__ import annotations

from dataclasses import dataclass, field

from besdk.db.pool import Tx
from besdk.recovery.checks import DIFF, PASS, Check, Difference, Read, Result
from besdk.tiering import reroute
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import OBJECT, POSTGRES, ident
from besdk.tiering.manifest import Manifest, parse
from besdk.tiering.plan import PUBLISHED, RELEASED

NAME = "be.tiering"
UNIT = "tier_unit"  # a unit whose copy is missing or short, or released with online rows left
STORED = "tier_object"  # an object of no live manifest version
ROWS = "tier_rows"  # warm rows of no live manifest version
SCHEMA = "tier_schema"  # an older structure version that cannot be read
DISPOSAL = ("tier_unit：副本缺失或不全的单元读取答暂时不可用，从备份或副本恢复对象后重做核对（RC11）；仍有在线行的已释放单元由每天的"
            "销毁作业继续删除。tier_object、tier_rows：恢复点之后写下的或未完成的转层，读取只认清单所以不开放，转层作业重做、孤儿"
            "清理过了窗口删除（TR17）。tier_schema：登记那个结构版本的转换器")


@dataclass
class _Live:
    """A published or released unit as the check reads it."""

    route: str
    manifest: Manifest | None = None
    live: set[str] = field(default_factory=set)  # "<generation>.<version>" of its manifests whose data must be there
    versions: set[int] = field(default_factory=set)  # the same, by version (warm rows); -1 when thawed
    left_online: int = 0


def recovery_check(cat: Catalog) -> Check:
    """The catalog's check, registered by the host before the component's own."""
    return Check(NAME, "装配项目协议 11 的恢复时的核对、协议 15 的 RC11；be-protocol P15.36", lambda read: _check(cat, read))


async def _check(cat: Catalog, read: Read) -> Result:
    units: dict[str, _Live] = {}
    warm: dict[tuple[str, int], int] = {}  # (unit, version) -> rows

    async def body(tx: Tx) -> None:
        for r in await tx.fetch("""SELECT u.id::text AS id, u.state, u.route, u.manifest_version AS current,
                m.version, m.generation, m.superseded_at IS NULL AS live, m.manifest,
                EXISTS (SELECT 1 FROM be_tier_thaw t WHERE t.unit = u.id) AS thawed
                FROM be_tier_unit u LEFT JOIN be_tier_manifest m ON m.unit = u.id ORDER BY u.id, m.version"""):
            if r["state"] not in (PUBLISHED, RELEASED) or r["version"] is None:
                continue
            u = units.get(r["id"])
            if u is None:
                u = units[r["id"]] = _Live(r["route"])
                if r["thawed"]:
                    u.versions.add(-1)
            if r["version"] == r["current"]:
                try:
                    u.manifest = parse(bytes(r["manifest"]))
                except Exception as e:
                    raise ValueError(f"tiering: unit {r['id']}: its manifest: {e}") from e
            if r["live"] or r["version"] == r["current"]:
                u.live.add(f"{r['generation']}.{r['version']}")
                u.versions.add(r["version"])
        for r in await tx.fetch("SELECT unit::text, version, count(*) FROM be_tier_row GROUP BY unit, version"):
            warm[(r[0], r[1])] = r[2]
        for cl in cat.classes.values():  # released units with online rows left
            for r in await tx.fetch(f"""SELECT m.unit::text, count(*) FROM be_tier_member m JOIN be_tier_unit u
                    ON u.id = m.unit JOIN {ident(cl.root.name)} r ON r.{ident(cl.root.pk)}::text = m.root_id
                    WHERE u.state = 'released' AND u.class = $1 GROUP BY m.unit""", cl.name):
                if (u := units.get(r[0])) is not None:
                    u.left_online = r[1]

    await read(body)
    diffs: list[Difference] = []
    if cat.store is None and any(u.route == OBJECT for u in units.values()):
        raise RuntimeError("units on the object route and no object store configured (TIERING_STORE_URL)")
    objects: dict[str, int] = {}  # key -> size
    if cat.store is not None:
        prefix = cat.store.prefix + "/"
        objects = {o.key: o.size for o in await cat.store.list(prefix)
                   if not o.key.startswith(prefix + "exports/")}
    for uid, u in units.items():
        m = u.manifest
        if u.route == POSTGRES and m is not None:
            want = sum(t.rows for t in m.tables)
            if (got := warm.get((uid, m.version), 0)) != want:
                diffs.append(Difference(UNIT, uid, f"the warm route has {got} of the manifest's {want} rows "
                                                   f"(version {m.version})"))
        elif u.route == OBJECT and m is not None:
            missing = [o.key for t in m.tables for o in t.objects if objects.get(o.key) != o.bytes]
            if missing:
                diffs.append(Difference(UNIT, uid, "the manifest's objects missing or not their size: "
                                        + ", ".join(missing)))
        if u.left_online:
            diffs.append(Difference(UNIT, uid, f"released with {u.left_online} online roots left: the daily "
                                               "destruction job deletes them"))
    for key in objects:  # <prefix>/<class>/<unit>/<generation>.<version>/<file>
        parts = key.removeprefix(cat.store.prefix + "/").split("/")
        if len(parts) == 4 and (u := units.get(parts[1])) is not None and parts[2] in u.live:
            continue
        diffs.append(Difference(STORED, key, "of no live manifest version: an orphan or an unfinished transfer, "
                                             "never read"))
    for (uid, version), n in warm.items():
        if (u := units.get(uid)) is not None and version in u.versions:
            continue
        diffs.append(Difference(ROWS, f"{uid}/{version}", f"{n} warm rows of no live manifest version: never read"))
    for name in cat.classes:
        for version, e in (await reroute.check_readable(cat, name)).items():
            diffs.append(Difference(SCHEMA, f"{name}@{version}", f"an older structure cannot be read: {e}"))
    if not diffs:
        return Result(PASS)
    diffs.sort(key=lambda d: (d.object_type, d.object_id))
    return Result(DIFF, diffs, DISPOSAL)
