"""The reads (P15.22-P15.27), the same cases as be-sdk-go's tiering/reads_integration_test.go: a released order read by
its key comes back from either route exactly as it was online; a missing object is unavailable, not absent; a list
under a snapshot reads each order once, filtered and masked by the current authorization; a thawed unit reads from its
copy and stays sealed."""
import datetime as dt
import uuid

import pytest

from besdk import errs
from besdk.tiering import reads, release, transfer
from besdk.tiering.classes import OBJECT, POSTGRES, Table
from besdk.tiering.rows import JSONB, NUMERIC
from besdk.tiering import rows as canonical
from tests.test_tiering_release_integration import backup_at
from tests.test_tiering_seal_integration import Env, orders_class
from tests.test_tiering_transfer_integration import BUCKET, admin, ensure_bucket, sealed_unit, store_for, with_route

pytestmark = pytest.mark.integration


async def online_canonical(e: Env, oid: str) -> tuple[dict, list[dict]]:
    """One order and its lines read online as canonical rows (the reference every route must match)."""
    c = orders_class()

    async def read(t: Table, where: str) -> list[dict]:
        names = sorted(t.columns)
        exprs = [f"t.{n}::text AS {n}" if t.columns[n] in (NUMERIC, JSONB) else f"t.{n}" for n in names]
        got = await e.a.fetch(f"SELECT {', '.join(exprs)} FROM {t.name} t WHERE {where}", uuid.UUID(oid))
        return [canonical.row(t.columns, dict(r)) for r in got]

    roots = await read(c.root, "t.id = $1")
    assert len(roots) == 1
    return roots[0], await read(c.children[0].table, "t.order_id = $1")


def by_line(ls: list[dict]) -> list[dict]:
    return sorted(ls, key=lambda r: str(r["line"]))


async def published(e: Env, route: str) -> tuple[str, dict, dict]:
    """A sealed unit of two orders moved to a route and its online rows released; the unit and the orders' canonical
    rows as they were online."""
    with_route(e, route)
    a, b = await e.order(400), await e.order(400)
    roots, lines = {}, {}
    for oid in (a, b):
        roots[oid], lines[oid] = await online_canonical(e, oid)
    groups = await e.cat.groupings("sales.orders")
    res = await e.cat.seal("sales.orders", groups[0])
    await transfer.transfer(e.cat, res.unit)
    e.cat.grace, e.cat.backups = dt.timedelta(0), backup_at(dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1))
    assert await release.release(e.cat, res.unit) == []
    return res.unit, roots, lines


async def delete_all(st) -> None:
    for o in await st.list(st.prefix + "/"):
        assert await admin("DELETE", BUCKET, o.key) == 204


@pytest.mark.parametrize("route", [POSTGRES, OBJECT])
async def test_reads_by_key_from_every_route(route):  # TR08, G4
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_get_" + route.replace("-", "_")) as e:
            e.cat.store = st
            open_ = await e.order(-1)
            _, roots, lines = await published(e, route)
            for oid in roots:
                agg = await reads.get(e.cat, "sales.orders", oid)
                assert agg is not None
                assert agg.root == roots[oid]
                assert by_line(agg.children["order_lines"]) == by_line(lines[oid])
                assert agg.route == route
            assert await reads.get(e.cat, "sales.orders", open_) is None, "still online: the caller reads it"
    finally:
        await st.close()


async def test_a_missing_object_is_unavailable_not_absent():  # TR09
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_missing") as e:
            e.cat.store = st
            _, roots, _ = await published(e, OBJECT)
            await delete_all(st)
            for oid in roots:
                with pytest.raises(errs.Error) as x:
                    await reads.get(e.cat, "sales.orders", oid)
                assert x.value.reason == "DEPENDENCY_UNAVAILABLE", x.value
    finally:
        await st.close()


async def test_a_list_under_a_snapshot_reads_each_order_once():  # TR07, TR12, P15.24-P15.26
    async with Env("py_tier_list") as e:
        with_route(e, POSTGRES)
        e.cat.grace = dt.timedelta(0)
        e.cat.backups = backup_at(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1))
        first = await sealed_unit(e)
        await transfer.transfer(e.cat, first)  # published before the snapshot, online rows still there

        s = await reads.snapshot(e.cat, "test list")
        e.le = str(uuid.uuid7())  # another legal entity: its unit published after the snapshot
        second = await sealed_unit(e)
        await transfer.transfer(e.cat, second)
        assert await release.release(e.cat, second) == ["lease"], "the snapshot's lease keeps its online rows"

        cl = orders_class()
        seen: dict[str, int] = {}
        got = await e.d.tx(lambda tx: tx.fetch(
            "SELECT r.id::text FROM orders r WHERE r.status = 'closed' AND " + s.online_exclusion(cl, "r")))
        for r in got:
            seen[r[0]] = seen.get(r[0], 0) + 1
        units = await reads.cold_units(e.cat, s, "sales.orders")
        assert units == [first]
        for u in units:
            for a in await reads.read_unit(e.cat, s, u):
                seen[str(a.root["id"])] = seen.get(str(a.root["id"]), 0) + 1
        assert len(seen) == 4, "two units of two orders"
        assert all(n == 1 for n in seen.values()), seen

        firsts = await reads.read_unit(e.cat, s, first)
        assert len(firsts) == 2
        hidden = str(firsts[0].root["id"])

        async def visible(root):
            return str(root["id"]) != hidden, ["id", "code"]

        aggs = await reads.read_unit(e.cat, s, first, visible)
        assert len(aggs) == 1, "the hidden order is left out"
        assert str(aggs[0].root["id"]) != hidden
        assert len(aggs[0].root) == 2, "masked to the readable fields"

        await e.a.execute("UPDATE be_tier_lease SET expires_at = now() - interval '1 second' WHERE id = $1",
                          uuid.UUID(s.lease))
        with pytest.raises(errs.Error) as x:
            await reads.read_unit(e.cat, s, first)
        assert x.value.reason == "QUERY_PLAN_CHANGED", x.value


async def test_a_thawed_unit_reads_from_its_copy():  # TR18
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_thaw") as e:
            e.cat.store = st
            unit, roots, _ = await published(e, OBJECT)
            await reads.thaw(e.cat, unit, dt.timedelta(days=30))
            await delete_all(st)
            for oid, root in roots.items():
                agg = await reads.get(e.cat, "sales.orders", oid)
                assert agg is not None and agg.root == root
                with pytest.raises(errs.Error) as x:
                    await e.write("INSERT INTO orders VALUES ($1, $2, 'x', NULL, 1, 'closed', now(), now(), NULL)",
                                  uuid.UUID(oid), uuid.UUID(e.le))
                assert x.value.reason == "RECORD_SEALED", "a thaw reopens no write"
            assert await transfer.clean_orphans(e.cat, "sales.orders", unit, dt.timedelta(hours=1)) == 0, \
                "a live thaw is not an orphan"
    finally:
        await st.close()
