"""Route changes, old versions and older structures (P15.33-P15.35, P15.39), the same cases as be-sdk-go's
tiering/reroute_integration_test.go: a released unit on the warm route moves to the object store when its class's
first route changes and its old version is released only past the grace, the leases and a backup; an erased version's
old objects are released; an older structure is read through its converter."""
import datetime as dt
import json
import uuid
from dataclasses import replace

import pytest

from besdk import errs
from besdk.tiering import erase, reads, reroute, transfer
from besdk.tiering.classes import OBJECT, POSTGRES
from besdk.tiering.plan import GRACE, LEASE, NO_BACKUP
from besdk.tiering.rows import TEXT
from besdk.work.runner import Runner
from tests.test_tiering_destroy_integration import QUIET, World
from tests.test_tiering_erase_integration import Erasable, wait_for
from tests.test_tiering_reads_integration import by_line, published
from tests.test_tiering_release_integration import backup_at
from tests.test_tiering_seal_integration import orders_class
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, store_for, with_route

pytestmark = pytest.mark.integration


async def objects_under(st, unit: str) -> int:
    return len(await st.list(f"{st.prefix}/sales.orders/{unit}/"))


def now(**delta) -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc) + dt.timedelta(**delta)


async def test_a_route_change_moves_a_released_unit():  # TR20, TR26, P15.35, P15.39
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with World("py_tier_reroute") as w:
            w.cat.store = st
            unit, roots, lines = await published(w, POSTGRES)
            _, _, version = await w.unit_at(unit)
            route_version = await w.count("SELECT route_version FROM be_tier_unit WHERE id = $1", uuid.UUID(unit))

            with_route(w, OBJECT)
            runner = Runner(w.d, "test/sales", QUIET)
            runner.add(transfer.transfer_job(w.cat, QUIET))
            await runner.start()
            try:
                async def moved():
                    return await w.a.fetchval("SELECT route FROM be_tier_unit WHERE id = $1", uuid.UUID(unit)) == OBJECT

                await wait_for(moved)  # the transfer job moves it
            finally:
                await runner.stop()

            state, _, now_version = await w.unit_at(unit)
            assert state == "released", "released still (TR26)"
            assert now_version == version + 1
            moved_version = await w.count("SELECT route_version FROM be_tier_unit WHERE id = $1", uuid.UUID(unit))
            assert moved_version > route_version, "the route version moves with the route"
            for oid in roots:
                agg = await reads.get(w.cat, "sales.orders", oid)
                assert agg.route == OBJECT
                assert agg.root == roots[oid], "read back as it was online"
                assert by_line(agg.children["order_lines"]) == by_line(lines[oid])
            assert await objects_under(st, unit) > 0
            old = "SELECT count(*) FROM be_tier_row WHERE unit = $1 AND version = $2"
            assert await w.count(old, uuid.UUID(unit), version) == 6, "the old route's rows are still there"

            w.cat.grace = dt.timedelta(days=7)
            assert GRACE in await reroute.release_old(w.cat, unit)
            w.cat.grace = dt.timedelta(0)
            await w.a.execute(f"INSERT INTO be_tier_lease VALUES ('{uuid.uuid7()}', {moved_version - 1}, 'export', "
                              "now() + interval '15 minutes')")
            assert await reroute.release_old(w.cat, unit) == [LEASE], "an export reading the old route"
            await w.a.execute("UPDATE be_tier_lease SET expires_at = now() - interval '1 second'")
            w.cat.backups = backup_at(now(hours=-1))
            assert await reroute.release_old(w.cat, unit) == [NO_BACKUP], \
                "a restore before the change still needs the old rows"
            w.cat.backups = backup_at(now(minutes=1))
            assert await reroute.release_old(w.cat, unit) == []
            assert await w.count(old, uuid.UUID(unit), version) == 0
            assert await w.count("SELECT count(*) FROM be_tier_manifest WHERE unit = $1 AND superseded_at IS NOT NULL",
                                 uuid.UUID(unit)) == 1
            assert await reroute.release_old(w.cat, unit) == [reroute.NOTHING_OLD]
            for oid, root in roots.items():
                assert (await reads.get(w.cat, "sales.orders", oid)).root == root, "the new route answers alone"
    finally:
        await st.close()


async def test_an_erased_versions_old_objects_are_released():  # P15.31, P15.39
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Erasable("py_tier_erase_old") as w:
            w.cat.store = st
            with_route(w, OBJECT)
            unit, orders = w.unit, w.orders
            await transfer.transfer(w.cat, unit)
            first = await objects_under(st, unit)
            await erase.erase(w.cat, "sales.orders", orders[0], "request ER-6")
            assert await objects_under(st, unit) == 2 * first, "both versions"
            w.cat.grace, w.cat.backups = dt.timedelta(0), backup_at(now(minutes=1))
            assert await reroute.release_old(w.cat, unit) == []
            assert await objects_under(st, unit) == first, "the old version's objects are gone"
            erased = await reads.get(w.cat, "sales.orders", orders[0])
            assert "note" not in erased.root
    finally:
        await st.close()


@pytest.mark.parametrize("route", [POSTGRES, OBJECT])
async def test_an_older_structure_is_read_through_its_converter(route):  # TR19, P15.33, P15.34
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Erasable("py_tier_schema_" + route.replace("-", "_")) as w:
            w.cat.store = st
            with_route(w, route)
            unit, orders = w.unit, w.orders
            await transfer.transfer(w.cat, unit)

            cl = w.cat.classes["sales.orders"]
            columns = {k: v for k, v in orders_class().root.columns.items() if k != "note"}
            columns["remark"] = TEXT
            w.cat.classes["sales.orders"] = replace(cl, schema_version="2", erasable=(),
                                                    root=replace(cl.root, columns=columns))

            with pytest.raises(errs.Error) as x:
                await reads.get(w.cat, "sales.orders", orders[0])
            assert x.value.reason == "QUERY_NOT_COMPLETABLE", "no converter"
            assert "1" in await reroute.check_readable(w.cat, "sales.orders")

            same = replace(w.cat, converters={})
            reroute.register(same, "sales.orders", "1", lambda table, row: row)
            with pytest.raises(Exception, match='note'):
                await reads.get(same, "sales.orders", orders[0])  # a converted row with a column the class no longer has

            def lose_locate(table, row):
                if table == "orders":  # renamed as it should be, but the locate column lost
                    row["remark"] = row.pop("note", None)
                    row.pop("legal_entity", None)
                return row

            bad = replace(w.cat, converters={})
            reroute.register(bad, "sales.orders", "1", lose_locate)
            with pytest.raises(Exception, match="legal_entity"):
                await reads.get(bad, "sales.orders", orders[0])  # a converted root without its locate column

            def rename(table, row):
                if table == "orders" and "note" in row:
                    row["remark"] = row.pop("note")
                return row

            reroute.register(w.cat, "sales.orders", "1", rename)
            agg = await reads.get(w.cat, "sales.orders", orders[0])
            assert agg.root["remark"] == "call me at +34 600 000 000"
            assert "note" not in agg.root
            assert await reroute.check_readable(w.cat, "sales.orders") == {}

            await reroute.reroute(w.cat, unit, route)  # written again: in the current structure
            raw = await w.a.fetchval("SELECT convert_from(m.manifest, 'UTF8') FROM be_tier_manifest m JOIN be_tier_unit u "
                                     "ON u.id = m.unit AND m.version = u.manifest_version WHERE u.id = $1",
                                     uuid.UUID(unit))
            assert json.loads(raw)["schema_version"] == "2"
            agg = await reads.get(w.cat, "sales.orders", orders[0])
            assert agg.root["remark"] == "call me at +34 600 000 000"
    finally:
        await st.close()
