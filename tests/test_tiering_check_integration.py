"""The recovery check be.tiering (P15.36, RC11, TR17), the same case as be-sdk-go's tiering/check_integration_test.go:
a route's copy missing or short, objects and warm rows of no live version (exports aside), a released unit with
online rows (which the daily destruction job then deletes) and an older structure without a converter are
differences; object units without a store are unprovable."""
import uuid
from dataclasses import replace

import pytest

from besdk._gen.be.recovery.v1 import check_pb2
from besdk.recovery import DIFF, PASS, UNPROVABLE, Checks, CheckServer
from besdk.tiering import check, destroy
from besdk.tiering.classes import OBJECT, POSTGRES
from besdk.work.runner import Runner
from tests.test_tiering_destroy_integration import QUIET, World
from tests.test_tiering_erase_integration import wait_for
from tests.test_tiering_reads_integration import published
from tests.test_tiering_transfer_integration import BUCKET, admin, ensure_bucket, store_for

pytestmark = pytest.mark.integration


async def tiering_check(w: World):
    reg = Checks()
    reg.add(check.recovery_check(w.cat))
    resp = await CheckServer(w.d, False, reg).Check(check_pb2.CheckRequest(recovery="r-1"), None)
    for r in resp.results:
        if r.check == check.NAME:
            return r
    raise AssertionError("no be.tiering result")


def differences(r) -> dict[str, str]:
    out: dict[str, str] = {}
    for d in r.differences:
        k = f"{d.object_type} {d.object_id}"
        out[k] = out.get(k, "") + d.detail + "; "
    return out


async def test_the_tiering_recovery_check():
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with World("py_tier_check") as w:
            w.cat.store = st
            warm_unit, warm_roots, _ = await published(w, POSTGRES)
            w.le = str(uuid.uuid7())
            cold_unit, _, _ = await published(w, OBJECT)
            await st.put(st.prefix + "/exports/x.ndjson", b"{}\n")
            r = await tiering_check(w)
            assert r.outcome == PASS, list(r.differences)

            part = [o.key for o in await st.list(f"{st.prefix}/sales.orders/{cold_unit}/")
                    if o.key.endswith(".parquet")][-1]
            assert await admin("DELETE", BUCKET, part) == 204
            await w.a.execute("DELETE FROM be_tier_row WHERE ctid IN (SELECT ctid FROM be_tier_row WHERE unit = $1 "
                              "LIMIT 1)", uuid.UUID(warm_unit))
            orphan = f"{st.prefix}/sales.orders/{cold_unit}/99.9/orders-0.parquet"
            await st.put(orphan, b"x")
            await w.a.execute("INSERT INTO be_tier_row VALUES ($1, 7, 'orders', 'k', '\\x00')", uuid.UUID(warm_unit))
            root = next(iter(warm_roots))
            async with w.a.transaction():  # an online row of a released unit back (the barrier lets its own unit through)
                await w.a.execute("SELECT set_config('be.tier_unit', $1, true)", warm_unit)
                await w.a.execute("INSERT INTO orders VALUES ($1, $2, 'x', NULL, 1, 'closed', now(), now(), NULL)",
                                  uuid.UUID(root), uuid.uuid7())

            r = await tiering_check(w)
            assert r.outcome == DIFF
            diffs = differences(r)
            assert part in diffs["tier_unit " + cold_unit], "RC11: an object of the manifest missing"
            assert "the warm route has 5 of the manifest's 6 rows" in diffs["tier_unit " + warm_unit]
            assert "tier_object " + orphan in diffs, "TR17: an object of no manifest"
            assert f"tier_rows {warm_unit}/7" in diffs
            assert f"tier_object {st.prefix}/exports/x.ndjson" not in diffs, "exports are not the catalog's"
            assert any(d.object_id == warm_unit and d.detail == "released with 1 online roots left: the daily "
                       "destruction job deletes them" for d in r.differences), list(r.differences)
            assert r.disposal

            runner = Runner(w.d, "test/sales", QUIET)
            runner.add(destroy.destroy_job(w.cat, QUIET))
            await runner.start()
            try:
                async def gone():
                    return await w.count("SELECT count(*) FROM orders WHERE id = $1", uuid.UUID(root)) == 0

                await wait_for(gone)  # the daily destruction job deletes it
            finally:
                await runner.stop()

            cl = w.cat.classes["sales.orders"]
            w.cat.classes["sales.orders"] = replace(cl, schema_version="2")
            assert "tier_schema sales.orders@1" in differences(await tiering_check(w)), "P15.34"
            w.cat.classes["sales.orders"] = cl

            w.cat.store = None
            assert (await tiering_check(w)).outcome == UNPROVABLE, "object units and no store"
    finally:
        await st.close()
