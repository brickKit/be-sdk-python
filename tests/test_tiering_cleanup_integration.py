"""The daily cleanup (P15.20, P15.27), the same case as be-sdk-go's tiering/cleanup_integration_test.go: it removes
what an interrupted transfer left of every unit of the class at once (the objects no manifest names and the warm rows
of no manifest version) once past the window; the published copies, the rows of the version a transfer may be writing
and a live thaw stay."""
import datetime as dt
import uuid

import pytest

from besdk.tiering import store, transfer
from besdk.tiering.classes import OBJECT, POSTGRES
from besdk.work.runner import Runner
from tests.test_tiering_destroy_integration import QUIET
from tests.test_tiering_erase_integration import wait_for
from tests.test_tiering_seal_integration import Env
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, sealed_unit, store_for, with_route

pytestmark = pytest.mark.integration


async def test_the_daily_cleanup_removes_every_units_orphans():
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_cleanup") as e:
            e.cat.store = st
            with_route(e, OBJECT)
            # two units on the object store, each with the copy of a worker whose claim was taken over
            olds, currents = [], []
            for _ in range(2):
                e.le = str(uuid.uuid7())  # another legal entity: another grouping
                unit = await sealed_unit(e)
                old = await transfer.claim(e.cat, unit)
                old_copy = await transfer.copy(e.cat, old, OBJECT)
                current = await transfer.claim(e.cat, unit)
                m = await transfer.copy(e.cat, current, OBJECT)
                await transfer.verify(e.cat, m)
                await transfer.publish(e.cat, current, m)
                olds.append(old_copy)
                currents.append(m)
            # a third on the warm route: an orphan row of a version no manifest names, and a row of the next version
            # (a transfer writing it now)
            with_route(e, POSTGRES)
            e.le = str(uuid.uuid7())
            warm = await sealed_unit(e)
            await transfer.transfer(e.cat, warm)
            await e.a.execute(f"INSERT INTO be_tier_row VALUES ('{warm}', 7, 'orders', 'orphan', '\\x00')")
            await e.a.execute(f"INSERT INTO be_tier_row VALUES ('{warm}', 2, 'orders', 'writing', '\\x00')")

            assert await transfer.clean_orphans(e.cat, "sales.orders", olds[0].unit, dt.timedelta(hours=1)) == 0, \
                "younger than the window: not yet"

            e.cat.orphan_age = dt.timedelta(microseconds=1)
            runner = Runner(e.d, "test/sales", QUIET)
            runner.add(transfer.cleanup_job(e.cat, QUIET))
            await runner.start()
            try:
                async def ran():
                    return await e.a.fetchval("SELECT count(*) FROM be_tier_row WHERE pk = 'orphan'") == 0

                await wait_for(ran)  # the daily job ran
            finally:
                await runner.stop()

            for i, old in enumerate(olds):
                for o in old.tables[0].objects:
                    with pytest.raises(store.Missing):
                        await st.get(o.key)  # the old worker's part
                await transfer.verify(e.cat, currents[i])  # the published copy stays
            assert await e.a.fetchval("SELECT count(*) FROM be_tier_row WHERE pk = 'writing'") == 1, \
                "the next version stays"
            assert await e.a.fetchval("SELECT count(*) FROM be_tier_row WHERE unit = $1 AND version = 1",
                                      uuid.UUID(warm)) > 0, "the published warm rows stay"
    finally:
        await st.close()
