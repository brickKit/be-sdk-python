"""What the long simulation found (P15.19, P15.22, P15.24, P15.29, P15.31, P15.39), the same cases as be-sdk-go's
tiering/moved_integration_test.go: a release reads the latest backup outside its transaction; an erasure whose unit
another writer claims meanwhile starts again; a destroyed record is gone from the decision on.

A crash point's hook is synchronous: what another member does at that point runs on a thread of its own, with its
own event loop and connections, while the member under test waits at the point."""
import asyncio
import datetime as dt
import threading
import uuid

import pytest

from besdk import crash, errs
from besdk.tiering import destroy, erase, holds, reads, release, reroute, transfer
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import OBJECT, POSTGRES
from tests import pgtest
from tests.test_db_integration import open_db, values
from tests.test_tiering_destroy_integration import World, set_retention
from tests.test_tiering_erase_integration import Erasable
from tests.test_tiering_seal_integration import Env
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, sealed_unit, store_for, with_route

pytestmark = pytest.mark.integration


def elsewhere(fn) -> None:
    """Runs another member's coroutine function to its end on a thread of its own; its failure is raised here."""
    out: list[BaseException] = []

    def run() -> None:
        try:
            asyncio.run(fn())
        except BaseException as e:  # noqa: BLE001 - raised in the caller
            out.append(e)

    t = threading.Thread(target=run)
    t.start()
    t.join()
    if out:
        raise out[0]


@pytest.fixture
def hook():
    yield crash.set_hook
    crash.set_hook(None)


async def test_a_release_reads_the_backup_outside_its_transaction():  # P15.19, P15.39
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_backup_outside") as e:
            e.cat.store = st
            with_route(e, OBJECT)
            unit = await sealed_unit(e)
            await transfer.transfer(e.cat, unit)
            e.cat.grace = dt.timedelta(0)

            async def backups():  # the run profile in a deployment; here the database, refused inside a transaction
                return await e.d.background_tx(lambda tx: tx.fetchval("SELECT now() + interval '1 hour'"))

            e.cat.backups = backups
            assert await release.release(e.cat, unit) == []
            await reroute.reroute(e.cat, unit, POSTGRES)
            assert await reroute.release_old(e.cat, unit) == [], \
                "the old version's release reads it outside its transaction too"
    finally:
        await st.close()


async def test_an_erasure_starts_again_when_the_unit_moves_on(hook):  # P15.31
    async with Erasable("py_tier_erase_moved") as w:
        with_route(w, POSTGRES)
        unit, orders = w.unit, w.orders
        await transfer.transfer(w.cat, unit)
        won = []

        async def claim_again():  # another writer takes the unit (a route change, a transfer, another erasure)
            a = await pgtest.admin()
            try:
                await a.execute("SET search_path = py_tier_erase_moved")
                await a.execute("UPDATE be_tier_unit SET generation = generation + 1 WHERE id = $1", uuid.UUID(unit))
            finally:
                await a.close()

        def at(point: str) -> None:
            if point == "tiering.claim/done" and not won:  # the erasure has its claim
                won.append(True)
                elsewhere(claim_again)

        hook(at)
        assert await erase.erase(w.cat, "sales.orders", orders[0], "a data subject's request") == erase.ERASED
        hook(None)
        assert won, "another writer won the unit during the erasure"
        a = await reads.get(w.cat, "sales.orders", orders[0])
        assert a is not None and "note" not in a.root, "erased after starting again"


async def test_a_destroyed_record_is_gone_from_the_decision_on(hook):  # P15.22, P15.24, P15.29
    name = "py_tier_destroy_window"
    async with World(name) as w:
        set_retention(w, min_days=365)
        with_route(w, POSTGRES)
        unit = await sealed_unit(w)  # sealed: its online rows are all there
        await holds.approve(w.cat, w.alice, "sales.orders", holds.Scope(holds.UNIT, unit), "reviewed")
        root = await w.a.fetchval("SELECT root_id FROM be_tier_member WHERE unit = $1 LIMIT 1", uuid.UUID(unit))
        cl = w.cat.classes["sales.orders"]
        seen: dict[str, object] = {}

        async def look():  # another request of the member, while the destruction deletes its rows
            d, pool = await open_db(name, values(name))
            try:
                cat = Catalog(d, {cl.name: cl})
                seen["rows"] = await d.tx(lambda tx: tx.fetchval("SELECT count(*) FROM orders WHERE id = $1",
                                                                 uuid.UUID(root)))
                try:
                    seen["get"] = await reads.get(cat, "sales.orders", root)
                except errs.Error as e:
                    seen["get"] = e.reason
                s = await reads.snapshot(cat, "a list")
                seen["listed"] = await d.tx(lambda tx: tx.fetchval(
                    "SELECT count(*) FROM orders r WHERE r.id = $1 AND " + s.online_exclusion(cl, "r"), uuid.UUID(root)))
                await reads.end(cat, s)
            finally:
                await pool.close()

        def at(point: str) -> None:
            if point == "tiering.destroy.decide/done" and not seen:
                elsewhere(look)

        hook(at)
        assert await destroy.destroy(w.cat, unit) == []
        hook(None)
        assert seen, "the decision was taken"
        assert seen["rows"] == 1, "its rows not deleted yet"
        assert seen["get"] == "NOT_FOUND", "by key: NOT_FOUND from the decision on"
        assert seen["listed"] == 0, "an online list leaves it out"
        assert await w.count("SELECT count(*) FROM orders WHERE id = $1", uuid.UUID(root)) == 0, "then its rows go"
