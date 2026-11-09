"""Erasure (P15.30, P15.31), the same cases as be-sdk-go's tiering/erase_integration_test.go: an archived order is
erased as a new manifest version on its unit's route, a held one deferred; an erasure in a sealed unit restarts its
transfer; an erasure after the recovery's point is replayed through the erase job; a hold set during an erasure
wins."""
import asyncio
import datetime as dt
import io
import uuid

import pytest

from besdk import obs
from besdk._gen.be.evidence.v1 import replay_pb2
from besdk.evidence import APPLIED, REFLECTED
from besdk.tiering import erase, holds, reads, release, transfer
from besdk.tiering.classes import OBJECT, POSTGRES
from besdk.work.runner import Runner
from tests import pgtest
from tests.test_tiering_destroy_integration import ALICE, QUIET, World, subject
from tests.test_tiering_release_integration import backup_at
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, store_for, with_route

pytestmark = pytest.mark.integration


class Erasable(World):
    """The orders' note is personal data; two orders with notes sealed in one unit (not transferred)."""

    async def __aenter__(self):
        await super().__aenter__()
        from dataclasses import replace

        self.cat.classes["sales.orders"] = replace(self.cat.classes["sales.orders"], erasable=("note",))
        self.orders = []
        for i in range(2):
            oid = await self.order(400)
            await self.a.execute("UPDATE orders SET note = $2 WHERE id = $1", uuid.UUID(oid),
                                 f"call me at +34 600 000 00{i}")
            self.orders.append(oid)
        groups = await self.cat.groupings("sales.orders")
        self.unit = (await self.cat.seal("sales.orders", groups[0])).unit
        self.logs = io.StringIO()
        self.cat.log = obs.new_logger(obs.Member("test/sales", "3.0.0"), "info", self.logs)
        return self


async def wait_for(cond, timeout: float = 20.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await cond():
        assert asyncio.get_running_loop().time() < deadline, "timed out"
        await asyncio.sleep(0.1)


@pytest.mark.parametrize("route", [POSTGRES, OBJECT])
async def test_an_archived_order_is_erased_as_a_new_version(route):  # TR15, P15.31
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Erasable("py_tier_erase_" + route.replace("-", "_")) as w:
            w.cat.store = st
            with_route(w, route)
            unit, orders = w.unit, w.orders
            await transfer.transfer(w.cat, unit)
            released = route == OBJECT
            if released:
                w.cat.grace = dt.timedelta(0)
                w.cat.backups = backup_at(dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=1))
                assert await release.release(w.cat, unit) == []
            await reads.thaw(w.cat, unit, dt.timedelta(hours=1))
            state, before, version = await w.unit_at(unit)

            assert await erase.erase(w.cat, "sales.orders", orders[0], "request ER-1") == erase.ERASED
            now_state, after, now_version = await w.unit_at(unit)
            assert now_state == state, "the state is unchanged"
            assert now_version == version + 1, "a new manifest version"
            assert after != before, "the content's digest is the erased one"
            erased = await reads.get(w.cat, "sales.orders", orders[0])
            assert erased is not None and "note" not in erased.root and erased.route == route
            kept = await reads.get(w.cat, "sales.orders", orders[1])
            assert kept.root["note"] == "call me at +34 600 000 001"
            if not released:
                assert await w.count("SELECT count(*) FROM orders WHERE id = $1 AND note IS NULL",
                                     uuid.UUID(orders[0])) == 1, "a published unit's online row is erased too"
            assert await w.count("SELECT count(*) FROM be_tier_thaw") == 0, "the thawed copy is dropped"
            assert w.kept.phases() == ["intent"]
            assert await w.outbox() == ["effective"]

            await holds.hold(w.cat, subject(ALICE), "sales.orders", holds.record_scope("sales.orders", orders[1]),
                             "legal", "a court order")
            assert await erase.erase(w.cat, "sales.orders", orders[1], "request ER-2") == erase.DEFERRED, "TR15"
            assert "tiering_erasure_deferred" in w.logs.getvalue()
            kept = await reads.get(w.cat, "sales.orders", orders[1])
            assert kept.root["note"] == "call me at +34 600 000 001", "kept while held"
            assert w.kept.phases() == ["intent"], "no intent for a deferred erasure"
    finally:
        await st.close()


async def test_an_erasure_in_a_sealed_unit_restarts_its_transfer():  # TR27, P15.31
    async with Erasable("py_tier_erase_sealed") as w:
        with_route(w, POSTGRES)
        unit, orders = w.unit, w.orders
        c = await transfer.claim(w.cat, unit)
        m = await transfer.copy(w.cat, c, POSTGRES)
        _, before, _ = await w.unit_at(unit)

        assert await erase.erase(w.cat, "sales.orders", orders[0], "request ER-3") == erase.ERASED
        state, after, _ = await w.unit_at(unit)
        assert state == "sealed" and after != before
        assert await w.count("SELECT count(*) FROM orders WHERE id = $1 AND note IS NULL", uuid.UUID(orders[0])) == 1
        with pytest.raises(transfer.StaleClaim):
            await transfer.publish(w.cat, c, m)  # the copy from before the erasure

        await transfer.transfer(w.cat, unit)  # the transfer starts again
        erased = await reads.get(w.cat, "sales.orders", orders[0])
        assert erased is not None and "note" not in erased.root


async def test_an_erasure_after_the_recovery_point_is_replayed():  # P15.30
    async with Erasable("py_tier_erase_replay") as w:
        with_route(w, POSTGRES)
        unit, orders = w.unit, w.orders
        await transfer.transfer(w.cat, unit)
        _, old, _ = await w.unit_at(unit)
        await erase.erase(w.cat, "sales.orders", orders[0], "request ER-4")
        effective = await w.effective()

        # back to before the erasure: version 1 with its digest, the note online, no history
        for sql in (f"UPDATE be_tier_unit SET manifest_version = 1, digest = '{old}'",
                    "DELETE FROM be_tier_manifest WHERE version = 2", "DELETE FROM be_change_history",
                    "DELETE FROM be_evidence_outbox"):
            await w.a.execute(sql)
        runner = Runner(w.d, "test/sales", QUIET)
        runner.add(erase.erase_job(w.cat, QUIET))
        w.cat.enqueue = runner.enqueue
        server = w.server()
        resp = await server.Replay(replay_pb2.ReplayRequest(record=effective), None)
        assert resp.outcome == APPLIED, resp.detail

        await runner.start()
        try:
            async def erased_again():
                return (await w.unit_at(unit))[2] == 2

            await wait_for(erased_again)
        finally:
            await runner.stop()
        erased = await reads.get(w.cat, "sales.orders", orders[0])
        assert "note" not in erased.root, "erased again"
        resp = await server.Replay(replay_pb2.ReplayRequest(record=effective), None)
        assert resp.outcome == REFLECTED


async def test_a_hold_set_during_an_erasure_wins():  # TR15, P15.31
    async with Erasable("py_tier_erase_race") as w:
        with_route(w, POSTGRES)
        unit, orders = w.unit, w.orders
        await transfer.transfer(w.cat, unit)
        _, _, version = await w.unit_at(unit)
        c = await pgtest.admin()
        try:
            await c.execute("SET search_path = py_tier_erase_race")
            tr = c.transaction()
            await tr.start()
            await c.execute("INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) VALUES "
                            "(gen_random_uuid(), 'record', $1, 'legal', $2, 'a court order')",
                            "sales.orders/" + orders[0], ALICE)
            task = asyncio.create_task(erase.erase(w.cat, "sales.orders", orders[0], "request ER-5"))
            await asyncio.sleep(0.5)
            assert not task.done(), "the erasure waits for the hold being set"
            await tr.commit()
            assert await task == erase.DEFERRED
        finally:
            await c.close()
        assert (await w.unit_at(unit))[2] == version, "no new version"
        assert await w.outbox() == ["cancelled"], "the intent is cancelled"
