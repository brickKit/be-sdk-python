"""Holds, approvals and destruction (P15.28-P15.30), the same cases as be-sdk-go's tiering/destroy_integration_test.go:
a destruction waits for its retention, an approval, no live hold and a retention that is not pending, then destroys
with its tombstone and two-phase evidence; a hold set during the decision wins; a destruction after the recovery's
point is replayed."""
import asyncio
import datetime as dt
import io
import json
import uuid
from dataclasses import replace

import pytest

from besdk import errs, obs
from besdk._gen.be.evidence.v1 import replay_pb2
from besdk.authz import Definitions, Operation, Resource, Segment, Subject
from besdk.authz.view import Grant
from besdk.evidence import APPLIED, REFLECTED, Replays, ReplayServer, Writer
from besdk.tiering import destroy, holds, reads
from besdk.tiering.classes import POSTGRES, Retention
from besdk.tiering.plan import APPROVAL, HELD, PENDING
from tests import pgtest
from tests.test_tiering_reads_integration import published
from tests.test_tiering_seal_integration import Env

pytestmark = pytest.mark.integration
ALICE = "0190f3a1-0000-7000-8000-0000000000a1"
BOB = "0190f3a1-0000-7000-8000-0000000000b2"
QUIET = obs.new_logger(obs.Member("test/sales", "3.0.0"), "error", io.StringIO())


class Keeper:
    """The evidence store: it keeps what it is sent."""

    def __init__(self):
        self.recs: list[dict] = []

    async def append(self, recs):
        self.recs += [json.loads(r.record) for r in recs]
        return ["accepted"] * len(recs)

    def phases(self) -> list[str]:
        return [r["phase"] for r in self.recs]


ARCHIVE_DEFS = Definitions("test/sales", [Resource("sales.archive", "sales.order.hold", {}, {})],
                           [Operation("sales.order.hold", "sales.archive", "manage", "low"),
                            Operation("sales.order.approve_destruction", "sales.archive", "manage", "low")])


class ArchiveGrants:
    """alice may set holds and approvals; bob may not."""

    def lookup_for(self, resource_type, principal, risk, tightening_only):
        segs = [Segment(op, {}, [], "2026-01-01T00:00:00Z", resource_type="sales.archive")
                for op in ("sales.order.hold", "sales.order.approve_destruction")] if principal == ALICE else []
        return Grant(segs, 1e12)


def subject(principal: str) -> Subject:
    ARCHIVE_DEFS.validate()
    return Subject(principal, ARCHIVE_DEFS, ArchiveGrants(), dt.datetime.now(dt.timezone.utc))  # type: ignore[arg-type]


class World(Env):
    """The orders' environment with an evidence store and the two subjects."""

    async def __aenter__(self):
        await super().__aenter__()
        self.kept = Keeper()
        self.cat.evidence = Writer(self.d, "test/sales", "test-1", QUIET, store=self.kept)
        self.alice, self.bob = subject(ALICE), subject(BOB)
        return self

    async def state(self, unit: str) -> str:
        return await self.a.fetchval("SELECT state FROM be_tier_unit WHERE id = $1", uuid.UUID(unit))

    async def unit_at(self, unit: str) -> tuple[str, str, int]:
        r = await self.a.fetchrow("SELECT state, digest, manifest_version FROM be_tier_unit WHERE id = $1",
                                  uuid.UUID(unit))
        return r["state"], r["digest"], r["manifest_version"]

    async def outbox(self) -> list[str]:
        return [r[0] for r in await self.a.fetch("SELECT phase FROM be_evidence_outbox ORDER BY phase")]

    async def count(self, sql: str, *args) -> int:
        return await self.a.fetchval(sql, *args)

    def server(self) -> ReplayServer:
        r = Replays()
        destroy.replays(self.cat, r)
        return ReplayServer(self.d, "test/sales", r, QUIET)

    async def effective(self) -> bytes:
        return bytes(await self.a.fetchval("SELECT record FROM be_evidence_outbox WHERE phase = 'effective'"))


def set_retention(w: World, **kw) -> None:
    cl = w.cat.classes["sales.orders"]
    w.cat.classes["sales.orders"] = replace(cl, retention=replace(cl.retention, **kw))


async def destroyable(w: World) -> str:
    """A released unit of two orders closed 400 days ago, on the warm route; the retention is a year."""
    set_retention(w, min_days=365)
    unit, _, _ = await published(w, POSTGRES)
    return unit


async def test_the_destruction_conditions():  # P15.28, P15.29, TR13, TR24
    async with World("py_tier_destroy") as w:
        unit = await destroyable(w)
        cl = w.cat.classes["sales.orders"]
        set_retention(w, min_days=3650)
        assert await destroy.destroy(w.cat, unit) == [destroy.NOT_DUE], "kept ten years"
        w.cat.classes["sales.orders"] = cl

        assert await destroy.destroy(w.cat, unit) == [APPROVAL]
        assert w.kept.phases() == [], "no intent while the decision cannot hold"

        with pytest.raises(errs.Error):
            await holds.approve(w.cat, w.bob, "sales.orders", holds.Scope(holds.CLASS, "sales.orders"), "x")
        standing = await holds.approve(w.cat, w.alice, "sales.orders", holds.Scope(holds.CLASS, "sales.orders"),
                                       "the commercial-law retention has run")
        await holds.withdraw(w.cat, w.alice, "sales.orders", standing)
        assert await destroy.destroy(w.cat, unit) == [APPROVAL], "a withdrawn approval does not count"
        await holds.approve(w.cat, w.alice, "sales.orders", holds.Scope(holds.UNIT, unit), "reviewed")

        root = await w.a.fetchval("SELECT root_id FROM be_tier_member WHERE unit = $1 LIMIT 1", uuid.UUID(unit))
        with pytest.raises(errs.Error):
            await holds.hold(w.cat, w.bob, "sales.orders", holds.record_scope("sales.orders", root), "legal", "x")
        hold = await holds.hold(w.cat, w.alice, "sales.orders", holds.record_scope("sales.orders", root), "legal",
                                "a court order")
        assert await destroy.destroy(w.cat, unit) == [HELD], "one of its orders is held (TR13)"
        for s in (holds.Scope(holds.UNIT, unit), holds.Scope(holds.CLASS, "sales.orders")):
            other = await holds.hold(w.cat, w.alice, "sales.orders", s, "dispute", "a dispute")
            await holds.lift_hold(w.cat, w.alice, "sales.orders", hold)
            assert await destroy.destroy(w.cat, unit) == [HELD], f"held by its {s.kind}"
            hold = other
        await holds.lift_hold(w.cat, w.alice, "sales.orders", hold)
        with pytest.raises(errs.Error) as x:
            await holds.lift_hold(w.cat, w.alice, "sales.orders", hold)
        assert x.value.reason == "NOT_FOUND", "lifted once"

        w.cat.classes["sales.orders"] = replace(cl, retention=Retention(pending=True))
        assert await destroy.destroy(w.cat, unit) == [PENDING], "no basis: never destroyed (TR24)"
        w.cat.classes["sales.orders"] = cl

        before = await w.count("SELECT version FROM be_tier_catalog")
        assert await destroy.destroy(w.cat, unit) == []
        assert await w.state(unit) == "destroyed"
        assert await w.count("SELECT version FROM be_tier_catalog") == before + 1
        tomb = json.loads(await w.a.fetchval("SELECT tombstone::text FROM be_tier_unit WHERE id = $1", uuid.UUID(unit)))
        assert tomb["unit"] == unit and tomb["basis"] == "test" and tomb["digest"]
        assert await w.count("SELECT count(*) FROM be_tier_row WHERE unit = $1", uuid.UUID(unit)) == 0, \
            "the warm rows are gone"
        assert await w.count("SELECT count(*) FROM be_tier_member WHERE unit = $1", uuid.UUID(unit)) == 2, \
            "the members stay"
        assert w.kept.phases() == ["intent"], "the intent before the decision"
        assert await w.outbox() == ["effective"], "the conclusion in it"
        with pytest.raises(errs.Error) as x:
            await reads.get(w.cat, "sales.orders", root)
        assert x.value.reason == "NOT_FOUND", "read by key: NOT_FOUND (P15.29)"


async def test_a_hold_set_during_the_decision_wins():  # TR14, P15.29
    async with World("py_tier_destroy_race") as w:
        unit = await destroyable(w)
        await holds.approve(w.cat, w.alice, "sales.orders", holds.Scope(holds.UNIT, unit), "reviewed")
        c = await pgtest.admin()
        try:
            await c.execute("SET search_path = py_tier_destroy_race")
            tr = c.transaction()
            await tr.start()
            await c.execute("INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) VALUES "
                            "(gen_random_uuid(), 'unit', $1, 'legal', $2, 'a court order')", unit, ALICE)
            task = asyncio.create_task(destroy.destroy(w.cat, unit))
            await asyncio.sleep(0.5)
            assert not task.done(), "the decision waits for the hold being set"
            await tr.commit()
            assert await task == [HELD]
        finally:
            await c.close()
        assert await w.state(unit) == "released"
        assert w.kept.phases() == ["intent"]
        assert await w.outbox() == ["cancelled"], "the intent is cancelled"


async def test_a_destruction_after_the_recovery_point_is_replayed():  # TR16, RC08, P15.30
    async with World("py_tier_destroy_replay") as w:
        unit = await destroyable(w)
        await holds.approve(w.cat, w.alice, "sales.orders", holds.Scope(holds.UNIT, unit), "reviewed")
        assert await destroy.destroy(w.cat, unit) == []
        effective = await w.effective()

        # the database goes back to before the destruction: the unit released, its rows there, no approval, no history
        for sql in ("UPDATE be_tier_unit SET state = 'released', destroyed_at = NULL, tombstone = NULL",
                    f"INSERT INTO be_tier_row VALUES ('{unit}', 1, 'orders', 'restored', '\\x00')",
                    "DELETE FROM be_tier_approval", "DELETE FROM be_change_history", "DELETE FROM be_evidence_outbox"):
            await w.a.execute(sql)
        server = w.server()
        resp = await server.Replay(replay_pb2.ReplayRequest(record=effective), None)
        assert resp.outcome == APPLIED, resp.detail
        assert await w.state(unit) == "destroyed"

        assert await destroy.destroy(w.cat, unit) == [], "the next destruction run deletes its data again"
        assert await w.count("SELECT count(*) FROM be_tier_row WHERE unit = $1", uuid.UUID(unit)) == 0, \
            "the restored rows are gone"
        resp = await server.Replay(replay_pb2.ReplayRequest(record=effective), None)
        assert resp.outcome == REFLECTED
