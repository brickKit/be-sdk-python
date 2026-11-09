"""Reading a class's history (P15.26, P15.38), the same cases as be-sdk-go's tiering/history_integration_test.go: a
history read is the online and the cold orders once each under the caller's authorization now; past the limit it
becomes an export written by the queue job, read only by its requester, deleted when it expires; with nowhere to
export it is not completable; an export failing on every attempt is recorded failed."""
import asyncio
import datetime as dt
import io
import json
import uuid
from dataclasses import replace

import aiohttp
import pytest

from besdk import canon, errs, obs
from besdk.authz import Definitions, Dimension, Fact, Field, Operation, Resource, Segment, Subject, mask
from besdk.authz.segment import Tri
from besdk.authz.view import Grant
from besdk.tiering import history, reads, store
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import OBJECT
from besdk.work.runner import Runner
from tests.test_tiering_erase_integration import wait_for
from tests.test_tiering_reads_integration import published
from tests.test_tiering_seal_integration import Env, orders_class
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, store_for

pytestmark = pytest.mark.integration
ALICE = "0190f3a1-0000-7000-8000-0000000000a1"
BOB = "0190f3a1-0000-7000-8000-0000000000b2"
QUIET = obs.new_logger(obs.Member("test/sales", "3.0.0"), "error", io.StringIO())

ORDER = Resource("sales.order", "sales.order.read", {"legal_entity": Dimension("value_set", "legal_entity")},
                 {n: Field("ordinary") for n in ("id", "code", "total", "lines")})
ORDER_DEFS = Definitions("test/sales", [ORDER], [Operation("sales.order.read", "sales.order", "read", "low")])


class Grants:
    """The authorization member's answer as a view: alice's segment over the entities, changed by the test (a
    revocation)."""

    def __init__(self):
        self.entities: list[str] = []

    def lookup_for(self, resource_type, principal, risk, tightening_only):
        if principal != ALICE:
            return Grant([], 1e12)
        return Grant([Segment.from_dict({
            "resource_type": "sales.order", "operation": "sales.order.read",
            "atoms": {"legal_entity": {"kind": "in", "values": list(self.entities)}},
            "read_fields": ["id", "code", "lines"], "valid": {"from": "2026-01-01T00:00:00Z"}})], 1e12)


def facts(row: dict) -> dict:
    return {"legal_entity": Fact.of(str(row["legal_entity"]))}


def shape(oid, code, total, lines, d) -> dict:
    return mask(ORDER, {"id": oid, "code": code, "total": total, "lines": lines}, d.fields)


def order_reader(w) -> history.Reader:
    """The component's reader: online orders under the snapshot, cold ones as the SDK hands them. w.slow makes the
    online part take longer than a short limit; negative, the shaping fails."""
    cl = orders_class()

    def output(a, d):
        if w.slow < 0:  # a defect in the component's shaping
            raise RuntimeError("cannot shape the order")
        return shape(str(a.root["id"]), str(a.root["code"]), str(a.root["total"]),
                     len(a.children.get("order_lines", [])), d)

    async def online(tx, s, sub, params, emit):
        if w.slow > 0:
            await asyncio.sleep(w.slow)
        decide = sub.decider("sales.order.read")
        for r in await tx.fetch("SELECT r.id::text, r.code, r.total::text, r.legal_entity::text, (SELECT count(*) FROM "
                                "order_lines l WHERE l.order_id = r.id) FROM orders r WHERE "
                                + s.online_exclusion(cl, "r")):
            d = decide(facts({"legal_entity": r[3]}))
            if d.visible is not Tri.TRUE:
                continue
            emit(shape(r[0], r[1], canon.decimal(r[2]), r[4], d))

    return history.Reader("sales.order.read", facts, output, online)


class World(Env):
    """Three legal entities, each with an open order online and a released unit of two closed orders in the object
    store; alice may read the first two entities' orders, their id, code and lines (the total masked)."""

    async def __aenter__(self):
        await super().__aenter__()
        self.slow = 0.0
        self.g = Grants()
        self.st = store_for()
        self.cat.store = self.st
        self.les = []
        for _ in range(3):
            self.le = str(uuid.uuid7())
            self.les.append(self.le)
            await self.order(-1)
            await published(self, OBJECT)
        self.g.entities = self.les[:2]
        ORDER_DEFS.validate()
        self.sub = self.subject(ALICE)
        self.cat.subject = self.subject
        history.register(self.cat, "sales.orders", order_reader(self))
        return self

    async def __aexit__(self, *exc):
        await self.st.close()
        await super().__aexit__(*exc)

    def subject(self, principal: str) -> Subject:
        return Subject(principal, ORDER_DEFS, self.g, dt.datetime.now(dt.timezone.utc))  # type: ignore[arg-type]

    async def finished(self, eid: str, timeout: float = 30.0) -> history.ExportStatus:
        got = {}

        async def done():
            try:
                got["st"] = await history.export_status(self.cat, eid, ALICE)
            except errs.Error:
                return False
            return got["st"].state != "pending"

        await wait_for(done, timeout)
        return got["st"]


def line(o: dict) -> str:
    return json.dumps(o, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


async def world(name: str) -> World:
    await ensure_bucket(BUCKET, False)
    return World(name)


async def test_a_history_read_is_complete_under_the_current_authorization():  # TR12, G4, P15.26
    async with await world("py_tier_history") as w:
        out, pending = await history.history(w.cat, w.sub, "sales.orders")
        assert pending is None
        assert len(out) == 6, "two entities: an open order and two archived ones each"
        assert len({o["id"] for o in out}) == 6
        for o in out:
            assert o["total"] is None and o["_masked"] == ["total"]
            assert o["lines"] == 2, "the lines read with the order"
        assert await w.a.fetchval("SELECT count(*) FROM be_tier_lease") == 0, "the read's lease ends with it"

        w.g.entities = [w.les[0]]
        out, _ = await history.history(w.cat, w.sub, "sales.orders")
        assert len(out) == 3, "the revoked entity's orders are not read any more"

        # the request's parameters select among the cold aggregates too (the reader's match)
        r = replace(order_reader(w), match=lambda root, params: params.get("lines", 0) == 0)
        history.register(w.cat, "sales.orders", r)
        out, _ = await history.history(w.cat, w.sub, "sales.orders", {"lines": 2})
        assert len(out) == 1, "the cold ones not matched: the online order only"


async def test_a_history_past_the_limit_becomes_an_export():  # TR11, TR12, P15.38
    async with await world("py_tier_export") as w:
        runner = Runner(w.d, "test/sales", QUIET)
        runner.add(history.export_job(w.cat, QUIET))
        w.cat.enqueue = runner.enqueue
        w.cat.cold_sync_limit, w.slow = 0.2, 1.0
        out, pending = await history.history(w.cat, w.sub, "sales.orders", {"history": True})
        assert pending is not None, "past the limit"
        assert out == [], "never a part"
        assert (await history.export_status(w.cat, pending.export, ALICE)).state == "pending"
        with pytest.raises(errs.Error) as x:
            await history.export_status(w.cat, pending.export, BOB)
        assert x.value.reason == "NOT_FOUND", "another principal"

        w.g.entities = [w.les[1]]  # before the job runs: the export applies the authorization then
        w.slow = 0
        await runner.start()
        try:
            st = await w.finished(pending.export)
        finally:
            await runner.stop()
        assert st.state == "done", st.reason
        assert st.rows == 3 and st.url

        async with aiohttp.ClientSession() as c, c.get(st.url) as resp:
            body = await resp.read()
            assert resp.status == 200, body
        sync, _ = await history.history(w.cat, w.sub, "sales.orders")
        assert sorted(body.decode().removesuffix("\n").split("\n")) == sorted(line(o) for o in sync), \
            "the export is what a complete read answers"

        await w.a.execute("UPDATE be_tier_export SET expires_at = now() - interval '1 second'")
        assert await history.clean_exports(w.cat) == 1
        with pytest.raises(store.Missing):
            await w.st.get(history.export_key(w.cat, pending.export))  # the object is gone
        with pytest.raises(errs.Error) as x:
            await history.export_status(w.cat, pending.export, ALICE)
        assert x.value.reason == "NOT_FOUND"


@pytest.mark.parametrize("name", ["no store", "locked", "no exports", "no reader", "the default"])
async def test_a_history_that_cannot_be_exported_is_not_completable(name):  # TR11, P15.38
    async with await world("py_tier_noexport_" + name.replace(" ", "_")) as w:
        async def enqueue(tx, job, args):
            return ""

        w.slow = 1.0
        c = Catalog(w.d, {"sales.orders": orders_class()}, store=w.st, enqueue=enqueue, subject=w.cat.subject,
                    cold_sync_limit=0.1)
        history.register(c, "sales.orders", order_reader(w))
        read = "sales.orders"
        if name == "no store":
            c.store = None
        elif name == "locked":
            c.store_locked = True
        elif name == "no exports":
            c.enqueue = None
        elif name == "no reader":
            c.classes["other"] = orders_class()
            read = "other"
        if name == "the default":
            out, pending = await history.history(c, w.sub, read)
            assert out == [] and pending is not None, "the same read with a store becomes an export"
            return
        with pytest.raises(errs.Error) as x:
            await history.history(c, w.sub, read)
        assert x.value.reason == "QUERY_NOT_COMPLETABLE", x.value


async def test_an_export_that_keeps_failing_is_recorded_failed():  # P15.38
    async with await world("py_tier_export_fail") as w:
        runner = Runner(w.d, "test/sales", QUIET)
        runner.add(replace(history.export_job(w.cat, QUIET), max_attempts=2))
        w.cat.enqueue = runner.enqueue
        eid = await history.start_export(w.cat, ALICE, "sales.orders")
        w.slow = -1
        await runner.start()
        try:
            st = await w.finished(eid, 60.0)
        finally:
            await runner.stop()
        assert st.state == "failed" and st.reason == "INTERNAL" and not st.url
