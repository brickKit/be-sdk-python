"""The recorder's side and the owner's replay of the evidence (P12.15-P12.23), the same cases as be-sdk-go's
evidence/evidence_integration_test.go and replay_integration_test.go: a tightening in two phases; an unreachable
store does not hold it back (the alert channel, then the double failure and its own record); a cancelled conclusion
and a refused record; the commit sequence follows the commit order; the gRPC store against the test deployment's
evidence store; the replay re-executes, answers reflected, dangling, unsupported, deferred, and checks what lies
outside the recovery group."""
import asyncio
import io
import json
import time
from datetime import datetime, timezone

import pytest

from besdk import errs, obs, reqctx
from besdk._gen.be.evidence.v1 import replay_pb2
from besdk.calls.client import Member as Outbound
from besdk.db.pool import in_tx
from besdk.evidence import (APPLIED, CONSISTENT, DANGLING, DEFERRED, DOUBLE_FAILURE, INCONSISTENT, INTENT_TIMEOUT,
                            REFLECTED, UNSUPPORTED, Dangling, Deferred, Object, Replays, ReplayServer, Writer, reflected,
                            seal)
from besdk.evidence.transfer import GRPCStore, transfer
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
BOB = "0190f3a1-0000-7000-8000-0000000000b2"
ORDER = Object("demo.order", "0190f3a1-0000-7000-8000-0000000000f1", "7")
TABLES = {"0001_orders.up.sql": "CREATE TABLE orders (id text PRIMARY KEY, state text NOT NULL); "
                                "CREATE TABLE touched (id text);"}


def logger():
    out = io.StringIO()
    return obs.new_logger(obs.Member("demo/orders", "3.0.0"), "info", out), out


class StubStore:
    def __init__(self):
        self.got, self.delay, self.down, self.reply = [], 0.0, False, None

    async def append(self, recs):
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.down:
            raise ConnectionError("the evidence store is down")
        self.got += recs
        return [self.reply(s) if self.reply else "accepted" for s in recs]


class StubAlert:
    def __init__(self):
        self.got, self.fail = [], False

    async def alert(self, body):
        if self.fail:
            return False
        self.got.append(body)
        return True


async def schema(name, files=None):
    v, _ = await migrated(name, files or TABLES)
    return await open_db(name, v)


def fields(s):
    return json.loads(s.record)


async def test_a_tightening_is_written_in_two_phases():  # P12.15-P12.18
    d, pool = await schema("py_evd1")
    a = await pgtest.admin()
    try:
        store, (log, _) = StubStore(), logger()
        w = Writer(d, "demo/orders", "orders-1", log, store=store)
        p = await w.intent("security_tightening", BOB, ORDER, {"termination": "revoke"})
        assert p.went == "store" and len(store.got) == 1
        i = fields(store.got[0])
        assert i["phase"] == "intent" and "position" not in i, "an intent has no position (vector position-by-phase)"
        assert i["recorder"] == {"component": "demo/orders", "instance": "orders-1"}
        await d.tx(p.effective)
        h = await a.fetchrow("SELECT epoch, commit_seq FROM py_evd1.be_change_history WHERE evidence_id = $1", p.id)
        assert h["commit_seq"] == 1
        assert await d.tx(lambda tx: reflected(tx, p.id)), "reflected by its identity"
        assert not await d.tx(lambda tx: reflected(tx, "0190f3a1-0000-7000-8000-00000000dead"))
        assert await transfer(d, store, log) == 1 and len(store.got) == 2
        e = fields(store.got[1])
        assert e["phase"] == "effective" and e["id"] == p.id
        assert e["position"] == {"epoch": h["epoch"], "commit_seq": h["commit_seq"]}, "the commit position"
        assert await a.fetchval("SELECT count(*) FROM py_evd1.be_evidence_outbox") == 0, "deleted after the receipt"
    finally:
        await a.close()
        await pool.close()


async def test_an_unreachable_store_does_not_hold_a_tightening_back():  # P12.16, P12.23, RC17, RC22
    d, pool = await schema("py_evd2")
    try:
        store, alert, (log, out) = StubStore(), StubAlert(), logger()
        store.delay = 5.0
        w = Writer(d, "demo/orders", "orders-1", log, store=store, alert=alert)
        start = time.monotonic()
        p = await w.intent("security_tightening", BOB, ORDER, {"termination": "revoke"})
        assert time.monotonic() - start < INTENT_TIMEOUT + 1, "no longer than the time limit"
        assert p.went == "alert" and alert.got[0]["phase"] == "intent"
        await d.tx(p.effective)

        alert.fail, store.delay, store.down = True, 0.0, True
        p = await w.intent("security_tightening", BOB, ORDER, {"termination": "narrow"})
        assert p.went == "lost"
        assert '"event": "evidence_double_failure"' in out.getvalue() or '"event":"evidence_double_failure"' in out.getvalue()
        await d.tx(p.effective)  # the tightening goes on
        with pytest.raises(ConnectionError):
            await transfer(d, store, log)  # the store is down: the conclusions stay local

        store.down = False
        await transfer(d, store, log)
        lost = [fields(s) for s in store.got if fields(s)["category"] == DOUBLE_FAILURE]
        assert len(lost) == 1, "the double failure's record reached the store (P12.23)"
        lost = lost[0]
        assert lost["phase"] == "general" and lost["subject"] == BOB
        assert lost["change"]["record"] == p.id and lost["change"]["category"] == "security_tightening"
        assert lost["change"]["at"] and "termination" not in lost["change"], "not the lost record's change"
    finally:
        await pool.close()


async def test_a_cancelled_conclusion_and_a_refused_record():  # P12.9, P12.15, P12.16
    d, pool = await schema("py_evd3")
    a = await pgtest.admin()
    try:
        store, (log, out) = StubStore(), logger()
        w = Writer(d, "demo/orders", "orders-1", log, store=store)
        p = await w.intent("security_tightening", BOB, ORDER, {"termination": "revoke"})

        async def failing(tx):
            await p.effective(tx)
            raise errs.new("REQUEST_INVALID", "a check failed")
        with pytest.raises(errs.Error):
            await d.tx(failing)
        await p.cancelled()
        assert await a.fetchval("SELECT count(*) FROM py_evd3.be_change_history") == 0, "rolled back with the change"
        store.reply = lambda s: "conflict" if s.phase == "cancelled" else "accepted"
        await d.tx(lambda tx: w.general(tx, "security_widening", BOB, ORDER, {"grant": "g1"}))
        assert await transfer(d, store, log) == 2
        cancelled = [fields(s) for s in store.got if s.phase == "cancelled"]
        assert len(cancelled) == 1 and "position" not in cancelled[0], "a cancelled conclusion never committed"
        general = [fields(s) for s in store.got if s.phase == "general"]
        assert "position" in general[0], "a general record carries its commit position (vector position-by-phase)"
        assert await a.fetchval("SELECT stopped FROM py_evd3.be_evidence_outbox WHERE phase = 'cancelled'") == "conflict"
        assert "evidence_refused" in out.getvalue()
        before = len(store.got)
        await transfer(d, store, log)
        assert len(store.got) == before, "a stopped row is not sent again"
    finally:
        await a.close()
        await pool.close()


async def test_the_commit_sequence_follows_the_commit_order():  # P12.17
    d, pool = await schema("py_evd4")
    a = await pgtest.admin()
    try:
        w = Writer(d, "demo/orders", "orders-1", logger()[0])
        numbered = asyncio.Event()
        done = {}

        async def first(tx):
            await w.general(tx, "security_widening", BOB, ORDER, {"grant": "a"})
            numbered.set()
            await asyncio.sleep(0.4)  # A holds its number before committing

        async def run_a():
            await d.tx(first)
            done["a"] = time.monotonic()

        ta = asyncio.create_task(run_a())
        await numbered.wait()
        b = await d.tx(lambda tx: w.general(tx, "security_widening", BOB, ORDER, {"grant": "b"}))
        done["b"] = time.monotonic()
        await ta
        assert done["b"] >= done["a"] - 0.05, "B committed after A"
        assert await a.fetchval("SELECT commit_seq FROM py_evd4.be_change_history WHERE evidence_id = $1", b) == 2
    finally:
        await a.close()
        await pool.close()


async def test_the_grpc_store_talks_to_the_evidence_store(world):  # P12.10, P12.12
    d, pool = await schema("py_evd5")
    out = Outbound()
    out.set_tls(world.tls("demo/orders"))
    try:
        w = Writer(d, "demo/orders", "orders-1", logger()[0], store=GRPCStore(out.grpc("infra/audit", world.audit_addr)))
        p = await w.intent("security_tightening", BOB, ORDER, {"termination": "revoke"})
        assert p.went == "store"
        await d.tx(p.effective)
        with reqctx.deadline_scope(time.monotonic() + 5):  # calls need a deadline (P4.4)
            await transfer(d, w.store, w.log)
            assert {p.id + "/intent", p.id + "/effective"} <= set(world.evidence)
            assert await w.store.append([seal(w.record(p.id, "security_tightening", "intent", BOB, ORDER,
                                                       {"termination": "other"}))]) == ["conflict"], "other bytes"
        world.evidence_down = True
        p2 = await w.intent("security_tightening", BOB, ORDER, {"termination": "narrow"})
        assert p2.went == "lost", "no store, no alert channel: a double failure"
    finally:
        await out.close()
        await pool.close()


async def test_the_owner_replays_its_evidence():  # P12.19-P12.21
    d, pool = await schema("py_evr1")
    a = await pgtest.admin()
    try:
        await a.execute(f"INSERT INTO py_evr1.orders VALUES ('{ORDER.id}', 'open')")
        replays = Replays()

        async def tighten(tx, r):
            await tx.execute("INSERT INTO touched VALUES ($1)", r.id)  # a write before the check
            status = await tx.execute("UPDATE orders SET state = $2 WHERE id = $1", r.object.id, r.change["termination"])
            if status.endswith(" 0"):
                raise Dangling()

        replays.add("security_tightening", tighten)
        s = ReplayServer(d, "demo/orders", replays, logger()[0])
        w = Writer(d, "demo/orders", "orders-1", logger()[0])
        suffix = {"effective": "001", "intent": "002", "general": "003", "cancelled": "004"}

        def record(phase, category, obj, component="demo/orders"):
            r = w.record("0190f3a1-0000-7000-8000-00000000e" + suffix[phase], category, phase, BOB, obj,
                         {"termination": "revoked"})
            r["recorder"] = {"component": component, "instance": "orders-1"}
            return seal(r).record

        async def ask(raw):
            return (await s.Replay(replay_pb2.ReplayRequest(record=raw, recovery="r1"), None)).outcome

        async def count(sql, *args):
            return await a.fetchval(sql, *args)

        eff = record("effective", "security_tightening", ORDER)
        assert await ask(eff) == APPLIED
        assert await count("SELECT state FROM py_evr1.orders") == "revoked"
        assert await count("SELECT count(*) FROM py_evr1.be_change_history WHERE evidence_id = $1",
                           "0190f3a1-0000-7000-8000-00000000e001") == 1, "the history has the record's own identity"
        await a.execute("UPDATE py_evr1.orders SET state = 'open'")
        assert await ask(eff) == REFLECTED, "again: reflected, nothing done"
        assert await count("SELECT state FROM py_evr1.orders") == "open"

        gone = record("intent", "security_tightening", Object("demo.order", "0190f3a1-0000-7000-8000-000000000999"))
        assert await ask(gone) == DANGLING
        for table in ("be_change_history WHERE evidence_id", "touched WHERE id"):
            assert await count(f"SELECT count(*) FROM py_evr1.{table} = $1",
                               "0190f3a1-0000-7000-8000-00000000e002") == 0, "rolled back, nothing written"

        assert await ask(record("general", "security_widening", ORDER)) == UNSUPPORTED

        async def later(tx, r):
            await tx.execute("INSERT INTO touched VALUES ($1)", r.id)
            raise Deferred("the signal is not applied yet")

        replays.add("security_hold_release", later)
        assert await ask(record("general", "security_hold_release", ORDER)) == DEFERRED
        for table in ("be_change_history WHERE evidence_id", "touched WHERE id"):
            assert await count(f"SELECT count(*) FROM py_evr1.{table} = $1",
                               "0190f3a1-0000-7000-8000-00000000e003") == 0, "deferred: rolled back"

        for raw in (record("effective", "security_tightening", ORDER, "demo/other"),
                    record("cancelled", "security_tightening", ORDER), b'{"record_type":"other"}'):
            with pytest.raises(errs.Error) as e:
                await ask(raw)
            assert e.value.reason == "REQUEST_INVALID", "another recorder's, a cancelled one, not a record"
    finally:
        await a.close()
        await pool.close()


async def test_the_owner_checks_what_lies_outside_the_recovery_group():  # P12.20
    d, pool = await schema("py_evr2")
    a = await pgtest.admin()
    try:
        asked = []

        async def check(r):
            asked.append(r)
            if in_tx():
                raise RuntimeError("the check runs in a transaction")
            kind = r.change["kind"]
            if kind == "password":
                return True, ""
            if kind == "second_factor":
                return False, "the factors are back"
            raise ConnectionError("the product cannot be reached")

        replays = Replays()
        replays.check("credential_tightening", check)
        s = ReplayServer(d, "demo/iam", replays, logger()[0])
        at = datetime(2026, 11, 5, 21, 0, 0, 123000, tzinfo=timezone.utc)
        w = Writer(d, "demo/iam", "iam-1", logger()[0], now=lambda: at)

        def record(n, kind):
            return seal(w.record("0190f3a1-0000-7000-8000-00000000c" + n, "credential_tightening", "intent", BOB,
                                 Object("demo.account", BOB, "3"), {"kind": kind})).record

        async def ask(raw):
            r = await s.Replay(replay_pb2.ReplayRequest(record=raw, recovery="r1"), None)
            return r.outcome, r.detail

        assert (await ask(record("001", "password")))[0] == CONSISTENT
        assert asked[0].at == at and asked[0].object.id == BOB, "the check sees the record's time and object"
        assert (await ask(record("001", "password")))[0] == CONSISTENT, "checked again: nothing written"
        assert len(asked) == 2
        assert await ask(record("002", "second_factor")) == (INCONSISTENT, "the factors are back")
        got, detail = await ask(record("003", "unknown"))
        assert got == INCONSISTENT and "cannot be reached" in detail, "not asked: inconsistent, for review"
        assert await a.fetchval("SELECT count(*) FROM py_evr2.be_change_history") == 0, "a check writes no history"
        await a.execute("""INSERT INTO py_evr2.be_change_history (evidence_id, object_type, object_id, epoch, commit_seq)
            VALUES ('0190f3a1-0000-7000-8000-00000000c004', 'demo.account', $1, '1.1', 1)""", BOB)
        n = len(asked)
        assert (await ask(record("004", "password")))[0] == REFLECTED, "in the restored history: not checked"
        assert len(asked) == n
    finally:
        await a.close()
        await pool.close()
