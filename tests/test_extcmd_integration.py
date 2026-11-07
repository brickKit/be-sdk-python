"""External commands on a real database (P13.1-P13.13), the same cases as be-sdk-go's
extcmd/commands_integration_test.go and reconcile_integration_test.go: a request idempotent by its reference; a
successful send; an unknown result queried first and sent again under dedupe; not found without dedupe goes to manual
and a person registers it; retries and takeovers; nothing sent past the deadline; query-only; an irrevocable command
needs its issue record; the step 7 reconciliation by queries and by listing."""
import io
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from besdk import errs, obs
from besdk._gen.be.external.v1 import reconcile_pb2
from besdk.evidence import Writer
from besdk.extcmd import reconcile as rc
from besdk.extcmd.commands import Answer, Commands, Result
from besdk.extcmd.server import ReconcileServer
from besdk.extcmd.state import (FAILED, MANUAL, MAYBE_REACHED, NOT_REACHED, PREPARED, RETRY_WAIT, SUCCEEDED, UNKNOWN,
                                Capabilities)
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
PLAIN = Capabilities(query=True)
DEDUPE = Capabilities(query=True, dedupe_window=timedelta(hours=24))
AMOUNT = {"amount": "100.00", "currency": "EUR"}


class FakeExternal:
    """Sends answer by a scripted class, queries by what it holds."""

    def __init__(self):
        self.classes, self.held, self.sends, self.queries, self.query_down = [], {}, [], [], False

    async def send(self, c):
        self.sends.append(c.reference)
        cls = self.classes.pop(0) if self.classes else SUCCEEDED
        if cls == "error":
            raise TimeoutError("read timeout")
        if cls not in (NOT_REACHED, MAYBE_REACHED):
            self.held[c.reference] = cls
        return Result(cls, external_id="ext-" + c.reference, response="ok")

    async def query(self, c):
        self.queries.append(c.reference)
        if self.query_down:
            raise ConnectionError("query endpoint down")
        s = self.held.get(c.reference)
        return Answer(s is not None, s or "")


class Listing(FakeExternal):
    """An external system that also lists its records (P13.11)."""

    def __init__(self):
        super().__init__()
        self.listed, self.list_down = 0, False

    async def list(self, start, end):
        self.listed += 1
        if self.list_down:
            raise ConnectionError("listing down")
        return [rc.Record(ref, s) for ref, s in self.held.items()]


class World:
    def __init__(self, name, ops, ext=None):
        self.name, self.ops, self.ext = name, ops, ext or FakeExternal()

    async def __aenter__(self):
        v, _ = await migrated(self.name, {"0001_x.up.sql": "CREATE TABLE x (id int);"})
        self.d, self.pool = await open_db(self.name, v)
        self.a = await pgtest.admin()
        self.c = Commands(self.d, "demo/pay", obs.new_logger(obs.Member("demo/pay", "3.0.0"), "error", io.StringIO()),
                          adapter=self.ext, ops=self.ops)
        return self

    async def __aexit__(self, *exc):
        await self.a.close()
        await self.pool.close()

    async def request(self, ref, params=AMOUNT, op="pay"):
        return await self.d.tx(lambda tx: self.c.request(tx, "demo/sales", ref, op, params))

    async def state(self, cid):
        return await self.a.fetchval(f"SELECT state FROM {self.name}.be_external_command WHERE id = $1", uuid.UUID(cid))

    async def sql(self, q, *args):
        await self.a.execute(q.replace("{s}", self.name), *args)

    async def due(self, cid):  # the backoff over: due now
        await self.sql("UPDATE {s}.be_external_command SET next_at = now() - interval '1 second' WHERE id = $1",
                       uuid.UUID(cid))


async def test_a_request_is_idempotent_by_its_reference():  # P13.2, EI05
    async with World("py_xc_req", {"pay": PLAIN}) as w:
        a, s = await w.request("r1")
        assert s == PREPARED
        b, _ = await w.request("r1")
        assert a.id == b.id, "the same command"
        with pytest.raises(errs.Error) as e:
            await w.request("r1", {"amount": "120.00", "currency": "EUR"})
        assert e.value.reason == "IDEMPOTENCY_KEY_REUSED"
        assert await w.a.fetchval("SELECT count(*) FROM py_xc_req.be_external_command") == 1
        with pytest.raises(errs.Error):
            await w.request("r2", op="refund")  # an operation the adapter does not declare


async def test_a_successful_send():
    async with World("py_xc_ok", {"pay": PLAIN}) as w:
        c, _ = await w.request("r1")
        await w.c.run()
        assert await w.state(c.id) == SUCCEEDED
        r = await w.a.fetchrow("""SELECT a.reached, c.external_id FROM py_xc_ok.be_external_attempt a
            JOIN py_xc_ok.be_external_command c ON c.id = a.command WHERE a.command = $1""", uuid.UUID(c.id))
        assert r["reached"] and r["external_id"] == "ext-r1"
        await w.c.run()
        assert w.ext.sends == ["r1"], "a final command is not processed again"


async def test_an_unknown_result_is_queried_first():  # EI01, EI04, P13.7
    async with World("py_xc_unknown", {"pay": DEDUPE}) as w:
        w.ext.classes = ["error"]
        c, _ = await w.request("r1")
        await w.c.run()
        assert await w.state(c.id) == UNKNOWN and w.ext.sends == ["r1"], "EI01"
        await w.c.run()
        assert await w.state(c.id) == SUCCEEDED, "not found, the external system deduplicates: sent again (EI04)"
        assert w.ext.sends == ["r1", "r1"], "the same reference"
        assert await w.a.fetchval("SELECT resend_deadline FROM py_xc_unknown.be_external_command WHERE id = $1",
                                  uuid.UUID(c.id)) is not None, "written after a send that may have reached"


async def test_not_found_without_dedupe_goes_to_manual():  # EI02, EI03, P13.8
    async with World("py_xc_manual", {"pay": PLAIN}) as w:
        w.ext.classes = [MAYBE_REACHED]
        c, _ = await w.request("r1")
        await w.c.run()
        assert await w.state(c.id) == UNKNOWN
        w.ext.query_down = True
        await w.c.run()
        assert await w.state(c.id) == UNKNOWN, "EI02: the query unavailable: unknown, backoff"
        nxt = await w.a.fetchval("SELECT next_at FROM py_xc_manual.be_external_command WHERE id = $1", uuid.UUID(c.id))
        assert nxt > datetime.now(timezone.utc) + timedelta(seconds=30), "backed off"
        w.ext.query_down = False
        await w.due(c.id)
        await w.c.run()
        assert await w.state(c.id) == MANUAL and w.ext.sends == ["r1"], "EI03: never sent again"
        await w.d.tx(lambda tx: w.c.register(tx, c.id, FAILED, "the bank confirms it never arrived and is closed"))
        assert await w.state(c.id) == FAILED
        with pytest.raises(errs.Error) as e:  # only a manual command is registered (P13.8)
            await w.d.tx(lambda tx: w.c.register(tx, c.id, SUCCEEDED, "a second opinion"))
        assert e.value.reason == "REQUEST_INVALID" and await w.state(c.id) == FAILED


async def test_retries_and_takeovers():  # EI30
    async with World("py_xc_retry", {"pay": PLAIN}) as w:
        w.ext.classes = [NOT_REACHED]
        c, _ = await w.request("r1")
        await w.c.run()
        assert await w.state(c.id) == RETRY_WAIT
        await w.c.run()
        assert len(w.ext.sends) == 1, "not before the backoff"
        await w.due(c.id)
        await w.c.run()
        assert await w.state(c.id) == SUCCEEDED
        assert await w.a.fetchval("SELECT first_reached_at FROM py_xc_retry.be_external_command WHERE id = $1",
                                  uuid.UUID(c.id)) is not None, "the second send may have reached"
        d, _ = await w.request("r2")
        await w.sql("UPDATE {s}.be_external_command SET state = 'sending', next_at = now() - interval '1 second' "
                    "WHERE id = $1", uuid.UUID(d.id))
        await w.c.run()
        assert await w.state(d.id) == UNKNOWN and "r2" not in w.ext.sends, "the claimant gone while sending"


async def test_past_the_deadline_nothing_is_sent():  # P13.4, EI40
    async with World("py_xc_deadline", {"pay": DEDUPE}) as w:
        c, _ = await w.request("r1")
        await w.sql("UPDATE {s}.be_external_command SET state = 'retry_wait', "
                    "resend_deadline = now() - interval '1 minute' WHERE id = $1", uuid.UUID(c.id))
        await w.c.run()
        assert await w.state(c.id) == UNKNOWN and w.ext.sends == []


async def test_query_only_sends_nothing():  # P13.10
    async with World("py_xc_qo", {"pay": PLAIN}) as w:
        w.c.query_only = True
        c, _ = await w.request("r1")
        await w.c.run()
        assert await w.state(c.id) == PREPARED and w.ext.sends == []
        w.ext.held["r2"] = SUCCEEDED
        d, _ = await w.request("r2")
        await w.sql("UPDATE {s}.be_external_command SET state = 'unknown' WHERE id = $1", uuid.UUID(d.id))
        await w.c.run()
        assert await w.state(d.id) == SUCCEEDED, "queried"
        w.c.ops = {"pay": DEDUPE}  # "not found" where the table would send again: query-only keeps it unknown
        e, _ = await w.request("r3")
        await w.sql("UPDATE {s}.be_external_command SET state = 'unknown', first_reached_at = now() WHERE id = $1",
                    uuid.UUID(e.id))
        await w.c.run()
        assert await w.state(e.id) == UNKNOWN and w.ext.sends == [], "not sent again in query-only"


class Store:
    def __init__(self):
        self.down, self.recs = True, []

    async def append(self, recs):
        if self.down:
            raise ConnectionError("evidence store down")
        self.recs += recs
        return ["accepted"] * len(recs)


async def test_an_irrevocable_command_needs_its_issue_record():  # P13.4
    async with World("py_xc_irrev", {"pay": Capabilities(query=True, irrevocable=True)}) as w:
        c, _ = await w.request("r1")
        await w.c.run()
        assert await w.state(c.id) == RETRY_WAIT and w.ext.sends == [], "no evidence writer: never sent"
        st = Store()
        w.c.evidence = Writer(w.d, "demo/pay", "i1", w.c.log, store=st)
        await w.due(c.id)
        await w.c.run()
        assert await w.state(c.id) == RETRY_WAIT and w.ext.sends == [], "the store down, no alert channel: not sent"
        st.down = False
        await w.due(c.id)
        await w.c.run()
        assert await w.state(c.id) == SUCCEEDED and w.ext.sends == ["r1"]
        assert len(st.recs) == 1 and b'"external_command_issued"' in st.recs[0].record, "the issue record first"
        assert b'"phase":"intent"' in st.recs[0].record


def request(recovery="r-1", issued=(), t=None):
    t = t or datetime.now(timezone.utc) - timedelta(hours=1)
    return reconcile_pb2.ReconcileRequest(recovery=recovery, t=t.isoformat().replace("+00:00", "Z"),
                                          window_before_seconds=60, issued=list(issued))


async def test_reconcile_by_queries():  # P13.11-P13.13
    async with World("py_xc_rq", {"pay": PLAIN}) as w:
        s = ReconcileServer(w.c)
        held, _ = await w.request("held")
        gone, _ = await w.request("gone")
        man, _ = await w.request("man")
        await w.sql("UPDATE {s}.be_external_command SET state = 'manual' WHERE id = $1", uuid.UUID(man.id))
        w.ext.held.update({"held": SUCCEEDED, "issued-x": SUCCEEDED})
        resp = await s.Reconcile(request(issued=["issued-x", "issued-y"]), None)
        got = {c.reference: (c.conclusion, c.state) for c in resp.conclusions}
        assert got["held"] == (rc.MATCHED, SUCCEEDED)
        assert got["man"][0] == rc.CONCLUSION_MANUAL
        assert got["gone"][0] == rc.CONCLUSION_MANUAL, "no dedupe, may still land: a person decides"
        assert got["issued-x"][0] == rc.EXTERNAL_ONLY and got["issued-y"][0] == rc.POSSIBLY_EXECUTED
        assert await w.state(held.id) == SUCCEEDED and await w.state(gone.id) == MANUAL
        assert await w.a.fetchval("SELECT unrecorded_until FROM py_xc_rq.be_external_command WHERE id = $1",
                                  uuid.UUID(gone.id)) is not None
        again = await s.Reconcile(request(issued=["issued-x", "issued-y"]), None)
        assert {c.reference: (c.conclusion, c.state) for c in again.conclusions} == got, "the same again"
        w.ext.query_down = True
        await w.request("later")
        n = await w.a.fetchval("SELECT count(*) FROM py_xc_rq.be_external_reconciled")
        with pytest.raises(errs.Error) as e:
            await s.Reconcile(request("r-2"), None)
        assert e.value.reason == "DEPENDENCY_UNAVAILABLE"
        assert await w.a.fetchval("SELECT count(*) FROM py_xc_rq.be_external_reconciled") == n, "nothing written"


async def test_reconcile_by_listing():  # P13.11
    async with World("py_xc_rl", {"pay": PLAIN}, Listing()) as w:
        s = ReconcileServer(w.c)
        recent, _ = await w.request("recent")
        old, _ = await w.request("old")
        t = datetime.now(timezone.utc) - timedelta(minutes=10)
        await w.sql("UPDATE {s}.be_external_command SET first_reached_at = $2 WHERE id = $1", uuid.UUID(old.id),
                    t - timedelta(hours=2))  # sent before the window: asked by its reference
        w.ext.held.update({"recent": SUCCEEDED, "old": SUCCEEDED})
        resp = await s.Reconcile(request(t=t), None)
        assert {c.reference: c.conclusion for c in resp.conclusions} == {"recent": rc.MATCHED, "old": rc.MATCHED}
        assert w.ext.listed == 1 and w.ext.queries == ["old"], "one listing; only the one before the window queried"
        w.ext.list_down = True
        with pytest.raises(errs.Error) as e:
            await s.Reconcile(request("r-2", t=t), None)
        assert e.value.reason == "DEPENDENCY_UNAVAILABLE"
