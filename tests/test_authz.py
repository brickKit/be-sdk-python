"""Authorization on the component side: vector segment-eval (single-object part), segment digests of vector
canonical, the declarations' checks (P8.7) and the view holder (P8.9-P8.13), ported from the Go SDK's tests."""
import io
from datetime import datetime, timezone

import pytest

from besdk import errs, protocol, reqctx
from besdk.authz import (AMOUNT, NULL, UNOBTAINED, Definitions, Dimension, Fact, Field, Holder, Operation, Resource,
                         Segment, Subject, evaluate, mask, stale_basis)
from besdk.authz.view import DRIFT, INTERVAL, T_STALL, W_HIGH, W_READ, Shard, _Held
from besdk.guard import UserGuard
from besdk.http import Route
from besdk.identity import User
from besdk.obs import Member, new_logger

ALICE = "0190f3a1-0000-7000-8000-0000000000a1"


def fixture():
    cs = protocol.vector("segment-eval")["cases"]
    f = cs[0]
    res = f["resource"]
    r = Resource(res["type"], res["view_operation"],
                 {n: Dimension(d["kind"], d["column"], d.get("principal_role", ""), d.get("currency_column", ""))
                  for n, d in res["dimensions"].items()},
                 {n: Field(fd["class"], "k." + n) for n, fd in res["fields"].items()})
    return f, r, cs[1:]


def facts(r, rec):
    out = {}
    for name, d in r.dimensions.items():
        raw = rec.get("amount" if d.kind == AMOUNT else d.column) or {}
        if raw.get("state") == "value":
            v = raw["value"]
            out[name] = Fact.money(v["amount"], v["currency"]) if d.kind == AMOUNT else Fact.of(v)
        else:
            out[name] = NULL if raw.get("state") == "null" else UNOBTAINED
    return out


class OneShard:
    """A holder stand-in with every segment valid: the vector is about evaluation, not validity."""

    def __init__(self, segs):
        self.segs = segs

    def lookup(self, resource_type, principal, risk):
        from besdk.authz.view import Grant

        return Grant(self.segs, 1e12)

    def lookup_for(self, resource_type, principal, risk, tightening_only):
        return self.lookup(resource_type, principal, risk)


def test_segment_eval_vectors():
    f, r, cases = fixture()
    at = datetime.fromisoformat(f["at"])
    defs = Definitions("demo/orders", [r], [Operation(r.view_operation, r.type, "read", "low"),
                                            Operation("demo.order.approve", r.type, "action", "low")])
    defs.validate()
    n = 0
    for c in cases:
        if c["kind"] != "evaluate":
            continue
        segs = [Segment.from_dict(s) for s in c["segments"]]
        for s in segs:
            s.check(r)
        subject = Subject(c["principal"], defs, OneShard(segs), at)  # type: ignore[arg-type]
        for rec in f["records"]:
            want, rid = c["expect"]["records"][rec["id"]], rec["id"]
            d = evaluate(r, segs, c["action"], facts(r, rec), c["principal"], at)
            read = evaluate(r, segs, r.view_operation, facts(r, rec), c["principal"], at)
            assert (d.visible.value, d.action.value, read.fields, d.fields) == (
                want["visible"], want["action"], want["fields"], want["action_fields"]), (c["id"], rid)
            for op, key in ((r.view_operation, "read_error"), (c["action"], "action_error")):
                try:
                    subject.check(op, facts(r, rec))
                    got = None
                except errs.Error as e:
                    got = e.reason
                assert got == want[key], (c["id"], rid, key)
            n += 1
    assert n > 0


def test_segment_digests_of_vector_canonical():
    for c in protocol.vector("canonical")["cases"]:
        if c["kind"] == "segment":
            assert Segment.from_dict(c["in"]).digest() == c["expect"]["digest"], c["id"]


def test_segment_check_vectors():
    """What a component refuses against its own resource definition (P8.14, EN08-EN10)."""
    _, r, cases = fixture()
    checks = [c for c in cases if c["kind"] == "check"]
    assert checks, "precondition: the vector has check cases"
    for c in checks:
        try:
            Segment.from_dict(c["segment"]).check(r)
            ok = True
        except ValueError:
            ok = False
        assert ok == c["valid"], c["id"]


def test_mask_vectors():
    """The masked output (P8.31): unreadable declared fields None and named under _masked; secret and undeclared
    keys dropped."""
    _, r, cases = fixture()
    masks = [c for c in cases if c["kind"] == "mask"]
    assert masks, "precondition: the vector has mask cases"
    for c in masks:
        assert mask(r, c["object"], c["readable"]) == c["expect"], c["id"]


def test_segments_that_do_not_fit_the_definitions_are_refused():
    """A segment missing a declared dimension would leave it unconstrained; on adoption it is refused, logged once,
    and the rest of the view is used (P8.14, FX01, EN08)."""
    order = Resource("demo.order", "demo.order.read", {"region": Dimension("value_set", "region")},
                     {"id": Field("ordinary")})
    defs = Definitions("demo/orders", [order], [Operation("demo.order.read", "demo.order", "read", "low")])
    defs.validate()
    base = {"resource_type": "demo.order", "operation": "demo.order.read", "read_fields": ["id"],
            "valid": {"from": "2026-01-01T00:00:00Z"}}
    no_region = Segment.from_dict({**base, "atoms": {}})
    fits = Segment.from_dict({**base, "atoms": {"region": {"kind": "in", "values": ["east"]}}})
    out = io.StringIO()
    h = Holder("demo/orders", None, new_logger(Member("demo/orders", "3.0.0"), "info", out), defs=defs)  # type: ignore[arg-type]
    view = Shard("orders", ["demo.order"], {ALICE: ([no_region, fits], False, 0)}, "sha256:1")
    assert h.apply({"instance": "", "shards": [{"shard": "orders", "status": "changed", "view": view}]}, 1, h.boot())
    g = h.lookup("demo.order", ALICE, "low")
    assert g.segments == [fits], "the segment without the region is not used; the other is"
    assert out.getvalue().count('"segment_refused"') == 1, out.getvalue()
    d = evaluate(order, g.segments, "demo.order.read", {"region": Fact.of("west")}, ALICE, datetime.now(timezone.utc))
    assert d.visible.value == "false", "a west record stays invisible: the refused segment is not read as region all"
    assert h.shards["orders"].shard.digest == "sha256:1", "the held digest is the received bytes' (P8.11)"


def test_declarations_are_checked_together():  # P8.7, CK-AZ-01
    d = Definitions("demo/x", [Resource("demo.thing", "demo.thing.read", None, {"a": Field("plain")}),
                               Resource("Bad", "demo.bad.read", {"m": Dimension(AMOUNT, "m")})],
                    [Operation("demo.thing.read", "demo.thing", "read", ""),
                     Operation("demo.thing.read", "demo.thing", "read", "low")])
    with pytest.raises(ValueError) as e:
        d.validate()
    msg = str(e.value)
    for want in ("dimensions must be declared", "field class 'plain'", "not a registry key", "currency column",
                 "risk class must be low or high", "declared twice", "Bad: its view operation"):
        assert want in msg, want
    ok = Definitions("demo/x", [Resource("demo.thing", "demo.thing.read", {})],
                     [Operation("demo.thing.read", "demo.thing", "read", "low")])
    ok.validate()
    with pytest.raises(ValueError, match="not declared: demo.thing.write"):
        ok.check_operations(["demo.thing.read", "demo.thing.write"])


class FakeSource:
    def __init__(self, clock, shard, gen="1", status="changed", delay=0.0, read_only=False):
        self.clock, self.shard, self.gen, self.status, self.delay, self.bad_nonce = clock, shard, gen, status, delay, False
        self.read_only = read_only

    async def verify(self, req):
        self.clock[0] += self.delay  # the answer arrives late
        return {"nonce": "replayed" if self.bad_nonce else req["nonce"],
                "recovery_generation": {"domain": "authz.recovery", "value": self.gen},
                "shards": [{"shard": "orders", "status": self.status, "view": self.shard,
                            **({"read_only": True} if self.read_only else {})}]}


def shard(disabled=False):
    seg = {"resource_type": "demo.order", "operation": "demo.order.read", "atoms": {}, "read_fields": [],
           "valid": {"from": "2026-01-01T00:00:00Z"}}
    return {"shard": "orders", "applicability": ["demo.order"], "digest": "sha256:1",
            "subjects": {ALICE: {"segments": [seg], "accounts_disabled": disabled}}}


def holder(**kw):
    clock = [1000.0]
    src = FakeSource(clock, kw.pop("shard", shard()), **kw)
    h = Holder("demo/orders", src, new_logger(Member("demo/orders", "3.0.0"), "debug", io.StringIO()),
               boot=lambda: clock[0], wall=lambda: 1792200000 + clock[0])
    return h, src, clock


def expired(h, typ="demo.order", risk="low"):
    with pytest.raises(errs.Error) as e:
        h.lookup(typ, ALICE, risk)
    assert errs.is_(e.value, "be", "AUTHORIZATION_VIEW_EXPIRED")


def test_no_view_after_start():
    expired(holder()[0])


async def test_validity_counts_from_the_request():  # P8.10, CK-AZ-03
    h, src, clock = holder(delay=10.0)
    sent = clock[0]
    await h.verify()
    g = h.lookup("demo.order", ALICE, "low")
    assert len(g.segments) == 1
    assert g.cutoff == sent + W_READ * (1 - DRIFT), "a late answer does not extend the validity"
    expired(h, risk="high")  # 10 s after sending: past W_high
    clock[0] = sent + 29
    h.lookup("demo.order", ALICE, "low")
    clock[0] = sent + 30
    expired(h)


async def test_answers_that_do_not_renew():  # P8.9, P8.11
    h, src, clock = holder()
    await h.verify()
    start = clock[0]
    src.bad_nonce, src.status = True, "unchanged"
    clock[0] = start + 20
    with pytest.raises(ValueError):
        await h.verify()  # a replayed answer is discarded
    src.bad_nonce, src.status = False, "closed"
    await h.verify()
    clock[0] = start + 31
    expired(h)  # neither renewed it
    src.status = "changed"
    await h.verify()
    h.lookup("demo.order", ALICE, "low")
    src.gen = "0"  # an older recovery generation: a rollback
    await h.verify()
    expired(h)
    src.gen = "2"  # a newer one is adopted
    await h.verify()
    h.lookup("demo.order", ALICE, "low")
    expired(h, "demo.invoice")  # the shard does not apply to this type


async def test_read_only_renewal_leaves_the_high_risk_validity():  # P8.11, security 03 (renewal and closing)
    h, src, clock = holder()
    await h.verify()
    full = clock[0]
    h.lookup("demo.order", ALICE, "high")  # the precondition: a full renewal is valid for high-risk operations
    src.status, src.read_only = "unchanged", True
    clock[0] = full + 3
    await h.verify()
    assert h.lookup("demo.order", ALICE, "high").cutoff == full + W_HIGH * (1 - DRIFT), "the high-risk cutoff stays"
    assert h.lookup("demo.order", ALICE, "low").cutoff == clock[0] + W_READ * (1 - DRIFT), "ordinary reads are renewed"
    src.status, src.shard = "changed", {**shard(), "digest": "sha256:2"}  # a new view, still read-only
    clock[0] = full + 4
    await h.verify()
    assert h.lookup("demo.order", ALICE, "high").cutoff == full + W_HIGH * (1 - DRIFT)
    clock[0] = full + W_HIGH
    expired(h, risk="high")
    h.lookup("demo.order", ALICE, "low")  # ordinary reads go on
    src.read_only = False  # the source caught up: a full renewal opens high-risk operations again
    await h.verify()
    h.lookup("demo.order", ALICE, "high")


async def test_tightening_only_outlives_a_stale_source():  # P8.29
    h, src, clock = holder()
    await h.verify()
    full = clock[0]
    with pytest.raises(errs.Error):
        h.lookup_stale("demo.order", ALICE)  # fully renewed and fresh: the ordinary lookup decides, not this one

    # the member says the source is stale (read_only), then closed; past both validities the last view still decides
    src.status, src.read_only = "unchanged", True
    clock[0] = full + 1
    await h.verify()
    src.status, src.read_only = "closed", False
    clock[0] = full + W_READ + W_HIGH
    await h.verify()
    expired(h)
    g = h.lookup_stale("demo.order", ALICE)
    assert g.stale
    assert len(g.segments) == 1
    assert g.since == 1792200000 + full, "the last full renewal"
    assert g.cutoff > clock[0], "the change may commit"

    # a full renewal, then the member stops answering: that is no stale source, the view closes as before
    src.status = "unchanged"
    await h.verify()
    h.lookup("demo.order", ALICE, "high")  # fresh again: the ordinary lookup decides
    clock[0] += W_READ + W_HIGH
    expired(h)
    with pytest.raises(errs.Error) as e:
        h.lookup_stale("demo.order", ALICE)
    assert errs.is_(e.value, "be", "AUTHORIZATION_VIEW_EXPIRED")


async def test_tightening_only_needs_a_full_renewal_once():  # P8.29
    h, src, clock = holder(read_only=True)
    await h.verify()
    clock[0] += W_READ + W_HIGH
    with pytest.raises(errs.Error) as e:
        h.lookup_stale("demo.order", ALICE)
    assert errs.is_(e.value, "be", "AUTHORIZATION_VIEW_EXPIRED")


async def stale_member():
    """A holder whose source the member closed past both validities, with a tightening-only operation (P8.29)."""
    sh = shard()
    segs = sh["subjects"][ALICE]["segments"]
    for op in ("demo.order.revoke", "demo.order.approve"):
        segs.append({"resource_type": "demo.order", "operation": op, "atoms": {}, "read_fields": [],
                     "valid": {"from": "2026-01-01T00:00:00Z"}})
    h, src, clock = holder(shard=sh)
    await h.verify()
    src.status = "closed"
    clock[0] += W_READ + W_HIGH
    await h.verify()
    defs = Definitions("demo/orders", [Resource("demo.order", "demo.order.read", {})],
                       [Operation("demo.order.read", "demo.order", "read", "low"),
                        Operation("demo.order.revoke", "demo.order", "manage", "high", ["security_change"],
                                  tightening_only=True),
                        Operation("demo.order.approve", "demo.order", "action", "high")])
    defs.validate()
    return h, defs, clock


async def test_check_admits_tightening_only_on_a_stale_view():  # P8.29
    h, defs, clock = await stale_member()
    s = Subject(ALICE, defs, h, datetime.fromisoformat("2026-10-17T00:00:00Z"))
    with reqctx.cutoff_scope(clock[0] + 100):
        s.check("demo.order.revoke", {})  # a tightening-only operation on the stale source's last view
        assert stale_basis() == 1792200000 + 1000.0
        for op in ("demo.order.approve", "demo.order.read"):
            with pytest.raises(errs.Error) as e:
                s.check(op, {})
            assert errs.is_(e.value, "be", "AUTHORIZATION_VIEW_EXPIRED")
            assert stale_basis() is None, "a refused decision has no basis"


async def test_the_guard_admits_tightening_only_on_a_stale_view():  # P8.29
    h, defs, clock = await stale_member()

    class Verifier:
        async def verify(self, token):
            return User(ALICE, expiry=1792200000 + clock[0] + 600)

    class Request:
        headers = {"authorization": "Bearer t"}

    g = UserGuard(Verifier(), defs, h, h.log, boot=lambda: clock[0], now=lambda: 1792200000 + clock[0])  # type: ignore[arg-type]
    with await g.admit(Route("POST", "/revoke", None, "demo.order.revoke"), Request()):  # type: ignore[arg-type]
        assert reqctx.cutoff() == clock[0] + W_READ * (1 - DRIFT)
    for op in ("demo.order.approve", "demo.order.read"):
        with pytest.raises(errs.Error) as e:
            await g.admit(Route("POST", "/x", None, op), Request())  # type: ignore[arg-type]
        assert errs.is_(e.value, "be", "AUTHORIZATION_VIEW_EXPIRED")


async def test_a_read_only_first_view_is_not_valid_for_high_risk():  # P8.11
    h, src, clock = holder(read_only=True)
    clock[0] = 1.0  # just after the machine started: a zero high-risk renewal time would still look valid
    await h.verify()
    h.lookup("demo.order", ALICE, "low")
    expired(h, risk="high")


async def test_a_stall_invalidates_every_shard():  # P8.10
    h, src, clock = holder()
    h.tick()
    await h.verify()
    clock[0] += INTERVAL + T_STALL + 1  # the process was frozen
    h.tick()
    expired(h)


async def test_a_wall_clock_jump_invalidates_every_shard():  # P8.10
    h, src, clock = holder()
    h.tick()
    await h.verify()
    h.wall = lambda: 1792200000 + clock[0] + T_STALL + 1
    h.tick()
    expired(h)


async def test_disabled_accounts_have_no_segments():
    h, _, _ = holder(shard=shard(disabled=True))
    await h.verify()
    assert h.lookup("demo.order", ALICE, "low").segments == []


async def test_check_moves_the_cutoff_to_the_evidence():  # P8.12
    h, src, clock = holder()
    await h.verify()
    defs = Definitions("demo/orders", [Resource("demo.order", "demo.order.read", {}, {"number": Field("ordinary")})],
                       [Operation("demo.order.read", "demo.order", "read", "low")])
    defs.validate()
    s = Subject(ALICE, defs, h, datetime.fromisoformat("2026-10-17T00:00:00Z"))
    with reqctx.cutoff_scope(clock[0] + 100):
        s.check("demo.order.read", {})
        assert reqctx.cutoff() == clock[0] + W_READ * (1 - DRIFT)
    assert reqctx.cutoff() is None


async def test_a_verification_that_times_out_is_not_a_stall():  # P8.10: only the wait between verifications is timed
    import asyncio

    h, src, clock = holder()
    answer, calls = src.verify, []

    async def slow_then_failing(req):
        calls.append(1)
        if len(calls) == 1:
            return await answer(req)
        clock[0] += INTERVAL - 0.1  # the authorization member is slow: the call runs into its deadline
        raise TimeoutError("deadline")

    src.verify = slow_then_failing

    async def sleep(seconds):
        clock[0] += seconds  # the wait, on time
        if len(calls) == 3:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await h.run(sleep)
    assert h.lookup("demo.order", ALICE, "low").segments, "the view renewed by the first answer is still valid"


def test_instants_without_an_offset_never_hold():
    for bad in ("2026-01-01T00:00:00", "2026-01-01"):
        seg = Segment("demo.order.read", {}, [], bad)
        assert not seg.active_at(datetime.fromisoformat("2026-10-17T00:00:00Z")), bad
    seg = Segment("demo.order.read", {}, [], "2026-01-01T00:00:00Z", "2027-01-01")
    assert not seg.active_at(datetime.fromisoformat("2026-10-17T00:00:00Z"))


def test_amounts_are_decimal_strings_as_the_protocol_writes_them():
    from besdk.authz.segment import Atom, Tri, _interval

    a = Atom("interval", currency="CNY", lo="0", lo_closed=True)
    assert _interval(a, Fact.money("100", "CNY")) is Tri.TRUE
    for odd in ("1_000", " 100", "NaN", "Infinity", "١٠٠"):
        assert _interval(a, Fact.money(odd, "CNY")) is Tri.UNKNOWN, odd


def test_view_accept_vectors():  # P8.11
    before, sent = 1.0, 9.0
    for c in protocol.vector("view-accept")["cases"]:
        h = Holder("demo/orders", None, new_logger(Member("demo/orders", "3.0.0"), "error", io.StringIO()))  # type: ignore[arg-type]
        if c["held"]["recovery"] is not None:
            h.recovery = c["held"]["recovery"]
        h.instance = c["held"]["instance"] or ""
        for name, s in c["held"]["shards"].items():
            h.shards[name] = _Held(Shard(name, [], {}, "old", s["revisions"]), before, s.get("stopped", False),
                                   s.get("suspended", False), high_at=before)
        ans = {"instance": c["answer"]["instance"], "shards": [
            {"shard": a["shard"], "status": a["status"], "read_only": a.get("read_only", False),
             **({"view": Shard(a["shard"], [], {}, "new", a["view"]["security_revisions"])} if a.get("view") else {})}
            for a in c["answer"]["shards"]]}
        accepted = h.apply(ans, c["answer"]["recovery"], sent)
        got = {name: "dropped" for name in c["held"]["shards"]}
        for name, x in h.shards.items():
            fresh, carried, new = x.high_at == sent, x.high_at == before, x.shard.digest == "new"
            if x.stopped or x.suspended:
                got[name] = "stopped" if x.stopped else "suspended"
            elif x.sent_at != sent:
                got[name] = "kept" if carried else "kept with its high-risk validity changed"
            elif new:
                got[name] = ("replaced" if fresh else "replaced_read" if carried else
                             "adopted_read" if x.high_at is None else "replaced with another high-risk validity")
            else:
                got[name] = ("renewed" if fresh else "renewed_read" if carried else
                             "renewed with no high-risk validity")
        assert accepted == (c["expect"]["answer"] == "accepted"), c["id"]
        assert got == c["expect"]["shards"], c["id"]
        assert h.instance == (c["expect"]["instance"] or ""), c["id"]


async def test_grpc_source_carries_read_only():  # P8.11: the contract's ShardAnswer.read_only reaches the holder
    from be.authz.v3 import authz_pb2

    from besdk.authz import GRPCSource

    class Stub:
        async def Verify(self, req, metadata=None):  # noqa: N802 (the gRPC method name)
            return authz_pb2.VerifyResponse(
                nonce=req.nonce, instance="authz-inst-a",
                recovery_generation=authz_pb2.RevisionRef(domain="authz.recovery", value="1"),
                shards=[authz_pb2.ShardAnswer(shard="s1", status=authz_pb2.SHARD_STATUS_UNCHANGED, read_only=True),
                        authz_pb2.ShardAnswer(shard="s2", status=authz_pb2.SHARD_STATUS_UNCHANGED)])

    src = GRPCSource.__new__(GRPCSource)
    src._stub = Stub()
    a = await src.verify({"component": "demo/orders", "nonce": "n1", "held": []})
    assert [s["read_only"] for s in a["shards"]] == [True, False]


def test_a_segment_on_a_non_pushdownable_dimension_makes_the_list_not_completable():
    """A list whose segment constrains a dimension the resource declares not pushdownable answers
    QUERY_NOT_COMPLETABLE, never compiled as a local column or dropped (P8.17, security 01's AE16); all on it
    compiles. The same as be-sdk-go's authz.TestASegmentOnANonPushdownableDimensionMakesTheListNotCompletable."""
    from besdk.authz.defs import NOT_PUSHDOWNABLE
    from besdk.authz.plan import compile_plan

    r = Resource("demo.doc", "demo.doc.read", {"team": Dimension("value_set", "team", pushdown=NOT_PUSHDOWNABLE),
                                               "region": Dimension("value_set", "region")}, {})

    def seg(team):
        return Segment.from_dict({"resource_type": "demo.doc", "operation": "demo.doc.read", "read_fields": [],
                                  "valid": {"from": "2026-01-01T00:00:00Z"},
                                  "atoms": {"team": team, "region": {"kind": "in", "values": ["east"]}}})
    with pytest.raises(errs.Error) as e:
        compile_plan(r, [seg({"kind": "in", "values": ["t1"]})], "demo.doc.read", "p", datetime.now(timezone.utc))
    assert errs.is_(e.value, "be", "QUERY_NOT_COMPLETABLE")
    p = compile_plan(r, [seg({"kind": "all"})], "demo.doc.read", "p", datetime.now(timezone.utc))
    assert "team" not in p.where, "all on it constrains nothing"
