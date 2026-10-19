"""Authorization on the component side: vector segment-eval (single-object part), segment digests of vector
canonical, the declarations' checks (P8.7) and the view holder (P8.9-P8.13), ported from the Go SDK's tests."""
import io
from datetime import datetime

import pytest

from besdk import errs, protocol, reqctx
from besdk.authz import (AMOUNT, NULL, UNOBTAINED, Definitions, Dimension, Fact, Field, Holder, Operation, Resource,
                         Segment, Subject, evaluate, mask)
from besdk.authz.view import DRIFT, INTERVAL, T_STALL, W_READ, Shard, _Held
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
            assert (d.visible.value, d.action.value, d.fields) == (want["visible"], want["action"], want["fields"]), \
                (c["id"], rid)
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


def test_mask_keeps_only_readable_fields():
    assert mask({"number": "1", "note": "x", "undeclared": 1}, ["number"]) == {"number": "1"}


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
    def __init__(self, clock, shard, gen="1", status="changed", delay=0.0):
        self.clock, self.shard, self.gen, self.status, self.delay, self.bad_nonce = clock, shard, gen, status, delay, False

    async def verify(self, req):
        self.clock[0] += self.delay  # the answer arrives late
        return {"nonce": "replayed" if self.bad_nonce else req["nonce"],
                "recovery_generation": {"domain": "authz.recovery", "value": self.gen},
                "shards": [{"shard": "orders", "status": self.status, "view": self.shard}]}


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
                                   s.get("suspended", False))
        ans = {"instance": c["answer"]["instance"], "shards": [
            {"shard": a["shard"], "status": a["status"],
             **({"view": Shard(a["shard"], [], {}, "new", a["view"]["security_revisions"])} if a.get("view") else {})}
            for a in c["answer"]["shards"]]}
        accepted = h.apply(ans, c["answer"]["recovery"], sent)
        got = {name: "dropped" for name in c["held"]["shards"]}
        for name, x in h.shards.items():
            got[name] = ("stopped" if x.stopped else "suspended" if x.suspended else
                         "replaced" if x.sent_at == sent and x.shard.digest == "new" else
                         "renewed" if x.sent_at == sent else "kept")
        assert accepted == (c["expect"]["answer"] == "accepted"), c["id"]
        assert got == c["expect"]["shards"], c["id"]
        assert h.instance == (c["expect"]["instance"] or ""), c["id"]
