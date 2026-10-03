"""Command idempotency against PostgreSQL (P13, CP-IDEM-01…07): atomic claim, replay without executing,
binding to (command, target, fingerprint), caller namespaces, in-progress, rollback, expiry, concurrency."""
import asyncio

import pytest

import besdk
from besdk import Error, context
from tests.integration.test_store import rt  # noqa: F401 - fixture

CREATE = "conformance.widget.create"


def cmd(key="k1", name=CREATE, request=None, target=""):
    return besdk.Command(key=key, name=name, request=request if request is not None else {"name": "A"}, target=target)


async def run(rt, c, calls, *, sub="u1", value=None):
    async def do():
        calls.append(1)
        return value if value is not None else {"id": f"w{len(calls)}"}

    with context.scope(sub=sub):
        return await rt.store().tx(lambda tx: besdk.idempotent(tx, c, do))


async def test_replay_returns_first_result_without_executing(rt):
    calls = []
    assert await run(rt, cmd(), calls) == ({"id": "w1"}, False)
    assert await run(rt, cmd(request={"name": "A"}), calls) == ({"id": "w1"}, True)
    assert len(calls) == 1


@pytest.mark.parametrize("other", [cmd(request={"name": "B"}), cmd(name="conformance.widget.approve"),
                                   cmd(target="w9")])
async def test_mismatch(rt, other):
    calls = []
    await run(rt, cmd(), calls)
    with pytest.raises(Error) as ei:
        await run(rt, other, calls)
    assert (ei.value.reason, ei.value.http) == ("IDEMPOTENCY_MISMATCH", 400) and len(calls) == 1


async def test_callers_are_independent(rt):
    calls = []
    await run(rt, cmd(), calls, sub="u1")
    assert await run(rt, cmd(request={"name": "other"}), calls, sub="u2") == ({"id": "w2"}, False)
    with context.scope(caller="erp/sales"):
        assert (await rt.store().tx(lambda tx: besdk.idempotent(tx, cmd(), lambda: _const("svc"))))[1] is False


async def _const(v):
    return v


async def test_rolled_back_claim_does_not_exist(rt):
    calls = []

    async def failing():
        calls.append(1)
        raise besdk.Error(besdk.Code.FAILED_PRECONDITION, "WIDGET_NOT_DRAFT")

    with context.scope(sub="u1"), pytest.raises(Error):
        await rt.store().tx(lambda tx: besdk.idempotent(tx, cmd(), failing))
    assert await run(rt, cmd(), calls) == ({"id": "w2"}, False)


async def test_two_step_in_progress_complete_and_release(rt):
    c = cmd(name="conformance.widget.approve", target="w1")
    with context.scope(sub="u1"):
        assert (await rt.store().tx(lambda tx: tx.idem_claim(c))).found is False
        with pytest.raises(Error) as ei:
            await rt.store().tx(lambda tx: tx.idem_claim(c))
        assert (ei.value.reason, ei.value.http) == ("IDEMPOTENCY_IN_PROGRESS", 409)
        assert (await rt.store().tx(lambda tx: tx.idem_lookup(c))).in_progress is True
        await rt.store().tx(lambda tx: tx.idem_release(c))
        assert (await rt.store().tx(lambda tx: tx.idem_claim(c))).found is False
        await rt.store().tx(lambda tx: tx.idem_complete(c, {"status": "APPROVED"}))
        p = await rt.store().tx(lambda tx: tx.idem_claim(c))
        assert (p.found, p.result) == (True, {"status": "APPROVED"})


async def test_expired_key_is_a_new_command(rt, ident):
    calls = []
    await run(rt, cmd(), calls)
    ident.sql(f"UPDATE \"{ident.schema}\".besdk_idempotency SET expires_at = now() - interval '1 second'")
    assert await run(rt, cmd(request={"name": "changed"}), calls) == ({"id": "w2"}, False)
    rows = ident.sql(f"SELECT expires_at - created_at FROM \"{ident.schema}\".besdk_idempotency")
    assert rows[0][0].days == 30


async def test_concurrent_same_key_executes_once(rt):
    calls = []

    async def slow():
        calls.append(1)
        await asyncio.sleep(0.3)
        return {"id": "only"}

    async def one():
        with context.scope(sub="u1"):
            return await rt.store().tx(lambda tx: besdk.idempotent(tx, cmd(), slow))

    results = await asyncio.gather(one(), one())
    assert len(calls) == 1
    assert sorted(r[1] for r in results) == [False, True] and {r[0]["id"] for r in results} == {"only"}


async def test_no_key_just_executes(rt):
    calls = []
    await run(rt, cmd(key=None), calls)
    await run(rt, cmd(key=None), calls)
    assert len(calls) == 2
