"""Idempotency keys against a real database (P4.23-P4.25): the claim, the command's writes and the outcome in one
transaction; a replay returns the stored outcome without running the command; another request under the key is
refused; concurrent first uses run the command once; a failed command keeps nothing; an expired record is claimed
afresh; the cleanup removes only expired records."""
import asyncio

import pytest

from besdk import errs, idem
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
EFFECTS = {"0001_effects.up.sql": "CREATE TABLE IF NOT EXISTS effects (item text NOT NULL, n int NOT NULL);"}


async def idem_db(name):
    v, _ = await migrated(name, EFFECTS)
    d, pool = await open_db(name, v)
    a = await pgtest.admin()
    await a.execute(f"DELETE FROM {name}.be_idempotency; DELETE FROM {name}.effects")
    return d, pool, a


def request(key="k-1", body=b'{"amount":"10.00"}', principal="user-1"):
    return idem.Request(principal, key, "demo.pay.create", "POST /api/pay", body)


def command(n=1):
    async def fn(tx):
        await tx.execute("INSERT INTO effects VALUES ('pay', $1)", n)
        return idem.Outcome(201, {"id": "pay-1"})
    return fn


async def test_a_replay_returns_the_stored_outcome():  # P4.23, P4.24
    d, pool, a = await idem_db("py_idem1")
    try:
        out, replayed = await idem.do(d, request(), command())
        assert (out.status, out.result, replayed) == (201, {"id": "pay-1"}, False)
        out, replayed = await idem.do(d, request(), command())
        assert (out.status, out.result, replayed) == (201, {"id": "pay-1"}, True)
        assert await a.fetchval("SELECT count(*) FROM py_idem1.effects") == 1, "the command ran once"
        out, replayed = await idem.do(d, request(principal="user-2"), command())
        assert not replayed, "another principal's same key is another key (P4.21)"
        with pytest.raises(errs.Error) as e:
            await idem.do(d, request(body=b'{"amount":"99.00"}'), command())
        assert e.value.reason == "IDEMPOTENCY_KEY_REUSED"
    finally:
        await a.close()
        await pool.close()


async def test_concurrent_first_uses_run_the_command_once():  # P4.23; testing: idempotency includes concurrency
    d, pool, a = await idem_db("py_idem2")
    try:
        async def slow(tx):
            await tx.execute("INSERT INTO effects VALUES ('pay', 1)")
            await asyncio.sleep(0.3)
            return idem.Outcome(201, {"id": "pay-1"})

        results = await asyncio.gather(*(idem.do(d, request(), slow) for _ in range(5)), return_exceptions=True)
        assert await a.fetchval("SELECT count(*) FROM py_idem2.effects") == 1, "one execution"
        done = [r for r in results if not isinstance(r, BaseException)]
        refused = [r for r in results if isinstance(r, BaseException)]
        assert len([r for r in done if not r[1]]) == 1, "exactly one ran the command; the others replayed its outcome"
        assert all(r[0].result == {"id": "pay-1"} for r in done)
        assert all(isinstance(r, errs.Error) and r.reason == "IDEMPOTENCY_IN_PROGRESS" for r in refused), refused
    finally:
        await a.close()
        await pool.close()


async def test_a_failed_command_keeps_nothing():  # P4.23
    d, pool, a = await idem_db("py_idem3")
    try:
        async def fails(tx):
            await tx.execute("INSERT INTO effects VALUES ('pay', 1)")
            raise errs.new("REQUEST_INVALID", "no")

        with pytest.raises(errs.Error):
            await idem.do(d, request(), fails)
        assert await a.fetchval("SELECT count(*) FROM py_idem3.be_idempotency") == 0
        assert await a.fetchval("SELECT count(*) FROM py_idem3.effects") == 0
        _, replayed = await idem.do(d, request(), command())
        assert not replayed, "the key can be used again"
    finally:
        await a.close()
        await pool.close()


async def test_an_expired_record_is_claimed_afresh_and_the_cleanup_removes_only_expired():  # P4.25
    d, pool, a = await idem_db("py_idem4")
    try:
        await idem.do(d, request("old"), command(1))
        await idem.do(d, request("new"), command(2))
        await a.execute("UPDATE py_idem4.be_idempotency SET expires_at = now() - interval '1 second' WHERE key = 'old'")
        out, replayed = await idem.do(d, request("old", body=b"{}"), command(3))
        assert not replayed, "an expired record not yet cleaned up counts as gone"
        await a.execute("UPDATE py_idem4.be_idempotency SET expires_at = now() - interval '1 second' WHERE key = 'old'")
        assert await idem.cleanup(d) == 1
        assert [r["key"] for r in await a.fetch("SELECT key FROM py_idem4.be_idempotency")] == ["new"]
    finally:
        await a.close()
        await pool.close()


def test_the_cleanup_is_an_hourly_singleton():
    job = idem.cleanup_job(None)  # type: ignore[arg-type]
    assert (job.name, job.kind, job.interval) == ("be.idempotency.cleanup", "singleton", 3600.0)
