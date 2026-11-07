"""Background work items (protocol chapter 07) against a real database: identities and operation IDs, claims and
takeovers, the claim check of every commit, stops that give the lease back, effect classes, failures and dead letters,
business waits, singletons, deadlines, wake-ups, items due later, every jobs, the claim order and the idle cost."""
import asyncio
import datetime as dt
import io
import time

import pytest

from besdk import obs
from besdk.work import (EVERY, IDEMPOTENT_EXTERNAL, LOCAL, NON_IDEMPOTENT_EXTERNAL, QUEUE, SINGLETON, Job, Lost,
                        NotEffective, Permanent, Runner, Wait)
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
LOG = obs.new_logger(obs.Member("demo/work", "3.0.0"), "error", io.StringIO())
EFFECTS = {"0001_effects.up.sql": "CREATE TABLE IF NOT EXISTS effects (item text NOT NULL, n int NOT NULL);"}


async def work_db(name):
    """A migrated member schema with an effects table, emptied; its DB and an admin connection."""
    v, _ = await migrated(name, EFFECTS)
    d, pool = await open_db(name, v)
    a = await pgtest.admin()
    await a.execute(f"DELETE FROM {name}.be_work_item; DELETE FROM {name}.effects; DELETE FROM {name}.be_cron_watermark")
    return d, pool, a


async def state(a, name, iid):
    return await a.fetchrow(f"""SELECT state, attempts, epoch, pause_reason, operation_id, holder, lease_until,
        lease_until <= now() AS expired, run_at, last_error, progress, outcome_unknown FROM {name}.be_work_item
        WHERE id = $1""", iid)


async def enqueue(d, runner, job, args=None, at=None):
    return await d.tx(lambda tx: runner.enqueue(tx, job, args, at))


async def expire(a, name, iid):
    """The holder crashed: its lease ran out."""
    await a.execute(f"UPDATE {name}.be_work_item SET lease_until = now() - interval '1 second' WHERE id = $1", iid)


async def nothing(r):
    return None


async def until(f, timeout=10.0, what=""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await f():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(what or "never")


async def test_identities_and_the_rollback():  # P7.2, P7.3, CK-WK-01
    d, pool, a = await work_db("py_wk1")
    try:
        w = Runner(d, "demo/wk1", LOG)
        w.add(Job("demo.q", QUEUE, nothing, LOCAL))

        async def rolled_back(tx):
            await w.enqueue(tx, "demo.q", {"n": 1})
            raise RuntimeError("the business write failed")

        with pytest.raises(Exception):
            await d.tx(rolled_back)
        assert await a.fetchval("SELECT count(*) FROM py_wk1.be_work_item") == 0, "rolled back with the business write"
        iid = await enqueue(d, w, "demo.q", {"n": 2})
        s = await state(a, "py_wk1", iid)
        assert s["state"] == "pending" and s["operation_id"] == f"{iid}:demo.q"
        r = await w.claim()
        assert r is not None and r.id == iid and r.args == {"n": 2} and r.attempts == 1
        with pytest.raises(Exception) as e:  # raised inside the transaction: the database layer wraps it
            await enqueue(d, w, "demo.missing")
        assert isinstance(e.value.__cause__, ValueError) and "not a declared queue job" in str(e.value.__cause__)
    finally:
        await a.close()
        await pool.close()


async def test_a_taken_over_holder_commits_nothing():  # P7.5, P7.6, CK-WK-02, CK-WK-03
    d, pool, a = await work_db("py_wk2")
    try:
        job = Job("demo.q", QUEUE, nothing, LOCAL)
        wa, wb = Runner(d, "demo/wk2", LOG), Runner(d, "demo/wk2", LOG)
        wa.add(job)
        wb.add(Job("demo.q", QUEUE, nothing, LOCAL))
        iid = await enqueue(d, wa, "demo.q")
        ra = await wa.claim()
        assert await wb.claim() is None, "a live lease is not taken over"
        await expire(a, "py_wk2", iid)
        rb = await wb.claim()
        assert rb is not None and rb.id == iid
        assert rb.claim[0] == ra.claim[0] + 1 and rb.attempts == 2, "taken over: the epoch and the attempts go up"
        assert rb.operation_id == ra.operation_id, "the operation ID is kept on takeover (P7.2)"
        with pytest.raises(Lost):
            await ra.complete()
        await a.execute("UPDATE py_wk2.be_recovery_generation SET generation = generation + 1")
        try:
            with pytest.raises(Lost):
                await rb.complete()
        finally:
            await a.execute("UPDATE py_wk2.be_recovery_generation SET generation = generation - 1")
        assert (await state(a, "py_wk2", iid))["state"] == "running", "neither holder committed"
    finally:
        await a.close()
        await pool.close()


async def test_stop_gives_the_lease_and_the_attempt_back():  # P7.8, CK-WK-04
    d, pool, a = await work_db("py_wk3")
    try:
        started = asyncio.Event()

        async def slow(r):
            started.set()
            await asyncio.sleep(60)

        wa = Runner(d, "demo/wk3", LOG, poll=0.05)
        wa.add(Job("demo.q", QUEUE, slow, LOCAL))
        iid = await enqueue(d, wa, "demo.q")
        await wa.start()
        await asyncio.wait_for(started.wait(), 10)
        await wa.stop()
        s = await state(a, "py_wk3", iid)
        assert s["state"] == "running" and s["expired"] and s["attempts"] == 0, dict(s)
        wb = Runner(d, "demo/wk3", LOG)
        wb.add(Job("demo.q", QUEUE, nothing, LOCAL))
        rb = await wb.claim()
        assert rb is not None and rb.attempts == 1, "another replica takes it at once, the attempt not spent"
    finally:
        await a.close()
        await pool.close()


async def test_a_local_effect_happens_once_after_a_crash():  # P7.10, CK-WK-06
    d, pool, a = await work_db("py_wk4")
    try:
        async def effect(r):
            await r.complete(lambda tx: tx.execute("INSERT INTO effects VALUES ($1, 1)", r.id))

        wa, wb = Runner(d, "demo/wk4", LOG), Runner(d, "demo/wk4", LOG, poll=0.05)
        for w in (wa, wb):
            w.add(Job("demo.q", QUEUE, effect, LOCAL))
        iid = await enqueue(d, wa, "demo.q")
        ra = await wa.claim()  # A crashes before its commit
        await expire(a, "py_wk4", iid)
        await wb.start()
        try:
            await until(lambda: _done(a, "py_wk4", iid), what="B completes the item")
        finally:
            await wb.stop()
        with pytest.raises(Lost):
            await effect(ra)  # A comes back: its commit, with its effect, is refused
        assert await a.fetchval("SELECT count(*) FROM py_wk4.effects") == 1
    finally:
        await a.close()
        await pool.close()


async def _done(a, name, iid, st="done"):
    return (await state(a, name, iid))["state"] == st


async def test_a_non_idempotent_call_is_never_sent_twice():  # P7.12, CK-WK-07
    d, pool, a = await work_db("py_wk5")
    try:
        w = Runner(d, "demo/wk5", LOG)
        w.add(Job("demo.pay", QUEUE, nothing, NON_IDEMPOTENT_EXTERNAL))
        iid = await enqueue(d, w, "demo.pay")
        r = await w.claim()
        await r.before_call()  # then the holder crashes: the call may have been sent
        await expire(a, "py_wk5", iid)
        assert await w.claim() is None
        s = await state(a, "py_wk5", iid)
        assert s["state"] == "paused" and s["pause_reason"] == "outcome_unknown"

        async def refused(r):
            await r.before_call()
            raise NotEffective("the other side refused it")

        async def broken(r):
            await r.before_call()
            raise RuntimeError("the connection broke after sending")

        for handle, want in ((refused, "retry_wait"), (broken, "paused")):
            w2 = Runner(d, "demo/wk5", LOG)
            w2.add(Job("demo.pay", QUEUE, handle, NON_IDEMPOTENT_EXTERNAL))
            iid = await enqueue(d, w2, "demo.pay")
            await w2.run(await w2.claim())
            assert (await state(a, "py_wk5", iid))["state"] == want, handle.__name__
    finally:
        await a.close()
        await pool.close()


async def test_retries_then_dead_with_its_dead_letter():  # P7.13, P7.14, CK-WK-08
    d, pool, a = await work_db("py_wk6")
    try:
        async def fails(r):
            raise RuntimeError("not yet")

        async def letter(tx, item, cause):
            await tx.execute("INSERT INTO effects VALUES ($1, 99)", item.id)

        w = Runner(d, "demo/wk6", LOG)
        w.add(Job("demo.q", QUEUE, fails, LOCAL, max_attempts=2, dead_letter=letter))
        w.add(Job("demo.p", QUEUE, lambda r: _raise(Permanent("bad input")), LOCAL))
        iid = await enqueue(d, w, "demo.q")
        await w.run(await w.claim())
        s = await state(a, "py_wk6", iid)
        assert s["state"] == "retry_wait" and s["last_error"] == "not yet"
        secs = await a.fetchval("SELECT extract(epoch FROM run_at - now()) FROM py_wk6.be_work_item WHERE id = $1", iid)
        assert 0.5 < secs <= 1.0, "the first backoff is 1 s"
        assert await w.claim() is None, "not before its backoff"
        await a.execute("UPDATE py_wk6.be_work_item SET run_at = now() WHERE id = $1", iid)
        await w.run(await w.claim())
        assert (await state(a, "py_wk6", iid))["state"] == "dead"
        assert await a.fetchval("SELECT n FROM py_wk6.effects WHERE item = $1", iid) == 99, "the dead letter, same tx"
        await a.execute("UPDATE py_wk6.be_work_item SET run_at = now() - interval '1 hour' WHERE id = $1", iid)
        assert await w.claim() is None, "dead is never retried by itself"
        pid = await enqueue(d, w, "demo.p")
        await w.run(await w.claim())
        assert (await state(a, "py_wk6", pid))["state"] == "dead", "a permanent failure is dead at once"
    finally:
        await a.close()
        await pool.close()


async def _raise(e):
    raise e


async def test_a_business_wait_spends_no_attempt_and_ends_past_its_limit():  # P7.13
    d, pool, a = await work_db("py_wk7")
    try:
        w = Runner(d, "demo/wk7", LOG)
        w.add(Job("demo.q", QUEUE, lambda r: _raise(Wait("the source is not proven yet")), LOCAL, max_wait=3600))
        w.add(Job("demo.short", QUEUE, lambda r: _raise(Wait("still processing")), LOCAL, max_wait=1))
        iid = await enqueue(d, w, "demo.q")
        for _ in range(3):
            await w.run(await w.claim())
            s = await state(a, "py_wk7", iid)
            assert s["state"] == "retry_wait" and s["attempts"] == 0, "a wait spends no attempt"
            await a.execute("UPDATE py_wk7.be_work_item SET run_at = now() WHERE id = $1", iid)
        sid = await enqueue(d, w, "demo.short")
        await w.run(await w.claim())
        await a.execute("UPDATE py_wk7.be_work_item SET run_at = now(), waiting_since = now() - interval '5 seconds' "
                        "WHERE id = $1", sid)
        while (r := await w.claim()) is not None and r.id != sid:
            await w.run(r)
        await w.run(r)
        assert (await state(a, "py_wk7", sid))["state"] == "dead", "past its max_wait"
    finally:
        await a.close()
        await pool.close()


async def test_a_singleton_continues_from_its_committed_progress():  # P7.17, CK-WK-09
    d, pool, a = await work_db("py_wk8")
    try:
        seen = []

        async def segments(r):
            done = (r.progress or {}).get("done", 0)
            seen.append((done, r.operation_id))
            if done == 0:
                await r.checkpoint({"done": 1})
                raise asyncio.CancelledError  # stands in for the crash right after the checkpoint

        wa = Runner(d, "demo/wk8", LOG)
        wa.add(Job("demo.s", SINGLETON, segments, LOCAL, interval=3600))
        await wa._ensure_singleton(wa.jobs["demo.s"])
        ra = await wa.claim()
        with pytest.raises(asyncio.CancelledError):
            await segments(ra)
        segment_op = ra.operation_id
        await expire(a, "py_wk8", "demo.s")
        wb = Runner(d, "demo/wk8", LOG)
        wb.add(Job("demo.s", SINGLETON, segments, LOCAL, interval=3600))
        rb = await wb.claim()
        assert rb.progress == {"done": 1} and rb.operation_id == segment_op, "from the committed segment"
        await wb.run(rb)
        s = await state(a, "py_wk8", "demo.s")
        assert s["state"] == "pending" and s["attempts"] == 0 and s["progress"] is None
        assert await a.fetchval("SELECT run_at > now() + interval '59 minutes' FROM py_wk8.be_work_item "
                                "WHERE id = 'demo.s'"), "again after its interval"
    finally:
        await a.close()
        await pool.close()


async def test_a_run_past_its_deadline_is_retried():
    d, pool, a = await work_db("py_wk9")
    try:
        w = Runner(d, "demo/wk9", LOG)
        w.add(Job("demo.q", QUEUE, lambda r: asyncio.sleep(5), LOCAL, timeout=0.2))
        iid = await enqueue(d, w, "demo.q")
        t = time.monotonic()
        await w.run(await w.claim())
        assert time.monotonic() - t < 2
        s = await state(a, "py_wk9", iid)
        assert s["state"] == "retry_wait" and "deadline" in s["last_error"]
    finally:
        await a.close()
        await pool.close()


async def test_a_commit_wakes_a_worker_and_a_later_item_waits():  # P7.21, P7.24
    d, pool, a = await work_db("py_wk10")
    try:
        w = Runner(d, "demo/wk10", LOG, poll=10)
        w.add(Job("demo.q", QUEUE, nothing, LOCAL))
        await w.start()
        try:
            await asyncio.sleep(0.2)  # past the first poll
            t = time.monotonic()
            iid = await enqueue(d, w, "demo.q")
            await until(lambda: _done(a, "py_wk10", iid), 3, "woken by the commit, not the 10 s poll")
            assert time.monotonic() - t < 1
        finally:
            await w.stop()
        w = Runner(d, "demo/wk10", LOG, poll=0.1)
        w.add(Job("demo.q", QUEUE, nothing, LOCAL))
        at = await a.fetchval("SELECT now() + interval '1.5 seconds'")
        iid = await enqueue(d, w, "demo.q", at=at)
        await w.start()
        try:
            await asyncio.sleep(0.8)
            assert (await state(a, "py_wk10", iid))["state"] == "pending", "not before its run_at"
            await until(lambda: _done(a, "py_wk10", iid), 5, "done once due")
        finally:
            await w.stop()
    finally:
        await a.close()
        await pool.close()


async def test_every_jobs_run_on_their_timer():  # P7.18
    d, pool, a = await work_db("py_wk11")
    try:
        ticks = []

        async def tick(r):
            assert r is None
            ticks.append(time.monotonic())

        w = Runner(d, "demo/wk11", LOG)
        w.add(Job("demo.tick", EVERY, tick, interval=0.1))
        await w.start()
        await asyncio.sleep(0.55)
        await w.stop()
        assert 4 <= len(ticks) <= 7
        assert await a.fetchval("SELECT count(*) FROM py_wk11.be_work_item") == 0, "every jobs have no items"
    finally:
        await a.close()
        await pool.close()


async def test_an_expired_lease_is_claimed_before_due_items():  # P7.22
    d, pool, a = await work_db("py_wk12")
    try:
        w = Runner(d, "demo/wk12", LOG)
        w.add(Job("demo.q", QUEUE, nothing, LOCAL))
        old = await enqueue(d, w, "demo.q")
        await w.claim()
        await expire(a, "py_wk12", old)
        for _ in range(3):
            await enqueue(d, w, "demo.q")
        assert (await w.claim()).id == old, "the taken-over item first: it does not starve behind new ones"
    finally:
        await a.close()
        await pool.close()


async def test_an_idle_member_claims_once_a_poll():  # P7.23
    d, pool, a = await work_db("py_wk13")
    try:
        w = Runner(d, "demo/wk13", LOG, poll=0.25, concurrency=4)
        w.add(Job("demo.q", QUEUE, nothing, LOCAL))
        await w.start()
        await asyncio.sleep(1.1)
        await w.stop()
        assert 3 <= w.claims <= 6, f"{w.claims} claims in 1.1 s at a 0.25 s poll with 4 workers (not one per worker)"
    finally:
        await a.close()
        await pool.close()


async def test_an_idempotent_takeover_keeps_its_keys_and_recorded_calls():  # P7.11, CK-WK-06
    d, pool, a = await work_db("py_wk14")
    try:
        w = Runner(d, "demo/wk14", LOG)
        w.add(Job("demo.q", QUEUE, nothing, IDEMPOTENT_EXTERNAL))
        iid = await enqueue(d, w, "demo.q")
        ra = await w.claim()
        await ra.record_call(ra.sub_key("charge"), "unknown")
        await expire(a, "py_wk14", iid)
        rb = await w.claim()
        assert rb.sub_key("charge") == f"{iid}:demo.q:charge" and rb.call_results == {f"{iid}:demo.q:charge": "unknown"}
    finally:
        await a.close()
        await pool.close()


def test_declarations_are_checked():
    for bad in (Job("", QUEUE, nothing, LOCAL), Job("x", QUEUE, nothing), Job("x", "weekly", nothing, LOCAL),
                Job("x", SINGLETON, nothing, LOCAL), Job("x", EVERY, nothing)):
        with pytest.raises(ValueError):
            bad.prepare()
    w = Runner(None, "demo/x", LOG)  # type: ignore[arg-type]
    w.add(Job("x", QUEUE, nothing, LOCAL))
    with pytest.raises(ValueError):
        w.add(Job("x", QUEUE, nothing, LOCAL))


def test_the_backoff():
    from besdk.work import backoff
    assert [backoff(n) for n in (0, 1, 2, 3, 9, 10, 30)] == [1, 1, 2, 4, 256, 300, 300]
    assert dt.timedelta(seconds=backoff(10)) == dt.timedelta(minutes=5)
