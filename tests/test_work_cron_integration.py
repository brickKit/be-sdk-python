"""Cron jobs against a real database (P7.9, P7.16, P7.23): two replicas materialize and claim a slot once; a job
seen for the first time starts at now without history; a downtime is backfilled by the policy, the backlog beyond its
limit paused; the scheduler opens no transaction before a job's next slot."""
import asyncio
import datetime as dt
import io

import pytest

from besdk import obs
from besdk.work import ALL, CRON, LOCAL, Job, Runner
from tests.test_work_integration import EFFECTS, work_db

pytestmark = pytest.mark.integration
LOG = obs.new_logger(obs.Member("demo/cron", "3.0.0"), "error", io.StringIO())


async def test_two_replicas_run_a_slot_once():  # P7.9, CK-WK-05
    d, pool, a = await work_db("py_cr1")
    try:
        async def effect(r):
            await r.complete(lambda tx: tx.execute("INSERT INTO effects VALUES ($1, 1)", r.id))

        runners = [Runner(d, "demo/cr1", LOG) for _ in range(2)]
        for w in runners:
            w.add(Job("demo.c", CRON, effect, LOCAL, cron="* * * * *", time_zone="UTC"))
        start = await a.fetchval("SELECT date_trunc('minute', now()) - interval '10 minutes'")
        await a.execute("INSERT INTO py_cr1.be_cron_watermark (job, watermark_at) VALUES ('demo.c', $1)", start)
        await asyncio.gather(*(w.materialize("demo.c") for w in runners))
        items = await a.fetch("SELECT id, state FROM py_cr1.be_work_item")
        assert len(items) == 1 and items[0]["state"] == "pending", "latest: the last missed slot, materialized once"

        async def drain(w):
            while (r := await w.claim()) is not None:
                await w.run(r)

        await asyncio.gather(*(drain(w) for w in runners))
        assert await a.fetchval("SELECT count(*) FROM py_cr1.effects") == 1, "and run once"
        await asyncio.gather(*(w.materialize("demo.c") for w in runners))
        assert await a.fetchval("SELECT count(*) FROM py_cr1.be_work_item") == 1, "the done slot is not materialized again"
    finally:
        await a.close()
        await pool.close()


async def test_a_new_job_starts_at_now_and_a_backlog_beyond_its_limit_pauses():  # P7.16
    d, pool, a = await work_db("py_cr2")
    try:
        w = Runner(d, "demo/cr2", LOG)
        w.add(Job("demo.c", CRON, lambda r: asyncio.sleep(0), LOCAL, cron="0 * * * *", time_zone="UTC", backfill=ALL,
                  max_backlog=3))
        now = dt.datetime(2026, 10, 17, 6, 0, 30, tzinfo=dt.UTC)
        await w.materialize("demo.c", now)
        assert await a.fetchval("SELECT count(*) FROM py_cr2.be_work_item") == 0, "first seen: no history"
        assert await a.fetchval("SELECT watermark_at FROM py_cr2.be_cron_watermark") == now
        await w.materialize("demo.c", now + dt.timedelta(hours=5))
        rows = await a.fetch("SELECT slot, state, pause_reason FROM py_cr2.be_work_item ORDER BY slot")
        assert [(r["slot"], r["state"], r["pause_reason"]) for r in rows] == [
            ("2026-10-17T07:00", "pending", None), ("2026-10-17T08:00", "pending", None),
            ("2026-10-17T09:00", "pending", None), ("2026-10-17T10:00", "paused", "backlog_exceeded"),
            ("2026-10-17T11:00", "paused", "backlog_exceeded")]
        assert await a.fetchval("SELECT last_slot FROM py_cr2.be_cron_watermark") == "2026-10-17T11:00"
        ids = await a.fetch("SELECT id, operation_id FROM py_cr2.be_work_item")
        assert all(r["id"] == "demo.c@" + r["id"].split("@")[1] for r in ids)  # P7.2: <job>@<slot>
        assert len({r["operation_id"] for r in ids}) == len(ids), "each slot its own operation ID"
    finally:
        await a.close()
        await pool.close()


async def test_the_scheduler_waits_for_the_next_slot():  # P7.23
    d, pool, a = await work_db("py_cr3")
    try:
        w = Runner(d, "demo/cr3", LOG)
        w.add(Job("demo.c", CRON, lambda r: asyncio.sleep(0), LOCAL, cron="0 0 1 1 *", time_zone="UTC"))
        await w.start()
        await asyncio.sleep(2.5)  # the scheduler's loop runs every second
        await w.stop()
        assert w.materialized == 1, "once to learn the next slot (next 1 January), then no transaction until it"
    finally:
        await a.close()
        await pool.close()
