"""Supervised background work (P1.7): failures are recovered, logged, counted and restarted with
exponential backoff; one piece stopping never stops another; stop cancels everything."""
import asyncio
import io
import json

from besdk import logs
from besdk.supervise import Supervisor


async def test_failing_work_restarts_with_backoff_and_others_keep_running():
    buf = io.StringIO()
    sup = Supervisor(logs.member_logger("c/x", "1", stream=buf), initial=0.01, maximum=0.04)
    runs = {"bad": 0, "good": 0}

    async def bad():
        runs["bad"] += 1
        raise RuntimeError("boom")

    async def good():
        while True:
            runs["good"] += 1
            await asyncio.sleep(0.005)

    sup.start("bad", bad)
    sup.start("good", good)
    await asyncio.sleep(0.15)
    await sup.stop()
    assert runs["bad"] >= 3 and runs["good"] >= 10
    lines = [json.loads(x) for x in buf.getvalue().splitlines()]
    assert any(x["msg"] == "background_work_failed" and x["work"] == "bad" and "boom" in x["error"] for x in lines)
    assert sup.failures["bad"] >= 3


async def test_stop_cancels_and_waits():
    sup = Supervisor(logs.member_logger("c/x", "1", stream=io.StringIO()))
    cancelled = asyncio.Event()

    async def forever():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    sup.start("forever", forever)
    await asyncio.sleep(0)
    await sup.stop()
    assert cancelled.is_set()


async def test_work_that_returns_is_not_restarted():
    sup = Supervisor(logs.member_logger("c/x", "1", stream=io.StringIO()), initial=0.001)
    n = 0

    async def once():
        nonlocal n
        n += 1

    sup.start("once", once)
    await asyncio.sleep(0.05)
    await sup.stop()
    assert n == 1
