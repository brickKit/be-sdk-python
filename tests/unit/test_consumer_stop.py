"""A consumer that stops must leave no pull request behind (P1.6, P12.5).

The pull request of a fetch stays registered at the server until it expires. A message published
after the instance stopped is handed to that request, which nobody reads, and is redelivered only
after the ack wait (30 s). So a stopping consumer sends no new pull request, lets the running one
end, and hands back at once whatever it brings."""
import asyncio
import logging
from types import SimpleNamespace

import pytest

from besdk.events import consumer as C
from besdk.events.model import Subscription

SUBJECT = "conformance.owner.updated.v1"


class _Msg:
    def __init__(self, log: list):
        self.log = log

    async def nak(self, delay=None) -> None:
        self.log.append("nak")


class _Psub:
    """One fetch that stays in flight until ``release`` is set, then brings one message."""

    def __init__(self, log: list):
        self.log, self.fetching, self.release, self.timeouts = log, asyncio.Event(), asyncio.Event(), []

    async def fetch(self, batch: int = 1, timeout: float = 5.0):
        self.timeouts.append(timeout)
        self.log.append("fetch-start")
        self.fetching.set()
        await self.release.wait()
        self.log.append("fetch-end")
        return [_Msg(self.log)]

    async def unsubscribe(self) -> None:
        self.log.append("unsubscribe")


def _consumer(log: list, psub: _Psub) -> C.Consumer:
    async def nothing(*a, **kw):
        return None

    async def info(stream, durable):
        return SimpleNamespace(config=SimpleNamespace(ack_wait=C.ACK_WAIT, max_ack_pending=C.MAX_ACK_PENDING,
                                                      max_deliver=-1, backoff=None))

    async def bind(**kw):
        return psub

    async def handler(event) -> None:
        log.append("handled")

    bus = SimpleNamespace(ensure_stream_for=nothing, ensure_dlq=nothing,
                          js=SimpleNamespace(consumer_info=info, pull_subscribe_bind=bind))
    return C.Consumer(member="conformance/widget-py", sub=Subscription(subject=SUBJECT, run=handler), store=None,
                      bus=bus, contract=SimpleNamespace(get=lambda s: None), max_deliver=3, backoff_ns=[10**9],
                      metrics=None, logger=logging.getLogger("test"), tracer=None, propagator=None)


async def test_stop_waits_for_the_running_fetch_and_hands_its_messages_back():
    log: list = []
    psub = _Psub(log)
    c = _consumer(log, psub)
    run = asyncio.create_task(c.run())
    await asyncio.wait_for(psub.fetching.wait(), 2)

    stop = asyncio.create_task(c.stop())
    await asyncio.sleep(0.05)
    assert not stop.done(), "stop returned while a pull request was still waiting at the server"

    psub.release.set()
    await asyncio.wait_for(stop, 2)
    await asyncio.wait_for(run, 2)  # the loop ends by itself: no new fetch
    assert log == ["fetch-start", "fetch-end", "nak", "unsubscribe"]
    assert all(t <= 1.0 for t in psub.timeouts), "a pull request may outlive a stop by its timeout: keep it short"


async def test_cancel_during_a_fetch_still_lets_it_end():
    log: list = []
    psub = _Psub(log)
    c = _consumer(log, psub)
    run = asyncio.create_task(c.run())
    await asyncio.wait_for(psub.fetching.wait(), 2)

    run.cancel()
    await asyncio.sleep(0.05)
    assert not run.done(), "the consumer ended while its pull request was still waiting at the server"

    psub.release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(run, 2)
    assert log == ["fetch-start", "fetch-end", "nak", "unsubscribe"]
