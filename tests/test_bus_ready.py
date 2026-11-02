"""P6.34: the bus may start after the component; the component waits for it, as for the database (P5.10)."""
import asyncio
import io
import socket
import time

import pytest

from besdk import obs
from besdk.db.pool import StoppedWaiting
from besdk.events.bus import BusRefused, ready_bus


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.parametrize("url", [f"nats://127.0.0.1:{free_port()}",
                                 f"pgqueue://u:p@127.0.0.1:{free_port()}/db?schema=be_bus"])
async def test_nothing_listens_so_it_waits_until_stopped(url):
    out, stop = io.StringIO(), asyncio.Event()
    asyncio.get_running_loop().call_later(2.5, stop.set)
    start = time.monotonic()
    with pytest.raises(StoppedWaiting):
        await ready_bus(url, "demo/x", obs.new_logger(obs.Member("demo/x", "3.0.0"), "info", out), stop)
    assert time.monotonic() - start >= 2, "waited until stopped"
    assert "waiting for the bus" in out.getvalue()
    assert "u:p@" not in out.getvalue(), "the credentials never reach the logs"


async def test_a_wrong_scheme_is_refused_at_once():
    start = time.monotonic()
    with pytest.raises(BusRefused):
        await ready_bus("amqp://127.0.0.1:5672", "demo/x", obs.new_logger(obs.Member("demo/x", "3.0.0")), asyncio.Event())
    assert time.monotonic() - start < 1
