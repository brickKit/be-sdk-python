"""What an adapter learns from a failed call (be-protocol P13.5): the status a dependency answered outside the protocol
(status_of), and whether the request certainly did not leave (not_sent): a refused connection or a call the SDK held
back, never an answer that came back."""
import socket
import time

import pytest
from aiohttp import web

from besdk import errs, reqctx
from besdk.calls.client import Member, not_sent, status_of


@pytest.fixture
async def server():
    async def plain(request):
        return web.Response(status=int(request.match_info["status"]), text="no")

    app = web.Application()
    app.router.add_route("*", "/s/{status}", plain)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    yield f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"  # type: ignore[union-attr]
    await runner.cleanup()


def closed() -> str:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}"


async def failure(url: str, path: str, method: str = "POST") -> errs.Error:
    out = Member()
    try:
        with reqctx.deadline_scope(time.monotonic() + 5), pytest.raises(errs.Error) as e:
            await out.http("external", url).do(method, path, {"x": 1})
        return e.value
    finally:
        await out.close()


@pytest.mark.parametrize("status", [404, 422, 429])
async def test_the_status_outside_the_protocol(server, status):
    e = await failure(server, f"/s/{status}", "GET" if status == 404 else "POST")
    assert status_of(e) == status
    assert not not_sent(e), "an answer came back: the request reached it"


async def test_a_refused_connection_was_not_sent():
    e = await failure(closed(), "/s/200")
    assert not_sent(e) and status_of(e) == 0


async def test_a_gateway_error_may_have_reached(server):
    e = await failure(server, "/s/502")
    assert not not_sent(e) and status_of(e) == 502
