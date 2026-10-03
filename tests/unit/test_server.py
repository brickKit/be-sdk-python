"""The listening socket and server timeouts (P1.13, P3.5): dual-stack, slow headers and slow bodies cut."""
import asyncio
import io
import socket

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from besdk import logs
from besdk.http.server import HttpServer, listen


def _app():
    async def ok(request):
        await request.body()
        return PlainTextResponse("ok")

    return Starlette(routes=[Route("/healthz", ok, methods=["GET", "POST"])])


@pytest.fixture
async def server():
    sock = listen(0)
    srv = HttpServer(_app(), sock, logs.member_logger("c/x", "1", stream=io.StringIO()), grace=1.0,
                     header_timeout=0.3, body_timeout=0.6)
    await srv.start()
    yield srv
    await srv.stop()


async def _get(host: str, port: int) -> bytes:
    r, w = await asyncio.open_connection(host, port)
    w.write(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    data = await asyncio.wait_for(r.read(), 2)
    w.close()
    return data


async def test_dual_stack(server):
    assert (await _get("127.0.0.1", server.port)).startswith(b"HTTP/1.1 200")
    if socket.has_ipv6:
        assert (await _get("::1", server.port)).startswith(b"HTTP/1.1 200")


async def _closed_after(port: int, payload: bytes, within: float) -> float:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(payload)
    t0 = asyncio.get_running_loop().time()
    data = await asyncio.wait_for(r.read(), within)
    w.close()
    assert data == b"" or not data.startswith(b"HTTP/1.1 200")
    return asyncio.get_running_loop().time() - t0


async def test_slow_headers_are_cut(server):
    took = await _closed_after(server.port, b"GET /healthz HTTP/1.1\r\nHost: x\r\n", 2)
    assert 0.2 < took < 1.5


async def test_slow_body_is_cut(server):
    took = await _closed_after(server.port, b"POST /healthz HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\nabc", 3)
    assert 0.4 < took < 2


async def test_idle_keep_alive_then_new_request_resets_header_timer(server):
    r, w = await asyncio.open_connection("127.0.0.1", server.port)
    w.write(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
    head = await asyncio.wait_for(r.readuntil(b"ok"), 2)
    assert head.startswith(b"HTTP/1.1 200")
    w.write(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
    assert (await asyncio.wait_for(r.read(), 2)).startswith(b"HTTP/1.1 200")
    w.close()
