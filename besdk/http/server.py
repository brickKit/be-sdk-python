"""The HTTP server of one member (be-protocol P1.13, P3.5): uvicorn with the httptools parser on a
dual-stack socket, plus the timeouts uvicorn lacks: request headers within 5 s, the whole request within
30 s, idle keep-alive 120 s. The runtime owns signals; uvicorn never installs handlers here."""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Any

import uvicorn
from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

HEADER_TIMEOUT = 5.0
BODY_TIMEOUT = 30.0
KEEP_ALIVE = 120


def listen(port: int) -> socket.socket:
    """One socket on all interfaces, IPv4 and IPv6 (``::`` with V6ONLY off); IPv4 only when the host has no IPv6."""
    try:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        addr: tuple = ("::", port)
    except OSError:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = ("0.0.0.0", port)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(addr)
    sock.listen(1024)
    sock.setblocking(False)
    return sock


def protocol_class(header_timeout: float, body_timeout: float) -> type:
    class BeHttpProtocol(HttpToolsProtocol):
        """Cuts a connection whose request headers or body arrive too slowly (slowloris, P3.5)."""

        _cut: asyncio.TimerHandle | None = None

        def _arm(self, seconds: float) -> None:
            self._disarm()
            self._cut = self.loop.call_later(seconds, self._close)

        def _disarm(self) -> None:
            if self._cut is not None:
                self._cut.cancel()
                self._cut = None

        def _close(self) -> None:
            if not self.transport.is_closing():
                self.transport.close()

        def connection_made(self, transport: Any) -> None:  # type: ignore[override]
            super().connection_made(transport)
            self._arm(header_timeout)

        def connection_lost(self, exc: Exception | None) -> None:
            self._disarm()
            super().connection_lost(exc)

        def on_message_begin(self) -> None:
            super().on_message_begin()
            self._arm(header_timeout)

        def on_headers_complete(self) -> None:
            super().on_headers_complete()
            self._arm(body_timeout)

        def on_message_complete(self) -> None:
            self._disarm()
            super().on_message_complete()

    return BeHttpProtocol


def _quiet_uvicorn() -> None:
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers = [logging.NullHandler()]
        lg.propagate = False


class HttpServer:
    """Serve an ASGI app on a prepared socket; ``stop`` drains in-flight requests within ``grace``."""

    def __init__(self, app: Any, sock: socket.socket, logger: logging.Logger, *, grace: float = 25.0,
                 header_timeout: float = HEADER_TIMEOUT, body_timeout: float = BODY_TIMEOUT):
        _quiet_uvicorn()
        self.sock, self.logger = sock, logger
        cfg = uvicorn.Config(app, http=protocol_class(header_timeout, body_timeout), lifespan="off",
                             log_config=None, access_log=False, timeout_keep_alive=KEEP_ALIVE,
                             timeout_graceful_shutdown=int(max(1, grace)), server_header=False, ws="none")
        self.server = uvicorn.Server(cfg)
        self._task: asyncio.Task | None = None

    @property
    def port(self) -> int:
        return self.sock.getsockname()[1]

    async def start(self) -> None:
        self._task = asyncio.create_task(self.server._serve(sockets=[self.sock]), name="besdk:http")
        while not self.server.started:
            if self._task.done():
                self._task.result()
            await asyncio.sleep(0.005)

    async def stop(self, grace: float | None = None) -> None:
        """Stop accepting, let in-flight requests finish within the grace given at construction, close."""
        self.server.should_exit = True
        if self._task is not None:
            await self._task
        self.sock.close()
