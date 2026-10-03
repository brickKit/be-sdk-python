"""The physical connection pool (be-protocol P10.5, P10.11, P2.9) on asyncpg 0.31.

Standalone, one pool of ``PG_POOL_MAX`` connections logged in as ``PG_USER``; in a shell, one pool of
``min(Σ members' PG_POOL_MAX, the shell's PG_POOL_MAX)`` logged in as the shell's role, each member
limited by its own budget (``besdk.store.store.Store``). The password is read from its file for every
new connection, so a rotated password applies to the connections opened after the change. The session
time zone is UTC (a start-up parameter, never a ``SET``). The prepared-statement cache stays on: every
statement carries its member's ``/* be:<schema> */`` prefix (repro r1-04), and the cache grows with the
number of members. The pool keeps no idle minimum (``PG_POOL_MIN_IDLE`` is retired in be-protocol rc.2):
connections open on demand and close after ``PG_CONN_MAX_IDLE_TIME``.
"""

from __future__ import annotations

import contextlib
import time
from typing import Any, AsyncIterator, Callable

import asyncpg

CACHE_PER_MEMBER = 100


class PhysicalPool:
    def __init__(self, *, host: str, port: int, database: str, user: str, password: Callable[[], str],
                 max_size: int, min_size: int = 0, members: int = 1, max_lifetime: float = 1800.0,
                 max_idle: float = 300.0):
        self.kw = dict(host=host, port=port, database=database, user=user, password=password,
                       max_size=max_size, min_size=min(min_size, max_size),
                       max_inactive_connection_lifetime=max_idle,
                       statement_cache_size=CACHE_PER_MEMBER * max(1, members),
                       server_settings={"TimeZone": "UTC", "application_name": "besdk"})
        self.max_lifetime = max_lifetime
        self.max_size = max_size
        self.pool: asyncpg.Pool | None = None
        self._born: dict[int, float] = {}
        self.server_version = 0

    async def _init(self, conn: asyncpg.Connection) -> None:
        self._born[id(conn)] = time.monotonic()

    async def open(self) -> None:
        """Create the pool; raises when PostgreSQL is not reachable (the caller retries with backoff)."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(init=self._init, **self.kw)
            async with self.pool.acquire() as c:
                self.server_version = int(await c.fetchval("SELECT current_setting('server_version_num')"))

    @property
    def ready(self) -> bool:
        return self.pool is not None

    async def acquire(self, timeout: float) -> Any:
        assert self.pool is not None
        self._maybe_expire()
        return await self.pool.acquire(timeout=timeout)

    async def release(self, conn: Any) -> None:
        assert self.pool is not None
        await self.pool.release(conn)

    def _maybe_expire(self) -> None:
        """``PG_CONN_MAX_LIFETIME``: once the oldest connection is past it, connections are replaced as they
        are released (asyncpg's generation mechanism)."""
        if self._born and time.monotonic() - min(self._born.values()) > self.max_lifetime and self.pool is not None:
            self._born.clear()
            self.pool._generation += 1  # same effect as expire_connections(), without awaiting

    async def expire_all(self) -> None:
        if self.pool is not None:
            await self.pool.expire_connections()

    @contextlib.asynccontextmanager
    async def raw(self) -> AsyncIterator[asyncpg.Connection]:
        """A connection outside the store, for the SDK's own diagnostics and tests only."""
        assert self.pool is not None
        async with self.pool.acquire() as c:
            yield c

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
