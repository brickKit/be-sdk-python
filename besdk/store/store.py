"""``Store``: transactions bound to one member's identity (be-protocol P10.1–P10.7).

Every transaction begins with ``SET LOCAL`` role, search_path, application_name and three timeouts
(issued as one ``set_config(…, true)`` round trip, identical in effect), re-runs its body on 40001 /
40P01 up to 3 attempts, maps SQLSTATEs to the protocol's reasons, waits for a connection at most
``min(PG_POOL_ACQUIRE_TIMEOUT, remaining)`` within the member's budget, and refuses a nested
transaction. The store never uses ``PG_OWNER_USER`` (P10.12).
"""

from __future__ import annotations

import asyncio
import enum
import logging
import random
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

import asyncpg

from besdk import context, errors
from besdk.metrics import BeMetrics
from besdk.store.pool import PhysicalPool
from besdk.store.tx import Tx

if TYPE_CHECKING:
    from besdk.runtime import Runtime

T = TypeVar("T")
STATEMENT_TIMEOUT, LOCK_TIMEOUT, IDLE_TIMEOUT, SNAPSHOT_TIMEOUT = 5.0, 2.0, 30.0, 30.0
_SET = ("SELECT set_config('role', $1, true), set_config('search_path', $2, true), "
        "set_config('application_name', $3, true), set_config('statement_timeout', $4, true), "
        "set_config('lock_timeout', $5, true), set_config('idle_in_transaction_session_timeout', $6, true)")
_SET17 = _SET + ", set_config('transaction_timeout', $7, true)"


class Isolation(enum.Enum):
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"


@dataclass(frozen=True)
class DBIdentity:
    role: str
    schema: str
    member: str
    owner: str = ""


def _ms(seconds: float) -> str:
    return f"{max(1, int(seconds * 1000))}ms"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sqlstate_of(e: BaseException) -> str | None:
    return getattr(e, "sqlstate", None)


def is_unique_violation(e: BaseException) -> bool:
    return sqlstate_of(e) == "23505"


def is_lock_timeout(e: BaseException) -> bool:
    return sqlstate_of(e) == "55P03" or (isinstance(e, errors.Error) and e.reason == "LOCK_TIMEOUT")


def unavailable(cause: BaseException) -> errors.Error:
    """The database cannot be reached: 503 DEPENDENCY_UNAVAILABLE, dependency "db" (stage-B ruling)."""
    err = errors.be_error("DEPENDENCY_UNAVAILABLE", {"dependency": "db"})
    err.internal_message = f"{type(cause).__name__}: {cause}"
    return err


class Store:
    def __init__(self, pool: PhysicalPool, identity: DBIdentity, *, budget: int, acquire_timeout: float,
                 logger: logging.Logger, metrics: BeMetrics, opener: Callable[[], Awaitable[None]] | None = None):
        self.pool, self.identity = pool, identity
        self._budget = asyncio.Semaphore(budget)
        self._budget_n, self._in_use = budget, 0
        self.acquire_timeout = acquire_timeout
        self.logger, self.metrics = logger, metrics
        self._opener = opener
        self.publisher: Any = None  # besdk.events.outbox.Publisher, set when the member declares events

    @classmethod
    def for_runtime(cls, rt: "Runtime") -> "Store":
        """Standalone: a physical pool of its own; in a shell the launcher hands over the shared pool."""
        c = rt.config
        pool = rt.shared.pool
        if pool is None:
            secret = c.secret("PG_PASSWORD_FILE")
            pool = PhysicalPool(host=c.require("PG_HOST"), port=c.int("PG_PORT", 5432),
                                database=c.require("PG_DATABASE"), user=c.require("PG_USER"),
                                password=secret.current, max_size=c.int("PG_POOL_MAX", 10),
                                min_size=c.int("PG_POOL_MIN_IDLE", 2),
                                max_lifetime=c.duration("PG_CONN_MAX_LIFETIME", 1800.0),
                                max_idle=c.duration("PG_CONN_MAX_IDLE_TIME", 300.0))
            rt.shared.pool = pool
        ident = DBIdentity(c.require("PG_USER"), c.require("PG_SCHEMA"), rt.id, c.require("PG_OWNER_USER"))
        return cls(pool, ident, budget=c.int("PG_POOL_MAX", 10), acquire_timeout=c.duration("PG_POOL_ACQUIRE_TIMEOUT", 5.0),
                   logger=rt.logger, metrics=rt.metrics)

    @classmethod
    def for_member(cls, pool: PhysicalPool, *, member: str, role: str, schema: str, owner: str, budget: int,
                   logger: logging.Logger, metrics: BeMetrics, acquire_timeout: float = 5.0) -> "Store":
        return cls(pool, DBIdentity(role, schema, member, owner), budget=budget, acquire_timeout=acquire_timeout,
                   logger=logger, metrics=metrics)

    async def open(self) -> None:
        await self.pool.open()

    async def close(self) -> None:
        await self.pool.close()

    # --- transactions ----------------------------------------------------------------------------

    async def tx(self, fn: Callable[[Tx], Awaitable[T]], *, isolation: Isolation = Isolation.READ_COMMITTED,
                 read_only: bool = False, statement_timeout: float | None = None, lock_timeout: float | None = None,
                 idle_timeout: float | None = None, max_attempts: int = errors.MAX_TX_ATTEMPTS) -> T:
        """Run ``fn(tx)`` in a transaction; ``fn`` may be re-run on 40001 / 40P01, so it touches only ``tx``."""
        unit = context.current()
        if unit.tx is not None:
            raise errors.be_error("NESTED_TX", message="a transaction inside a transaction")
        attempt = 1
        while True:
            try:
                return await self._once(fn, isolation, read_only, statement_timeout or STATEMENT_TIMEOUT,
                                        lock_timeout or LOCK_TIMEOUT, idle_timeout or IDLE_TIMEOUT)
            except asyncpg.PostgresError as e:
                ctx = "deadline_exceeded" if (context.remaining() or 1) <= 0 else "none"
                out = errors.classify_sqlstate(e.sqlstate or "", attempt=attempt, context=ctx,
                                               max_attempts=max_attempts)
                if not out.retry:
                    err = out.error
                    if err.code == errors.Code.INTERNAL:
                        err = errors.internal(e)
                    raise err from e
                self.metrics.tx_retries.labels(sqlstate=e.sqlstate).inc()
                await asyncio.sleep(out.base_delay_ms / 1000 * (0.5 + random.random()))
                attempt += 1

    async def read_snapshot(self, fn: Callable[[Tx], Awaitable[T]]) -> T:
        """``REPEATABLE READ READ ONLY`` with statements up to 30 s (P10.3)."""
        return await self.tx(fn, isolation=Isolation.REPEATABLE_READ, read_only=True,
                             statement_timeout=SNAPSHOT_TIMEOUT)

    async def _acquire(self) -> Any:
        remaining = context.remaining()
        wait = self.acquire_timeout if remaining is None else max(0.0, min(self.acquire_timeout, remaining))
        start = time.monotonic()
        if not self.pool.ready:
            await self._open_or_fail()
        try:
            await asyncio.wait_for(self._budget.acquire(), wait)
        except TimeoutError:
            raise errors.be_error("DB_POOL_EXHAUSTED") from None
        try:
            conn = await self.pool.acquire(timeout=max(0.001, wait - (time.monotonic() - start)))
        except (TimeoutError, asyncio.TimeoutError):
            self._budget.release()
            raise errors.be_error("DB_POOL_EXHAUSTED") from None
        except (OSError, asyncpg.InterfaceError) as e:
            self._budget.release()
            raise unavailable(e) from e
        except BaseException:
            self._budget.release()
            raise
        self.metrics.db_pool_wait.observe(time.monotonic() - start)
        self._in_use += 1
        self.metrics.db_pool_in_use.set(self._in_use)
        return conn

    async def _open_or_fail(self) -> None:
        try:
            await self.pool.open()
        except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError) as e:
            if sqlstate_of(e) == "53300":
                raise errors.be_error("DB_TOO_MANY_CONNECTIONS") from e
            raise unavailable(e) from e

    async def _release(self, conn: Any) -> None:
        self._in_use -= 1
        self.metrics.db_pool_in_use.set(self._in_use)
        try:
            await self.pool.release(conn)
        finally:
            self._budget.release()

    async def _once(self, fn: Callable[[Tx], Awaitable[T]], isolation: Isolation, read_only: bool,
                    statement: float, lock: float, idle: float) -> T:
        remaining = context.remaining()
        if remaining is not None:
            if remaining <= 0:
                raise errors.be_error("DEADLINE_BUDGET_EXHAUSTED")
            statement = min(statement, remaining)
        conn = await self._acquire()
        try:
            async with conn.transaction(isolation=isolation.value, readonly=read_only):
                await self._begin(conn, statement, lock, idle, remaining)
                tx = Tx(self, conn, context.current().req)
                with context.scope(tx=tx):
                    return await fn(tx)
        finally:
            await self._release(conn)

    async def _begin(self, conn: Any, statement: float, lock: float, idle: float, remaining: float | None) -> None:
        i = self.identity
        args = [i.role, _quote_ident(i.schema), i.member, _ms(statement), _ms(lock), _ms(idle)]
        if self.pool.server_version >= 170000:
            args.append(_ms(remaining) if remaining is not None else "0")
            await conn.execute(f"/* be:{i.schema} */ " + _SET17, *args)
        else:
            await conn.execute(f"/* be:{i.schema} */ " + _SET, *args)

    # --- the start-up probe (P10.7) --------------------------------------------------------------

    async def probe(self, *, shell: bool = False) -> list[str]:
        """Identity problems (empty = passed). A missing database capability raises ``CapabilityMissing``."""
        floor = 160000 if shell else 140000
        await self._open_or_fail()
        if self.pool.server_version < floor:
            raise CapabilityMissing(f"PostgreSQL {self.pool.server_version} is below {floor}")
        return await self.tx(lambda tx: _identity_problems(tx, self.identity.owner))


class CapabilityMissing(Exception):
    """A required database capability is missing: fatal (P1.8)."""


async def _identity_problems(tx: Tx, owner: str) -> list[str]:
    out = []
    usage, create = await tx.fetchrow(
        "SELECT has_schema_privilege(current_user, current_schema(), 'USAGE'), "
        "has_schema_privilege(current_user, current_schema(), 'CREATE')")
    if not usage:
        out.append("the runtime role has no USAGE on the schema")
    if create:
        out.append("the runtime role may CREATE in the schema")
    if await tx.fetchval("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1) "
                         "AND pg_has_role(current_user, $1, 'MEMBER')", owner):
        out.append("the runtime role is a member of the owner role")
    rows = await tx.fetch(
        "SELECT c.relname, pg_get_userbyid(c.relowner) AS owner, "
        "has_table_privilege(current_user, c.oid, 'SELECT,INSERT,UPDATE,DELETE') AS dml "
        "FROM pg_class c WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind IN ('r', 'p')")
    for r in rows:
        if r["owner"] != owner:
            out.append(f"table {r['relname']} is owned by {r['owner']}, not the owner role")
        if not r["dml"]:
            out.append(f"the runtime role lacks DML on {r['relname']}")
    return out
