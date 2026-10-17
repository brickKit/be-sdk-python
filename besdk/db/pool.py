"""Pools, the transaction entry and the start checks (be-protocol chapter 05, P5.1-P5.10)."""
from __future__ import annotations

import asyncio
import contextvars
import random
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, TypeVar

import asyncpg

from besdk import clock, errs, reqctx
from besdk.config import Secret
from besdk.db import Settings, Target
from besdk.obs import Logger

FOREGROUND_WAIT = 2.0  # how long a foreground transaction waits for a connection before SERVICE_BUSY
MAX_ATTEMPTS = 3  # serialization failures and deadlocks redo the whole transaction
COMMIT_WINDOW = 2.0  # C_commit: the longest time from the pre-commit cutoff check to COMMIT (P8.12)

T = TypeVar("T")
_in_tx: contextvars.ContextVar[bool] = contextvars.ContextVar("be_in_tx", default=False)


def in_tx() -> bool:
    """Whether the current task runs inside a transaction; the call clients refuse network calls then (P4.20)."""
    return _in_tx.get()


class Pool:
    """One physical connection pool to a target. The launcher opens it with the member's runtime role; the shell
    opens one per target with the shell's login role, which holds the members' roles WITH INHERIT FALSE."""

    def __init__(self, target: Target, pool: asyncpg.Pool):
        self.target, self._pool, self._version = target, pool, 0

    @staticmethod
    async def open(target: Target, user: str, password: Secret | None, app_name: str, max_conns: int,
                   prepared: bool) -> Pool:
        """Connects lazily; the password is read from the secret at every new connection, so a rotated file applies
        to new connections (P2.8). prepared=False keeps no statement cache: safe behind a transaction-mode pooler."""
        pool = await asyncpg.create_pool(
            host=target.host, port=target.port, database=target.database, user=user,
            password=(lambda: password.value()) if password else None, min_size=0, max_size=max(max_conns, 1),
            statement_cache_size=100 if prepared else 0, server_settings={"application_name": app_name, "timezone": "UTC"})
        return Pool(target, pool)

    async def close(self) -> None:
        await self._pool.close()

    async def server_version(self) -> int:
        if not self._version:
            try:
                self._version = int(await self._pool.fetchval("SELECT current_setting('server_version_num')"))
            except (asyncpg.PostgresError, OSError, ValueError):
                return 0
        return self._version

    async def ping(self) -> None:
        async with self._pool.acquire(timeout=5) as conn:
            await conn.fetchval("SELECT 1")

    async def check_durable(self) -> None:
        """P1.10: fsync on, synchronous_commit not weaker than on; a failure keeps the component out of ready."""
        fsync, sync_commit = await self._pool.fetchrow(
            "SELECT current_setting('fsync'), current_setting('synchronous_commit')")
        if fsync != "on":
            raise StartCheckError(f"{self.target}: fsync is {fsync!r}, must be on")
        if sync_commit not in ("on", "remote_write", "remote_apply"):
            raise StartCheckError(f"{self.target}: synchronous_commit is {sync_commit!r}, must be on, remote_write or remote_apply")

    async def ready(self, log: Logger, stop: asyncio.Event | None = None) -> None:
        """Wait until the target answers (the database may start after the component), then run the durable check.
        Wrong credentials, a missing database and weak durable settings end the wait; connection failures are
        retried every second until stop is set (then StoppedWaiting)."""
        attempt = 0
        while True:
            attempt += 1
            try:
                await self.ping()
                break
            except (asyncpg.InvalidPasswordError, asyncpg.InvalidAuthorizationSpecificationError) as e:
                raise CredentialsError(f"{self.target}: {e}") from e
            except asyncpg.InvalidCatalogNameError as e:
                raise StartCheckError(f"{self.target}: {e} (not provisioned)") from e
            except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError) as e:
                if attempt == 1 or attempt % 10 == 0:
                    log.warn("waiting for the database", target=str(self.target), error=repr(e))
            if stop is not None:
                try:
                    await asyncio.wait_for(stop.wait(), 1)
                    raise StoppedWaiting()
                except TimeoutError:
                    continue
            await asyncio.sleep(1)
        await self.check_durable()


class CredentialsError(Exception):
    """The database refused the credentials: exit 78."""


class StartCheckError(Exception):
    """A start check of the database failed: exit 69."""


class StoppedWaiting(Exception):
    """Stopped while waiting for the database: exit 0."""


class Tx:
    """The transaction a function runs in. Every statement carries the member's prefix, so the same SQL of two
    members never shares a cached statement (P5.4)."""

    def __init__(self, conn: asyncpg.Connection, prefix: str):
        self._conn, self._prefix = conn, prefix

    async def execute(self, sql: str, *args: Any) -> str:
        """Runs a statement; returns its status, e.g. "UPDATE 3" (rows_affected reads the count)."""
        return await self._conn.execute(self._prefix + sql, *args)

    async def fetch(self, sql: str, *args: Any) -> list[asyncpg.Record]:
        return await self._conn.fetch(self._prefix + sql, *args)

    async def fetchrow(self, sql: str, *args: Any) -> asyncpg.Record | None:
        return await self._conn.fetchrow(self._prefix + sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        return await self._conn.fetchval(self._prefix + sql, *args)


def rows_affected(status: str) -> int:
    tail = status.rsplit(" ", 1)[-1]
    return int(tail) if tail.isdigit() else 0


@dataclass
class TxOptions:
    serializable: bool = False
    statement_timeout: float | None = None
    lock_timeout: float | None = None
    # bounds the whole transaction: transaction_timeout on PostgreSQL 17 and later; earlier versions get the statement
    # and idle-in-transaction timeouts capped at it instead (P6.5)
    transaction_timeout: float | None = None


class DB:
    """One member's handle on its database: a shared or own pool, the member's role and schema, its timeouts and its
    connection budget. Built by the launcher or the shell; modules get it from the runtime."""

    def __init__(self, member: str, pool: Pool, s: Settings):
        self.member, self.pool, self.settings = member, pool, s
        self._prefix = f"/* {member} */ "
        self._fg, self._bg = asyncio.Semaphore(max(s.foreground, 1)), asyncio.Semaphore(max(s.background, 1))

    async def tx(self, fn: Callable[[Tx], Awaitable[T]], options: TxOptions | None = None) -> T:
        """Run fn in a foreground transaction (a user request); waits at most 2 s for the member's budget."""
        return await self._run(fn, options or TxOptions(), foreground=True)

    async def background_tx(self, fn: Callable[[Tx], Awaitable[T]], options: TxOptions | None = None) -> T:
        """Run fn in a background transaction (consumers, forwarding); waits for its budget instead of failing fast."""
        return await self._run(fn, options or TxOptions(), foreground=False)

    async def _run(self, fn: Callable[[Tx], Awaitable[T]], o: TxOptions, foreground: bool) -> T:
        if _in_tx.get():
            raise errs.new("INTERNAL").wrap(RuntimeError("NESTED_TX: a transaction was opened inside another"))
        t = self.settings.timeouts
        statement, lock, idle = o.statement_timeout or t.statement, o.lock_timeout or t.lock, t.idle_in_tx
        native = False
        if o.transaction_timeout:
            native = await self.pool.server_version() >= 170000
            if not native:
                statement, idle = min(statement, o.transaction_timeout), min(idle, o.transaction_timeout)
        sem = self._fg if foreground else self._bg
        if foreground:
            try:
                await asyncio.wait_for(sem.acquire(), FOREGROUND_WAIT)
            except TimeoutError:
                raise errs.new("SERVICE_BUSY", "no database connection became free in time").with_(
                    "resource", "db_pool").with_retry_after(1) from None
        else:
            await sem.acquire()
        try:
            last: BaseException | None = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    return await self._once(fn, o, statement, lock, idle, native)
                except (asyncpg.SerializationError, asyncpg.DeadlockDetectedError) as e:
                    last = e
                    if attempt < MAX_ATTEMPTS:
                        await asyncio.sleep(random.uniform(0, 0.02 * (1 << attempt)))
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    raise  # a cancelled task stays cancelled: the transaction rolled back, nothing to map
                except BaseException as e:  # noqa: BLE001 - mapped below
                    last = e
                    break
            raise map_error(last) from last
        finally:
            sem.release()

    async def _once(self, fn: Callable[[Tx], Awaitable[T]], o: TxOptions, statement: float, lock: float, idle: float,
                    native: bool) -> T:
        async with self.pool._pool.acquire() as conn:
            async with conn.transaction(isolation="serializable" if o.serializable else "read_committed"):
                # everything LOCAL: it ends with the transaction and never stays on the pooled connection (0-12)
                await conn.execute(self._prefix + "SELECT set_config('role', $1, true), set_config('search_path', $2, true), "
                                   "set_config('statement_timeout', $3, true), set_config('lock_timeout', $4, true), "
                                   "set_config('idle_in_transaction_session_timeout', $5, true), "
                                   "set_config('application_name', $6, true)",
                                   self.settings.user, _ident(self.settings.schema), _ms(statement), _ms(lock), _ms(idle),
                                   self.member)
                if o.transaction_timeout and native:
                    await conn.execute(self._prefix + "SELECT set_config('transaction_timeout', $1, true)",
                                       _ms(o.transaction_timeout))
                token = _in_tx.set(True)
                try:
                    result = await fn(Tx(conn, self._prefix))
                finally:
                    _in_tx.reset(token)
                if (cutoff := reqctx.cutoff()) is not None:
                    # P8.12: the database ends the transaction if COMMIT does not follow the check within C_commit;
                    # then the check itself, on the suspend-aware clock
                    await conn.execute(self._prefix + "SELECT set_config('idle_in_transaction_session_timeout', $1, true)",
                                       _ms(COMMIT_WINDOW))
                    if clock.since_boot() >= cutoff:
                        raise errs.new("AUTHORIZATION_VIEW_EXPIRED",
                                       "the authorization expired before the change could be committed")
                return result


def _ms(seconds: float) -> str:
    return f"{int(seconds * 1000)}ms"


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


_UNAVAILABLE = (asyncpg.TooManyConnectionsError, asyncpg.AdminShutdownError, asyncpg.CrashShutdownError,
                asyncpg.CannotConnectNowError)


def map_error(err: BaseException | None) -> errs.Error:
    """Database errors as the protocol's reasons (vector db-errors); errors the module raised pass through."""
    if isinstance(err, errs.Error):
        return err
    if isinstance(err, (asyncpg.SerializationError, asyncpg.DeadlockDetectedError)):
        return errs.new("TX_CONFLICT", "a concurrent change conflicted; try again").wrap(err)
    if isinstance(err, asyncpg.LockNotAvailableError):
        return errs.new("LOCK_TIMEOUT", "waiting for a lock took too long").wrap(err)
    if isinstance(err, asyncpg.QueryCanceledError):
        return errs.new("STATEMENT_TIMEOUT", "the database took too long").wrap(err)
    if isinstance(err, _UNAVAILABLE) or (isinstance(err, (OSError, asyncpg.InterfaceError,
                                                          asyncpg.ConnectionDoesNotExistError))):
        return errs.new("DEPENDENCY_UNAVAILABLE", "the database is unavailable").with_("dependency", "db").wrap(err)
    return errs.from_exception(err)  # type: ignore[return-value]
