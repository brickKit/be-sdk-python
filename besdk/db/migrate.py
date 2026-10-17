"""The migrate entry (be-protocol P5.11-P5.16; A111): the SDK's own files, then the component's, as the owner role
directly on the database. The state tables have golang-migrate's shape and its advisory lock, so the Go and Python
SDKs read and write the same state and never migrate one schema at the same time."""
from __future__ import annotations

import asyncio
import zlib
from dataclasses import dataclass, field
from importlib.resources.abc import Traversable

import asyncpg

from besdk import protocol
from besdk.config import Problem, Secret, Values
from besdk.db import Target
from besdk.db.lint import files_of, lint
from besdk.obs import Logger

LOCK_WAIT = 3.0  # each attempt waits this long for a lock (protocol 14, "parameters")
LOCK_ATTEMPTS = 5
_LOCK_SALT = 1486364155  # golang-migrate's advisory lock salt


@dataclass(frozen=True)
class MigrationSet:
    name: str  # for logs: "be-sdk" or the component ID
    table: str  # the state table
    files: Traversable | dict[str, str]


def sdk_migrations() -> MigrationSet:
    """The SDK's own tables, shared by every SDK through be-protocol (P5.12)."""
    return MigrationSet("be-sdk", "be_sdk_migrations", protocol.migrations())


@dataclass
class MigrateConfig:
    target: Target
    user: str
    password: Secret | None
    schema: str
    member: str
    log: Logger
    lock_wait: float = LOCK_WAIT
    attempts: int = LOCK_ATTEMPTS


def config_from(member: str, v: Values, log: Logger) -> tuple[MigrateConfig, list[Problem]]:
    """The migrate entry's settings; owner credentials are required here and read nowhere else."""
    problems = []
    if not v.string("PG_OWNER_USER"):
        problems.append(Problem("PG_OWNER_USER", "missing", "the owner role, required by migrate"))
    if v.secret("PG_OWNER_PASSWORD") is None:
        problems.append(Problem("PG_OWNER_PASSWORD", "missing", "the owner password, required by migrate"))
    host = v.string("PG_MIGRATION_HOST") or v.string("PG_HOST")
    port = v.int("PG_MIGRATION_PORT") or v.int("PG_PORT")
    return MigrateConfig(Target(host, port, v.string("PG_DATABASE")), v.string("PG_OWNER_USER"),
                         v.secret("PG_OWNER_PASSWORD"), v.string("PG_SCHEMA"), member, log), problems


def advisory_lock_id(database: str, schema: str, table: str) -> int:
    """golang-migrate's lock: CRC-32 of schema, table and database joined by NUL, times its salt, as uint32."""
    return (zlib.crc32("\x00".join([schema, table, database]).encode()) * _LOCK_SALT) & 0xFFFFFFFF


class MigrateError(Exception):
    pass


async def migrate(c: MigrateConfig, *sets: MigrationSet) -> None:
    """Check, then apply the sets in order. Running it again with nothing new succeeds without changes."""
    for s in sets:
        if problems := lint(s.files, c.schema):
            raise MigrateError(f"{s.name}: refused migrations:\n  " + "\n  ".join(map(str, problems)))
    for s in sets:
        await _migrate_set(c, s)


async def _migrate_set(c: MigrateConfig, s: MigrationSet) -> None:
    attempt = 0
    while True:
        attempt += 1
        blockers: list[str] = []
        try:
            await _apply_once(c, s, blockers)
            return
        except asyncpg.LockNotAvailableError:
            who = "; ".join(blockers) or "unknown (the blocker had finished before it was seen)"
            if attempt >= c.attempts:
                raise MigrateError(f"{s.name}: a lock was not granted within {c.lock_wait}s, {attempt} times; "
                                   f"blocked by: {who}") from None
            c.log.warn("migration waiting for a lock; retrying", set=s.name, attempt=attempt, blocked_by=who)
            await asyncio.sleep(1)


async def _connect(c: MigrateConfig, app: str, lock_wait: bool = True) -> asyncpg.Connection:
    settings = {"application_name": app, "search_path": '"' + c.schema.replace('"', '""') + '"', "timezone": "UTC"}
    if lock_wait:
        settings["lock_timeout"] = f"{int(c.lock_wait * 1000)}ms"
    return await asyncpg.connect(host=c.target.host, port=c.target.port, database=c.target.database, user=c.user,
                                 password=c.password.value() if c.password else None, server_settings=settings,
                                 statement_cache_size=0)


def _versions(files: Traversable | dict[str, str]) -> list[tuple[int, str, str]]:
    out = [(int(name.split("_", 1)[0]), name, body or "") for name, body in files_of(files).items()
           if name.endswith(".up.sql")]
    return sorted(out)


async def _apply_once(c: MigrateConfig, s: MigrationSet, blockers: list[str]) -> None:
    conn = await _connect(c, c.member + " migrate")
    watcher = asyncio.create_task(_watch(c, await conn.fetchval("SELECT pg_backend_pid()"), blockers))
    table = '"' + c.schema.replace('"', '""') + '"."' + s.table + '"'
    lock = advisory_lock_id(c.target.database, c.schema, s.table)
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", lock)  # bounded by lock_timeout
        try:
            await _apply_locked(c, s, conn, table)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", lock)
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        await conn.close()


async def _apply_locked(c: MigrateConfig, s: MigrationSet, conn: asyncpg.Connection, table: str) -> None:
    await conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (version bigint not null primary key, dirty boolean not null)")
    row = await conn.fetchrow(f"SELECT version, dirty FROM {table} LIMIT 1")
    current = row["version"] if row else -1
    files = _versions(s.files)
    if row and row["dirty"]:
        name = next((n for v, n, _ in files if v == current), f"version {current}")
        raise MigrateError(f"{s.name}: version {current} is marked dirty by an earlier run that stopped mid-way; check "
                           f"whether {name} was applied, then correct the state table {c.schema}.{s.table}")
    for version, name, body in files:
        if version <= current:
            continue
        await _set_version(conn, table, version, True)
        try:
            await conn.execute(body)  # one simple query: the whole file is one implicit transaction
        except asyncpg.LockNotAvailableError:
            await _set_version(conn, table, current, False)
            raise
        except asyncpg.PostgresError as e:
            # this run's file failed and left no trace, so its state goes back to the file before it (P5.15)
            await _set_version(conn, table, current, False)
            raise MigrateError(f"{s.name}: migration {name} failed: {e}") from e
        await _set_version(conn, table, version, False)
        current = version
    c.log.info("migrations applied", set=s.name, schema_version=current)


async def _set_version(conn: asyncpg.Connection, table: str, version: int, dirty: bool) -> None:
    async with conn.transaction():
        await conn.execute(f"TRUNCATE {table}")
        if version >= 0:
            await conn.execute(f"INSERT INTO {table} (version, dirty) VALUES ($1, $2)", version, dirty)


async def _watch(c: MigrateConfig, pid: int, blockers: list[str]) -> None:
    """Poll pg_blocking_pids of the migrating backend from a second connection and keep the latest non-empty answer,
    so a lock timeout can name who held the lock. Another role's state and query are visible only with
    pg_read_all_stats, which the owner does not hold: pid, user and application name still identify it."""
    try:
        conn = await _connect(c, c.member + " migrate watcher", lock_wait=False)
    except (OSError, asyncpg.PostgresError):
        return
    try:
        while True:
            rows = await conn.fetch("""SELECT pid, coalesce(usename, '') AS usr, application_name AS app,
                coalesce(state, '') AS state, coalesce(query, '') AS query,
                coalesce(extract(epoch FROM now() - xact_start), 0)::float8 AS secs
                FROM pg_stat_activity WHERE pid = ANY(pg_blocking_pids($1)) ORDER BY pid""", pid)
            if rows:
                blockers[:] = [f"pid {r['pid']} (user {r['usr']}, application {r['app']!r})" if not r["state"] else
                               f"pid {r['pid']} (user {r['usr']}, application {r['app']!r}, {r['state']}, in transaction "
                               f"{r['secs']:.0f}s: {' '.join(r['query'].split())[:80]})" for r in rows]
            await asyncio.sleep(0.25)
    finally:
        await conn.close()
