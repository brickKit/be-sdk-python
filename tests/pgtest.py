"""Integration test database (marker "integration"): a PostgreSQL 16+ superuser DSN in BE_SDK_TEST_PG_ADMIN (the
assembly project's be-postgres). Without it the tests fail: a test that did not run is not a pass (protocol 18). The
database be_sdk_test is shared with the Go SDK's tests; Python schemas start with py_."""
from __future__ import annotations

import os
from urllib.parse import urlsplit

import asyncpg

DATABASE = "be_sdk_test"


def admin_dsn(database: str = "") -> str:
    dsn = os.environ.get("BE_SDK_TEST_PG_ADMIN", "")
    if not dsn:
        raise AssertionError("BE_SDK_TEST_PG_ADMIN is not set: integration tests need a PostgreSQL superuser DSN")
    if database:
        u = urlsplit(dsn)
        dsn = u._replace(path="/" + database).geturl()
    return dsn


def host_port() -> tuple[str, int]:
    u = urlsplit(admin_dsn())
    return u.hostname or "localhost", u.port or 5432


async def admin(database: str = DATABASE) -> asyncpg.Connection:
    if database:
        await ensure_database(database)
    return await asyncpg.connect(admin_dsn(database))


async def ensure_database(name: str) -> None:
    c = await asyncpg.connect(admin_dsn())
    try:
        if not await c.fetchval("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name):
            try:
                await c.execute(f'CREATE DATABASE "{name}"')
            except asyncpg.DuplicateDatabaseError:
                pass
    finally:
        await c.close()


async def role(c: asyncpg.Connection, name: str, password: str) -> None:
    if not await c.fetchval("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", name):
        try:
            await c.execute(f"CREATE ROLE {name} LOGIN PASSWORD '{password}'")
        except asyncpg.DuplicateObjectError:
            pass
    await c.execute(f"ALTER ROLE {name} LOGIN PASSWORD '{password}'")


async def member(name: str, shell_role: str = "") -> None:
    """A member's schema, owner and runtime roles provisioned as the assembly's provisioning script does."""
    c = await admin()
    try:
        owner, rw = f"sdk_{name}_owner", f"sdk_{name}_rw"
        await role(c, owner, name + "-owner-pw")
        await role(c, rw, name + "-pw")
        for sql in [f"DROP SCHEMA IF EXISTS {name} CASCADE", f"GRANT CONNECT ON DATABASE {DATABASE} TO {owner}, {rw}",
                    f"CREATE SCHEMA {name} AUTHORIZATION {owner}", f"GRANT USAGE ON SCHEMA {name} TO {rw}",
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {name} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {rw}",
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {name} GRANT EXECUTE ON FUNCTIONS TO {rw}"]:
            await c.execute(sql)
        if shell_role:
            await c.execute(f"GRANT {rw} TO {shell_role} WITH INHERIT FALSE, SET TRUE")
    finally:
        await c.close()


def member_env(name: str) -> dict[str, str]:
    """A member's database configuration as BrickKit would inject it."""
    host, port = host_port()
    return {"PG_HOST": host, "PG_PORT": str(port), "PG_DATABASE": DATABASE, "PG_USER": f"sdk_{name}_rw",
            "PG_PASSWORD": name + "-pw", "PG_SCHEMA": name, "PG_OWNER_USER": f"sdk_{name}_owner",
            "PG_OWNER_PASSWORD": name + "-owner-pw"}
