import asyncio
import io
import time

import asyncpg
import pytest

from besdk import config, db, errs, obs
from besdk.config import MapSource
from besdk.db import migrate as mig
from besdk.db.docno import Series, next_gapped_number, next_number, register_imported
from besdk.db.pool import DB, Pool
from tests import pgtest

pytestmark = pytest.mark.integration

LOG = obs.new_logger(obs.Member("demo/test", "3.0.0"), "debug", io.StringIO())


def values(name, **extra):
    return config.parse(db.migrate_keys(), MapSource({**pgtest.member_env(name), **extra}))


async def migrated(name, files=None, **extra):
    await pgtest.member(name)
    v = values(name, **extra)
    c, problems = mig.config_from("demo/" + name, v, LOG)
    assert problems == []
    await mig.migrate(c, mig.sdk_migrations(), mig.MigrationSet("demo/" + name, "schema_migrations",
                                                                files or {"0001_items.up.sql": "CREATE TABLE items (id int PRIMARY KEY);"}))
    return v, c


async def open_db(name, v, user=None, password=None, foreground=None):
    s = db.settings_from(v)
    if foreground is not None:
        s = db.Settings(s.target, s.user, s.password, s.schema, s.prepared, s.timeouts, foreground, s.background)
    pool = await Pool.open(s.target, user or s.user, password or s.password, "demo/" + name, 8, s.prepared)
    return DB("demo/" + name, pool, s), pool


async def test_migrate_twice_and_the_state_in_the_component_schema():  # P5.12, CK-DB-05
    v, c = await migrated("py_mig1")
    await mig.migrate(c, mig.sdk_migrations(), mig.MigrationSet("demo/py_mig1", "schema_migrations",
                                                                {"0001_items.up.sql": "CREATE TABLE items (id int PRIMARY KEY);"}))
    a = await pgtest.admin()
    try:
        rows = await a.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'py_mig1' "
                             "AND table_name IN ('be_sdk_migrations', 'schema_migrations', 'items') ORDER BY 1")
        assert [r[0] for r in rows] == ["be_sdk_migrations", "items", "schema_migrations"]
        assert await a.fetchrow("SELECT version, dirty FROM py_mig1.schema_migrations") == (1, False)
        assert (await a.fetchrow("SELECT version, dirty FROM py_mig1.be_sdk_migrations"))["version"] == 7
    finally:
        await a.close()


async def test_a_failed_file_leaves_no_trace_and_the_entry_stays_rerunnable():  # P5.15
    files = {"0001_a.up.sql": "CREATE TABLE a (id int);",
             "0002_b.up.sql": "CREATE TABLE b (id int); INSERT INTO nowhere VALUES (1);"}
    with pytest.raises(mig.MigrateError, match="0002_b.up.sql failed"):
        await migrated("py_mig2", files)
    a = await pgtest.admin()
    try:
        assert await a.fetchrow("SELECT version, dirty FROM py_mig2.schema_migrations") == (1, False)
        assert await a.fetchval("SELECT to_regclass('py_mig2.b')") is None, "the failed file left nothing behind"
    finally:
        await a.close()
    files["0002_b.up.sql"] = "CREATE TABLE b (id int);"
    v = values("py_mig2")
    c, _ = mig.config_from("demo/py_mig2", v, LOG)
    await mig.migrate(c, mig.sdk_migrations(), mig.MigrationSet("demo/py_mig2", "schema_migrations", files))


async def test_a_lock_wait_reports_the_blocker():  # P5.14, CK-DB-06
    v, c = await migrated("py_mig3")
    a = await pgtest.admin()
    try:
        await a.execute("SET application_name = 'the blocker'")
        tx = a.transaction()
        await tx.start()
        await a.execute("LOCK TABLE py_mig3.items IN ACCESS EXCLUSIVE MODE")
        c.lock_wait, c.attempts = 0.5, 2
        with pytest.raises(mig.MigrateError) as e:
            await mig.migrate(c, mig.MigrationSet("demo/py_mig3", "schema_migrations", {
                "0001_items.up.sql": "CREATE TABLE items (id int PRIMARY KEY);",
                "0002_more.up.sql": "ALTER TABLE items ADD COLUMN note text;"}))
        assert "2 times" in str(e.value) and "the blocker" in str(e.value), str(e.value)
        await tx.rollback()
    finally:
        await a.close()


async def test_two_instances_migrate_at_once():  # P5.16
    await pgtest.member("py_mig4")
    v = values("py_mig4")
    files = {"0001_log.up.sql": "CREATE TABLE runs (n int); INSERT INTO runs VALUES (1);",
             "0002_more.up.sql": "INSERT INTO runs VALUES (2);"}
    configs = [mig.config_from("demo/py_mig4", v, LOG)[0] for _ in range(2)]
    await asyncio.gather(*(mig.migrate(c, mig.sdk_migrations(), mig.MigrationSet("demo/py_mig4", "schema_migrations", files))
                           for c in configs))
    a = await pgtest.admin()
    try:
        assert await a.fetchval("SELECT count(*) FROM py_mig4.runs") == 2, "every file ran exactly once"
    finally:
        await a.close()


async def test_refused_migrations_change_nothing():  # P5.13
    with pytest.raises(mig.MigrateError, match="GRANT"):
        await migrated("py_mig5", {"0001_a.up.sql": "CREATE TABLE a (id int);", "0002_b.up.sql": "GRANT SELECT ON a TO public;"})
    a = await pgtest.admin()
    try:
        assert await a.fetchval("SELECT to_regclass('py_mig5.be_sdk_migrations')") is None
    finally:
        await a.close()


async def test_role_schema_and_timeouts_are_per_transaction_and_never_leak():  # P5.5, P5.6, CK-DB-03
    a = await pgtest.admin()
    try:
        await pgtest.role(a, "sdk_py_shell", "shell-pw")
    finally:
        await a.close()
    await pgtest.member("py_tx1", shell_role="sdk_py_shell")
    v = values("py_tx1")
    c, _ = mig.config_from("demo/py_tx1", v, LOG)
    await mig.migrate(c, mig.sdk_migrations())
    d, pool = await open_db("py_tx1", v, user="sdk_py_shell", password=config.Secret("shell-pw"))
    try:
        inside = await d.tx(lambda tx: tx.fetchrow("SELECT current_user AS u, current_setting('search_path') AS sp, "
                                                   "current_setting('statement_timeout') AS st, current_setting('lock_timeout') AS lt, "
                                                   "current_setting('application_name') AS app"))
        assert dict(inside) == {"u": "sdk_py_tx1_rw", "sp": '"py_tx1"', "st": "5s", "lt": "2s", "app": "demo/py_tx1"}
        after = await pool._pool.fetchrow("SELECT current_user AS u, current_setting('statement_timeout') AS st, "
                                          "current_setting('search_path') AS sp")
        assert after["u"] == "sdk_py_shell" and after["st"] == "0" and "py_tx1" not in after["sp"]
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await pool._pool.fetchval("SELECT count(*) FROM py_tx1.be_doc_series")
    finally:
        await pool.close()


async def test_nesting_retries_and_lock_timeouts():  # P5.7, P5.8, P5.9
    v, _ = await migrated("py_tx2")
    d, pool = await open_db("py_tx2", v)
    try:
        async def nested(tx):
            await d.tx(lambda inner: inner.fetchval("SELECT 1"))

        with pytest.raises(errs.Error) as e:
            await d.tx(nested)
        assert e.value.reason == "INTERNAL" and "NESTED_TX" in str(e.value)

        attempts = 0

        async def conflicting(tx):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise asyncpg.PostgresError.new({"C": "40001", "M": "could not serialize"})
            return "done"

        assert await d.tx(conflicting) == "done" and attempts == 3
        a = await pgtest.admin()
        try:
            tx = a.transaction()
            await tx.start()
            await a.execute("LOCK TABLE py_tx2.items IN ACCESS EXCLUSIVE MODE")
            started = time.monotonic()
            with pytest.raises(errs.Error) as e:
                await d.tx(lambda t: t.execute("INSERT INTO items VALUES (1)"))
            assert e.value.reason == "LOCK_TIMEOUT" and 1.5 < time.monotonic() - started < 4
            await tx.rollback()
        finally:
            await a.close()
    finally:
        await pool.close()


async def test_foreground_budget_and_durable_check():  # P5.2, P1.10, CK-DB-02
    v, _ = await migrated("py_tx3")
    d, pool = await open_db("py_tx3", v, foreground=1)
    try:
        await pool.ready(LOG)
        hold = asyncio.create_task(d.tx(lambda tx: tx.fetchval("SELECT pg_sleep(3)")))
        await asyncio.sleep(0.2)
        started = time.monotonic()
        with pytest.raises(errs.Error) as e:
            await d.tx(lambda tx: tx.fetchval("SELECT 1"))
        assert e.value.reason == "SERVICE_BUSY" and e.value.metadata == {"resource": "db_pool"}
        assert 1.8 < time.monotonic() - started < 2.6
        await hold
    finally:
        await pool.close()


async def test_document_numbers():  # P5.18-P5.20, CK-DB-07
    v, _ = await migrated("py_doc")
    d, pool = await open_db("py_doc", v)
    s = Series("le-1", "SO", "2026-10")
    try:
        assert await d.tx(lambda tx: next_number(tx, s)) == 1

        async def rolled_back(tx):
            await next_number(tx, s)
            raise errs.new("REQUEST_INVALID", "changed my mind")

        with pytest.raises(errs.Error):
            await d.tx(rolled_back)
        numbers = await asyncio.gather(*(d.tx(lambda tx: next_number(tx, s)) for _ in range(6)))
        assert sorted(numbers) == [2, 3, 4, 5, 6, 7], "no gap after the rollback, no duplicate under concurrency"
        await d.tx(lambda tx: register_imported(tx, Series("le-1", "OLD"), False, 900))
        assert await next_gapped_number(d, Series("le-1", "OLD")) == 901
        with pytest.raises(errs.Error):
            await d.tx(lambda tx: next_number(tx, Series("le-1", "OLD")))
    finally:
        await pool.close()


async def test_no_calls_inside_a_transaction():  # P4.20
    from besdk import reqctx
    from besdk.calls.client import Member

    v, _ = await migrated("py_tx4")
    d, pool = await open_db("py_tx4", v)
    out = Member()
    try:
        async def calls_out(tx):
            with reqctx.deadline_scope(time.monotonic() + 5):
                await out.http("demo/other", "http://127.0.0.1:9").do("GET", "/api/x")

        with pytest.raises(errs.Error) as e:
            await d.tx(calls_out)
        assert e.value.reason == "INTERNAL" and "NETWORK_IN_TX" in str(e.value)
    finally:
        await asyncio.gather(out.close(), pool.close())


async def test_a_cancelled_task_stays_cancelled():  # the forwarder and consumers stop by cancellation
    v, _ = await migrated("py_tx5")
    d, pool = await open_db("py_tx5", v)
    try:
        task = asyncio.create_task(d.background_tx(lambda tx: tx.fetchval("SELECT pg_sleep(5)")))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await pool.close()


async def test_after_commit_runs_only_on_commit():  # P6.29
    v, _ = await migrated("py_after")
    d, pool = await open_db("py_after", v)
    ran: list[str] = []
    try:
        async def ok(tx):
            tx.after_commit(lambda: ran.append("committed"))
            assert ran == [], "not before the commit"

        async def fails(tx):
            tx.after_commit(lambda: ran.append("rolled back"))
            raise ValueError("roll back")

        await d.tx(ok)
        with pytest.raises(Exception):
            await d.tx(fails)
        assert ran == ["committed"]
    finally:
        await pool.close()


async def test_a_session_setting_is_refused_and_nothing_reaches_the_next_transaction():  # P5.30, P5.5
    v, _ = await migrated("py_session")
    d, pool = await open_db("py_session", v)
    try:
        async def session_set(tx):
            await tx.execute("SET work_mem = '7MB'")

        with pytest.raises(Exception):
            await d.tx(session_set)

        async def local_set(tx):
            await tx.execute("SET LOCAL work_mem = '7MB'")
            assert await tx.fetchval("SELECT current_setting('work_mem')") == "7MB"

        async def read(tx):
            return await tx.fetchval("SELECT current_setting('work_mem')")

        for _ in range(3):  # the pool keeps its connections without a session reset on release
            await d.tx(local_set)
            assert await d.tx(read) != "7MB", "a transaction's setting reached the next transaction"
    finally:
        await pool.close()


async def test_a_connection_a_function_left_a_setting_on_is_closed():  # P5.31
    v, _ = await migrated("py_leak")
    s = db.settings_from(v)
    logged = io.StringIO()
    log = obs.new_logger(obs.Member("demo/py_leak", "0"), "info", logged)
    pool = await Pool.open(s.target, s.user, s.password, "demo/py_leak", 1, s.prepared, log)
    d = DB("demo/py_leak", pool, s)
    a = await pgtest.admin()
    try:
        await a.execute("CREATE OR REPLACE FUNCTION py_leak.leak_time_zone() RETURNS void LANGUAGE plpgsql AS $$ "
                        "BEGIN PERFORM set_config('TimeZone', 'Asia/Tokyo', false); END $$")

        async def leak(tx):
            pid = await tx.fetchval("SELECT pg_backend_pid()")
            await tx.execute("SELECT leak_time_zone()")
            return pid

        async def read(tx):
            return await tx.fetchrow("SELECT pg_backend_pid() AS pid, current_setting('TimeZone') AS zone")

        pid1 = await d.tx(leak)  # the transaction itself succeeds
        row = await d.tx(read)
        assert row["zone"] == "UTC", "the next transaction does not inherit the leaked time zone"
        assert row["pid"] != pid1, "the one pooled connection was replaced"
        out = logged.getvalue()
        assert "P5.31" in out and "TimeZone" in out and "demo/py_leak" in out, out
    finally:
        await a.execute("DROP FUNCTION IF EXISTS py_leak.leak_time_zone()")
        await a.close()
        await pool.close()
