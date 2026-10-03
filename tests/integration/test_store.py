"""The store (P10): identity per transaction, timeouts, retries, SQLSTATE mapping, pools, the probe,
advisory locks, the statement prefix on a shared physical pool, password files re-read (P2.9)."""
import asyncio
import io

import asyncpg
import psycopg
import pytest
from prometheus_client import generate_latest

from besdk import Code, Error, Module, context
from besdk.runtime import Runtime, Shared, Spec
from besdk.store.pool import PhysicalPool
from besdk.store.store import Store
from tests.integration.conftest import Identity, admin_dsn, component_dir
from tests.integration.test_migrate import migrator


async def _empty(rt):
    return Module()


def runtime(root, ident, **env) -> Runtime:
    spec = Spec(id="conformance/widget-py", migrations=root / "migrations", contracts=root / "contracts", create=_empty)
    shared = Shared.standalone(spec_id=spec.id)
    return Runtime(spec, ident.env(**env), shared, log_stream=io.StringIO())


@pytest.fixture
async def rt(tmp_path, ident):
    root = component_dir(tmp_path)
    migrator(root, ident).up()
    r = runtime(root, ident, PG_POOL_MAX="2")
    await r.store().open()
    yield r
    await r.store().close()


async def test_every_transaction_sets_identity_and_timeouts(rt, ident):
    async def body(tx):
        return await tx.fetchrow(
            "SELECT current_user AS u, current_schema() AS s, current_setting('application_name') AS app, "
            "current_setting('statement_timeout') AS st, current_setting('lock_timeout') AS lt, "
            "current_setting('idle_in_transaction_session_timeout') AS it, current_setting('TimeZone') AS tz")

    row = await rt.store().tx(body)
    assert (row["u"], row["s"], row["app"]) == (ident.user, ident.schema, "conformance/widget-py@1.0.0")  # rc.2: <id>@<version>
    assert (row["st"], row["lt"], row["it"], row["tz"]) == ("5s", "2s", "30s", "UTC")
    async with rt.store().pool.raw() as conn:  # the pooled connection is clean afterwards
        assert await conn.fetchval("SHOW search_path") == '"$user", public'


async def test_dml_on_migrated_tables_and_read_snapshot(rt):
    await rt.store().tx(lambda tx: tx.execute("INSERT INTO widget (id, name) VALUES (gen_random_uuid(), 'a')"))
    assert await rt.store().read_snapshot(lambda tx: tx.fetchval("SELECT count(*) FROM widget")) == 1
    with pytest.raises(Error) as ei:
        await rt.store().read_snapshot(lambda tx: tx.execute("DELETE FROM widget"))
    assert ei.value.code == Code.INTERNAL  # 25006 read-only transaction: a programming error


async def test_retries_serialization_failures_then_conflict(rt):
    attempts = []

    async def flaky(tx):
        attempts.append(1)
        if len(attempts) < 3:
            raise asyncpg.exceptions.SerializationError("could not serialize")
        return "ok"

    assert await rt.store().tx(flaky) == "ok" and len(attempts) == 3
    assert 'be_tx_retries_total{component="conformance/widget-py",sqlstate="40001"} 2.0' in generate_latest(
        rt.registry).decode()

    async def always(tx):
        raise asyncpg.exceptions.DeadlockDetectedError("deadlock")

    with pytest.raises(Error) as ei:
        await rt.store().tx(always)
    assert (ei.value.code, ei.value.reason) == (Code.ABORTED, "TX_CONFLICT")


async def test_nested_transaction_refused(rt):
    async def outer(tx):
        await rt.store().tx(lambda t: t.fetchval("SELECT 1"))

    with pytest.raises(Error) as ei:
        await rt.store().tx(outer)
    assert ei.value.reason == "NESTED_TX"


async def test_statement_timeout_and_deadline(rt):
    with pytest.raises(Error) as ei:
        await rt.store().tx(lambda tx: tx.execute("SELECT pg_sleep(2)"), statement_timeout=0.2)
    assert (ei.value.code, ei.value.reason) == (Code.DEADLINE_EXCEEDED, "STATEMENT_TIMEOUT")
    with context.scope(deadline=context.deadline_in(0.3)):
        with pytest.raises(Error) as ei:
            await rt.store().tx(lambda tx: tx.execute("SELECT pg_sleep(2)"))
    assert ei.value.reason == "STATEMENT_TIMEOUT"


async def test_lock_timeout(rt, ident):
    await rt.store().tx(lambda tx: tx.execute("INSERT INTO widget (id, name) VALUES ('0192f0c4-0000-7000-8000-000000000001', 'a')"))
    with psycopg.connect(admin_dsn(ident.hostport)) as c:
        c.execute(f'SELECT 1 FROM "{ident.schema}".widget FOR UPDATE')
        with pytest.raises(Error) as ei:
            await rt.store().tx(lambda tx: tx.execute("SELECT 1 FROM widget FOR UPDATE"), lock_timeout=0.2)
    assert (ei.value.code, ei.value.reason) == (Code.ABORTED, "LOCK_TIMEOUT")


async def test_pool_budget_exhausted(tmp_path, ident):
    root = component_dir(tmp_path)
    migrator(root, ident).up()
    r = runtime(root, ident, PG_POOL_MAX="1", PG_POOL_ACQUIRE_TIMEOUT="200ms")
    await r.store().open()
    hold = asyncio.create_task(r.store().tx(lambda tx: tx.execute("SELECT pg_sleep(1)")))
    await asyncio.sleep(0.2)
    with pytest.raises(Error) as ei:
        await r.store().tx(lambda tx: tx.fetchval("SELECT 1"))
    assert (ei.value.code, ei.value.reason) == (Code.RESOURCE_EXHAUSTED, "DB_POOL_EXHAUSTED")
    await hold
    await r.store().close()


async def test_identity_probe(rt, ident):
    assert await rt.store().probe() == []
    ident.sql(f'GRANT "{ident.owner}" TO "{ident.user}"')
    problems = await rt.store().probe()
    assert any("member of the owner" in p for p in problems)


async def test_password_file_reread_for_new_connections(rt, ident):
    ident.sql(f"ALTER ROLE \"{ident.user}\" PASSWORD 'rotated-1'")
    ident.user_file.write_text("rotated-1\n")
    rt.config.secret("PG_PASSWORD_FILE").poll()
    await rt.store().pool.expire_all()
    assert await rt.store().tx(lambda tx: tx.fetchval("SELECT 42")) == 42


async def test_advisory_locks(rt):
    got = []

    async def holder(tx):
        await tx.lock("order", "LE01", "42")
        got.append("held")
        await asyncio.sleep(0.3)

    t = asyncio.create_task(rt.store().tx(holder))
    await asyncio.sleep(0.1)
    assert await rt.store().tx(lambda tx: tx.try_lock("order", "LE01", "42")) is False
    assert await rt.store().tx(lambda tx: tx.try_lock("order", "LE01", "43")) is True
    await t


async def test_shared_physical_pool_two_members_statement_prefix(tmp_path, pg16):
    """r1-04 S2 on a NOINHERIT shell login: same SQL text, different result shapes per member."""
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a, b = Identity(pg16, tmp_path / "a", "a"), Identity(pg16, tmp_path / "b", "b")
    migrator(component_dir(tmp_path / "a", "conformance/a",
                           migrations={"0001_t": "CREATE TABLE thing (id bigint PRIMARY KEY, payload text);"}), a).up()
    migrator(component_dir(tmp_path / "b", "conformance/b",
                           migrations={"0001_t": "CREATE TABLE thing (id bigint PRIMARY KEY, payload jsonb, extra int);"}),
             b).up()
    a.sql("INSERT INTO \"%s\".thing VALUES (1, 'a-text')" % a.schema)
    b.sql("INSERT INTO \"%s\".thing VALUES (1, '{\"b\": 1}', 7)" % b.schema)
    shell = "sh_" + a.schema[2:]
    a.sql(f"CREATE ROLE \"{shell}\" LOGIN NOINHERIT PASSWORD 'sh'")
    a.sql(f'GRANT "{a.user}", "{b.user}" TO "{shell}" WITH INHERIT FALSE, SET TRUE')
    host, port = pg16.rsplit(":", 1)
    pool = PhysicalPool(host=host, port=int(port), database="postgres", user=shell, password=lambda: "sh",
                        max_size=1, min_size=0, members=2)
    await pool.open()
    import logging
    from besdk import metrics
    sa = Store.for_member(pool, member="conformance/a", role=a.user, schema=a.schema, owner=a.owner, budget=1,
                          logger=logging.getLogger("t"), metrics=metrics.BeMetrics(metrics.ComponentRegistry("conformance/a")))
    sb = Store.for_member(pool, member="conformance/b", role=b.user, schema=b.schema, owner=b.owner, budget=1,
                          logger=logging.getLogger("t"), metrics=metrics.BeMetrics(metrics.ComponentRegistry("conformance/b")))
    for _ in range(3):
        ra = await sa.tx(lambda tx: tx.fetchrow("SELECT id, payload FROM thing WHERE id = $1", 1))
        rb = await sb.tx(lambda tx: tx.fetchrow("SELECT id, payload FROM thing WHERE id = $1", 1))
        assert ra["payload"] == "a-text" and rb["payload"] == '{"b": 1}'
    assert await sa.tx(lambda tx: tx.fetchval("SELECT current_setting('application_name')")) == "conformance/a"
    async with pool.raw() as conn:  # without SET LOCAL ROLE the shell role reads nothing of a member
        with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
            await conn.fetch(f'SELECT * FROM "{a.schema}".thing')
    await pool.close()


async def test_pg14_floor(tmp_path, pg14):
    ident = Identity(pg14, tmp_path)
    root = component_dir(tmp_path)
    migrator(root, ident).up()
    r = runtime(root, ident)
    await r.store().open()
    assert await r.store().probe() == []
    assert await r.store().tx(lambda tx: tx.fetchval("SELECT current_schema()")) == ident.schema
    await r.store().close()
