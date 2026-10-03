"""The lifecycle engine, P0 (P16.1, P16.2, P16.5, P16.6, P16.10): partition windows from lifecycle.yaml
created by the migration and kept ahead at run time through the SECURITY DEFINER functions (the runtime
role has no DDL), expired outbox partitions dropped, sealed units immutable, every migrated table declared."""
import io
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

import besdk
from besdk import Module
from besdk.jobs.runner import JobsRuntime
from besdk.lifecycle.decl import LifecycleInvalid, units
from besdk.migrate import outbox_partition_name
from besdk.runtime import Runtime, Shared, Spec
from tests.integration.conftest import component_dir
from tests.integration.test_migrate import migrator

UTC = timezone.utc
MIG = {"0001_orders": "CREATE TABLE orders (id uuid NOT NULL, created_at timestamptz NOT NULL, status text, "
                      "PRIMARY KEY (id, created_at)) PARTITION BY RANGE (created_at);",
       "0002_audit": "CREATE TABLE audit_log (id bigint NOT NULL, at timestamptz NOT NULL) PARTITION BY RANGE (at);",
       "0003_settings": "CREATE TABLE settings (k text PRIMARY KEY, v text);"}
LIFE = """lifecycle: v1
tables:
  orders: {class: document, partition: {by: created_at, grain: week, ahead: 3}}
  audit_log: {class: audit, partition: {by: at, grain: month}}
  settings: {class: reference}
"""
PROPS = {"DATA_LIFECYCLE": {"type": "string", "default": '{"mode":"on"}'}}


def parts(ident, parent):
    return sorted(r[0] for r in ident.sql(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
        f"JOIN pg_namespace n ON n.oid = p.relnamespace WHERE n.nspname = '{ident.schema}' AND p.relname = '{parent}'"))


@pytest.fixture
def root(tmp_path, ident):
    r = component_dir(tmp_path, migrations=MIG, lifecycle=LIFE, props=PROPS)
    migrator(r, ident).up()
    return r


async def _empty(rt):
    return Module()


def runtime(root, ident, **env):
    spec = Spec(id="conformance/widget-py", migrations=root / "migrations", contracts=root / "contracts", create=_empty)
    return Runtime(spec, ident.env(**env), Shared.standalone(spec_id=spec.id), log_stream=io.StringIO())


def test_migration_creates_the_declared_windows(root, ident):
    now = datetime.now(UTC)
    assert parts(ident, "orders") == sorted(n for n, _, _ in units("orders", "week", now, ahead=3))
    assert parts(ident, "audit_log") == sorted(n for n, _, _ in units("audit_log", "month", now, ahead=2))
    ident.sql(f"INSERT INTO \"{ident.schema}\".orders VALUES (gen_random_uuid(), now(), 'NEW')")  # writes today


def test_every_migrated_table_is_declared(tmp_path, ident):
    r = component_dir(tmp_path, migrations=MIG, lifecycle="lifecycle: v1\ntables:\n  orders: {class: document}\n")
    with pytest.raises(LifecycleInvalid) as ei:
        migrator(r, ident).up()
    assert "audit_log" in str(ei.value) and "settings" in str(ei.value)


async def test_engine_keeps_the_window_and_drops_old_outbox_partitions(root, ident):
    s = ident.schema
    newest = sorted(n for n, _, _ in units("orders", "week", datetime.now(UTC), ahead=3))[-1]
    ident.sql(f'ALTER TABLE "{s}".orders DETACH PARTITION "{s}".{newest}')
    ident.sql(f'DROP TABLE "{s}".{newest}')
    old = (datetime.now(UTC) - timedelta(weeks=5)).date()
    old_lo = old - timedelta(days=old.isoweekday() - 1)
    old_name = outbox_partition_name(old_lo)
    ident.sql(f"SET ROLE \"{ident.owner}\"; SET search_path TO \"{s}\"; SELECT besdk_ensure_range_partition("
              f"'besdk_outbox', '{old_name}', '{old_lo}T00:00:00Z', '{old_lo + timedelta(weeks=1)}T00:00:00Z')")
    rt = runtime(root, ident)
    jr = JobsRuntime(rt, Module())
    assert await jr.run_once("be.lifecycle") == "ok"
    assert newest in parts(ident, "orders")  # recreated by the runtime role through the platform function
    assert old_name not in parts(ident, "besdk_outbox")
    units_rows = dict(ident.sql(f"SELECT unit_key, state FROM \"{s}\".besdk_lifecycle_units WHERE table_name = 'orders'"))
    assert units_rows[newest] == "ACTIVE"
    actions = {r[0] for r in ident.sql(f'SELECT action FROM "{s}".besdk_lifecycle_log')}
    assert {"partition_created", "partition_dropped"} <= actions
    await rt.store().close()


async def test_dry_run_and_off_change_nothing(root, ident):
    s = ident.schema
    newest = sorted(n for n, _, _ in units("orders", "week", datetime.now(UTC), ahead=3))[-1]
    ident.sql(f'ALTER TABLE "{s}".orders DETACH PARTITION "{s}".{newest}')
    ident.sql(f'DROP TABLE "{s}".{newest}')
    for mode in ("off", "dry-run"):
        rt = runtime(root, ident, DATA_LIFECYCLE=f'{{"mode":"{mode}"}}')
        await JobsRuntime(rt, Module()).run_once("be.lifecycle")
        assert newest not in parts(ident, "orders")
        await rt.store().close()


async def test_sealed_unit_refuses_update_delete_truncate(root, ident):
    s = ident.schema
    unit = units("orders", "week", datetime.now(UTC), ahead=0)[0][0]
    ident.sql(f"INSERT INTO \"{s}\".orders VALUES (gen_random_uuid(), now(), 'CLOSED')")
    rt = runtime(root, ident)
    await rt.store().tx(lambda tx: tx.seal("orders", unit))
    for sql in ("UPDATE orders SET status = 'X'", "DELETE FROM orders"):
        with pytest.raises(besdk.Error) as ei:
            await rt.store().tx(lambda tx, sql=sql: tx.execute(sql))
        assert (ei.value.reason, ei.value.code) == ("UNIT_SEALED", besdk.Code.FAILED_PRECONDITION)
    with pytest.raises(psycopg.Error) as pe:  # the runtime role cannot TRUNCATE at all; the owner is stopped too
        ident.sql(f'SET ROLE "{ident.owner}"; TRUNCATE "{s}".{unit}')
    assert pe.value.sqlstate == "BE001"
    assert ident.sql(f"SELECT state FROM \"{s}\".besdk_lifecycle_units WHERE unit_key = '{unit}'") == [("SEALED",)]
    await rt.store().close()
