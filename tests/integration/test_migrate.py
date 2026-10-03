"""Migrations (P1.1, P11.1–P11.4, P16.6): yoyo as the owner, state in the component's schema, the
platform migration after the component's, idempotent, lifecycle.yaml ignored, per-schema lock."""
import io
import threading
from datetime import datetime, timezone

import psycopg
import pytest

from besdk import logs
from besdk.config import Config, Manifest
from besdk.migrate import PLATFORM_VERSION, Migrator, image_component_version
from tests.integration.conftest import Identity, component_dir


def migrator(root, ident, **env):
    m = Manifest.load(root / "component.yaml")
    cfg = Config.load(ident.env(**env), m)
    return Migrator(cfg, root / "migrations", m.id, logs.member_logger(m.id, "1", stream=io.StringIO()))


def tables(ident):
    return {r[0]: r[1] for r in ident.sql(
        f"SELECT c.relname, pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{ident.schema}' AND c.relkind IN ('r', 'p')")}


def test_up_twice_is_idempotent_and_owned_by_the_owner(tmp_path, ident):
    root = component_dir(tmp_path)
    mg = migrator(root, ident)
    mg.up()
    t1 = tables(ident)
    assert "widget" in t1 and "besdk_outbox" in t1 and "_yoyo_migration" in t1 and "yoyo_lock" in t1
    assert set(t1.values()) == {ident.owner}
    log1 = ident.sql(f'SELECT count(*) FROM "{ident.schema}"._yoyo_log')[0][0]
    mg.up()
    assert tables(ident) == t1
    assert ident.sql(f'SELECT count(*) FROM "{ident.schema}"._yoyo_log')[0][0] == log1
    ids = {r[0] for r in ident.sql(f'SELECT migration_id FROM "{ident.schema}"._yoyo_migration')}
    assert ids == {"0001_widget", "besdk-0001_platform"}
    assert ident.sql(f'SELECT component, version FROM "{ident.schema}".besdk_platform_version') == [
        ("conformance/widget-py", PLATFORM_VERSION)]
    assert not ident.sql("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%yoyo%'")


def test_outbox_window_accepts_writes_today(tmp_path, ident):
    migrator(component_dir(tmp_path), ident).up()
    iy, iw, _ = datetime.now(timezone.utc).isocalendar()
    parts = {r[0] for r in ident.sql(
        f"SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
        f"JOIN pg_namespace n ON n.oid = p.relnamespace WHERE n.nspname = '{ident.schema}' AND p.relname = 'besdk_outbox'")}
    assert f"besdk_outbox_{iy}w{iw:02d}" in parts and len(parts) == 3  # stage-B ruling: <isoyear>w<ww>


def test_down_and_status(tmp_path, ident):
    root = component_dir(tmp_path, migrations={"0001_widget": "CREATE TABLE widget (id uuid PRIMARY KEY);",
                                               "0002_note": "ALTER TABLE widget ADD COLUMN note text;"})
    (root / "migrations" / "0002_note.rollback.sql").write_text("ALTER TABLE widget DROP COLUMN note;")
    mg = migrator(root, ident)
    mg.up()
    assert mg.status() == {"applied": ["0001_widget", "0002_note"], "pending": [], "platform": PLATFORM_VERSION}
    mg.down(1)
    assert mg.status()["pending"] == ["0002_note"]
    assert "besdk_outbox" in tables(ident)  # the platform migration is never rolled back
    assert image_component_version(root / "migrations") == "0002"


def test_no_transaction_header(tmp_path, ident):
    root = component_dir(tmp_path, migrations={
        "0001_widget": "CREATE TABLE widget (id uuid PRIMARY KEY, name text);",
        "0002_idx": "-- be:no-transaction\nCREATE INDEX CONCURRENTLY widget_name ON widget (name);"})
    migrator(root, ident).up()
    assert ident.sql(f"SELECT 1 FROM pg_indexes WHERE schemaname = '{ident.schema}' AND indexname = 'widget_name'")


def test_two_schemas_migrate_concurrently(tmp_path, pg16):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a, b = Identity(pg16, tmp_path / "a"), Identity(pg16, tmp_path / "b")
    ra, rb = component_dir(tmp_path / "a", "conformance/a"), component_dir(tmp_path / "b", "conformance/b")
    errs = []

    def run(root, ident):
        try:
            migrator(root, ident).up()
        except Exception as e:  # noqa: BLE001
            errs.append(e)

    ts = [threading.Thread(target=run, args=x) for x in ((ra, a), (rb, b))]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert errs == []


def test_newer_schema_is_detected(tmp_path, ident):
    root = component_dir(tmp_path, migrations={"0001_widget": "CREATE TABLE widget (id uuid PRIMARY KEY);",
                                               "0002_more": "CREATE TABLE more (id uuid PRIMARY KEY);"})
    migrator(root, ident).up()
    (root / "migrations" / "0002_more.sql").unlink()  # an older image
    mg = migrator(root, ident)
    assert mg.newer_than_image() == ["0002_more"]
    mg.up()  # the migrate entry point only warns (rolling back must not be blocked)


def test_migration_connects_as_owner_through_migration_host(tmp_path, ident):
    root = component_dir(tmp_path)
    host, port = ident.hostport.rsplit(":", 1)
    migrator(root, ident, PG_HOST="nowhere.invalid", PG_PORT="1", PG_MIGRATION_HOST=host, PG_MIGRATION_PORT=port).up()
    assert tables(ident)["widget"] == ident.owner


@pytest.mark.parametrize("which", ["pg14"])
def test_pg14_floor(tmp_path, pg14, which):
    ident = Identity(pg14, tmp_path)
    migrator(component_dir(tmp_path), ident).up()
    assert "besdk_outbox" in tables(ident)


def test_authz_projection_tables_only_with_resources(tmp_path, ident):
    """ddl/07 only in schemas whose component owns resource types (stage-B ruling, CP-DB-04)."""
    root = component_dir(tmp_path)
    migrator(root, ident).up()
    assert "besdk_authz_acl" not in tables(ident)
    (root / "assembly.yaml").write_text(
        "resources:\n  - {type: conformance.widget.widget, view_key: conformance.widget.view, relations: {}, "
        "derivation: direct, dimensions: [owner]}\n")
    m = migrator(root, ident)
    m.up()
    assert {"besdk_authz_acl", "besdk_authz_cursor"} <= set(tables(ident))
    assert "besdk-0001_authz" in m.applied()


def test_contract_step_waits_for_older_versions(tmp_path, ident):
    """P11.4: `-- be:contract after=<v>` runs only once no session <id>@<w> with w <= v is connected; the
    files before it stay applied and the step fails with exit 1 (ContractBlocked)."""
    from besdk.migrate import ContractBlocked

    root = component_dir(tmp_path, migrations={
        "0001_widget": "CREATE TABLE widget (id uuid PRIMARY KEY, legacy text);",
        "0002_note": "ALTER TABLE widget ADD COLUMN note text;",
        "0003_drop_legacy": "-- be:contract after=1.4.0\nALTER TABLE widget DROP COLUMN legacy;"})
    host, port = ident.hostport.rsplit(":", 1)

    def session(version):
        return psycopg.connect(f"postgresql://postgres:x@{host}:{port}/postgres",
                               application_name=f"conformance/widget-py@{version}")

    with session("1.10.0"), session("1.4.0"):
        with pytest.raises(ContractBlocked) as ei:
            migrator(root, ident).up()
        assert "0003_drop_legacy" in str(ei.value) and "1.4.0" in str(ei.value) and "1.10.0" not in str(ei.value)
        assert migrator(root, ident).status()["pending"] == ["0003_drop_legacy"]
    with session("1.5.0"):
        migrator(root, ident).up()  # only newer versions remain
    assert migrator(root, ident).status()["pending"] == []
