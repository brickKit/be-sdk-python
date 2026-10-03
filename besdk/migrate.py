"""Migrations (be-protocol P11.1–P11.4, P16.6): the component's ``*.sql`` with yoyo, then the platform
migration, logged in as the owner ``PG_OWNER_USER`` on a dedicated direct connection
(``PG_MIGRATION_HOST`` / ``PG_MIGRATION_PORT``, falling back to ``PG_HOST`` / ``PG_PORT``).

yoyo keeps its state tables in the component's schema (``_yoyo_migration``, ``_yoyo_log``,
``_yoyo_version``, ``yoyo_lock``: the lock is per schema); platform migration ids start with ``besdk-``.
``lifecycle.yaml`` is never read as a migration. A file starting with ``-- be:no-transaction`` runs outside
a transaction. The runtime never uses this module except on the migrate entry point (P10.12).
"""

from __future__ import annotations

import logging
import re
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from importlib import resources
from pathlib import Path
from urllib.parse import quote

from besdk.config import Config

PLATFORM_VERSION = 1
PLATFORM_IDS = {1: "besdk-0001_platform"}
WINDOW_AHEAD = 2  # weeks of besdk_outbox kept ready beyond the current one (P16.6)
LOCK_RETRIES = 3
_COMPONENT_FILE = re.compile(r"(?!besdk-)([0-9]+)_[A-Za-z0-9_]+\.sql")


def component_files(directory: Path) -> list[Path]:
    """The component's forward migrations, in order; ``*.rollback.sql`` and ``lifecycle.yaml`` excluded."""
    return sorted(p for p in Path(directory).glob("*.sql")
                  if not p.name.endswith(".rollback.sql") and _COMPONENT_FILE.fullmatch(p.name))


def component_ids(directory: Path) -> list[str]:
    return [p.stem for p in component_files(directory)]


def image_component_version(directory: Path) -> str | None:
    """The newest component migration in the image, e.g. ``0007`` (``/_be/info``)."""
    files = component_files(directory)
    return files[-1].name.split("_", 1)[0] if files else None


def _platform_sql() -> str:
    ddl = resources.files("besdk").joinpath("_protocol/ddl")
    return "\n".join(ddl.joinpath(n).read_text() for n in sorted(x.name for x in ddl.iterdir() if x.name.endswith(".sql")))


def _stage(directory: Path, into: Path) -> None:
    """Copy the component's migrations for yoyo, translating ``-- be:no-transaction``; add the platform's."""
    for p in Path(directory).glob("*.sql"):
        text = p.read_text()
        if text.lstrip().startswith("-- be:no-transaction"):
            text = "-- transactional: false\n" + text
        (into / p.name).write_text(text)
    (into / f"{PLATFORM_IDS[1]}.sql").write_text(_platform_sql())


def iso_week_start(d: date) -> date:
    return d - timedelta(days=d.isoweekday() - 1)


class Migrator:
    def __init__(self, config: Config, directory: Path, component_id: str, logger: logging.Logger):
        self.config, self.directory, self.component_id, self.logger = config, Path(directory), component_id, logger
        self.schema = config.require("PG_SCHEMA")

    # --- connection ------------------------------------------------------------------------------

    def uri(self) -> str:
        c = self.config
        host = c.string("PG_MIGRATION_HOST") or c.require("PG_HOST")
        port = c.int("PG_MIGRATION_PORT", 0) or c.int("PG_PORT", 5432)
        user, pw = c.require("PG_OWNER_USER"), c.secret("PG_OWNER_PASSWORD_FILE").current()
        opts = quote("-c lock_timeout=5s -c statement_timeout=15min -c TimeZone=UTC", safe="")
        return (f"postgresql+psycopg://{quote(user, safe='')}:{quote(pw, safe='')}@{host}:{port}/"
                f"{quote(c.require('PG_DATABASE'), safe='')}?schema={quote(self.schema, safe='')}&options={opts}"
                f"&application_name={quote(self.component_id + ' migrate', safe='')}")

    def _backend(self):
        from yoyo import get_backend

        return get_backend(self.uri())

    def _migrations(self, staged: Path):
        from yoyo import read_migrations

        return read_migrations(str(staged))

    # --- commands --------------------------------------------------------------------------------

    def up(self) -> None:
        """Component migrations, then the platform migration and the current partition window. Idempotent."""
        newer = self.newer_than_image()
        if newer:
            self.logger.warning("schema_newer_than_image", extra={"migrations": ",".join(newer)})
        with tempfile.TemporaryDirectory() as tmp:
            _stage(self.directory, Path(tmp))
            backend = self._backend()
            ms = self._migrations(Path(tmp))
            for attempt in range(1, LOCK_RETRIES + 2):
                try:
                    with backend.lock(timeout=900):
                        backend.apply_migrations(backend.to_apply(ms))
                    break
                except Exception as e:  # noqa: BLE001
                    if _sqlstate(e) != "55P03" or attempt > LOCK_RETRIES:
                        if _sqlstate(e) == "55P03":
                            self._log_blockers(backend)
                        raise
                    time.sleep(0.5 * 2 ** attempt)
            self._after(backend)
        self.logger.info("migrations_applied", extra={"component": image_component_version(self.directory) or "",
                                                      "platform": PLATFORM_VERSION})

    def _after(self, backend) -> None:
        """The platform version row and the current window of every platform partitioned table."""
        backend.execute("INSERT INTO besdk_platform_version (component, version) VALUES (:c, :v) "
                        "ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now()",
                        {"c": self.component_id, "v": PLATFORM_VERSION})
        start = iso_week_start(datetime.now(timezone.utc).date())
        for i in range(WINDOW_AHEAD + 1):
            lo = start + timedelta(weeks=i)
            y, w, _ = lo.isocalendar()
            backend.execute("SELECT besdk_ensure_range_partition('besdk_outbox', :n, :lo, :hi)",
                            {"n": f"besdk_outbox_w{y}_{w:02d}", "lo": f"{lo}T00:00:00Z",
                             "hi": f"{lo + timedelta(weeks=1)}T00:00:00Z"})
        backend.commit()

    def _log_blockers(self, backend) -> None:
        try:
            rows = backend.execute(
                "SELECT DISTINCT a.pid, left(a.query, 200) FROM pg_locks l JOIN pg_stat_activity a USING (pid) "
                "JOIN pg_class c ON c.oid = l.relation JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :s AND l.granted AND a.pid <> pg_backend_pid()", {"s": self.schema}).fetchall()
            for pid, q in rows:
                self.logger.error("migration_blocked_by", extra={"pid": pid, "query": q})
        except Exception:  # noqa: BLE001 - best effort diagnostics
            pass

    def down(self, n: int) -> None:
        """Roll back the last ``n`` component migrations; the platform migration stays."""
        with tempfile.TemporaryDirectory() as tmp:
            _stage(self.directory, Path(tmp))
            backend = self._backend()
            ms = self._migrations(Path(tmp))
            with backend.lock(timeout=900):
                todo = [m for m in backend.to_rollback(ms) if not m.id.startswith("besdk-")][:n]
                backend.rollback_migrations(type(ms)(todo))

    def applied(self) -> list[str]:
        backend = self._backend()
        backend.ensure_internal_schema_updated()
        return [r[0] for r in backend.execute("SELECT migration_id FROM _yoyo_migration ORDER BY applied_at_utc").fetchall()]

    def status(self) -> dict:
        done = self.applied()
        mine = component_ids(self.directory)
        return {"applied": [i for i in done if not i.startswith("besdk-")],
                "pending": [i for i in mine if i not in done],
                "platform": max((v for v, i in PLATFORM_IDS.items() if i in done), default=None)}

    def newer_than_image(self) -> list[str]:
        """Applied component migrations this image does not have (P1.8)."""
        try:
            done = self.applied()
        except Exception:  # noqa: BLE001 - a fresh schema has no state tables yet
            return []
        mine = set(component_ids(self.directory))
        return [i for i in done if not i.startswith("besdk-") and i not in mine]


def _sqlstate(e: BaseException) -> str | None:
    while e is not None:
        s = getattr(e, "sqlstate", None)
        if s:
            return s
        e = e.__cause__ or e.__context__
    return None


