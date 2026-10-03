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
AUTHZ_ID = "besdk-0001_authz"  # ddl/07, only for a component that declares resources (CP-DB-04)
AUTHZ_DDL = "07-authz-projection.sql"
LOCK_RETRIES = 3
_COMPONENT_FILE = re.compile(r"(?!besdk-)([0-9]+)_[A-Za-z0-9_]+\.sql")
_CONTRACT = re.compile(r"--\s*be:contract\s+after=(\S+)")


class ContractBlocked(Exception):
    """P11.4: a contract step waits for older versions to disconnect; the migration step exits 1."""


def semver(v: str) -> tuple[int, ...]:
    core = re.split(r"[-+]", v, maxsplit=1)[0]
    return tuple(int(x) if x.isdigit() else 0 for x in core.split("."))


def contract_after(path: Path) -> str | None:
    """``<version>`` of a ``-- be:contract after=<version>`` header line, else None."""
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("--"):
            return None
        m = _CONTRACT.match(line.strip())
        if m:
            return m.group(1)
    return None


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
    names = sorted(x.name for x in ddl.iterdir() if x.name.endswith(".sql") and x.name != AUTHZ_DDL)
    return "\n".join(ddl.joinpath(n).read_text() for n in names)


def declares_resources(component_root: Path) -> bool:
    from besdk.auth.resources import assembly

    return bool(assembly(component_root).get("resources"))


def platform_ids(component_root: Path) -> list[str]:
    """The platform migrations this component gets: the platform, plus the projection with resources."""
    return [PLATFORM_IDS[PLATFORM_VERSION]] + ([AUTHZ_ID] if declares_resources(component_root) else [])


def _stage(directory: Path, into: Path) -> None:
    """Copy the component's migrations for yoyo, translating ``-- be:no-transaction``; add the platform's."""
    for p in Path(directory).glob("*.sql"):
        text = p.read_text()
        if text.lstrip().startswith("-- be:no-transaction"):
            text = "-- transactional: false\n" + text
        (into / p.name).write_text(text)
    (into / f"{PLATFORM_IDS[1]}.sql").write_text(_platform_sql())
    if declares_resources(Path(directory).parent):
        ddl = resources.files("besdk").joinpath("_protocol/ddl")
        (into / f"{AUTHZ_ID}.sql").write_text(ddl.joinpath(AUTHZ_DDL).read_text())


def outbox_partition_name(d: date) -> str:
    """``besdk_outbox_<isoyear>w<ww>`` for the ISO week containing ``d`` (stage-B ruling, P16)."""
    y, w, _ = d.isocalendar()
    return f"besdk_outbox_{y}w{w:02d}"


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
                        self._apply_gated(backend, backend.to_apply(ms))
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

    def _apply_gated(self, backend, todo) -> None:
        """Apply in order; stop before a contract step while an older version is still connected (P11.4)."""
        for i, m in enumerate(todo):
            after = contract_after(Path(m.path)) if getattr(m, "path", None) else None
            if after is None:
                continue
            blocking = self._older_sessions(backend, after)
            if blocking:
                backend.apply_migrations(type(todo)(list(todo)[:i]))
                detail = ", ".join(f"{v} ({n} sessions)" for v, n in blocking)
                self.logger.error("contract_step_blocked", extra={"migration": m.id, "after": after,
                                                                  "blocking": detail})
                raise ContractBlocked(f"{m.id} waits for versions <= {after} to disconnect: {detail}")
        backend.apply_migrations(todo)

    def _older_sessions(self, backend, after: str) -> list[tuple[str, int]]:
        prefix = self.component_id + "@"
        rows = backend.execute(
            "SELECT application_name, count(*) FROM pg_stat_activity WHERE left(application_name, :n) = :p "
            "GROUP BY 1", {"n": len(prefix), "p": prefix}).fetchall()
        out = [(name[len(prefix):], n) for name, n in rows]
        return sorted((v, n) for v, n in out if semver(v) <= semver(after))

    def _declared(self, backend) -> "Declaration":
        """lifecycle.yaml loads, keeps its invariants and declares every table of the schema (P16.1)."""
        from besdk.lifecycle.decl import Declaration, LifecycleInvalid

        decl = Declaration.load(self.directory)
        rows = backend.execute(
            "SELECT c.relname FROM pg_class c WHERE c.relnamespace = current_schema()::regnamespace "
            "AND c.relkind IN ('r', 'p') AND NOT c.relispartition AND left(c.relname, 6) <> 'besdk_' "
            "AND left(c.relname, 6) <> '_yoyo_' AND c.relname <> 'yoyo_lock'").fetchall()
        missing = sorted(r[0] for r in rows if r[0] not in decl.tables)
        if missing:
            raise LifecycleInvalid(f"lifecycle.yaml does not declare: {', '.join(missing)}")
        return decl

    def _after(self, backend) -> None:
        """The platform version row and the current window of every partitioned table (P16.6, P16.10)."""
        from besdk.lifecycle.decl import units
        from besdk.lifecycle.engine import OUTBOX_AHEAD

        decl = self._declared(backend)
        backend.execute("INSERT INTO besdk_platform_version (component, version) VALUES (:c, :v) "
                        "ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = now()",
                        {"c": self.component_id, "v": PLATFORM_VERSION})
        now = datetime.now(timezone.utc)
        window = [("besdk_outbox", u) for u in units("besdk_outbox", "week", now, ahead=OUTBOX_AHEAD)]
        for name in decl.partitioned():
            t = decl.tables[name]
            window += [(name, u) for u in units(name, t.grain, now, ahead=t.ahead)]
        for parent, (n, lo, hi) in window:
            backend.execute("SELECT besdk_ensure_range_partition(:p, :n, :lo, :hi)",
                            {"p": parent, "n": n, "lo": lo.isoformat(), "hi": hi.isoformat()})
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


