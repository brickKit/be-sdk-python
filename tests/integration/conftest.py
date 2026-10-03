"""Integration fixtures: throwaway PostgreSQL 16 / 14 and NATS 2.12 started by `make itest`
(container prefix sdkb-py-). Each test gets its own schema, owner role and runtime role, created the way
the project's database initialisation creates them (be-protocol P10, roles table)."""

import os
import uuid
from pathlib import Path

import psycopg
import pytest
import yaml

PG16 = os.environ.get("BESDK_IT_PG16")
PG14 = os.environ.get("BESDK_IT_PG14")
NATS = os.environ.get("BESDK_IT_NATS")

pytestmark = pytest.mark.integration


def admin_dsn(hostport: str, db: str = "postgres") -> str:
    host, port = hostport.rsplit(":", 1)
    return f"postgresql://postgres:x@{host}:{port}/{db}"


def _need(v):
    if not v:
        pytest.skip("integration containers not running (make itest)")
    return v


class Identity:
    """One component's database identity: schema, owner role, runtime role, password files."""

    def __init__(self, hostport: str, tmp: Path, tag: str = ""):
        n = uuid.uuid4().hex[:8]
        self.hostport, self.schema = hostport, f"s_{tag}{n}"
        self.owner, self.user = f"o_{tag}{n}", f"r_{tag}{n}"
        self.owner_pw, self.user_pw = "own-" + n, "run-" + n
        self.tmp = tmp
        (tmp / "secrets").mkdir(exist_ok=True)
        self.owner_file = tmp / "secrets" / f"{self.owner}"
        self.user_file = tmp / "secrets" / f"{self.user}"
        self.owner_file.write_text(self.owner_pw + "\n")
        self.user_file.write_text(self.user_pw + "\n")
        with psycopg.connect(admin_dsn(hostport), autocommit=True) as c:
            c.execute(f"CREATE ROLE \"{self.owner}\" LOGIN PASSWORD '{self.owner_pw}'")
            c.execute(f"CREATE ROLE \"{self.user}\" LOGIN PASSWORD '{self.user_pw}'")
            c.execute(f'CREATE SCHEMA "{self.schema}"')
            c.execute(f'GRANT USAGE, CREATE ON SCHEMA "{self.schema}" TO "{self.owner}"')
            c.execute(f'GRANT USAGE ON SCHEMA "{self.schema}" TO "{self.user}"')
            c.execute(f'ALTER DEFAULT PRIVILEGES FOR ROLE "{self.owner}" IN SCHEMA "{self.schema}" '
                      f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{self.user}"')
            c.execute(f'ALTER DEFAULT PRIVILEGES FOR ROLE "{self.owner}" IN SCHEMA "{self.schema}" '
                      f'GRANT USAGE, SELECT ON SEQUENCES TO "{self.user}"')

    def env(self, **over) -> dict[str, str]:
        host, port = self.hostport.rsplit(":", 1)
        e = {"PG_HOST": host, "PG_PORT": port, "PG_DATABASE": "postgres", "PG_USER": self.user,
             "PG_PASSWORD_FILE": str(self.user_file), "PG_OWNER_USER": self.owner,
             "PG_OWNER_PASSWORD_FILE": str(self.owner_file), "PG_SCHEMA": self.schema}
        e.update(over)
        return {k: v for k, v in e.items() if v is not None}

    def sql(self, q: str):
        with psycopg.connect(admin_dsn(self.hostport), autocommit=True) as c:
            cur = c.execute(q)
            return cur.fetchall() if cur.description else None


DB_PROPS = {
    "PG_HOST": {"type": "string"}, "PG_PORT": {"type": "integer", "default": 5432},
    "PG_DATABASE": {"type": "string"}, "PG_USER": {"type": "string"},
    "PG_PASSWORD_FILE": {"type": "string", "secret": True, "mount": "file"},
    "PG_OWNER_USER": {"type": "string"}, "PG_OWNER_PASSWORD_FILE": {"type": "string", "secret": True, "mount": "file"},
    "PG_SCHEMA": {"type": "string"}, "PG_POOL_MAX": {"type": "integer", "default": 10},
    "PG_POOL_ACQUIRE_TIMEOUT": {"type": "string", "default": "5s"},
    "PG_CONN_MAX_LIFETIME": {"type": "string", "default": "30m"},
    "PG_CONN_MAX_IDLE_TIME": {"type": "string", "default": "5m"},
    "PG_MIGRATION_HOST": {"type": "string"}, "PG_MIGRATION_PORT": {"type": "integer"},
    "LOG_LEVEL": {"type": "string", "default": "info"},
}
DB_REQUIRED = ["PG_HOST", "PG_DATABASE", "PG_USER", "PG_PASSWORD_FILE", "PG_OWNER_USER", "PG_OWNER_PASSWORD_FILE",
               "PG_SCHEMA"]


def component_dir(tmp: Path, component_id: str = "conformance/widget-py", migrations: dict | None = None,
                  props: dict | None = None, extra: dict | None = None) -> Path:
    """A component directory: component.yaml, contracts/, migrations/ (with lifecycle.yaml)."""
    root = tmp / component_id.replace("/", "_")
    (root / "contracts" / "events").mkdir(parents=True, exist_ok=True)
    (root / "migrations").mkdir(parents=True, exist_ok=True)
    doc = {"metadata": {"id": component_id, "version": "1.0.0"},
           "configSchema": {"properties": {**DB_PROPS, **(props or {})}, "required": DB_REQUIRED},
           "deployment": {"port": 0}}
    doc.update(extra or {})
    (root / "component.yaml").write_text(yaml.safe_dump(doc))
    (root / "migrations" / "lifecycle.yaml").write_text("version: 1\ntables: {}\n")
    for name, sql in (migrations or {"0001_widget": "CREATE TABLE widget (id uuid PRIMARY KEY, name text NOT NULL);"}).items():
        (root / "migrations" / f"{name}.sql").write_text(sql)
    return root


@pytest.fixture
def pg16():
    return _need(PG16)


@pytest.fixture
def pg14():
    return _need(PG14)


@pytest.fixture
def nats_url():
    return "nats://" + _need(NATS)


@pytest.fixture
def ident(pg16, tmp_path):
    return Identity(pg16, tmp_path)
