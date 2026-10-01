import urllib.parse

import pytest

from besdk.connection import nats_url, pg_dsn
from besdk.runtime import Config

BASE = {"PG_HOST": "db", "PG_PORT": "5432", "PG_DATABASE": "brickkit_db", "PG_USER": "infra_print", "PG_PASSWORD": "pw"}


def test_pg_dsn():
    assert pg_dsn(Config(BASE)) == "postgresql://infra_print:pw@db:5432/brickkit_db"


def test_pg_dsn_escapes_password():
    dsn = pg_dsn(Config({**BASE, "PG_PASSWORD": "p@ss:w/rd%1"}))
    assert urllib.parse.unquote(urllib.parse.urlsplit(dsn).password) == "p@ss:w/rd%1"


def test_pg_dsn_lists_all_missing():
    with pytest.raises(ValueError) as e:
        pg_dsn(Config({"PG_PASSWORD": "x"}))
    for k in ("PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER"):
        assert k in str(e.value)


def test_pg_dsn_empty_password_allowed_but_key_required():
    pg_dsn(Config({**BASE, "PG_PASSWORD": ""}))
    with pytest.raises(ValueError, match="PG_PASSWORD"):
        pg_dsn(Config({k: v for k, v in BASE.items() if k != "PG_PASSWORD"}))


def test_nats_url():
    assert nats_url(Config({"NATS_URL": "nats://n:4222"})) == "nats://n:4222"
    with pytest.raises(ValueError):
        nats_url(Config({}))
