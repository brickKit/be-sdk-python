"""Config (P2): validated once at start from configSchema + the protocol catalogue; strict typed reads;
undeclared reads refused; secrets read from files and re-read when they change."""
import os

import pytest

from besdk.config import Config, ConfigErrors, Manifest
from besdk.errors import ProtocolError

SCHEMA = {
    "properties": {
        "PG_HOST": {"type": "string"},
        "PG_PORT": {"type": "integer", "default": 5432},
        "PG_PASSWORD_FILE": {"type": "string", "secret": True, "mount": "file"},
        "PG_POOL_ACQUIRE_TIMEOUT": {"type": "string", "default": "5s"},
        "PG_MIGRATION_HOST": {"type": "string"},
        "EVENTS_BACKOFF": {"type": "string", "default": "1s,10s,1m,5m,15m,30m,1h"},
        "AUTHZ_URL": {"type": "string"},
        "AUTHZ_GRPC_URL": {"type": "string"},
        "IAM_GRPC_URL": {"type": "string"},
        "LOG_LEVEL": {"type": "string", "default": "info"},
        "S3_FORCE_PATH_STYLE": {"type": "boolean", "default": False},
        "JOBS_OVERRIDES": {"type": "string", "default": ""},
        "WIDGET_APPROVE_TIMEOUT": {"type": "string", "default": "30s"},
        "WIDGET_HOLD": {"type": "integer", "default": 900, "minimum": 1},
    },
    "required": ["PG_HOST", "PG_PASSWORD_FILE", "AUTHZ_URL"],
}


def manifest(**over):
    doc = {"metadata": {"id": "conformance/widget-py", "version": "1.0.0"},
           "dependencies": {"components": ["conformance/peer@1.0.0", {"id": "mdm/org@3.0.0", "optional": True}]},
           "configSchema": SCHEMA, "deployment": {"port": 8080, "extraPorts": [{"name": "grpc", "port": 9090}]}}
    doc.update(over)
    return Manifest.from_doc(doc)


@pytest.fixture
def secret_file(tmp_path):
    p = tmp_path / "PG_PASSWORD_FILE"
    p.write_text("pw-1\n")
    return p


def env(secret_file, **over):
    e = {"COMPONENT_ID": "conformance/widget-py", "COMPONENT_VERSION": "1.0.0", "PG_HOST": "db",
         "PG_PASSWORD_FILE": str(secret_file), "AUTHZ_URL": "http://infra-authz-3-0-0:8223/",
         "AUTHZ_GRPC_URL": "http://infra-authz-3-0-0:9223", "CONFORMANCE_PEER_ENDPOINT": "http://peer:8080",
         "CONFORMANCE_PEER_GRPC_ENDPOINT": "http://peer:9090", "UNRELATED": "x"}
    e.update(over)
    return {k: v for k, v in e.items() if v is not None}


def test_typed_reads_and_defaults(secret_file):
    c = Config.load(env(secret_file), manifest())
    assert c.require("PG_HOST") == "db"
    assert c.int("PG_PORT") == 5432
    assert c.duration("PG_POOL_ACQUIRE_TIMEOUT") == 5.0
    assert c.durations("EVENTS_BACKOFF")[:2] == [1.0, 10.0]
    assert c.bool("S3_FORCE_PATH_STYLE") is False
    assert c.string("LOG_LEVEL") == "info"
    assert c.json("JOBS_OVERRIDES", {}) == {}
    assert c.duration("WIDGET_APPROVE_TIMEOUT") == 30.0
    assert c.int("WIDGET_HOLD") == 900


def test_default_from_another_key(secret_file):
    c = Config.load(env(secret_file), manifest())
    assert c.string("PG_MIGRATION_HOST") == "db"


def test_family_addresses(secret_file):
    c = Config.load(env(secret_file), manifest())
    assert c.family("AUTHZ_URL") == "http://infra-authz-3-0-0:8223"
    assert c.family("AUTHZ_GRPC_URL") == "infra-authz-3-0-0:9223"
    assert c.family("IAM_GRPC_URL") is None  # absent = degrade


def test_dependency_endpoints(secret_file):
    c = Config.load(env(secret_file), manifest())
    assert c.endpoint("conformance/peer", "grpc") == "peer:9090"
    assert c.endpoint("conformance/peer") == "peer:8080"
    assert c.endpoint("mdm/org") is None  # optional, not installed


def test_all_errors_reported_together(secret_file):
    with pytest.raises(ConfigErrors) as ei:
        Config.load(env(secret_file, PG_HOST=None, PG_PORT="5432x", AUTHZ_URL="http://authz:8223/authz/v2",
                        WIDGET_HOLD="0"), manifest())
    got = sorted((e.key, e.reason) for e in ei.value.errors)
    assert got == [("AUTHZ_URL", "CONFIG_INVALID"), ("PG_HOST", "CONFIG_MISSING"), ("PG_PORT", "CONFIG_INVALID"),
                   ("WIDGET_HOLD", "CONFIG_INVALID")]


def test_component_id_mismatch(secret_file):
    with pytest.raises(ConfigErrors) as ei:
        Config.load(env(secret_file, COMPONENT_ID="conformance/other"), manifest())
    assert [e.key for e in ei.value.errors] == ["COMPONENT_ID"]


def test_undeclared_read_is_refused(secret_file):
    c = Config.load(env(secret_file), manifest())
    with pytest.raises(ProtocolError) as ei:
        c.string("UNRELATED")
    assert ei.value.reason == "CONFIG_UNDECLARED"


def test_required_dependency_endpoint_missing(secret_file):
    with pytest.raises(ConfigErrors) as ei:
        Config.load(env(secret_file, CONFORMANCE_PEER_GRPC_ENDPOINT=None, CONFORMANCE_PEER_ENDPOINT=None), manifest())
    assert [e.key for e in ei.value.errors] == ["CONFORMANCE_PEER_ENDPOINT"]


def test_secret_file_read_and_reread(secret_file):
    c = Config.load(env(secret_file), manifest())
    s = c.secret("PG_PASSWORD_FILE")
    assert s.current() == "pw-1"
    assert c.string("PG_PASSWORD_FILE") == str(secret_file)  # String of a _FILE key is the path
    secret_file.write_text("pw-2-longer\n")
    assert s.poll() is True
    assert s.current() == "pw-2-longer"
    assert s.poll() is False


def test_secret_reread_failure_keeps_last_value(secret_file):
    failures = []
    c = Config.load(env(secret_file), manifest())
    s = c.secret("PG_PASSWORD_FILE")
    s.on_failure = lambda key, err: failures.append(key)
    secret_file.write_text("")  # empty after a change: keep the last good value
    os.utime(secret_file, ns=(1, 1))
    s.poll()
    assert s.current() == "pw-1"
    assert failures == ["PG_PASSWORD_FILE"]


def test_secret_missing_file_is_config_error(tmp_path, secret_file):
    with pytest.raises(ConfigErrors) as ei:
        Config.load(env(secret_file, PG_PASSWORD_FILE=str(tmp_path / "nope")), manifest())
    assert [(e.key, e.reason) for e in ei.value.errors] == [("PG_PASSWORD_FILE", "CONFIG_INVALID")]


def test_secret_of_non_file_key_refused(secret_file):
    c = Config.load(env(secret_file), manifest())
    with pytest.raises(ProtocolError):
        c.secret("PG_HOST")


def test_manifest_ports_and_member_config(secret_file):
    m = manifest()
    assert m.id == "conformance/widget-py" and m.port == 8080 and m.extra_ports == {"grpc": 9090}
    assert [d.id for d in m.dependencies] == ["conformance/peer", "mdm/org"]
    assert [d.optional for d in m.dependencies] == [False, True]
