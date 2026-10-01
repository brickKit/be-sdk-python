import pytest

from besdk.shell_runner import parse_served_members

PEM = "-----BEGIN PRIVATE KEY-----\nMIIB$abc\"q\\x\n-----END PRIVATE KEY-----\n"


def test_unset_is_error():
    with pytest.raises(RuntimeError, match="未设置"):
        parse_served_members(None)


def test_empty_string_is_error():
    with pytest.raises(RuntimeError):
        parse_served_members("  ")


def test_zero_members():
    assert parse_served_members("[]") == []


def test_fields_and_secret_verbatim():
    raw = ('[{"componentId":"infra/print","version":"2.0.0","httpPort":8402,'
           '"extraPorts":[{"name":"grpc","port":9402}],'
           '"config":{"PG_SCHEMA":"infra_print","K":"-----BEGIN PRIVATE KEY-----\\nMIIB$abc\\"q\\\\x\\n-----END PRIVATE KEY-----\\n"}}]')
    [m] = parse_served_members(raw)
    assert (m.component_id, m.version, m.http_port) == ("infra/print", "2.0.0", 8402)
    assert m.extra_ports == {"grpc": 9402}
    assert m.config["PG_SCHEMA"] == "infra_print"
    assert m.config["K"] == PEM
