import pytest

from besdk.manifest import load_own_http_port, load_own_ports


def _write(tmp_path, body):
    f = tmp_path / "component.yaml"
    f.write_text(body, encoding="utf-8")
    return f


def test_load_own_ports_reads_http_and_extra(tmp_path):
    f = _write(tmp_path, "deployment:\n  port: 8001\n  extraPorts:\n    - {name: grpc, port: 9400}\n")
    ports = load_own_ports(f)
    assert ports.http_port == 8001 and ports.extra_ports == {"grpc": 9400}


@pytest.mark.parametrize("loader", [load_own_ports, load_own_http_port])
@pytest.mark.parametrize("body", ["- a\n- b\n", "just a string\n", "42\n"])
def test_top_level_not_a_mapping_is_a_clear_error(tmp_path, loader, body):
    f = _write(tmp_path, body)
    with pytest.raises(RuntimeError, match="component.yaml"):
        loader(f)


@pytest.mark.parametrize("loader", [load_own_ports, load_own_http_port])
def test_deployment_not_a_mapping_is_a_clear_error(tmp_path, loader):
    f = _write(tmp_path, "deployment: [1, 2]\n")
    with pytest.raises(RuntimeError, match="deployment"):
        loader(f)


def test_load_own_ports_missing_port_is_a_clear_error(tmp_path):
    f = _write(tmp_path, "deployment: {}\n")
    with pytest.raises(RuntimeError, match="deployment.port"):
        load_own_ports(f)


def test_both_loaders_parse_the_file_once_each(tmp_path, monkeypatch):
    import yaml

    f = _write(tmp_path, "deployment:\n  port: 8001\n")
    calls = []
    real = yaml.safe_load
    monkeypatch.setattr(yaml, "safe_load", lambda s: calls.append(1) or real(s))
    load_own_ports(f)
    assert len(calls) == 1
