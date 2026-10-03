"""The entry point (P1.1, P1.2, P1.6, P1.13): argument errors exit 64 before the configuration is read,
configuration errors exit 78 naming the key, SIGTERM drains in-flight requests and exits 0."""
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from besdk.main import main
from tests.unit.fixture_component import spec

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("argv", [["serve"], ["migrate"], ["migrate", "sideways"], ["migrate", "down"],
                                  ["migrate", "down", "x"], ["job"], ["job", "run"], ["--help"]])
def test_unknown_arguments_exit_64_before_config(argv, capsys):
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=argv, env={})  # an empty environment would be a 78 if it were read
    assert ei.value.code == 64


def test_config_errors_exit_78_one_line_per_key(capsys):
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=[], env={"TINY_LIMIT": "three"})
    assert ei.value.code == 78
    lines = [json.loads(x) for x in capsys.readouterr().out.splitlines()]
    keys = sorted(x["key"] for x in lines if x["msg"] == "config_invalid")
    assert keys == ["TINY_LIMIT", "TINY_NAME"]


def test_component_id_mismatch_exits_78(capsys):
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=[], env={"TINY_NAME": "x", "COMPONENT_ID": "conformance/other"})
    assert ei.value.code == 78


def test_job_run_unknown_job_exits_64(capsys):
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=["job", "run", "nope"], env={"TINY_NAME": "x"})
    assert ei.value.code == 64


def test_job_run_runs_the_job_once_and_exits_0(capsys):
    from tests.unit import fixture_component as fc

    fc.PINGS.clear()
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=["job", "run", "tiny.ping"], env={"TINY_NAME": "x"})
    assert ei.value.code == 0 and fc.PINGS == ["tiny.ping"]


def test_job_run_failure_exits_1(capsys):
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=["job", "run", "tiny.fail"], env={"TINY_NAME": "x"})
    assert ei.value.code == 1


def test_job_run_ignores_enabled_false(capsys):
    from tests.unit import fixture_component as fc

    fc.PINGS.clear()
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=["job", "run", "tiny.ping"],
             env={"TINY_NAME": "x", "JOBS_OVERRIDES": '{"tiny.ping": {"enabled": false}}'})
    assert ei.value.code == 0 and fc.PINGS == ["tiny.ping"]


@pytest.mark.parametrize("argv", [[], ["job", "run", "tiny.ping"]])
def test_invalid_jobs_overrides_exit_78(argv, capsys):
    with pytest.raises(SystemExit) as ei:
        main(spec, argv=argv, env={"TINY_NAME": "x", "JOBS_OVERRIDES": '{"tiny.ping": {"interval": "soon"}}'})
    assert ei.value.code == 78
    assert any(json.loads(x).get("key") == "JOBS_OVERRIDES" for x in capsys.readouterr().out.splitlines())


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_serve_and_graceful_sigterm():
    port = _free_port()
    env = {**os.environ, "TINY_NAME": "x", "PORT": str(port), "SHUTDOWN_GRACE": "5s", "PYTHONPATH": str(ROOT)}
    p = subprocess.Popen([sys.executable, "-m", "tests.unit.fixture_component"], cwd=ROOT, env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        for _ in range(100):
            try:
                if httpx.get(f"http://127.0.0.1:{port}/healthz").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.05)
        assert httpx.get(f"http://[::1]:{port}/healthz").status_code == 200
        assert httpx.get(f"http://127.0.0.1:{port}/readyz").status_code == 200
        import threading
        out = {}
        t = threading.Thread(target=lambda: out.setdefault(
            "r", httpx.get(f"http://127.0.0.1:{port}/conformance/tiny/slow?ms=1500", timeout=5)))
        t.start()
        time.sleep(0.3)
        p.send_signal(signal.SIGTERM)
        t.join()
        assert out["r"].status_code == 200  # in-flight request finished
        assert p.wait(timeout=10) == 0
    finally:
        if p.poll() is None:
            p.kill()
