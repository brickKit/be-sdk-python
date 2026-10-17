import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_sigterm_stops_the_process_gracefully():  # P1.14, P1.16
    port = free_port()
    env = {**os.environ, "COMPONENT_ID": "demo/proc", "COMPONENT_VERSION": "3.0.0", "PORT": str(port),
           "STOP_DRAIN_DELAY": "100ms"}
    p = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(__file__), "sigterm_app.py")], env=env,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        for _ in range(300):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1) as r:
                    if r.status == 200:
                        break
            except OSError:
                time.sleep(0.02)
        else:
            raise AssertionError("never ready")
        p.send_signal(signal.SIGTERM)
        out, err = p.communicate(timeout=10)
    finally:
        p.kill()
    assert p.returncode == 0, err
    events = [json.loads(line).get("event") for line in out.splitlines() if line.startswith("{")]
    assert events[-1] == "stopped" and "stopping" in events
