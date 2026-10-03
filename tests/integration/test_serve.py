"""Serving with a database (P1.4, P10.7): /readyz waits for the identity probe and the migrations, then
latches; /healthz never depends on PostgreSQL."""
import asyncio
import socket

import httpx

from besdk import Module
from besdk.config import Config, Manifest
from besdk.runtime import Spec
from besdk.serve import serve
from tests.integration.conftest import component_dir
from tests.integration.test_migrate import migrator


async def _create(rt):
    return Module()


def _port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_readyz_waits_for_migrations_then_latches(tmp_path, ident):
    root = component_dir(tmp_path)
    port = _port()
    env = ident.env(PORT=str(port))
    m = Manifest.load(root / "component.yaml")
    spec = Spec(id=m.id, migrations=root / "migrations", contracts=root / "contracts", create=_create)
    stop = asyncio.Event()
    task = asyncio.create_task(serve(spec, env, m, Config.load(env, m), stop=stop))
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as c:
        for _ in range(100):
            try:
                if (await c.get("/healthz")).status_code == 200:
                    break
            except httpx.HTTPError:
                await asyncio.sleep(0.05)
        r = await c.get("/readyz")
        assert r.status_code == 503 and "migrations" in r.json()["metadata"]["waiting"]
        await asyncio.to_thread(migrator(root, ident).up)
        for _ in range(400):  # the readiness loop re-checks every 15 s
            if (await c.get("/readyz")).status_code == 200:
                break
            await asyncio.sleep(0.1)
        assert (await c.get("/readyz")).status_code == 200
        info = (await c.get("/_be/info")).json()
        assert info["migrations"] == {"component": "0001", "platform": 1} and "db" in info["profiles"]
        assert 'be_db_identity_ok{component="conformance/widget-py"} 1.0' in (await c.get("/metrics")).text
    stop.set()
    assert await task == 0
