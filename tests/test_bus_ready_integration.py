"""CK-BUS-10 (P6.34): a bus that starts after the component is waited for; refused credentials exit 78."""
import asyncio
import io
import os
import time
import uuid
from urllib.parse import urlsplit

import httpx
import pytest

from besdk import launcher
from besdk.component import Definition
from besdk.config import EnvSource
from tests import pgtest
from tests.test_events_interop import PyConsumer, free_port, member_env

pytestmark = pytest.mark.integration

SEEN = {"0001_seen.up.sql": "CREATE TABLE IF NOT EXISTS seen (event_id uuid PRIMARY KEY, size int NOT NULL);"}


async def late_proxy(target_host, target_port, delay):
    """Listens on a free port only after delay, then forwards to the bus: the machine boots both, and the component's
    unit can't wait for the bus."""
    port = free_port()

    async def pipe(r, w):
        try:
            while data := await r.read(65536):
                w.write(data)
                await w.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            w.close()

    async def handle(r, w):
        ur, uw = await asyncio.open_connection(target_host, target_port)
        await asyncio.gather(pipe(r, uw), pipe(ur, w))

    async def later():
        await asyncio.sleep(delay)
        return await asyncio.start_server(handle, "127.0.0.1", port)

    return port, asyncio.create_task(later())


async def serve(d, env, stop, out):
    for args in (["migrate"], []):
        if args:
            env = {**env, "COMPONENT_ID": d.id, "PORT": str(free_port())}
            return env, asyncio.create_task(launcher.run(d, launcher.Options(env=EnvSource(lookup_env=env.get), stdout=out,
                                                                             stderr=out, host="127.0.0.1"), stop))
        assert await launcher.run(d, launcher.Options(args=args, env=EnvSource(lookup_env=env.get), stdout=out, stderr=out),
                                  asyncio.Event()) == 0, out.getvalue()


async def test_a_late_bus_is_waited_for_and_refused_credentials_exit_78():
    nats = urlsplit(os.environ["BE_SDK_TEST_NATS_URL"])
    d = Definition("demo/pylate" + uuid.uuid4().hex[:6], "3.0.0", PyConsumer, subscribes=True, migrations=SEEN)
    port, proxy = await late_proxy(nats.hostname, nats.port or 4222, 1.5)
    env = await member_env("py_bus_late", {"BUS_URL": f"nats://127.0.0.1:{port}"})
    out, stop = io.StringIO(), asyncio.Event()
    start = time.monotonic()
    env, task = await serve(d, env, stop, out)
    async with httpx.AsyncClient() as c:
        for _ in range(1000):
            try:
                if (await c.get(f"http://127.0.0.1:{env['PORT']}/readyz")).status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            assert not task.done(), out.getvalue()
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("never ready\n" + out.getvalue())
    assert time.monotonic() - start >= 1.5, "ready only once the bus answers"
    assert "waiting for the bus" in out.getvalue()
    stop.set()
    assert await task == 0, out.getvalue()
    (await proxy).close()

    u = urlsplit(pgtest.admin_dsn(pgtest.DATABASE))
    wrong = u._replace(scheme="pgqueue", netloc=f"{u.username}:not-the-password@{u.hostname}:{u.port}",
                       query="schema=be_bus_e2e&sslmode=disable").geturl()
    out = io.StringIO()
    got = await launcher.run(d, launcher.Options(env=EnvSource(lookup_env={**env, "BUS_URL": wrong}.get), stdout=out,
                                                 stderr=out, host="127.0.0.1"), asyncio.Event())
    assert got == 78, out.getvalue()
    assert "the bus refused the connection settings" in out.getvalue()
    assert "not-the-password" not in out.getvalue(), "a secret never reaches the logs"
