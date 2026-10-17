"""Events between the two SDKs (P6.13, P6.16-P6.25, A111's language neutrality): a Go component (tests/interop/goevents,
built from be-sdk-go) and a Python component exchange events in both directions on JetStream and on the PostgreSQL
queue, a payload over 64 KiB included, and every event takes effect once."""
import asyncio
import io
import os
import pathlib
import signal
import socket
import subprocess
import uuid
from urllib.parse import urlsplit

import httpx
import pytest
from fastapi.responses import JSONResponse

from besdk import launcher
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.events import jetstream, pgqueue
from besdk.events.consumer import Sub
from besdk.events.core import MAX_PAYLOAD, Event, Projection, Stream
from besdk.http import Route
from tests import pgtest
from tests.test_transports import LOG_BUS

pytestmark = pytest.mark.integration
HELPER = pathlib.Path(__file__).parent / "interop" / "goevents"
THINGS = Stream("demo.things", 2)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def s3_env(prefix):
    url, access, secret = (os.environ.get(k, "") for k in ("BE_SDK_TEST_S3_URL", "BE_SDK_TEST_S3_ACCESS_KEY",
                                                           "BE_SDK_TEST_S3_SECRET_KEY"))
    assert url and access and secret, "BE_SDK_TEST_S3_* must name object storage"
    return {"OBJECT_STORE_URL": url, "OBJECT_STORE_BUCKET": "be-sdk-test", "OBJECT_STORE_PREFIX": prefix,
            "OBJECT_STORE_ACCESS_KEY": access, "OBJECT_STORE_SECRET_KEY": secret}


class PyProducer:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def create(c):
            rid = str(uuid.uuid7())
            data = {"id": rid, "from": "python"}
            if c.query("big"):
                data["blob"] = "p" * (MAX_PAYLOAD + 1000)
            await self.rt.db.tx(lambda tx: self.rt.events.emit(tx, THINGS.name, Event(
                "demo.things.created.v1", rid, 1, Projection.FULL, data)))
            return JSONResponse({"id": rid}, status_code=201)

        r.add(Route("POST", "/api/things", create, public=True))


class PyConsumer:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def seen(c):
            row = await self.rt.db.tx(lambda tx: tx.fetchrow(
                "SELECT count(*) AS count, count(*) FILTER (WHERE size > 65536) AS big FROM seen"))
            return JSONResponse(dict(row))

        r.add(Route("GET", "/api/seen", seen, public=True))

    def subscriptions(self, s):
        async def handle(tx, e):
            await tx.execute("INSERT INTO seen VALUES ($1, $2)", uuid.UUID(e.id), len(e.decode().get("blob", "")))

        s.add(Sub("things", [THINGS.name], handle, types=["demo.things.created.v1"]))


async def bus(kind):
    if kind == "jetstream":
        url = os.environ["BE_SDK_TEST_NATS_URL"]
        tr = await jetstream.Transport.connect(url, "interop-cleanup")
        try:
            await tr.delete_stream(THINGS.name)
        except Exception:  # noqa: BLE001 - not there yet
            pass
        await tr.close()
        return url
    schema = "py_bus_iop_" + uuid.uuid4().hex[:8]  # a fresh queue: no earlier messages
    a = await pgtest.admin()
    try:
        await a.execute(f"CREATE SCHEMA {schema}")
    finally:
        await a.close()
    u = urlsplit(pgtest.admin_dsn(pgtest.DATABASE))
    url = u._replace(scheme="pgqueue", query=f"schema={schema}&sslmode=disable").geturl()
    await pgqueue.migrate_bus(url, LOG_BUS)
    return url


async def member_env(name, extra):
    await pgtest.member(name)
    return {**pgtest.member_env(name), "COMPONENT_VERSION": "3.0.0", "STOP_DRAIN_DELAY": "50ms", **extra}


async def start_python(d, env):
    out = io.StringIO()
    assert await launcher.run(d, launcher.Options(args=["migrate"], env=EnvSource(lookup_env=env.get), stdout=out,
                                                  stderr=out), asyncio.Event()) == 0, out.getvalue()
    port, stop = free_port(), asyncio.Event()
    env = {**env, "COMPONENT_ID": d.id, "PORT": str(port)}
    task = asyncio.create_task(launcher.run(d, launcher.Options(env=EnvSource(lookup_env=env.get), stdout=out,
                                                                stderr=out, host="127.0.0.1"), stop))
    await ready(f"http://127.0.0.1:{port}", out)
    task.out = out  # type: ignore[attr-defined]
    return f"http://127.0.0.1:{port}", stop, task


async def start_go(binary, member_id, role, env):
    full = {**os.environ, **env, "MEMBER_ID": member_id, "ROLE": role, "COMPONENT_ID": member_id}
    p = await asyncio.to_thread(subprocess.run, [str(binary), "migrate"], env=full, capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr
    port = free_port()
    proc = subprocess.Popen([str(binary)], env={**full, "PORT": str(port)}, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    await ready(f"http://127.0.0.1:{port}", None, proc)
    return f"http://127.0.0.1:{port}", proc


async def ready(url, out=None, proc=None):
    async with httpx.AsyncClient() as c:
        for _ in range(1000):
            try:
                if (await c.get(url + "/readyz")).status_code == 200:
                    return
            except httpx.ConnectError:
                pass
            if proc is not None and proc.poll() is not None:
                raise AssertionError(proc.stdout.read())
            await asyncio.sleep(0.02)
    raise AssertionError("never ready: " + (out.getvalue() if out else ""))


async def stop_go(proc):
    proc.send_signal(signal.SIGTERM)
    await asyncio.to_thread(proc.wait, 15)
    assert proc.returncode == 0, proc.stdout.read()


async def counted(url, want):
    async with httpx.AsyncClient() as c:
        for _ in range(300):
            got = (await c.get(url + "/api/seen")).json()
            if got == want:
                return
            await asyncio.sleep(0.05)
    raise AssertionError(f"{got} != {want}")


@pytest.fixture(scope="module")
def go_binary(tmp_path_factory):
    out = tmp_path_factory.mktemp("goevents") / "goevents"
    subprocess.run(["go", "build", "-o", str(out), "."], cwd=HELPER, check=True, timeout=600)
    return out


@pytest.mark.parametrize("kind", ["jetstream", "pgqueue"])
async def test_python_to_go(kind, go_binary):
    url = await bus(kind)
    sfx = uuid.uuid4().hex[:6]
    prod_env = await member_env("py_iop_pp", {"BUS_URL": url, "IMPLEMENTATION_INSTANCE": str(uuid.uuid4()),
                                              **s3_env("py-iop-pp/")})
    cons_env = await member_env("py_iop_gc", {"BUS_URL": url})
    gurl, gproc = await start_go(go_binary, "demo/gocons" + sfx, "consumer", cons_env)
    purl, pstop, ptask = await start_python(Definition("demo/pyprod" + sfx, "3.0.0", PyProducer,
                                                       migrations={"0001_m.up.sql": "CREATE TABLE marker (id int);"},
                                                       streams=[THINGS]), prod_env)
    try:
        async with httpx.AsyncClient() as c:
            for i in range(5):
                assert (await c.post(purl + "/api/things")).status_code == 201
            assert (await c.post(purl + "/api/things?big=1")).status_code == 201
        await counted(gurl, {"count": 6, "big": 1})
    finally:
        pstop.set()
        assert await ptask == 0, ptask.out.getvalue()
        await stop_go(gproc)


@pytest.mark.parametrize("kind", ["jetstream", "pgqueue"])
async def test_go_to_python(kind, go_binary):
    url = await bus(kind)
    sfx = uuid.uuid4().hex[:6]
    prod_env = await member_env("py_iop_gp", {"BUS_URL": url, "IMPLEMENTATION_INSTANCE": str(uuid.uuid4()),
                                              **s3_env("py-iop-gp/")})
    cons_env = await member_env("py_iop_pc", {"BUS_URL": url})
    curl, cstop, ctask = await start_python(Definition(
        "demo/pycons" + sfx, "3.0.0", PyConsumer, subscribes=True,
        migrations={"0001_seen.up.sql": "CREATE TABLE seen (event_id uuid PRIMARY KEY, size int NOT NULL);"}), cons_env)
    gurl, gproc = await start_go(go_binary, "demo/goprod" + sfx, "producer", prod_env)
    try:
        async with httpx.AsyncClient() as c:
            for i in range(5):
                assert (await c.post(gurl + "/api/things")).status_code == 201
            assert (await c.post(gurl + "/api/things?big=1")).status_code == 201
        await counted(curl, {"count": 6, "big": 1})
    finally:
        await stop_go(gproc)
        cstop.set()
        assert await ctask == 0, ctask.out.getvalue()
