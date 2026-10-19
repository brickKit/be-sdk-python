import asyncio
import io
import json
import re
import socket
import time

import httpx
from fastapi.responses import JSONResponse, Response

from besdk import launcher, protocol
from besdk.component import Definition
from besdk.config import Key, Type
from besdk.http import Route


class Demo:
    def __init__(self):
        self.started = self.stopped = False
        self.release = asyncio.Event()

    def routes(self, r):
        async def hello(c):
            return JSONResponse({"greeting": "hi"})

        async def slow(c):
            await self.release.wait()
            return Response(status_code=204)

        r.add(Route("GET", "/api/hello", hello, public=True))
        r.add(Route("GET", "/api/slow", slow, public=True))

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True


def definition(demo, new=None):
    return Definition("demo/alpha", "3.0.0", new or (lambda rt: demo), keys=[Key("GREETING", Type.STRING, default="hello")],
                      capabilities=["core"])


def env(values):
    return launcher.EnvSource(lookup_env=values.get, read=lambda p: open(p, "rb").read())


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def test_unknown_entry():
    err = io.StringIO()
    code = await launcher.run(definition(Demo()), launcher.Options(args=["dance"], env=env({}), stderr=err,
                                                                   stdout=io.StringIO()), asyncio.Event())
    assert code == launcher.EXIT_USAGE and "dance" in err.getvalue()
    structured_lines(err.getvalue())


TIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")


def structured_lines(out: str) -> None:
    """Every line is a structured log line (P10.2): RFC 3339 UTC time to the millisecond, level, msg, component and
    version."""
    for line in out.strip().splitlines():
        r = json.loads(line)
        assert TIME.fullmatch(r.get("time", "")), line
        for k in ("level", "msg", "component", "version"):
            assert r.get(k), f"{k} missing: {line}"


async def test_configuration_errors_all_at_once():
    err = io.StringIO()
    code = await launcher.run(definition(Demo()), launcher.Options(env=env(
        {"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "2.9.9", "LOG_LEVEL": "loud"}), stderr=err,
        stdout=io.StringIO()), asyncio.Event())
    assert code == launcher.EXIT_CONFIGURATION
    for want in ['"key":"PORT"', '"key":"LOG_LEVEL"', '"key":"COMPONENT_VERSION"']:
        assert want in err.getvalue()
    structured_lines(err.getvalue())


async def test_module_failures_do_not_crash():
    base = {"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "3.0.0", "PORT": str(free_port())}

    def bad_wiring(rt):
        raise RuntimeError("bad wiring")

    async def bad_async(rt):
        raise ValueError("boom")

    for new in (bad_wiring, bad_async):
        out = io.StringIO()
        code = await launcher.run(definition(Demo(), new), launcher.Options(env=env(base), stdout=out,
                                                                            stderr=io.StringIO()), asyncio.Event())
        assert code == launcher.EXIT_FAILURE
        assert "the module could not be built" in out.getvalue()


async def test_serve_ready_and_graceful_stop():
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    demo, stop, out = Demo(), asyncio.Event(), io.StringIO()
    run = asyncio.create_task(launcher.run(definition(demo), launcher.Options(env=env(
        {"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "3.0.0", "PORT": str(port), "STOP_DRAIN_DELAY": "400ms",
         "STOP_TIMEOUT": "5s"}), stdout=out, stderr=io.StringIO(), host="127.0.0.1"), stop))
    async with httpx.AsyncClient(base_url=base) as client:
        for _ in range(500):
            try:
                if (await client.get("/readyz")).status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("never ready: " + out.getvalue())
        assert demo.started
        r = await client.get("/api/hello")
        assert r.status_code == 200 and r.json() == {"greeting": "hi"}
        desc = (await client.get("/.well-known/be-component")).json()
        assert desc["component_id"] == "demo/alpha" and desc["protocol"] == protocol.capabilities()["protocol"]
        assert desc["sdk"].startswith("be-sdk-python/")
        slow = asyncio.create_task(client.get("/api/slow"))
        await asyncio.sleep(0.05)
        stop.set()  # the stop signal
        for _ in range(100):
            if (await client.get("/readyz")).status_code == 503:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("still ready while stopping")
        demo.release.set()
        assert (await slow).status_code == 204, "a request in flight finishes during the stop"
    assert await run == launcher.EXIT_OK
    assert demo.stopped
    events = [json.loads(line).get("event") for line in out.getvalue().splitlines()]
    assert {"ready", "stopping", "stopped"} <= set(events)
    async with httpx.AsyncClient(base_url=base) as client:
        try:
            await client.get("/healthz")
            raise AssertionError("still listening after the stop")
        except httpx.ConnectError:
            pass


async def test_responses_are_not_held_back_on_a_kept_alive_connection():
    # a response leaves in more than one write; with Nagle on, the second waits for the client's delayed ACK (about
    # 40 ms on Linux), so every request on a kept-alive connection takes 40 ms whatever the work (P1.18)
    port = free_port()
    stop = asyncio.Event()
    run = asyncio.create_task(launcher.run(definition(Demo()), launcher.Options(env=env(
        {"COMPONENT_ID": "demo/alpha", "COMPONENT_VERSION": "3.0.0", "PORT": str(port), "STOP_DRAIN_DELAY": "0s"}),
        stdout=io.StringIO(), stderr=io.StringIO(), host="127.0.0.1"), stop))
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
        for _ in range(500):
            try:
                if (await client.get("/readyz")).status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.01)
        took = []
        for _ in range(20):
            started = time.perf_counter()
            assert (await client.get("/api/hello")).status_code == 200
            took.append(time.perf_counter() - started)
    stop.set()
    await run
    assert sorted(took)[len(took) // 2] < 0.015, f"median {sorted(took)[len(took) // 2] * 1000:.1f} ms"


async def test_serves_the_system_surface_and_stops_it_gracefully(world):
    import grpc
    from google.protobuf import wrappers_pb2

    from besdk import reqctx
    from besdk.calls.client import Member, options
    from besdk.system import Method

    release = asyncio.Event()

    class System(Demo):
        def services(self, s):
            async def say(request, context):
                if request.value == "slow":
                    await release.wait()
                return wrappers_pb2.StringValue(value="echo " + request.value)

            s.add("demo.Echo", {"Say": grpc.unary_unary_rpc_method_handler(
                say, request_deserializer=wrappers_pb2.StringValue.FromString,
                response_serializer=wrappers_pb2.StringValue.SerializeToString)},
                {"Say": Method("demo.system.say", ["demo/client"])})

    port, gport, stop = free_port(), free_port(), asyncio.Event()
    from besdk.testing import defs

    d = Definition("demo/system", "3.0.0", lambda rt: System(), grpc_port=gport,
                   authz=defs("demo/system", "demo.thing", "demo.thing.read", "demo.system.say"))
    run = asyncio.create_task(launcher.run(d, launcher.Options(env=env(
        {"COMPONENT_ID": "demo/system", "COMPONENT_VERSION": "3.0.0", "PORT": str(port), "STOP_DRAIN_DELAY": "200ms",
         **world.env("demo/system")}),
        stdout=io.StringIO(), stderr=io.StringIO(), host="127.0.0.1"), stop))
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
        for _ in range(500):
            try:
                if (await client.get("/readyz")).status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.01)
    out = Member()
    out.set_tls(world.tls("demo/client"))
    say = out.grpc("demo/system", f"127.0.0.1:{gport}").unary_unary(
        "/demo.Echo/Say", request_serializer=wrappers_pb2.StringValue.SerializeToString,
        response_deserializer=wrappers_pb2.StringValue.FromString)

    async def call(v):
        with reqctx.deadline_scope(time.monotonic() + 3):
            return (await say(wrappers_pb2.StringValue(value=v), metadata=options(idempotent=True))).value

    assert await call("hi") == "echo hi"
    slow = asyncio.create_task(call("slow"))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.sleep(0.1)
    release.set()
    assert await slow == "echo slow", "a call in flight finishes during the stop"
    assert await run == launcher.EXIT_OK
    await out.close()


def test_the_http_stack_uses_its_accelerators():
    # measured on the probes at 100/s: the server parses requests in C with httptools, and outbound HTTP on aiohttp
    # (its C parser) takes about 40% less CPU a call than httpx did. The server names httptools, so a missing wheel
    # fails at start instead of quietly falling back to the slower parser; aiohttp falls back silently, so this checks
    # that its extension is the one in use.
    from aiohttp import http_parser

    from besdk import host

    assert http_parser.HttpResponseParser is getattr(http_parser, "HttpResponseParserC", None)
    server = host.HTTPServer(object(), host.listen(0, "127.0.0.1"))
    try:
        assert server.server.config.http == "httptools"
    finally:
        server.sock.close()


def test_servers_keep_idle_connections_longer_than_clients():  # P1.20
    # a server that closes an idle keep-alive connection first races the client reusing it: the request fails with
    # EOF, and a POST is not retried (measured: 2 in 2000 at 200/s on a saturated server that closed at 5 s)
    from besdk import host
    from besdk.calls import client

    server = host.HTTPServer(object(), host.listen(0, "127.0.0.1"))
    try:
        assert server.server.config.timeout_keep_alive >= 120
        assert server.server.config.timeout_keep_alive > client.CLIENT_IDLE_TIMEOUT
    finally:
        server.sock.close()
