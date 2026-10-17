"""A -> B -> C over real servers: only one layer retries (P4.18), errors pass through, IDs and deadlines travel."""
import asyncio
import io
import socket
import time

import grpc
import httpx
import pytest
from fastapi.responses import JSONResponse
from google.protobuf import wrappers_pb2
from opentelemetry.sdk.trace import TracerProvider

from besdk import errs, reqctx
from besdk.calls.client import Member, options
from besdk.http import Deps as HTTPDeps, Platform, Route, Routes, build_app
from besdk.host import HTTPServer, listen
from besdk.obs import Member as ObsMember, new_logger, new_registry
from besdk.system import Deps, Method, Services, build_server

SAY = "/demo.Echo/Say"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def services(behavior, callers=("demo/a", "demo/b")):
    s = Services()
    s.add("demo.Echo", {"Say": grpc.unary_unary_rpc_method_handler(
        behavior, request_deserializer=wrappers_pb2.StringValue.FromString,
        response_serializer=wrappers_pb2.StringValue.SerializeToString)},
        {"Say": Method("demo.echo.say", list(callers))})
    return s


async def serve(s, logs):
    server = build_server(Deps(new_logger(ObsMember("demo/x", "3.0.0"), "debug", logs), new_registry(),
                               TracerProvider().get_tracer("t")), s)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    return server, f"127.0.0.1:{port}"


def say(channel):
    return channel.unary_unary(SAY, request_serializer=wrappers_pb2.StringValue.SerializeToString,
                               response_deserializer=wrappers_pb2.StringValue.FromString)


async def test_grpc_chain_retries_at_one_layer_only():
    calls = {"b": 0, "c": 0}
    seen = {}

    async def c_say(request, context):
        calls["c"] += 1
        seen["c_request_id"] = reqctx.request_id()
        raise errs.new("SERVICE_BUSY", "c is busy")

    c_server, c_addr = await serve(services(c_say), io.StringIO())
    b_out = Member()

    async def b_say(request, context):
        calls["b"] += 1
        seen["b_remaining"] = reqctx.remaining()
        return await say(b_out.grpc("demo/c", c_addr))(request, metadata=options(idempotent=True))

    b_server, b_addr = await serve(services(b_say), io.StringIO())
    a_out = Member()
    try:
        with reqctx.request_scope("chain-1"), reqctx.deadline_scope(time.monotonic() + 5):
            with pytest.raises(errs.Error) as e:
                await say(a_out.grpc("demo/b", b_addr))(wrappers_pb2.StringValue(value="hi"), metadata=options(idempotent=True))
        assert calls == {"b": 1, "c": 3}, "C is retried by B; A does not retry B's relayed answer"
        assert e.value.reason == "DEPENDENCY_UNAVAILABLE" and e.value.metadata["dependency"] == "demo/c"
        assert seen["c_request_id"] == "chain-1", "the request ID travels"
        assert seen["b_remaining"] < 3.0, "B's deadline is A's attempt timeout, never longer"
        dep = a_out.dependency("demo/b", "grpc")
        assert dep.budget.tokens == 10000 and dep.breaker.state == "closed", "the relayed failure is not A's to count"
    finally:
        await asyncio.gather(a_out.close(), b_out.close(), c_server.stop(None), b_server.stop(None))


async def test_component_errors_pass_through_unchanged():
    out_of_stock = errs.define("erp/inventory", "INSUFFICIENT_STOCK", errs.Code.FAILED_PRECONDITION, 409, errs.RETRY_NO,
                               "Not enough stock")

    async def c_say(request, context):
        raise out_of_stock.new("only 3 left").with_("available", "3").with_violations(
            errs.Violation("/quantity", "TOO_LARGE", "at most 3"))

    server, addr = await serve(services(c_say), io.StringIO())
    out = Member()
    try:
        with reqctx.deadline_scope(time.monotonic() + 5), pytest.raises(errs.Error) as e:
            await say(out.grpc("erp/inventory", addr))(wrappers_pb2.StringValue(value="x"))
        # P3.3: reason, domain and metadata cross the surfaces unchanged; the HTTP status of a component reason is not
        # carried over gRPC, so it follows the code
        assert (e.value.domain, e.value.reason, e.value.code) == ("erp/inventory", "INSUFFICIENT_STOCK", "FAILED_PRECONDITION")
        assert e.value.metadata == {"available": "3"} and e.value.violations[0].field == "/quantity"
    finally:
        await asyncio.gather(out.close(), server.stop(None))


def test_undeclared_methods_are_refused():
    async def nothing(request, context):
        return request

    s = Services()
    s.add("demo.Echo", {"Say": grpc.unary_unary_rpc_method_handler(nothing)}, {})
    with pytest.raises(ValueError, match="no operation declared"):
        s.validate()
    s = services(nothing, callers=())
    with pytest.raises(ValueError, match="no callers declared"):
        s.validate()


async def test_http_chain_retries_at_one_layer_and_forwards_the_user():
    calls = {"b": 0, "c": 0}
    seen = {}

    async def c_get(c):
        calls["c"] += 1
        seen["auth"] = c.header("authorization")
        raise errs.new("SERVICE_BUSY", "busy")

    class Guard:
        async def admit(self, route, request):
            return None

    def app(routes):
        return build_app(HTTPDeps(new_logger(ObsMember("demo/x", "3.0.0"), "debug", io.StringIO()), new_registry(),
                                  TracerProvider().get_tracer("t"), Platform(lambda: (True, "ready", []), dict), Guard()),
                         routes)

    rc = Routes()
    rc.get("/api/c", "demo.c.read", c_get)
    c_srv = HTTPServer(app(rc), listen(0, "127.0.0.1"))
    await c_srv.start()
    b_out = Member()
    c_url = "http://127.0.0.1:%d" % c_srv.sock.getsockname()[1]

    async def b_get(c):
        calls["b"] += 1
        return JSONResponse(await b_out.http("demo/c", c_url).do("GET", "/api/c", forward_user=True))

    rb = Routes()
    rb.get("/api/b", "demo.b.read", b_get)
    b_srv = HTTPServer(app(rb), listen(0, "127.0.0.1"))
    await b_srv.start()
    a_out = Member()
    try:
        with reqctx.request_scope("http-1", "Bearer alice"), reqctx.deadline_scope(time.monotonic() + 5):
            with pytest.raises(errs.Error) as e:
                await a_out.http("demo/b", "http://127.0.0.1:%d" % b_srv.sock.getsockname()[1]).do(
                    "GET", "/api/b", forward_user=True)
        assert calls == {"b": 1, "c": 3}
        assert e.value.reason == "DEPENDENCY_UNAVAILABLE" and e.value.metadata["dependency"] == "demo/c"
        assert seen["auth"] == "Bearer alice", "the user's own credential reaches C"
    finally:
        await asyncio.gather(a_out.close(), b_out.close(), b_srv.shutdown(0), c_srv.shutdown(0))


async def test_http_connect_failure_is_retried_then_reported():
    out = Member()
    out.dependency("demo/gone", "http").sleep = lambda _: asyncio.sleep(0)
    try:
        with reqctx.deadline_scope(time.monotonic() + 5), pytest.raises(errs.Error) as e:
            await out.http("demo/gone", f"http://127.0.0.1:{free_port()}").do("GET", "/api/x")
        assert e.value.reason == "DEPENDENCY_UNAVAILABLE"
        assert out.dependency("demo/gone", "http").budget.tokens == 7000, "three connect failures counted"
    finally:
        await out.close()
