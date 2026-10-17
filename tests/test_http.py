import io
import json

import httpx
import pytest
from fastapi.responses import JSONResponse, Response
from opentelemetry.sdk.trace import TracerProvider

from besdk import errs, reqctx
from besdk.http import (DEFAULT_DEADLINE, HEADER_REQUEST_ID, HEADER_TIMEOUT_MS, ORCHESTRATION_DEADLINE, Deps, Platform,
                        Route, Routes, build_app)
from besdk.obs import Member, new_logger, new_registry


class AdmitAll:
    """Stands in for the guard: these tests are about the chain around it."""

    async def admit(self, route, request):
        return None


class Harness:
    def __init__(self, routes):
        self.logs, self.ready = io.StringIO(), False
        m = Member("demo/alpha", "3.0.0")
        d = Deps(new_logger(m, "debug", self.logs), new_registry(), TracerProvider().get_tracer("test"),
                 Platform(lambda: (self.ready, "starting", ["demo/beta"]), lambda: {"component_id": m.id}), AdmitAll())
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=build_app(d, routes)), base_url="http://t")

    async def do(self, method, path, body=None, headers=None):
        return await self.client.request(method, path, content=body, headers=headers or {})


def problem(resp):
    assert resp.headers["content-type"] == "application/problem+json"
    return resp.json()


async def nothing(c):
    return Response(status_code=204)


def test_route_validation():
    rs = Routes()
    rs.get("/api/a", "", nothing)
    rs.add(Route("GET", "/api/b", nothing, operation="x", public=True))
    rs.get("/healthz", "op", nothing)
    rs.get("/api/c", "op", nothing)
    rs.get("/api/c", "op", nothing)
    with pytest.raises(ValueError) as e:
        rs.validate()
    for want in ["/api/a: declares no operation", "/api/b: a public route", "/healthz belongs to the platform",
                 "/api/c: registered twice"]:
        assert want in str(e.value)
    ok = Routes()
    ok.get("/api/x", "demo.alpha.read", nothing)
    ok.add(Route("GET", "/api/open", nothing, public=True))
    ok.validate()


async def test_request_id_and_deadline():
    seen = {}

    async def x(c):
        seen["left"] = reqctx.remaining()
        return JSONResponse({"request_id": c.request_id, "trace_id": c.trace_id})

    async def long(c):
        seen["left"] = reqctx.remaining()
        return None

    rs = Routes()
    rs.get("/api/x", "op", x)
    rs.add(Route("GET", "/api/long", long, operation="op", orchestration=True))
    h = Harness(rs)
    r = await h.do("GET", "/api/x", headers={HEADER_REQUEST_ID: "abc-123"})
    assert r.status_code == 200 and r.headers[HEADER_REQUEST_ID] == "abc-123"
    assert abs(seen["left"] - DEFAULT_DEADLINE) < 0.5, "no header: the route's default"
    r = await h.do("GET", "/api/x", headers={HEADER_REQUEST_ID: "bad id with spaces", HEADER_TIMEOUT_MS: "1500"})
    assert r.headers[HEADER_REQUEST_ID] != "bad id with spaces" and len(r.headers[HEADER_REQUEST_ID]) == 36
    assert abs(seen["left"] - 1.5) < 0.3, "a shorter remaining time wins"
    await h.do("GET", "/api/x", headers={HEADER_TIMEOUT_MS: "60000"})
    assert abs(seen["left"] - DEFAULT_DEADLINE) < 0.5, "a deadline never grows"
    assert (await h.do("GET", "/api/long")).status_code == 204
    assert abs(seen["left"] - ORCHESTRATION_DEADLINE) < 0.5
    r = await h.do("GET", "/api/x", headers={HEADER_TIMEOUT_MS: "-5"})
    assert r.status_code == 400 and problem(r)["reason"] == "REQUEST_INVALID"


async def test_errors_crashes_bodies_and_unknown_routes():
    async def busy(c):
        raise errs.new("SERVICE_BUSY", "busy").with_retry_after(2)

    async def boom(c):
        raise RuntimeError("db password is hunter2")

    async def small(c):
        await c.json()
        return None

    rs = Routes()
    rs.get("/api/busy", "op", busy)
    rs.get("/api/boom", "op", boom)
    rs.add(Route("POST", "/api/small", small, operation="op", max_body=16))
    h = Harness(rs)
    r = await h.do("GET", "/api/busy")
    assert r.status_code == 503 and r.headers["retry-after"] == "2"
    p = problem(r)
    assert p["reason"] == "SERVICE_BUSY" and len(p["trace_id"]) == 32
    r = await h.do("GET", "/api/boom")
    assert r.status_code == 500 and "hunter2" not in r.text
    assert "unhandled exception while serving a request" in h.logs.getvalue()
    r = await h.do("POST", "/api/small", body=b'{"a":"0123456789abcdef"}')
    assert r.status_code == 413 and problem(r)["reason"] == "BODY_TOO_LARGE"
    r = await h.do("POST", "/api/small", body=b'{"a":')
    assert r.status_code == 400 and problem(r)["reason"] == "REQUEST_INVALID"
    r = await h.do("GET", "/api/nothing")
    assert r.status_code == 404 and problem(r)["reason"] == "NOT_FOUND"


async def test_a_handler_past_its_deadline_answers_deadline_exceeded():
    import asyncio

    async def slow(c):
        await asyncio.sleep(1)
        return None

    rs = Routes()
    rs.add(Route("GET", "/api/slow", slow, operation="op", timeout=0.05))
    r = await Harness(rs).do("GET", "/api/slow")
    assert r.status_code == 504 and problem(r)["reason"] == "DEADLINE_EXCEEDED"


async def test_platform_endpoints():
    rs = Routes()
    rs.get("/api/x", "op", nothing)
    h = Harness(rs)
    assert (await h.do("GET", "/healthz")).status_code == 200
    assert (await h.do("HEAD", "/healthz")).status_code == 200
    r = await h.do("GET", "/readyz")
    assert r.status_code == 503
    p = problem(r)
    assert p["reason"] == "NOT_READY" and p["metadata"] == {"state": "starting", "waiting": "demo/beta"}
    h.ready = True
    assert (await h.do("GET", "/readyz")).status_code == 200
    assert "demo/alpha" in (await h.do("GET", "/.well-known/be-component")).text
    before = len(h.logs.getvalue())
    await h.do("GET", "/healthz")
    assert len(h.logs.getvalue()) == before, "platform endpoints are not access-logged"
    await h.do("GET", "/api/x")
    metrics = (await h.do("GET", "/metrics")).text
    assert 'be_http_server_duration_seconds_count{method="GET",route="/api/x",status="204"} 1.0' in metrics
    assert any(json.loads(line)["msg"] == "access" for line in h.logs.getvalue().splitlines())
