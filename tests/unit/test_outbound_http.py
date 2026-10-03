"""User-plane HTTP as the caller (P8.1, P8.2) and third-party HTTP (P8.3), plus the transaction guard (P8.4)."""
import io

import pytest
from prometheus_client import generate_latest
from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from besdk import Code, Error, Module, context, logs
from besdk.http.server import HttpServer, listen
from tests.unit._rt import build, env, manifest

SEEN: list[dict] = []


async def echo(request):
    SEEN.append(dict(request.headers))
    return JSONResponse({"ok": True})


async def refuse(request):
    body = {"type": "urn:be:conformance/peer:QUOTA_EXCEEDED", "status": 400, "code": "FAILED_PRECONDITION",
            "reason": "QUOTA_EXCEEDED", "domain": "conformance/peer", "metadata": {"available": "2"}}
    return Response(content=__import__("json").dumps(body), status_code=400, media_type="application/problem+json")


@pytest.fixture
async def peer_http():
    app = Starlette(routes=[Route("/conformance/peer/echo", echo), Route("/conformance/peer/refuse", refuse)])
    srv = HttpServer(app, listen(0), logs.member_logger("c/p", "1", stream=io.StringIO()), grace=1)
    await srv.start()
    SEEN.clear()
    yield srv.port
    await srv.stop()


async def _empty(rt):
    return Module()


@pytest.fixture
def rt(tmp_path, peer_http):
    doc = manifest(dependencies={"components": ["conformance/peer@1.0.0"]})
    e = env(CONFORMANCE_PEER_ENDPOINT=f"http://127.0.0.1:{peer_http}",
            CONFORMANCE_PEER_GRPC_ENDPOINT="http://127.0.0.1:1")
    r, _, _ = build(tmp_path, _empty, doc=doc, environ=e)
    return r


async def test_needs_a_user(rt):
    with pytest.raises(Error) as ei:
        await rt.user_http("conformance/peer").json("GET", "/conformance/peer/echo")
    assert ei.value.code == Code.UNAUTHENTICATED


async def test_forwards_the_callers_token_and_context(rt):
    with context.scope(token="tok-1", request_id="r-9", authz_revision="42"):
        with rt.tracer.start_as_current_span("x"):
            assert await rt.user_http("conformance/peer").json("GET", "/conformance/peer/echo") == {"ok": True}
    h = SEEN[-1]
    assert h["authorization"] == "Bearer tok-1" and h["x-request-id"] == "r-9" and h["x-authz-revision"] == "42"
    assert h["traceparent"].startswith("00-")
    assert ('be_http_client_requests_total{component="conformance/widget-py",method="GET",status_code="200",'
            'target="conformance/peer"} 1.0') in generate_latest(rt.registry).decode()


async def test_problem_is_restored(rt):
    with context.scope(token="tok-1"):
        with pytest.raises(Error) as ei:
            await rt.user_http("conformance/peer").json("GET", "/conformance/peer/refuse")
    e = ei.value
    assert (e.code, e.reason, e.domain, e.http) == (Code.FAILED_PRECONDITION, "QUOTA_EXCEEDED", "conformance/peer", 400)


async def test_transaction_guard(rt):
    with context.scope(token="tok-1", tx=object()):
        with pytest.raises(Error) as ei:
            await rt.user_http("conformance/peer").json("GET", "/conformance/peer/echo")
        assert ei.value.reason == "NETWORK_IN_TX"
        with pytest.raises(Error) as ei:
            await rt.external_http("dingtalk").get("http://127.0.0.1:1/")
        assert ei.value.reason == "NETWORK_IN_TX"


async def test_external_forwards_no_internal_header(rt, peer_http):
    with context.scope(token="tok-1", request_id="r-9"):
        r = await rt.external_http("dingtalk").get(f"http://127.0.0.1:{peer_http}/conformance/peer/echo")
    assert r.status_code == 200
    h = SEEN[-1]
    assert "authorization" not in h and "x-request-id" not in h and not any(k.startswith("be-") for k in h)
    assert 'target="dingtalk"' in generate_latest(rt.registry).decode()
