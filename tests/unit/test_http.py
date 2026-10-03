"""The HTTP surface (P1.3, P1.4, P3, P4.1, P18.2, P18.3, P20.4) through the ASGI app, no sockets."""
import asyncio

import httpx
import pytest

import besdk
from besdk import PUBLIC, AUTHENTICATED, Module, PermKey
from tests.unit._rt import build, lines

VIEW = PermKey("conformance.widget.view")
APPROVE = PermKey("conformance.widget.approve")


def routes(r: besdk.Router):
    @r.get("/widgets/{wid}", guard=VIEW)
    async def get_widget(wid: str):
        return {"id": wid, "sub": besdk.access().user.sub}

    @r.post("/widgets/{wid}/approve", guard=APPROVE)
    async def approve(wid: str):
        return {"ok": True}

    @r.get("/public", guard=PUBLIC)
    async def public():
        return {"ok": True}

    @r.get("/me", guard=AUTHENTICATED)
    async def me():
        return {"sub": besdk.access().user.sub}

    @r.get("/fail", guard=PUBLIC)
    async def fail():
        raise besdk.Error(besdk.Code.FAILED_PRECONDITION, "WIDGET_NOT_DRAFT", {"status": "APPROVED"})

    @r.get("/crash", guard=PUBLIC)
    async def crash():
        raise RuntimeError("SELECT secret FROM table failed")

    @r.get("/slow", guard=PUBLIC, timeout=0.2)
    async def slow():
        await asyncio.sleep(5)

    @r.post("/upload", guard=PUBLIC, body_limit=4 * 1024 * 1024)
    async def upload(body: dict):
        return {"n": len(body.get("data", ""))}

    @r.post("/echo", guard=PUBLIC)
    async def echo(body: dict):
        return body

    @r.post("/typed", guard=PUBLIC)
    async def typed(q: int):
        return {"q": q}


async def _create(rt):
    return Module(http=routes)


@pytest.fixture
async def setup(tmp_path):
    rt, fakes, log = build(tmp_path, _create)
    module = await rt.spec.create(rt)
    app = rt.http_app(module)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w")
    yield rt, fakes, log, client
    await client.aclose()


BASE = "/conformance/widget-py"


async def test_healthz_get_and_head(setup):
    _, _, _, c = setup
    assert (await c.get("/healthz")).status_code == 200
    assert (await c.head("/healthz")).status_code == 200


async def test_readyz_waits_then_latches(setup):
    rt, _, _, c = setup
    r = await c.get("/readyz")
    assert r.status_code == 503 and r.headers["content-type"] == "application/problem+json"
    assert r.json()["reason"] == "NOT_READY" and r.json()["metadata"]["waiting"] == "bundle"
    await rt.shared.bundle_source.fetch()
    rt.readiness.refresh(rt)
    assert (await c.get("/readyz")).status_code == 200
    rt.shared.bundle_source.bundle = None  # a later outage never turns it back
    rt.readiness.refresh(rt)
    assert (await c.get("/readyz")).status_code == 200


async def test_protected_route_before_bundle_is_503(setup):
    _, fakes, _, c = setup
    r = await c.get(f"{BASE}/widgets/w1", headers={"Authorization": "Bearer " + fakes.iam.token(roles=["rep"])})
    assert r.status_code == 503 and r.json()["reason"] == "AUTHZ_NOT_READY"


async def test_before_bundle_token_is_checked_first(setup):
    """401 before 503 (stage-B ruling): a missing or invalid token is refused before the bundle check."""
    _, _, _, c = setup
    for path in ("/widgets/w1", "/me"):
        r = await c.get(f"{BASE}{path}")
        assert r.status_code == 401 and r.json()["reason"] == "TOKEN_INVALID"
        r = await c.get(f"{BASE}{path}", headers={"Authorization": "Bearer not.a.jwt"})
        assert r.status_code == 401 and r.json()["reason"] == "TOKEN_INVALID"
    assert (await c.get(f"{BASE}/public")).status_code == 200


async def test_guards(setup):
    rt, fakes, log, c = setup
    await rt.shared.bundle_source.fetch()
    tok = fakes.iam.token(roles=["rep"])
    r = await c.get(f"{BASE}/widgets/w1", headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 200 and r.json() == {"id": "w1", "sub": "u_me"}
    r = await c.post(f"{BASE}/widgets/w1/approve", headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 403
    assert (r.json()["reason"], r.json()["metadata"]) == ("MISSING_PERMISSION", {"permission": APPROVE})
    r = await c.get(f"{BASE}/widgets/w1")
    assert r.status_code == 401 and r.json()["reason"] == "TOKEN_INVALID"
    assert (await c.get(f"{BASE}/public")).status_code == 200
    assert (await c.get(f"{BASE}/me", headers={"Authorization": f"Bearer {tok}"})).json() == {"sub": "u_me"}
    access = [x for x in lines(log) if x["msg"] == "http_request" and x["http.route"] == f"{BASE}/widgets/{{wid}}"]
    assert access[0]["sub"] == "u_me" and access[0]["perm"] == VIEW
    assert "Bearer" not in log.getvalue()


async def test_problem_body_for_component_and_internal_errors(setup):
    _, _, _, c = setup
    r = await c.get(f"{BASE}/fail", headers={"X-Request-Id": "r-1"})
    b = r.json()
    assert r.status_code == 400 and r.headers["content-type"] == "application/problem+json"
    assert (b["type"], b["domain"], b["reason"], b["code"]) == (
        "urn:be:conformance/widget-py:WIDGET_NOT_DRAFT", "conformance/widget-py", "WIDGET_NOT_DRAFT",
        "FAILED_PRECONDITION")
    assert b["instance"] == f"{BASE}/fail" and b["request_id"] == "r-1" and len(b["trace_id"]) == 32
    r = await c.get(f"{BASE}/crash")
    b = r.json()
    assert r.status_code == 500 and (b["reason"], b["domain"]) == ("INTERNAL", "be")
    assert "secret" not in r.text


async def test_request_id_echoed_or_trace_id(setup):
    _, _, _, c = setup
    assert (await c.get(f"{BASE}/public", headers={"X-Request-Id": "abc"})).headers["X-Request-Id"] == "abc"
    tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    r = await c.get(f"{BASE}/fail", headers={"traceparent": tp})
    assert r.headers["X-Request-Id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert r.json()["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"


async def test_deadline_answers_504(setup):
    _, _, _, c = setup
    r = await c.get(f"{BASE}/slow")
    assert r.status_code == 504
    assert (r.json()["code"], r.json()["reason"]) == ("DEADLINE_EXCEEDED", "DEADLINE_BUDGET_EXHAUSTED")


async def test_body_limits(setup):
    _, _, _, c = setup
    big = {"data": "x" * (1024 * 1024 + 10)}
    r = await c.post(f"{BASE}/echo", json=big)
    assert r.status_code == 413 and r.json()["reason"] == "BODY_TOO_LARGE"
    assert (await c.post(f"{BASE}/upload", json=big)).json() == {"n": 1024 * 1024 + 10}


async def test_validation_and_unknown_route(setup):
    _, _, _, c = setup
    r = await c.post(f"{BASE}/typed?q=abc")
    assert r.status_code == 400 and r.json()["code"] == "INVALID_ARGUMENT" and r.json()["violations"]
    assert (r.json()["reason"], r.json()["domain"]) == ("REQUEST_INVALID", "be")
    r = await c.get("/nope")
    assert r.status_code == 404 and (r.json()["reason"], r.json()["domain"]) == ("NOT_FOUND", "be")


async def test_metrics_use_route_template(setup):
    _, _, _, c = setup
    await c.get(f"{BASE}/fail")
    text = (await c.get("/metrics")).text
    assert ('be_http_server_requests_total{component="conformance/widget-py",method="GET",'
            'route="/conformance/widget-py/fail",status_code="400"} 1.0') in text


async def test_stale_token_header(setup):
    import time
    rt, fakes, _, c = setup
    fakes.bundle = {**fakes.bundle, "stale_since": {"u_me": int(time.time())}}
    await rt.shared.bundle_source.fetch()
    tok = fakes.iam.token(roles=["rep"], iat=int(time.time()) - 60)
    r = await c.get(f"{BASE}/widgets/w1", headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 401 and r.json()["reason"] == "TOKEN_STALE"
    assert r.headers["WWW-Authenticate"] == 'Bearer error="token_stale"'


async def test_authz_metrics(setup):
    rt, fakes, _, c = setup
    await rt.shared.bundle_source.fetch()
    tok = fakes.iam.token(roles=["rep"])
    await c.post(f"{BASE}/widgets/w1/approve", headers={"Authorization": f"Bearer {tok}"})
    await c.get(f"{BASE}/widgets/w1")
    text = (await c.get("/metrics")).text
    assert 'be_authz_denied_total{component="conformance/widget-py",reason="MISSING_PERMISSION"} 1.0' in text
    assert 'be_authz_denied_total{component="conformance/widget-py",reason="TOKEN_INVALID"} 1.0' in text
    assert 'be_authz_bundle_age_seconds{component="conformance/widget-py"}' in text


async def test_be_info(setup):
    _, _, _, c = setup
    info = (await c.get("/_be/info")).json()
    assert info["component_id"] == "conformance/widget-py" and info["protocol"] == "1.0"
    assert info["sdk"]["name"] == "be-sdk-python" and info["language"]["name"] == "python"
    assert {"core", "obs", "err", "auth", "grpc"} <= set(info["profiles"])
    assert info["members"] is None and info["capabilities"] == []  # job_run once P14.8 is implemented


def test_route_without_guard_is_a_programming_error():
    r = besdk.Router("conformance/widget-py")
    with pytest.raises(TypeError):
        r.get("/x")
