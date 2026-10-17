"""Observability output (be-protocol chapter 10): log lines and the metrics' families, labels and bounds."""
import io
import json
import re

import httpx
from fastapi.responses import JSONResponse
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from besdk.calls.client import Member as Outbound
from besdk.http import Deps, Platform, Route, Routes, build_app
from besdk.obs import DURATION_BUCKETS, Member, new_logger, new_registry

TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z")


async def test_log_lines_and_access_lines():  # P10.1-P10.3
    out, registry = io.StringIO(), new_registry()

    async def hello(c):
        return JSONResponse({})

    routes = Routes()
    routes.add(Route("GET", "/api/things/{id}", hello, public=True))
    app = build_app(Deps(new_logger(Member("demo/a", "3.0.0"), "info", out), registry, TracerProvider().get_tracer("t"),
                         Platform(lambda: (True, "ready", []), lambda: {})), routes)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        await c.get("/api/things/7", headers={"Authorization": "Bearer secret-token-xyz"})
        await c.get("/healthz")
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert lines, "an access line"
    for r in lines:
        assert TIME.fullmatch(r["time"]) and r["level"] in ("DEBUG", "INFO", "WARN", "ERROR")
        assert r["msg"] and r["component"] == "demo/a" and r["version"] == "3.0.0"
    access = [r for r in lines if r["msg"] == "access"]
    assert len(access) == 1 and access[0]["route"] == "/api/things/{id}", "the template, and no line for /healthz"
    assert {"method", "status", "duration_ms", "request_id"} <= set(access[0])
    assert re.fullmatch(r"[0-9a-f]{32}", access[0]["trace_id"])
    assert "secret-token-xyz" not in out.getvalue()  # P10.4

    families = {f.name: f for f in text_string_to_metric_families(generate_latest(registry).decode())}
    h = families["be_http_server_duration_seconds"]
    assert h.type == "histogram"
    bounds = sorted({float(s.labels["le"]) for s in h.samples if s.name.endswith("_bucket")})
    assert bounds == [*DURATION_BUCKETS, float("inf")]
    assert {k for s in h.samples for k in s.labels} == {"method", "route", "status", "le"}


def test_the_outbound_families():  # P10.6
    registry = new_registry()
    Outbound(registry).dependency("demo/b", "grpc")
    text = generate_latest(registry).decode()
    families = {f.name: f.type for f in text_string_to_metric_families(text)}
    assert families.get("be_outbound_attempts") == "counter" and families.get("be_outbound_breaker_open") == "gauge"
    assert families.get("be_outbound_rejected") == "counter"
