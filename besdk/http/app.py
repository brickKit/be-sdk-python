"""The member's ASGI application: user-plane routes, the operations endpoints and the one middleware that
gives every request its request ID, server span, problem+json errors, RED metrics and access-log line
(be-protocol P1.3, P1.4, P3, P4.1, P18)."""

from __future__ import annotations

import json
import sys
import time
from importlib import metadata
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request
from opentelemetry import trace
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse, PlainTextResponse, Response

from besdk import context, errors
from besdk.auth.access import PermKey
from besdk.http.router import Router

if TYPE_CHECKING:
    from besdk.runtime import Module, Runtime

PROTOCOL = "1.0"
_OPS = ("/healthz", "/readyz", "/metrics", "/_be/info")


def problem_response(rt: "Runtime", err: errors.Error, path: str, req: dict) -> Response:
    p = errors.problem(err, path=path, request_id=req.get("request_id", ""), trace_id=req.get("trace_id", ""),
                       locale=rt.locale, catalog=rt.catalog, component_domain=rt.error_domain)
    if err.reason == "TOKEN_STALE" and err.domain == "be":
        p.headers["WWW-Authenticate"] = 'Bearer error="token_stale"'
    return Response(json.dumps(p.body, ensure_ascii=False), status_code=p.status, headers=p.headers,
                    media_type=p.content_type)


class BeMiddleware:
    """Pure ASGI: request ID, trace extraction and server span, error mapping, metrics, access log."""

    def __init__(self, app: Any, rt: "Runtime"):
        self.app, self.rt = app, rt

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        rt, start = self.rt, time.perf_counter()
        headers = {k.decode("latin-1"): v.decode("latin-1") for k, v in scope["headers"]}
        parent = rt.telemetry.propagator.extract(headers)
        method = scope["method"]
        with rt.tracer.start_as_current_span(method, context=parent, kind=trace.SpanKind.SERVER) as span:
            sc = span.get_span_context()
            trace_id = f"{sc.trace_id:032x}"
            req = scope["be"] = {"request_id": headers.get("x-request-id") or trace_id, "trace_id": trace_id}
            sent: dict = {"status": 0}

            async def send_wrapper(message: dict) -> None:
                if message["type"] == "http.response.start":
                    sent["status"] = message["status"]
                    message.setdefault("headers", [])
                    extra = [(k.lower().encode(), v.encode()) for k, v in (req.get("headers") or {}).items()]
                    message["headers"] = [*message["headers"], (b"x-request-id", req["request_id"].encode()), *extra]
                await send(message)

            err: errors.Error | None = None
            with context.scope(request_id=req["request_id"], member=rt.id, req=req):
                try:
                    await self.app(scope, receive, send_wrapper)
                except Exception as e:  # noqa: BLE001 - the one place every error is mapped
                    err = errors.to_error(e)
                    if not sent["status"]:
                        await problem_response(rt, err, scope["path"], req)(scope, receive, send_wrapper)
            route = scope.get("route")
            template = getattr(route, "path", None) or ("<ops>" if scope["path"] in _OPS else "<unmatched>")
            span.update_name(f"{method} {template}")
            span.set_attribute("http.route", template)
            span.set_attribute("http.response.status_code", sent["status"])
            self._observe(method, template, sent["status"], start, req, err or req.get("error"), scope["path"])

    def _observe(self, method: str, route: str, status: int, start: float, req: dict, err: Any, path: str) -> None:
        rt, dur = self.rt, time.perf_counter() - start
        rt.metrics.http_server_requests.labels(method=method, route=route, status_code=str(status)).inc()
        rt.metrics.http_server_duration.labels(method=method, route=route).observe(dur)
        fields: dict[str, Any] = {"http.request.method": method, "http.route": route,
                                  "http.response.status_code": status, "duration_ms": round(dur * 1000, 3)}
        for k in ("sub", "perm"):
            if req.get(k):
                fields[k] = req[k]
        level = "info"
        if isinstance(err, errors.Error):
            level = errors.log_level(err.code)
            fields["error.code"], fields["error.reason"] = err.code.name, err.reason or ""
            fields["error"] = err.internal_message or str(err)
        if path in _OPS and status < 500:
            level = "debug"
        if level != "none":
            getattr(rt.logger, {"warn": "warning"}.get(level, level))("http_request", extra=fields)


def _info(rt: "Runtime", module: "Module") -> dict:
    v = sys.version_info
    ports = {"http": rt.port, **rt.extra_ports}
    return {
        "component_id": rt.id, "component_version": rt.version, "protocol": PROTOCOL,
        "sdk": {"name": "be-sdk-python", "version": _sdk_version()},
        "language": {"name": "python", "version": f"{v.major}.{v.minor}.{v.micro}"},
        "profiles": rt.profiles(module), "ports": ports,
        "migrations": rt.migration_info(), "capabilities": ["job_run"], "tzdata": _tzdata(), "members": None,
    }


def _sdk_version() -> str:
    try:
        return metadata.version("besdk")
    except metadata.PackageNotFoundError:
        return "0.6.0"


def _tzdata() -> str:
    try:
        import tzdata

        return tzdata.IANA_VERSION
    except (ImportError, AttributeError):
        return ""


def ops_key(component_id: str) -> PermKey:
    """``<domain>.<name>.ops`` (P14.4), registered by the project's tooling."""
    return PermKey(component_id.replace("/", ".") + ".ops")


def _mount_ops(router: Router, rt: "Runtime") -> None:
    """``GET /{d}/{n}/_ops/jobs`` (P14.4): read-only state of every job, from this replica's view."""

    @router.get("/_ops/jobs", guard=ops_key(rt.id))
    async def ops_jobs() -> dict:
        out = []
        for name, p in sorted(rt.jobs.plan.items()):
            st = rt.jobs.status.get(name, {})
            out.append({"name": name, "kind": p.job.kind.value, "enabled": p.enabled,
                        "schedule": p.schedule.text if p.schedule else None, "interval_seconds": p.interval,
                        "last_success": st.get("last_success"), "last_error": st.get("last_error", "")})
        for kind in ("worker", "reconciler"):
            for name in sorted(getattr(rt.jobs, kind + "s")):
                st = rt.jobs.status.get(name, {})
                out.append({"name": name, "kind": "queue" if kind == "worker" else kind, "enabled": rt.jobs.enabled(name),
                            "last_success": st.get("last_success"), "last_error": st.get("last_error", "")})
        return {"jobs": out}


def build_app(rt: "Runtime", module: "Module") -> FastAPI:
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
    app.state.rt = rt
    router = Router(rt.id)
    if module.http:
        module.http(router)
    if rt.jobs is not None and rt.shared.verifier is not None:
        _mount_ops(router, rt)
    rt.protected_routes = router.protected
    app.include_router(router.api)

    @app.api_route("/healthz", methods=["GET", "HEAD"], include_in_schema=False)
    async def healthz() -> Response:
        return PlainTextResponse("ok")

    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> Response:
        rt.readiness.refresh(rt)
        waiting = rt.readiness.waiting()
        if not waiting:
            return PlainTextResponse("ready")
        err = errors.be_error("NOT_READY", {"waiting": ",".join(waiting)})
        return problem_response(rt, err, "/readyz", request.scope.get("be", {}))

    @app.get("/metrics", include_in_schema=False)
    async def metrics_() -> Response:
        src = rt.shared.bundle_source
        if src is not None and src.bundle is not None:
            rt.metrics.authz_bundle_age.set(src.age())
        return Response(generate_latest(rt.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/_be/info", include_in_schema=False)
    async def info() -> Response:
        return JSONResponse(_info(rt, module))

    @app.exception_handler(StarletteHTTPException)
    async def http_exc(request: Request, exc: StarletteHTTPException) -> Response:
        err = errors.be_error("NOT_FOUND") if exc.status_code in (404, 405) else errors.internal(exc)
        return problem_response(rt, err, request.url.path, request.scope.get("be", {}))

    app.add_middleware(BeMiddleware, rt=rt)
    return app
