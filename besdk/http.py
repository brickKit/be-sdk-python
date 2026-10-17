"""The user surface: one FastAPI application per member, the SDK's middleware chain written once (request ID, trace,
deadline, body limit, error mapping, crash recovery, access log and metrics), and route registration in which every
route declares its operation (protocol chapters 01, 03 and 04)."""
from __future__ import annotations

import asyncio
import re
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from opentelemetry import trace
from prometheus_client import CollectorRegistry, Histogram, generate_latest
from prometheus_client.exposition import CONTENT_TYPE_LATEST
from starlette.exceptions import HTTPException as StarletteHTTPException

from besdk import errs, reqctx
from besdk.obs import PROPAGATOR, Logger

HEADER_REQUEST_ID = "X-Request-Id"
HEADER_TIMEOUT_MS = "Be-Timeout-Ms"
PROBLEM_CONTENT_TYPE = "application/problem+json"

# Default deadlines and limits (protocol 17's starting values, P4.3).
DEFAULT_DEADLINE = 10.0
ORCHESTRATION_DEADLINE = 15.0
DEFAULT_MAX_BODY = 1 << 20

_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")
_RESERVED = ("/healthz", "/readyz", "/metrics", "/.well-known/")

Handler = Callable[["Context"], Awaitable[Response]]


@dataclass
class Route:
    method: str
    path: str  # FastAPI syntax ({id}), under the component's own prefix
    handle: Handler
    # The operation this route performs, as the component's resource definition declares it. Required unless public:
    # a route without an operation would have no authorization check at all.
    operation: str = ""
    public: bool = False  # deliberately open: no operation, no authorization; it has to be said explicitly
    orchestration: bool = False  # the longer default deadline (P4.3)
    timeout: float = 0.0  # overrides the default deadline, seconds
    max_body: int = 0  # default 1 MiB

    @property
    def deadline(self) -> float:
        return self.timeout or (ORCHESTRATION_DEADLINE if self.orchestration else DEFAULT_DEADLINE)


class Routes:
    """A module's routes."""

    def __init__(self) -> None:
        self._list: list[Route] = []

    def add(self, r: Route) -> None:
        self._list.append(r)

    def get(self, path: str, operation: str, h: Handler) -> None:
        self.add(Route("GET", path, h, operation))

    def post(self, path: str, operation: str, h: Handler) -> None:
        self.add(Route("POST", path, h, operation))

    def list(self) -> list[Route]:
        return list(self._list)

    def validate(self) -> None:
        """Every route before anything listens: an operation unless public, no clash with the platform's endpoints,
        no duplicates. All problems are reported together."""
        problems, seen = [], set()
        for r in self._list:
            rid = f"{r.method} {r.path}"
            if not r.method or not r.path.startswith("/"):
                problems.append(f"{rid}: a method and an absolute path are required")
            elif not r.operation and not r.public:
                problems.append(f"{rid}: declares no operation; declare it, or mark the route public explicitly")
            elif r.operation and r.public:
                problems.append(f"{rid}: a public route has no operation")
            elif rid in seen:
                problems.append(f"{rid}: registered twice")
            for p in _RESERVED:
                if r.path == p.rstrip("/") or r.path.startswith(p):
                    problems.append(f"{rid}: {p} belongs to the platform")
            seen.add(rid)
        if problems:
            raise ValueError("routes: " + "; ".join(problems))


class Context:
    """What a handler gets: the request, its request and trace IDs, its operation and the member's logger tagged with
    both. The deadline and the trace are in the task's context; pass nothing, call through the runtime."""

    def __init__(self, request: Request, route: Route, request_id: str, trace_id: str, logger: Logger):
        self.request, self.route, self.request_id, self.trace_id, self.logger = request, route, request_id, trace_id, logger
        self._body: bytes | None = None

    @property
    def operation(self) -> str:
        return self.route.operation

    def param(self, name: str) -> str:
        return self.request.path_params.get(name, "")

    def query(self, name: str) -> str:
        return self.request.query_params.get(name, "")

    def header(self, name: str) -> str:
        return self.request.headers.get(name, "")

    async def body(self) -> bytes:
        """The body's bytes, read once, up to the route's limit (over it: BODY_TOO_LARGE)."""
        if self._body is None:
            limit = self.route.max_body or DEFAULT_MAX_BODY
            declared = self.request.headers.get("content-length", "")
            if declared.isdigit() and int(declared) > limit:
                raise errs.new("BODY_TOO_LARGE", "the request body is too large")
            chunks, size = [], 0
            async for chunk in self.request.stream():
                size += len(chunk)
                if size > limit:
                    raise errs.new("BODY_TOO_LARGE", "the request body is too large")
                chunks.append(chunk)
            self._body = b"".join(chunks)
        return self._body

    async def json(self) -> Any:
        """The body decoded as JSON; anything else is REQUEST_INVALID. Unknown fields are the handler's to ignore."""
        import json

        body = await self.body()
        if not body.strip():
            raise errs.new("REQUEST_INVALID", "the request body is empty")
        try:
            return json.loads(body)
        except ValueError:
            raise errs.new("REQUEST_INVALID", "the request body is not valid JSON") from None


class Guard(Protocol):
    """Admits a request to a non-public route (protocol chapter 08): verifies the caller's credential and binds the
    verified subject and the request's cutoff into the task's context."""

    async def admit(self, route: Route, request: Request) -> None: ...


@dataclass
class Platform:
    """The endpoints the SDK owns (protocol chapter 01)."""

    ready: Callable[[], tuple[bool, str, list[str]]]  # ready, state, members still waited for (a shell)
    describe: Callable[[], dict]
    metrics: Callable[[], bytes] | None = None  # None: the member's own registry


@dataclass
class Deps:
    logger: Logger
    registry: CollectorRegistry
    tracer: trace.Tracer
    platform: Platform
    guard: Guard | None = None


def problem_response(e: errs.Error, trace_id: str) -> JSONResponse:
    headers = {"Content-Type": PROBLEM_CONTENT_TYPE}
    if ra := e.retry_after_header():
        headers["Retry-After"] = ra
    p = e.problem(trace_id)
    return JSONResponse(p, status_code=p["status"], headers=headers, media_type=PROBLEM_CONTENT_TYPE)


def build_app(d: Deps, routes: Routes) -> FastAPI:
    """The member's user-surface application. Routes are validated first."""
    routes.validate()
    if d.guard is None:
        for r in routes.list():
            if not r.public:
                raise ValueError(f"route {r.method} {r.path}: no guard to admit its callers")
    duration = Histogram("be_http_server_duration_seconds", "User-surface request duration by route and status.",
                         ["method", "route", "status"], registry=d.registry)
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)
    _platform(app, d)
    for r in routes.list():
        app.add_api_route(r.path, _endpoint(d, r, duration), methods=[r.method], include_in_schema=False)

    @app.exception_handler(StarletteHTTPException)
    async def _no_route(request: Request, exc: StarletteHTTPException) -> Response:
        return problem_response(errs.new("NOT_FOUND", "no such route"), "")

    return app


def _platform(app: FastAPI, d: Deps) -> None:
    async def health(request: Request) -> Response:
        return PlainTextResponse("ok")

    async def ready(request: Request) -> Response:
        ok, state, waiting = d.platform.ready()
        if ok:
            return PlainTextResponse("ok")
        e = errs.new("NOT_READY", "not ready").with_("state", state)
        if waiting:
            e = e.with_("waiting", ",".join(waiting))
        return problem_response(e, "")

    async def describe(request: Request) -> Response:
        return JSONResponse(d.platform.describe())

    async def metrics(request: Request) -> Response:
        body = d.platform.metrics() if d.platform.metrics else generate_latest(d.registry)
        return Response(body, media_type=CONTENT_TYPE_LATEST)

    app.add_api_route("/healthz", health, methods=["GET", "HEAD"], include_in_schema=False)
    app.add_api_route("/readyz", ready, methods=["GET"], include_in_schema=False)
    app.add_api_route("/.well-known/be-component", describe, methods=["GET"], include_in_schema=False)
    app.add_api_route("/metrics", metrics, methods=["GET"], include_in_schema=False)


def _endpoint(d: Deps, r: Route, duration: Histogram) -> Callable[[Request], Awaitable[Response]]:
    async def endpoint(request: Request) -> Response:
        return await _serve(request, d, r, duration)

    return endpoint


async def _serve(request: Request, d: Deps, r: Route, duration: Histogram) -> Response:
    """One request through the chain: request ID, trace, deadline, guard, handler, error mapping, access log, metrics."""
    start = time.monotonic()
    rid = request.headers.get(HEADER_REQUEST_ID, "")
    if not _REQUEST_ID.fullmatch(rid):
        rid = str(uuid.uuid7())
    parent = PROPAGATOR.extract(carrier=dict(request.headers))
    with (reqctx.request_scope(rid, request.headers.get("authorization", "")),
          d.tracer.start_as_current_span(f"{r.method} {r.path}", context=parent, kind=trace.SpanKind.SERVER) as span):
        trace_id = format(span.get_span_context().trace_id, "032x")
        log = d.logger.with_(request_id=rid, trace_id=trace_id)
        try:
            response = await _run(request, d, r, rid, trace_id, log)
        except BaseException as exc:  # noqa: BLE001 - every failure becomes the error object, never a crash
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            e = errs.from_exception(exc)
            if not isinstance(exc, errs.Error) and e.internal:
                log.error("unhandled exception while serving a request", error=repr(exc),
                          stack="".join(traceback.format_exception(exc)), path=request.url.path)
            elif (level := errs.log_level(e.code)) is not None:
                log.log(level, "request failed", reason=e.reason, domain=e.domain, error=str(e))
            response = problem_response(e, trace_id)
    response.headers[HEADER_REQUEST_ID] = rid
    elapsed = time.monotonic() - start
    duration.labels(r.method, r.path, str(response.status_code)).observe(elapsed)
    log.log(errs.access_log_level(response.status_code), "access", method=r.method, route=r.path,
            status=response.status_code, duration_ms=int(elapsed * 1000))
    return response


async def _run(request: Request, d: Deps, r: Route, rid: str, trace_id: str, log: Logger) -> Response:
    limit = r.deadline
    if h := request.headers.get(HEADER_TIMEOUT_MS, ""):
        if not h.isdigit() or int(h) <= 0:
            raise errs.new("REQUEST_INVALID", f"{HEADER_TIMEOUT_MS} must be a positive integer").with_violations(
                errs.Violation("header:" + HEADER_TIMEOUT_MS, "INVALID", "positive integer of milliseconds"))
        limit = min(limit, int(h) / 1000)
    with reqctx.deadline_scope(time.monotonic() + limit):
        return await _admitted(request, d, r, rid, trace_id, log, limit)


async def _admitted(request: Request, d: Deps, r: Route, rid: str, trace_id: str, log: Logger, limit: float) -> Response:
    async with asyncio.timeout(limit):
        if not r.public:
            await d.guard.admit(r, request)  # type: ignore[union-attr]
        result = await r.handle(Context(request, r, rid, trace_id, log))
    if result is None:
        return Response(status_code=204)
    return result
