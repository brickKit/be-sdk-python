"""The system surface: one grpc.aio server per member, the SDK's chain around every method (crash recovery, request
ID, trace, deadline, caller check, error mapping to google.rpc.Status, access log and metrics), and service
registration in which every method declares its operation and its callers, like the user surface's routes."""
from __future__ import annotations

import asyncio
import re
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import grpc
from grpc_status import rpc_status
from opentelemetry import trace
from prometheus_client import CollectorRegistry, Histogram

from besdk import errs, errwire, reqctx
from besdk.calls.client import METADATA_REQUEST_ID
from besdk.obs import PROPAGATOR, Logger

DEFAULT_DEADLINE, ORCHESTRATION_DEADLINE = 10.0, 15.0
_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")


@dataclass
class Method:
    """One method of a registered service."""

    operation: str  # required: the system surface has no public methods
    callers: list[str] = field(default_factory=list)  # the components allowed to call it (P8.6)
    orchestration: bool = False
    timeout: float = 0.0

    @property
    def deadline(self) -> float:
        return self.timeout or (ORCHESTRATION_DEADLINE if self.orchestration else DEFAULT_DEADLINE)


class _Recorder:
    """Stands in for the server when a generated add_<Service>Servicer_to_server registers its handlers."""

    def __init__(self) -> None:
        self.services: dict[str, dict[str, grpc.RpcMethodHandler]] = {}

    def add_registered_method_handlers(self, service: str, handlers: dict[str, grpc.RpcMethodHandler]) -> None:
        self.services[service] = dict(handlers)

    def add_generic_rpc_handlers(self, handlers: Any) -> None:
        pass  # the registered handlers above carry the same methods


class Services:
    """A module's gRPC services."""

    def __init__(self) -> None:
        self._handlers: dict[str, grpc.RpcMethodHandler] = {}  # full method -> handler
        self._declared: dict[str, Method] = {}
        self._services: dict[str, list[str]] = {}

    def register(self, add_to_server: Callable[[Any, Any], None], servicer: Any, methods: dict[str, Method]) -> None:
        """Register generated code's service: add_to_server is add_<Service>Servicer_to_server; methods maps each
        method name (e.g. "Hold") to its declaration."""
        rec = _Recorder()
        add_to_server(servicer, rec)
        for name, handlers in rec.services.items():
            self.add(name, handlers, methods)

    def add(self, service: str, handlers: dict[str, grpc.RpcMethodHandler], methods: dict[str, Method]) -> None:
        """Register a service's unary handlers by method name (what register does for generated code)."""
        self._services[service] = list(handlers)
        for name, h in handlers.items():
            self._handlers[f"/{service}/{name}"] = h
        for name, m in methods.items():
            self._declared[f"/{service}/{name}"] = m

    @property
    def empty(self) -> bool:
        return not self._handlers

    def operations(self) -> list[str]:
        return [m.operation for m in self._declared.values()]

    def validate(self) -> None:
        """Every method declares an operation and its callers; no declaration without a method; unary only."""
        problems = []
        for full, h in self._handlers.items():
            d = self._declared.get(full)
            if d is None or not d.operation:
                problems.append(f"{full}: no operation declared")
            elif not d.callers:
                problems.append(f"{full}: no callers declared")
            if h.request_streaming or h.response_streaming:
                problems.append(f"{full}: streams are not supported")
        problems += [f"{full}: declared but not a method of a registered service" for full in self._declared
                     if full not in self._handlers]
        if problems:
            raise ValueError("system surface: " + "; ".join(problems))


@dataclass
class Deps:
    logger: Logger
    registry: CollectorRegistry
    tracer: trace.Tracer
    # the caller check (P8.5, P8.6): returns the calling component or raises; None until a service identity exists
    authorize: Callable[[grpc.aio.ServicerContext, Method], str] | None = None


def build_server(d: Deps, s: Services) -> grpc.aio.Server:
    """The member's system-surface server with every method wrapped in the chain. Listening is the caller's."""
    s.validate()
    duration = Histogram("be_grpc_server_duration_seconds", "System-surface call duration by method and code.",
                         ["method", "code"], registry=d.registry)
    handlers = {}
    for full, h in s._handlers.items():
        handlers[full] = grpc.unary_unary_rpc_method_handler(_wrap(d, full, s._declared[full], h.unary_unary, duration),
                                                              request_deserializer=h.request_deserializer,
                                                              response_serializer=h.response_serializer)
    server = grpc.aio.server()
    for service, names in s._services.items():
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            service, {n: handlers[f"/{service}/{n}"] for n in names}),))
    return server


def _wrap(d: Deps, full: str, decl: Method, behavior: Callable[[Any, Any], Awaitable[Any]], duration: Histogram):
    async def call(request: Any, context: grpc.aio.ServicerContext) -> Any:
        start = time.monotonic()
        md = {k: v for k, v in (context.invocation_metadata() or ())}
        rid = md.get(METADATA_REQUEST_ID, "")
        if not _REQUEST_ID.fullmatch(rid):
            rid = str(uuid.uuid7())
        await context.send_initial_metadata(((METADATA_REQUEST_ID, rid),))
        parent = PROPAGATOR.extract(carrier=md)
        code = "OK"
        with (reqctx.request_scope(rid), d.tracer.start_as_current_span(full, context=parent,
                                                                         kind=trace.SpanKind.SERVER) as span):
            log = d.logger.with_(request_id=rid, trace_id=format(span.get_span_context().trace_id, "032x"))
            # the effective deadline is the shorter of the method's and the caller's (P4.3)
            limit = decl.deadline
            if (left := context.time_remaining()) is not None:
                limit = min(limit, left)
            failure: errs.Error | None = None
            try:
                with reqctx.deadline_scope(time.monotonic() + limit):
                    async with asyncio.timeout(limit):
                        if d.authorize is not None:
                            d.authorize(context, decl)
                        return await behavior(request, context)
            except BaseException as exc:  # noqa: BLE001 - every failure becomes the error object, never a crash
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                failure = errs.from_exception(exc)
                if not isinstance(exc, errs.Error) and failure.internal:
                    log.error("unhandled exception while serving a call", method=full, error=repr(exc),
                              stack="".join(traceback.format_exception(exc)))
                elif (level := errs.log_level(failure.code)) is not None:
                    log.log(level, "call failed", method=full, reason=failure.reason, domain=failure.domain,
                            error=str(failure))
                code = failure.code.value
            finally:
                elapsed = time.monotonic() - start
                duration.labels(full, code).observe(elapsed)
                log.log(errs.access_log_level(errs.http_status(code) if code != "OK" else 200), "access", method=full,
                        code=code, duration_ms=int(elapsed * 1000))
        await context.abort_with_status(rpc_status.to_status(errwire.to_status(failure)))  # type: ignore[arg-type]

    return call
