"""The member's gRPC server (be-protocol P7.3–P7.5, P7.10, P4.2, P18): one interceptor applies, in order,
panic recovery → identity → deadline floor → batch limit → error-detail normalisation → RED metrics →
tracing, for unary calls (P7.11: no streaming rpcs)."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any, Callable

import grpc
from opentelemetry import trace

from besdk import context, errors
from besdk.rpc import System
from besdk.rpc import limits as L
from besdk.rpc import status as S

if TYPE_CHECKING:
    from besdk.runtime import Runtime

DEADLINE_FLOOR = 10.0
MAX_RECV = 4 * 1024 * 1024


def server_options(max_age: float) -> list[tuple[str, int]]:
    """P7.5: 4 MiB receive, MaxConnectionAge + 30 s grace, keepalive enforcement MinTime 20 s."""
    return [("grpc.max_receive_message_length", MAX_RECV), ("grpc.max_connection_age_ms", int(max_age * 1000)),
            ("grpc.max_connection_age_grace_ms", 30_000), ("grpc.http2.min_ping_interval_without_data_ms", 20_000),
            ("grpc.keepalive_permit_without_calls", 0), ("grpc.http2.max_ping_strikes", 2)]


class _Interceptor(grpc.aio.ServerInterceptor):
    def __init__(self, rt: "Runtime"):
        self.rt = rt

    async def intercept_service(self, continuation: Callable, details: Any) -> Any:
        handler = await continuation(details)
        if handler is None or handler.unary_unary is None:
            return handler
        behaviour = handler.unary_unary
        service, _, method = details.method.lstrip("/").partition("/")
        md = {k: v for k, v in details.invocation_metadata or () if isinstance(v, str)}

        async def wrapped(request: Any, ctx: grpc.aio.ServicerContext) -> Any:
            return await self._call(behaviour, request, ctx, service, method, md)

        return grpc.unary_unary_rpc_method_handler(wrapped, request_deserializer=handler.request_deserializer,
                                                   response_serializer=handler.response_serializer)

    async def _call(self, behaviour: Callable, request: Any, ctx: Any, service: str, method: str, md: dict) -> Any:
        rt, start = self.rt, time.perf_counter()
        parent = rt.telemetry.propagator.extract(md)
        code, err = errors.Code.OK, None
        with rt.tracer.start_as_current_span(f"{service}/{method}", context=parent, kind=trace.SpanKind.SERVER) as sp:
            trace_id = f"{sp.get_span_context().trace_id:032x}"
            try:
                return await self._admitted(behaviour, request, ctx, md, trace_id)
            except grpc.aio.AbortError:
                code = errors.Code(ctx.code().value[0]) if ctx.code() else errors.Code.UNKNOWN
                raise
            except Exception as e:  # noqa: BLE001 - recovery and normalisation (P7.4, P4.2)
                err = errors.to_error(e)
                shown = errors.visible(err, rt.error_domain)
                code = shown.code
                message = errors.problem(shown, path="", request_id="", trace_id=trace_id, locale=rt.locale,
                                         catalog=rt.catalog, component_domain=rt.error_domain).body["detail"]
                st = S.to_status(shown, component_domain=rt.error_domain, message=message)
                await ctx.abort(S.grpc_code(shown.code), message, trailing_metadata=S.trailing(st))
            finally:
                self._observe(service, method, code, start, md, err)

    async def _admitted(self, behaviour: Callable, request: Any, ctx: Any, md: dict, trace_id: str) -> Any:
        caller = md.get("be-caller", "")
        if not caller:
            raise errors.be_error("MISSING_CALLER")
        remaining = ctx.time_remaining()
        deadline = time.monotonic() + (remaining if remaining is not None else DEADLINE_FLOOR)
        act = _json(md.get("be-actor-act"))
        sysp = System(caller, md.get("be-actor-sub", ""), act)
        with context.scope(deadline=deadline, caller=caller, system=sysp, member=self.rt.id,
                           request_id=md.get("x-request-id") or trace_id):
            hit = L.check(request)
            if hit:
                field, mx, got = hit
                raise errors.be_error("BATCH_TOO_LARGE", {"field": field, "max": str(mx), "got": str(got)},
                                      violations=[errors.Violation(field, "BATCH_TOO_LARGE", f"at most {mx} items")])
            return await behaviour(request, ctx)

    def _observe(self, service: str, method: str, code: errors.Code, start: float, md: dict,
                 err: errors.Error | None) -> None:
        rt, dur = self.rt, time.perf_counter() - start
        rt.metrics.grpc_server_handled.labels(service=service, method=method, code=code.name).inc()
        rt.metrics.grpc_server_duration.labels(service=service, method=method).observe(dur)
        level = errors.log_level(code)
        fields: dict[str, Any] = {"rpc.service": service, "rpc.method": method, "rpc.grpc.status_code": int(code),
                                  "duration_ms": round(dur * 1000, 3), "caller": md.get("be-caller", "")}
        if err is not None:
            fields.update({"error.code": err.code.name, "error.reason": err.reason or "",
                           "error": err.internal_message or str(err)})
        if level != "none":
            getattr(rt.logger, {"warn": "warning"}.get(level, level))("grpc_request", extra=fields)


def _json(v: str | None) -> Any:
    try:
        return json.loads(v) if v else None
    except ValueError:
        return None


class GrpcServer:
    """One gRPC server per member on its own port (P19.2), listening on all interfaces."""

    def __init__(self, rt: "Runtime", register: Callable[[grpc.aio.Server], None], *, port: int | None = None):
        self.rt = rt
        max_age = rt.config.duration("GRPC_MAX_CONNECTION_AGE", 300.0) \
            if "GRPC_MAX_CONNECTION_AGE" in rt.config.declared() else 300.0
        self.server = grpc.aio.server(interceptors=[_Interceptor(rt)], options=server_options(max_age))
        register(self.server)
        want = rt.extra_ports.get("grpc", 0) if port is None else port
        self.port = self.server.add_insecure_port(f"[::]:{want}")

    async def start(self) -> None:
        await self.server.start()

    async def stop(self, grace: float | None) -> None:
        await self.server.stop(grace)
