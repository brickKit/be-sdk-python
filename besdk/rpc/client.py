"""Outbound gRPC (be-protocol P7.2, P7.6–P7.9, P7.12, P8.4): cached channels per (member, dependency,
port), keepalive, a service config with retries for idempotent methods only and a retry budget, and one
client chain: transaction guard, default deadline, bulkhead, metadata, RED metrics, error restore."""

from __future__ import annotations

import json
import sys
import time
from typing import TYPE_CHECKING, Any

import grpc
from opentelemetry import trace

from besdk import context, errors
from besdk.rpc import status as S

if TYPE_CHECKING:
    from besdk.outbound import Bulkhead
    from besdk.runtime import Runtime

OUT_DEADLINE = 3.0
MIN_BUDGET = 0.05
RETRYABLE = (1, 2)  # MethodOptions.IdempotencyLevel NO_SIDE_EFFECTS, IDEMPOTENT


def retry_methods(package_prefix: str) -> list[dict[str, str]]:
    """Idempotent methods of the dependency's services, from the generated code already imported."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for mod in list(sys.modules.values()):
        fd = getattr(mod, "DESCRIPTOR", None)
        if not getattr(mod, "__name__", "").endswith("_pb2") or fd is None or not hasattr(fd, "services_by_name"):
            continue
        if not fd.package.startswith(package_prefix) or fd.name in seen:
            continue
        seen.add(fd.name)
        for svc in fd.services_by_name.values():
            for m in svc.methods:
                if m.GetOptions().idempotency_level in RETRYABLE:
                    out.append({"service": svc.full_name, "method": m.name})
    return out


def service_config(methods: list[dict[str, str]]) -> str:
    """The service config every runtime generates (P7.8): 3 attempts in total, budget 10 / 0.1."""
    cfg: dict[str, Any] = {"retryThrottling": {"maxTokens": 10, "tokenRatio": 0.1}}
    if methods:
        cfg["methodConfig"] = [{"name": methods, "retryPolicy": {
            "maxAttempts": 3, "initialBackoff": "0.05s", "maxBackoff": "0.5s", "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE"]}}]
    return json.dumps(cfg)


def channel_options(methods: list[dict[str, str]], member: str) -> list[tuple[str, Any]]:
    """P7.6 keepalive: ping after 30 s idle on an active call, 10 s timeout, none without calls."""
    return [("grpc.keepalive_time_ms", 30_000), ("grpc.keepalive_timeout_ms", 10_000),
            ("grpc.keepalive_permit_without_calls", 0), ("grpc.http2.max_pings_without_data", 0),
            ("grpc.enable_retries", 1), ("grpc.service_config", service_config(methods)),
            ("grpc.primary_user_agent", f"besdk-python {member}")]


class _UnaryCall:
    """One unary method of a dependency, called through the runtime's chain (P7.12): transaction guard,
    default deadline, bulkhead, metadata, client RED metrics; gRPC errors come back as ``besdk.Error``."""

    def __init__(self, inner: Any, ch: "BeChannel", method: str):
        self.inner, self.ch, self.method = inner, ch, method
        self.name = method.rsplit("/", 1)[-1]

    async def __call__(self, request: Any, *, timeout: float | None = None, metadata: Any = None, **kw: Any) -> Any:
        unit, ch = context.current(), self.ch
        if unit.tx is not None:
            raise errors.be_error("NETWORK_IN_TX", message=f"gRPC {self.method} inside a transaction")
        t = budget(timeout)
        if not ch.bulkhead.try_acquire():
            raise errors.be_error("OUTBOUND_LIMIT", {"target": ch.target})
        rt, start, code = ch.rt, time.perf_counter(), errors.Code.OK
        try:
            with rt.tracer.start_as_current_span(self.method.lstrip("/"), kind=trace.SpanKind.CLIENT):
                md = list(metadata or ()) + outbound_metadata(rt, unit)
                try:
                    return await self.inner(request, timeout=t, metadata=md, **kw)
                except grpc.aio.AioRpcError as e:
                    err = S.from_rpc_error(e)
                    code = err.code
                    raise err from None
        finally:
            ch.bulkhead.release()
            rt.metrics.grpc_client_handled.labels(target=ch.target, method=self.name, code=code.name).inc()
            rt.metrics.grpc_client_duration.labels(target=ch.target, method=self.name).observe(
                time.perf_counter() - start)


class BeChannel:
    """What ``rt.conn`` returns: a grpc.aio channel whose unary methods go through the runtime's chain.
    Generated stubs take it as they take any channel."""

    def __init__(self, rt: "Runtime", target: str, inner: grpc.aio.Channel, bulkhead: "Bulkhead"):
        self.rt, self.target, self.inner, self.bulkhead = rt, target, inner, bulkhead

    def unary_unary(self, method: str, *args: Any, **kw: Any) -> _UnaryCall:
        return _UnaryCall(self.inner.unary_unary(method, *args, **kw), self, method)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def close(self, grace: float | None = None) -> None:
        await self.inner.close(grace)


def budget(explicit: float | None = None) -> float:
    """``min(3 s, remaining − 50 ms)``; under 50 ms left the call is not sent (P7.7)."""
    remaining = context.remaining()
    t = OUT_DEADLINE if explicit is None else min(explicit, OUT_DEADLINE)
    if remaining is not None:
        if remaining < MIN_BUDGET:
            raise errors.be_error("DEADLINE_BUDGET_EXHAUSTED")
        t = min(t, remaining - MIN_BUDGET)
    return t


def outbound_metadata(rt: "Runtime", unit: context.Unit) -> list[tuple[str, str]]:
    """P7.2: W3C trace context, request ID, be-caller always, the actor read at call time."""
    carrier: dict[str, str] = {}
    rt.telemetry.propagator.inject(carrier)
    md = [(k, v) for k, v in carrier.items()]
    if unit.request_id:
        md.append(("x-request-id", unit.request_id))
    md.append(("be-caller", rt.id))
    sub = unit.sub or (unit.system.actor_sub if unit.system else "")
    act = unit.act if unit.sub else (unit.system.act if unit.system else None)
    if sub:
        md.append(("be-actor-sub", sub))
    if act:
        md.append(("be-actor-act", json.dumps(act, separators=(",", ":"))))
    return md


def open_channel(rt: "Runtime", dependency: str, target: str, bulkhead: "Bulkhead") -> BeChannel:
    prefix = dependency.replace("/", ".").replace("-", "_") + "."
    opts = channel_options(retry_methods(prefix), rt.id)
    return BeChannel(rt, dependency, grpc.aio.insecure_channel(target, options=opts), bulkhead)
