"""Calling others (be-protocol P7.6–P7.9, P8): gRPC channels, user-plane HTTP on behalf of the caller,
third-party HTTP. One bulkhead of 64 concurrent calls per (member, dependency), shared by gRPC and
user-plane HTTP; none of them starts inside an open transaction (P8.4)."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import httpx

from besdk import context, errors
from besdk.rpc.client import budget, open_channel

if TYPE_CHECKING:
    import grpc

    from besdk.runtime import Runtime

BULKHEAD = 64
FAMILIES = {"infra/authz": ("AUTHZ_URL", "AUTHZ_GRPC_URL"), "infra/iam": ("IAM_URL", "IAM_GRPC_URL")}
_FORWARD = ("x-request-id", "traceparent", "tracestate", "baggage")


class Bulkhead:
    """At most ``limit`` calls at once; the next one fails at once, never queues (P7.9)."""

    def __init__(self, rt: "Runtime", target: str, limit: int = BULKHEAD):
        self.limit, self.inflight = limit, 0
        self.gauge = rt.metrics.outbound_inflight.labels(target=target)

    def try_acquire(self) -> bool:
        if self.inflight >= self.limit:
            return False
        self.inflight += 1
        self.gauge.set(self.inflight)
        return True

    def release(self) -> None:
        self.inflight -= 1
        self.gauge.set(self.inflight)


def guard_tx(what: str) -> None:
    if context.current().tx is not None:
        raise errors.be_error("NETWORK_IN_TX", message=f"{what} inside a transaction")


class Outbound:
    def __init__(self, rt: "Runtime"):
        self.rt = rt
        self._channels: dict[tuple[str, str], Any] = {}
        self._bulkheads: dict[str, Bulkhead] = {}
        self._user: dict[str, UserHTTP] = {}
        self._external: dict[str, httpx.AsyncClient] = {}

    def bulkhead(self, dep: str) -> Bulkhead:
        if dep not in self._bulkheads:
            self._bulkheads[dep] = Bulkhead(self.rt, dep)
        return self._bulkheads[dep]

    def _address(self, dep: str, port: str) -> str | None:
        if dep in FAMILIES:
            return self.rt.config.family(FAMILIES[dep][1 if port == "grpc" else 0])
        return self.rt.config.endpoint(dep, port)

    def conn(self, dep: str, port: str = "grpc") -> "grpc.aio.Channel | None":
        """Created lazily, reused (P7.6); None when the optional dependency is not installed (P2.5)."""
        key = (dep, port)
        if key not in self._channels:
            target = self._address(dep, port)
            if target is None:
                return None
            self._channels[key] = open_channel(self.rt, dep, target.removeprefix("http://"), self.bulkhead(dep))
        return self._channels[key]

    def user_http(self, dep: str) -> "UserHTTP | None":
        if dep not in self._user:
            addr = self._address(dep, "")
            if addr is None:
                return None
            base = addr if addr.startswith("http://") else f"http://{addr}"
            self._user[dep] = UserHTTP(self.rt, dep, base, self.bulkhead(dep))
        return self._user[dep]

    def external_http(self, name: str, *, timeout: float = 10.0, max_conns: int = 32) -> httpx.AsyncClient:
        """A third-party client: own timeout, metrics, no internal header ever forwarded (P8.3)."""
        if name not in self._external:
            rt = self.rt

            async def on_request(req: httpx.Request) -> None:
                guard_tx(f"HTTP to {name}")
                req.extensions["be_start"] = time.perf_counter()

            async def on_response(resp: httpx.Response) -> None:
                start = resp.request.extensions.get("be_start", time.perf_counter())
                rt.metrics.http_client_requests.labels(target=name, method=resp.request.method,
                                                       status_code=str(resp.status_code)).inc()
                rt.metrics.http_client_duration.labels(target=name, method=resp.request.method).observe(
                    time.perf_counter() - start)

            self._external[name] = httpx.AsyncClient(
                timeout=timeout, limits=httpx.Limits(max_connections=max_conns),
                event_hooks={"request": [on_request], "response": [on_response]})
        return self._external[name]

    async def close(self) -> None:
        for ch in self._channels.values():
            await ch.close()
        for u in self._user.values():
            await u.client.aclose()
        for c in self._external.values():
            await c.aclose()
        self._channels.clear()


class UserHTTP:
    """REST to another component as the current user: the caller's token forwarded unchanged (P8.1, P8.2)."""

    def __init__(self, rt: "Runtime", dep: str, base: str, bulkhead: Bulkhead):
        self.rt, self.dep, self.bulkhead = rt, dep, bulkhead
        self.client = httpx.AsyncClient(base_url=base)

    def _headers(self) -> dict[str, str]:
        unit = context.current()
        if not unit.token:
            raise errors.Error(errors.Code.UNAUTHENTICATED, "TOKEN_INVALID", domain="be",
                               message="user-plane HTTP needs a user in the context")
        h = {"Authorization": f"Bearer {unit.token}", "X-Request-Id": unit.request_id}
        carrier: dict[str, str] = {}
        self.rt.telemetry.propagator.inject(carrier)
        h.update(carrier)
        if unit.authz_revision:
            h["X-Authz-Revision"] = unit.authz_revision
        return {k: v for k, v in h.items() if v}

    async def request(self, method: str, path: str, **kw: Any) -> httpx.Response:
        guard_tx(f"HTTP to {self.dep}")
        headers = {**self._headers(), **(kw.pop("headers", None) or {})}
        timeout = budget()
        if not self.bulkhead.try_acquire():
            raise errors.be_error("OUTBOUND_LIMIT", {"target": self.dep})
        start, status = time.perf_counter(), 0
        try:
            for attempt in (1, 2):
                try:
                    r = await self.client.request(method, path, headers=headers, timeout=timeout, **kw)
                    status = r.status_code
                    return r
                except (httpx.ReadError, httpx.RemoteProtocolError):
                    if method != "GET" or attempt == 2:  # P8.2: only GET, once, on a reset connection
                        raise
                except httpx.TimeoutException:
                    raise errors.be_error("DEADLINE_BUDGET_EXHAUSTED") from None
            raise AssertionError("unreachable")
        finally:
            self.bulkhead.release()
            self.rt.metrics.http_client_requests.labels(target=self.dep, method=method,
                                                        status_code=str(status)).inc()
            self.rt.metrics.http_client_duration.labels(target=self.dep, method=method).observe(
                time.perf_counter() - start)

    async def json(self, method: str, path: str, body: Any = None) -> Any:
        """JSON in, JSON out; a problem+json answer is raised as the same error (P8.2)."""
        r = await self.request(method, path, json=body) if body is not None else await self.request(method, path)
        if r.status_code >= 400:
            problem = r.json() if r.headers.get("content-type", "").startswith("application/problem+json") else None
            raise errors.restore_http(r.status_code, problem)
        return r.json() if r.content else None
