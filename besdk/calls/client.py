"""The outbound side of chapter 04: per (member, dependency) a concurrency limit, a circuit breaker and a retry budget;
per call a deadline that only shrinks, at most three attempts for idempotent methods when the dependency itself is
unavailable, and one answer upward (P4.19). The HTTP and gRPC clients both go through Dependency.do."""
from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Any, Awaitable, Callable

import grpc
import httpx
from prometheus_client import CollectorRegistry, Counter, Gauge

from besdk import errs, errwire, reqctx
from besdk.calls import policy
from besdk.calls.outcome import Class, Result, classify, upward
from besdk.db.pool import in_tx
from besdk.obs import PROPAGATOR

HEADER_REQUEST_ID, HEADER_TIMEOUT_MS, HEADER_IDEMPOTENCY_KEY = "X-Request-Id", "Be-Timeout-Ms", "Idempotency-Key"
METADATA_REQUEST_ID, METADATA_IDEMPOTENCY_KEY = "x-request-id", "idempotency-key"
MAX_RESPONSE = 16 << 20
# reserved metadata the client interceptor reads and strips: per-call flags for generated stubs
_IDEMPOTENT = "x-be-sdk-idempotent"
_STATES = {policy.CLOSED: 0, policy.HALF_OPEN: 1, policy.OPEN: 2}


def options(idempotent: bool = False, idempotency_key: str = "") -> list[tuple[str, str]]:
    """Metadata for a system-surface call: mark a method the contract declares free of side effects or idempotent
    (only such calls are retried, P4.6) and send a command's idempotency key, the same on every attempt (P4.2)."""
    md = [(_IDEMPOTENT, "1")] if idempotent else []
    return md + ([(METADATA_IDEMPOTENCY_KEY, idempotency_key)] if idempotency_key else [])


class _Metrics:
    def __init__(self, registry: CollectorRegistry | None):
        reg = registry or CollectorRegistry()
        self.attempts = Counter("be_outbound_attempts_total", "Outbound attempts by dependency, surface and class.",
                                ["dependency", "surface", "class"], registry=reg)
        self.rejected = Counter("be_outbound_rejected_total", "Outbound calls not sent, by reason.",
                                ["dependency", "surface", "reason"], registry=reg)
        self.breaker = Gauge("be_outbound_breaker_state", "Circuit breaker state: 0 closed, 1 half open, 2 open.",
                             ["dependency", "surface"], registry=reg)


class Dependency:
    """One (member, dependency, surface): its concurrency limit, breaker and retry budget."""

    def __init__(self, id: str, surface: str, metrics: _Metrics):
        self.id, self.surface, self._m = id, surface, metrics
        self.breaker, self.budget, self.in_flight = policy.Breaker(), policy.Budget(), 0
        self.now: Callable[[], float] = time.monotonic  # the breaker's clock (tests replace it)
        self.sleep: Callable[[float], Awaitable[None]] = asyncio.sleep  # the backoff (tests replace it)

    async def do(self, idempotent: bool, attempt: Callable[[float], Awaitable[Result]]) -> None:
        """One call: at most three attempts, each given its own shrinking timeout (seconds) to honour. Raises the
        caller's answer upward (P4.19)."""
        if in_tx():
            raise errs.new("INTERNAL").wrap(RuntimeError("NETWORK_IN_TX: an outbound call inside a database transaction"))
        if self.in_flight >= policy.OUTBOUND_LIMIT:
            self._m.rejected.labels(self.id, self.surface, "outbound_limit").inc()
            raise errs.new("OUTBOUND_LIMIT", "too many calls to a dependency are in progress").with_(
                "dependency", self.id).with_retry_after(1)
        self.in_flight += 1
        try:
            await self._attempts(idempotent, attempt)
        finally:
            self.in_flight -= 1

    async def _attempts(self, idempotent: bool, attempt: Callable[[float], Awaitable[Result]]) -> None:
        last, last_class, n = Result(), Class.OTHER, 0
        while True:
            n += 1
            timeout = policy.attempt_timeout(reqctx.remaining())
            if timeout is None:
                if n > 1:
                    raise upward(last_class, last, idempotent, self.id)  # type: ignore[misc]
                self._m.rejected.labels(self.id, self.surface, "deadline_budget").inc()
                raise errs.new("DEADLINE_BUDGET_EXHAUSTED", "not enough time was left to call a dependency").with_(
                    "dependency", self.id)
            permit = self.breaker.start(self.now())
            self._state()
            if permit is None:
                if n > 1:
                    raise upward(last_class, last, idempotent, self.id)  # type: ignore[misc]
                self._m.rejected.labels(self.id, self.surface, "circuit_open").inc()
                raise errs.new("CIRCUIT_OPEN", "a dependency is failing; calls to it are paused").with_(
                    "dependency", self.id).with_retry_after(1)
            try:
                with reqctx.deadline_scope(time.monotonic() + timeout):
                    async with asyncio.timeout(timeout):
                        r = await attempt(timeout)
            except TimeoutError as e:
                r = Result(err=e, code=errs.Code.DEADLINE_EXCEEDED.value)
            cls = classify(r, False)
            self.breaker.end(permit, self.now(), cls.breaker)
            self.budget.record(cls.budget)
            self._m.attempts.labels(self.id, self.surface, cls.value).inc()
            self._state()
            if cls == Class.SUCCESS:
                return
            last, last_class = r, cls
            backoff = random.uniform(0, policy.max_backoff(n))
            left = reqctx.remaining()
            if not policy.should_retry(idempotent, cls.retry_outcome, n, self.budget.allows_retry(),
                                       None if left is None else left - backoff):
                raise upward(cls, r, idempotent, self.id)  # type: ignore[misc]
            await self.sleep(backoff)

    def _state(self) -> None:
        self._m.breaker.labels(self.id, self.surface).set(_STATES[self.breaker.state])


class HTTP:
    """Calls one dependency's user surface; build it with Member.http."""

    def __init__(self, dep: Dependency, base: str, client: httpx.AsyncClient):
        self.dep, self.base, self._client = dep, base.rstrip("/"), client

    async def do(self, method: str, path: str, body: Any = None, *, idempotent: bool = False,
                 idempotency_key: str = "", forward_user: bool = False) -> Any:
        """Send the request; returns the decoded 2xx JSON answer (None without a body). path carries the query string.
        GET and HEAD are idempotent; forward_user sends the caller's own credential (reads on the user's behalf go
        through the user surface with it, never with the service's identity). Errors are the protocol's."""
        content = None if body is None else json.dumps(body).encode()
        auth = ""
        if forward_user and not (auth := reqctx.user_credential()):
            raise errs.from_exception(RuntimeError("calls: forward_user without a user credential in the context"))
        answer: list[bytes] = []

        async def attempt(timeout: float) -> Result:
            headers = {"Accept": "application/json", HEADER_TIMEOUT_MS: str(max(1, int(timeout * 1000)))}
            if content is not None:
                headers["Content-Type"] = "application/json"
            if rid := reqctx.request_id():
                headers[HEADER_REQUEST_ID] = rid
            if idempotency_key:
                headers[HEADER_IDEMPOTENCY_KEY] = idempotency_key
            if auth:
                headers["Authorization"] = auth
            PROPAGATOR.inject(headers)
            try:
                resp = await self._client.request(method, self.base + path, content=content, headers=headers)
            except httpx.ConnectError as e:
                return Result(err=e, code=errs.Code.UNAVAILABLE.value, connect_failed=True)
            except httpx.TimeoutException as e:
                return Result(err=e, code=errs.Code.DEADLINE_EXCEEDED.value)
            except httpx.TransportError as e:
                return Result(err=e, code=errs.Code.UNAVAILABLE.value)
            if resp.status_code < 300:
                answer.append(resp.content[:MAX_RESPONSE])
                return Result()
            remote, code = errwire.from_http(resp.status_code, resp.headers.get("content-type", ""),
                                             resp.headers.get("retry-after", ""), resp.content[:MAX_RESPONSE])
            if remote is not None:
                return Result(err=remote, code=code, remote=remote)
            return Result(err=RuntimeError(f"{self.dep.id} answered {resp.status_code}"), code=code)

        await self.dep.do(idempotent or method in ("GET", "HEAD"), attempt)
        if answer and answer[-1]:
            try:
                return json.loads(answer[-1])
            except ValueError as e:
                raise errs.from_exception(ValueError(f"decoding the answer of {self.dep.id}: {e}")) from e
        return None


class _Interceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Every unary call on a member's channel goes through the dependency's limit, breaker, budget and deadline."""

    def __init__(self, dep: Dependency):
        self.dep = dep

    async def intercept_unary_unary(self, continuation, details, request):  # type: ignore[override]
        md = list(details.metadata or [])
        idempotent = any(k == _IDEMPOTENT for k, _ in md)
        md = [(k, v) for k, v in md if k != _IDEMPOTENT]
        response: list[Any] = []

        async def attempt(timeout: float) -> Result:
            out = [(k, v) for k, v in md if k != METADATA_REQUEST_ID]
            if rid := reqctx.request_id():
                out.append((METADATA_REQUEST_ID, rid))
            carrier: dict[str, str] = {}
            PROPAGATOR.inject(carrier)
            out += list(carrier.items())
            call_details = grpc.aio.ClientCallDetails(details.method, timeout, out, details.credentials,
                                                      details.wait_for_ready)
            try:
                call = await continuation(call_details, request)
                response.append(await call)
                return Result()
            except grpc.aio.AioRpcError as e:
                remote, code = errwire.from_grpc(e.code(), e.details() or "", e.trailing_metadata())
                if remote is not None:
                    return Result(err=remote, code=code, remote=remote)
                return Result(err=e, code=code)

        await self.dep.do(idempotent, attempt)
        return response[-1]


class Member:
    """One member's dependencies: each (dependency, surface) is created once and kept for the member's life, with
    the member's own HTTP client and gRPC channels (never process-wide ones)."""

    def __init__(self, registry: CollectorRegistry | None = None):
        self._metrics = _Metrics(registry)
        self._deps: dict[tuple[str, str], Dependency] = {}
        self._client: httpx.AsyncClient | None = None
        self._channels: dict[tuple[str, str], grpc.aio.Channel] = {}
        self.channel_credentials: grpc.ChannelCredentials | None = None  # the member's mTLS identity (chapter 08)

    def dependency(self, id: str, surface: str) -> Dependency:
        key = (id, surface)
        if key not in self._deps:
            self._deps[key] = Dependency(id, surface, self._metrics)
        return self._deps[key]

    def http(self, id: str, base: str) -> HTTP:
        if self._client is None:
            self._client = httpx.AsyncClient(trust_env=False, timeout=None, limits=httpx.Limits(
                max_connections=policy.OUTBOUND_LIMIT, keepalive_expiry=90))
        return HTTP(self.dependency(id, "http"), base, self._client)

    def grpc(self, id: str, target: str, options: list[tuple[str, Any]] | None = None) -> grpc.aio.Channel:
        """The member's channel to a dependency's system surface at target (host:port)."""
        key = (id, target)
        if key not in self._channels:
            interceptors = [_Interceptor(self.dependency(id, "grpc"))]
            if self.channel_credentials is not None:
                self._channels[key] = grpc.aio.secure_channel(target, self.channel_credentials, options or [],
                                                              interceptors=interceptors)
            else:
                self._channels[key] = grpc.aio.insecure_channel(target, options or [], interceptors=interceptors)
        return self._channels[key]

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
        await asyncio.gather(*(c.close() for c in self._channels.values()))
        self._channels.clear()
