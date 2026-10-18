"""Builds and runs one member: the same code path for a component running alone (launcher) and for a member of a
shell, so the two behave the same (protocol 17: "each member alone and in a shell behaves the same")."""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import socket
import time
import traceback
from dataclasses import dataclass, field
from datetime import timedelta
from typing import IO, Callable
from urllib.parse import urlsplit

import uvicorn
from fastapi import FastAPI

import besdk
from besdk import config, obs, protocol, reqctx, system
from besdk.audit import Auditor, GRPCSender
from besdk.authz import GRPCSource, Holder
from besdk.calls import client as calls
from besdk.component import Definition, Starter, Stopper
from besdk.config import Key, Problem, Type, Values
from besdk.guard import UserGuard
from besdk.http import Deps, Platform, Routes, build_app
from besdk.identity import KeySet, ServiceTLS, Verifier
from besdk.events.consumer import Consumer, Subscriptions
from besdk.events.objectstore import HTTPClaims, S3Store
from besdk.events.outbox import Forwarder, Producer
from besdk.runtime import Runtime

# Exit codes (protocol chapter 01).
EXIT_OK, EXIT_FAILURE, EXIT_USAGE, EXIT_STARTUP_CHECK, EXIT_CONFIGURATION = 0, 1, 64, 69, 78
STARTING, READY, STOPPING = "starting", "ready", "stopping"
SECRET_REREAD_EVERY = 30.0


def sdk_keys() -> list[Key]:
    """Read for every member on top of its own keys."""
    return [Key("LOG_LEVEL", Type.ENUM, values=("debug", "info", "warn", "error"), default="info"),
            Key("OTEL_URL", Type.STRING, process=True),
            Key("STOP_DRAIN_DELAY", Type.DURATION, default=timedelta(seconds=2)),
            Key("STOP_TIMEOUT", Type.DURATION, default=timedelta(seconds=30))]


def security_keys() -> list[Key]:
    """Identity and authorization keys (chapters 02 and 08); which are required depends on what the member serves."""
    return [Key("DEPLOYMENT_ID", Type.STRING, process=True), Key("IAM_ISSUER", Type.STRING, process=True),
            Key("IAM_JWKS_URL", Type.STRING, process=True), Key("AUTHZ_GRPC_URL", Type.STRING, process=True),
            Key("AUDIT_GRPC_URL", Type.STRING, process=True), Key("SERVICE_TLS_CERT_FILE", Type.STRING),
            Key("SERVICE_TLS_KEY_FILE", Type.STRING), Key("SERVICE_TLS_CA_FILE", Type.STRING)]


def keys(d: Definition) -> list[Key]:
    """A definition's keys plus the SDK's (and the database keys when the component owns a database)."""
    from besdk import db

    from besdk.events import bus, objectstore

    out = [*d.keys, *sdk_keys(), *security_keys(), *(db.serve_keys() if d.migrations is not None else [])]
    if d.streams or d.subscribes:
        out += [*bus.keys(bool(d.streams)), *objectstore.store_keys()]
    return out


class ConfigError(Exception):
    """Problems found while building a member; the launcher exits 78."""

    def __init__(self, problems: list[Problem]):
        super().__init__("; ".join(str(p) for p in problems))
        self.problems = problems


class BuildError(Exception):
    """The module could not be built; the launcher exits 1."""


def report_config(out: IO[str], member: str, problems: list[Problem]) -> None:
    """Configuration problems, one JSON line each (P2.6), naming the member."""
    for p in problems:
        out.write(json.dumps({"level": "ERROR", "msg": "configuration", "component": member, "key": p.key,
                              "kind": p.kind, "expected": p.expected, "got": p.got}, separators=(",", ":")) + "\n")
    out.flush()


@dataclass
class Shared:
    """What every member of a process shares: where addresses are looked up, file reading, the log stream."""

    addresses: config.Source | None
    read: Callable[[str], bytes]
    out: IO[str]
    db: object = None  # the member's besdk.db.pool.DB, None without migrations
    transport: object = None  # the process's bus (one per process), None when no member uses events
    keys: object = None  # the process's identity keys (besdk.identity.KeySet), None when IAM_JWKS_URL is not set


@dataclass
class Member:
    definition: Definition
    values: Values
    logger: obs.Logger
    registry: object
    tracers: object
    runtime: Runtime
    module: object
    app: FastAPI
    state: str = STARTING
    system: object = None  # the member's grpc.aio server; None without services
    forwarder: object = None
    transport: object = None
    claims: object = None
    consumers: list = field(default_factory=list)
    ready: Callable[[], tuple[bool, str, list[str]]] | None = None  # the shell's readiness for its own port, if any
    tls: object = None  # the member's service identity (besdk.identity.ServiceTLS)
    holder: object = None  # the member's authorization view (besdk.authz.Holder), with protected routes
    auditor: object = None  # with a database: the audit table and its transfer
    audit_sender: object = None  # the audit component's system surface, None without AUDIT_GRPC_URL
    _subs: list = field(default_factory=list)
    _tasks: list[asyncio.Task] = field(default_factory=list)

    def describe(self) -> dict:
        """The self-description (P1.8)."""
        return {"component_id": self.definition.id, "version": self.definition.version,
                "protocol": protocol.capabilities()["protocol"], "sdk": "be-sdk-python/" + besdk.Version,
                "capabilities": list(self.definition.capabilities)}

    async def start(self, read: Callable[[str], bytes]) -> None:
        """The module's background work (if any) and the secret re-reading."""
        if isinstance(self.module, Starter):
            await self.module.start()
        if self.forwarder is not None:
            await self.forwarder.start()  # type: ignore[attr-defined]
        for c in self.consumers:
            self._subs.append(await self.transport.subscribe(c.spec(), c.handle))  # type: ignore[attr-defined]
        self._tasks.append(asyncio.create_task(self._reread_secrets(read)))
        if self.tls is not None:
            self._tasks.append(asyncio.create_task(self._reread_certificate()))
        if self.auditor is not None:
            self._tasks.append(asyncio.create_task(self.auditor.run(self.audit_sender, self.logger)))  # type: ignore[attr-defined]
        if self.holder is not None:
            # one verification before ready, so the first requests find a view; best effort: the authorization member
            # may be down, and then protected requests answer a retryable 503 until it is back
            try:
                with reqctx.deadline_scope(time.monotonic() + 2):
                    await self.holder.verify()  # type: ignore[attr-defined]
            except Exception as e:  # noqa: BLE001
                self.logger.warn("no authorization view yet; protected requests wait for one", error=str(e))
            self._tasks.append(asyncio.create_task(self.holder.run()))  # type: ignore[attr-defined]

    def stop_background(self) -> None:
        for t in self._tasks:
            t.cancel()

    async def stop(self) -> None:
        try:
            # the events in hand finish; the rest stay with the transport
            await asyncio.gather(*(s.stop() for s in self._subs), return_exceptions=True)
            if self.forwarder is not None:
                await self.forwarder.stop()  # type: ignore[attr-defined]
                if (store := self.forwarder.producer.claims) is not None:  # type: ignore[attr-defined]
                    await store.close()
            if isinstance(self.module, Stopper):
                await self.module.stop()
        finally:
            await self.runtime.outbound.close()  # type: ignore[union-attr]
            if self.claims is not None:
                await self.claims.close()  # type: ignore[attr-defined]

    async def _reread_certificate(self) -> None:
        while True:
            await asyncio.sleep(SECRET_REREAD_EVERY)
            try:
                if self.tls.reload():  # type: ignore[attr-defined]
                    self.logger.info("the service certificate changed; new connections use it")
            except ValueError as e:
                self.logger.warn("the service certificate could not be re-read; keeping the previous one", error=str(e))

    async def _reread_secrets(self, read: Callable[[str], bytes]) -> None:
        secrets = self.values.file_secrets()
        while secrets:
            await asyncio.sleep(SECRET_REREAD_EVERY)
            for s in secrets:
                try:
                    if s.refresh(read):
                        self.logger.info("a secret file changed; new connections use the new value")
                except OSError as e:
                    self.logger.warn("a secret file could not be re-read; keeping the previous value", error=str(e))


async def build(d: Definition, values: Values, shared: Shared, ready: Callable[[], tuple[bool, str, list[str]]] | None = None) -> Member:
    """One member: its logger, registry, tracer, runtime, module, user-surface application and system surface."""
    m = obs.Member(d.id, d.version)
    log = obs.new_logger(m, values.string("LOG_LEVEL"), shared.out)
    registry = obs.new_registry()
    tracers = obs.new_tracer_provider(m, values.string("OTEL_URL"))
    outbound = calls.Member(registry)
    try:
        tls = _service_identity(d, values, shared)
    except ValueError as e:
        log.error("the member could not be built", error=str(e))
        raise BuildError(str(e)) from e
    if tls is not None:
        outbound.set_tls(tls)
    producer = forwarder = None
    if d.streams:
        producer = Producer(d.id, d.streams, S3Store.from_values(values))
        forwarder = Forwarder(shared.db, producer, shared.transport, d.id, values.string("IMPLEMENTATION_INSTANCE"),
                              log)  # type: ignore[arg-type]
    auditor = Auditor(shared.db) if shared.db is not None else None  # type: ignore[arg-type]
    rt = Runtime(member=m, config=values, logger=log, metrics=registry, tracer=tracers.get_tracer(d.id),
                 addresses=shared.addresses, db=shared.db, outbound=outbound, events=producer, audit=auditor)
    try:
        module = d.new(rt)
        if inspect.isawaitable(module):
            module = await module
        routes = Routes()
        module.routes(routes)
        services = system.Services()
        if hasattr(module, "services"):
            module.services(services)
    except Exception as e:  # noqa: BLE001 - a module's wiring failure is reported, never a crash
        log.error("the module could not be built", error=repr(e), stack="".join(traceback.format_exception(e)))
        raise BuildError(str(e)) from e
    member = Member(d, values, log, registry, tracers, rt, module, FastAPI(), ready=ready)
    member.forwarder, member.transport, member.tls, member.auditor = forwarder, shared.transport, tls, auditor
    if d.subscribes:
        subs = Subscriptions()
        module.subscriptions(subs)  # type: ignore[attr-defined]
        member.claims = HTTPClaims()
        member.consumers = [Consumer(d.id, s, shared.db, log, member.claims) for s in subs.list]  # type: ignore[arg-type]
    try:
        _surfaces(member, shared, outbound, routes, services)
    except ValueError as e:
        log.error("the member could not be built", error=str(e))
        raise BuildError(str(e)) from e
    return member


def _surfaces(member: Member, shared: Shared, outbound: calls.Member, routes: Routes, services: system.Services) -> None:
    """The audit transfer's target, the user surface with its guard, and the system surface (mTLS only)."""
    d, log = member.definition, member.logger
    if member.auditor is not None:
        member.audit_sender = _audit_sender(member.values, outbound, log)
    ops = [r.operation for r in routes.list() if not r.public]
    guard = _guard(member, shared, outbound, ops) if ops else None
    member.app = build_app(Deps(log, member.registry, member.runtime.tracer, Platform(
        member.ready or (lambda: (member.state == READY, member.state, [])), member.describe), guard), routes)
    if not services.empty:
        _declared(d, services.operations(), "system methods")
        member.system = system.build_server(system.Deps(log, member.registry, member.runtime.tracer, member.tls),
                                            services)


def _service_identity(d: Definition, values: Values, shared: Shared) -> ServiceTLS | None:
    """The member's mTLS identity when its certificate is configured (P8.5): all three files and the deployment."""
    files = [values.string(k) for k in ("SERVICE_TLS_CERT_FILE", "SERVICE_TLS_KEY_FILE", "SERVICE_TLS_CA_FILE")]
    if not any(files):
        return None
    problems = [Problem(k, "missing", "set together with the other SERVICE_TLS_* keys")
                for k in ("SERVICE_TLS_CERT_FILE", "SERVICE_TLS_KEY_FILE", "SERVICE_TLS_CA_FILE", "DEPLOYMENT_ID")
                if not values.string(k)]
    if problems:
        raise ConfigError(problems)
    return ServiceTLS(values.string("DEPLOYMENT_ID"), d.id, *files, shared.read)


def _declared(d: Definition, ops: list[str], what: str) -> None:
    """Routes and methods name declared operations (P8.7)."""
    if d.authz is None:
        raise ValueError(f"the module has {what} but the definition declares no resources and operations (authz)")
    d.authz.validate()
    d.authz.check_operations(ops)


def _guard(member: Member, shared: Shared, outbound: calls.Member, ops: list[str]) -> UserGuard:
    """The admission of protected routes: declared operations, the identity keys, and a view holder that verifies
    with the authorization member over mTLS (P8.7, P8.9)."""
    d, values = member.definition, member.values
    _declared(d, ops, "protected routes")
    problems = [Problem(k, "missing", why) for k, why in (
        ("DEPLOYMENT_ID", "the deployment identity: protected routes accept its audience"),
        ("IAM_ISSUER", "the identity member's issuer: protected routes verify tokens"),
        ("IAM_JWKS_URL", "the identity member's keys: protected routes verify tokens"),
        ("AUTHZ_GRPC_URL", "the authorization member's system surface: protected routes need a view"))
        if not values.string(k)]
    if member.tls is None:
        problems.append(Problem("SERVICE_TLS_CERT_FILE", "missing",
                                "the member's service identity: the view is verified over mTLS"))
    if problems or shared.keys is None:
        raise ConfigError(problems)
    channel = outbound.grpc("infra/authz", host_port(values.string("AUTHZ_GRPC_URL")))
    member.holder = Holder(d.id, GRPCSource(channel), member.logger)
    verifier = Verifier(values.string("IAM_ISSUER"), "be:deployment:" + values.string("DEPLOYMENT_ID"),
                        shared.keys)  # type: ignore[arg-type]
    return UserGuard(verifier, d.authz, member.holder, member.logger)  # type: ignore[arg-type]


def _audit_sender(values: Values, outbound: calls.Member, log: obs.Logger) -> GRPCSender | None:
    """The audit component's system surface; without AUDIT_GRPC_URL the records stay local (P8.21)."""
    if not (addr := values.string("AUDIT_GRPC_URL")):
        log.warn("AUDIT_GRPC_URL is not set: audit records stay in the local table until it is")
        return None
    return GRPCSender(outbound.grpc("infra/audit", host_port(addr)))


def host_port(addr: str) -> str:
    """An address as BrickKit derives it (http://service:port) or a bare host:port."""
    u = urlsplit(addr)
    return u.netloc if u.netloc else addr


async def open_keys(values: Values, log: obs.Logger) -> KeySet | None:
    """The process's identity keys when IAM_JWKS_URL is set; a failing first fetch is reported and the set keeps
    retrying (the identity member may start later)."""
    if not (url := values.string("IAM_JWKS_URL")):
        return None
    keys, err = await KeySet.open(url)
    if err is not None:
        log.warn("the identity keys could not be fetched yet; they are retried", error=str(err))
    return keys


class _Server(uvicorn.Server):
    """uvicorn without its own signal handling: the launcher (or the shell) owns the process's signals."""

    @contextlib.contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield

    def install_signal_handlers(self) -> None:  # older uvicorn versions
        pass


class HTTPServer:
    """A member's user surface on a socket bound before start, so a busy port fails before anything is ready."""

    def __init__(self, app: FastAPI, sock: socket.socket):
        self.sock, self.name = sock, "http %s:%d" % sock.getsockname()[:2]
        # httptools: requests parsed in C (h11, the pure-Python fallback, cost about 5% more CPU per request)
        self.server = _Server(uvicorn.Config(app, http="httptools", log_config=None, access_log=False, lifespan="off"))
        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self.server.serve(sockets=[self.sock]))
        while not self.server.started:
            if self.task.done():
                self.task.result()
            await asyncio.sleep(0.01)

    async def shutdown(self, timeout: float) -> bool:
        """Stop accepting; let requests in flight finish until timeout. False when some were still running."""
        self.server.config.timeout_graceful_shutdown = max(timeout, 0.001)
        self.server.should_exit = True
        try:
            await asyncio.wait_for(asyncio.shield(self.task), timeout + 1)  # type: ignore[arg-type]
        except TimeoutError:
            self.server.force_exit = True
            return False
        return not self.server.server_state.connections


class GRPCServer:
    """A member's system surface: stop accepting, let calls in flight finish until the stop deadline."""

    def __init__(self, server, port: int, tls, host: str = "0.0.0.0"):
        self.server = server
        bound = system.serve_port(server, f"{host}:{port}", tls)
        if bound == 0:
            raise OSError(f"cannot listen on {host}:{port}")
        self.port, self.name, self.task = bound, f"grpc {host}:{bound}", None

    async def start(self) -> None:
        await self.server.start()
        self.task = asyncio.create_task(self.server.wait_for_termination())

    async def shutdown(self, timeout: float) -> bool:
        started = time.monotonic()
        await self.server.stop(max(timeout, 0.001))
        return time.monotonic() - started < max(timeout, 0.001)


def listen(port: int, host: str = "0.0.0.0") -> socket.socket:
    # IPPROTO_TCP, not 0: asyncio turns Nagle off (TCP_NODELAY) only on connections whose socket says TCP, and
    # accepted connections inherit it from here (P1.18)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(1024)
    sock.setblocking(False)
    return sock


async def stop_all(log: obs.Logger, members: list[Member], servers: list[HTTPServer | GRPCServer], drain: float,
                   timeout: float) -> int:
    """The graceful stop of P1.14-P1.17 for one or more members: every member is not-ready at once, the drain delay
    passes while the listeners still answer, all servers shut down in parallel letting requests finish, then every
    module stops in parallel; tracers close last. Everything ends by the stop timeout. Returns the exit code."""
    deadline = time.monotonic() + timeout
    for m in members:
        m.state = STOPPING
    log.info("stopping", event="stopping")
    if (d := min(drain, deadline - time.monotonic())) > 0:
        await asyncio.sleep(d)
    code = EXIT_OK
    clean = await asyncio.gather(*(s.shutdown(deadline - time.monotonic()) for s in servers))
    for s, ok in zip(servers, clean):
        if not ok:
            log.warn("requests still running at the stop deadline", server=s.name)
            code = EXIT_FAILURE
    for m in members:
        m.stop_background()
    results = await asyncio.gather(*(asyncio.wait_for(m.stop(), max(deadline - time.monotonic(), 0.001))
                                     for m in members), return_exceptions=True)
    for m, r in zip(members, results):
        if isinstance(r, BaseException):
            log.warn("a member did not stop cleanly", member=m.definition.id, error=repr(r))
            code = EXIT_FAILURE
    for m in members:
        m.tracers.shutdown()  # type: ignore[attr-defined]
    log.info("stopped", event="stopped", exit_code=code)
    return code
