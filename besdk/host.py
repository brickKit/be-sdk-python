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

import uvicorn
from fastapi import FastAPI

import besdk
from besdk import config, obs, protocol, system
from besdk.calls import client as calls
from besdk.component import Definition, Starter, Stopper
from besdk.config import Key, Problem, Type, Values
from besdk.http import Deps, Guard, Platform, Routes, build_app
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
    guard: Callable[[Member], Guard | None] = lambda m: None
    # the system surface's caller check for a member (chapter 08); None: no service identity yet
    authorize: Callable[[Member], Callable | None] = lambda m: None
    db: object = None  # the member's besdk.db.pool.DB, None without migrations
    transport: object = None  # the process's bus (one per process), None when no member uses events


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

    def stop_background(self) -> None:
        for t in self._tasks:
            t.cancel()

    async def stop(self) -> None:
        try:
            # the events in hand finish; the rest stay with the transport
            await asyncio.gather(*(s.stop() for s in self._subs), return_exceptions=True)
            if self.forwarder is not None:
                await self.forwarder.stop()  # type: ignore[attr-defined]
            if isinstance(self.module, Stopper):
                await self.module.stop()
        finally:
            await self.runtime.outbound.close()  # type: ignore[union-attr]
            if self.claims is not None:
                await self.claims.close()  # type: ignore[attr-defined]

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
    """One member: its logger, registry, tracer, runtime, module and user-surface application."""
    m = obs.Member(d.id, d.version)
    log = obs.new_logger(m, values.string("LOG_LEVEL"), shared.out)
    registry = obs.new_registry()
    tracers = obs.new_tracer_provider(m, values.string("OTEL_URL"))
    outbound = calls.Member(registry)
    producer = forwarder = None
    if d.streams:
        producer = Producer(d.id, d.streams, S3Store.from_values(values))
        forwarder = Forwarder(shared.db, producer, shared.transport, d.id, values.string("IMPLEMENTATION_INSTANCE"),
                              log)  # type: ignore[arg-type]
    rt = Runtime(member=m, config=values, logger=log, metrics=registry, tracer=tracers.get_tracer(d.id),
                 addresses=shared.addresses, db=shared.db, outbound=outbound, events=producer)
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
    member = Member(d, values, log, registry, tracers, rt, module, FastAPI())
    member.forwarder, member.transport = forwarder, shared.transport
    if d.subscribes:
        subs = Subscriptions()
        module.subscriptions(subs)  # type: ignore[attr-defined]
        member.claims = HTTPClaims()
        member.consumers = [Consumer(d.id, s, shared.db, log, member.claims) for s in subs.list]  # type: ignore[arg-type]
    guard = shared.guard(member)
    member.app = build_app(Deps(log, registry, rt.tracer, Platform(
        ready or (lambda: (member.state == READY, member.state, [])), member.describe), guard), routes)
    if not services.empty:
        member.system = system.build_server(system.Deps(log, registry, rt.tracer, shared.authorize(member)), services)
    return member


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
        self.server = _Server(uvicorn.Config(app, log_config=None, access_log=False, lifespan="off"))
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

    def __init__(self, server, port: int, host: str = "0.0.0.0"):
        self.server = server
        bound = server.add_insecure_port(f"{host}:{port}")  # mTLS arrives with the service identity (chapter 08)
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
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
