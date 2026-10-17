"""The shell launcher (protocol 17, "shell obligations"): one process hosting the members installed in it, each on its
own port with its own runtime, configuration, logger, metrics and tracer; nothing but the process is merged. It
refuses to start, naming the members, when what BrickKit asks it to host does not match what is installed, or when
any member's configuration is invalid."""
from __future__ import annotations

import asyncio
import inspect
import json
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import timedelta
from importlib import metadata
from typing import IO, Callable

from packaging.version import InvalidVersion, Version
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.metrics_core import Metric

import besdk
from besdk import clock, config, db, host, obs, protocol
from besdk.component import Definition
from besdk.config import ConfigError, EnvSource, Key, MapSource, Problem, Type
from besdk.db.pool import DB, Pool
from besdk.events.bus import open_bus
from besdk.http import Deps, Platform, Routes, build_app
from besdk.launcher import ready_exit


@dataclass(frozen=True)
class Identity:
    """The shell's own component identity, e.g. be/py-render."""

    id: str
    version: str


@dataclass(frozen=True)
class Installed:
    """The distribution that provides a member's code, as the environment's metadata says."""

    name: str
    version: str
    direct_url: dict | None  # PEP 610: present for installs from a path, a VCS or an archive URL


def installed_of(d: Definition) -> tuple[Installed | None, str]:
    """The distribution providing a definition: the top-level package of its new function, looked up in the installed
    distributions' metadata."""
    module = inspect.getmodule(d.new)
    top = (module.__name__ if module else "").split(".")[0]
    names = metadata.packages_distributions().get(top, [])
    if not names:
        return None, top
    dist = metadata.distribution(names[0])
    raw = dist.read_text("direct_url.json")
    return Installed(dist.metadata["Name"], dist.version, json.loads(raw) if raw else None), top


@dataclass
class Options:
    args: list[str] = field(default_factory=list)
    env: EnvSource = field(default_factory=EnvSource)
    stdout: IO[str] = field(default_factory=lambda: sys.stdout)
    stderr: IO[str] = field(default_factory=lambda: sys.stderr)
    host: str = "0.0.0.0"
    installed_of: Callable[[Definition], tuple[Installed | None, str]] = installed_of  # replaced in tests


SHELL_KEYS = [
    Key("COMPONENT_ID", Type.STRING, required=True), Key("COMPONENT_VERSION", Type.STRING, required=True),
    Key("PORT", Type.INTEGER, required=True), Key("BRICKKIT_SERVED_MEMBERS_CONFIG", Type.STRING, required=True),
    Key("SHELL_ALLOW_REPLACE", Type.BOOLEAN, default=False),
    Key("PG_SHELL_USER", Type.STRING), Key("PG_SHELL_PASSWORD", Type.STRING, secret=True),
    Key("LOG_LEVEL", Type.ENUM, values=("debug", "info", "warn", "error"), default="info"),
    Key("OTEL_URL", Type.STRING),
    Key("STOP_DRAIN_DELAY", Type.DURATION, default=timedelta(seconds=2)),
    Key("STOP_TIMEOUT", Type.DURATION, default=timedelta(seconds=30)),
]


def main(self: Identity, *members: Definition) -> None:
    """Run the shell as a process and exit with its exit code."""

    async def run_until_signal() -> int:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)
        return await run(self, list(members), Options(args=sys.argv[1:]), stop)

    sys.exit(asyncio.run(run_until_signal()))


@dataclass
class Served:
    """One member as BrickKit asks the shell to host it (BRICKKIT_SERVED_MEMBERS_CONFIG)."""

    component_id: str
    version: str
    http_port: int
    extra_ports: dict[str, int]
    config: dict[str, str]

    @staticmethod
    def parse(raw: str) -> list[Served]:
        return [Served(m["componentId"], m.get("version", ""), int(m.get("httpPort") or 0),
                       {p["name"]: int(p["port"]) for p in m.get("extraPorts") or []}, dict(m.get("config") or {}))
                for m in json.loads(raw)]


@dataclass
class Planned:
    definition: Definition
    served: Served
    values: config.Values


def _refusal(member: str, key: str, kind: str, detail: str) -> Problem:
    return Problem(f"{member}:{key}", kind, detail)


def _same_version(a: str, b: str) -> bool:
    try:
        return Version(a) == Version(b)
    except InvalidVersion:
        return a == b


def check(raw: str, defs: list[Definition], lookup: Callable[[Definition], tuple[Installed | None, str]],
          allow_replace: bool) -> tuple[list[Planned], list[Problem], list[str]]:
    """Protocol 17's refusals: members listed but not installed, installed at another version (by the definition and
    by the distribution metadata; no install from a local path in a release build), members whose configuration is
    invalid, and process-level keys that disagree. Every refusal is returned at once."""
    try:
        served = Served.parse(raw)
    except (ValueError, KeyError, TypeError) as e:
        return [], [_refusal("shell", "BRICKKIT_SERVED_MEMBERS_CONFIG", "invalid", f"JSON list of members: {e}")], []
    by_id = {d.id: d for d in defs}
    out, refusals, warnings = [], [], []
    seen_ids: set[str] = set()
    seen_ports: dict[int, str] = {}
    for s in served:
        mid = s.component_id
        if mid in seen_ids:
            refusals.append(_refusal(mid, "componentId", "invalid", "listed twice"))
            continue
        seen_ids.add(mid)
        for name, port in [("httpPort", s.http_port), *((f"extraPorts.{n}", p) for n, p in s.extra_ports.items())]:
            if port <= 0 or port in seen_ports:
                refusals.append(_refusal(mid, name, "invalid", f"a free port ({port} is taken by {seen_ports.get(port, '')})"))
            seen_ports[port] = mid
        d = by_id.get(mid)
        if d is None:
            refusals.append(_refusal(mid, "componentId", "invalid", "a member installed in this shell; this shell has no module " + mid))
            continue
        if d.version != s.version:
            refusals.append(_refusal(mid, "version", "invalid", f"{s.version}, the version installed is {d.version}"))
        inst, top = lookup(d)
        if inst is None:
            refusals.append(_refusal(mid, "distribution", "invalid", f"no installed distribution provides the package {top}"))
        else:
            if not _same_version(inst.version, s.version):
                refusals.append(_refusal(mid, "distribution", "invalid",
                                         f"distribution {inst.name} at {s.version}, but the environment has {inst.version}"))
            url = (inst.direct_url or {}).get("url", "")
            if url.startswith("file:"):
                msg = f"distribution {inst.name} is installed from {url}"
                if allow_replace:
                    warnings.append(f"{mid}: {msg} (allowed by SHELL_ALLOW_REPLACE; never in a release build)")
                else:
                    refusals.append(_refusal(mid, "distribution", "invalid", "no local install in a release build; " + msg))
        try:
            out.append(Planned(d, s, config.parse(host.keys(d), MapSource(s.config))))
        except ConfigError as e:
            refusals += [_refusal(mid, p.key, p.kind, p.expected) for p in e.problems]
    return out, refusals + process_keys_agree(out) + targets_agree(out), warnings


def targets_agree(members: list[Planned]) -> list[Problem]:
    """Members on the same database target share one pool, so they agree on how statements run there (P2.11, P5.3)."""
    first: dict[db.Target, Planned] = {}
    out = []
    for m in members:
        if m.definition.migrations is None:
            continue
        s = db.settings_from(m.values)
        if (f := first.setdefault(s.target, m)) is not m and db.settings_from(f.values).prepared != s.prepared:
            out.append(_refusal(m.definition.id, "PG_POOLER_PREPARED", "invalid",
                                f"the same value as {f.definition.id} (members on target {s.target} share one pool)"))
    return out


def process_keys_agree(members: list[Planned]) -> list[Problem]:
    """Every key marked process has the same value in every member that declares it (P2.11)."""
    first: dict[str, tuple[str, object]] = {}
    out = []
    for m in members:
        for k in host.keys(m.definition):
            if not k.process:
                continue
            value, _ = m.values.raw(k.name)
            if k.secret and (s := m.values.secret(k.name)) is not None:
                value = s.value()  # compared by value, never shown
            if k.name not in first:
                first[k.name] = (m.definition.id, value)
            elif first[k.name][1] != value:
                out.append(_refusal(m.definition.id, k.name, "invalid",
                                    f"the same value as {first[k.name][0]} (a process-level key must agree across the shell)"))
    return sorted(out, key=lambda p: p.key)


async def run(self: Identity, defs: list[Definition], o: Options, stop: asyncio.Event) -> int:
    """Run the shell until stop is set and return the exit code."""
    if o.args and o.args[0] != "serve":
        o.stderr.write(json.dumps({"level": "ERROR", "msg": "a shell only serves; migrations run from each member's "
                                   f"own image (entry {o.args[0]!r})"}) + "\n")
        return host.EXIT_USAGE
    started = time.monotonic()
    problems: list[Problem] = []
    try:
        sv = config.parse_platform(SHELL_KEYS, o.env)
    except ConfigError as e:
        host.report_config(o.stderr, self.id, e.problems)
        return host.EXIT_CONFIGURATION
    if sv.string("COMPONENT_ID") != self.id:
        problems.append(Problem("COMPONENT_ID", "invalid", self.id, sv.string("COMPONENT_ID")))
    if sv.string("COMPONENT_VERSION") != self.version:
        problems.append(Problem("COMPONENT_VERSION", "invalid", self.version, sv.string("COMPONENT_VERSION")))
    plans, refusals, warnings = check(sv.string("BRICKKIT_SERVED_MEMBERS_CONFIG"), defs, o.installed_of,
                                      sv.bool("SHELL_ALLOW_REPLACE"))
    if any(p.definition.migrations is not None for p in plans) and (
            not sv.string("PG_SHELL_USER") or sv.secret("PG_SHELL_PASSWORD") is None):
        refusals.append(_refusal(self.id, "PG_SHELL_USER", "missing",
                                 "the shell's login role and PG_SHELL_PASSWORD: a hosted member owns a database"))
    if problems or refusals:
        host.report_config(o.stderr, self.id, problems + refusals)
        return host.EXIT_CONFIGURATION
    log = obs.new_logger(obs.Member(self.id, self.version), sv.string("LOG_LEVEL"), o.stdout)
    for w in warnings:
        log.warn("shell built for development", detail=w)
    try:
        clock.check()
    except clock.ClockError as e:
        log.error("start check failed: no clock that counts suspension (CLOCK_BOOTTIME)", error=str(e))
        return host.EXIT_STARTUP_CHECK
    return await _serve(self, sv, plans, log, o, stop, started)


async def _serve(self: Identity, sv: config.Values, plans: list[Planned], log: obs.Logger, o: Options,
                 stop: asyncio.Event, started: float) -> int:
    pools = await _open_pools(self, sv, plans)
    transport = None
    try:
        users = [p for p in plans if p.definition.streams or p.definition.subscribes]
        if users:  # one bus per process; BUS_URL is a process-level key, so the members agree on it
            try:
                transport = await open_bus(users[0].values.secret("BUS_URL").value(), self.id, log)  # type: ignore[union-attr]
            except (OSError, ValueError, ConnectionError) as e:
                log.error("the bus is not usable", error=str(e))
                return host.EXIT_STARTUP_CHECK
        return await _serve_members(self, sv, plans, log, o, stop, started, pools, transport)
    finally:
        if transport is not None:
            await transport.close()
        await asyncio.gather(*(p.close() for p in pools.values()))


async def _open_pools(self: Identity, sv: config.Values, plans: list[Planned]) -> dict[db.Target, Pool]:
    """One pool per database target, with the shell's login role; its size is the sum of the members' budgets on that
    target (P5.1, P5.2). The login role holds the members' runtime roles WITH INHERIT FALSE and switches per
    transaction."""
    budgets: dict[db.Target, tuple[int, bool]] = {}
    for p in plans:
        if p.definition.migrations is not None:
            s = db.settings_from(p.values)
            size, _ = budgets.get(s.target, (0, s.prepared))
            budgets[s.target] = (size + s.foreground + s.background, s.prepared)
    return {t: await Pool.open(t, sv.string("PG_SHELL_USER"), sv.secret("PG_SHELL_PASSWORD"), self.id, size, prepared)
            for t, (size, prepared) in budgets.items()}


async def _serve_members(self: Identity, sv: config.Values, plans: list[Planned], log: obs.Logger, o: Options,
                         stop: asyncio.Event, started: float, pools: dict[db.Target, Pool], transport=None) -> int:
    members: list[host.Member] = []
    servers: list = []

    async def fail(member: str, msg: str, err: BaseException) -> int:
        log.error(msg, member=member, error=repr(err))
        await asyncio.gather(*(s.shutdown(0) for s in servers))
        return host.EXIT_FAILURE

    for p in plans:
        try:
            database = None
            if p.definition.migrations is not None:
                st = db.settings_from(p.values)
                database = DB(p.definition.id, pools[st.target], st)
            m = await host.build(p.definition, p.values, host.Shared(MapSource(p.served.config), o.env.read_file, o.stdout,
                                                                     db=database, transport=transport))
        except host.ConfigError as e:
            host.report_config(o.stderr, p.definition.id, e.problems)
            await asyncio.gather(*(s.shutdown(0) for s in servers))
            return host.EXIT_CONFIGURATION
        except (host.BuildError, ValueError) as e:
            return await fail(p.definition.id, "a member could not be built", e)
        try:
            server = host.HTTPServer(m.app, host.listen(p.served.http_port, o.host))
            await server.start()
        except OSError as e:
            return await fail(p.definition.id, "cannot listen on the member's port", e)
        members.append(m)
        servers.append(server)
        if m.system is not None:
            port = p.served.extra_ports.get("grpc", 0)
            if port <= 0:
                return await fail(p.definition.id, "the member registers gRPC services but BrickKit gave it no grpc port",
                                  ValueError("no extra port grpc"))
            try:
                servers.append(host.GRPCServer(m.system, port, o.host))
                await servers[-1].start()
            except (OSError, RuntimeError) as e:
                return await fail(p.definition.id, "cannot listen on the member's grpc port", e)
    try:
        own = host.HTTPServer(_own_surface(self, sv, members), host.listen(sv.int("PORT"), o.host))
        await own.start()
    except OSError as e:
        return await fail(self.id, "cannot listen on PORT", e)
    servers.append(own)
    for pool in pools.values():
        if (code := await ready_exit(pool, log, stop)) is not None:
            await asyncio.gather(*(s.shutdown(0) for s in servers))
            return code
    for m in members:  # in the order BrickKit lists them
        try:
            await m.start(o.env.read_file)
        except Exception as e:  # noqa: BLE001
            return await fail(m.definition.id, "a member could not start", e)
        m.state = host.READY
    log.info("ready", event="ready", protocol=protocol.capabilities()["protocol"], sdk=besdk.Version,
             members=len(members), startup_ms=int((time.monotonic() - started) * 1000))
    await stop.wait()
    # the shell's stop grace is never shorter than any member's (P1.17)
    drain = max([sv.duration("STOP_DRAIN_DELAY")] + [m.values.duration("STOP_DRAIN_DELAY") for m in members])
    timeout = max([sv.duration("STOP_TIMEOUT")] + [m.values.duration("STOP_TIMEOUT") for m in members])
    return await host.stop_all(log, members, servers, drain.total_seconds(), timeout.total_seconds())


class _Labelled:
    """One member's registry with component=<member> on every series, so the shell's /metrics tells the members apart
    (protocol 17: each member labelled)."""

    def __init__(self, registry: CollectorRegistry, component: str):
        self.registry, self.component = registry, component

    def collect(self):
        for family in self.registry.collect():
            out = Metric(family.name, family.documentation, family.type, family.unit)
            for s in family.samples:
                out.add_sample(s.name, {**s.labels, "component": self.component}, s.value, s.timestamp, s.exemplar)
            yield out


def _own_surface(self: Identity, sv: config.Values, members: list[host.Member]):
    """The shell's own port: health, readiness of all members, self-description with the members, and every member's
    metrics labelled with its component."""
    me = obs.Member(self.id, self.version)
    aggregate = CollectorRegistry(auto_describe=False)
    for m in members:
        aggregate.register(_Labelled(m.registry, m.definition.id))  # type: ignore[arg-type]

    def ready() -> tuple[bool, str, list[str]]:
        waiting = [m.definition.id for m in members if m.state != host.READY]
        if not waiting:
            return True, host.READY, []
        return False, host.STOPPING if members and members[0].state == host.STOPPING else host.STARTING, waiting

    def describe() -> dict:
        return {"component_id": self.id, "version": self.version, "protocol": protocol.capabilities()["protocol"],
                "sdk": "be-sdk-python/" + besdk.Version, "capabilities": [], "members": [m.describe() for m in members]}

    tracer = obs.new_tracer_provider(me, sv.string("OTEL_URL")).get_tracer(self.id)
    return build_app(Deps(obs.new_logger(me, sv.string("LOG_LEVEL"), _Discard()), obs.new_registry(), tracer,
                          Platform(ready, describe, lambda: generate_latest(aggregate))), Routes())


class _Discard:
    def write(self, s: str) -> int:
        return len(s)

    def flush(self) -> None:
        pass
