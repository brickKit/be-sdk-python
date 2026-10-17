"""Runs one component on its own (protocol chapter 01): entries, configuration, the user surface on PORT, readiness
and the graceful stop. main is the only place that touches the real process (environment, signals, exit); run takes
all of that as arguments so it can be tested."""
from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import IO

import asyncpg

import besdk
from besdk import clock, config, db, host, obs, protocol
from besdk.db import migrate
from besdk.db.pool import DB, CredentialsError, Pool, StartCheckError, StoppedWaiting
from besdk.events.bus import open_bus
from besdk.component import Definition
from besdk.config import ConfigError, EnvSource, Key, Problem, Type

EXIT_OK, EXIT_FAILURE, EXIT_USAGE = host.EXIT_OK, host.EXIT_FAILURE, host.EXIT_USAGE
EXIT_STARTUP_CHECK, EXIT_CONFIGURATION = host.EXIT_STARTUP_CHECK, host.EXIT_CONFIGURATION

# Injected by BrickKit; the launcher reads them, the module never does.
PLATFORM_KEYS = [Key("COMPONENT_ID", Type.STRING, required=True), Key("COMPONENT_VERSION", Type.STRING, required=True),
                 Key("PORT", Type.INTEGER, required=True)]


@dataclass
class Options:
    """The process-facing inputs of run."""

    args: list[str] = field(default_factory=list)
    env: EnvSource = field(default_factory=EnvSource)
    stdout: IO[str] = field(default_factory=lambda: sys.stdout)
    stderr: IO[str] = field(default_factory=lambda: sys.stderr)
    host: str = "0.0.0.0"


def main(d: Definition) -> None:
    """Run the component as a process and exit with its exit code. Call it from the component's entry point."""

    async def run_until_signal() -> int:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop.set)  # also as PID 1 in a container (P1.16)
        return await run(d, Options(args=sys.argv[1:]), stop)

    sys.exit(asyncio.run(run_until_signal()))


async def run(d: Definition, o: Options, stop: asyncio.Event) -> int:
    """Run one entry of the component until stop is set (the stop signal) and return the exit code."""
    entry = o.args[0] if o.args else "serve"
    if entry == "serve":
        return await _serve(d, o, stop)
    if entry == "migrate":
        return await _migrate(d, o)
    o.stderr.write(json.dumps({"level": "ERROR", "msg": f"unknown entry {entry!r}; use serve or migrate"}) + "\n")
    return EXIT_USAGE


def _parse_all(d: Definition, src: config.Source) -> tuple[config.Values | None, config.Values | None, list[Problem]]:
    """The component's and the SDK's keys; the platform keys checked against the definition."""
    problems: list[Problem] = []
    values = plat = None
    try:
        values = config.parse(host.keys(d), src)
    except ConfigError as e:
        problems += e.problems
    try:
        plat = config.parse_platform(PLATFORM_KEYS, src)
    except ConfigError as e:
        problems += e.problems
    # compared on the raw values, so a mismatch is reported even when another platform key is missing
    if (cid := src.lookup("COMPONENT_ID")) and cid != d.id:
        problems.append(Problem("COMPONENT_ID", "invalid", d.id, cid))
    if (ver := src.lookup("COMPONENT_VERSION")) and ver != d.version:
        problems.append(Problem("COMPONENT_VERSION", "invalid", d.version + " (the version built into this image)", ver))
    return values, plat, problems


async def _serve(d: Definition, o: Options, stop: asyncio.Event) -> int:
    started = time.monotonic()
    values, plat, problems = _parse_all(d, o.env)
    if problems:
        host.report_config(o.stderr, d.id, problems)
        return EXIT_CONFIGURATION
    assert values is not None and plat is not None
    pool = database = None
    if d.migrations is not None:
        # alone, the component connects with its own runtime login role (protocol 14, "connection pools")
        s = db.settings_from(values)
        pool = await Pool.open(s.target, s.user, s.password, d.id, s.foreground + s.background, s.prepared)
        database = DB(d.id, pool, s)
    transport = None
    try:
        if d.streams or d.subscribes:
            # one bus connection per process (protocol 17)
            blog = obs.new_logger(obs.Member(d.id, d.version), values.string("LOG_LEVEL"), o.stdout)
            try:
                transport = await open_bus(values.secret("BUS_URL").value(), d.id, blog)  # type: ignore[union-attr]
            except (OSError, ValueError, ConnectionError) as e:
                blog.error("the bus is not usable", error=str(e))
                return EXIT_STARTUP_CHECK
        return await _serve_member(d, o, stop, values, plat, started, pool, database, transport)
    finally:
        if transport is not None:
            await transport.close()
        if pool is not None:
            await pool.close()


async def _serve_member(d: Definition, o: Options, stop: asyncio.Event, values: config.Values, plat: config.Values,
                        started: float, pool: Pool | None, database: DB | None, transport=None) -> int:
    try:
        m = await host.build(d, values, host.Shared(o.env, o.env.read_file, o.stdout, db=database, transport=transport))
    except host.ConfigError as e:
        host.report_config(o.stderr, d.id, e.problems)
        return EXIT_CONFIGURATION
    except (host.BuildError, ValueError) as e:
        if not isinstance(e, host.BuildError):
            o.stdout.write(json.dumps({"level": "ERROR", "component": d.id, "msg": str(e)}) + "\n")
        return EXIT_FAILURE
    log = m.logger
    try:
        clock.check()
    except clock.ClockError as e:
        log.error("start check failed: no clock that counts suspension (CLOCK_BOOTTIME)", error=str(e))
        return EXIT_STARTUP_CHECK
    if pool is not None:
        if (code := await ready_exit(pool, log, stop)) is not None:
            return code
    try:
        server = host.HTTPServer(m.app, host.listen(plat.int("PORT"), o.host))
        await server.start()
    except OSError as e:
        log.error("cannot listen on PORT", error=str(e))
        return EXIT_FAILURE
    servers: list = [server]
    if m.system is not None:
        if d.grpc_port <= 0:
            log.error("the module registers gRPC services but the definition has no grpc_port")
            await server.shutdown(0)
            return EXIT_FAILURE
        try:
            servers.append(host.GRPCServer(m.system, d.grpc_port, o.host))
            await servers[-1].start()
        except (OSError, RuntimeError) as e:
            log.error("cannot listen on the grpc port", error=str(e))
            await server.shutdown(0)
            return EXIT_FAILURE
    try:
        await m.start(o.env.read_file)
    except Exception as e:  # noqa: BLE001
        log.error("the module could not start", error=repr(e))
        await asyncio.gather(*(s.shutdown(0) for s in servers))
        return EXIT_FAILURE
    m.state = host.READY
    log.info("ready", event="ready", protocol=protocol.capabilities()["protocol"], sdk=besdk.Version,
             startup_ms=int((time.monotonic() - started) * 1000))
    waiter = asyncio.create_task(stop.wait())
    done, _ = await asyncio.wait({waiter, server.task}, return_when=asyncio.FIRST_COMPLETED)  # type: ignore[arg-type]
    if server.task in done:
        log.error("the user surface stopped unexpectedly")
        return EXIT_FAILURE
    return await host.stop_all(log, [m], servers, values.duration("STOP_DRAIN_DELAY").total_seconds(),
                               values.duration("STOP_TIMEOUT").total_seconds())


async def ready_exit(pool: Pool, log, stop: asyncio.Event) -> int | None:
    """Wait for the database (P5.10); None when it is usable, else the exit code."""
    try:
        await pool.ready(log, stop)
        return None
    except StoppedWaiting:
        return EXIT_OK  # stopped while waiting for the database
    except CredentialsError as e:
        log.error("the database refused the credentials", error=str(e))
        return EXIT_CONFIGURATION
    except StartCheckError as e:
        log.error("a start check of the database failed", error=str(e))
        return EXIT_STARTUP_CHECK


async def _migrate(d: Definition, o: Options) -> int:
    """The migrate entry (P1.1, P1.2): the SDK's tables, then the component's, as the owner role directly on the
    database. Only the database keys are read, so the entry does not depend on the rest of the configuration."""
    if d.migrations is None:
        o.stderr.write(json.dumps({"level": "ERROR", "component": d.id, "msg": "migrate: this component has no database"}) + "\n")
        return EXIT_USAGE
    try:
        values = config.parse([*db.migrate_keys(), host.sdk_keys()[0]], o.env)
    except ConfigError as e:
        host.report_config(o.stderr, d.id, e.problems)
        return EXIT_CONFIGURATION
    log = obs.new_logger(obs.Member(d.id, d.version), values.string("LOG_LEVEL"), o.stdout)
    cfg, problems = migrate.config_from(d.id, values, log)
    if problems:
        host.report_config(o.stderr, d.id, problems)
        return EXIT_CONFIGURATION
    try:
        await migrate.migrate(cfg, migrate.sdk_migrations(), migrate.MigrationSet(d.id, "schema_migrations", d.migrations))
    except (migrate.MigrateError, OSError, asyncpg.PostgresError) as e:
        log.error("migrate failed", error=str(e))  # the last line names the failed migration (P1.2)
        return EXIT_FAILURE
    log.info("migrate done")
    return EXIT_OK
