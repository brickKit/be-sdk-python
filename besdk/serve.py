"""Serving one component standalone (be-protocol P1.2, P1.4, P1.6–P1.8, P10.7): open the ports, connect in
the background, run the one-time start hook, then wait for SIGTERM and stop in order."""

from __future__ import annotations

import asyncio
import json
import signal
from pathlib import Path
from typing import Any

from besdk import logs
from besdk.config import SECRET_POLL, Config, Manifest
from besdk.http.server import HttpServer, listen
from besdk.jobs.model import JobsConfigError
from besdk.runtime import Module, Runtime, Shared, Spec

START_TIMEOUT = 30.0
EX_OK, EX_FATAL, EX_USAGE, EX_CONFIG = 0, 1, 64, 78


class Fatal(Exception):
    """A fatal condition found after start (P1.8): the process exits non-zero."""


def migrate(spec: Spec, manifest: Manifest, config: Config, cmd: tuple) -> int:
    from besdk.migrate import Migrator

    lg = logs.member_logger(manifest.id, manifest.version)
    if "PG_SCHEMA" not in config.declared():
        lg.info("no_database")
        return EX_OK
    m = Migrator(config, Path(spec.migrations), manifest.id, lg)
    try:
        if cmd[1] == "up":
            m.up()
        elif cmd[1] == "down":
            m.down(cmd[2])
        else:
            print(json.dumps(m.status()))
    except Exception as e:  # noqa: BLE001 - the migration step reports and fails
        lg.error("migration_failed", extra={"error": f"{type(e).__name__}: {e}"})
        return EX_FATAL
    return EX_OK


def _jobs(rt: Runtime, module: Module, **kw: Any) -> Any:
    """The member's job plan; an invalid declaration or JOBS_OVERRIDES is a configuration error (78)."""
    from besdk.jobs.runner import JobsRuntime

    from besdk.lifecycle.decl import LifecycleInvalid

    try:
        rt.jobs = JobsRuntime(rt, module, **kw)
    except JobsConfigError as e:
        rt.logger.error("config_invalid", extra={"key": "JOBS_OVERRIDES", "reason": "CONFIG_INVALID", "error": str(e)})
        return None
    except LifecycleInvalid as e:  # P16.1: fatal, names the table
        rt.logger.error("lifecycle_invalid", extra={"error": str(e)})
        return None
    return rt.jobs


async def job_run(spec: Spec, env: dict[str, str], manifest: Manifest, config: Config, name: str) -> int:
    """``job run <name>`` (P14.8): no server and no other background work; one run through the same tables;
    0 ok or no-op, 1 failed, 64 unknown name, 78 configuration error."""
    rt = Runtime(spec, env, Shared.standalone(spec_id=spec.id, otel_base_url=_otel(config)), manifest=manifest,
                 config=config)
    try:
        return await _job_run(rt, name)
    finally:
        if rt._store is not None:  # noqa: SLF001
            await rt._store.close()  # noqa: SLF001
        rt.telemetry.shutdown()
        await rt.shared.close()


async def _job_run(rt: Runtime, name: str) -> int:
    try:
        module = await asyncio.wait_for(rt.spec.create(rt), START_TIMEOUT)
    except Exception as e:  # noqa: BLE001
        rt.logger.error("create_failed", extra={"error": f"{type(e).__name__}: {e}"})
        return EX_FATAL
    jobs = _jobs(rt, module, holder_suffix=f"job-run:{rt.instance}")
    if jobs is None:
        return EX_CONFIG
    if name not in jobs.names():
        rt.logger.error("job_unknown", extra={"job": name})
        return EX_USAGE
    if "PG_SCHEMA" in rt.config.declared() and not await _schema_current(rt):
        return EX_FATAL
    try:
        outcome = await jobs.run_once(name)
    except Exception:  # noqa: BLE001 - execute logged it
        return EX_FATAL
    rt.logger.info("job_run_finished", extra={"job": name, "outcome": outcome})
    return EX_OK


async def _schema_current(rt: Runtime) -> bool:
    """P1.8 for a one-shot run: the schema is migrated and not newer than the image."""
    try:
        behind, newer = await _migration_state(rt)
    except Exception as e:  # noqa: BLE001
        rt.logger.error("database_unreachable", extra={"error": getattr(e, "internal_message", "") or str(e)})
        return False
    if newer or behind:
        rt.logger.error("schema_version_mismatch", extra={"pending": ",".join(behind), "newer": ",".join(newer)})
        return False
    return True


def _otel(config: Config) -> str:
    return config.string("OTEL_BASE_URL") if "OTEL_BASE_URL" in config.declared() else ""


async def serve(spec: Spec, env: dict[str, str], manifest: Manifest, config: Config,
                stop: asyncio.Event | None = None) -> int:
    shared = Shared.standalone(spec_id=spec.id, otel_base_url=_otel(config))
    rt = Runtime(spec, env, shared, manifest=manifest, config=config)
    stop = stop or _signals()
    fatal: list[str] = []
    try:
        module = await asyncio.wait_for(spec.create(rt), START_TIMEOUT)
    except Exception as e:  # noqa: BLE001 - initialisation failed (P1.8)
        rt.logger.error("create_failed", extra={"error": f"{type(e).__name__}: {e}"})
        await shared.close()
        return EX_FATAL
    if _jobs(rt, module) is None:
        await shared.close()
        return EX_CONFIG
    servers = await _open_ports(rt, module)
    _background(rt, module, stop, fatal)
    if module.start is not None:
        try:
            await asyncio.wait_for(module.start(), START_TIMEOUT)
        except Exception as e:  # noqa: BLE001
            rt.logger.error("start_failed", extra={"error": f"{type(e).__name__}: {e}"})
            fatal.append("start")
            stop.set()
    rt.logger.info("serving", extra={"port": rt.port, "ports": json.dumps(rt.extra_ports)})
    await stop.wait()
    await _shutdown(rt, module, servers)
    return EX_FATAL if fatal else EX_OK


def _signals() -> asyncio.Event:
    ev = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, ev.set)
    return ev


async def _open_ports(rt: Runtime, module: Module) -> list[Any]:
    """P1.2 step 2: the main port, and the gRPC port when the module registers services."""
    app = rt.http_app(module)
    http = HttpServer(app, listen(rt.port), rt.logger, grace=rt.shutdown_grace)
    await http.start()
    servers: list[Any] = [http]
    if module.grpc is not None:
        from besdk.rpc.server import GrpcServer

        g = GrpcServer(rt, module.grpc)
        await g.start()
        servers.append(g)
    return servers


def _background(rt: Runtime, module: Module, stop: asyncio.Event, fatal: list[str]) -> None:
    """P1.2 step 3: everything that connects runs in the background, supervised."""
    src = rt.shared.bundle_source
    if rt.protected_routes and src is not None:
        rt.supervisor.start("be.authz.bundle", src.run)
    if rt.config.secrets():
        rt.supervisor.start("be.secrets", lambda: _poll_secrets(rt))
    if "PG_SCHEMA" in rt.config.declared():
        rt.readiness.need("db_identity")
        rt.readiness.need("migrations")
        rt.supervisor.start("be.db", lambda: _db_ready(rt, stop, fatal))
    else:
        asyncio.get_running_loop().create_task(rt.jobs.start())
    asyncio.get_running_loop().create_task(rt.start_events(module))


async def _poll_secrets(rt: Runtime) -> None:
    while True:
        await asyncio.sleep(SECRET_POLL)
        for s in rt.config.secrets():
            s.poll()


async def _db_ready(rt: Runtime, stop: asyncio.Event, fatal: list[str]) -> None:
    """Connect with 0.5 s → 15 s backoff, run the probe (P10.7) and the migration check (P1.4, P1.8)."""
    from besdk.store.store import CapabilityMissing

    store, delay = rt.store(), 0.5
    while True:
        try:
            problems = await store.probe()
            behind, newer = await _migration_state(rt)
        except CapabilityMissing as e:
            rt.logger.error("database_capability_missing", extra={"error": str(e)})
            fatal.append("capability")
            stop.set()
            return
        except Exception as e:  # noqa: BLE001 - not reachable yet
            rt.logger.warning("database_unreachable", extra={"error": f"{type(e).__name__}: {e}", "retry_in_s": delay})
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15.0)
            continue
        if newer:
            rt.logger.error("schema_newer_than_image", extra={"migrations": ",".join(newer)})
            fatal.append("schema")
            stop.set()
            return
        rt.metrics.db_identity_ok.set(0 if problems else 1)
        for p in problems:
            rt.logger.error("database_identity_problem", extra={"error": p})
        if not problems:
            rt.readiness.mark("db_identity")
        if not behind:
            rt.readiness.mark("migrations")
        if not problems and not behind:
            await rt.jobs.start()  # background work runs on a migrated schema only
            return
        await asyncio.sleep(2.0)


async def _migration_state(rt: Runtime) -> tuple[list[str], list[str]]:
    """(missing in the schema, applied but unknown to this image)."""
    from besdk.migrate import AUTHZ_ID, PLATFORM_IDS, component_ids, platform_ids

    mine = component_ids(Path(rt.spec.migrations)) + platform_ids(Path(rt.spec.migrations).parent)

    async def read(tx):
        if not await tx.fetchval("SELECT to_regclass('_yoyo_migration') IS NOT NULL"):
            return []
        return [r[0] for r in await tx.fetch("SELECT migration_id FROM _yoyo_migration")]

    done = await rt.store().tx(read)
    behind = [i for i in mine if i not in done]
    newer = [i for i in done if i not in mine and not i.startswith("besdk-")]
    newer += [i for i in done if i.startswith("besdk-") and i not in (*PLATFORM_IDS.values(), AUTHZ_ID)]
    return behind, newer


async def _shutdown(rt: Runtime, module: Module, servers: list[Any]) -> None:
    """P1.6: stop accepting and finish in-flight requests within SHUTDOWN_GRACE, then background work."""
    rt.logger.info("stopping")
    await asyncio.gather(*(s.stop(rt.shutdown_grace) for s in servers), return_exceptions=True)
    await rt.stop_events()
    await rt.supervisor.stop(timeout=5.0)
    if rt.jobs is not None and "PG_SCHEMA" in rt.config.declared():
        await rt.jobs.stop()
    if module.stop is not None:
        try:
            await asyncio.wait_for(module.stop(), 10.0)
        except Exception as e:  # noqa: BLE001
            rt.logger.warning("stop_failed", extra={"error": f"{type(e).__name__}: {e}"})
    if rt._outbound is not None:  # noqa: SLF001
        await rt.outbound().close()
    rt.telemetry.shutdown()
    await rt.shared.close()
    rt.logger.info("stopped")
