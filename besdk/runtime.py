"""``Spec``, ``Module`` and ``Runtime`` (apis §2.1, §2.2, §3): what a component declares and what it gets.

A ``Runtime`` is one member: its configuration, logger, metrics registry, tracer and meter, store,
connections and supervised background work. ``Shared`` holds the four process-wide things of P19.3
(telemetry exporter and propagator, token verifier and bundle, the physical pool, the bus connection);
standalone, the process has exactly one member.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Sequence, TextIO

import httpx
import yaml

from besdk import errors, logs, metrics, telemetry
from besdk.auth.access import Authorizer
from besdk.auth.jwt import JwksCache, Verifier
from besdk.auth.source import BundleSource
from besdk.config import Config, Manifest
from besdk.events.model import Events
from besdk.supervise import Supervisor

if TYPE_CHECKING:
    from besdk.http.router import Router


@dataclass(frozen=True)
class Spec:
    """The component's declaration; ``main(spec)`` runs it standalone, a shell imports the same Spec."""

    id: str
    migrations: Path
    contracts: Path
    create: Callable[["Runtime"], Awaitable["Module"]]
    manifest: Path | None = None  # default: <contracts>/../component.yaml
    error_domain: str | None = None  # a slot-family member answers with its family's ID (P4.1)

    def manifest_path(self) -> Path:
        return Path(self.manifest) if self.manifest else Path(self.contracts).parent / "component.yaml"


@dataclass
class Module:
    """What ``create`` returns: declarations only; the SDK owns servers, loops and connections."""

    http: Callable[["Router"], None] | None = None
    grpc: Callable[[Any], None] | None = None
    events: Events = field(default_factory=Events)
    jobs: Sequence[Any] = ()
    workers: Sequence[Any] = ()
    reconcilers: Sequence[Any] = ()
    snapshots: Sequence[Any] = ()
    sharing: Sequence[Any] = ()
    lifecycle: Any = None
    start: Callable[[], Awaitable[None]] | None = None
    stop: Callable[[], Awaitable[None]] | None = None


class Shared:
    """The process-wide half (P19.3). Created once per process by ``main`` or the shell launcher."""

    def __init__(self, platform: telemetry.Platform, http: httpx.AsyncClient):
        self.platform = platform
        self.http = http  # JWKS and bundle fetches
        self.verifier: Verifier | None = None
        self.bundle_source: BundleSource | None = None
        self.pool: Any = None  # besdk.store.pool.PhysicalPool
        self.bus: Any = None  # besdk.events.bus.Bus

    @classmethod
    def standalone(cls, *, spec_id: str, otel_base_url: str = "",
                   http_transport: httpx.AsyncBaseTransport | None = None) -> "Shared":
        platform = telemetry.Platform(spec_id, exporter=telemetry.otlp_exporter(otel_base_url))
        return cls(platform, httpx.AsyncClient(transport=http_transport))

    def ensure_auth(self, config: Config, logger: logging.Logger) -> None:
        if self.verifier is None:
            jwks = JwksCache(config.family("IAM_URL") or "", self.http)
            self.verifier = Verifier(issuer=config.require("IAM_ISSUER"), tenant=config.require("TENANT_ID"),
                                     jwks=jwks)
            self.bundle_source = BundleSource(config.family("AUTHZ_URL") or "", self.http, logger)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
        if self.bus is not None:
            await self.bus.close()
        await self.http.aclose()
        self.platform.shutdown()


class Readiness:
    """``/readyz`` (P1.4): the conditions that apply to this member; once met, latched."""

    def __init__(self) -> None:
        self.required: list[str] = []
        self.met: set[str] = set()

    def need(self, name: str) -> None:
        if name not in self.required:
            self.required.append(name)

    def mark(self, name: str) -> None:
        self.met.add(name)

    def refresh(self, rt: "Runtime") -> None:
        src = rt.shared.bundle_source
        if "bundle" in self.required and src is not None and src.bundle is not None:
            self.met.add("bundle")

    def waiting(self) -> list[str]:
        return [n for n in self.required if n not in self.met]


class Runtime:
    """One member's runtime. Components use only its public methods and attributes."""

    def __init__(self, spec: Spec, env: dict[str, str], shared: Shared, *, log_stream: TextIO | None = None,
                 port: int | None = None, extra_ports: dict[str, int] | None = None):
        self.spec = spec
        self.manifest = Manifest.load(spec.manifest_path())
        self.config = Config.load(env, self.manifest)
        self.id = self.manifest.id
        self.version = env.get("COMPONENT_VERSION") or self.manifest.version
        self.shared = shared
        self.logger = logs.member_logger(self.id, self.version, level=self._proto("LOG_LEVEL", "info"),
                                         stream=log_stream)
        self.registry = metrics.ComponentRegistry(self.id)
        self.metrics = metrics.BeMetrics(self.registry)
        self.telemetry = shared.platform.member(self.id, self.version, self.registry)
        self.tracer, self.meter = self.telemetry.tracer, self.telemetry.meter
        self.error_domain = spec.error_domain or self.id
        self.catalog = errors.Catalog.with_file(Path(spec.contracts) / "errors.yaml", self.error_domain)
        self.locale = self._proto("DEFAULT_LOCALE", "zh-CN")
        self.http_default_timeout = self._seconds("HTTP_DEFAULT_TIMEOUT", 10.0)
        self.shutdown_grace = self._seconds("SHUTDOWN_GRACE", 25.0)
        self.port = port if port is not None else int(env.get("PORT") or self.manifest.port)
        self.extra_ports = dict(extra_ports if extra_ports is not None else self.manifest.extra_ports)
        self.supervisor = Supervisor(self.logger)
        self.readiness = Readiness()
        self.protected_routes = False
        self._store: Any = None
        self._outbound: Any = None
        self._wire_secrets()
        if self._declared("AUTHZ_URL") and self._declared("IAM_URL"):
            shared.ensure_auth(self.config, self.logger)

    # --- configuration helpers -------------------------------------------------------------------

    def _declared(self, key: str) -> bool:
        return key in self.config.declared()

    def _proto(self, key: str, default: str) -> str:
        return self.config.string(key, default) if self._declared(key) else default

    def _seconds(self, key: str, default: float) -> float:
        return self.config.duration(key, default) if self._declared(key) else default

    def _wire_secrets(self) -> None:
        for s in self.config.secrets():
            s.on_reload = lambda key: self.logger.info("secret_reloaded", extra={"key": key})
            s.on_failure = self._secret_failed

    def _secret_failed(self, key: str, err: Exception) -> None:
        self.logger.error("secret_reload_failed", extra={"key": key, "error": type(err).__name__})
        self.metrics.secret_reload_failures.labels(key=key).inc()

    # --- the component's API ---------------------------------------------------------------------

    def authorizer(self) -> Authorizer:
        if self.shared.verifier is None or self.shared.bundle_source is None:
            raise errors.internal("protected route without AUTHZ_URL / IAM_URL in configSchema")
        return Authorizer(self.shared.verifier, self.shared.bundle_source)

    def store(self) -> Any:
        """The database store bound to this member's identity (P10); no PG_SCHEMA → error."""
        if self._store is None:
            if not self._declared("PG_SCHEMA"):
                raise errors.internal("no database: PG_SCHEMA is not declared")
            from besdk.store.store import Store

            self._store = Store.for_runtime(self)
        return self._store

    def outbound(self) -> Any:
        if self._outbound is None:
            from besdk.outbound import Outbound

            self._outbound = Outbound(self)
        return self._outbound

    def conn(self, dep: str, port: str = "grpc") -> Any:
        """A cached gRPC channel to a dependency's port (P7.6); absent optional dependency → None."""
        return self.outbound().conn(dep, port)

    def user_http(self, dep: str) -> Any:
        return self.outbound().user_http(dep)

    def external_http(self, name: str, *, timeout: float = 10.0, max_conns: int = 32) -> httpx.AsyncClient:
        return self.outbound().external_http(name, timeout=timeout, max_conns=max_conns)

    def capabilities(self) -> dict[str, Any]:
        src = self.shared.bundle_source
        return dict(src.bundle.raw.get("capabilities", {})) if src and src.bundle else {}

    def http_app(self, module: Module) -> Any:
        from besdk.http.app import build_app

        app = build_app(self, module)
        if self.protected_routes:
            self.readiness.need("bundle")
        return app

    # --- self-description ------------------------------------------------------------------------

    def profiles(self, module: Module) -> list[str]:
        """The profiles the suite selects from the manifests (conformance-cases.yaml)."""
        m, out = self.manifest, ["core", "obs", "err"]
        if self.protected_routes:
            out.append("auth")
        if _scoped(self.spec):
            out.append("scope")
        if "grpc" in m.extra_ports:
            out.append("grpc")
        if m.dependencies:
            out.append("outbound")
        ev = m.doc.get("events") or {}
        out += ["events-pub"] if ev.get("publishes") else []
        out += ["events-sub"] if ev.get("subscribes") else []
        if self._declared("PG_SCHEMA"):
            out += ["db", "jobs", "lifecycle"]
        if self._declared("S3_BUCKET"):
            out.append("blob")
        return out

    def migration_info(self) -> dict[str, Any]:
        if not self._declared("PG_SCHEMA"):
            return {"component": None, "platform": None}
        from besdk.migrate import PLATFORM_VERSION, image_component_version

        return {"component": image_component_version(Path(self.spec.migrations)), "platform": PLATFORM_VERSION}


def _scoped(spec: Spec) -> bool:
    p = spec.manifest_path().parent / "assembly.yaml"
    if not p.exists():
        return False
    doc = yaml.safe_load(p.read_text()) or {}
    return doc.get("data_scopes", "none") != "none" or bool(doc.get("resources"))
