"""The only door a module has to the outside: its member identity, configuration, dependency addresses, logger,
metrics and tracer (component implementation conventions: "the runtime is the only entry"). The launcher (single
run) or the shell builds one Runtime per member; modules only read it."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from opentelemetry.trace import Tracer
from prometheus_client import CollectorRegistry

from besdk.config import Source, Values
from besdk.obs import Logger, Member


def env_prefix(component_id: str) -> str:
    """A component ID as BrickKit's variable prefix: "/" and "-" become "_", upper case."""
    return component_id.replace("/", "_").replace("-", "_").upper()


@dataclass(frozen=True)
class Endpoint:
    """A dependency's address as BrickKit derived it (http://<versioned-service>:<port>)."""

    url: str
    host: str  # host:port, for dialling gRPC


@dataclass
class Runtime:
    member: Member
    config: Values
    logger: Logger
    metrics: CollectorRegistry
    tracer: Tracer
    addresses: Source | None = None  # where the platform's *_ENDPOINT values are looked up
    db: Any = None  # the member's database (besdk.db), None without migrations
    outbound: Any = None  # the member's call clients (besdk.calls.client.Member)
    events: Any = None  # writes the member's events into its outbox inside a transaction (besdk.events.outbox.Producer)

    def http(self, component_id: str) -> Any:
        """The client for a dependency's user surface (deadline, retries, breaker and limit per dependency, chapter
        04); None: the dependency is absent."""
        ep = self.dependency(component_id)
        return None if ep is None or self.outbound is None else self.outbound.http(component_id, ep.url)

    def grpc(self, component_id: str) -> Any:
        """The channel to a dependency's system surface (its "grpc" port) for generated stubs; mark idempotent
        methods and commands' keys with besdk.calls.client.options(...). None: the dependency is absent."""
        ep = self.dependency_port(component_id, "grpc")
        return None if ep is None or self.outbound is None else self.outbound.grpc(component_id, ep.host)

    def dependency(self, component_id: str) -> Endpoint | None:
        """The address of a dependency's main port; None means the dependency is absent (an optional one that is not
        installed): degrade as the boundary card says. It is never an empty string."""
        return self._lookup(env_prefix(component_id) + "_ENDPOINT")

    def dependency_port(self, component_id: str, port: str) -> Endpoint | None:
        """The address of a named extra port of a dependency (e.g. its "grpc" system port)."""
        return self._lookup(env_prefix(component_id) + "_" + env_prefix(port) + "_ENDPOINT")

    def _lookup(self, name: str) -> Endpoint | None:
        raw = self.addresses.lookup(name) if self.addresses else None
        if not raw:
            return None
        parts = urlsplit(raw)
        return Endpoint(raw, parts.netloc or raw)
