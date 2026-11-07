"""What a component hands the launcher or the shell: its identity, its declared keys and the one module entry, used
the same way when it runs alone and when a shell hosts it."""
from __future__ import annotations

from dataclasses import dataclass, field
from importlib.resources.abc import Traversable
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, runtime_checkable

from besdk.config import Key

if TYPE_CHECKING:
    from besdk.http import Routes
    from besdk.runtime import Runtime


class Module(Protocol):
    """A built component: it registers its user-surface routes."""

    def routes(self, r: Routes) -> None: ...


@runtime_checkable
class Starter(Protocol):
    """Modules with background work; start returns once that work runs, before the member reports ready."""

    async def start(self) -> None: ...


@runtime_checkable
class Stopper(Protocol):
    """Modules that must finish something on stop; called within the stop deadline."""

    async def stop(self) -> None: ...


@dataclass
class Definition:
    """Describes a component. Build it in the component's package and pass it to launcher.main (single run) or list
    it in a shell. new must not read the process environment, start listeners, initialise process-wide state or exit
    the process: everything comes from the runtime."""

    id: str  # component ID, e.g. infra/print
    version: str  # the version this code is; checked against COMPONENT_VERSION at start
    new: Callable[[Runtime], Module | Awaitable[Module]]
    keys: list[Key] = field(default_factory=list)
    capabilities: list[str] = field(default_factory=list)
    # The component's migration files (<version>_<title>.up.sql); set means it owns a database.
    migrations: Traversable | None = None
    grpc_port: int = 0  # the "grpc" extra port of component.yaml
    # The event streams the component produces (chapter 06); they need migrations (the outbox). rt.events emits.
    streams: list = field(default_factory=list)
    # The operation of the streams' redelivery, be.source.v1.ResendService (P6.38): required with streams, declared in
    # authz; any component of the deployment may call it.
    resend: str = ""
    # The component writes tightening intents or irreversible commands' records (P12.16): the alert channel (the
    # run profile, RUN_PROFILE_*) takes an intent the evidence store does not; every member with migrations writes
    # evidence through rt.evidence, without this flag a failed intent is only recorded as a double failure.
    evidence: bool = False
    # The operation of the evidence replay, be.evidence.v1.ReplayService (P12.19): required when the module
    # implements replays(r: besdk.evidence.Replays), declared in the component's resource definition.
    replay: str = ""
    # The module implements external_commands() -> (adapter, {operation: besdk.extcmd.Capabilities}) (be-protocol
    # chapter 13): it sends external commands through rt.external; needs migrations.
    external_commands: bool = False
    # The operation of a full recovery's checks (be.recovery.v1.CheckService, chapter 14) and, with
    # external_commands, of the external reconciliation (be.external.v1.ReconcileService, P13.11), declared in the
    # component's resource definition.
    recovery: str = ""
    # The module implements subscriptions(s: besdk.events.consumer.Subscriptions); needs migrations (the inbox).
    subscribes: bool = False
    # The resources and operations the component declares (besdk.authz.Definitions); required with protected routes or
    # system methods, which each name one of its operations (P8.7).
    authz: Any = None
