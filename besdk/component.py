"""What a component hands the launcher or the shell: its identity, its declared keys and the one module entry, used
the same way when it runs alone and when a shell hosts it."""
from __future__ import annotations

from dataclasses import dataclass, field
from importlib.resources.abc import Traversable
from typing import TYPE_CHECKING, Awaitable, Callable, Protocol, runtime_checkable

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
