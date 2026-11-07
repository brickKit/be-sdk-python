"""The crash points of the long simulation (foundations item 6, T6; be-sdk-go's tiering/crash.go is the same idea): the
SDK's background state transitions call point(name) just before their transaction commits ("<name>/commit": a process
that dies there leaves the transaction open, so it rolls back) and after it committed ("<name>/done"). In production
point does nothing; the simulation's replicas set a hook that ends the process at a chosen point."""
from __future__ import annotations

from collections.abc import Callable

_hook: Callable[[str], None] | None = None


def set_hook(fn: Callable[[str], None] | None) -> None:
    """Sets the hook (the simulation's replicas only); None removes it."""
    global _hook
    _hook = fn


def point(name: str) -> None:
    if _hook is not None:
        _hook(name)
