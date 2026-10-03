"""The system plane, gRPC between components (be-protocol P7)."""

from dataclasses import dataclass
from typing import Any

from besdk import context


@dataclass(frozen=True)
class System:
    """The system principal of an inbound gRPC call (P7.3): recorded, never used to grant access."""

    caller: str
    actor_sub: str = ""
    act: Any = None


def system() -> System | None:
    return context.current().system


def caller_of() -> str:
    """``user:<sub>`` | ``svc:<caller>`` | ``system``."""
    u = context.current()
    if u.access is not None:
        return f"user:{u.access.user.sub}"
    if u.system is not None:
        return f"svc:{u.system.caller}"
    return "system"
