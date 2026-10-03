"""What travels with one unit of work (apis §1.2): deadline, request id, caller, access, the open
transaction, the event being handled. Python carries it in a ``contextvars.ContextVar``; the SDK sets it
at every entry (HTTP middleware, gRPC interceptor, event handler, job runner)."""

from __future__ import annotations

import contextlib
import time
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any, Iterator


@dataclass(frozen=True)
class HandledEvent:
    """The event a handler is processing: causation and hop count of what it publishes (P12.8)."""

    id: str
    hop_count: int
    subject: str = ""
    delivery: int = 0


@dataclass(frozen=True)
class Unit:
    request_id: str = ""
    deadline: float | None = None  # time.monotonic() value
    sub: str = ""
    act: Any = None
    perm: str = ""
    caller: str = ""  # be-caller on the system plane
    access: Any = None  # besdk.auth.Access
    system: Any = None  # besdk.System
    token: str = ""  # raw bearer token, private: only UserHTTP forwards it (P5.8)
    authz_revision: str = ""
    tx: Any = None  # the open Tx, if any (P8.4, P10.6)
    event: HandledEvent | None = None
    job: str = ""
    job_epoch: int = 0  # the singleton lease's fencing token while a singleton run holds it (P14)
    member: str = ""  # component ID of the member doing the work
    req: dict | None = None  # mutable facts of the current HTTP request, read by the access log


_unit: ContextVar[Unit] = ContextVar("besdk_unit", default=Unit())


def current() -> Unit:
    return _unit.get()


@contextlib.contextmanager
def scope(**changes: Any) -> Iterator[Unit]:
    """Run the block with these fields changed; restored afterwards."""
    token = _unit.set(replace(_unit.get(), **changes))
    try:
        yield _unit.get()
    finally:
        _unit.reset(token)


def remaining() -> float | None:
    """Seconds left until the unit's deadline (may be negative); None without a deadline."""
    d = _unit.get().deadline
    return None if d is None else d - time.monotonic()


def deadline_in(seconds: float) -> float:
    """An absolute deadline ``seconds`` from now, never later than the current one (P9.1)."""
    d = time.monotonic() + seconds
    cur = _unit.get().deadline
    return d if cur is None else min(d, cur)
