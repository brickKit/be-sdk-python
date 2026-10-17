"""Request-scoped values that outbound calls forward (protocol P4.2): the request ID, the caller's own credential (for
reads on the user's behalf) and the request's deadline. They live in context variables, so every task of a request
sees them and nothing leaks between requests."""
from __future__ import annotations

import contextlib
import contextvars
import time
from typing import Iterator

_request_id: contextvars.ContextVar[str] = contextvars.ContextVar("be_request_id", default="")
_user_credential: contextvars.ContextVar[str] = contextvars.ContextVar("be_user_credential", default="")
_deadline: contextvars.ContextVar[float | None] = contextvars.ContextVar("be_deadline", default=None)
# a one-element cell per scope, so a decision can move it earlier for the rest of the scope (and its subtasks)
_cutoff: contextvars.ContextVar[list[float] | None] = contextvars.ContextVar("be_cutoff", default=None)


def request_id() -> str:
    return _request_id.get()


def user_credential() -> str:
    return _user_credential.get()


def deadline() -> float | None:
    """The request's deadline on the monotonic clock (time.monotonic()), or None."""
    return _deadline.get()


def remaining() -> float | None:
    """Seconds left until the deadline (may be negative), or None without a deadline."""
    d = _deadline.get()
    return None if d is None else d - time.monotonic()


@contextlib.contextmanager
def request_scope(request_id: str, user_credential: str = "") -> Iterator[None]:
    """The values of one incoming request, reset when it ends (the SDK's request layers use it, never module code):
    nothing leaks into the next request, however the server schedules tasks. The deadline starts unset."""
    tokens = (_request_id.set(request_id), _user_credential.set(user_credential), _deadline.set(None))
    try:
        yield
    finally:
        _deadline.reset(tokens[2])
        _user_credential.reset(tokens[1])
        _request_id.reset(tokens[0])


@contextlib.contextmanager
def deadline_scope(deadline: float) -> Iterator[None]:
    """A deadline for the block, on the monotonic clock; it only ever shortens the one already in force."""
    current = _deadline.get()
    token = _deadline.set(deadline if current is None else min(current, deadline))
    try:
        yield
    finally:
        _deadline.reset(token)


def cutoff() -> float | None:
    """The request's authorization cutoff on the suspend-aware clock (besdk.clock.since_boot()), or None: write
    transactions check it once more before COMMIT (P8.12)."""
    cell = _cutoff.get()
    return None if cell is None else cell[0]


@contextlib.contextmanager
def cutoff_scope(at: float) -> Iterator[None]:
    """The cutoff for the block (the guard binds it); it only ever moves earlier."""
    current = cutoff()
    token = _cutoff.set([at if current is None else min(current, at)])
    try:
        yield
    finally:
        _cutoff.reset(token)


def tighten_cutoff(at: float) -> None:
    """Moves the cutoff of the enclosing scope earlier until the scope ends, e.g. when a record's decision used a
    shard with a shorter validity. Outside any scope there is no cutoff to move."""
    if (cell := _cutoff.get()) is not None:
        cell[0] = min(cell[0], at)
