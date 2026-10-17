"""How one attempt counts (vector call-outcomes) and what the caller answers upward when it was the last (P4.18,
P4.19)."""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from besdk import errs
from besdk.calls import policy


@dataclass
class Result:
    """What one attempt returned, as the transport saw it."""

    err: BaseException | None = None  # None on success
    code: str = "OK"  # the answer's code
    remote: errs.Error | None = None  # the protocol error the dependency sent, None when it sent none
    connect_failed: bool = False  # the connection failed before the request was sent


class Class(StrEnum):
    SUCCESS = "success"
    UNAVAILABLE = "unavailable"
    CONNECT_FAILED = "connect_failed"
    DEADLINE = "deadline"
    RELAYED = "relayed"
    OTHER = "other"
    CANCELLED = "cancelled"

    @property
    def breaker(self) -> str:
        if self == Class.SUCCESS:
            return policy.BREAKER_SUCCESS
        if self in (Class.UNAVAILABLE, Class.CONNECT_FAILED, Class.DEADLINE):
            return policy.BREAKER_FAILURE
        return policy.BREAKER_IGNORED

    @property
    def budget(self) -> str:
        if self == Class.SUCCESS:
            return policy.BUDGET_SUCCESS
        if self in (Class.UNAVAILABLE, Class.CONNECT_FAILED):
            return policy.BUDGET_UNAVAILABLE
        return policy.BUDGET_OTHER

    @property
    def retry_outcome(self) -> str:
        return {Class.UNAVAILABLE: policy.OUTCOME_UNAVAILABLE,
                Class.CONNECT_FAILED: policy.OUTCOME_CONNECT_FAILED}.get(self, policy.OUTCOME_NONE)


# the platform reasons a dependency uses to report failures further down (P4.18)
_RELAYED = {"DEPENDENCY_UNAVAILABLE", "CIRCUIT_OPEN", "DEADLINE_BUDGET_EXHAUSTED", "RESULT_UNKNOWN"}


def classify(r: Result, caller_cancelled: bool) -> Class:
    if caller_cancelled and r.err is not None:
        return Class.CANCELLED
    if r.connect_failed:
        return Class.CONNECT_FAILED
    if r.err is None:
        return Class.SUCCESS
    if r.remote is not None and r.remote.domain == errs.PLATFORM_DOMAIN and r.remote.reason in _RELAYED:
        return Class.RELAYED
    if r.code == errs.Code.UNAVAILABLE:
        return Class.UNAVAILABLE
    if r.code == errs.Code.DEADLINE_EXCEEDED:
        return Class.DEADLINE
    return Class.OTHER


def upward(c: Class, r: Result, idempotent: bool, dependency: str) -> errs.Error | None:
    """The error the caller answers when this was the final attempt (P4.19)."""
    if c == Class.SUCCESS:
        return None
    if c in (Class.UNAVAILABLE, Class.CONNECT_FAILED):
        e = errs.new("DEPENDENCY_UNAVAILABLE", "a dependency is unavailable").with_("dependency", dependency)
        if r.remote is not None and r.remote.retry_after > 0:
            e = e.with_retry_after(r.remote.retry_after)
        return e.wrap(r.err) if r.err else e
    if c == Class.DEADLINE:
        if idempotent:
            e = errs.new("DEADLINE_EXCEEDED", "a dependency did not answer in time")
        else:
            e = errs.new("RESULT_UNKNOWN", "a dependency did not answer in time; query the result before repeating")
        e = e.with_("dependency", dependency)
        return e.wrap(r.err) if r.err else e
    if c == Class.CANCELLED:
        return errs.new("REQUEST_CANCELLED", "the request was cancelled")
    if r.remote is not None:
        return r.remote
    return errs.from_exception(r.err or RuntimeError("calls: an attempt failed without an error"))
