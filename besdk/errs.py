"""The one error object of protocol chapter 03: a reason with its domain, gRPC code and HTTP status.

Handlers raise Error (any other exception becomes INTERNAL); the HTTP and gRPC layers render it.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import math
from dataclasses import dataclass, field
from enum import StrEnum


class Code(StrEnum):
    """The gRPC status code names the protocol uses."""

    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    FAILED_PRECONDITION = "FAILED_PRECONDITION"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    NOT_FOUND = "NOT_FOUND"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    ABORTED = "ABORTED"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    CANCELLED = "CANCELLED"
    INTERNAL = "INTERNAL"
    UNKNOWN = "UNKNOWN"
    DATA_LOSS = "DATA_LOSS"
    UNIMPLEMENTED = "UNIMPLEMENTED"
    UNAVAILABLE = "UNAVAILABLE"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"


_CODE_HTTP = {
    Code.INVALID_ARGUMENT: 400, Code.FAILED_PRECONDITION: 400, Code.OUT_OF_RANGE: 400, Code.UNAUTHENTICATED: 401,
    Code.PERMISSION_DENIED: 403, Code.NOT_FOUND: 404, Code.ALREADY_EXISTS: 409, Code.ABORTED: 409,
    Code.RESOURCE_EXHAUSTED: 429, Code.CANCELLED: 499, Code.INTERNAL: 500, Code.UNKNOWN: 500, Code.DATA_LOSS: 500,
    Code.UNIMPLEMENTED: 501, Code.UNAVAILABLE: 503, Code.DEADLINE_EXCEEDED: 504,
}


def http_status(code: str) -> int:
    """The HTTP status of a gRPC code (the P3 code table); unknown codes are 500."""
    return _CODE_HTTP.get(code, 500)  # type: ignore[call-overload]


def log_level(code: str) -> int | None:
    """The level an error of this code is logged at (P3.9); None: not logged as an error (CANCELLED)."""
    if code in (Code.INTERNAL, Code.UNKNOWN, Code.DATA_LOSS):
        return logging.ERROR
    if code in (Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED):
        return logging.WARNING
    if code == Code.CANCELLED:
        return None
    return logging.INFO


def access_log_level(status: int) -> int:
    """The level of a request's access log line by HTTP status (P3.9)."""
    return {500: logging.ERROR, 503: logging.WARNING, 504: logging.WARNING}.get(status, logging.INFO)


PLATFORM_DOMAIN = "be"
INTERNAL_DETAIL = "an internal error occurred; quote the trace id when reporting it"

# Retryability of a reason, as the catalog states it.
RETRY_NO, RETRY_YES, RETRY_AFTER_REFRESH, RETRY_QUERY_FIRST, RETRY_NONE = (
    "no", "yes", "after_refresh", "query_first", "none")


@dataclass(frozen=True)
class Entry:
    reason: str
    code: Code
    http: int  # 0: by the code
    retry: str
    title: str

    @property
    def status(self) -> int:
        return self.http or http_status(self.code)


_C = Code
PLATFORM: dict[str, Entry] = {e.reason: e for e in [
    Entry("REQUEST_INVALID", _C.INVALID_ARGUMENT, 0, RETRY_NO, "The request is not valid"),
    Entry("BODY_TOO_LARGE", _C.INVALID_ARGUMENT, 413, RETRY_NO, "The request body is too large"),
    Entry("CURSOR_INVALID", _C.INVALID_ARGUMENT, 0, RETRY_NO, "The cursor does not match the request"),
    Entry("BATCH_TOO_LARGE", _C.INVALID_ARGUMENT, 0, RETRY_NO, "Too many identities in one batch"),
    Entry("TOKEN_INVALID", _C.UNAUTHENTICATED, 0, RETRY_NO, "The credentials are not valid"),
    Entry("TOKEN_STALE", _C.UNAUTHENTICATED, 0, RETRY_AFTER_REFRESH, "The credentials are out of date"),
    Entry("SERVICE_CREDENTIAL_INVALID", _C.UNAUTHENTICATED, 0, RETRY_NO, "The calling service is not authenticated"),
    Entry("PERMISSION_DENIED", _C.PERMISSION_DENIED, 0, RETRY_NO, "Not allowed"),
    Entry("CONTAINMENT_NOT_PROVEN", _C.PERMISSION_DENIED, 0, RETRY_NO, "The change exceeds what can be proven"),
    Entry("NOT_FOUND", _C.NOT_FOUND, 0, RETRY_NO, "Not found"),
    Entry("QUERY_NOT_COMPLETABLE", _C.FAILED_PRECONDITION, 422, RETRY_NO, "The query cannot be completed"),
    Entry("FIELD_NOT_QUERYABLE", _C.FAILED_PRECONDITION, 422, RETRY_NO, "A field cannot be used in this query"),
    Entry("QUERY_PLAN_CHANGED", _C.ABORTED, 0, RETRY_NO, "The query changed; run it again"),
    Entry("CHANGE_KEY_CONFLICT", _C.ALREADY_EXISTS, 0, RETRY_NO, "The change key was used for different content"),
    Entry("CHANGE_REVISION_STALE", _C.ABORTED, 0, RETRY_NO, "The target changed; preview again"),
    Entry("LAST_ADMIN_PROTECTED", _C.FAILED_PRECONDITION, 409, RETRY_NO, "The last administrator is protected"),
    Entry("IDEMPOTENCY_KEY_REUSED", _C.INVALID_ARGUMENT, 422, RETRY_NO, "The idempotency key was used for another request"),
    Entry("IDEMPOTENCY_IN_PROGRESS", _C.ABORTED, 0, RETRY_YES, "The first use of this idempotency key has not finished"),
    Entry("LOCK_TIMEOUT", _C.ABORTED, 0, RETRY_YES, "Waiting for a lock took too long"),
    Entry("TX_CONFLICT", _C.ABORTED, 0, RETRY_YES, "A concurrent change conflicted"),
    Entry("STATEMENT_TIMEOUT", _C.DEADLINE_EXCEEDED, 0, RETRY_YES, "The database took too long"),
    Entry("SERVICE_BUSY", _C.UNAVAILABLE, 0, RETRY_YES, "The service is busy"),
    Entry("OUTBOUND_LIMIT", _C.RESOURCE_EXHAUSTED, 503, RETRY_YES, "Too many calls to a dependency are in progress"),
    Entry("CIRCUIT_OPEN", _C.UNAVAILABLE, 0, RETRY_YES, "A dependency is failing; calls to it are paused"),
    Entry("DEPENDENCY_UNAVAILABLE", _C.UNAVAILABLE, 0, RETRY_YES, "A dependency is unavailable"),
    Entry("DEADLINE_BUDGET_EXHAUSTED", _C.DEADLINE_EXCEEDED, 0, RETRY_NO, "Not enough time was left to make a call"),
    Entry("DEADLINE_EXCEEDED", _C.DEADLINE_EXCEEDED, 0, RETRY_QUERY_FIRST, "The request ran out of time"),
    Entry("REQUEST_CANCELLED", _C.CANCELLED, 0, RETRY_NONE, "The request was cancelled"),
    Entry("NOT_READY", _C.UNAVAILABLE, 0, RETRY_YES, "The service is not ready"),
    Entry("AUTHORIZATION_UNPROVABLE", _C.UNAVAILABLE, 0, RETRY_YES, "Authorization cannot be proven right now"),
    Entry("AUTHORIZATION_VIEW_EXPIRED", _C.UNAVAILABLE, 0, RETRY_YES, "The authorization view has expired"),
    Entry("RECOVERY_IN_PROGRESS", _C.UNAVAILABLE, 0, RETRY_YES, "The deployment is being recovered"),
    Entry("MEMBER_SWITCH_IN_PROGRESS", _C.UNAVAILABLE, 0, RETRY_YES, "A provider switch is in progress"),
    Entry("CAPABILITY_UNSUPPORTED", _C.UNIMPLEMENTED, 0, RETRY_NO, "This capability is not supported"),
    Entry("RESULT_UNKNOWN", _C.DEADLINE_EXCEEDED, 0, RETRY_QUERY_FIRST, "The result is not known yet; query it"),
    Entry("AUDIT_CAPACITY_EXHAUSTED", _C.UNAVAILABLE, 0, RETRY_YES,
          "Audited operations are paused until the audit backlog drains"),
    Entry("INTERNAL", _C.INTERNAL, 0, RETRY_NO, "Internal error"),
]}


@dataclass(frozen=True)
class Violation:
    """One field error of REQUEST_INVALID."""

    field: str
    reason: str
    description: str


@dataclass
class Error(Exception):
    """The error object. Build platform errors with new(), component errors with define(...).new()."""

    reason: str
    domain: str
    code: Code
    http: int
    title: str
    detail: str
    metadata: dict[str, str] = field(default_factory=dict)
    violations: list[Violation] = field(default_factory=list)
    retry_after: float = 0.0  # seconds
    cause: BaseException | None = None  # logged, never sent

    def __str__(self) -> str:
        s = f"{self.domain}/{self.reason}: {self.detail}"
        return f"{s}: {self.cause}" if self.cause is not None else s

    def __hash__(self) -> int:
        return id(self)

    def _clone(self) -> Error:
        c = copy.copy(self)
        c.metadata, c.violations = dict(self.metadata), list(self.violations)
        return c

    def with_(self, key: str, value: str) -> Error:
        """A copy with one metadata entry added (values are strings, AIP-193)."""
        c = self._clone()
        c.metadata[key] = value
        return c

    def with_violations(self, *v: Violation) -> Error:
        c = self._clone()
        c.violations.extend(v)
        return c

    def with_retry_after(self, seconds: float) -> Error:
        c = self._clone()
        c.retry_after = seconds
        return c

    def wrap(self, cause: BaseException) -> Error:
        """A copy keeping cause for the logs; the cause never reaches the caller."""
        c = self._clone()
        c.cause = cause
        return c

    def is_(self, domain: str, reason: str) -> bool:
        return self.domain == domain and self.reason == reason

    @property
    def internal(self) -> bool:
        return self.code in (Code.INTERNAL, Code.UNKNOWN, Code.DATA_LOSS)

    def problem(self, trace_id: str) -> dict:
        """The user-surface body (problem+json). INTERNAL never carries anything but the generic detail (P3.4)."""
        if self.internal:
            return {"type": "urn:be:be:INTERNAL", "title": PLATFORM["INTERNAL"].title, "status": 500,
                    "detail": INTERNAL_DETAIL, "reason": "INTERNAL", "domain": PLATFORM_DOMAIN, "trace_id": trace_id}
        title = self.title or self.reason
        p = {"type": f"urn:be:{self.domain}:{self.reason}", "title": title, "status": self.http,
             "detail": self.detail or title, "reason": self.reason, "domain": self.domain, "trace_id": trace_id}
        if self.metadata:
            p["metadata"] = dict(self.metadata)
        if self.violations:
            p["violations"] = [v.__dict__ for v in self.violations]
        return p

    def retry_after_header(self) -> str | None:
        """Retry-After in whole seconds, rounded up (P3.6)."""
        return str(math.ceil(self.retry_after)) if self.retry_after > 0 else None


def is_(err: BaseException | None, domain: str, reason: str) -> bool:
    return isinstance(err, Error) and err.is_(domain, reason)


def new(reason: str, detail: str = "") -> Error:
    """A platform error (domain "be"). An unknown reason is a programming error: INTERNAL, keeping the bad reason in
    the cause so it shows in the logs and tests rather than on the wire."""
    e = PLATFORM.get(reason)
    if e is None:
        i = PLATFORM["INTERNAL"]
        return Error(i.reason, PLATFORM_DOMAIN, i.code, i.status, i.title, INTERNAL_DETAIL,
                     cause=ValueError(f"errs.new called with a reason outside the catalog: {reason}"))
    return Error(e.reason, PLATFORM_DOMAIN, e.code, e.status, e.title, detail)


@dataclass(frozen=True)
class Declared:
    """A component's own reason, declared in its contract (P3.7)."""

    domain: str
    entry: Entry

    def new(self, detail: str = "") -> Error:
        e = self.entry
        return Error(e.reason, self.domain, e.code, e.status, e.title, detail)


def define(domain: str, reason: str, code: Code, http: int, retry: str, title: str) -> Declared:
    return Declared(domain, Entry(reason, code, http, retry, title))


def from_exception(err: BaseException | None) -> Error | None:
    """Any exception as the error object: Error as is, cancellation and timeouts as REQUEST_CANCELLED and
    DEADLINE_EXCEEDED, anything else INTERNAL with the original kept as the cause."""
    if err is None:
        return None
    if isinstance(err, Error):
        return err
    if isinstance(err, asyncio.CancelledError):
        return new("REQUEST_CANCELLED", "the request was cancelled").wrap(err)
    if isinstance(err, TimeoutError):
        return new("DEADLINE_EXCEEDED", "the request ran out of time").wrap(err)
    return new("INTERNAL", INTERNAL_DETAIL).wrap(err)
