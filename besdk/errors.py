"""The error model (be-protocol P4, P10.4): codes, the `be` reasons, problem+json, SQLSTATE classes.

Component code raises ``besdk.Error(code, reason, metadata, message)``; the SDK's HTTP and gRPC layers
map every exception through :func:`to_error` and :func:`problem`, so an unknown exception always leaves
the process as the generic ``INTERNAL`` body (P4.3).
"""

from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass, field
from enum import IntEnum
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Any, Mapping, Sequence

import yaml


class ProtocolError(ValueError):
    """A value that breaks a protocol rule; ``reason`` is the vector error class (CONFIG_MISSING, …)."""

    def __init__(self, reason: str, detail: str = "", *, key: str | None = None):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail
        self.key = key


class Code(IntEnum):
    """Canonical gRPC status codes."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16

    @classmethod
    def parse(cls, name: str) -> "Code":
        try:
            return cls[name]
        except KeyError:
            raise ProtocolError("CODE_UNKNOWN", f"{name!r} is not a canonical code name") from None


_HTTP = {
    Code.OK: 200, Code.CANCELLED: 499, Code.UNKNOWN: 500, Code.INVALID_ARGUMENT: 400,
    Code.DEADLINE_EXCEEDED: 504, Code.NOT_FOUND: 404, Code.ALREADY_EXISTS: 409, Code.PERMISSION_DENIED: 403,
    Code.RESOURCE_EXHAUSTED: 429, Code.FAILED_PRECONDITION: 400, Code.ABORTED: 409, Code.OUT_OF_RANGE: 400,
    Code.UNIMPLEMENTED: 501, Code.INTERNAL: 500, Code.UNAVAILABLE: 503, Code.DATA_LOSS: 500,
    Code.UNAUTHENTICATED: 401,
}
_HIDDEN = (Code.INTERNAL, Code.UNKNOWN, Code.DATA_LOSS)


def http_status(code: Code, reason: str | None = None, domain: str | None = None) -> int:
    """gRPC code → HTTP status (P4.2); the one exception is ``be``/``BODY_TOO_LARGE`` → 413."""
    if reason == "BODY_TOO_LARGE" and domain == "be":
        return 413
    return _HTTP[code]


def log_level(code: Code) -> str:
    """The level an error is logged at (P4.6): error, warn, info or none."""
    if code in _HIDDEN:
        return "error"
    if code in (Code.UNAVAILABLE, Code.DEADLINE_EXCEEDED):
        return "warn"
    if code in (Code.CANCELLED, Code.OK):
        return "none"
    return "info"


def access_log_level(code: Code) -> str:
    """The level of an access-log line by its outcome (P3.10, P4.6): every request is logged, so OK and
    CANCELLED are info; otherwise as ``log_level``."""
    return "info" if code in (Code.OK, Code.CANCELLED) else log_level(code)


@dataclass(frozen=True)
class Violation:
    field: str
    reason: str
    description: str = ""


class Error(Exception):
    """An error with a reason (P4). ``domain=None`` means the raising component's own domain."""

    def __init__(self, code: Code, reason: str | None, metadata: Mapping[str, str] | None = None,
                 message: str = "", *, domain: str | None = None, violations: Sequence[Violation] = (),
                 retry_after: float | None = None):
        super().__init__(message or reason or code.name)
        self.code = code
        self.reason = reason
        self.metadata = dict(metadata or {})
        self.message = message
        self.domain = domain
        self.violations = tuple(violations)
        self.retry_after = retry_after
        self.internal_message = ""

    @property
    def http(self) -> int:
        return http_status(self.code, self.reason, self.domain)

    def __repr__(self) -> str:
        return f"Error({self.code.name}, {self.domain}/{self.reason}, {self.metadata})"


# --- catalogues ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Entry:
    reason: str
    code: Code
    http: int
    params: tuple[str, ...]
    title: Mapping[str, str]
    message: Mapping[str, str]


class Catalog:
    """Reason catalogues by domain: ``be`` (shipped) plus a component's ``contracts/errors.yaml``."""

    def __init__(self, docs: Sequence[Mapping[str, Any]] = ()):
        self._by: dict[tuple[str, str], Entry] = {}
        for doc in (_be_doc(), *docs):
            for r in doc["reasons"]:
                self._by[(doc["domain"], r["reason"])] = Entry(
                    r["reason"], Code.parse(r["code"]), int(r["http"]), tuple(r.get("params", ())),
                    r["title"], r["message"])

    @classmethod
    def with_file(cls, path: Path | None, domain: str | None = None) -> "Catalog":
        """The ``be`` catalogue plus a component's errors.yaml; ``domain`` replaces the file's own."""
        if path is None or not path.exists():
            return cls()
        doc = yaml.safe_load(path.read_text())
        if domain:
            doc = {**doc, "domain": domain}
        return cls([doc])

    def get(self, domain: str | None, reason: str | None) -> Entry | None:
        return self._by.get((domain or "", reason or ""))


@cache
def _be_doc() -> dict:
    return yaml.safe_load(resources.files("besdk").joinpath("_protocol/errors-be.yaml").read_text())


@cache
def _default_catalog() -> Catalog:
    return Catalog()


def be_reason(reason: str) -> Entry:
    """The catalogue row of a reserved reason; unknown → ProtocolError REASON_UNKNOWN."""
    e = _default_catalog().get("be", reason)
    if e is None:
        raise ProtocolError("REASON_UNKNOWN", f"{reason} is not a be reason")
    return e


def be_error(reason: str, metadata: Mapping[str, str] | None = None, message: str = "", **kw) -> Error:
    """An error of domain ``be`` with the catalogue's code (P4.7: only catalogued reasons)."""
    return Error(be_reason(reason).code, reason, metadata, message, domain="be", **kw)


_REASON_RE = re.compile(r"[A-Z][A-Z0-9]*(_[A-Z0-9]+)*")


def check_reason_name(reason: str, domain: str) -> None:
    """P4.1 / P4.7: UPPER_SNAKE, and never a platform reason outside ``domain: be``."""
    if not _REASON_RE.fullmatch(reason):
        raise ProtocolError("REASON_NAME_INVALID", reason)
    if domain != "be" and _default_catalog().get("be", reason) is not None:
        raise ProtocolError("REASON_RESERVED", reason)


# --- conversion ----------------------------------------------------------------------------------


def internal(cause: BaseException | str) -> Error:
    """The generic INTERNAL error; ``cause`` is kept for the log only (P4.3)."""
    err = be_error("INTERNAL")
    err.internal_message = cause if isinstance(cause, str) else f"{type(cause).__name__}: {cause}"
    return err


def to_error(exc: BaseException) -> Error:
    """Any exception → an Error; unknown exceptions become INTERNAL with the original text kept for logs."""
    if isinstance(exc, Error):
        return exc
    if isinstance(exc, asyncio.CancelledError):
        return be_error("REQUEST_CANCELLED")
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return be_error("DEADLINE_BUDGET_EXHAUSTED")
    return internal(exc)


def from_parts(*, code: str | None, reason: str | None, domain: str | None,
               metadata: Mapping[str, Any] | None = None, violations: Sequence[Mapping[str, str]] | None = None,
               internal_message: str | None = None) -> Error:
    """Build an Error from wire-shaped parts; anything unclassified becomes INTERNAL (P4.3)."""
    if code is None:
        return internal(internal_message or "unclassified error")
    err = Error(Code.parse(code), reason, metadata, domain=domain,
                violations=[Violation(v["field"], v["reason"], v.get("description", "")) for v in violations or ()])
    err.metadata = dict(metadata or {})
    err.internal_message = internal_message or ""
    return err


def restore_http(status: int, problem: Mapping[str, Any] | None) -> Error:
    """A dependency's REST answer → the error the caller sees (P8.2, vectors errors/restore_http)."""
    p = problem or {}
    if p.get("code") in Code.__members__:
        err = Error(Code[p["code"]], p.get("reason"), _str_meta(p.get("metadata")), p.get("detail", ""),
                    domain=p.get("domain") if p.get("reason") else None)
        if not p.get("reason"):
            err.reason, err.domain = None, None
    else:
        err = Error(_code_of_status(status), None)
    return _RestoredError.of(err, status)


class _RestoredError(Error):
    """An Error that keeps the HTTP status the dependency answered with."""

    @classmethod
    def of(cls, e: Error, status: int) -> "_RestoredError":
        r = cls(e.code, e.reason, e.metadata, e.message, domain=e.domain, violations=e.violations,
                retry_after=e.retry_after)
        r._status = status
        return r

    @property
    def http(self) -> int:
        return self._status


def _code_of_status(status: int) -> Code:
    table = {400: Code.INVALID_ARGUMENT, 413: Code.INVALID_ARGUMENT, 401: Code.UNAUTHENTICATED,
             403: Code.PERMISSION_DENIED, 404: Code.NOT_FOUND, 409: Code.ABORTED, 429: Code.RESOURCE_EXHAUSTED,
             499: Code.CANCELLED, 501: Code.UNIMPLEMENTED, 502: Code.UNAVAILABLE, 503: Code.UNAVAILABLE,
             504: Code.DEADLINE_EXCEEDED}
    if status in table:
        return table[status]
    return Code.FAILED_PRECONDITION if 400 <= status < 500 else Code.UNKNOWN


def _str_meta(m: Any) -> dict[str, str]:
    return {str(k): v if isinstance(v, str) else str(v) for k, v in (m or {}).items()}


# --- SQLSTATE (P10.4) ----------------------------------------------------------------------------


@dataclass(frozen=True)
class SqlOutcome:
    retry: bool
    base_delay_ms: int = 0
    error: Error | None = None


MAX_TX_ATTEMPTS = 3


def classify_sqlstate(sqlstate: str, *, attempt: int = 1, context: str = "none",
                      mapped: Error | None = None, max_attempts: int = MAX_TX_ATTEMPTS) -> SqlOutcome:
    """What a transaction does with a SQLSTATE (vectors errors/sqlstate)."""
    if sqlstate in ("40001", "40P01"):
        if attempt < max_attempts:
            return SqlOutcome(True, 10 * 2 ** (attempt - 1))
        return SqlOutcome(False, error=be_error("TX_CONFLICT"))
    if sqlstate == "55P03":
        return SqlOutcome(False, error=be_error("LOCK_TIMEOUT"))
    if sqlstate == "57014" and context == "cancelled":
        return SqlOutcome(False, error=be_error("REQUEST_CANCELLED"))
    if sqlstate.startswith("08") or sqlstate in ("57P01", "57P02", "57P03"):
        return SqlOutcome(False, error=be_error("DEPENDENCY_UNAVAILABLE", {"dependency": "db"}))
    if sqlstate in ("57014", "25P04"):
        return SqlOutcome(False, error=be_error("STATEMENT_TIMEOUT"))
    if sqlstate == "53300":
        return SqlOutcome(False, error=be_error("DB_TOO_MANY_CONNECTIONS"))
    if sqlstate == "BE001":
        return SqlOutcome(False, error=be_error("UNIT_SEALED"))
    if sqlstate == "23505" and mapped is not None:
        return SqlOutcome(False, error=mapped)
    return SqlOutcome(False, error=internal(f"SQLSTATE {sqlstate}"))


# --- problem+json (P4.1) -------------------------------------------------------------------------


@dataclass
class Problem:
    status: int
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str = "application/problem+json"


def _lang(locale: str) -> str:
    return "zh" if locale.lower().startswith("zh") else "en"


def _render(template: str, metadata: Mapping[str, str]) -> str:
    return re.sub(r"\{([a-z][a-z0-9_]*)\}", lambda m: metadata.get(m.group(1), m.group(0)), template)


def visible(err: Error, component_domain: str | None = None) -> Error:
    """The error as a caller may see it: hidden codes and unclassified errors become INTERNAL (P4.3)."""
    domain = err.domain or component_domain
    if err.code in _HIDDEN or err.reason is None and err.code is not Code.CANCELLED or domain is None:
        shown = be_error("INTERNAL")
        if err.code in (Code.UNKNOWN, Code.DATA_LOSS):
            shown.code = err.code
        return shown
    if domain == err.domain:
        return err
    return Error(err.code, err.reason, err.metadata, err.message, domain=domain, violations=err.violations,
                 retry_after=err.retry_after)


def problem(err: Error, *, path: str, request_id: str, trace_id: str, locale: str = "zh-CN",
            catalog: Catalog | None = None, component_domain: str | None = None) -> Problem:
    """The problem+json answer for ``err`` (P4.1); title and detail in ``locale`` from the catalogue."""
    for v in err.metadata.values():
        if not isinstance(v, str):
            raise ProtocolError("METADATA_NOT_STRING", "metadata values are strings only (P4.8)")
    shown = visible(err, component_domain)
    reason = shown.reason or shown.code.name
    domain = shown.domain or "be"
    entry = (catalog or _default_catalog()).get(domain, reason)
    lang = _lang(locale)
    title = entry.title.get(lang) or entry.title.get("en") if entry else reason
    detail = _render(entry.message.get(lang) or entry.message["en"], shown.metadata) if entry else (
        shown.message or title)
    status = shown.http
    body: dict[str, Any] = {
        "type": f"urn:be:{domain}:{reason}", "title": title, "status": status, "code": shown.code.name,
        "reason": reason, "domain": domain, "detail": detail, "metadata": dict(shown.metadata),
    }
    if shown.violations:
        body["violations"] = [{"field": v.field, "reason": v.reason, "description": v.description}
                              for v in shown.violations]
    body.update({"instance": path, "request_id": request_id, "trace_id": trace_id})
    headers = {}
    ra = retry_after_header(shown.code, None if shown.retry_after is None else shown.retry_after * 1000)
    if ra is not None:
        headers["Retry-After"] = ra
    return Problem(status, body, headers)


def retry_after_header(code: Code, retry_delay_ms: float | None) -> str | None:
    """``Retry-After`` only with 429 and 503 and only when a delay is carried; whole seconds, up."""
    if retry_delay_ms is None or _HTTP[code] not in (429, 503):
        return None
    return str(max(1, math.ceil(retry_delay_ms / 1000)))
