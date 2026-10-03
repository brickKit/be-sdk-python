"""Structured logs (be-protocol P18.2): stdout, one JSON object per line, ≤ 2 KiB, automatic redaction.

Each member gets its own ``logging.Logger`` (never the root logger, never ``basicConfig``), so members
of a shell keep their own ``component_id`` and ``LOG_LEVEL``. Business code logs with
``rt.logger.info("event_name", extra={...})``; the extra fields are redacted by key, never by value.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, TextIO

from besdk import context

MAX_LINE = 2048
TRUNC = "…[TRUNCATED]"
REDACTED = "[REDACTED]"
ENVELOPE = ("time", "level", "msg", "component_id", "component_version", "trace_id", "span_id", "request_id")
PROTECTED = ("phone", "mobile", "id_card", "password", "bank_card", "email", "token", "secret", "authorization",
             "cookie", "set_cookie", "api_key")
_PROTECTED_WORDS = [tuple(p.split("_")) for p in PROTECTED]
_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
_NAMES = {logging.DEBUG: "debug", logging.INFO: "info", logging.WARNING: "warn", logging.ERROR: "error",
          logging.CRITICAL: "error"}
_STD_ATTRS = set(logging.LogRecord("x", 0, "", 0, "", None, None).__dict__) | {"message", "asctime", "taskName"}


def _words(key: str) -> list[str]:
    key = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key)
    key = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", key)
    return [w for w in re.split(r"[_\-.]+", key.lower()) if w]


def protected_key(key: str) -> bool:
    """A key is protected when a protected name's words appear in it as one contiguous run (P18.2)."""
    words = _words(key)
    for name in _PROTECTED_WORDS:
        n = len(name)
        for i in range(len(words) - n + 1):
            run = words[i:i + n]
            if run[:-1] == list(name[:-1]) and run[-1] in (name[-1], name[-1] + "s"):
                return True
    return False


def _walk(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: REDACTED if protected_key(k) else _walk(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_walk(x) for x in v]
    return v


def redact(record: dict[str, Any]) -> dict[str, Any]:
    """Redact personal-data keys; envelope fields and values are never scanned (vectors redaction)."""
    return {k: v if k in ENVELOPE else (REDACTED if protected_key(k) else _walk(v)) for k, v in record.items()}


def _dump(d: dict) -> str:
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), default=str)


def _strings(d: Any, path: tuple = ()) -> list[tuple[tuple, str]]:
    out = []
    items = d.items() if isinstance(d, dict) else enumerate(d) if isinstance(d, list) else ()
    for k, v in items:
        if not path and k in ENVELOPE:
            continue
        if isinstance(v, str):
            out.append(((*path, k), v))
        elif isinstance(v, (dict, list)):
            out.extend(_strings(v, (*path, k)))
    return out


def _set(d: Any, path: tuple, value: Any) -> None:
    for k in path[:-1]:
        d = d[k]
    d[path[-1]] = value


def fit(record: dict[str, Any], limit: int = MAX_LINE) -> str:
    """Serialise; when longer than ``limit`` bytes, cut string values longest first (P18.2)."""
    line = _dump(record)
    if len(line.encode()) <= limit:
        return line
    record = json.loads(line)
    record["truncated"] = True
    while len((line := _dump(record)).encode()) > limit:
        cands = [(p, s) for p, s in _strings(record) if not s.endswith(TRUNC) or len(s) > len(TRUNC)]
        if not cands:
            break
        path, s = max(cands, key=lambda ps: len(ps[1].encode()))
        excess = len(line.encode()) - limit
        keep = max(0, len(s) - max(excess, 1) - len(TRUNC))
        base = s[:-len(TRUNC)] if s.endswith(TRUNC) else s
        _set(record, path, base[:min(keep, len(base) - 1 if base else 0)] + TRUNC)
    return line


class JsonFormatter(logging.Formatter):
    """One JSON object per line with the envelope, the unit's context and the call's fields."""

    def __init__(self, component_id: str, version: str):
        super().__init__()
        self.component_id = component_id
        self.version = version

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "time": _rfc3339_ns(getattr(record, "created_ns", None) or int(record.created * 1e9)),
            "level": _NAMES.get(record.levelno, "info"),
            "msg": record.getMessage(),
            "component_id": self.component_id,
            "component_version": self.version,
        }
        _add_trace(out)
        _add_unit(out)
        for k, v in record.__dict__.items():
            if k not in _STD_ATTRS and not k.startswith("_"):
                out[k] = v
        if record.exc_info and "error" not in out:
            out["error"] = "".join(traceback.format_exception_only(record.exc_info[1])).strip()
        return fit(redact(out))


def _rfc3339_ns(ns: int) -> str:
    sec, frac = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(sec, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{frac:09d}Z"


def _add_trace(out: dict) -> None:
    from opentelemetry import trace

    sc = trace.get_current_span().get_span_context()
    if sc.is_valid:
        out["trace_id"] = f"{sc.trace_id:032x}"
        out["span_id"] = f"{sc.span_id:016x}"


def _add_unit(out: dict) -> None:
    u = context.current()
    for name in ("request_id", "sub", "perm", "caller", "job"):
        if v := getattr(u, name):
            out[name] = v
    if u.act:
        out["act"] = json.dumps(u.act, separators=(",", ":"))
    if u.event is not None:
        out.update(event_id=u.event.id, subject=u.event.subject, delivery=u.event.delivery)


def member_logger(component_id: str, version: str, *, level: str = "info", stream: TextIO | None = None) -> logging.Logger:
    """A logger of its own for one member: JSON to stdout, its own level, not propagating to root."""
    lg = logging.Logger(f"besdk.{component_id}", _LEVELS.get(level, logging.INFO))
    h = logging.StreamHandler(stream or sys.stdout)
    h.setFormatter(JsonFormatter(component_id, version))
    lg.addHandler(h)
    lg.propagate = False
    return lg
