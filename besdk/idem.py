"""Command idempotency (be-protocol P3.7, P13): fingerprints, the caller namespace, the atomic claim in
``besdk_idempotency`` and the replay of a completed command.

One-step command (claim, execute, complete in one transaction; a replay never calls ``do``)::

    cmd = besdk.Command(key=besdk.resolve_key(header, body.idempotency_key), name=CREATE,
                        request=body.model_dump(exclude={"idempotency_key"}))
    result, replayed = await rt.store().tx(lambda tx: besdk.idempotent(tx, cmd, lambda: create(tx, body)))

Two-step command (claim, a network call, complete): ``tx.idem_claim`` in one transaction, the call, then
``tx.idem_complete`` (or ``tx.idem_release`` when the step failed for certain) in another.

The order of checks is the component's (P13.5): validate → authorize the target → claim → state machine →
write. A replayed create is read back through the component's own scoped read.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Awaitable, Callable, TypeVar

from besdk import context, errors
from besdk.config_values import parse_json

if TYPE_CHECKING:
    from besdk.store.tx import Tx

T = TypeVar("T")
VALIDITY = timedelta(days=30)


# --- fingerprint (RFC 8785 JCS, no Unicode normalisation, SHA-256) -------------------------------------------


def _es_number(x: float) -> str:
    """ECMAScript Number::toString of a double (RFC 8785 §3.2.2.3)."""
    if x == 0:
        return "0"
    if x < 0:
        return "-" + _es_number(-x)
    sign, digits, exp = Decimal(repr(x)).as_tuple()
    ds = list(digits)
    while len(ds) > 1 and ds[-1] == 0:
        ds.pop()
        exp += 1
    s = "".join(map(str, ds))
    k, n = len(s), exp + len(ds)
    if k <= n <= 21:
        return s + "0" * (n - k)
    if 0 < n <= 21:
        return s[:n] + "." + s[n:]
    if -6 < n <= 0:
        return "0." + "0" * (-n) + s
    e = n - 1
    mant = s if k == 1 else s[0] + "." + s[1:]
    return f"{mant}e{'+' if e >= 0 else '-'}{abs(e)}"


def _utf16(s: str) -> bytes:
    return s.encode("utf-16-be", "surrogatepass")


def canonical(v: Any) -> str:
    """The JCS text of an already parsed JSON value. Strings are not normalised (NFC and NFD differ)."""
    if v is None:
        return "null"
    if v is True:
        return "true"
    if v is False:
        return "false"
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except OverflowError:
            raise errors.ProtocolError("JSON_INVALID", "a number outside the range of a double") from None
        if math.isinf(f) or math.isnan(f):
            raise errors.ProtocolError("JSON_INVALID", "not a finite number")
        return _es_number(f)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(canonical(x) for x in v) + "]"
    if isinstance(v, dict):
        keys = sorted(v, key=lambda k: _utf16(str(k)))
        return "{" + ",".join(f"{canonical(str(k))}:{canonical(v[k])}" for k in keys) + "}"
    raise errors.ProtocolError("JSON_INVALID", f"{type(v).__name__} is not a JSON value")


def _plain(v: Any) -> Any:
    """Pydantic models and dataclasses become JSON values; Decimal stays exact as a string (0301)."""
    if hasattr(v, "model_dump"):
        return v.model_dump(mode="json")
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, dict):
        return {k: _plain(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_plain(x) for x in v]
    return v


def fingerprint(request: Any) -> bytes:
    """SHA-256 of the canonical form of the command's fingerprint fields (P13.2)."""
    return hashlib.sha256(canonical(_plain(request)).encode("utf-8")).digest()


def fingerprint_text(text: str) -> tuple[str, bytes]:
    """(canonical text, SHA-256) of a JSON text; not I-JSON → ProtocolError JSON_INVALID."""
    try:
        v = parse_json(text)
    except (ValueError, RecursionError) as e:
        raise errors.ProtocolError("JSON_INVALID", str(e)) from None
    c = canonical(v)
    return c, hashlib.sha256(c.encode("utf-8")).digest()


# --- keys, namespaces, expiry ---------------------------------------------------------------------


def resolve_key(header: str | None, body: str | None) -> str | None:
    """``Idempotency-Key`` header or ``idempotency_key`` field; both and different → 400 (P3.7)."""
    if header and body and header != body:
        raise errors.be_error("IDEMPOTENCY_MISMATCH", message="Idempotency-Key and idempotency_key differ")
    return header or body or None


def caller_namespace(*, kind: str, sub: str = "", be_caller: str = "") -> str:
    """``user:<sub>`` | ``svc:<be-caller>`` | ``system`` (P13.1)."""
    if kind == "user":
        return f"user:{sub}"
    if kind == "system_call":
        return f"svc:{be_caller}"
    return "system"


def caller_of() -> str:
    """The namespace of the current unit of work: a system call, a user request, else background work."""
    u = context.current()
    if u.caller:
        return caller_namespace(kind="system_call", be_caller=u.caller)
    if u.sub:
        return caller_namespace(kind="user", sub=u.sub)
    return "system"


def expires_at(created_at: datetime) -> datetime:
    return created_at + VALIDITY


# --- the decision (pure) --------------------------------------------------------------------------


@dataclass(frozen=True)
class Stored:
    command: str
    target: str
    request_hash: bytes
    status: str  # CLAIMED | DONE
    result: Any
    expires_at: datetime


@dataclass(frozen=True)
class Decision:
    outcome: str  # EXECUTE | REPLAY | REJECT
    result: Any = None
    error: errors.Error | None = None


def decide(row: Stored | None, *, command: str, target: str, request_hash: bytes, now: datetime) -> Decision:
    """What an incoming command does given the row stored for (caller, key) (P13.2, P13.3, P13.7)."""
    if row is None or now >= row.expires_at:
        return Decision("EXECUTE")
    if (row.command, row.target, bytes(row.request_hash)) != (command, target, request_hash):
        return Decision("REJECT", error=errors.be_error("IDEMPOTENCY_MISMATCH"))
    if row.status != "DONE":
        return Decision("REJECT", error=errors.be_error("IDEMPOTENCY_IN_PROGRESS"))
    return Decision("REPLAY", result=row.result)


# --- the table ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Command:
    """``key`` from the header or field (None: no idempotency); ``name`` the permission key or rpc full name;
    ``target`` the aggregate ID ('' for a create); ``request`` the fingerprint fields."""

    key: str | None
    name: str
    request: Any = None
    target: str = ""

    def hash(self) -> bytes:
        return fingerprint({} if self.request is None else self.request)


@dataclass(frozen=True)
class Prior:
    found: bool
    in_progress: bool = False
    result: Any = None


# Takes a free key, or an expired row of the same (caller, key), in one statement (P10.9, P13.6, P13.7).
_CLAIM = ("INSERT INTO besdk_idempotency (caller, idempotency_key, command, target, request_hash, status, "
          "expires_at) VALUES ($1, $2, $3, $4, $5, 'CLAIMED', now() + interval '30 days') "
          "ON CONFLICT (caller, idempotency_key) DO UPDATE SET command = EXCLUDED.command, "
          "target = EXCLUDED.target, request_hash = EXCLUDED.request_hash, status = 'CLAIMED', result = NULL, "
          "created_at = now(), updated_at = now(), expires_at = EXCLUDED.expires_at "
          "WHERE besdk_idempotency.expires_at <= now() RETURNING status")
_HELD = ("SELECT command, target, request_hash, status, result, expires_at, now() AS now FROM besdk_idempotency "
         "WHERE caller = $1 AND idempotency_key = $2 FOR UPDATE")
_PEEK = ("SELECT command, target, request_hash, status, result, expires_at, now() AS now FROM besdk_idempotency "
         "WHERE caller = $1 AND idempotency_key = $2")
_DONE = ("UPDATE besdk_idempotency SET status = 'DONE', result = $3::jsonb, updated_at = now() "
         "WHERE caller = $1 AND idempotency_key = $2 AND status = 'CLAIMED'")
_RELEASE = "DELETE FROM besdk_idempotency WHERE caller = $1 AND idempotency_key = $2 AND status = 'CLAIMED'"


def _decide_row(r: Any, cmd: Command) -> Decision:
    stored = Stored(r["command"], r["target"], bytes(r["request_hash"]), r["status"],
                    None if r["result"] is None else json.loads(r["result"]), r["expires_at"])
    return decide(stored, command=cmd.name, target=cmd.target, request_hash=cmd.hash(), now=r["now"])


def _result(d: Decision) -> Any:
    return (d.result or {}).get("body")


async def claim(tx: "Tx", cmd: Command) -> Prior:
    """Claim the key, or report the completed result; mismatch and in-progress raise (P13.2, P13.3)."""
    caller = caller_of()
    if await tx.fetchval(_CLAIM, caller, cmd.key, cmd.name, cmd.target, cmd.hash()) is not None:
        return Prior(found=False)
    d = _decide_row(await tx.fetchrow(_HELD, caller, cmd.key), cmd)
    if d.error is not None:
        raise d.error
    return Prior(found=True, result=_result(d))


async def lookup(tx: "Tx", cmd: Command) -> Prior:
    """Read only: whether the key was used, still in progress, or completed (mismatch raises)."""
    r = await tx.fetchrow(_PEEK, caller_of(), cmd.key)
    if r is None:
        return Prior(found=False)
    d = _decide_row(r, cmd)
    if d.outcome == "EXECUTE":
        return Prior(found=False)
    if d.error is not None and d.error.reason == "IDEMPOTENCY_IN_PROGRESS":
        return Prior(found=True, in_progress=True)
    if d.error is not None:
        raise d.error
    return Prior(found=True, result=_result(d))


async def complete(tx: "Tx", cmd: Command, result: Any) -> None:
    body = json.dumps({"body": _plain(result)}, ensure_ascii=False, default=str)
    await tx.execute(_DONE, caller_of(), cmd.key, body)


async def release(tx: "Tx", cmd: Command) -> None:
    """A step failed for certain: free the key so the same command may be retried (P13.3)."""
    await tx.execute(_RELEASE, caller_of(), cmd.key)


async def idempotent(tx: "Tx", cmd: Command, do: Callable[[], Awaitable[T]]) -> tuple[T, bool]:
    """One step: claim, ``do()``, complete, all in ``tx``; returns (result, replayed). No key: just ``do()``."""
    if not cmd.key:
        return await do(), False
    prior = await claim(tx, cmd)
    if prior.found:
        return prior.result, True
    res = await do()
    await complete(tx, cmd, res)
    return res, False
