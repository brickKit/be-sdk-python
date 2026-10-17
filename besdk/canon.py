"""The canonical encoding of security records (P8.8; assembly project security 09a): normalized decimals, instants and
text, RFC 8785 serialization of strings, integers, booleans, arrays and objects, and domain-separated SHA-256
digests. No JSON null and no floating point ever appear; the bytes equal the Go SDK's."""
from __future__ import annotations

import hashlib
import unicodedata
from datetime import datetime, timezone
from decimal import Context, Decimal as D, InvalidOperation
from typing import Any

_MAX_INT = 1 << 53
_ESC = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r", "\t": "\\t"}


class Refused(ValueError):
    """A value the encoding forbids."""


def decimal(s: str) -> str:
    """No exponent, no leading "+", no superfluous zeros, "0" for zero; at most 38 significant digits and 18 fraction
    digits."""
    try:
        d = D(s)
    except InvalidOperation:
        raise Refused(f"decimal {s!r}") from None
    if not d.is_finite():
        raise Refused(f"decimal {s!r}")
    if d == 0:
        return "0"
    r = d.normalize(Context(prec=100))
    sign, digits, exp = r.as_tuple()
    if len(digits) > 38 or -exp > 18:  # type: ignore[operator]
        raise Refused(f"decimal {s!r}")
    out = format(r, "f")
    if "." in out:
        out = out.rstrip("0").rstrip(".")
    return out


def instant(s: str) -> str:
    """An RFC 3339 time with an offset as UTC with six fraction digits."""
    try:
        t = datetime.fromisoformat(s.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        raise Refused(f"instant {s!r}") from None
    if t.tzinfo is None or "T" not in s.upper():
        raise Refused(f"instant {s!r}")
    return time(t)


def time(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def text(s: str) -> str:
    return unicodedata.normalize("NFC", s)


def _string(s: str, out: list[str]) -> None:
    out.append('"')
    for ch in s:
        if ch in _ESC:
            out.append(_ESC[ch])
        elif ord(ch) < 0x20:
            out.append(f"\\u{ord(ch):04x}")
        else:
            out.append(ch)
    out.append('"')


def _encode(v: Any, out: list[str]) -> None:
    if v is None:
        raise Refused("null")
    if isinstance(v, bool):
        out.append("true" if v else "false")
    elif isinstance(v, int):
        if abs(v) >= _MAX_INT:
            raise Refused(f"integer {v} is not below 2^53")
        out.append(str(v))
    elif isinstance(v, float):
        raise Refused("floating point")
    elif isinstance(v, str):
        _string(v, out)
    elif isinstance(v, (list, tuple)):
        out.append("[")
        for i, e in enumerate(v):
            if i:
                out.append(",")
            _encode(e, out)
        out.append("]")
    elif isinstance(v, dict):
        out.append("{")
        for i, k in enumerate(sorted(v, key=lambda k: k.encode("utf-16-be"))):
            if i:
                out.append(",")
            _string(k, out)
            out.append(":")
            _encode(v[k], out)
        out.append("}")
    else:
        raise Refused(f"type {type(v).__name__}")


def encode(v: Any) -> bytes:
    out: list[str] = []
    _encode(v, out)
    return "".join(out).encode()


def digest(record_type: str, major: int, v: Any) -> str:
    """"sha256:" + SHA-256 of "<record type>.v<major>\\n" followed by the canonical bytes."""
    return "sha256:" + hashlib.sha256(f"{record_type}.v{major}\n".encode() + encode(v)).hexdigest()


def sorted_set(items: list[Any]) -> list[Any]:
    """A set's elements ordered by their canonical bytes, duplicates dropped."""
    seen = {encode(it): it for it in items}
    return [seen[k] for k in sorted(seen)]
