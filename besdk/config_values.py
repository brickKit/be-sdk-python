"""Pure parsing rules of configuration values, key names and addresses (be-protocol P2; vectors `config`).

Every function here is deterministic and raises :class:`besdk.errors.ProtocolError` with the vector
error class (``CONFIG_MISSING``, ``CONFIG_INVALID``, …). :mod:`besdk.config` builds the runtime's
``Config`` on top of them.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from besdk.errors import ProtocolError

MAX_INT = 2**53 - 1
MAX_DURATION_NS = 2**63 - 1
PLATFORM_EXACT = ("COMPONENT_ID", "COMPONENT_VERSION", "PORT")
RESERVED_EXACT = (*PLATFORM_EXACT, "BRICKKIT_SERVED_MEMBERS", "BRICKKIT_SERVED_MEMBERS_CONFIG")
FAMILY_KEYS = ("AUTHZ_URL", "AUTHZ_GRPC_URL", "IAM_URL", "IAM_GRPC_URL")


@dataclass(frozen=True)
class KeySpec:
    """One declared key: its parse format and presence rules (catalogue row or configSchema item)."""

    name: str
    format: str = "string"  # string integer boolean duration duration_list url json enum number zone locale
    required: bool = False
    default: str | None = None
    secret: bool = False
    minimum: int | None = None
    schemes: tuple[str, ...] = ()
    json_kind: str | None = None
    enum: tuple[str, ...] = ()


@dataclass(frozen=True)
class Parsed:
    set: bool
    value: Any = None


def _invalid(spec: KeySpec, why: str) -> ProtocolError:
    return ProtocolError("CONFIG_INVALID", f"{spec.name}: {why}", key=spec.name)


def parse_value(spec: KeySpec, raw: str | None) -> Parsed:
    """Presence, default and strict typing of one value (P2.3); a bad value never falls back."""
    if raw == "":  # rc.2: an empty value is absent for every type (P2.3)
        raw = None
    if raw is None:
        if spec.default is not None and spec.default != "":
            return Parsed(True, _typed(spec, spec.default, "default"))
        if spec.required:
            raise ProtocolError("CONFIG_MISSING", f"{spec.name} is required", key=spec.name)
        return Parsed(False)
    return Parsed(True, _typed(spec, raw, "value"))


def _typed(spec: KeySpec, raw: str, what: str) -> Any:
    try:
        return _PARSERS[spec.format](spec, raw)
    except ProtocolError:
        raise
    except (ValueError, KeyError) as e:
        raise _invalid(spec, f"{what} {raw!r}: {e}") from None


def _p_string(spec: KeySpec, raw: str) -> str:
    if spec.secret:
        return _secret_path(spec, raw)
    if spec.enum and raw not in spec.enum:
        raise _invalid(spec, f"{raw!r} is not one of {list(spec.enum)}")
    return raw


def _secret_path(spec: KeySpec, raw: str) -> str:
    if not raw.startswith("/") or raw.endswith("/") or "\n" in raw:
        raise _invalid(spec, "a secret key holds the absolute path of its file (P2.7)")
    return raw


def _p_int(spec: KeySpec, raw: str) -> int:
    if not re.fullmatch(r"-?[0-9]+", raw):
        raise _invalid(spec, f"{raw!r} is not an integer")
    v = int(raw)
    if abs(v) > MAX_INT:
        raise _invalid(spec, f"{raw} is outside ±(2^53−1)")
    if spec.minimum is not None and v < spec.minimum:
        raise _invalid(spec, f"{v} is below the minimum {spec.minimum}")
    return v


def _p_number(spec: KeySpec, raw: str) -> float:
    if not re.fullmatch(r"-?[0-9]+(\.[0-9]+)?", raw):
        raise _invalid(spec, f"{raw!r} is not a number")
    return float(raw)


def _p_bool(spec: KeySpec, raw: str) -> bool:
    table = {"true": True, "1": True, "false": False, "0": False}
    if raw not in table:
        raise _invalid(spec, f"{raw!r} is not true, false, 1 or 0")
    return table[raw]


def _p_duration(spec: KeySpec, raw: str) -> int:
    ns = parse_duration_ns(raw)
    if ns is None:
        raise _invalid(spec, f"{raw!r} is not a duration (Go syntax, e.g. 5s, 200ms, 1h30m)")
    return ns


def _p_duration_list(spec: KeySpec, raw: str) -> list[int]:
    out = []
    for part in raw.split(","):
        ns = parse_duration_ns(part) if part else None
        if ns is None or ns <= 0:
            raise _invalid(spec, f"{raw!r} is not a comma-separated list of positive durations")
        out.append(ns)
    return out


_URL_RE = re.compile(r"([a-z][a-z0-9+.-]*)://(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._~-]+)(?::([0-9]{1,5}))?([/?#]\S*)?")


def _p_url(spec: KeySpec, raw: str) -> str:
    m = _URL_RE.fullmatch(raw)
    if not m or (m.group(3) is not None and not 1 <= int(m.group(3)) <= 65535):
        raise _invalid(spec, f"{raw!r} is not scheme://host[:port][path]")
    if spec.schemes and m.group(1) not in spec.schemes:
        raise _invalid(spec, f"scheme {m.group(1)!r} is not one of {list(spec.schemes)}")
    return raw


def _no_dupes(pairs: list[tuple[str, Any]]) -> dict:
    out: dict = {}
    for k, v in pairs:
        if k in out:
            raise ValueError(f"duplicate name {k!r}")
        out[k] = v
    return out


def _no_constant(name: str) -> Any:
    raise ValueError(f"{name} is not I-JSON")


def parse_json(raw: str) -> Any:
    """I-JSON: duplicate names and NaN/Infinity rejected."""
    return json.loads(raw, object_pairs_hook=_no_dupes, parse_constant=_no_constant)


def _p_json(spec: KeySpec, raw: str) -> Any:
    try:
        v = parse_json(raw)
    except ValueError as e:
        raise _invalid(spec, f"not I-JSON: {e}") from None
    kind = {"object": dict, "array": list}.get(spec.json_kind or "")
    if kind and not isinstance(v, kind):
        raise _invalid(spec, f"must be a JSON {spec.json_kind}")
    return v


def _p_enum(spec: KeySpec, raw: str) -> str:
    if raw not in spec.enum:
        raise _invalid(spec, f"{raw!r} is not one of {list(spec.enum)}")
    return raw


def _p_zone(spec: KeySpec, raw: str) -> str:
    from zoneinfo import ZoneInfo, available_timezones

    if raw not in available_timezones():
        raise _invalid(spec, f"{raw!r} is not an IANA zone")
    ZoneInfo(raw)
    return raw


def _p_locale(spec: KeySpec, raw: str) -> str:
    if not re.fullmatch(r"[a-z]{2,3}(-[A-Za-z0-9]{2,8})*", raw):
        raise _invalid(spec, f"{raw!r} is not a BCP 47 tag")
    return raw


_PARSERS = {
    "string": _p_string, "integer": _p_int, "int": _p_int, "number": _p_number, "boolean": _p_bool,
    "bool": _p_bool, "duration": _p_duration, "duration_list": _p_duration_list, "durations": _p_duration_list,
    "url": _p_url, "json": _p_json, "enum": _p_enum, "zone": _p_zone, "locale": _p_locale,
}

# --- Go time.ParseDuration ----------------------------------------------------------------------

_UNITS = {"ns": 1, "us": 1_000, "µs": 1_000, "μs": 1_000, "ms": 1_000_000, "s": 1_000_000_000,
          "m": 60_000_000_000, "h": 3_600_000_000_000}
_COMPONENT = re.compile(r"([0-9]*)(?:\.([0-9]*))?(ns|us|µs|μs|ms|s|m|h)")


def parse_duration_ns(s: str) -> int | None:
    """Go's ``time.ParseDuration`` (vectors config/duration), negative rejected; None when invalid."""
    if s.startswith("+"):
        s = s[1:]
    elif s.startswith("-"):
        return None
    if s == "0":
        return 0
    if not s:
        return None
    total, pos = 0, 0
    while pos < len(s):
        m = _COMPONENT.match(s, pos)
        if not m or (not m.group(1) and not m.group(2)):
            return None
        whole, frac, unit = m.group(1) or "0", m.group(2) or "", _UNITS[m.group(3)]
        v = int(whole) * unit
        if frac:
            v += int(float(int(frac)) * (unit / 10 ** len(frac)))
        total += v
        if total > MAX_DURATION_NS:
            return None
        pos = m.end()
    return total


# --- secrets, names, addresses ------------------------------------------------------------------


def secret_text(content: str, *, required: bool, key: str) -> str | None:
    """A text secret: exactly one trailing LF or CRLF removed; empty = not set (P2.9)."""
    if content.endswith("\r\n"):
        content = content[:-2]
    elif content.endswith("\n"):
        content = content[:-1]
    if content == "":
        if required:
            raise ProtocolError("CONFIG_MISSING", f"secret file of {key} is empty", key=key)
        return None
    return content


def check_readable(key: str, declared: set[str]) -> bool:
    """A read of an undeclared key is a programming error (P2.2), except the platform's names."""
    if key in declared or key in PLATFORM_EXACT or key.endswith("_ENDPOINT"):
        return True
    raise ProtocolError("CONFIG_UNDECLARED", f"{key} is not declared in configSchema", key=key)


_ID_RE = re.compile(r"[a-z][a-z0-9-]*/[a-z][a-z0-9-]*")
_PORT_RE = re.compile(r"[a-z][a-z0-9-]*")


def endpoint_name(dependency: str, port: str = "") -> str:
    """``<DEP>[_<PORT>]_ENDPOINT`` (brickKit's environment contract)."""
    if not _ID_RE.fullmatch(dependency):
        raise ProtocolError("COMPONENT_INVALID", dependency)
    if port and not _PORT_RE.fullmatch(port):
        raise ProtocolError("PORT_NAME_INVALID", port)
    name = dependency.upper().replace("/", "_").replace("-", "_")
    if port:
        name += "_" + port.upper().replace("-", "_")
    return name + "_ENDPOINT"


_ADDR_RE = re.compile(r"http://(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+):([0-9]{1,5})/?")


def endpoint_address(value: str | None) -> str | None:
    """``http://host:port[/]`` → ``host:port``; absent → None (P2.5, P2.6)."""
    if value is None:
        return None
    m = _ADDR_RE.fullmatch(value)
    if not m or not 1 <= int(m.group(2)) <= 65535:
        raise ProtocolError("CONFIG_INVALID", f"address {value!r} is not http://host:port")
    return f"{m.group(1)}:{m.group(2)}"


def family_address(key: str, value: str | None) -> str | None:
    """A slot-family key → REST base ``http://host:port`` or gRPC target ``host:port`` (P2.10)."""
    if key not in FAMILY_KEYS:
        raise ProtocolError("CONFIG_KEY_INVALID", f"{key} is not a family address key", key=key)
    addr = endpoint_address(value)
    if addr is None:
        return None
    return addr if key.endswith("_GRPC_URL") else f"http://{addr}"


_KEY_RE = re.compile(r"[A-Z][A-Z0-9_]*")


def check_key_name(key: str) -> None:
    """Key names a component may declare (P2.4)."""
    if not _KEY_RE.fullmatch(key):
        raise ProtocolError("CONFIG_KEY_INVALID", repr(key), key=key)
    if key in RESERVED_EXACT or key.startswith("BRICKKIT_SERVED_MEMBERS") or key.endswith("_ENDPOINT"):
        raise ProtocolError("CONFIG_KEY_RESERVED", key, key=key)


def check_declaration(key: str, *, secret: bool = False, mount: str | None = None,
                      type_: str | None = None) -> None:
    """``secret: true`` ⇔ ``mount: file`` ⇔ the name ends in ``_FILE`` (P2.12)."""
    check_key_name(key)
    if mount is not None and mount != "file":
        raise ProtocolError("MOUNT_INVALID", key, key=key)
    if mount == "file" and not secret:
        raise ProtocolError("MOUNT_NEEDS_SECRET", key, key=key)
    if mount == "file" and type_ not in (None, "string"):
        raise ProtocolError("MOUNT_NEEDS_STRING", key, key=key)
    if secret and mount != "file":
        raise ProtocolError("SECRET_NOT_FILE", key, key=key)
    if mount == "file" and not key.endswith("_FILE"):
        raise ProtocolError("FILE_SUFFIX_REQUIRED", key, key=key)
    if not secret and key.endswith("_FILE"):
        raise ProtocolError("FILE_SUFFIX_RESERVED", key, key=key)
