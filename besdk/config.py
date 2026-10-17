"""A component's declared configuration keys (protocol chapter 02): strict types, empty means unset, no fallback on
invalid values, every problem reported at once, secrets as values or <KEY>_FILE.

Modules never read the process environment: the launcher builds a source from it (single run) or from the member's
JSON (shell) and hands the parsed Values to the module through the runtime.
"""
from __future__ import annotations

import os
import re
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Callable, Mapping, Protocol


class Type(StrEnum):
    """The types of P2.4."""

    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DURATION = "duration"
    ENUM = "enum"


@dataclass(frozen=True)
class Key:
    """One configuration key, as the component's configSchema declares it."""

    name: str
    type: Type
    required: bool = False
    default: str | int | bool | timedelta | None = None
    values: tuple[str, ...] = ()  # enum only
    min: int | None = None  # integer only, inclusive
    max: int | None = None
    secret: bool = False
    process: bool = False  # must agree across a shell's members (P2.11)


class Source(Protocol):
    def lookup(self, name: str) -> str | None: ...


class FileSource(Source, Protocol):
    def read_file(self, path: str) -> bytes: ...


class MapSource:
    """A source over a plain mapping: a shell member's configuration, or tests."""

    def __init__(self, values: Mapping[str, str]):
        self._values = dict(values)

    def lookup(self, name: str) -> str | None:
        return self._values.get(name)


@dataclass
class EnvSource:
    """The single-run source: the process environment and the files it points at. Only the launcher builds it."""

    lookup_env: Callable[[str], str | None] = os.environ.get
    read: Callable[[str], bytes] = lambda path: open(path, "rb").read()

    def lookup(self, name: str) -> str | None:
        return self.lookup_env(name)

    def read_file(self, path: str) -> bytes:
        return self.read(path)


@dataclass(frozen=True)
class Problem:
    """One configuration error. It never holds a secret's value."""

    key: str
    kind: str  # missing | invalid | unknown_enum | out_of_range | conflict | undeclared
    expected: str = ""
    got: str = ""

    def __str__(self) -> str:
        s = f"{self.key}: {self.kind}"
        if self.expected:
            s += f" (expected {self.expected})"
        if self.got:
            s += f" got {self.got!r}"
        return s


_INT = re.compile(r"-?(0|[1-9][0-9]*)")
_DUR = re.compile(r"(0|[1-9][0-9]*)(ms|s|m|h)")
_NAME = re.compile(r"[A-Z][A-Z0-9_]*")
_UNIT = {"ms": timedelta(milliseconds=1), "s": timedelta(seconds=1), "m": timedelta(minutes=1), "h": timedelta(hours=1)}
_INT64 = (-(2**63), 2**63 - 1)

# Injected by BrickKit and never declared by components (P2.3).
PLATFORM_KEYS = ("COMPONENT_ID", "COMPONENT_VERSION", "PORT", "BRICKKIT_SERVED_MEMBERS", "BRICKKIT_SERVED_MEMBERS_CONFIG")


def check_declaration(keys: list[Key]) -> list[Problem]:
    """Declarations the protocol forbids: bad names, _ENDPOINT suffixes, platform names, duplicates, empty enums."""
    out, seen = [], set()
    for k in keys:
        if not _NAME.fullmatch(k.name):
            out.append(Problem(k.name, "undeclared", "UPPER_SNAKE name"))
        elif k.name.endswith("_ENDPOINT") or k.name in PLATFORM_KEYS:
            out.append(Problem(k.name, "undeclared", "a name the platform does not own"))
        elif k.name in seen:
            out.append(Problem(k.name, "undeclared", "declared once"))
        elif k.type == Type.ENUM and not k.values:
            out.append(Problem(k.name, "undeclared", "enum values"))
        seen.add(k.name)
    return out


def _expected(k: Key) -> str:
    return {Type.INTEGER: "a base-10 integer", Type.BOOLEAN: "true or false",
            Type.DURATION: "an integer with ms, s, m or h", Type.ENUM: "one of " + ", ".join(k.values)}.get(k.type, str(k.type))


def _parse_one(k: Key, raw: str) -> tuple[object, str]:
    match k.type:
        case Type.STRING:
            return raw, ""
        case Type.INTEGER:
            if not _INT.fullmatch(raw) or raw == "-0":
                return None, "invalid"
            n = int(raw)
            if not _INT64[0] <= n <= _INT64[1]:
                return None, "invalid"
            if (k.min is not None and n < k.min) or (k.max is not None and n > k.max):
                return None, "out_of_range"
            return n, ""
        case Type.BOOLEAN:
            return (raw == "true", "") if raw in ("true", "false") else (None, "invalid")
        case Type.DURATION:
            m = _DUR.fullmatch(raw)
            if not m or int(m.group(1)) > _INT64[1]:
                return None, "invalid"
            return int(m.group(1)) * _UNIT[m.group(2)], ""
        case Type.ENUM:
            return (raw, "") if raw in k.values else (None, "unknown_enum")
    return None, "invalid"


def _trim(b: bytes) -> str:
    s = b.decode()
    s = s.removesuffix("\n")
    return s.removesuffix("\r")


class ConfigError(Exception):
    """Every configuration problem at once (P2.6); the launcher exits 78."""

    def __init__(self, problems: list[Problem]):
        super().__init__("; ".join(str(p) for p in problems))
        self.problems = problems


def parse(keys: list[Key], src: Source) -> Values:
    """Read every declared key; raises ConfigError with every problem at once (P2.6)."""
    if problems := check_declaration(keys):
        raise ConfigError(problems)
    return _parse(keys, src)


def parse_platform(keys: list[Key], src: Source) -> Values:
    """Read keys the platform owns (COMPONENT_ID, PORT, ...); only the launcher and the shell call it."""
    return _parse(keys, src)


def _parse(keys: list[Key], src: Source) -> Values:
    v = Values({k.name: k for k in keys})
    problems: list[Problem] = []
    for k in keys:
        raw, path = src.lookup(k.name), ""
        if k.secret and hasattr(src, "read_file"):
            file = src.lookup(k.name + "_FILE")
            if file:
                if raw:
                    problems.append(Problem(k.name, "conflict", f"either {k.name} or {k.name}_FILE"))
                    continue
                try:
                    raw, path = _trim(src.read_file(file)), file
                except (OSError, UnicodeDecodeError):
                    problems.append(Problem(k.name + "_FILE", "invalid", "a readable file", file))
                    continue
        if not raw:
            if k.required:
                problems.append(Problem(k.name, "missing", _expected(k)))
            elif k.default is not None:
                v._vals[k.name] = k.default
            continue
        val, kind = _parse_one(k, raw)
        if kind:
            problems.append(Problem(k.name, kind, _expected(k), "" if k.secret else raw[:64]))
            continue
        v._vals[k.name] = val
        if k.secret:
            v._secrets[k.name] = Secret(raw, path)
    if problems:
        raise ConfigError(problems)
    return v


class Secret:
    """A secret value; when it came from a file, refresh re-reads that file (P2.8). Never log value()."""

    def __init__(self, value: str, path: str = ""):
        self._value, self._path, self._lock = value, path, threading.Lock()

    def value(self) -> str:
        return self._value

    @property
    def from_file(self) -> bool:
        return bool(self._path)

    def refresh(self, read: Callable[[str], bytes]) -> bool:
        """Re-read the file; True when the value changed. A failed read raises and keeps the old value."""
        if not self._path:
            return False
        new = _trim(read(self._path))
        with self._lock:
            changed, self._value = new != self._value, new
        return changed


@dataclass
class Values:
    """A component's parsed configuration. Asking for an undeclared key, or with the wrong type, is a programming
    error (raised; the SDK's recovery turns it into INTERNAL)."""

    _keys: dict[str, Key]
    _vals: dict[str, object] = field(default_factory=dict)
    _secrets: dict[str, Secret] = field(default_factory=dict)

    def _get(self, name: str, t: Type) -> object:
        k = self._keys.get(name)
        if k is None:
            raise KeyError(f"config: key {name} is not declared")
        if k.type != t and not (t == Type.STRING and k.type == Type.ENUM):
            raise TypeError(f"config: key {name} is {k.type}, read as {t}")
        return self._vals.get(name)

    def has(self, name: str) -> bool:
        if name not in self._keys:
            raise KeyError(f"config: key {name} is not declared")
        return name in self._vals

    def string(self, name: str) -> str:
        if name in self._secrets:
            return self._secrets[name].value()
        val = self._get(name, Type.STRING)
        return "" if val is None else str(val)

    def int(self, name: str) -> int:
        val = self._get(name, Type.INTEGER)
        return 0 if val is None else int(val)  # type: ignore[call-overload]

    def bool(self, name: str) -> bool:
        return bool(self._get(name, Type.BOOLEAN))

    def duration(self, name: str) -> timedelta:
        val = self._get(name, Type.DURATION)
        return timedelta(0) if val is None else val  # type: ignore[return-value]

    def secret(self, name: str) -> Secret | None:
        k = self._keys.get(name)
        if k is None or not k.secret:
            raise KeyError(f"config: key {name} is not a declared secret")
        return self._secrets.get(name)

    def file_secrets(self) -> list[Secret]:
        """Every secret read from a file, for the runtime's re-read loop."""
        return [s for s in self._secrets.values() if s.from_file]

    def raw(self, name: str) -> tuple[object, bool]:
        """A parsed non-secret value for comparisons across shell members (P2.11)."""
        k = self._keys.get(name)
        if k is None or k.secret or name not in self._vals:
            return None, False
        return self._vals[name], True
