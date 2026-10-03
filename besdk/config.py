"""The component's configuration (be-protocol P2): read once from the platform, validated against the
image's ``component.yaml`` ``configSchema`` and the protocol key catalogue, then read with typed getters
that never fail at run time. Secrets are files, re-read when they change (P2.7, P2.9)."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from functools import cache
from importlib import resources
from pathlib import Path
from typing import Any, Callable, Mapping

import yaml

from besdk import config_values as V
from besdk.errors import ProtocolError

SECRET_POLL = 10.0  # seconds between two looks at a secret file (P2.9: at most 30 s)


@cache
def catalogue() -> dict[str, dict]:
    """The protocol keys of be-protocol ``schemas/config-keys.yaml`` by name."""
    doc = yaml.safe_load(resources.files("besdk").joinpath("_protocol/config-keys.yaml").read_text())
    return {k["name"]: k for k in doc["keys"]}


@dataclass(frozen=True)
class Dependency:
    id: str
    version: str
    optional: bool


@dataclass(frozen=True)
class Manifest:
    """What the SDK reads from the image's ``component.yaml``."""

    id: str
    version: str
    port: int
    extra_ports: dict[str, int]
    dependencies: tuple[Dependency, ...]
    properties: Mapping[str, Mapping[str, Any]]
    required: frozenset[str]
    stop_grace: int = 30
    doc: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_doc(cls, doc: Mapping[str, Any]) -> "Manifest":
        deps = []
        for d in (doc.get("dependencies") or {}).get("components") or ():
            ref, opt = (d, False) if isinstance(d, str) else (d["id"], bool(d.get("optional")))
            cid, _, ver = ref.partition("@")
            deps.append(Dependency(cid, ver, opt))
        dep = doc.get("deployment") or {}
        schema = doc.get("configSchema") or {}
        return cls(id=doc["metadata"]["id"], version=str(doc["metadata"]["version"]), port=int(dep.get("port", 0)),
                   extra_ports={p["name"]: int(p["port"]) for p in dep.get("extraPorts") or ()},
                   dependencies=tuple(deps), properties=schema.get("properties") or {},
                   required=frozenset(schema.get("required") or ()),
                   stop_grace=int(dep.get("stopGracePeriodSeconds", 30)), doc=doc)

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        return cls.from_doc(yaml.safe_load(Path(path).read_text()))


class ConfigErrors(Exception):
    """Every configuration problem found at start (P1.2: one log line each, exit 78)."""

    def __init__(self, errors: list[ProtocolError]):
        super().__init__("; ".join(str(e) for e in errors))
        self.errors = errors


def _yaml_default(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        import json

        return json.dumps(v, separators=(",", ":"))
    return str(v)


_TYPE_FORMAT = {"string": "string", "integer": "integer", "number": "number", "boolean": "boolean",
                "array": "json", "object": "json"}


def spec_of(name: str, item: Mapping[str, Any], required: bool) -> V.KeySpec:
    """A key's parse rules: the catalogue's format for a protocol key, the schema type otherwise."""
    row = catalogue().get(name)
    default = _yaml_default(item.get("default"))
    secret = bool(item.get("secret"))
    if row is not None:
        fmt = {"int": "integer", "bool": "boolean", "durations": "duration_list"}.get(row["format"], row["format"])
        if default is None:
            default = row.get("default")
        return V.KeySpec(name, fmt, required, default, secret or bool(row.get("secret")),
                         schemes=tuple(row.get("schemes") or ()), enum=tuple(row.get("enum") or ()),
                         json_kind="object" if fmt == "json" else None)
    fmt = _TYPE_FORMAT.get(item.get("type", "string"), "string")
    return V.KeySpec(name, fmt, required, default, secret, minimum=item.get("minimum"),
                     enum=tuple(item.get("enum") or ()) if fmt == "string" else (),
                     json_kind={"array": "array", "object": "object"}.get(item.get("type", "")))


class Secret:
    """A file-delivered secret: read at start, re-read when the file's mtime or size changes (P2.9)."""

    def __init__(self, key: str, path: str, required: bool):
        self.key, self.path, self.required = key, path, required
        self.on_reload: Callable[[str], None] | None = None
        self.on_failure: Callable[[str, Exception], None] | None = None
        self._listeners: list[Callable[[], None]] = []
        self._stat = self._stat_now()
        self._bytes = self._read()
        self._checked = time.monotonic()

    def _stat_now(self) -> tuple[int, int]:
        st = os.stat(self.path)
        return st.st_mtime_ns, st.st_size

    def _read(self) -> bytes:
        data = Path(self.path).read_bytes()
        V.secret_text(data.decode("utf-8", "replace"), required=self.required, key=self.key)
        return data

    def poll(self) -> bool:
        """Look at the file; re-read when it changed. True when a new value was taken."""
        self._checked = time.monotonic()
        try:
            st = self._stat_now()
            if st == self._stat:
                return False
            self._stat = st
            self._bytes = self._read()
        except (OSError, ProtocolError) as e:
            if self.on_failure:
                self.on_failure(self.key, e)
            return False
        if self.on_reload:
            self.on_reload(self.key)
        for cb in list(self._listeners):
            cb()
        return True

    def current(self) -> str:
        """The text value: exactly one trailing LF / CRLF removed."""
        if time.monotonic() - self._checked > SECRET_POLL:
            self.poll()
        return V.secret_text(self._bytes.decode("utf-8"), required=False, key=self.key) or ""

    def bytes(self) -> bytes:
        """A component's own binary secret, byte for byte."""
        if time.monotonic() - self._checked > SECRET_POLL:
            self.poll()
        return self._bytes

    def on_change(self, cb: Callable[[], None]) -> None:
        """Called after every successful re-read (signing keys, credential pairs)."""
        self._listeners.append(cb)


class Config:
    """Only the keys ``configSchema`` declares, plus the platform's names; all validated at start."""

    def __init__(self, env: Mapping[str, str], manifest: Manifest, specs: dict[str, V.KeySpec],
                 values: dict[str, V.Parsed], texts: dict[str, str | None], secrets: dict[str, Secret]):
        self._env = env
        self.manifest = manifest
        self._specs = specs
        self._values = values
        self._texts = texts
        self._secrets = secrets

    # --- construction --------------------------------------------------------------------------

    @classmethod
    def load(cls, env: Mapping[str, str], manifest: Manifest) -> "Config":
        """Parse every declared key; collect all errors and raise them together (exit 78)."""
        errors: list[ProtocolError] = []
        cid = env.get("COMPONENT_ID")
        if cid is not None and cid != manifest.id:
            errors.append(ProtocolError("CONFIG_INVALID", f"COMPONENT_ID {cid} is not {manifest.id}",
                                        key="COMPONENT_ID"))
        specs = {k: spec_of(k, item, k in manifest.required) for k, item in manifest.properties.items()}
        values: dict[str, V.Parsed] = {}
        texts: dict[str, str | None] = {}
        secrets: dict[str, Secret] = {}
        for name, spec in specs.items():
            try:
                texts[name], values[name] = cls._parse_one(env, specs, name)
                if name in V.FAMILY_KEYS:
                    V.family_address(name, env.get(name) or None)
                if spec.secret and values[name].set:
                    secrets[name] = Secret(name, values[name].value, spec.required)
            except ProtocolError as e:
                errors.append(e if e.key else ProtocolError(e.reason, f"{name}: {e.detail}", key=name))
            except OSError as e:
                errors.append(ProtocolError("CONFIG_INVALID", f"{name}: cannot read the secret file ({e.strerror})",
                                            key=name))
        errors.extend(cls._check_endpoints(env, manifest))
        if errors:
            raise ConfigErrors(errors)
        return cls(env, manifest, specs, values, texts, secrets)

    @staticmethod
    def _parse_one(env: Mapping[str, str], specs: dict[str, V.KeySpec], name: str) -> tuple[str | None, V.Parsed]:
        """(effective text, parsed value): the env value, else ``default_from``'s value, else the default."""
        spec = specs[name]
        raw = env.get(name)
        if raw in (None, "") and not spec.secret:
            src = catalogue().get(name, {}).get("default_from")
            if src and src in specs and env.get(src) not in (None, ""):
                raw = env[src]
        parsed = V.parse_value(spec, raw)
        if not parsed.set:
            return None, parsed
        empty = raw is None or (raw == "" and (spec.format != "string" or spec.secret))
        return (spec.default if empty else raw), parsed

    @staticmethod
    def _check_endpoints(env: Mapping[str, str], manifest: Manifest) -> list[ProtocolError]:
        errors = []
        for dep in manifest.dependencies:
            name = V.endpoint_name(dep.id, "")
            try:
                if V.endpoint_address(env.get(name)) is None and not dep.optional:
                    errors.append(ProtocolError("CONFIG_MISSING", f"{name} (required dependency {dep.id})",
                                                key=name))
            except ProtocolError as e:
                errors.append(ProtocolError(e.reason, e.detail, key=name))
        return errors

    # --- reads ---------------------------------------------------------------------------------

    def _get(self, key: str) -> V.Parsed:
        V.check_readable(key, set(self._specs))
        p = self._values.get(key)
        if p is None:  # a platform name
            raw = self._env.get(key)
            return V.Parsed(raw is not None, raw)
        return p

    def has(self, key: str) -> bool:
        return self._get(key).set

    def raw(self, key: str) -> str | None:
        """The value as written (after defaults), or None when not set."""
        p = self._get(key)
        if key not in self._specs:
            return p.value
        return self._texts.get(key)

    def require(self, key: str) -> str:
        v = self.raw(key)
        if v is None:
            raise ProtocolError("CONFIG_MISSING", key, key=key)
        return v

    def string(self, key: str, default: str = "") -> str:
        v = self.raw(key)
        return default if v is None else v

    def _typed(self, key: str, fmt: str) -> Any:
        p = self._get(key)
        if not p.set:
            return None
        spec = self._specs.get(key)
        if spec is not None and spec.format == fmt and not spec.secret:
            return p.value
        return V.parse_value(V.KeySpec(key, fmt), self.raw(key)).value

    def int(self, key: str, default: int = 0) -> int:
        v = self._typed(key, "integer")
        return default if v is None else v

    def bool(self, key: str, default: bool = False) -> bool:
        v = self._typed(key, "boolean")
        return default if v is None else v

    def duration(self, key: str, default: float = 0.0) -> float:
        """A Go-syntax duration in seconds."""
        v = self._typed(key, "duration")
        return default if v is None else v / 1e9

    def durations(self, key: str, default: list[float] | None = None) -> list[float]:
        v = self._typed(key, "duration_list")
        return list(default or []) if v is None else [x / 1e9 for x in v]

    def json(self, key: str, default: Any = None) -> Any:
        v = self._typed(key, "json")
        return default if v is None else v

    def secret(self, key: str) -> Secret:
        """Only for ``_FILE`` keys (P2.12)."""
        spec = self._specs.get(key)
        if spec is None or not spec.secret:
            raise ProtocolError("CONFIG_KEY_INVALID", f"{key} is not a secret (_FILE) key", key=key)
        if key not in self._secrets:
            raise ProtocolError("CONFIG_MISSING", key, key=key)
        return self._secrets[key]

    def secrets(self) -> list[Secret]:
        return list(self._secrets.values())

    def family(self, key: str) -> str | None:
        """A slot-family address: REST base for ``*_URL``, dial target for ``*_GRPC_URL`` (P2.10)."""
        self._get(key)
        return V.family_address(key, self._env.get(key) or None)

    def endpoint(self, dependency: str, port: str = "") -> str | None:
        """``host:port`` of a dependency's port; None when the optional dependency is absent (P2.5)."""
        return V.endpoint_address(self._env.get(V.endpoint_name(dependency, port)))

    def declared(self) -> list[str]:
        return list(self._specs)
