"""What a component's own tests compare its documents with, so they never drift from the code: the configSchema block
of component.yaml (exactly the keys the SDK reads for serve and migrate), events.publishes (exactly the event
contracts) and contract/openapi.yaml (exactly the registered routes). The same checks as be-sdk-go's
launcher.ManifestProblems, launcher.EventProblems and httpx.OpenAPIProblems."""
from __future__ import annotations

import pathlib
from datetime import timedelta

import yaml

from besdk import db, host
from besdk.component import Definition
from besdk.config import Key, Type
from besdk.http import Route

# the keys whose value is a file path BrickKit mounts (the service certificates, re-read on rotation)
FILE_KEYS = {"SERVICE_TLS_CERT_FILE", "SERVICE_TLS_KEY_FILE", "SERVICE_TLS_CA_FILE"}


def _keys(d: Definition) -> list[Key]:
    seen, out = set(), []
    for k in [*host.keys(d), *(db.migrate_keys() if d.migrations is not None else [])]:
        if k.name not in seen:
            seen.add(k.name)
            out.append(k)
    return out


def _describe(k: Key, own: str) -> str:
    parts = [own] if own else []
    if k.type == Type.DURATION:
        parts.append("a duration (e.g. 2s, 500ms)")
    if k.default is not None:
        v = k.default
        if isinstance(v, timedelta):
            v = f"{v.total_seconds():g}s"
        parts.append(f"SDK default {str(v).lower() if isinstance(v, bool) else v}")
    if k.process:
        parts.append("process-level: the same for every member of a shell")
    if k.name in FILE_KEYS:
        parts.append("a file path, re-read on rotation")
    return "; ".join(parts)


def config_schema(d: Definition, own: dict[str, str] | None = None) -> dict:
    """The configSchema block: the component's own keys, the SDK's, the database's and the bus's, with their types,
    secrets and defaults. own describes the component's own keys."""
    own = own or {}
    props: dict = {}
    for k in _keys(d):
        p: dict = {"type": {Type.INTEGER: "integer", Type.BOOLEAN: "boolean"}.get(k.type, "string")}
        if k.type == Type.ENUM:
            p["enum"] = list(k.values)
        if k.secret or k.name in FILE_KEYS:
            p["secret"] = True
        if k.name in FILE_KEYS:
            p["mount"] = "file"
        if text := _describe(k, own.get(k.name, "")):
            p["description"] = text
        props[k.name] = p
    return {"type": "object", "required": sorted(k.name for k in _keys(d) if k.required), "properties": props}


def config_schema_yaml(d: Definition, own: dict[str, str] | None = None) -> str:
    """config_schema as the block of a component.yaml, to paste."""
    return yaml.safe_dump({"configSchema": config_schema(d, own)}, allow_unicode=True, sort_keys=True, indent=2)


def manifest_problems(manifest: str, d: Definition, own: dict[str, str] | None = None) -> list[str]:
    """metadata.name and metadata.version against the definition's id and version (a release that bumps one and not
    the other would refuse to start), then every key of component.yaml's configSchema missing, extra or declared
    differently from config_schema."""
    try:
        doc = yaml.safe_load(manifest) or {}
    except yaml.YAMLError as e:
        return [f"component.yaml: {e}"]
    meta, got = doc.get("metadata") or {}, doc.get("configSchema") or {}
    out = []
    if str(meta.get("name", "")) != d.id:
        out.append(f'metadata.name: "{meta.get("name", "")}", the code is "{d.id}"')
    if str(meta.get("version", "")) != d.version:
        out.append(f'metadata.version: "{meta.get("version", "")}", the code is "{d.version}"')
    want = config_schema(d, own)
    got_req = set(got.get("required") or [])
    out += [f"{k}: not listed as required" for k in want["required"] if k not in got_req]
    out += [f"{k}: listed as required, but the SDK does not require it" for k in sorted(got_req - set(want["required"]))]
    wp, gp = want["properties"], got.get("properties") or {}
    for k in sorted(set(wp) | set(gp)):
        if k not in gp:
            out.append(f"{k}: not declared")
        elif k not in wp:
            out.append(f"{k}: declared, but nothing reads it")
        elif gp[k] != wp[k]:
            out.append(f"{k}: declared as {gp[k]}, the SDK reads {wp[k]}")
    return out


def event_problems(manifest: str, contracts: pathlib.Path) -> list[str]:
    """events.publishes against the event contracts (one <type>.schema.json each in the component's contract/events/)."""
    try:
        listed = set(((yaml.safe_load(manifest) or {}).get("events") or {}).get("publishes") or [])
    except yaml.YAMLError as e:
        return [f"component.yaml: {e}"]
    have = {p.name.removesuffix(".schema.json") for p in contracts.glob("*.schema.json")} if contracts.exists() else set()
    out = [f"{e}: in events.publishes, but there is no {e}.schema.json" for e in listed - have]
    out += [f"{e}: has a contract, but is not in events.publishes" for e in have - listed]
    return sorted(out)


def openapi_problems(document: str, routes: list[Route]) -> list[str]:
    """Every route must be a path and method of the OpenAPI document with the same x-operation (empty for a public
    route), and every operation there must be registered."""
    try:
        paths = (yaml.safe_load(document) or {}).get("paths") or {}
    except yaml.YAMLError as e:
        return [f"openapi: {e}"]
    documented = {}
    for path, item in paths.items():
        for method, op in (item or {}).items():
            if method.upper() in ("GET", "PUT", "POST", "PATCH", "DELETE", "HEAD", "OPTIONS"):
                documented[f"{method.upper()} {path}"] = (op or {}).get("x-operation") or ""
    out, registered = [], set()
    for r in routes:
        key = f"{r.method} {r.path}"
        registered.add(key)
        if key not in documented:
            out.append(f"{key}: registered but not in the OpenAPI document")
        elif documented[key] != r.operation:
            out.append(f"{key}: x-operation {documented[key]!r} in the document, {r.operation!r} registered")
    out += [f"{k}: in the OpenAPI document but not registered" for k in documented if k not in registered]
    return sorted(out)
