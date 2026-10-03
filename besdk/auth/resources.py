"""The resource types a component owns (``assembly.yaml`` ``resources``, be-protocol P6.10, P6.12) with the
columns of their tables (``data_scopes``). Declaring one mounts the resource contract and the projection."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from besdk.auth.evaluate import ResourceType
from besdk.auth.scope import Columns


@dataclass(frozen=True)
class Declared:
    rtype: ResourceType
    columns: Columns
    table: str | None


def assembly(manifest_dir: Path) -> dict[str, Any]:
    p = Path(manifest_dir) / "assembly.yaml"
    return (yaml.safe_load(p.read_text()) or {}) if p.exists() else {}


def load(manifest_dir: Path, component_id: str) -> dict[str, Declared]:
    doc = assembly(manifest_dir)
    out: dict[str, Declared] = {}
    for r in doc.get("resources") or ():
        rt = ResourceType.of({"owner_component": component_id, **r})
        out[rt.type] = Declared(rt, Columns.from_data_scopes(r.get("table"), doc.get("data_scopes")), r.get("table"))
    return out


def pulled_types(declared: dict[str, Declared]) -> list[str]:
    """The projection pulls the component's own types and the external types they inherit from (P6.12)."""
    out = set(declared)
    for d in declared.values():
        out.update(i["from"] for i in d.rtype.inherits if i.get("from"))
    return sorted(out)
