"""The lifecycle resource contract ``/{d}/{n}/_lifecycle/*`` (be-protocol P16.4, P16.8), mounted in full in
every component with a database. Units and holds answer from the platform tables; the operations that need
an adapter this runtime does not have (thaw and export need a cold store; verify, erasure and destruction
arrive with the engine's later steps) answer 501 CAPABILITY_UNAVAILABLE with ``metadata.capability``."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from besdk import errors
from besdk.auth.access import PermKey

if TYPE_CHECKING:
    from besdk.http.router import Router
    from besdk.runtime import Runtime

_UNITS = ("SELECT table_name, unit_key, range_from, range_to, list_value, state, rows, sealed_at, cold_at, "
          "thawed_until, destroyed_at, blocked_reason FROM besdk_lifecycle_units "
          "WHERE ($1::text IS NULL OR table_name = $1) ORDER BY table_name, unit_key LIMIT 1000")
_HOLDS = ("SELECT hold_id, scope, reason, placed_by, placed_at FROM besdk_holds WHERE released_at IS NULL "
          "ORDER BY placed_at")


def keys(component_id: str) -> tuple[PermKey, PermKey, PermKey]:
    base = component_id.replace("/", ".") + ".lifecycle."
    return PermKey(base + "read"), PermKey(base + "thaw"), PermKey(base + "admin")


def _plain(r: Any) -> dict:
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()}


def _unavailable(capability: str):
    async def handler() -> None:
        raise errors.be_error("CAPABILITY_UNAVAILABLE", {"capability": capability})

    return handler


def mount(r: "Router", rt: "Runtime") -> None:
    read, thaw, admin = keys(rt.id)

    @r.get("/_lifecycle/units", guard=read)
    async def lifecycle_units(table: str | None = None) -> dict:
        rows = await rt.store().tx(lambda tx: tx.fetch(_UNITS, table))
        return {"units": [_plain(x) for x in rows]}

    @r.get("/_lifecycle/holds", guard=admin)
    async def lifecycle_holds() -> dict:
        rows = await rt.store().tx(lambda tx: tx.fetch(_HOLDS))
        return {"holds": [_plain(x) for x in rows]}

    for method, path, guard, capability in (
            ("post", "/_lifecycle/units/{table}/{unit}:thaw", thaw, "cold_store"),
            ("get", "/_lifecycle/verify", read, "verify"),
            ("post", "/_lifecycle/exports", read, "cold_store"),
            ("get", "/_lifecycle/exports/{job_id}", read, "cold_store"),
            ("post", "/_lifecycle/holds", admin, "holds"),
            ("delete", "/_lifecycle/holds/{hold_id}", admin, "holds"),
            ("post", "/_lifecycle/erasures", admin, "erasure"),
            ("get", "/_lifecycle/erasures/{request_id}", admin, "erasure"),
            ("get", "/_lifecycle/destructions", admin, "destruction"),
            ("post", "/_lifecycle/destructions/{destruction_id}:approve", admin, "destruction")):
        getattr(r, method)(path, guard=guard)(_unavailable(capability))
