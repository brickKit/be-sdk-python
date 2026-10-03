"""The canonical list predicate (be-protocol P6.5): one static, parameterised SQL fragment per resource type.

Its parameters are the evaluated scope (E6–E9) and nothing else; its column names come from the
declaration (``assembly.yaml`` ``data_scopes`` for the type's table), never from a request. It adds a
``has(K)`` guard to the rule branch, so a type without identity dimensions still matches nothing for a
caller without K. No row-level security (0206).

    sql, args = besdk.access().scope("conformance.widget.widget").predicate("w", start=2)
    rows = await tx.fetch(f"SELECT … FROM widgets w WHERE w.created_at > $1 AND {sql} ORDER BY …", since, *args)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Mapping

from besdk.auth import evaluate as E
from besdk.auth.bundle import Principal

_IDENT = re.compile(r"[a-z_][a-z0-9_]*")


def _ident(name: str) -> str:
    if not isinstance(name, str) or not _IDENT.fullmatch(name):
        raise ValueError(f"{name!r} is not a plain SQL identifier")
    return name


@dataclass(frozen=True)
class Columns:
    """Columns of the type's table: record id, owner sub, department path, and one per resource dimension."""

    id: str = "id"
    owner: str = "owner_id"
    org: str = "dept_path"
    dims: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for c in (self.id, self.owner, self.org, *self.dims.values()):
            _ident(c)

    @classmethod
    def from_data_scopes(cls, table: str | None, data_scopes: Any) -> "Columns":
        """``data_scopes: [{dimension, column, tables}]`` of assembly.yaml, for ``table``."""
        cols: dict[str, str] = {}
        for d in data_scopes if isinstance(data_scopes, list) else ():
            if table is None or table in (d.get("tables") or [table]):
                cols[d["dimension"]] = d["column"]
        owner, org = cols.pop("owner", "owner_id"), cols.pop("org", "dept_path")
        return cls(owner=owner, org=org, dims=cols)

    def dim(self, d: str) -> str:
        return self.dims.get(d, d)


@dataclass(frozen=True)
class Scope:
    rtype: E.ResourceType
    params: E.ScopeParams
    has: bool
    columns: Columns
    degraded: tuple[str, ...] = ()

    @classmethod
    def of(cls, p: Principal, key: str, rtype: E.ResourceType, columns: Columns,
           graph_ids: list[str] | None = None) -> "Scope":
        return cls(rtype, E.scope_params(p, key, rtype, graph_ids=graph_ids), p.has(key), columns,
                   tuple(E.degraded(p, rtype)))

    def predicate(self, alias: str = "o", start: int = 1) -> tuple[str, list[Any]]:
        """``(sql, args)``: the fragment uses ``$start`` … and ``args`` holds their values in order."""
        a, c, sp = _ident(alias), self.columns, self.params
        args: list[Any] = []

        def arg(v: Any, cast: str = "") -> str:
            args.append(v)
            return f"${start + len(args) - 1}{cast}"

        rule = [arg(self.has, "::bool")]
        dims = self.rtype.dimensions
        if "owner" in dims or "org" in dims:
            ident = [arg(sp.s_all, "::bool")]
            ident.append(f"{a}.{c.owner} = ANY({arg(list(sp.s_owners), '::text[]')})" if "owner" in dims else "false")
            if "org" in dims:
                ident.append(f"{a}.{c.org} = ANY({arg(list(sp.s_dept_exact), '::text[]')})")
                ident.append(f"{a}.{c.org} LIKE ANY({arg(list(sp.s_dept_prefix), '::text[]')})")
            rule.append("(" + " OR ".join(ident) + ")")
        for d in self.rtype.resource_dims:
            v = sp.s_dims[d]
            rule.append(f"({arg(v.all, '::bool')} OR {a}.{_ident(c.dim(d))}::text = ANY({arg(list(v.ids), '::text[]')}))")
        acl = (f"({arg(sp.s_acl, '::bool')} AND EXISTS (SELECT 1 FROM besdk_authz_acl acl "
               f"WHERE acl.rtype = {arg(self.rtype.type, '::text')} AND acl.rid = {a}.{c.id}::text "
               f"AND acl.relation = ANY({arg(list(sp.s_relations), '::text[]')}) "
               f"AND acl.subject = ANY({arg(list(sp.s_subjects), '::text[]')}) "
               f"AND (acl.expires_at IS NULL OR acl.expires_at > now())))")
        graph = f"{a}.{c.id}::text = ANY({arg(list(sp.s_graph_ids), '::text[]')})"
        return f"(({' AND '.join(rule)}) OR {acl} OR {graph})", args
