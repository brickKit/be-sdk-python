"""The meaning of a bundle for one caller and one resource type (contract-infra-authz EVALUATION.md E6–E12,
be-protocol P6.3–P6.8): the canonical scope parameters a list filters with, the single-record decision,
field masks and the explain facts. Pure functions of a ``Principal`` (E1–E5 live in ``bundle.py``).

The list predicate (``besdk.auth.scope``) and ``decide`` use the same parameters, so a row is in a list
for key K exactly when ``decide(K, row)`` makes it visible (P6.7, E10).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from besdk.auth.bundle import _RANK, Principal, _level_of

_DEPT = re.compile(r"/([^/]+/)*")
IDENTITY = ("owner", "org")


@dataclass(frozen=True)
class ResourceType:
    """One ``resources`` entry (catalog.schema.json ``resource_type``)."""

    type: str
    view_key: str
    owner_component: str = ""
    derivation: str = "direct"
    keys: tuple[str, ...] = ()
    dimensions: tuple[str, ...] = ()
    relations: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    fields: tuple[Mapping[str, Any], ...] = ()
    share: Mapping[str, Any] | None = None
    inherits: tuple[Mapping[str, Any], ...] = ()

    @classmethod
    def of(cls, d: Mapping[str, Any]) -> "ResourceType":
        return cls(type=d["type"], view_key=d["view_key"], owner_component=d.get("owner_component", ""),
                   derivation=d.get("derivation", "direct"), keys=tuple(d.get("keys") or ()),
                   dimensions=tuple(d.get("dimensions") or ()), relations=dict(d.get("relations") or {}),
                   fields=tuple(d.get("fields") or ()), share=d.get("share"), inherits=tuple(d.get("inherits") or ()))

    @property
    def resource_dims(self) -> list[str]:
        return [x for x in self.dimensions if x not in IDENTITY]

    def gives(self, relation: str, key: str, _seen: frozenset[str] = frozenset()) -> bool:
        """E8: K is in the relation's grants or, transitively, in a relation it includes."""
        r = self.relations.get(relation) or {}
        if key in (r.get("grants") or ()):
            return True
        return any(self.gives(i, key, _seen | {relation}) for i in r.get("includes") or () if i not in _seen)

    def capability_of(self, relation: str) -> str:
        return "relation_sync" if (self.relations.get(relation) or {}).get("owned_by") == "component" else "sharing"


@dataclass(frozen=True)
class Row:
    """The facts of one record the owner knows: id, owner sub, department path, resource dimension values."""

    id: str
    owner: str = ""
    dept_path: str = ""
    values: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def of(cls, d: Mapping[str, Any]) -> "Row":
        return cls(str(d["id"]), d.get("owner") or "", d.get("dept_path") or "", dict(d.get("values") or {}))


@dataclass(frozen=True)
class AclRow:
    rtype: str
    rid: str
    relation: str
    subject: str
    expires_at: datetime | None = None

    @classmethod
    def of(cls, d: Mapping[str, Any]) -> "AclRow":
        exp = d.get("expires_at")
        if isinstance(exp, str):
            exp = datetime.fromisoformat(exp.replace("Z", "+00:00"))
        return cls(d["rtype"], str(d["rid"]), d["relation"], d["subject"], exp)

    def counts(self, now: float) -> bool:
        return self.expires_at is None or self.expires_at.timestamp() > now


@dataclass(frozen=True)
class Dim:
    all: bool
    ids: tuple[str, ...]


@dataclass(frozen=True)
class ScopeParams:
    """The parameters of the canonical predicate (P6.5), names as in the spec without ``@``."""

    s_all: bool
    s_owners: tuple[str, ...]
    s_dept_exact: tuple[str, ...]
    s_dept_prefix: tuple[str, ...]
    s_dims: Mapping[str, Dim]
    s_acl: bool
    s_relations: tuple[str, ...]
    s_subjects: tuple[str, ...]
    s_graph_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {"s_acl": self.s_acl, "s_all": self.s_all, "s_dept_exact": list(self.s_dept_exact),
                "s_dept_prefix": list(self.s_dept_prefix),
                "s_dims": {d: {"all": v.all, "ids": list(v.ids)} for d, v in sorted(self.s_dims.items())},
                "s_graph_ids": list(self.s_graph_ids), "s_owners": list(self.s_owners),
                "s_relations": list(self.s_relations), "s_subjects": list(self.s_subjects)}


def _s(xs: Iterable[str]) -> tuple[str, ...]:
    """Deduplicated, ascending by byte order (EVALUATION.md, Ordering)."""
    return tuple(sorted(set(xs), key=lambda s: s.encode()))


def valid_dept(path: str) -> bool:
    return bool(path) and bool(_DEPT.fullmatch(path))


def like_prefix(path: str) -> str:
    """E6 prefix encoding: ``\\``, ``%``, ``_`` escaped with ``\\``, then ``%``."""
    return re.sub(r"([\\%_])", r"\\\1", path) + "%"


def _cap(p: Principal) -> str | None:
    """The lowest max_level of the ceilings; None without ceilings."""
    if not p.ceilings:
        return None
    return min((c.get("max_level", "own") for c in p.ceilings), key=_RANK.__getitem__)


def _values(p: Principal, key: str, dim: str) -> set[str]:
    if not p.ceilings_allow(key):
        return set()
    grants = p.bundle.raw.get("grants", {})
    out: set[str] = set()
    for r in p.holders(key):
        out.update((grants.get(r, {}).get("values") or {}).get(dim) or ())
    return out


def _org_values(p: Principal, key: str) -> set[str]:
    vals, cap = _values(p, key, "org"), _cap(p)
    if cap is not None and _RANK[cap] < _RANK["subtree"]:
        vals = {v for v in vals if v == "*"}
    if cap is not None and _RANK[cap] < _RANK["all"]:
        vals.discard("*")
    return vals


def subjects(p: Principal, key: str) -> tuple[str, ...]:
    """E8 / P6.4: S(K)."""
    out = {f"user:{p.claims.get('sub', '')}"} | {f"role:{r}" for r in p.roles}
    dept = p.claims.get("dept_path") or ""
    if isinstance(dept, str) and valid_dept(dept):
        out.add(f"dept:{dept}")
        segs = [x for x in dept.split("/") if x]
        out.update("dept_tree:/" + "".join(s + "/" for s in segs[:i]) for i in range(len(segs) + 1))
    out.update(f"user:{d['from']}" for d in p.delegations(key))
    return _s(out)


def relations(p: Principal, key: str, rt: ResourceType) -> tuple[str, ...]:
    if not p.ceilings_allow(key):
        return ()
    caps = p.bundle.raw.get("capabilities", {})
    out = [r for r in rt.relations if rt.gives(r, key) and caps.get(rt.capability_of(r)) is True]
    for c in p.ceilings:
        out = [r for r in out if r in (c.get("relations") or ())]
    return _s(out)


def scope_params(p: Principal, key: str, rt: ResourceType, *, graph_ids: Sequence[str] | None = None) -> ScopeParams:
    """E6–E9: the canonical predicate's parameters for route key K on type T."""
    level, sub = p.level(key), p.claims.get("sub", "")
    dept = p.claims.get("dept_path") or ""
    dept_ok = isinstance(dept, str) and valid_dept(dept)
    org = _org_values(p, key)
    allowed = p.ceilings_allow(key)
    owners = ({sub} if level != "none" else set()) | ({d["from"] for d in p.delegations(key)} if allowed else set())
    prefix = {like_prefix(dept)} if level == "subtree" and dept_ok else set()
    prefix |= {like_prefix(v) for v in org if v != "*" and valid_dept(v)}
    dims = {}
    for d in rt.resource_dims:
        vals = _values(p, key, d)
        dims[d] = Dim("*" in vals, _s(vals - {"*"}))
    rels = relations(p, key, rt)
    graph = _s(graph_ids or ()) if rt.derivation == "graph" and p.bundle.capability("graph") and allowed else ()
    return ScopeParams(
        s_all=level != "none" and (level == "all" or "*" in org), s_owners=_s(owners),
        s_dept_exact=(dept,) if level == "dept" and dept_ok else (), s_dept_prefix=_s(prefix), s_dims=dims,
        s_acl=bool(rels), s_relations=rels, s_subjects=subjects(p, key), s_graph_ids=graph)


def degraded(p: Principal, rt: ResourceType) -> list[str]:
    """E9: ``graph`` for a graph type on a member without the capability (header X-Authz-Degraded)."""
    return ["graph"] if rt.derivation == "graph" and not p.bundle.capability("graph") else []


def fields(p: Principal, rt: ResourceType) -> dict[str, list[str]]:
    """E11: masked columns (no read key) and read-only columns (read without edit)."""
    masked, ro = set(), set()
    for f in rt.fields:
        if not p.has(f["read"]):
            masked.update(f["columns"])
        elif not f.get("edit") or not p.has(f["edit"]):
            ro.update(f["columns"])
    return {"masked": list(_s(masked)), "read_only": list(_s(ro))}


# --- one record (E10) -----------------------------------------------------------------------------


def _like(pattern: str, value: str) -> bool:
    """SQL LIKE with backslash escape, as PostgreSQL evaluates the canonical predicate."""
    rx, i = "", 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "\\" and i + 1 < len(pattern):
            rx += re.escape(pattern[i + 1])
            i += 2
            continue
        rx += ".*" if ch == "%" else "." if ch == "_" else re.escape(ch)
        i += 1
    return re.fullmatch(rx, value, re.S) is not None


def identity_holds(rt: ResourceType, sp: ScopeParams, row: Row) -> bool:
    has_owner, has_org = "owner" in rt.dimensions, "org" in rt.dimensions
    if not (has_owner or has_org) or sp.s_all:
        return True
    if has_owner and row.owner in sp.s_owners:
        return True
    return has_org and (row.dept_path in sp.s_dept_exact or any(_like(x, row.dept_path) for x in sp.s_dept_prefix))


def failing_dims(rt: ResourceType, sp: ScopeParams, row: Row) -> list[str]:
    return [d for d in rt.resource_dims if not (sp.s_dims[d].all or row.values.get(d) in sp.s_dims[d].ids)]


def matching_acl(rt: ResourceType, sp: ScopeParams, row: Row, acl: Sequence[AclRow], now: float) -> list[AclRow]:
    if not sp.s_acl:
        return []
    return [a for a in acl if a.rtype == rt.type and a.rid == row.id and a.relation in sp.s_relations
            and a.subject in sp.s_subjects and a.counts(now)]


def rule_holds(p: Principal, key: str, rt: ResourceType, sp: ScopeParams, row: Row) -> bool:
    return p.has(key) and identity_holds(rt, sp, row) and not failing_dims(rt, sp, row)


def vis(p: Principal, key: str, rt: ResourceType, row: Row, *, acl: Sequence[AclRow] = (),
        graph_ids: Sequence[str] | None = None) -> bool:
    sp = scope_params(p, key, rt, graph_ids=graph_ids)
    return (rule_holds(p, key, rt, sp, row) or bool(matching_acl(rt, sp, row, acl, p.now))
            or row.id in sp.s_graph_ids)


@dataclass(frozen=True)
class Decision:
    visible: bool
    allowed: bool
    reason: str

    def err(self) -> Any:
        """The refusal to raise (404 when not visible, R62); None when allowed."""
        from besdk.errors import be_error

        return None if self.allowed else be_error(self.reason)


def decide(p: Principal, key: str, rt: ResourceType, row: Row, *, acl: Sequence[AclRow] = (),
           graph_ids: Sequence[str] | None = None) -> Decision:
    """E10: visible by the type's view_key, allowed by the route key K."""
    visible = vis(p, rt.view_key, rt, row, acl=acl, graph_ids=graph_ids)
    allowed = visible and vis(p, key, rt, row, acl=acl, graph_ids=graph_ids)
    if not visible:
        return Decision(False, False, "NOT_FOUND")
    if allowed:
        return Decision(True, True, "")
    return Decision(True, False, "OUT_OF_SCOPE" if p.has(key) else "MISSING_PERMISSION")


# --- explain (E12) --------------------------------------------------------------------------------


def _fact(kind: str, source: str, detail: str) -> dict[str, str]:
    return {"detail": detail, "kind": kind, "source": source}


def _sorted(facts: list[dict[str, str]]) -> list[dict[str, str]]:
    uniq = {(f["kind"], f["source"], f["detail"]): f for f in facts}
    return [uniq[k] for k in sorted(uniq, key=lambda t: tuple(x.encode() for x in t))]


def _reasons(p: Principal, key: str, rt: ResourceType, sp: ScopeParams, row: Row, acl: Sequence[AclRow]) -> list:
    out = []
    if rule_holds(p, key, rt, sp, row):
        holders = p.holders(key)
        out += [_fact("role_key", h, key) for h in holders]
        if holders and any(d in rt.dimensions for d in IDENTITY):
            grants = p.bundle.raw.get("grants", {})
            best = max(_RANK[_level_of(grants.get(h, {}), key)] for h in holders)
            first = sorted(h for h in holders if _RANK[_level_of(grants.get(h, {}), key)] == best)[0]
            out.append(_fact("level", first, p.level(key)))
        out += [_fact("dimension", d, row.values.get(d, "")) for d in rt.resource_dims]
        out += [_fact("delegation", d.get("id", ""), d["from"]) for d in p.delegations(key) if d["from"] == row.owner]
        out += [_fact("ceiling", c, key) for c in p.claims.get("ceil") or ()]
    for a in matching_acl(rt, sp, row, acl, p.now):
        out.append(_fact("relation" if rt.capability_of(a.relation) == "relation_sync" else "share", a.relation,
                         a.subject))
    if row.id in sp.s_graph_ids:
        out.append(_fact("relation", "graph", row.id))
    return out


def _missing(p: Principal, key: str, rt: ResourceType, sp: ScopeParams, row: Row, visible: bool) -> list:
    out = []
    refusing = [c for c, prof in zip(p.claims.get("ceil") or (), p.ceilings)
                if key not in (prof.get("keys") or ()) and key not in (prof.get("fields") or ())]
    if refusing:
        out += [_fact("ceiling", c, key) for c in refusing]
    elif not (p.holders(key) or p.delegations(key)):
        out.append(_fact("role_key", "", key))
    else:
        if not identity_holds(rt, sp, row):
            out.append(_fact("level", "", p.level(key)))
        out += [_fact("dimension", d, row.values.get(d, "") if visible else "") for d in failing_dims(rt, sp, row)]
    caps = p.bundle.raw.get("capabilities", {})
    out += [_fact("capability", rt.capability_of(r), "") for r in rt.relations
            if rt.gives(r, key) and caps.get(rt.capability_of(r)) is not True]
    if rt.derivation == "graph" and not p.bundle.capability("graph"):
        out.append(_fact("capability", "graph", ""))
    return out


def explain(p: Principal, key: str, rt: ResourceType, row: Row, *, acl: Sequence[AclRow] = (),
            graph_ids: Sequence[str] | None = None) -> dict[str, list[dict[str, str]]]:
    """E12: ``reasons`` when K holds on the record, else ``missing``; R62 hides attributes of invisible rows."""
    sp = scope_params(p, key, rt, graph_ids=graph_ids)
    if vis(p, key, rt, row, acl=acl, graph_ids=graph_ids):
        return {"missing": [], "reasons": _sorted(_reasons(p, key, rt, sp, row, acl))}
    visible = vis(p, rt.view_key, rt, row, acl=acl, graph_ids=graph_ids)
    return {"missing": _sorted(_missing(p, key, rt, sp, row, visible)), "reasons": []}


def now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()
