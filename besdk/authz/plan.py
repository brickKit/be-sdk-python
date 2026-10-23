"""Compiling a subject's segments into a list's SQL (P8.17, P8.18): a predicate to AND with the request's own filter,
its arguments, one boolean expression per segment whose results give each row's readable fields, and the plan's
digest a cursor carries (security 07). The same compilation as be-sdk-go's authz.Compile."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime

from besdk import errs
from besdk.authz.defs import SECRET, Resource
from besdk.authz.segment import Atom, Segment


@dataclass
class Plan:
    where: str
    args: list = field(default_factory=list)
    hits: list[str] = field(default_factory=list)
    digest: str = ""
    sets: list[list[str]] = field(default_factory=list)

    def fields(self, hits: list[bool]) -> list[str]:
        """A row's readable fields from its hit bitmap (never a secret field)."""
        seen: set[str] = set()
        for i, h in enumerate(hits):
            if h and i < len(self.sets):
                seen.update(self.sets[i])
        return sorted(seen)

    def readable_everywhere(self, name: str) -> bool:
        """Every segment of the plan reads the field: every visible row can be filtered, sorted or searched by it
        without revealing it (security 07's sufficient condition, simplified to one segment per row: P9.4)."""
        return bool(self.sets) and all(name in s for s in self.sets)


def ident(*parts: str) -> str:
    """A quoted, qualified identifier."""
    return ".".join('"' + p.replace('"', '""') + '"' for p in parts if p)


def _like_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class _Compiler:
    def __init__(self, r: Resource, alias: str, principal: str, base: int):
        self.r, self.alias, self.principal, self.base, self.args = r, alias, principal, base, []

    def arg(self, v) -> str:
        self.args.append(v)
        return f"${self.base + len(self.args)}"

    def col(self, name: str) -> str:
        return ident(self.alias, name)

    def atom(self, dim: str, a: Atom) -> str:
        d = (self.r.dimensions or {})[dim]
        col = self.col(d.column)
        match a.kind:
            case "all":
                return ""
            case "in":
                return f"{col} = ANY({self.arg(list(a.values))})" if a.values else "FALSE"
            case "self":
                return f"{col} = {self.arg(self.principal)}"
            case "tree":
                parts = []
                for it in a.items:
                    if it.mode == "subtree":
                        parts.append(f"({col} = {self.arg(it.path)} OR {col} LIKE {self.arg(_like_escape(it.path) + '/%')} "
                                     "ESCAPE '\\')")
                    else:
                        parts.append(f"{col} = {self.arg(it.path)}")
                return "(" + " OR ".join(parts) + ")" if parts else "FALSE"
            case "interval":
                parts = [f"{col} IS NOT NULL", f"{self.col(d.currency_column)} = {self.arg(a.currency)}"]
                if a.lo is not None:
                    parts.append(f"{col} {'>=' if a.lo_closed else '>'} {self.arg(a.lo)}::text::numeric")
                if a.hi is not None:
                    parts.append(f"{col} {'<=' if a.hi_closed else '<'} {self.arg(a.hi)}::text::numeric")
                return "(" + " AND ".join(parts) + ")"
        raise errs.new("QUERY_NOT_COMPLETABLE", "a segment cannot be compiled into the query")

    def segment(self, s: Segment) -> str:
        parts = [p for dim in sorted(s.atoms) if (p := self.atom(dim, s.atoms[dim]))]  # a stable text
        return "(" + " AND ".join(parts) + ")" if parts else "TRUE"


def _or_all(preds: list[str]) -> str:
    return "(" + " OR ".join(preds) + ")" if preds else "FALSE"


def compile_plan(r: Resource, segs: list[Segment], op: str, principal: str, at: datetime, alias: str = "",
                 arg_base: int = 0) -> Plan:
    """The list plan for operation op over a table alias (empty: unqualified columns); placeholders start after
    arg_base. Visible = the view operation's segments; for another operation the list is visible AND permitted, and the
    fields come from that operation's segments."""
    c = _Compiler(r, alias, principal, arg_base)

    def active(key: str) -> list[Segment]:
        return [s for s in segs if s.operation == key and s.active_at(at)]

    view = active(r.view_operation)
    view_preds = [c.segment(s) for s in view]
    where, field_segs, hit_preds, used = _or_all(view_preds), view, view_preds, list(view)
    if op != r.view_operation:
        op_segs = active(op)
        op_preds = [c.segment(s) for s in op_segs]
        where = where + " AND " + _or_all(op_preds)
        field_segs, hit_preds, used = op_segs, op_preds, view + op_segs
    sets = [[f for f in s.read_fields if f in r.fields and r.fields[f].cls != SECRET] for s in field_segs]
    digests = sorted(s.digest() for s in used)
    digest = hashlib.sha256("\n".join(digests + [r.digest(), op]).encode()).hexdigest()[:32]
    return Plan(where, c.args, hit_preds, digest, sets)
