"""Complete segments and their single-record three-valued evaluation (protocol P8.14-P8.16; security 01, 07, 09a)."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
import re
from decimal import Decimal
from enum import Enum

from besdk import canon, errs
from besdk.authz.defs import AMOUNT, ORG_TREE, PRINCIPAL, SECRET, Resource


class Tri(Enum):
    """A three-valued result (security 07)."""

    FALSE = "false"
    TRUE = "true"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Fact:
    """One fact of a record with explicit presence: a value, a known null, or not obtained."""

    state: str  # value, null, unobtained
    value: str = ""
    currency: str = ""  # amount facts

    @staticmethod
    def of(v: str) -> Fact:
        return Fact("value", v)

    @staticmethod
    def money(amount: str, currency: str) -> Fact:
        return Fact("value", amount, currency)


NULL, UNOBTAINED = Fact("null"), Fact("unobtained")
_DECIMAL = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?")


@dataclass
class TreeItem:
    path: str
    mode: str  # exact, subtree


@dataclass
class Atom:
    """A segment's condition on one dimension."""

    kind: str  # all, in, tree, self, interval
    values: list[str] = field(default_factory=list)
    items: list[TreeItem] = field(default_factory=list)
    currency: str = ""
    lo: str | None = None
    lo_closed: bool | None = None
    hi: str | None = None
    hi_closed: bool | None = None

    @staticmethod
    def from_dict(d: dict) -> Atom:
        return Atom(d["kind"], list(d.get("values") or []), [TreeItem(i["path"], i["mode"]) for i in d.get("items") or []],
                    d.get("currency", ""), d.get("lo"), d.get("lo_closed"), d.get("hi"), d.get("hi_closed"))


def _instant(s: str) -> datetime:
    """An RFC 3339 instant: a time without an offset (or a bare date) is not one (Go's time.Parse refuses it too)."""
    t = datetime.fromisoformat(s)
    if t.tzinfo is None or "T" not in s.upper():
        raise ValueError(f"not an RFC 3339 instant: {s!r}")
    return t


@dataclass
class Segment:
    """One complete segment: one resource type, one operation, one atom per declared dimension."""

    operation: str
    atoms: dict[str, Atom]
    read_fields: list[str]
    valid_from: str
    valid_until: str = ""  # left-closed, right-open; empty means no fixed end
    resource_type: str = ""
    write_fields: list[str] = field(default_factory=list)
    binding: str = ""
    source: str = ""
    definition: dict | None = None

    @staticmethod
    def from_dict(d: dict) -> Segment:
        """A segment as the authorization view carries it (security 09a)."""
        valid = d.get("valid") or {}
        return Segment(d["operation"], {k: Atom.from_dict(a) for k, a in (d.get("atoms") or {}).items()},
                       list(d.get("read_fields") or []), valid.get("from", ""), valid.get("until", ""),
                       d.get("resource_type", ""), list(d.get("write_fields") or []), d.get("binding", ""),
                       d.get("source", ""), d.get("definition"))

    def to_dict(self) -> dict:
        valid = {"from": self.valid_from, **({"until": self.valid_until} if self.valid_until else {})}
        atoms = {}
        for k, a in self.atoms.items():
            m: dict = {"kind": a.kind}
            if a.values:
                m["values"] = a.values
            if a.items:
                m["items"] = [{"path": i.path, "mode": i.mode} for i in a.items]
            for name in ("currency", "lo", "lo_closed", "hi", "hi_closed"):
                if (v := getattr(a, name)) not in (None, ""):
                    m[name] = v
            atoms[k] = m
        out = {"operation": self.operation, "atoms": atoms, "read_fields": self.read_fields, "valid": valid}
        for name in ("resource_type", "write_fields", "binding", "source", "definition"):
            if v := getattr(self, name):
                out[name] = v
        return out

    def active_at(self, t: datetime) -> bool:
        try:
            if t < _instant(self.valid_from):
                return False
            return not self.valid_until or t < _instant(self.valid_until)
        except ValueError:
            return False

    def check(self, r: Resource) -> None:
        """Exactly one atom per declared dimension, atoms well formed, written fields readable (09a, EN08-EN10)."""
        dims = r.dimensions or {}
        if len(self.atoms) != len(dims):
            raise ValueError(f"segment for {self.operation}: {len(self.atoms)} atoms for {len(dims)} dimensions")
        for dim, a in self.atoms.items():
            d = dims.get(dim)
            if d is None:
                raise ValueError(f"segment for {self.operation}: dimension {dim} is not declared")
            bad = {"all": False, "in": False, "tree": d.kind != ORG_TREE, "self": d.kind != PRINCIPAL,
                   "interval": d.kind != AMOUNT or (a.lo is None and a.hi is None)
                   or (a.lo is None) != (a.lo_closed is None) or (a.hi is None) != (a.hi_closed is None)}
            if a.kind not in bad:
                raise ValueError(f"segment for {self.operation}: atom kind {a.kind!r} is not understood")
            if bad[a.kind]:
                raise ValueError(f"segment for {self.operation}: {a.kind} on {dim}")
        if unreadable := set(self.write_fields) - set(self.read_fields):
            raise ValueError(f"segment for {self.operation}: field {sorted(unreadable)[0]} writable but not readable")

    def canonical(self, resource_type: str) -> dict:
        """The normalized segment as the encoder's types (security 07's normalization, 09a's encoding)."""
        atoms = {}
        for dim, a in self.atoms.items():
            m: dict = {"kind": a.kind}
            if a.kind == "in":
                m["values"] = canon.sorted_set([canon.text(v) for v in a.values])
            elif a.kind == "tree":
                m["items"] = canon.sorted_set([{"path": i.path, "mode": i.mode} for i in a.items])
            elif a.kind == "interval":
                m["currency"] = a.currency
                for name, v, closed in (("lo", a.lo, a.lo_closed), ("hi", a.hi, a.hi_closed)):
                    if v is not None:
                        m[name], m[name + "_closed"] = canon.decimal(v), closed
            atoms[dim] = m
        valid = {"from": canon.instant(self.valid_from)}
        if self.valid_until:
            valid["until"] = canon.instant(self.valid_until)
        out = {"operation": self.operation, "atoms": atoms, "read_fields": canon.sorted_set(list(self.read_fields)),
               "write_fields": canon.sorted_set(list(self.write_fields)), "valid": valid}
        if resource_type:
            out["resource_type"] = resource_type
        if self.binding:
            out["binding"] = self.binding
        if self.source:
            out["source"] = self.source
        if self.definition:
            out["definition"] = json.loads(json.dumps(self.definition))
        return out

    def digest(self) -> str:
        """The segment's identity: the digest of its normalized form (09a)."""
        return canon.digest("be.security.segment", 1, self.canonical(self.resource_type))


def _atom(a: Atom, f: Fact | None, principal: str) -> Tri:
    if a.kind == "all":
        return Tri.TRUE
    if f is None or f.state not in ("value", "null"):
        return Tri.UNKNOWN
    if f.state == "null":
        return Tri.FALSE
    if a.kind == "in":
        return Tri.TRUE if f.value in a.values else Tri.FALSE
    if a.kind == "self":
        return Tri.TRUE if f.value == principal else Tri.FALSE
    if a.kind == "tree":
        if not f.value:
            return Tri.FALSE  # an empty path is not the root
        hit = any(f.value == i.path or (i.mode == "subtree" and f.value.startswith(i.path + "/")) for i in a.items)
        return Tri.TRUE if hit else Tri.FALSE
    if a.kind == "interval":
        return _interval(a, f)
    return Tri.UNKNOWN  # an atom this code does not understand never allows


def _interval(a: Atom, f: Fact) -> Tri:
    if f.currency != a.currency:
        return Tri.FALSE
    if any(v is not None and not _DECIMAL.fullmatch(v) for v in (f.value, a.lo, a.hi)):
        return Tri.UNKNOWN  # not a decimal string as the protocol writes them (no spaces, underscores, NaN)
    amount = Decimal(f.value)
    lo = Decimal(a.lo) if a.lo is not None else None
    hi = Decimal(a.hi) if a.hi is not None else None
    if lo is not None and (amount < lo or (amount == lo and not a.lo_closed)):
        return Tri.FALSE
    if hi is not None and (amount > hi or (amount == hi and not a.hi_closed)):
        return Tri.FALSE
    return Tri.TRUE


def _segment(s: Segment, facts: dict[str, Fact], principal: str, at: datetime) -> Tri:
    if not s.active_at(at):
        return Tri.FALSE
    out = Tri.TRUE
    for dim, a in s.atoms.items():
        r = _atom(a, facts.get(dim), principal)
        if r is Tri.FALSE:
            return Tri.FALSE
        if r is Tri.UNKNOWN:
            out = Tri.UNKNOWN
    return out


def _entitled(segs: list[Segment], op: str, facts: dict[str, Fact], principal: str,
              at: datetime) -> tuple[Tri, set[str]]:
    """OR over the operation's segments; fields are the union of the true segments' read fields."""
    res, fields = Tri.FALSE, set()
    for s in segs:
        if s.operation != op:
            continue
        r = _segment(s, facts, principal, at)
        if r is Tri.TRUE:
            res = Tri.TRUE
            fields.update(s.read_fields)
        elif r is Tri.UNKNOWN and res is Tri.FALSE:
            res = Tri.UNKNOWN
    return res, fields


def _and(a: Tri, b: Tri) -> Tri:
    if Tri.FALSE in (a, b):
        return Tri.FALSE
    return Tri.UNKNOWN if Tri.UNKNOWN in (a, b) else Tri.TRUE


@dataclass
class Decision:
    """The result for one record (security 01's formulas)."""

    visible: Tri
    action: Tri  # the asked operation's permission; equals visible for the view operation
    fields: list[str]  # readable fields (never a secret one), sorted; empty unless visible

    def error(self) -> errs.Error | None:
        """The answer (P8.15, P8.16): not visible NOT_FOUND; the operation definitely not allowed PERMISSION_DENIED,
        even when visibility is unknown (a known mismatch is denied by the operation's rule, security 01); otherwise
        unknown AUTHORIZATION_UNPROVABLE; None when allowed."""
        if self.visible is Tri.FALSE:
            return errs.new("NOT_FOUND", "not found")
        if self.action is Tri.FALSE:
            return errs.new("PERMISSION_DENIED", "not allowed")
        if Tri.UNKNOWN in (self.visible, self.action):
            return errs.new("AUTHORIZATION_UNPROVABLE", "the authorization cannot be proven right now")
        return None


def evaluate(r: Resource, segs: list[Segment], op: str, facts: dict[str, Fact], principal: str,
             at: datetime) -> Decision:
    """Decides one record: visibility by the resource's view operation, the operation's permission, readable fields."""
    visible, fields = _entitled(segs, r.view_operation, facts, principal, at)
    action = visible
    if op != r.view_operation:
        action = _and(visible, _entitled(segs, op, facts, principal, at)[0])
    readable = []
    if visible is Tri.TRUE:
        readable = sorted(f for f in fields if (fd := r.fields.get(f)) is not None and fd.cls != SECRET)
    return Decision(visible, action, readable)
