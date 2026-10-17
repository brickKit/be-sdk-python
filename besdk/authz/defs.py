"""Resource, operation and field declarations (protocol P8.7, P8.8; security 06 and 09a)."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from besdk import canon

# Dimension kinds (security 07).
VALUE_SET, ORG_TREE, PRINCIPAL, AMOUNT = "value_set", "org_tree", "principal", "amount"
# Field classes.
ORDINARY, RESTRICTED, SECRET = "ordinary", "restricted", "secret"

_KEY = re.compile(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*")


@dataclass
class Dimension:
    """One authorization dimension of a resource and where its fact lives."""

    kind: str
    column: str  # the column on the owner's table
    principal_role: str = ""  # principal dimensions: which party (owner, creator)
    currency_column: str = ""  # amount dimensions


@dataclass
class Field:
    """One output field."""

    cls: str  # ordinary, restricted, secret
    read_key: str = ""  # restricted fields
    write_key: str = ""


@dataclass
class Resource:
    """A resource type the component owns."""

    type: str
    view_operation: str
    dimensions: dict[str, Dimension] | None  # all of them; none is an explicit empty dict
    fields: dict[str, Field] = field(default_factory=dict)  # every field that may be output

    def record(self) -> dict:
        """The resource definition as its security record body (security 09a), for the digest and submission."""
        dims = {}
        for name, d in (self.dimensions or {}).items():
            m = {"kind": d.kind, "column": d.column, "pushdown": "local_column"}
            if d.principal_role:
                m["principal_role"] = d.principal_role
            if d.currency_column:
                m["currency_column"] = d.currency_column
            dims[name] = m
        fields = {}
        for name, f in self.fields.items():
            m = {"class": f.cls}
            if f.read_key:
                m["read_key"] = f.read_key
            if f.write_key:
                m["write_key"] = f.write_key
            fields[name] = m
        return {"type": self.type, "view_operation": self.view_operation, "dimensions": dims, "fields": fields}

    def digest(self) -> str:
        """The resource definition's digest (P8.8)."""
        return canon.digest("be.security.resource_definition", 1, self.record())


@dataclass
class Operation:
    """One operation (security 09a's operation definition)."""

    key: str
    resource_type: str
    purpose: str  # read, special_read, export, action, create, manage
    risk_class: str  # low, high
    side_effects: list[str] = field(default_factory=list)
    audit: bool = False  # audit: required


@dataclass
class Definitions:
    """Everything a component declares."""

    component: str
    resources: list[Resource] = field(default_factory=list)
    operations: list[Operation] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._resources: dict[str, Resource] = {}
        self._operations: dict[str, Operation] = {}

    def validate(self) -> None:
        """Checks the declarations (P8.7) and indexes them; every problem is reported together."""
        problems: list[str] = []
        self._resources, self._operations = {}, {}
        for r in self.resources:
            if not _KEY.fullmatch(r.type):
                problems.append(f"resource {r.type!r}: not a registry key")
            if r.dimensions is None:
                problems.append(f"{r.type}: dimensions must be declared (an empty dict when there are none)")
            for name, d in (r.dimensions or {}).items():
                if d.kind not in (VALUE_SET, ORG_TREE, PRINCIPAL, AMOUNT):
                    problems.append(f"{r.type}.{name}: dimension kind {d.kind!r}")
                elif not d.column:
                    problems.append(f"{r.type}.{name}: no column")
                elif d.kind == AMOUNT and not d.currency_column:
                    problems.append(f"{r.type}.{name}: an amount needs its currency column")
            for name, f in r.fields.items():
                if f.cls not in (ORDINARY, RESTRICTED, SECRET):
                    problems.append(f"{r.type}.{name}: field class {f.cls!r} (every field is classified)")
                if f.cls == RESTRICTED and not f.read_key:
                    problems.append(f"{r.type}.{name}: a restricted field needs its read key")
            self._resources[r.type] = r
        for o in self.operations:
            if not _KEY.fullmatch(o.key):
                problems.append(f"operation {o.key!r}: not a registry key")
            if o.resource_type not in self._resources:
                problems.append(f"{o.key}: resource type {o.resource_type} is not declared")
            if o.risk_class not in ("low", "high"):
                problems.append(f"{o.key}: risk class must be low or high (never assumed low)")
            if o.key in self._operations:
                problems.append(f"{o.key}: declared twice")
            self._operations[o.key] = o
        for r in self.resources:
            v = self._operations.get(r.view_operation)
            if v is None or v.resource_type != r.type:
                problems.append(f"{r.type}: its view operation {r.view_operation} is not declared for it")
        if problems:
            raise ValueError("authorization declarations: " + "; ".join(sorted(problems)))

    def operation(self, key: str) -> Operation | None:
        return self._operations.get(key)

    def resource(self, typ: str) -> Resource | None:
        return self._resources.get(typ)

    def check_operations(self, ops: list[str]) -> None:
        """Refuses routes and methods whose operations are not declared (P8.7)."""
        missing = sorted({op for op in ops if op not in self._operations})
        if missing:
            raise ValueError("operations used by routes or methods but not declared: " + ", ".join(missing))
