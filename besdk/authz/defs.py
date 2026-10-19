"""Resource, operation and field declarations (protocol P8.7, P8.8; security 06 and 09a)."""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

from besdk import canon, records

# Dimension kinds (security 07).
VALUE_SET, ORG_TREE, PRINCIPAL, AMOUNT = "value_set", "org_tree", "principal", "amount"
# Field classes.
ORDINARY, RESTRICTED, SECRET = "ordinary", "restricted", "secret"

# Pushdown modes of a dimension (security 09a).
LOCAL_COLUMN, LOCAL_PROJECTION, NOT_PUSHDOWNABLE = "local_column", "local_projection", "not_pushdownable"

_KEY = re.compile(r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*")
# The closed value lists of security 09a's definitions.
_PURPOSES = ("read", "special_read", "export", "action", "create", "manage")
_PATHS = ("default", "strict_acceptance", "strict_barrier")
_SIDE_EFFECTS = ("business_write", "external_command", "job_or_subscription", "file_capability", "security_change",
                 "notification")
_QUERY_USES = ("filter", "sort", "group", "aggregate")


@dataclass
class Dimension:
    """One authorization dimension of a resource and where its fact lives."""

    kind: str
    column: str  # the column on the owner's table
    principal_role: str = ""  # principal dimensions: which party (owner, creator)
    currency_column: str = ""  # amount dimensions
    pushdown: str = ""  # where the query compiler finds the fact; empty is LOCAL_COLUMN
    projection_watermark: str = ""  # the revision domain of the projection's completeness (LOCAL_PROJECTION)


@dataclass
class Field:
    """One output field."""

    cls: str  # ordinary, restricted, secret
    read_key: str = ""  # restricted fields
    write_key: str = ""
    derived_from: list[str] = field(default_factory=list)  # reading it needs reading all of these
    queryable: list[str] = field(default_factory=list)  # filter, sort, group, aggregate; none by default


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
            m = {"kind": d.kind, "column": d.column, "pushdown": d.pushdown or LOCAL_COLUMN}
            if d.principal_role:
                m["principal_role"] = d.principal_role
            if d.currency_column:
                m["currency_column"] = d.currency_column
            if d.projection_watermark:
                m["projection_watermark"] = d.projection_watermark
            dims[name] = m
        fields = {}
        for name, f in self.fields.items():
            m = {"class": f.cls}
            if f.read_key:
                m["read_key"] = f.read_key
            if f.write_key:
                m["write_key"] = f.write_key
            if f.derived_from:
                m["derived_from"] = sorted(set(f.derived_from))
            if f.queryable:
                m["queryable"] = sorted(set(f.queryable))
            fields[name] = m
        return {"type": self.type, "view_operation": self.view_operation, "dimensions": dims, "fields": fields}

    def digest(self) -> str:
        """The resource definition's digest (P8.8)."""
        return canon.digest("be.security.resource_definition", 1, self.record())


@dataclass
class Operation:
    """One operation (security 09a's operation definition). The defaults of the flags are the safe ones: not
    delegable, never under impersonation, no appointment needed, the default path."""

    key: str
    resource_type: str
    purpose: str  # read, special_read, export, action, create, manage
    risk_class: str  # low, high
    side_effects: list[str] = field(default_factory=list)
    audit: bool = False  # audit: required
    minimum_path: str = ""  # default, strict_acceptance or strict_barrier; empty is default
    requires_appointment: bool = False
    delegable: bool = False
    impersonation_allowed: bool = False  # only operations without side effects
    tightening_only: bool = False  # only manage operations whose only side effect is security_change
    output_read_operation: str = ""
    self_review_rule: str = ""

    def record(self) -> dict:
        """The operation definition as its security record body (security 09a)."""
        m: dict[str, Any] = {"key": self.key, "resource_type": self.resource_type, "purpose": self.purpose,
                             "risk_class": self.risk_class, "minimum_path": self.minimum_path or "default",
                             "side_effects": sorted(set(self.side_effects)),
                             "requires_appointment": self.requires_appointment, "delegable": self.delegable,
                             "impersonation_allowed": self.impersonation_allowed,
                             "audit": "required" if self.audit else "none"}
        if self.tightening_only:
            m["tightening_only"] = True
        if self.output_read_operation:
            m["output_read_operation"] = self.output_read_operation
        if self.self_review_rule:
            m["self_review_rule"] = self.self_review_rule
        return m


@dataclass
class Sealed:
    """One definition record as it is submitted (D76): canonical bytes without a digest, and the digest."""

    record_type: str
    key: str
    raw: bytes
    digest: str


class Rejected(Exception):
    """A submission the authorization member refused: every reason it gave. Retrying the same definitions does not
    help until the registry or the definitions change."""

    def __init__(self, reasons: list[str]):
        super().__init__("the authorization member refused the definitions: " + "; ".join(reasons))
        self.reasons = reasons


def stable_id(*parts: str) -> str:
    """A definition record's identity: the same for the same component, kind and key in every version and
    deployment. A UUID with the version 7 and variant bits set over SHA-256 bits; its time field carries no time."""
    b = bytearray(hashlib.sha256(("be.security.definition\n" + "\n".join(parts)).encode()).digest()[:16])
    b[6] = b[6] & 0x0F | 0x70
    b[8] = b[8] & 0x3F | 0x80
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def set_digest(component: str, version: str, sealed: list[Sealed]) -> str:
    """The digest of a submission (be.security.definition_set)."""
    return records.seal("be.security.definition_set", {"component": component, "version": version,
                                                        "definitions": sorted({s.digest for s in sealed})})[1]


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
                elif d.pushdown and d.pushdown not in (LOCAL_COLUMN, LOCAL_PROJECTION, NOT_PUSHDOWNABLE):
                    problems.append(f"{r.type}.{name}: pushdown {d.pushdown!r}")
                elif d.pushdown == LOCAL_PROJECTION and not d.projection_watermark:
                    problems.append(f"{r.type}.{name}: a local projection needs its watermark domain")
            for name, f in r.fields.items():
                if f.cls not in (ORDINARY, RESTRICTED, SECRET):
                    problems.append(f"{r.type}.{name}: field class {f.cls!r} (every field is classified)")
                if f.cls == RESTRICTED and not f.read_key:
                    problems.append(f"{r.type}.{name}: a restricted field needs its read key")
                problems += [f"{r.type}.{name}: query use {q!r}" for q in f.queryable if q not in _QUERY_USES]
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
            if o.purpose not in _PURPOSES:
                problems.append(f"{o.key}: purpose {o.purpose!r}")
            if o.minimum_path and o.minimum_path not in _PATHS:
                problems.append(f"{o.key}: minimum path {o.minimum_path!r}")
            problems += [f"{o.key}: side effect {e!r}" for e in o.side_effects if e not in _SIDE_EFFECTS]
            if o.impersonation_allowed and o.side_effects:
                problems.append(f"{o.key}: an operation with side effects is never allowed under impersonation")
            if o.tightening_only and (o.purpose != "manage" or o.side_effects != ["security_change"]):
                problems.append(f"{o.key}: only a manage operation whose only side effect is security_change can be "
                                "tightening only")
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

    def records(self, deployment: str, version: str) -> list[Sealed]:
        """Every declared definition sealed with the record envelope (security 09a): the component is the authority,
        the identity is stable for the component, kind and key, and the revision is the component's version."""
        # the revision domain is a Key: a component ID's "/" and "-" are not allowed in one (test/probe-go2 -> test.probe_go2)
        domain = self.component.replace("/", ".").replace("-", "_") + ".definition"
        out = []
        for record_type, kind, key, body in (
                [("be.security.resource_definition", "resource", r.type, r.record()) for r in self.resources]
                + [("be.security.operation_definition", "operation", o.key, o.record()) for o in self.operations]):
            rec = {"record_type": record_type, "schema": "1.0", "deployment": deployment, "authority": self.component,
                   "id": stable_id(self.component, kind, key), "revision": {"domain": domain, "value": version}, **body}
            raw, digest = records.seal(record_type, rec)
            out.append(Sealed(record_type, key, raw, digest))
        return out

    async def submit(self, stub: Any, deployment: str, version: str) -> None:
        """Sends the definitions to the authorization member (DefinitionService.Submit) and checks that the accepted
        set is the one sent; raises Rejected on a refusal."""
        from be.authz.v3 import authz_pb2

        from besdk.calls.client import options

        sealed = self.records(deployment, version)
        want = set_digest(self.component, version, sealed)
        ans = await stub.Submit(authz_pb2.SubmitRequest(
            component=self.component, version=version,
            definitions=[authz_pb2.Definition(record=s.raw, digest=s.digest) for s in sealed]),
            metadata=options(idempotent=True))
        if ans.status == authz_pb2.SUBMIT_STATUS_ACCEPTED:
            if ans.set_digest != want:
                raise ValueError(f"the authorization member accepted another set of definitions ({ans.set_digest}, "
                                 f"sent {want})")
            return
        if ans.status == authz_pb2.SUBMIT_STATUS_REJECTED:
            raise Rejected([" ".join(x for x in (r.reason, r.digest, r.detail) if x) for r in ans.rejections])
        raise ValueError("the authorization member answered the submission without a status")

    def check_operations(self, ops: list[str]) -> None:
        """Refuses routes and methods whose operations are not declared (P8.7)."""
        missing = sorted({op for op in ops if op not in self._operations})
        if missing:
            raise ValueError("operations used by routes or methods but not declared: " + ", ".join(missing))
