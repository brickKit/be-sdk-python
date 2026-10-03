"""Guards, the route decision chain (be-protocol P6.2) and the per-request ``Access``.

Every business route declares exactly one guard: a permission key, ``PUBLIC`` or ``AUTHENTICATED``.
This wave decides keys (E1–E6); data scopes, single-record decisions and field masks join ``Access``
in the Access task.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from besdk import context, errors
from besdk.auth import evaluate as E
from besdk.auth.bundle import Bundle, Principal
from besdk.auth.scope import Scope
from besdk.auth.jwt import Verifier
from besdk.errors import Code, Error, be_error

_DEPT_RE = re.compile(r"/([^/]+/)*")


class PermKey(str):
    """A permission key guard, e.g. ``PermKey("erp.sales.view")``; authzgen generates these constants."""


PUBLIC = PermKey("")
AUTHENTICATED = PermKey("__authenticated__")


@dataclass(frozen=True)
class User:
    sub: str
    tenant_id: str
    roles: tuple[str, ...]
    dept_path: str
    has_dept: bool
    act: Any
    locale: str
    claims: Mapping[str, Any]

    @classmethod
    def of(cls, claims: Mapping[str, Any]) -> "User":
        dept = claims.get("dept_path") or ""
        valid = bool(_DEPT_RE.fullmatch(dept))
        return cls(sub=claims["sub"], tenant_id=claims.get("tenant_id", ""), roles=tuple(claims.get("roles") or ()),
                   dept_path=dept if valid else "", has_dept=valid, act=claims.get("act"),
                   locale=claims.get("locale", ""), claims=claims)


_ACL = ("SELECT rtype, rid, relation, subject, expires_at FROM besdk_authz_acl "
        "WHERE rtype = $1 AND rid = $2 AND (expires_at IS NULL OR expires_at > now())")


class Access:
    """What the caller may do, evaluated once per request against the bundle in use. ``key`` is the route's
    permission key: scopes and decisions default to it (P6.3, P6.6)."""

    def __init__(self, user: User, principal: Principal, key: str, rt: Any = None):
        self.user, self.principal, self.key, self.rt = user, principal, key, rt

    def has(self, key: str) -> bool:
        """A feature key, ceilings already applied (E4, E5)."""
        return self.principal.has(key)

    def level(self, key: str) -> str:
        return self.principal.level(key)

    # --- data scopes and decisions over declared resource types (P6.3–P6.8) --------------------------

    def _declared(self, rtype: str) -> Any:
        d = (getattr(self.rt, "resources", None) or {}).get(rtype)
        if d is None:
            raise errors.internal(f"resource type {rtype} is not declared in assembly.yaml resources")
        return d

    def scope(self, rtype: str, key: str | None = None) -> Scope:
        """The canonical predicate's parameters for ``key`` (default: the route's key) on ``rtype``."""
        d = self._declared(rtype)
        sc = Scope.of(self.principal, key or self.key, d.rtype, d.columns)
        req = context.current().req
        if sc.degraded and req is not None:
            req.setdefault("headers", {})["X-Authz-Degraded"] = ",".join(sc.degraded)
        return sc

    def check_dimension(self, rtype: str, dimension: str, value: Any, key: str | None = None) -> None:
        """A request parameter that is a dimension value outside the caller's scope answers 403 (P6.6)."""
        v = self.scope(rtype, key).params.s_dims.get(dimension)
        if v is None or not (v.all or str(value) in v.ids):
            raise be_error("OUT_OF_SCOPE")

    async def acl(self, rtype: str, rid: str, tx: Any = None) -> list[E.AclRow]:
        """The projection's direct tuples of one record (P6.12); none without a database."""
        if self.rt is None or "PG_SCHEMA" not in self.rt.config.declared():
            return []
        q = (lambda t: t.fetch(_ACL, rtype, rid))
        rows = await (q(tx) if tx is not None else self.rt.store().tx(q))
        return [E.AclRow(r["rtype"], r["rid"], r["relation"], r["subject"], r["expires_at"]) for r in rows]

    async def can(self, key: str, rtype: str, row: E.Row, *, tx: Any = None) -> E.Decision:
        """``{visible, allowed, reason}`` for one record (E10); ``d.err()`` is 404 when not visible."""
        d = self._declared(rtype)
        return E.decide(self.principal, key, d.rtype, row, acl=await self.acl(rtype, row.id, tx))

    async def row_access(self, rtype: str, row: E.Row, actions: Mapping[str, str], *, tx: Any = None) -> dict:
        """``_access: {<action>: bool}`` for one row of a list (P6.9)."""
        d, acl = self._declared(rtype), await self.acl(rtype, row.id, tx)
        return {a: E.decide(self.principal, k, d.rtype, row, acl=acl).allowed for a, k in actions.items()}

    def fields(self, rtype: str) -> dict[str, list[str]]:
        return E.fields(self.principal, self._declared(rtype).rtype)

    def mask(self, rtype: str, obj: dict) -> dict:
        """Masked columns set to null at the source and listed in ``_masked`` (P6.8)."""
        masked = [c for c in self.fields(rtype)["masked"] if c in obj]
        for c in masked:
            obj[c] = None
        obj["_masked"] = masked
        return obj

    def check_sortable(self, rtype: str, field: str) -> None:
        """Sorting, filtering or aggregating by a masked field answers 400 SORT_FORBIDDEN."""
        if field in self.fields(rtype)["masked"]:
            raise be_error("SORT_FORBIDDEN", {"field": field})

    def check_writable(self, rtype: str, changed: Any) -> None:
        """Writing a masked or read-only field answers 403 FIELD_FORBIDDEN."""
        f = self.fields(rtype)
        bad = sorted(set(changed) & (set(f["masked"]) | set(f["read_only"])))
        if bad:
            raise be_error("FIELD_FORBIDDEN", {"field": bad[0]})


class _Source(Protocol):
    bundle: Bundle | None


class Authorizer:
    """Process-wide in a shell (P19.3): the token verifier and the bundle source."""

    def __init__(self, verifier: Verifier, source: _Source, rt: Any = None):
        self.verifier, self.source, self.rt = verifier, source, rt

    async def decide(self, guard: PermKey, authorization: str | None) -> Access | None:
        """Allow (returning the evaluated access, None for Public) or raise the refusal (P6.2)."""
        if guard == PUBLIC:
            return None
        token = _bearer(authorization)
        claims = await self.verifier.verify(token)
        bundle = self.source.bundle
        if bundle is None:
            raise be_error("AUTHZ_NOT_READY")
        refused = bundle.check_token(claims)
        if refused == "TOKEN_STALE":
            raise be_error("TOKEN_STALE")
        if refused:
            raise be_error(refused)
        principal = bundle.principal(claims, now=time.time())
        access = Access(User.of(claims), principal, "" if guard == AUTHENTICATED else str(guard), self.rt)
        if guard != AUTHENTICATED and not principal.has(guard):
            raise be_error("MISSING_PERMISSION", {"permission": str(guard)})
        return access


def _bearer(header: str | None) -> str:
    if not header or not header.startswith("Bearer ") or not header[7:].strip():
        raise be_error("TOKEN_INVALID")
    return header[7:].strip()


def access() -> Access:
    """The current request's access; no user (a job, an event handler, a system call) → 401."""
    a = context.current().access
    if a is None:
        raise Error(Code.UNAUTHENTICATED, "TOKEN_INVALID", domain="be", message="no user in this unit of work")
    return a
