"""The authorization bundle, authz/2 (contract-infra-authz EVALUATION.md E1–E6), as far as the route
decision needs it (P6.2): acceptance, token checks, ``has(K)`` and ``level(K)``.

The data-scope parameters, single-record decisions and field masks (E6–E12) are added by the Access task.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

LEVELS = ("own", "dept", "subtree", "all")
_RANK = {n: i for i, n in enumerate(LEVELS)}
_CONTRACT_RE = re.compile(r"authz/2\.(0|[1-9][0-9]*)")
STALE_SKEW = 5


class BundleRefused(ValueError):
    """E1: a bundle whose ``contract`` is not ``authz/2.*`` is never used."""


def _in_window(w: Mapping[str, Any] | None, now: float) -> bool:
    if not w:
        return True
    start, end = w.get("from_ts"), w.get("until")
    return (start is None or now >= start) and (end is None or now < end)


def _delegated(claims: Mapping[str, Any]) -> bool:
    return bool(claims.get("act") or claims.get("ceil") or claims.get("dg"))


@dataclass(frozen=True)
class Bundle:
    raw: Mapping[str, Any]

    @classmethod
    def accept(cls, doc: Mapping[str, Any]) -> "Bundle":
        c = doc.get("contract")
        if not isinstance(c, str) or not _CONTRACT_RE.fullmatch(c):
            raise BundleRefused(f"bundle contract {c!r} is not authz/2.*")
        return cls(doc)

    @property
    def revision(self) -> str:
        return str(self.raw.get("revision", ""))

    def capability(self, name: str) -> bool:
        return self.raw.get("capabilities", {}).get(name) is True

    def check_token(self, claims: Mapping[str, Any]) -> str | None:
        """E2, in order; returns the refusing reason or None."""
        stale = self.raw.get("stale_since", {}).get(claims.get("sub", ""))
        if stale is not None and claims.get("iat", 0) < stale - STALE_SKEW:
            return "TOKEN_STALE"
        dg = claims.get("dg")
        if dg and dg in self.raw.get("revoked_grants", {}):
            return "TOKEN_STALE"
        if _delegated(claims) and not self.capability("delegation"):
            return "UNSUPPORTED_DELEGATION"
        act = claims.get("act")
        while act:
            need = {"agent": "agents", "user": "impersonation", "svc": None}.get(act.get("kind"), "?")
            if need == "?" or (need and not self.capability(need)):
                return "UNSUPPORTED_DELEGATION"
            act = act.get("act")
        return None

    def principal(self, claims: Mapping[str, Any], *, now: float) -> "Principal":
        return Principal(self, claims, now)


class Principal:
    """One verified caller evaluated against one bundle at one instant."""

    def __init__(self, bundle: Bundle, claims: Mapping[str, Any], now: float):
        self.bundle = bundle
        self.claims = claims
        self.now = now
        grants = bundle.raw.get("grants", {})
        self.roles = [r for r in claims.get("roles") or () if _in_window(grants.get(r), now)]
        profiles = bundle.raw.get("profiles", {})
        empty = {"keys": [], "max_level": "own", "relations": []}
        self.ceilings = [profiles.get(code, empty) for code in claims.get("ceil") or ()]

    def ceilings_allow(self, key: str) -> bool:
        """E4: every ceiling lists the key in ``keys`` or ``fields``."""
        return all(key in c.get("keys", ()) or key in c.get("fields", ()) for c in self.ceilings)

    def holders(self, key: str) -> list[str]:
        roles = self.bundle.raw.get("roles", {})
        return [r for r in self.roles if key in roles.get(r, ())]

    def delegations(self, key: str) -> list[Mapping[str, Any]]:
        """E5: on-behalf delegations to this caller covering the key, inside their window."""
        if not self.bundle.capability("delegation"):
            return []
        sub = self.claims.get("sub")
        return [d for d in self.bundle.raw.get("delegations", ())
                if d.get("mode") == "on_behalf" and d.get("to") == sub and key in d.get("keys", ())
                and _in_window(d, self.now)]

    def has(self, key: str) -> bool:
        return bool(self.holders(key) or self.delegations(key)) and self.ceilings_allow(key)

    def level(self, key: str) -> str:
        """E6: the highest level among the holders, capped by the ceilings; ``none`` without one."""
        holders = self.holders(key)
        if not holders or not self.ceilings_allow(key):
            return "none"
        grants = self.bundle.raw.get("grants", {})
        best = max((_level_of(grants.get(r, {}), key) for r in holders), key=_RANK.__getitem__)
        for c in self.ceilings:
            cap = c.get("max_level", "own")
            if _RANK[cap] < _RANK[best]:
                best = cap
        return best


def _level_of(grant: Mapping[str, Any], key: str) -> str:
    return grant.get("levels", {}).get(key) or grant.get("default_level") or "own"
