"""The authorization of one verified request (P8.12, P8.15, P8.16): who asks, the member's declarations and view.
The guard binds it for the handler; handlers decide records with it."""
from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from besdk import errs, reqctx
from besdk.authz.defs import Definitions, Operation, Resource
from besdk.authz.plan import Plan, compile_plan
from besdk.authz.segment import Decision, Fact, evaluate
from besdk.authz.view import Grant, Holder


@dataclass
class Subject:
    principal: str
    defs: Definitions
    holder: Holder
    at: datetime

    def _grant(self, op: str) -> tuple[Operation, Resource, Grant]:
        o = self.defs.operation(op)
        if o is None:
            raise errs.from_exception(ValueError(f"operation {op} is not declared"))  # type: ignore[misc]
        r = self.defs.resource(o.resource_type)
        risk = o.risk_class
        if (v := self.defs.operation(r.view_operation)) is not None and v.risk_class == "high":  # type: ignore[union-attr]
            risk = "high"
        g = self.holder.lookup(r.type, self.principal, risk)  # type: ignore[union-attr]
        reqctx.tighten_cutoff(g.cutoff)
        return o, r, g  # type: ignore[return-value]

    def check(self, op: str, facts: dict[str, Fact]) -> Decision:
        """Decides one record for an operation; a refusal raises the decision's error. The request's cutoff moves to
        the evidence's, so a write transaction checks it before COMMIT."""
        o, r, g = self._grant(op)
        d = evaluate(r, g.segments, o.key, facts, self.principal, self.at)
        if (e := d.error()) is not None:
            raise e
        return d

    def list(self, op: str, alias: str = "", arg_base: int = 0) -> Plan:
        """The list plan of an operation for this subject (P8.17): its predicate goes into the list's one SQL
        statement. A subject without segments gets FALSE, an empty page, not a refusal (security 07)."""
        o, r, g = self._grant(op)
        return compile_plan(r, g.segments, o.key, self.principal, self.at, alias, arg_base)


def mask(out: dict, readable: list[str]) -> dict:
    """Keeps only the readable fields of an output object; fields that are not declared never pass."""
    keep = set(readable)
    return {k: v for k, v in out.items() if k in keep}


_subject: contextvars.ContextVar[Subject | None] = contextvars.ContextVar("be_subject", default=None)


def subject() -> Subject | None:
    """The request's subject (bound by the guard for the handler); None on public routes."""
    return _subject.get()


@contextlib.contextmanager
def subject_scope(s: Subject) -> Iterator[None]:
    """Binds the request's subject for a block (the guard's admission)."""
    token = _subject.set(s)
    try:
        yield
    finally:
        _subject.reset(token)
