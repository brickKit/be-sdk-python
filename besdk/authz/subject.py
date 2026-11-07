"""The authorization of one verified request (P8.12, P8.15, P8.16): who asks, the member's declarations and view.
The guard binds it for the handler; handlers decide records with it."""
from __future__ import annotations

import contextlib
import contextvars
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterator

from besdk import errs, reqctx
from besdk.authz.defs import SECRET, Definitions, Operation, Resource
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
        _stale.set(None)
        g = self.holder.lookup_for(r.type, self.principal, risk, o.tightening_only)  # type: ignore[union-attr]
        reqctx.tighten_cutoff(g.cutoff)
        _stale.set(g.since if g.stale else None)
        return o, r, g  # type: ignore[return-value]

    def check(self, op: str, facts: dict[str, Fact]) -> Decision:
        """Decides one record for an operation; a refusal raises the decision's error. The request's cutoff moves to
        the evidence's, so a write transaction checks it before COMMIT."""
        o, r, g = self._grant(op)
        d = evaluate(r, g.segments, o.key, facts, self.principal, self.at)
        if (e := d.error()) is not None:
            raise e
        return d

    def decider(self, op: str) -> Callable[[dict[str, Fact]], Decision]:
        """Takes the subject's grant for an operation once and answers a function deciding each record of many by its
        facts (P8.15): for records read outside SQL, as the cold rows of an archive (be-protocol P15.26)."""
        o, r, g = self._grant(op)
        return lambda facts: evaluate(r, g.segments, o.key, facts, self.principal, self.at)

    def list(self, op: str, alias: str = "", arg_base: int = 0) -> Plan:
        """The list plan of an operation for this subject (P8.17): its predicate goes into the list's one SQL
        statement. A subject without segments gets FALSE, an empty page, not a refusal (security 07)."""
        o, r, g = self._grant(op)
        return compile_plan(r, g.segments, o.key, self.principal, self.at, alias, arg_base)


MASKED = "_masked"


def mask(r: Resource, out: dict, readable: list[str]) -> dict:
    """Shapes an output object for the caller (P8.31): readable fields as they are; the resource's other ordinary and
    restricted fields None and named, sorted, under _masked (only when there is one), so a caller tells "no value"
    from "not yours to see"; secret and undeclared keys never pass."""
    keep = set(readable)
    shaped: dict = {}
    masked = []
    for k, v in out.items():
        fd = r.fields.get(k)
        if fd is None or fd.cls == SECRET:
            continue
        if k in keep:
            shaped[k] = v
        else:
            shaped[k] = None
            masked.append(k)
    if masked:
        shaped[MASKED] = sorted(masked)
    return shaped


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


# the basis of the request's last decision when it was a stale source's last view (P8.29): that shard's last full
# renewal, wall seconds; None otherwise
_stale: contextvars.ContextVar[float | None] = contextvars.ContextVar("be_stale_basis", default=None)


def stale_basis() -> float | None:
    """The last full renewal of the view the request's last decision used, when that was a tightening_only operation
    admitted on the last view of a shard whose source the authorization member said is stale (P8.29); None when the
    decision used a valid view. The doer marks its audit "basis not proven latest" and applies security 03's table
    (one person within T_tight, two beyond)."""
    return _stale.get()
