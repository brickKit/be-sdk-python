"""Holds and destruction approvals (be-protocol P15.28, P15.29): records of their own, orthogonal to a unit's state; a
hold stops destruction and an erasure that loses content, an approval lets a due destruction go. Setting either needs
the class's declared operation, checked here (the irreversible step has one entrance). The operations are on a
resource the subject holds without record facts (no dimension, or unrestricted segments)."""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from besdk import errs
from besdk.db.pool import Tx
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import Class

UNIT, CLASS, RECORD = "unit", "class", "record"  # scope kinds; record (holds only): "<class>/<root id>"
SOURCES = ("legal", "audit", "dispute")  # P15.28


@dataclass(frozen=True)
class Scope:
    """What a hold or an approval covers; value: a unit id, a class, or "<class>/<root id>"."""

    kind: str
    value: str


def record_scope(cls: str, root: str) -> Scope:
    return Scope(RECORD, f"{cls}/{root}")


def _allowed(sub, op: str) -> None:
    """The subject's operation for a class (a decision without record facts)."""
    if sub is None:
        raise errs.new("PERMISSION_DENIED", "no authorized subject")
    sub.check(op, {})


async def _scope_of(tx: Tx, cls: str, s: Scope, record: bool) -> None:
    if s.kind == CLASS:
        if s.value != cls:
            raise errs.new("REQUEST_INVALID", "the class scope names another class")
    elif s.kind == UNIT:
        try:
            of = await tx.fetchval("SELECT class FROM be_tier_unit WHERE id::text = $1", s.value)
        except Exception:  # noqa: BLE001 - not a unit id
            of = None
        if of != cls:
            raise errs.new("NOT_FOUND", "no such unit of " + cls)
    elif s.kind == RECORD:
        if not record or not s.value.startswith(cls + "/") or len(s.value) == len(cls) + 1:
            raise errs.new("REQUEST_INVALID", "a record scope is <class>/<root id>, for holds only")
    else:
        raise errs.new("REQUEST_INVALID", "unknown scope kind " + s.kind)


def _declared(cat: Catalog, cls: str) -> Class:
    cl = cat.classes.get(cls)
    if cl is None:
        raise errs.new("NOT_FOUND", "no class " + cls)
    return cl


async def hold(cat: Catalog, sub, cls: str, s: Scope, source: str, reason: str) -> str:
    """Sets a hold on a scope of a class (P15.28); answers the hold's id."""
    cl = _declared(cat, cls)
    _allowed(sub, cl.hold_operation)
    if source not in SOURCES or not reason:
        raise errs.new("REQUEST_INVALID", "a hold needs its source (legal, audit, dispute) and its reason")
    hid = uuid.uuid7()

    async def body(tx: Tx) -> None:
        await _scope_of(tx, cls, s, True)
        await tx.execute("INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) "
                         "VALUES ($1, $2, $3, $4, $5, $6)", hid, s.kind, s.value, source, sub.principal, reason)

    await cat.db.tx(body)
    return str(hid)


async def lift_hold(cat: Catalog, sub, cls: str, hid: str) -> None:
    """Lifts a live hold (P15.28)."""
    cl = _declared(cat, cls)
    _allowed(sub, cl.hold_operation)
    status = await cat.db.tx(lambda tx: tx.execute(
        """UPDATE be_tier_hold SET released_at = now(), released_by = $3 WHERE id::text = $1 AND released_at IS NULL
        AND (scope = $2 OR (scope_kind = 'unit' AND scope IN (SELECT id::text FROM be_tier_unit WHERE class = $2))
        OR (scope_kind = 'record' AND scope LIKE $2 || '/%'))""", hid, cls, sub.principal))
    if status.endswith(" 0"):
        raise errs.new("NOT_FOUND", f"no live hold {hid} of {cls}")


async def approve(cat: Catalog, sub, cls: str, s: Scope, reason: str) -> str:
    """Gives a destruction approval of a unit, or a standing one of the class (P15.29); answers its id."""
    cl = _declared(cat, cls)
    _allowed(sub, cl.approve_operation)
    if not reason:
        raise errs.new("REQUEST_INVALID", "an approval needs its reason")
    aid = uuid.uuid7()

    async def body(tx: Tx) -> None:
        await _scope_of(tx, cls, s, False)
        await tx.execute("INSERT INTO be_tier_approval (id, scope_kind, scope, approved_by, reason) "
                         "VALUES ($1, $2, $3, $4, $5)", aid, s.kind, s.value, sub.principal, reason)

    await cat.db.tx(body)
    return str(aid)


async def withdraw(cat: Catalog, sub, cls: str, aid: str) -> None:
    """Withdraws a live approval (P15.29)."""
    cl = _declared(cat, cls)
    _allowed(sub, cl.approve_operation)
    status = await cat.db.tx(lambda tx: tx.execute(
        """UPDATE be_tier_approval SET withdrawn_at = now(), withdrawn_by = $3 WHERE id::text = $1
        AND withdrawn_at IS NULL AND (scope = $2 OR (scope_kind = 'unit' AND scope IN
        (SELECT id::text FROM be_tier_unit WHERE class = $2)))""", aid, cls, sub.principal))
    if status.endswith(" 0"):
        raise errs.new("NOT_FOUND", f"no live approval {aid} of {cls}")


async def holds_on(tx: Tx, unit: str, cls: str) -> int:
    """The live holds covering a unit: on it, on its class, or on any of its records."""
    return await tx.fetchval("""SELECT count(*) FROM be_tier_hold h WHERE h.released_at IS NULL AND (
        (h.scope_kind = 'unit' AND h.scope = $1) OR (h.scope_kind = 'class' AND h.scope = $2) OR
        (h.scope_kind = 'record' AND h.scope IN (SELECT $2 || '/' || m.root_id FROM be_tier_member m
        WHERE m.unit::text = $1)))""", unit, cls)


async def approved_for(tx: Tx, unit: str, cls: str) -> bool:
    """Whether a live approval covers a unit: its own, or its class's standing one."""
    return await tx.fetchval("""SELECT EXISTS (SELECT 1 FROM be_tier_approval WHERE withdrawn_at IS NULL AND
        ((scope_kind = 'unit' AND scope = $1) OR (scope_kind = 'class' AND scope = $2)))""", unit, cls)
