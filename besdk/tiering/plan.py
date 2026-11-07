"""The planning function (be-protocol P15.37; vector tier-plan): what a unit does next (none, seal, transfer, release,
destroy) and what holds it back. The same function in every SDK; the transfer job asks it for every unit it considers.
A held action answers none with its reasons sorted."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

COMPLETED, AUDIT = "completed_history", "audit_evidence"  # the tierable categories
OPEN, SEALED, PUBLISHED, RELEASED, DESTROYED = "open", "sealed", "published", "released", "destroyed"
NONE, SEAL, TRANSFER, RELEASE, DESTROY = "none", "seal", "transfer", "release", "destroy"
NOT_TIERABLE, RESIDENCE_BELOW, NEVER_REASON, RESIDENCE = "not_tierable", "residence_below_n", "never", "residence"
IMMUTABLE_ROUTE, GRACE, RECENT, LEASE, NO_BACKUP = "immutable_route", "grace", "recent_records", "lease", "no_backup"
PENDING, HELD, APPROVAL = "retention_pending", "held", "approval"
NEVER = -1  # a residence that keeps a class online
BLOCKED_ALERT_AFTER = dt.timedelta(days=30)  # blocked this long past its residence: alert (protocol 11)


@dataclass(frozen=True)
class PlanClass:
    category: str
    residence_days: int  # NEVER for "never"
    retention_pending: bool = False
    erasable: bool = False


@dataclass
class PlanUnit:
    state: str
    anchor_max: dt.datetime | None = None  # open: the latest anchor among the candidates
    blockers: list[str] = field(default_factory=list)
    route_immutable: bool = False
    published_at: dt.datetime | None = None
    newest_created: dt.datetime | None = None
    old_leases: int = 0
    backup_after_publish: bool = False
    retention_due: bool = False
    approved: bool = False
    holds: int = 0


@dataclass
class Decision:
    action: str
    reasons: list[str]
    alert: bool = False  # blocked for more than BLOCKED_ALERT_AFTER past its residence


def _held(action: str, why: list[str]) -> Decision:
    return Decision(action, []) if not why else Decision(NONE, sorted(why))


def plan(now: dt.datetime, n: dt.timedelta, grace: dt.timedelta, c: PlanClass, u: PlanUnit) -> Decision:
    """One unit's next action at now, with N (the list's default range) and the grace after a publish."""
    if c.category not in (COMPLETED, AUDIT):
        return Decision(NONE, [NOT_TIERABLE])
    residence = dt.timedelta(days=c.residence_days)
    if c.residence_days != NEVER and residence < n:
        return Decision(NONE, [RESIDENCE_BELOW])
    if u.state == DESTROYED:
        return Decision(NONE, [])
    if u.state != OPEN and u.retention_due:
        return _held(DESTROY, [w for w, on in ((PENDING, c.retention_pending), (HELD, u.holds > 0),
                                               (APPROVAL, not u.approved)) if on])
    if u.state == OPEN:
        if c.residence_days == NEVER:
            return Decision(NONE, [NEVER_REASON])
        due = u.anchor_max + residence  # type: ignore[operator]
        if due > now:
            return Decision(NONE, [RESIDENCE])
        if u.blockers:
            d = _held(SEAL, list(u.blockers))
            d.alert = now >= due + BLOCKED_ALERT_AFTER
            return d
        return Decision(SEAL, [])
    if u.state == SEALED:
        if u.route_immutable and c.erasable:
            return Decision(NONE, [IMMUTABLE_ROUTE])
        return Decision(TRANSFER, [])
    if u.state == PUBLISHED:
        why = []
        if u.published_at + grace > now:  # type: ignore[operator]
            why.append(GRACE)
        if u.newest_created > now - n:  # type: ignore[operator]
            why.append(RECENT)
        if u.old_leases > 0:
            why.append(LEASE)
        if not u.backup_after_publish:
            why.append(NO_BACKUP)
        return _held(RELEASE, why)
    return Decision(NONE, [])  # released
