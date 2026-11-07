"""A full recovery's checks (be-protocol chapter 14; the assembly project's protocol 15, steps 8-10): what a component
registers (its consistency checks, the presence questions of the object types it audits, how it voids the numbers a
raise skips over) and the SDK's own checks every component with a database has (P14.2)."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from besdk.db.docno import Series
from besdk.db.pool import Tx

PASS, DIFF, UNPROVABLE = "pass", "diff", "unprovable"  # a check's outcomes (P14.1)
PRESENT, OLDER, ABSENT = "present", "older", "absent"  # a presence question's answers (P14.3)
PROJECTIONS, CONSUMER_EPOCH, AUDIT_BACKLOG = "be.projections", "be.consumer_epoch", "be.audit_backlog"


@dataclass(frozen=True)
class Difference:
    object_type: str
    object_id: str
    detail: str


@dataclass
class Result:
    """A check's answer: read-only, the same answer in the same state."""

    outcome: str
    differences: list[Difference] = field(default_factory=list)
    disposal: str = ""  # what to do about the differences


# runs fn in a transaction that is always rolled back: a check reads its database only through it (P14.1)
Read = Callable[[Callable[[Tx], Awaitable[None]]], Awaitable[None]]


@dataclass(frozen=True)
class Check:
    """One consistency check: its identity, what it rests on (the assembly project's protocol and item) and how it
    runs. It reads through read and, across components, asks the other side over the system surface between two
    reads, never inside one. A check across components is reported by one side only (P14.2)."""

    name: str
    basis: str
    run: Callable[[Read], Awaitable[Result]]


def in_tx(f: Callable[[Tx], Awaitable[Result]]) -> Callable[[Read], Awaitable[Result]]:
    """A check that only reads its own database, in one rolled-back transaction."""
    async def run(read: Read) -> Result:
        out: list[Result] = []

        async def body(tx: Tx) -> None:
            out.append(await f(tx))

        await read(body)
        return out[0]

    return run


# answers, for ids of one object type, each present object's revision; an id not in the answer is absent
PresenceFunc = Callable[[Tx, list[str]], Awaitable[dict[str, int]]]
# voids the numbers from..to of a continuous series raised over them (P14.5), in the raising transaction
VoidFunc = Callable[[Tx, Series, int, int], Awaitable[None]]


def table_presence(table: str, id_column: str, version_column: str) -> PresenceFunc:
    """Presence from a table: the id and the version (a bigint) are its columns. The names are the component's own
    constants, quoted as identifiers."""
    q = (f'SELECT "{id_column}"::text AS k, "{version_column}" AS v FROM "{table}" '
         f'WHERE "{id_column}"::text = ANY($1)')

    async def presence(tx: Tx, ids: list[str]) -> dict[str, int]:
        return {r["k"]: r["v"] for r in await tx.fetch(q, ids)}

    return presence


class Checks:
    """What a module registers in recovery_checks(c)."""

    def __init__(self) -> None:
        self.checks: list[Check] = []
        self.presence: dict[str, PresenceFunc] = {}
        self.void: VoidFunc | None = None

    def add(self, check: Check) -> None:
        if not check.name or check.run is None:
            raise ValueError("a check needs a name and run")
        if any(c.name == check.name for c in self.checks):
            raise ValueError(f"check {check.name} registered twice")
        self.checks.append(check)

    def add_presence(self, object_type: str, f: PresenceFunc) -> None:
        """The presence question of an object type: every type the component audits business changes or sensitive
        reads of (P14.3)."""
        self.presence[object_type] = f

    def set_void(self, f: VoidFunc) -> None:
        """How the component voids the numbers a raise skips over; required when it has continuous series."""
        self.void = f


def _outcome(diffs: list[Difference], disposal: str) -> Result:
    return Result(PASS) if not diffs else Result(DIFF, diffs, disposal)


async def _rows(tx: Tx, sql: str) -> list[Difference]:
    return [Difference(r[0], r[1], r[2]) for r in await tx.fetch(sql)]


async def _projections(tx: Tx) -> Result:
    """Every projection's source aligned, and no quarantined subject."""
    diffs = await _rows(tx, """SELECT 'projection', projection, 'source ' || status FROM be_projection_source
        WHERE status <> 'aligned'
        UNION ALL
        SELECT 'projection_subject', projection || '/' || subject, 'quarantined: ' || count(*) || ' events'
            FROM be_watermark_quarantine GROUP BY projection, subject
        ORDER BY 1, 2""")
    return _outcome(diffs, "等投影对齐（补取与补发）；被隔离的主体按协议 02 修复后重做检查")


async def _consumer_epoch(tx: Tx) -> Result:
    """The consumer side recorded its progress in the database's current epoch (the redelivery after the rollback
    answered), and no redelivery left a gap."""
    rec = await tx.fetchrow("SELECT epoch_generation, epoch_timeline FROM be_consumer_epoch")
    cur = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
    diffs = []
    if rec is None or (rec[0], rec[1]) != (cur[0], cur[1]):
        diffs.append(Difference("consumer_epoch", f"{cur[0]}.{cur[1]}",
                                "the redelivery after the database went back has not answered (P6.39)"))
    diffs += await _rows(tx, """SELECT 'resend_gap', subscription || ':' || source_component || '/' || source_stream
            || '/' || source_partition, kind || coalesce(' after ' || progress, '')
        FROM be_resend_gap ORDER BY id""")
    return _outcome(diffs, "等补发答完；补发不能覆盖的（beyond_retention、ahead）列进恢复报告，按协议 15 的 RC04 处置")


async def _audit_backlog(tx: Tx) -> Result:
    """The local audit records not yet transferred (or in conflict), how many and since when."""
    r = await tx.fetchrow("""SELECT count(*) FILTER (WHERE transfer_state = 'pending') AS pending,
            count(*) FILTER (WHERE transfer_state = 'conflict') AS conflict,
            min(occurred_at) FILTER (WHERE transfer_state <> 'received') AS oldest
        FROM be_audit_log""")
    diffs = []
    if r["pending"]:
        diffs.append(Difference("audit_backlog", "pending", f"{r['pending']} records not transferred, the oldest at "
                                                            f"{r['oldest'].isoformat()}"))
    if r["conflict"]:
        diffs.append(Difference("audit_backlog", "conflict",
                                f"{r['conflict']} records the audit store answered with a conflict"))
    return _outcome(diffs, "等审计转运送完（AUDIT_GRPC_URL）后重做；冲突的记录按协议 03 核对")


def builtins(subscribes: bool) -> list[Check]:
    """The SDK's own checks; the consumer epoch only for a member that subscribes."""
    out = [Check(PROJECTIONS, "装配项目协议 02；be-protocol P9.25", in_tx(_projections))]
    if subscribes:
        out.append(Check(CONSUMER_EPOCH, "be-protocol P6.39、P6.40；装配项目协议 15 的 RC04", in_tx(_consumer_epoch)))
    out.append(Check(AUDIT_BACKLOG, "装配项目协议 03 的积压与故障", in_tx(_audit_backlog)))
    return out
