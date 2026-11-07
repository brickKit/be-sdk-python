"""The owner's replay of its evidence during a full recovery (P12.19-P12.21): be.evidence.v1.ReplayService.Replay,
callable only by the recovery tool (ops/recover). One record at a time: recorded by this component, reflected by its
identity already (nothing done), else the category's registered function re-applies the change in one transaction
with the change history row under the record's identity; or, for a category whose effect lies outside the recovery
group, the registered check asks the other side outside any transaction."""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

from besdk import errs
from besdk._gen.be.evidence.v1 import replay_pb2, replay_pb2_grpc
from besdk.db.pool import DB, Tx
from besdk.evidence.writer import RECORD_TYPE, Object, reflect, reflected
from besdk.obs import Logger

RECOVERY_TOOL = "ops/recover"
# outcomes (P12.20, P12.21)
REFLECTED, APPLIED, DANGLING, UNSUPPORTED = "reflected", "applied", "dangling", "unsupported"
DEFERRED, CONSISTENT, INCONSISTENT = "deferred", "consistent", "inconsistent"


class Dangling(Exception):
    """An object the change refers to no longer exists: nothing is done and the record goes to review."""


class Deferred(Exception):
    """What the re-execution depends on has not arrived yet: nothing is done, the recovery tool asks again (P12.22)."""


@dataclass(frozen=True)
class Replayed:
    """An evidence record its owner re-executes."""

    id: str
    category: str
    phase: str
    subject: str
    object: Object
    change: dict
    at: datetime  # when it was recorded


ReplayFunc = Callable[[Tx, Replayed], Awaitable[None]]
# (consistent, detail); raising is an inconsistent answer too (the other side could not be asked)
CheckFunc = Callable[[Replayed], Awaitable[tuple[bool, str]]]


class Replays:
    """A module's replay functions and checks by evidence category; a category is one or the other (P12.20)."""

    def __init__(self) -> None:
        self.replays: dict[str, ReplayFunc] = {}
        self.checks: dict[str, CheckFunc] = {}

    def _once(self, category: str) -> None:
        if category in self.replays or category in self.checks:
            raise ValueError(f"the category {category} is registered twice: one replay or one check (P12.20)")

    def add(self, category: str, f: ReplayFunc) -> None:
        self._once(category)
        self.replays[category] = f

    def check(self, category: str, f: CheckFunc) -> None:
        self._once(category)
        self.checks[category] = f


def parse(raw: bytes, component: str) -> Replayed:
    """A be.evidence.record this component recorded, in a phase that is replayed; raises ValueError."""
    try:
        rec = json.loads(raw)
    except ValueError:
        raise ValueError("the record is not JSON") from None
    if not isinstance(rec, dict) or rec.get("record_type") != RECORD_TYPE or not rec.get("id"):
        raise ValueError("not a be.evidence.record")
    if (rec.get("recorder") or {}).get("component") != component:
        raise ValueError(f"recorded by {(rec.get('recorder') or {}).get('component')}, not this component")
    if rec.get("phase") not in ("effective", "general", "intent"):
        raise ValueError(f"a record in phase {rec.get('phase')} is not replayed")
    o = rec.get("object") or {}
    try:
        at = datetime.fromisoformat(rec.get("at", ""))
    except ValueError:
        raise ValueError(f"the record's at {rec.get('at')!r} is not a time") from None
    return Replayed(rec["id"], rec.get("category", ""), rec["phase"], rec.get("subject", ""),
                    Object(o.get("type", ""), o.get("id", ""), o.get("version", "")), rec.get("change") or {}, at)


async def replay_tx(tx: Tx, r: Replayed, f: ReplayFunc) -> bool:
    """Replays one record in the caller's transaction (P12.20, P12.21): reflected already, it does nothing and answers
    True; else f re-executes it and the change history row is written under the record's identity (no new evidence:
    the store has it). Dangling and Deferred leave the caller to roll back."""
    if await reflected(tx, r.id):
        return True
    await f(tx, r)
    await reflect(tx, r.id, r.object)
    return False


class _Rollback(Exception):
    pass


class ReplayServer(replay_pb2_grpc.ReplayServiceServicer):
    def __init__(self, db: DB, component: str, replays: Replays, log: Logger):
        self.db, self.component, self.replays, self.log = db, component, replays, log

    async def Replay(self, request: replay_pb2.ReplayRequest, context) -> replay_pb2.ReplayResponse:  # noqa: N802
        try:
            r = parse(request.record, self.component)
        except ValueError as e:
            raise errs.new("REQUEST_INVALID", str(e)) from None
        if (check := self.replays.checks.get(r.category)) is not None:
            return await self._check(request.recovery, r, check)
        f = self.replays.replays.get(r.category)
        if f is None:
            return replay_pb2.ReplayResponse(outcome=UNSUPPORTED, detail="no replay for the category " + r.category)
        out = {"outcome": "", "detail": ""}

        async def body(tx: Tx) -> None:
            try:
                out["outcome"] = REFLECTED if await replay_tx(tx, r, f) else APPLIED
            except Dangling as e:
                out.update(outcome=DANGLING, detail=str(e) or "an object the change refers to no longer exists")
                raise _Rollback from e
            except Deferred as e:
                out.update(outcome=DEFERRED, detail=str(e) or "the replay depends on what has not arrived yet")
                raise _Rollback from e

        try:
            await self.db.tx(body)
        except Exception as e:  # the transaction entry maps what fn raised; the rollback is ours (Dangling, Deferred)
            if not isinstance(e.__cause__, _Rollback):
                raise
        self.log.info("an evidence record replayed", event="evidence_replayed", id=r.id, category=r.category,
                      phase=r.phase, outcome=out["outcome"], recovery=request.recovery)
        return replay_pb2.ReplayResponse(**out)

    async def _check(self, recovery: str, r: Replayed, f: CheckFunc) -> replay_pb2.ReplayResponse:
        """A checked category (P12.20): reflected by its identity, else the component's check outside any
        transaction; nothing written either way."""
        outcome, detail = REFLECTED, ""
        if not await self.db.tx(lambda tx: reflected(tx, r.id)):
            try:
                ok, why = await f(r)
                outcome, detail = (CONSISTENT, "") if ok else (INCONSISTENT, why)
            except Exception as e:  # noqa: BLE001 - not asked is inconsistent, for review
                outcome, detail = INCONSISTENT, f"could not be checked: {e}"
        self.log.info("an evidence record checked", event="evidence_checked", id=r.id, category=r.category,
                      phase=r.phase, outcome=outcome, detail=detail, recovery=recovery)
        return replay_pb2.ReplayResponse(outcome=outcome, detail=detail)
