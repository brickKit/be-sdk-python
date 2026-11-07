"""The recorder's side of the evidence (be-protocol chapter 12, P12.15-P12.18, P12.23): what a full restore must not
lose goes to the evidence store (the audit component). A tightening is written in two phases: the intent before the
business transaction (to the store, else the alert channel, else the double failure is recorded locally), the
conclusion in it. General records and conclusions go through the local evidence outbox, which be.evidence.transfer
empties (transfer.py); every evidence-bearing change writes its change history row, by which a replay knows it is
already reflected."""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from besdk import canon, reqctx
from besdk.db.pool import DB, Tx
from besdk.obs import Logger

RECORD_TYPE = "be.evidence.record"
INTENT_TIMEOUT = 2.0  # seconds the intent waits for the evidence store, and then for the alert channel (P12.16)
DOUBLE_FAILURE = "evidence_double_failure"  # the category of the record a double failure leaves (P12.23)
PHASES = ("intent", "effective", "cancelled", "general")


@dataclass(frozen=True)
class Object:
    """What a record changes, with its version before the change ("" when it has none)."""

    type: str
    id: str
    version: str = ""


@dataclass(frozen=True)
class Sealed:
    """A record's canonical bytes and digest (prefix be.evidence.record.v1)."""

    id: str
    phase: str
    record: bytes
    digest: str


class Store(Protocol):
    """The evidence store's append: one status per sealed record, in order ("accepted", "duplicate", "conflict",
    "invalid")."""

    async def append(self, recs: list[Sealed]) -> list[str]: ...


class Alerter(Protocol):
    """The alert channel (the run profile's alert kind, P12.7): whether its machine-readable copy was written."""

    async def alert(self, body: dict) -> bool: ...


def seal(record: dict) -> Sealed:
    """A record's canonical bytes and digest (the security records' canonical form, P12.9); raises canon.Refused for a
    value the canonical form does not take."""
    raw = canon.encode(record)
    return Sealed(record["id"], record["phase"], raw, canon.digest_bytes(RECORD_TYPE, 1, raw))


@dataclass(frozen=True)
class Position:
    epoch: str
    seq: int


async def commit(tx: Tx) -> Position:
    """The next commit sequence under the counter's row lock (P12.17): the evidence transactions of one database are
    serialized from here to their commit, so the sequence's order is the commit order."""
    e = await tx.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
    epoch = f"{e['generation']}.{e['timeline']}"
    last = await tx.fetchval("SELECT last_seq FROM be_evidence_counter FOR UPDATE") + 1
    await tx.execute("UPDATE be_evidence_counter SET epoch = $1, last_seq = $2", epoch, last)
    return Position(epoch, last)


async def reflect(tx: Tx, record_id: str, obj: Object) -> Position:
    """The change history row of a record: its identity, the object and its version before, the commit position."""
    pos = await commit(tx)
    await tx.execute("""INSERT INTO be_change_history (evidence_id, object_type, object_id, version_before, epoch,
        commit_seq) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)""", record_id, obj.type, obj.id, obj.version,
                     pos.epoch, pos.seq)
    return pos


async def reflected(tx: Tx, record_id: str) -> bool:
    """Whether a record's change is in this database (P12.18): its identity is in the change history."""
    return await tx.fetchval("SELECT 1 FROM be_change_history WHERE evidence_id = $1", record_id) is not None


@dataclass
class Writer:
    """One member's evidence. store None: the evidence store is not configured, every intent goes to the alert
    channel; alert None: no alert channel, a failed intent is a double failure."""

    db: DB
    component: str
    instance: str
    log: Logger
    store: Store | None = None
    alert: Alerter | None = None
    now: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    def record(self, rid: str, category: str, phase: str, subject: str, obj: Object, change: dict,
               pos: Position | None = None) -> dict:
        """One be.evidence.record's fields (P12.9)."""
        o = {"type": obj.type, "id": obj.id, **({"version": obj.version} if obj.version else {})}
        r = {"record_type": RECORD_TYPE, "schema": "1.0", "id": rid,
             "recorder": {"component": self.component, "instance": self.instance}, "category": category,
             "phase": phase, "object": o, "change": change, "at": canon.time(self.now())}
        if subject:
            r["subject"] = subject
        if pos is not None:
            r["position"] = {"epoch": pos.epoch, "commit_seq": pos.seq}
        return r

    async def intent(self, category: str, subject: str, obj: Object, change: dict) -> Pending:
        """A tightening's intent before the business transaction (P12.16): to the evidence store within
        INTENT_TIMEOUT, else to the alert channel within the same limit, else the double failure is recorded; the
        caller goes on in every case. The change is the change itself (what is removed), never the resulting state.
        An intent the canonical form cannot encode (a defect of the caller) is logged here at error level and raised."""
        p = Pending(self, str(uuid.uuid7()), category, subject, obj, change)
        rec = self.record(p.id, category, "intent", subject, obj, change)
        try:
            s = seal(rec)
        except canon.Refused as e:
            self.log.error("an intent could not be encoded: neither the evidence store nor the alert channel gets it",
                           event="evidence_intent_unsealed", category=category, object_type=obj.type, object=obj.id,
                           error=str(e))
            raise
        if self.store is not None:
            try:
                with reqctx.deadline_scope(time.monotonic() + INTENT_TIMEOUT):  # calls need one (P4.4)
                    st = await asyncio.wait_for(self.store.append([s]), INTENT_TIMEOUT)
                if len(st) == 1 and st[0] in ("accepted", "duplicate"):
                    p.went = "store"
                    return p
                why = f"answered {st}"
            except Exception as e:  # noqa: BLE001 - unreachable or slow: the alert channel takes it
                why = repr(e)
            self.log.warn("the evidence store did not take an intent: it goes to the alert channel", id=p.id,
                          category=category, error=why)
        if self.alert is not None:
            try:
                if await asyncio.wait_for(self.alert.alert(rec), INTENT_TIMEOUT):
                    p.went = "alert"
                    return p
            except Exception:  # noqa: BLE001 - the alert channel logs its own failure
                pass
        self.log.error("an intent reached neither the evidence store nor the alert channel",
                       event=DOUBLE_FAILURE, id=p.id, category=category)
        p.went = "lost"
        await self._double_failure(p, rec["at"])
        return p

    async def _double_failure(self, p: Pending, at: str) -> None:
        """The record of a lost intent, committed locally in its own transaction: the evidence outbox sends it once the
        store answers again, and a full recovery's step 9 lists it as a loss (P12.23). It names the lost record (its
        identity, category, time), never its change. Writing it may fail too: logged, the caller goes on."""
        try:
            await self.db.tx(lambda tx: self.general(tx, DOUBLE_FAILURE, p.subject, p.obj,
                                                     {"record": p.id, "category": p.category, "at": at}))
        except Exception as e:  # noqa: BLE001
            self.log.error("the double failure's own record could not be written",
                           event="evidence_double_failure_unrecorded", id=p.id, error=repr(e))

    async def queue(self, tx: Tx, record: dict) -> None:
        """A record into the evidence outbox, in the caller's transaction."""
        s = seal(record)
        await tx.execute("""INSERT INTO be_evidence_outbox (id, phase, record, digest) VALUES ($1, $2, $3, $4)
            ON CONFLICT (id, phase) DO NOTHING""", s.id, s.phase, s.record, s.digest)

    async def history(self, tx: Tx, rid: str, category: str, phase: str, subject: str, obj: Object,
                      change: dict) -> None:
        """The change history row and the record at its commit position, in the business transaction."""
        pos = await reflect(tx, rid, obj)
        await self.queue(tx, self.record(rid, category, phase, subject, obj, change, pos))

    async def general(self, tx: Tx, category: str, subject: str, obj: Object, change: dict) -> str:
        """A change that needs no intent (a widening, an external command's outcome) in the business transaction
        (P12.15): its change history row and the general record. Returns the record's identity."""
        rid = str(uuid.uuid7())
        await self.history(tx, rid, category, "general", subject, obj, change)
        return rid


@dataclass
class Pending:
    """A tightening whose intent is written: its conclusion follows in the business transaction. went is where the
    intent went: "store", "alert" or "lost" (the double failure)."""

    writer: Writer
    id: str
    category: str
    subject: str
    obj: Object
    change: dict
    went: str = ""

    async def effective(self, tx: Tx) -> None:
        """The conclusion in the business transaction (P12.16, P12.17): the change history row and the effective
        record at the commit sequence."""
        await self.writer.history(tx, self.id, self.category, "effective", self.subject, self.obj, self.change)

    async def cancelled(self) -> None:
        """The business transaction did not commit (P12.16): the cancelled conclusion, in a transaction of its own."""
        w = self.writer
        await w.db.tx(lambda tx: w.queue(tx, w.record(self.id, self.category, "cancelled", self.subject, self.obj,
                                                      self.change)))
