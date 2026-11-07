"""An adapter component's external commands (be-protocol chapter 13; the assembly project's protocol 10): a command is
written in the business transaction, the SDK's job be.extcmd.process sends it, and a result that is not known is
queried before anything is sent again. Each step reads the command under a row lock, asks the state table (state.py)
what the event makes of it and writes the answer; nothing is sent inside a transaction."""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol

from besdk import canon, crash, errs, reqctx
from besdk.config import Key, Type
from besdk.db.pool import DB, Tx
from besdk.evidence import Object, Writer
from besdk.extcmd.state import (ACCEPTED, BEFORE_SEND, CLAIM, FAILED, FINALITY_PASSED, LEASE_LOST, MANUAL,
                                MAYBE_REACHED, NOT_REACHED, PARAMS_RECORD, PREPARED, QUERY_DOWN, QUERY_FOUND,
                                QUERY_NOT_FOUND, REJECTED, RETRY_WAIT, SEND_RESULT, SENDING, SUCCEEDED, UNKNOWN,
                                WARN_AFTER, Capabilities, Event, Facts, deadline, step)
from besdk.obs import Logger

BACKOFF_START, BACKOFF_CAP = timedelta(minutes=1), timedelta(hours=1)
SEND_LEASE = timedelta(minutes=2)  # a claimed command stays sending this long before a run takes its claimant as gone
SEND_TIMEOUT = 30.0  # seconds one call to the external system may take (the adapter's read timeout lies within it)
LIST_TIMEOUT = 120.0  # the listing of the external records in a full recovery's reconciliation (P13.11)
RUN_EVERY, BATCH = 2.0, 50  # how often the job looks for due commands, and how many it takes a run
ISSUED_CATEGORY = "external_command_issued"  # the evidence category of an irrevocable command's issue record (P13.4)
QUERY_ONLY_KEY = "EXTERNAL_COMMANDS_QUERY_ONLY"  # the deployment's query-only switch (P13.10)


def keys() -> list[Key]:
    """The configuration keys of a member that sends external commands."""
    return [Key(QUERY_ONLY_KEY, Type.BOOLEAN, default=False)]  # query, never send (P13.10)


@dataclass
class Command:
    """A command as the adapter sends and queries it."""

    id: str
    requester: str
    reference: str
    operation: str
    params: dict
    expires_at: datetime | None = None
    external_id: str = ""


@dataclass
class Result:
    """A send's outcome as the adapter classifies it (P13.5): cls is not_reached, maybe_reached, accepted, succeeded,
    failed or rejected; outcome the external system's report kept on a final state; response a digest or a short
    summary of the external response, never a secret."""

    cls: str
    external_id: str = ""
    outcome: dict | None = None
    response: str = ""


@dataclass
class Answer:
    """A query's outcome: the external system has a record of the reference, in state, or has none."""

    found: bool
    state: str = ""
    external_id: str = ""
    outcome: dict | None = None
    response: str = ""


class Adapter(Protocol):
    """Talks to the external system. send raising is a result the adapter could not classify: maybe_reached; query
    raising is "the query is unavailable". An adapter that lists the external records by time window also has
    list(start, end) -> list[reconcile.Record] (P13.11)."""

    async def send(self, c: Command) -> Result: ...

    async def query(self, c: Command) -> Answer: ...


def backoff(attempts: int) -> timedelta:
    d = BACKOFF_START
    for _ in range(1, attempts):
        if d >= BACKOFF_CAP:
            break
        d *= 2
    return min(d, BACKOFF_CAP)


def digest_of(s: str) -> str:
    return "sha256:" + hashlib.sha256(s.encode()).hexdigest() if s else ""


def _outcome(m: dict | None) -> str | None:
    return json.dumps(m) if m else None


@dataclass
class _Row:
    cmd: Command
    facts: Facts
    caps: Capabilities
    attempts: int
    now: datetime


class _NotDue(Exception):
    pass


@dataclass
class Commands:
    """A member's external commands. evidence writes the issue records of irrevocable commands; without it
    irrevocable ones are never sent."""

    db: DB
    component: str
    log: Logger
    evidence: Writer | None = None
    query_only: bool = False  # EXTERNAL_COMMANDS_QUERY_ONLY (P13.10): query, never send
    adapter: Adapter | None = None
    ops: dict[str, Capabilities] = field(default_factory=dict)

    async def request(self, tx: Tx, requester: str, reference: str, operation: str, params: dict,
                      expires_at: datetime | None = None) -> tuple[Command, str]:
        """Writes a command in the caller's transaction (P13.2), idempotent by the requesting component and its
        business reference: the same reference with the same parameters returns the command already there (and its
        state); with other parameters it answers IDEMPOTENCY_KEY_REUSED and writes nothing. The job sends it after the
        commit."""
        if self.adapter is None:
            raise errs.new("CAPABILITY_UNSUPPORTED", "this member sends no external commands")
        caps = self.ops.get(operation)
        if caps is None:
            raise errs.new("REQUEST_INVALID", "no such external operation: " + operation)
        try:
            raw, digest = canon.encode(params), canon.digest(PARAMS_RECORD, 1, params)
        except canon.Refused as e:
            raise errs.new("REQUEST_INVALID", f"the parameters have no canonical form: {e}") from None
        cmd = Command(str(uuid.uuid7()), requester, reference, operation, params, expires_at)
        status = await tx.execute("""INSERT INTO be_external_command (id, requester, reference, operation, params,
            params_digest, irrevocable, state, reason, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'prepared', 'requested', $8)
            ON CONFLICT (requester, reference) DO NOTHING""", uuid.UUID(cmd.id), requester, reference, operation, raw,
                                  digest, caps.irrevocable, expires_at)
        if status.endswith(" 1"):
            return cmd, PREPARED
        r = await tx.fetchrow("""SELECT id::text AS id, state, params_digest, operation FROM be_external_command
            WHERE requester = $1 AND reference = $2""", requester, reference)
        if r["params_digest"] != digest or r["operation"] != operation:
            raise errs.new("IDEMPOTENCY_KEY_REUSED", "this business reference was requested with other parameters")
        cmd.id = r["id"]
        return cmd, r["state"]

    def process_job(self):
        """be.extcmd.process, the SDK's singleton job that sends and queries the due commands."""
        from besdk.work.job import IDEMPOTENT_EXTERNAL, SINGLETON, UNLIMITED, Job

        async def handle(run) -> None:
            await self.run()
            await run.complete()

        return Job("be.extcmd.process", SINGLETON, handle, IDEMPOTENT_EXTERNAL, interval=RUN_EVERY,
                   max_attempts=UNLIMITED, timeout=600.0)

    async def run(self) -> int:
        """Processes the commands due now, a batch at most; answers how many it took."""
        due = await self.db.background_tx(lambda tx: tx.fetch("""SELECT id::text AS id FROM be_external_command
            WHERE next_at <= now() AND state IN ('prepared', 'retry_wait', 'sending', 'unknown', 'accepted')
            ORDER BY next_at, id LIMIT $1""", BATCH))
        for r in due:
            await self._process(r["id"])
        return len(due)

    async def loop(self) -> None:
        """The commands as the host's loop in the recovery phases verifying and read_only, query only (P13.10)."""
        while True:
            try:
                await self.run()
            except Exception as e:  # noqa: BLE001 - the next run tries again
                self.log.warn("the external commands could not be processed", error=repr(e))
            await asyncio.sleep(RUN_EVERY)

    async def _load(self, tx: Tx, cid: str) -> _Row:
        """Locks a due command (another run holding it is skipped) and reads the primary's time."""
        r = await tx.fetchrow("""SELECT id::text AS id, requester, reference, operation, params, state, external_id,
                expires_at, resend_deadline, first_reached_at, last_reached_at, unknown_since, accepted_since,
                unrecorded_until, (SELECT count(*) FROM be_external_attempt a WHERE a.command = c.id) AS attempts,
                now() AS now
            FROM be_external_command c WHERE id = $1 AND next_at <= now() FOR UPDATE SKIP LOCKED""", uuid.UUID(cid))
        if r is None:
            raise _NotDue
        cmd = Command(r["id"], r["requester"], r["reference"], r["operation"], json.loads(bytes(r["params"])),
                      r["expires_at"], r["external_id"] or "")
        f = Facts(r["state"], r["resend_deadline"], r["expires_at"], r["first_reached_at"], r["last_reached_at"],
                  r["unknown_since"], r["accepted_since"], r["unrecorded_until"])
        return _Row(cmd, f, self.ops.get(r["operation"], Capabilities()), r["attempts"], r["now"])

    async def _process(self, cid: str) -> None:
        """Takes one command a step forward: a claim and a send, a takeover, a query."""
        todo = {}

        async def body(tx: Tx) -> None:
            todo.clear()
            r = todo["row"] = await self._load(tx, cid)
            s = r.facts.state
            if s in (PREPARED, RETRY_WAIT):
                if self.query_only:  # P13.10: they stay where they are
                    return await self._set(tx, r, s, "query-only: not sent", r.now + BACKOFF_START)
                nxt, why = step(r.facts, Event(CLAIM), r.caps, r.now)
                if nxt == UNKNOWN:
                    return await self._set(tx, r, UNKNOWN, why, r.now)
                todo["send"] = True
                await self._set(tx, r, SENDING, why, r.now + SEND_LEASE)  # committed before the send (P13.4)
                crash.point("extcmd.claim/commit")
                return None
            if s == SENDING:  # due while sending: its claimant is gone (P13.5)
                nxt, why = step(r.facts, Event(LEASE_LOST), r.caps, r.now)
                return await self._set(tx, r, nxt, why, r.now)
            if s == UNKNOWN:
                todo["query"] = True
                return None
            if s == ACCEPTED:
                fin, since = r.caps.finality, r.facts.accepted_since
                if fin > timedelta(0) and since is not None and r.now - since < fin:
                    return await self._set(tx, r, ACCEPTED, "waiting for the final state", since + fin)
                nxt, why = step(r.facts, Event(FINALITY_PASSED), r.caps, r.now)
                if nxt == ACCEPTED and r.caps.query:
                    todo["query"] = True
                    return None
                return await self._set(tx, r, nxt, why, r.now + BACKOFF_CAP)
            return None

        try:
            await self.db.background_tx(body)
        except Exception as e:  # the transaction entry maps what body raised; "not due" is ours
            if isinstance(e.__cause__, _NotDue) or isinstance(e, _NotDue):
                return
            raise
        if todo.get("send"):
            crash.point("extcmd.claim/done")
            await self._send(todo["row"])
        elif todo.get("query"):
            await self._query(todo["row"])

    async def _set(self, tx: Tx, r: _Row, state: str, why: str, nxt: datetime) -> None:
        """Writes a transition and when the command is next due."""
        if state != r.facts.state:
            self.log.info("an external command moved", event="external_command_state", command=r.cmd.id,
                          operation=r.cmd.operation, **{"from": r.facts.state}, to=state, why=why)
        await tx.execute("""UPDATE be_external_command SET state = $2, reason = $3, next_at = $4, updated_at = now(),
            unknown_since = CASE WHEN $2 = 'unknown' THEN coalesce(unknown_since, now()) ELSE NULL END,
            accepted_since = CASE WHEN $2 = 'accepted' THEN coalesce(accepted_since, now()) ELSE NULL END,
            settled_at = CASE WHEN $2 IN ('succeeded', 'failed', 'rejected') THEN now() END WHERE id = $1""",
                         uuid.UUID(r.cmd.id), state, why, nxt)
        r.facts.state = state

    async def _now(self, tx: Tx, r: _Row) -> None:
        r.now = await tx.fetchval("SELECT now()")

    async def _send(self, r: _Row) -> None:
        """Checks the deadline once more, writes an irrevocable command's issue record, sends, applies the result."""
        n = r.attempts + 1
        ok = {}

        async def before(tx: Tx) -> None:
            await self._now(tx, r)
            nxt, why = step(r.facts, Event(BEFORE_SEND), r.caps, r.now)
            if nxt == UNKNOWN:  # P13.4: not sent
                return await self._set(tx, r, UNKNOWN, why, r.now)
            ok["go"] = True
            await tx.execute("""INSERT INTO be_external_attempt (command, n, kind, started_at)
                VALUES ($1, $2, 'send', now())""", uuid.UUID(r.cmd.id), n)

        await self.db.background_tx(before)
        if not ok:
            return
        if r.caps.irrevocable and not await self._issued(r):
            async def not_sent(tx: Tx) -> None:
                await tx.execute("""UPDATE be_external_attempt SET ended_at = now(), result = 'not_sent_no_evidence'
                    WHERE command = $1 AND n = $2""", uuid.UUID(r.cmd.id), n)
                await self._set(tx, r, RETRY_WAIT, "the issue record reached neither the evidence store nor the alert "
                                                   "channel: not sent", r.now + backoff(n))
            return await self.db.background_tx(not_sent)
        try:
            with reqctx.deadline_scope(time.monotonic() + SEND_TIMEOUT):
                res = await asyncio.wait_for(self.adapter.send(r.cmd), SEND_TIMEOUT)  # type: ignore[union-attr]
        except Exception as e:  # noqa: BLE001 - a result the adapter could not classify may have reached
            res = Result(MAYBE_REACHED, response=repr(e))
        crash.point("extcmd.send/done")

        async def after(tx: Tx) -> None:
            await self._now(tx, r)
            reached = res.cls != NOT_REACHED
            if reached:
                r.facts.first_reached = r.facts.first_reached or r.now
                r.facts.last_reached = r.now
            r.facts.resend_deadline = deadline(r.facts, r.caps)
            await tx.execute("""UPDATE be_external_attempt SET ended_at = now(), result = $3, response = $4,
                reached = $5 WHERE command = $1 AND n = $2""", uuid.UUID(r.cmd.id), n, res.cls,
                             digest_of(res.response), reached)
            await tx.execute("""UPDATE be_external_command SET first_reached_at = $2, last_reached_at = $3,
                resend_deadline = $4, external_id = coalesce(nullif($5, ''), external_id),
                outcome = coalesce($6::jsonb, outcome) WHERE id = $1""", uuid.UUID(r.cmd.id), r.facts.first_reached,
                             r.facts.last_reached, r.facts.resend_deadline, res.external_id, _outcome(res.outcome))
            nxt, why = step(r.facts, Event(SEND_RESULT, result=res.cls), r.caps, r.now)
            await self._set(tx, r, nxt, why, self._next_at(r, nxt, n))
            crash.point("extcmd.result/commit")

        await self.db.background_tx(after)
        crash.point("extcmd.result/done")

    def _next_at(self, r: _Row, state: str, attempts: int) -> datetime:
        """When a command in state is next due."""
        if state == RETRY_WAIT:
            return r.now + backoff(attempts)
        if state in (UNKNOWN, SENDING):
            return r.now
        if state == ACCEPTED:
            return r.now + (r.caps.finality if r.caps.finality > timedelta(0) else BACKOFF_START)
        return r.now + BACKOFF_CAP  # manual or terminal: not processed again (the state takes it out)

    async def _issued(self, r: _Row) -> bool:
        """An irrevocable command's issue record before the send (P13.4): the evidence store, else the alert channel;
        False when neither took it."""
        if self.evidence is None:
            self.log.error("an irrevocable external command is not sent: this member writes no evidence",
                           command=r.cmd.id)
            return False
        try:
            p = await self.evidence.intent(ISSUED_CATEGORY, r.cmd.requester, Object("be.external.command", r.cmd.id),
                                           {"reference": r.cmd.reference, "operation": r.cmd.operation,
                                            "component": self.component,
                                            "params_digest": canon.digest(PARAMS_RECORD, 1, r.cmd.params)})
        except canon.Refused:
            return False
        return p.went in ("store", "alert")

    async def _query(self, r: _Row) -> None:
        """Asks the external system by the reference and applies the answer; a "not found" that allows it sends
        again with the same reference."""
        n = r.attempts + 1
        ans, e, result = Answer(False), Event(QUERY_DOWN), "unavailable"
        if r.caps.query:
            try:
                with reqctx.deadline_scope(time.monotonic() + SEND_TIMEOUT):
                    ans = await asyncio.wait_for(self.adapter.query(r.cmd), SEND_TIMEOUT)  # type: ignore[union-attr]
                e, result = ((Event(QUERY_FOUND, external=ans.state), "found:" + ans.state) if ans.found
                             else (Event(QUERY_NOT_FOUND), "not_found"))
            except Exception:  # noqa: BLE001 - the query is unavailable: backoff
                ans = Answer(False)
        crash.point("extcmd.query/done")
        resend = {}

        async def apply(tx: Tx) -> None:
            await self._now(tx, r)
            await tx.execute("""INSERT INTO be_external_attempt (command, n, kind, started_at, ended_at, result, response)
                VALUES ($1, $2, 'query', now(), now(), $3, $4)""", uuid.UUID(r.cmd.id), n, result,
                             digest_of(ans.response))
            if ans.found:
                await tx.execute("""UPDATE be_external_command SET external_id = coalesce(nullif($2, ''), external_id),
                    outcome = coalesce($3::jsonb, outcome) WHERE id = $1""", uuid.UUID(r.cmd.id), ans.external_id,
                                 _outcome(ans.outcome))
            nxt, why = step(r.facts, e, r.caps, r.now)
            if nxt == SENDING and self.query_only:  # P13.10: the table would send it again; query-only does not
                nxt, why = UNKNOWN, f"query-only: not sent again ({why})"
            if nxt == SENDING:
                resend["go"] = True
                return await self._set(tx, r, SENDING, why, r.now + SEND_LEASE)
            since = r.facts.unknown_since
            if nxt == UNKNOWN and since is not None and r.now - since > WARN_AFTER:
                self.log.warn("an external command's result has been unknown for over an hour",
                              event="external_command_unknown", command=r.cmd.id, operation=r.cmd.operation,
                              since=since.isoformat())
            await self._set(tx, r, nxt, why, r.now + backoff(n) if nxt in (UNKNOWN, ACCEPTED) else
                            self._next_at(r, nxt, n))

        await self.db.background_tx(apply)
        if resend:
            r.attempts = n  # the query was this attempt; the send is the next
            await self._send(r)

    async def register(self, tx: Tx, cid: str, result: str, why: str) -> None:
        """A person's verified outcome of a manual command (P13.8): succeeded, failed, rejected, or "did not happen"
        (sending: sent again with the same reference). The caller audits it with its grounds."""
        if result not in (SUCCEEDED, FAILED, REJECTED, SENDING):
            raise errs.new("REQUEST_INVALID", "a manual command is registered succeeded, failed, rejected or sending")
        s = await tx.fetchval("SELECT state FROM be_external_command WHERE id = $1 FOR UPDATE", uuid.UUID(cid))
        if s != MANUAL:
            raise errs.new("REQUEST_INVALID", f"only a manual command is registered (it is {s})")
        r = _Row(Command(cid, "", "", "", {}), Facts(s), Capabilities(), 0, await tx.fetchval("SELECT now()"))
        # "did not happen": the claim sends it again, same reference, through the deadline check
        await self._set(tx, r, RETRY_WAIT if result == SENDING else result, "registered: " + why, r.now)

