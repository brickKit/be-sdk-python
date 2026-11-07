"""The external command's state table (P13.3-P13.8; the assembly project's protocol 10), a pure function: the job reads
a command, asks step() what an event makes of it, and writes the answer. Checked against the vector external-command."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

# states (P13.3)
PREPARED, RETRY_WAIT, SENDING, UNKNOWN, ACCEPTED, MANUAL = "prepared", "retry_wait", "sending", "unknown", "accepted", "manual"
SUCCEEDED, FAILED, REJECTED = "succeeded", "failed", "rejected"
# send results as an adapter classifies them (P13.5), besides accepted, succeeded, failed, rejected
NOT_REACHED = "not_reached"  # certainly not processed: a connection failure, an explicit rate limit
MAYBE_REACHED = "maybe_reached"  # may have been processed: a read timeout, a gateway error, a cut before the answer
# event kinds
CLAIM, BEFORE_SEND, LEASE_LOST, SEND_RESULT = "claim", "before_send", "lease_lost", "send_result"
QUERY_FOUND, QUERY_NOT_FOUND, QUERY_DOWN = "query_found", "query_not_found", "query_unavailable"
FINALITY_PASSED, REGISTERED = "finality_passed", "registered"
# parameters (the assembly project's protocol 10)
WARN_AFTER = timedelta(hours=1)  # unknown this long: alert
MANUAL_AFTER = timedelta(hours=24)  # unknown, or accepted without a query, this long: manual
MIN_MARGIN = timedelta(hours=1)  # the resend deadline's safety margin, at least
PARAMS_RECORD = "be.external.command_params"  # the record type the parameters' digest is computed under (P13.2)


@dataclass(frozen=True)
class Capabilities:
    """What the external system provides for one operation (P13.9)."""

    dedupe_window: timedelta = timedelta(0)  # deduplicates by the business reference for this long; 0: does not
    last_use: bool = False  # the window counts from the last use (else from the first)
    query: bool = False  # answers a query by the business reference
    expiry: bool = False  # honours a request's expiry
    max_processing: timedelta = timedelta(0)  # a request older than this no longer lands; 0: not declared
    finality: timedelta = timedelta(0)  # an accepted request reaches its final state within this; 0: not declared
    irrevocable: bool = False  # repeating it has business consequences (money, invoices, signatures, carrier orders)


@dataclass
class Facts:
    """A command's times as step() reads them."""

    state: str
    resend_deadline: datetime | None = None
    expires_at: datetime | None = None
    first_reached: datetime | None = None
    last_reached: datetime | None = None
    unknown_since: datetime | None = None
    accepted_since: datetime | None = None
    # after a full recovery, sends may have gone on until this moment without a record (P13.12)
    unrecorded_until: datetime | None = None


@dataclass(frozen=True)
class Event:
    kind: str
    result: str = ""  # SEND_RESULT, REGISTERED
    external: str = ""  # QUERY_FOUND: accepted, succeeded, failed, rejected, or anything not final


def terminal(s: str) -> bool:
    return s in (SUCCEEDED, FAILED, REJECTED)


def deadline(f: Facts, c: Capabilities) -> datetime | None:
    """The resend deadline (P13.7): None without dedupe or without a send that may have reached the external system."""
    if c.dedupe_window <= timedelta(0):
        return None
    base = f.last_reached if c.last_use and f.last_reached is not None else f.first_reached
    if base is None:
        return None
    return base + c.dedupe_window - max(c.dedupe_window / 10, MIN_MARGIN)


def resendable(f: Facts, c: Capabilities, now: datetime) -> tuple[bool, str]:
    """A "not found" query (P13.6): sent again only when the earlier request can no longer land."""
    if (d := deadline(f, c)) is not None and not now > d:
        return True, "the external system deduplicates and the deadline has not passed"
    if f.expires_at is not None and now > f.expires_at:
        return True, "the request's expiry has passed"
    latest = f.last_reached
    if f.unrecorded_until is not None and (latest is None or f.unrecorded_until > latest):
        latest = f.unrecorded_until
    if c.max_processing > timedelta(0) and latest is not None and now - latest > c.max_processing:
        return True, "the longest processing time has passed"
    return False, "the earlier request may still land"


def step(f: Facts, e: Event, c: Capabilities, now: datetime) -> tuple[str, str]:
    """The state table (P13.3-P13.8), first matching row wins: the next state and why. Terminal states absorb
    everything; an event the table has no row for leaves the state as it is (the caller logs it)."""
    s = f.state
    if terminal(s):
        return s, "a terminal state absorbs every later result"
    past = f.resend_deadline is not None and now > f.resend_deadline
    k = e.kind
    if k == CLAIM and s in (PREPARED, RETRY_WAIT):
        return (UNKNOWN, "claimed past the resend deadline") if past else \
            (SENDING, "claimed: the transition commits before the send")
    if k == BEFORE_SEND and s == SENDING:
        return (UNKNOWN, "past the resend deadline before the send: not sent") if past else (SENDING, "sent")
    if k == LEASE_LOST and s == SENDING:
        return UNKNOWN, "the claimant is gone: whether it sent is not known"
    if k == SEND_RESULT and s == SENDING:
        r = e.result
        if r == NOT_REACHED:
            return RETRY_WAIT, "certainly not processed: backoff and send again"
        if r == MAYBE_REACHED:
            return UNKNOWN, "may have been processed: query first"
        if r == ACCEPTED:
            return ACCEPTED, "accepted, not final"
        if r in (SUCCEEDED, FAILED):
            return r, "final"
        if r == REJECTED:
            return REJECTED, "refused for good"
    elif k == QUERY_FOUND and s in (UNKNOWN, ACCEPTED):
        if terminal(e.external) or e.external == ACCEPTED:
            return e.external, "the external record"
        return s, "the external record, not final"
    elif k == QUERY_DOWN and s in (UNKNOWN, ACCEPTED):
        since = f.accepted_since if s == ACCEPTED else f.unknown_since
        if since is not None and now - since > MANUAL_AFTER:
            return MANUAL, "no answer within the manual limit"
        return s, "the query is unavailable: backoff"
    elif k == QUERY_NOT_FOUND and s == UNKNOWN:
        ok, why = resendable(f, c, now)
        return (SENDING if ok else MANUAL), why
    elif k == FINALITY_PASSED and s == ACCEPTED:
        if c.query:
            return ACCEPTED, "past the finality limit: query"
        if f.accepted_since is not None and now - f.accepted_since > MANUAL_AFTER:
            return MANUAL, "no query and past the manual limit"
        return ACCEPTED, "waiting for the callback or the reconciliation"
    elif k == REGISTERED and s == MANUAL:
        return e.result, "registered by a person, with its grounds"
    return s, "not a transition of the table: unchanged, logged"
