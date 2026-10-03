"""The pure parts of events (be-protocol P12; vectors `envelope`): names, CloudEvents headers, causation,
acceptance of an inbound message, the state-mode cursor and the redelivery decision."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from besdk import ids
from besdk.errors import ProtocolError
from besdk.events.model import Event

HOP_LIMIT = 10
_SEG = r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*"
_SUBJECT_RE = re.compile(rf"{_SEG}(?:\.{_SEG}){{2,}}\.v[1-9][0-9]*")
_COMPONENT_RE = re.compile(r"[a-z][a-z0-9-]*/[a-z][a-z0-9-]*")
_UINT_RE = re.compile(r"0|[1-9][0-9]*")
_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})")


def check_subject(subject: str) -> str:
    """``<domain>.<name>.<event…>.v<n>``: ≥ 4 segments, each ``[a-z][a-z0-9]*(_[a-z0-9]+)*`` (P12.3)."""
    if not isinstance(subject, str) or not _SUBJECT_RE.fullmatch(subject):
        raise ProtocolError("SUBJECT_INVALID", repr(subject))
    return subject


def check_component(component_id: str) -> str:
    if not _COMPONENT_RE.fullmatch(component_id):
        raise ProtocolError("COMPONENT_INVALID", repr(component_id))
    return component_id


def stream_of(subject: str) -> str:
    """``BE_<FIRST SEGMENT>`` (P12.4)."""
    return "BE_" + check_subject(subject).split(".", 1)[0].upper()


def stream_filter(subject: str) -> str:
    return check_subject(subject).split(".", 1)[0] + ".>"


def durable_name(component_id: str, subject: str) -> str:
    """``<component with / as _>__<subject with . as __>`` — injective (P12.5)."""
    check_component(component_id)
    return component_id.replace("/", "_") + "__" + check_subject(subject).replace(".", "__")


def dlq_subject(durable: str, subject: str) -> str:
    return f"dlq.{durable}.{subject}"


# --- publishing ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class OutboxRow:
    id: str
    subject: str
    aggregate_type: str
    aggregate_id: str
    aggregate_version: int
    occurred_at: str | datetime
    traceparent: str = ""
    causation_id: str = ""
    hop_count: int = 0
    payload_json: str = "{}"


def _as_dt(t: str | datetime) -> datetime:
    if isinstance(t, datetime):
        return t
    if not _TIME_RE.fullmatch(t):
        raise ProtocolError("ENVELOPE_INVALID", f"time {t!r}")
    return datetime.fromisoformat(t.replace("Z", "+00:00"))


def ce_time(t: str | datetime) -> str:
    """UTC with ``Z``; fractional seconds only when non-zero, trailing zeros removed, ≤ 6 digits."""
    d = _as_dt(t).astimezone(timezone.utc)
    s = d.strftime("%Y-%m-%dT%H:%M:%S")
    if d.microsecond:
        s += "." + f"{d.microsecond:06d}".rstrip("0")
    return s + "Z"


def legal_entity_of(payload: object) -> str:
    v = payload.get("legal_entity_id") if isinstance(payload, dict) else None
    return v if isinstance(v, str) else ""


def headers_of(row: OutboxRow, *, component_id: str, version: str, events_file: str,
               transaction_document: bool = False) -> dict[str, str]:
    """The complete header map of one outbox row, sorted by name (P12, Envelope)."""
    check_subject(row.subject)
    eid = str(ids.parse_id(row.id))
    if row.aggregate_version < 1 or row.hop_count < 0:
        raise ProtocolError("ENVELOPE_INVALID", "aggregate version ≥ 1, hop count ≥ 0")
    le = legal_entity_of(json.loads(row.payload_json))
    if transaction_document and not le:
        raise ProtocolError("LEGAL_ENTITY_MISSING", row.subject)
    h = {
        "Nats-Msg-Id": eid, "ce-specversion": "1.0", "ce-id": eid, "ce-source": component_id,
        "ce-type": row.subject, "ce-time": ce_time(row.occurred_at), "ce-subject": row.aggregate_id,
        "content-type": "application/json",
        "ce-dataschema": f"{component_id}@{version}/contracts/events/{events_file}#{row.subject}",
        "ce-aggregatetype": row.aggregate_type, "ce-aggregateversion": str(row.aggregate_version),
        "ce-hopcount": str(row.hop_count),
    }
    if row.causation_id:
        h["ce-causationid"] = row.causation_id
    if row.traceparent:
        h["traceparent"] = row.traceparent
    if le:
        h["ce-legalentity"] = le
    return dict(sorted(h.items()))


def derive(kind: str, *, handled: tuple[str, int] | None = None,
           job: tuple[str, int] | None = None) -> tuple[str, int]:
    """Causation and hop count of an event published (or a job enqueued) in this context (P12.8)."""
    if kind == "event" and handled is not None:
        return handled[0], handled[1] + 1
    if kind == "queued_job" and job is not None:
        return job
    return "", 0


# --- consuming -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Inbound:
    component_id: str
    subject: str
    aggregate_type: str
    transaction_document: bool = False

    @property
    def durable(self) -> str:
        return durable_name(self.component_id, self.subject)


@dataclass
class Acceptance:
    event: Event | None = None
    dlq_reason: str = ""
    dlq_subject: str = ""
    dlq_headers: dict[str, str] = field(default_factory=dict)


def _uint(h: dict, name: str, minimum: int = 0) -> int:
    v = h.get(name)
    if not isinstance(v, str) or not _UINT_RE.fullmatch(v) or int(v) < minimum:
        raise ProtocolError("ENVELOPE_INVALID", f"{name}={v!r}")
    return int(v)


def _event_of(sub: Inbound, h: dict, delivery: int) -> Event:
    for name in ("ce-id", "ce-source", "ce-type", "ce-time", "ce-subject", "ce-aggregatetype"):
        if not h.get(name):
            raise ProtocolError("ENVELOPE_INVALID", f"{name} missing")
    if h.get("ce-specversion") != "1.0" or h.get("ce-type") != sub.subject:
        raise ProtocolError("ENVELOPE_INVALID", "specversion or type")
    if h["ce-aggregatetype"] != sub.aggregate_type or h.get("content-type") != "application/json":
        raise ProtocolError("ENVELOPE_INVALID", "aggregate type or content type")
    try:
        eid = str(ids.parse_id(h["ce-id"]))
    except ProtocolError:
        raise ProtocolError("ENVELOPE_INVALID", "ce-id is not a UUIDv7") from None
    return Event(subject=h["ce-type"], aggregate_id=h["ce-subject"], version=_uint(h, "ce-aggregateversion", 1),
                 id=eid, aggregate_type=h["ce-aggregatetype"], source=h["ce-source"],
                 traceparent=h.get("traceparent", ""), causation_id=h.get("ce-causationid", ""),
                 occurred_at=_as_dt(h["ce-time"]), hop_count=_uint(h, "ce-hopcount"), delivery=delivery,
                 legal_entity=h.get("ce-legalentity", ""))


def accept(sub: Inbound, headers: dict[str, str], payload: bytes, delivery: int) -> Acceptance:
    """Decide whether a delivered message reaches the handler or the dead letters (P12.7, P12.8, P12.14)."""
    try:
        ev = _event_of(sub, headers, delivery)
        if ev.hop_count > HOP_LIMIT:
            raise ProtocolError("HOP_LIMIT", str(ev.hop_count))
        try:
            body = json.loads(payload)
        except ValueError:
            raise ProtocolError("PAYLOAD_INVALID", "not JSON") from None
        if not isinstance(body, dict):
            raise ProtocolError("PAYLOAD_INVALID", "not a JSON object")
        if sub.transaction_document and (not ev.legal_entity or ev.legal_entity != legal_entity_of(body)):
            raise ProtocolError("LEGAL_ENTITY_MISSING", "ce-legalentity missing or disagrees with the payload")
        ev.payload, ev.payload_bytes = body, payload
        return Acceptance(event=ev)
    except ProtocolError as e:
        return dead_letter(sub, e.reason, delivery)


def dead_letter(sub: Inbound, reason: str, delivery: int) -> Acceptance:
    d = sub.durable
    return Acceptance(dlq_reason=reason, dlq_subject=dlq_subject(d, sub.subject),
                      dlq_headers={"be-dlq-consumer": d, "be-dlq-delivery": str(delivery), "be-dlq-reason": reason})


def cursor_applies(cursor: int | None, version: int) -> bool:
    """State mode: a version runs the handler only when greater than the cursor (P12.6)."""
    return cursor is None or version > cursor


@dataclass(frozen=True)
class Redelivery:
    action: str  # ack | nak | dlq
    handled: bool
    delay_ns: int = 0
    reason: str = ""
    dlq_msg_id: str = ""


def redelivery(delivery: int, max_deliver: int, backoff_ns: list[int], outcome: str, durable: str,
               stream_seq: int) -> Redelivery:
    """The runtime's decision for one delivery; the broker has max_deliver −1 and no backoff (P12.5, P12.7)."""
    msg_id = f"dlq:{durable}:{stream_seq}"
    if delivery > max_deliver:
        return Redelivery("dlq", False, reason="MAX_DELIVER", dlq_msg_id=msg_id)
    if outcome == "ok":
        return Redelivery("ack", True)
    if outcome == "permanent":
        return Redelivery("dlq", True, reason="PERMANENT", dlq_msg_id=msg_id)
    return Redelivery("nak", True, delay_ns=backoff_ns[min(delivery, len(backoff_ns)) - 1])
