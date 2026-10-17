"""Streams, partitions, the CloudEvents envelope and the transport port (be-protocol chapter 06)."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum, StrEnum
from typing import Any, Awaitable, Callable, Protocol

MAX_PAYLOAD = 64 << 10  # over it, the payload travels by claim check (P6.14)
MAX_HOPS = 10  # P6.15
MAX_SUBJECTS = 1000
REDELIVERY_RETENTION = 7 * 24 * 3600.0  # published outbox rows (P6.12)
TRANSPORT_RETENTION = 7 * 24 * 3600.0  # messages in the transport (P6.27)

_STREAM = re.compile(r"[a-z0-9_]+(\.[a-z0-9_]+)*")
_FNV_OFFSET, _FNV_PRIME = 0xCBF29CE484222325, 0x100000001B3


@dataclass(frozen=True)
class Stream:
    """A declared event stream with its fixed number of partitions (P6.1)."""

    name: str  # e.g. erp.inventory.stock
    partitions: int  # 1-256, never changed after release

    def validate(self) -> None:
        if not _STREAM.fullmatch(self.name):
            raise ValueError(f"stream {self.name!r}: lower-case letters, digits, underscores and dots")
        if not 1 <= self.partitions <= 256:
            raise ValueError(f"stream {self.name}: partitions must be 1-256, not {self.partitions}")

    def partition_of(self, record_id: str) -> int:
        """FNV-1a 64 of the record ID's lower-case form, modulo the partition count."""
        h = _FNV_OFFSET
        for b in record_id.lower().encode():
            h = ((h ^ b) * _FNV_PRIME) & 0xFFFFFFFFFFFFFFFF
        return h % self.partitions


class Projection(StrEnum):
    FULL = "full"
    PARTIAL = "partial"
    DELETE = "delete"


@dataclass
class Event:
    """One event a module emits."""

    type: str  # declared in the contract with its major version: erp.inventory.stock_moved.v1
    record_id: str
    version: int
    projection: Projection
    data: Any = None  # encoded as JSON
    legal_entity: str = ""  # transaction facts
    subjects: list[str] | None = None  # watermark sources: the affected principals (at most 1000)


def subject(stream: str, partition: int, event_type: str) -> str:
    """The transport subject of a stream partition and type (P6.17)."""
    return f"be.{stream}.{partition}.{event_type}"


def jetstream_name(stream: str) -> str:
    return "BE_" + stream.replace(".", "_").upper()


def durable_name(component_id: str, subscription: str) -> str:
    """A subscription's durable name (P6.18)."""
    return (component_id + "_" + subscription).replace("/", "_").replace(".", "_").replace("-", "_")


def subject_matches(pattern: str, subj: str) -> bool:
    """NATS-style matching: "*" is one token, ">" the rest."""
    p, s = pattern.split("."), subj.split(".")
    for i, tok in enumerate(p):
        if tok == ">":
            return len(s) > i
        if i >= len(s) or (tok != "*" and tok != s[i]):
            return False
    return len(p) == len(s)


def go_json(v: Any) -> str:
    """JSON as the Go SDK writes it (compact, <, > and & escaped), so the envelope is byte-identical across SDKs."""
    s = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return s.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


H_ID, H_TYPE, H_SOURCE, H_SUBJECT = "ce-id", "ce-type", "ce-source", "ce-subject"
H_STREAM, H_PARTITION, H_IMPL, H_EPOCH, H_SEQ = "ce-bestream", "ce-bepartition", "ce-beimpl", "ce-beepoch", "ce-beseq"
H_VERSION, H_PROJECTION, H_DIGEST, H_HOPS = "ce-beversion", "ce-beprojection", "ce-bedigest", "ce-behops"
H_CAUSATION, H_LEGAL_ENTITY, H_SUBJECTS, H_CLAIM = "ce-becausation", "ce-belegalentity", "ce-besubjects", "ce-beclaim"


def digest(event_id: str, payload: bytes) -> str:
    """The row digest of P6.4: SHA-256 of the event ID, a zero byte and the payload, lower-case hex."""
    return hashlib.sha256(event_id.encode() + b"\x00" + payload).hexdigest()


@dataclass
class Row:
    """One outbox row as the forwarder publishes it."""

    ord: int
    event_id: str
    stream: str
    partition: int
    type: str
    occurred_at: datetime
    record_id: str
    version: int
    projection: str
    payload: bytes
    digest: str
    seq: int
    epoch: tuple[int, int]  # (recovery generation, timeline)
    legal_entity: str = ""
    subjects: list[str] | None = None
    causation: str = ""
    hops: int = 0
    trace: dict[str, str] = field(default_factory=dict)
    claim: bool = False

    def headers(self, source: str, impl: str) -> dict[str, str]:
        """The envelope of a numbered row (P6.13)."""
        at = self.occurred_at.astimezone(timezone.utc)
        h = {"ce-specversion": "1.0", H_ID: self.event_id, H_SOURCE: "urn:be:" + source, H_TYPE: self.type,
             "ce-time": at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z", H_SUBJECT: self.record_id,
             "ce-datacontenttype": "application/json", H_STREAM: self.stream, H_PARTITION: str(self.partition),
             H_IMPL: impl, H_EPOCH: f"{self.epoch[0]}.{self.epoch[1]}", H_SEQ: str(self.seq),
             H_VERSION: str(self.version), H_PROJECTION: self.projection, H_DIGEST: self.digest, H_HOPS: str(self.hops)}
        if self.causation:
            h[H_CAUSATION] = self.causation
        if self.legal_entity:
            h[H_LEGAL_ENTITY] = self.legal_entity
        if self.subjects is not None:
            h[H_SUBJECTS] = go_json(self.subjects)
        if self.claim:
            h[H_CLAIM] = "true"
        for k, v in self.trace.items():
            h["ce-" + k] = v
        return h


@dataclass
class Message:
    """What travels: a subject, the envelope headers and the payload; the ce-id header is the message ID."""

    subject: str
    headers: dict[str, str]
    data: bytes

    @property
    def id(self) -> str:
        return self.headers.get(H_ID, "")


@dataclass
class Delivery(Message):
    attempt: int = 1  # 1 for the first delivery
    in_progress: Callable[[], Awaitable[None]] | None = None  # extends the ack wait of a slow message (P6.19)


class Outcome(IntEnum):
    ACK = 0  # processed and committed
    NAK = 1  # failed; redeliver after the backoff
    TERM = 2  # never deliver again (dead-lettered)


DEFAULT_MAX_DELIVER, DEFAULT_ACK_WAIT, DEFAULT_BACKOFF_START, DEFAULT_BACKOFF_MAX = 3, 30.0, 1.0, 30.0


@dataclass
class SubscriptionSpec:
    """A durable subscription (P6.18); seconds throughout."""

    durable: str
    streams: list[str]
    filters: list[str] = field(default_factory=list)  # subjects (wildcards allowed); empty: every subject of the streams
    max_deliver: int = 0
    ack_wait: float = 0.0
    backoff: list[float] = field(default_factory=list)  # between deliveries; the last value repeats
    concurrency: int = 0

    def defaults(self) -> SubscriptionSpec:
        s = SubscriptionSpec(self.durable, list(self.streams), list(self.filters), self.max_deliver or DEFAULT_MAX_DELIVER,
                             self.ack_wait or DEFAULT_ACK_WAIT, list(self.backoff), self.concurrency or 1)
        if not s.backoff:
            d = DEFAULT_BACKOFF_START
            while d < DEFAULT_BACKOFF_MAX:
                s.backoff.append(d)
                d *= 2
            s.backoff.append(DEFAULT_BACKOFF_MAX)
        return s

    def backoff_for(self, attempt: int) -> float:
        """The delay before delivery number attempt+1."""
        return self.backoff[min(max(attempt, 1), len(self.backoff)) - 1]


Handler = Callable[[Delivery], Awaitable[Outcome]]


class Subscription(Protocol):
    async def stop(self) -> None: ...


class TooLarge(Exception):
    """Publish refused a message over the transport's limit (P6.21)."""


class Transport(Protocol):
    """The port both adapters implement (P6.16-P6.23)."""

    async def ensure_stream(self, s: Stream) -> None: ...

    async def publish(self, m: Message) -> None: ...

    async def subscribe(self, spec: SubscriptionSpec, h: Handler) -> Subscription: ...

    async def notify(self, subject: str, data: bytes) -> None: ...

    async def listen(self, subject: str, fn: Callable[[bytes], None]) -> Subscription: ...

    @property
    def max_message(self) -> int: ...

    async def close(self) -> None: ...
