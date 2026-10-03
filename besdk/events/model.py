"""What components declare and receive: ``Event``, ``Events``, ``Subscription`` (apis §2.7, §3)."""

from __future__ import annotations

import enum
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Sequence


@dataclass
class Event:
    """An event. Producers set subject, aggregate_id, version and payload; the SDK fills the rest."""

    subject: str
    aggregate_id: str
    version: int
    payload: Any = None
    id: str = ""
    # filled by the SDK on consumption
    aggregate_type: str = ""
    source: str = ""
    traceparent: str = ""
    causation_id: str = ""
    occurred_at: datetime | None = None
    hop_count: int = 0
    delivery: int = 0
    legal_entity: str = ""
    payload_bytes: bytes = b""

    def json(self) -> Any:
        """The payload as JSON (decoded once from the message)."""
        if self.payload is None and self.payload_bytes:
            self.payload = json.loads(self.payload_bytes)
        return self.payload


class StartFrom(enum.Enum):
    ALL = "all"  # first creation of the durable delivers what is still in the stream (P12.5)
    NEW = "new"


@dataclass(frozen=True)
class Subscription:
    """One durable consumer: exactly one of ``apply`` (in the cursor's transaction) or ``run`` (outside)."""

    subject: str
    consumer: str = ""
    apply: Callable[[Any, Event], Awaitable[None]] | None = None
    run: Callable[[Event], Awaitable[None]] | None = None
    max_deliver: int | None = None
    backoff: tuple[float, ...] | None = None  # seconds
    start_from: StartFrom = StartFrom.ALL
    concurrency: int = 4


@dataclass(frozen=True)
class Events:
    publishes: Sequence[str] = ()
    subscribe: Sequence[Subscription] = field(default_factory=tuple)


class Permanent(Exception):
    """A handler error that goes to the dead letters at once (P12.7)."""

    def __init__(self, cause: BaseException | str):
        super().__init__(str(cause))
        self.cause = cause


def permanent(err: BaseException | str) -> Permanent:
    return Permanent(err)
