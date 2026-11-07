"""What a stream declares as a source for other components' read models (chapter 09): a watermark source (P9.13) and
its record reads (P9.30). A watermark stream must declare its record reads, its pullers among the readers."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

RECORD_PAGE = 500  # how many records one Read asks for and one Snapshot page returns at most (P9.31, P9.32)
EXISTS, TOMBSTONE, PURGED = "exists", "tombstone", "purged"  # purged: the tombstone is gone too, or never existed


@dataclass
class Watermark:
    """A watermark source's declaration (P9.13): every event of the stream lists its subjects, and the member answers
    pulls of it."""

    operation: str  # the pull's operation, declared in the component's resource definition
    pullers: list[str]  # the components (service identities) that may pull: its watermark consumers and readers
    # runs and commits before a pull's first page writes its echoes (P9.16): a source mirroring an external product
    # pulls the product's changes up to now into the outbox here; when it fails no echo is written
    before: Callable[[], Awaitable[None]] | None = None


@dataclass
class Record:
    """One existing record; data is what its current full-state event carries (encoded the same way)."""

    id: str
    version: int
    data: Any


@dataclass
class ReadResult:
    """One ID's answer: exists (version and data), tombstone (the deleting version) or purged."""

    id: str
    state: str
    version: int = 0
    data: Any = None


@dataclass
class Records:
    """A stream's batch read and snapshot (P9.30): who may read, and two read functions over the member's own tables,
    run inside the SDK's transaction at REPEATABLE READ (the partitions' high points describe exactly what they read)."""

    operation: str
    readers: list[str]
    read: Callable[[Any, list[str]], Awaitable[list[ReadResult]]]  # (tx, ids) -> one result per ID, in order
    page: Callable[[Any, str, int], Awaitable[list[Record]]]  # (tx, after, limit) -> existing records by ID ascending
