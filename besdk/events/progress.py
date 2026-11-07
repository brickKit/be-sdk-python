"""A subscription's progress (P6.36, P6.37; vector inbox-progress): how far each source partition has an outcome, from
the inbox rows' source positions and the subscription's run starts. A redelivery after the consumer's database went
back starts from it (P6.39)."""
from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass

from besdk.db.pool import DB, Tx

# P6.37's L: within it, counted in running time, a message delivered for the first time has an outcome (handled, a
# dead letter, or not this subscription's). At least the acknowledgement wait times the delivery limit plus the
# backoffs between them (P6.19, P6.20).
DELIVERY_LIFETIME = 600.0


@dataclass(frozen=True)
class SourceKey:
    """One partition of a source stream, as an inbox row records it."""

    component: str  # the producing component (the envelope's source without urn:be:)
    impl: str  # its implementation instance
    stream: str
    partition: int


@dataclass(frozen=True, order=True)
class Point:
    """A place on one partition within one implementation instance: the epoch (generation, timeline), then the
    sequence; ordered in that order."""

    generation: int
    timeline: int
    seq: int

    def __str__(self) -> str:
        return f"{self.generation}.{self.timeline}/{self.seq}"


@dataclass(frozen=True)
class Handled:
    """What the progress needs of one inbox row: its source partition, its position (None for a row written before
    positions were kept) and when it was handled."""

    key: SourceKey
    pos: Point | None
    at: dt.datetime | float


def cut_of(runs: list, last_in: Callable[[object, object], object | None], lifetime: float):
    """P6.37's cut: from the latest run back, the first whose start is no later than its last handling less L gives
    that time less L; None when no run lasted L. last_in(start, next start or None) is the last handling in it."""
    starts = sorted(runs)
    for k in range(len(starts) - 1, -1, -1):
        last = last_in(starts[k], starts[k + 1] if k + 1 < len(starts) else None)
        if last is not None and starts[k] <= _minus(last, lifetime):
            return _minus(last, lifetime)
    return None


def _minus(t, seconds: float):
    return t - dt.timedelta(seconds=seconds) if isinstance(t, dt.datetime) else t - seconds


def progress_of(runs: list, rows: list[Handled], lifetime: float) -> dict[SourceKey, Point]:
    """P6.37 over a subscription's run starts and inbox rows: the largest position per source partition handled at or
    before the cut. No cut, no progress: redeliver from the first row kept."""
    def last_in(start, end):
        ats = [r.at for r in rows if r.at >= start and (end is None or r.at < end)]
        return max(ats) if ats else None

    cut = cut_of(runs, last_in, lifetime)
    out: dict[SourceKey, Point] = {}
    if cut is None:
        return out
    for r in rows:
        if r.pos is None or r.at > cut:
            continue
        if r.key not in out or out[r.key] < r.pos:
            out[r.key] = r.pos
    return out


async def start_run(db: DB, subscription: str) -> None:
    """Records that an instance of a subscription starts receiving (P6.36): its runs bound the cut."""
    await db.background_tx(lambda tx: tx.execute("INSERT INTO be_subscription_run (subscription) VALUES ($1)",
                                                 subscription))


async def progress(db: DB, subscription: str, lifetime: float = DELIVERY_LIFETIME) -> dict[SourceKey, Point]:
    """A subscription's progress read from its database (P6.37). The inbox has no index for it: read at a recovery."""
    async def body(tx: Tx) -> dict[SourceKey, Point]:
        runs = [r[0] for r in await tx.fetch(
            "SELECT started_at FROM be_subscription_run WHERE subscription = $1 ORDER BY started_at", subscription)]
        lasts = {}
        for i, start in enumerate(runs):
            end = runs[i + 1] if i + 1 < len(runs) else None
            if end is None:
                lasts[start] = await tx.fetchval("SELECT max(processed_at) FROM be_inbox WHERE subscription = $1 "
                                                 "AND processed_at >= $2", subscription, start)
            else:
                lasts[start] = await tx.fetchval("SELECT max(processed_at) FROM be_inbox WHERE subscription = $1 "
                                                 "AND processed_at >= $2 AND processed_at < $3", subscription, start, end)
        cut = cut_of(runs, lambda start, end: lasts.get(start), lifetime)
        if cut is None:
            return {}
        rows = await tx.fetch("""SELECT DISTINCT ON (source_component, source_impl, source_stream, source_partition)
            source_component, source_impl, source_stream, source_partition, epoch_generation, epoch_timeline, seq
            FROM be_inbox WHERE subscription = $1 AND seq IS NOT NULL AND processed_at <= $2
            ORDER BY source_component, source_impl, source_stream, source_partition,
                epoch_generation DESC, epoch_timeline DESC, seq DESC""", subscription, cut)
        return {SourceKey(r["source_component"], r["source_impl"], r["source_stream"], r["source_partition"]):
                Point(r["epoch_generation"], r["epoch_timeline"], r["seq"]) for r in rows}

    return await db.background_tx(body)
