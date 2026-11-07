"""Document number series (be-protocol P5.18-P5.20): numbers count per legal entity, per series code and per period
(empty when the series never restarts)."""
from __future__ import annotations

from dataclasses import dataclass

from besdk import errs
from besdk.db.pool import DB, Tx


@dataclass(frozen=True)
class Series:
    legal_entity: str
    code: str
    period: str = ""

    @property
    def key(self) -> str:
        return f"{self.legal_entity}/{self.code}/{self.period}"


async def next_number(tx: Tx, s: Series) -> int:
    """The next number of a continuous series, in the issuing transaction: the series row stays locked until the
    transaction ends and a rollback returns the number, so no gap is left. Take it after the idempotency claim."""
    return await _next(tx, s, True)


async def next_gapped_number(db: DB, s: Series) -> int:
    """A number of a series that allows gaps, in a transaction of its own; a number is never reused."""
    return await db.tx(lambda tx: _next(tx, s, False))


async def register_imported(tx: Tx, s: Series, gapless: bool, last: int) -> None:
    """Imported historical numbers: new numbers continue after last (never lowered)."""
    stored = await tx.fetchval("""INSERT INTO be_doc_series (series, last_number, gapless) VALUES ($1, $2, $3)
        ON CONFLICT (series) DO UPDATE SET last_number = greatest(be_doc_series.last_number, excluded.last_number)
        RETURNING gapless""", s.key, last, gapless)
    _same_kind(s, gapless, stored)


async def _next(tx: Tx, s: Series, gapless: bool) -> int:
    row = await tx.fetchrow("""INSERT INTO be_doc_series (series, last_number, gapless) VALUES ($1, 1, $2)
        ON CONFLICT (series) DO UPDATE SET last_number = be_doc_series.last_number + 1
        RETURNING last_number, gapless""", s.key, gapless)
    _same_kind(s, gapless, row["gapless"])  # the error rolls the transaction back, increment included
    return row["last_number"]


def _same_kind(s: Series, want: bool, stored: bool) -> None:
    if want != stored:
        raise errs.new("INTERNAL").wrap(ValueError(f"series {s.key} is declared gapless={stored} but used as gapless={want}"))


@dataclass(frozen=True)
class UsedMax:
    series: Series
    max: int


async def continuous_maxima(tx: Tx) -> list[UsedMax]:
    """Every continuous series with its used maximum, ordered by key (P14.5). The key is split on its first two
    slashes: a legal entity or a series code with a slash is not told apart."""
    out = []
    for r in await tx.fetch("SELECT series, last_number FROM be_doc_series WHERE gapless ORDER BY series"):
        parts = (r["series"].split("/", 2) + ["", ""])[:3]
        out.append(UsedMax(Series(*parts), r["last_number"]))
    return out


async def raise_continuous(tx: Tx, s: Series, to: int) -> int:
    """Raises a continuous series' used maximum to `to` (P14.5, P5.20: never lowered; a series not there is created)
    and answers the maximum before, so the owner voids the numbers between. A series that allows gaps answers
    REQUEST_INVALID."""
    r = await tx.fetchrow("SELECT last_number, gapless FROM be_doc_series WHERE series = $1 FOR UPDATE", s.key)
    before, gapless = (0, True) if r is None else (r["last_number"], r["gapless"])
    if not gapless:
        raise errs.new("REQUEST_INVALID", f"series {s.key} allows gaps: it is not raised")
    if to > before:
        await tx.execute("""INSERT INTO be_doc_series (series, last_number, gapless) VALUES ($1, $2, true)
            ON CONFLICT (series) DO UPDATE SET last_number = excluded.last_number""", s.key, to)
    return before
