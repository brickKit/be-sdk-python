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
