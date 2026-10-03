"""``migrations/lifecycle.yaml`` v1 (be-protocol P16.1, schemas/lifecycle.schema.json) and the range units of
P16.10. Loading checks the invariants a declaration can check by itself; a violation is fatal and names the
table. Coverage of every migrated table is checked against the schema by the migration step."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

UTC = timezone.utc
CLASSES = {"master", "reference", "document", "ledger", "audit", "queue", "snapshot", "platform"}
GRAINS = ("week", "month", "year")


class LifecycleInvalid(ValueError):
    """The declaration is missing, malformed or breaks an invariant (fatal, P16.1)."""


@dataclass(frozen=True)
class Table:
    name: str
    cls: str | None
    follows: str | None = None
    by: str | None = None
    grain: str | None = None
    ahead: int = 2
    kind: str | None = None  # "list" for LIST partitions opened by a command
    raw: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Declaration:
    tables: dict[str, Table]

    @classmethod
    def parse(cls, text: str) -> "Declaration":
        doc = yaml.safe_load(text) or {}
        if not isinstance(doc, dict) or doc.get("lifecycle") != "v1" or not isinstance(doc.get("tables"), dict):
            raise LifecycleInvalid("lifecycle.yaml must be `lifecycle: v1` with a `tables` mapping")
        return cls({n: _table(n, t or {}) for n, t in doc["tables"].items()})

    @classmethod
    def load(cls, migrations: Path) -> "Declaration":
        p = Path(migrations) / "lifecycle.yaml"
        if not p.exists():
            raise LifecycleInvalid(f"{p} is missing: every component with a database declares its tables")
        return cls.parse(p.read_text())

    def partitioned(self) -> list[str]:
        return [n for n, t in self.tables.items() if t.grain]


def _table(name: str, t: dict) -> Table:
    def bad(why: str) -> LifecycleInvalid:
        return LifecycleInvalid(f"lifecycle.yaml: table {name}: {why}")

    c = t.get("class")
    if c is None and not t.get("follows"):
        raise bad("declares neither class nor follows")
    if c is not None and c not in CLASSES:
        raise bad(f"unknown class {c!r}")
    if c == "ledger" and (t.get("pii") or (t.get("erasure") or {}).get("columns")):
        raise bad("a ledger table has no pii column and no erasure.columns")
    if c == "queue" and (t.get("tiers") or {}).get("cold") not in (None, "never"):
        raise bad("a queue table declares no tiers.cold")
    if c == "snapshot" and (t.get("retention") or {}).get("min"):
        raise bad("a snapshot table declares no retention.min")
    p = t.get("partition") or {}
    grain = p.get("grain")
    if grain is not None and grain not in GRAINS:
        raise bad(f"partition grain {grain!r}")
    ahead = p.get("ahead", 2)
    if not isinstance(ahead, int) or ahead < 1:
        raise bad("partition.ahead is an integer ≥ 1")
    return Table(name, c, t.get("follows"), p.get("by"), grain, ahead, p.get("kind"), dict(t))


# --- range units (P16.10) ---------------------------------------------------------------------------


def _start(grain: str, d: date) -> date:
    if grain == "week":
        return d - timedelta(days=d.isoweekday() - 1)
    if grain == "month":
        return d.replace(day=1)
    return d.replace(month=1, day=1)


def _next(grain: str, d: date) -> date:
    if grain == "week":
        return d + timedelta(weeks=1)
    if grain == "month":
        return (d.replace(day=28) + timedelta(days=4)).replace(day=1)
    return d.replace(year=d.year + 1)


def unit_name(parent: str, grain: str, lo: date) -> str:
    if grain == "week":
        y, w, _ = lo.isocalendar()
        return f"{parent}_{y}w{w:02d}"
    if grain == "month":
        return f"{parent}_{lo.year}m{lo.month:02d}"
    return f"{parent}_{lo.year}"


def units(parent: str, grain: str, now: datetime, *, ahead: int = 2) -> list[tuple[str, datetime, datetime]]:
    """The unit containing ``now`` and ``ahead`` more: (name, [lower, upper) in UTC)."""
    lo, out = _start(grain, now.astimezone(UTC).date()), []
    for _ in range(ahead + 1):
        hi = _next(grain, lo)
        out.append((unit_name(parent, grain, lo), datetime.combine(lo, datetime.min.time(), UTC),
                    datetime.combine(hi, datetime.min.time(), UTC)))
        lo = hi
    return out
