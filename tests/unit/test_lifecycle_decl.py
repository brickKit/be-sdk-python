"""lifecycle.yaml v1 (P16.1) and partition units (P16.10): the declaration is loaded and its invariants
checked (a violation is fatal and names the table); range partitions are named from their lower bound."""
from datetime import datetime, timezone

import pytest

from besdk.lifecycle.decl import Declaration, LifecycleInvalid, units

UTC = timezone.utc


def load(text):
    return Declaration.parse(text)


def test_load_tables_and_partitions():
    d = load("""
lifecycle: v1
tables:
  orders: {class: document, partition: {by: created_at, grain: week, ahead: 3}}
  audit_log: {class: audit, partition: {by: at, grain: month}}
  order_lines: {follows: orders}
  settings: {class: reference}
""")
    assert d.tables["orders"].grain == "week" and d.tables["orders"].ahead == 3
    assert d.tables["audit_log"].ahead == 2 and d.tables["settings"].grain is None
    assert sorted(d.partitioned()) == ["audit_log", "orders"]


@pytest.mark.parametrize("text,table", [
    ("lifecycle: v1\ntables:\n  books: {class: ledger, pii: [note]}\n", "books"),
    ("lifecycle: v1\ntables:\n  books: {class: ledger, erasure: {subject: customer, key: cid, columns: {name: anonymize}}}\n", "books"),
    ("lifecycle: v1\ntables:\n  jobs: {class: queue, tiers: {cold: 1y after created}}\n", "jobs"),
    ("lifecycle: v1\ntables:\n  snap: {class: snapshot, retention: {min: 1y after created}}\n", "snap"),
    ("lifecycle: v1\ntables:\n  x: {class: nope}\n", "x"),
    ("lifecycle: v1\ntables:\n  x: {}\n", "x"),
])
def test_invariants_name_the_table(text, table):
    with pytest.raises(LifecycleInvalid) as ei:
        load(text)
    assert table in str(ei.value)


@pytest.mark.parametrize("text", ["version: 1\ntables: {}\n", "lifecycle: v2\ntables: {}\n", "lifecycle: v1\n"])
def test_wrong_document(text):
    with pytest.raises(LifecycleInvalid):
        load(text)


def test_units_are_named_from_their_lower_bound():
    now = datetime(2026, 12, 30, 10, tzinfo=UTC)  # ISO week 2026-W53
    assert [(n, lo.isoformat(), hi.isoformat()) for n, lo, hi in units("orders", "week", now, ahead=2)] == [
        ("orders_2026w53", "2026-12-28T00:00:00+00:00", "2027-01-04T00:00:00+00:00"),
        ("orders_2027w01", "2027-01-04T00:00:00+00:00", "2027-01-11T00:00:00+00:00"),
        ("orders_2027w02", "2027-01-11T00:00:00+00:00", "2027-01-18T00:00:00+00:00")]
    assert [n for n, _, _ in units("audit_log", "month", now, ahead=2)] == [
        "audit_log_2026m12", "audit_log_2027m01", "audit_log_2027m02"]
    assert [n for n, _, _ in units("t", "year", now, ahead=1)] == ["t_2026", "t_2027"]
