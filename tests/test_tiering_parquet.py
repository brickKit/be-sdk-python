"""The cold route's Parquet files (P15.8, P15.15), the same cases as be-sdk-go's tiering/parquet_test.go: every column
type round-trips to the same canonical rows and hash; a file whose columns are not the table's is refused; a canonical
row turned back into driver values writes the same bytes; a text array is NFC each, in order."""
import datetime as dt
import json
import uuid
from decimal import Decimal

import pytest

from besdk import canon
from besdk.tiering import parquet, rows
from besdk.tiering.classes import Table
from besdk.tiering.rows import (BOOL, BYTEA, DATE, INT2, INT4, INT8, JSONB, NUMERIC, TEXT, TEXT_ARRAY, TIMESTAMPTZ,
                                UUID)

ALL = Table("everything", {"id": UUID, "t": TEXT, "i2": INT2, "i4": INT4, "i8": INT8, "n": NUMERIC, "b": BOOL,
                           "ts": TIMESTAMPTZ, "d": DATE, "j": JSONB, "bin": BYTEA, "tags": TEXT_ARRAY}, pk="id")
UTC = dt.timezone.utc


def u(*b):
    return uuid.UUID(bytes=bytes(b) + bytes(16 - len(b)))


ROWS = [
    {"id": u(1, 2, 3), "t": "café ünïcode 中文", "i2": -32768, "i4": 2147483647, "i8": 9007199254740993,
     "n": "-1250.5000", "b": True, "ts": dt.datetime(2026, 3, 1, 7, 0, 0, 500000, tzinfo=UTC), "d": dt.date(1970, 1, 1),
     "j": '{"a": [1, 2], "b": null}', "bin": bytes([0, 255, 16]), "tags": ["B-2", "café", "A-1"]},
    {"id": u(9), "t": "", "i2": None, "i4": None, "i8": None, "n": None, "b": False, "ts": None, "d": None, "j": None,
     "bin": b"", "tags": None},
    {"id": u(5), "t": None, "i2": 0, "i4": -1, "i8": -9007199254740993, "n": "0.000", "b": None,
     "ts": dt.datetime(1999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC), "d": dt.date(2099, 12, 31), "j": "[]",
     "bin": None, "tags": []},
]


def test_every_column_type_round_trips():
    back = parquet.read(ALL, parquet.write(ALL, ROWS))
    assert len(back) == len(ROWS)
    for want, got in zip(ROWS, back):
        assert rows.row_bytes(ALL.columns, got) == rows.row_bytes(ALL.columns, want)
    assert rows.table_hash(ALL.columns, ["id"], back) == rows.table_hash(ALL.columns, ["id"], ROWS)
    with pytest.raises(ValueError):
        parquet.read(Table("everything", {"id": UUID, "t": TEXT}, pk="id"), parquet.write(ALL, ROWS))


def test_a_canonical_row_is_written_again_as_it_was():
    for r in ROWS[:2]:
        first = rows.row_bytes(ALL.columns, r)
        back = rows.from_canonical(ALL.columns, json.loads(first))
        assert rows.row_bytes(ALL.columns, back) == first
        parquet.write(ALL, [back])  # written to an object again
    with pytest.raises(canon.Refused):
        rows.from_canonical(ALL.columns, {"i2": "40000"})  # out of the column's range
    with pytest.raises(canon.Refused):
        rows.from_canonical(ALL.columns, {"nope": "x"})


def test_a_text_array_is_nfc():
    assert rows.row_bytes({"tags": TEXT_ARRAY}, {"tags": ["café", "B"]}).decode() == '{"tags":["café","B"]}'
    raw = parquet.write(Table("x", {"id": UUID, "tags": TEXT_ARRAY}, pk="id"), [{"id": u(1), "tags": ["café"]}])
    assert parquet.read(Table("x", {"id": UUID, "tags": TEXT_ARRAY}, pk="id"), raw)[0]["tags"] == ["café"]


def test_the_driver_gives_numeric_as_text():
    with pytest.raises(canon.Refused):
        parquet.write(Table("x", {"id": UUID, "n": NUMERIC}, pk="id"), [{"id": u(1), "n": Decimal("1.5")}])
