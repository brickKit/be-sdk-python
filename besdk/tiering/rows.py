"""The canonical row and the unit's digest (be-protocol P15.8-P15.10; vector tier-digest): a row is an object keyed by
column name whose values follow the column's type (text NFC; integers and numeric as canonical decimal strings, so
no 2^53 limit; uuid lower case; timestamptz UTC microseconds; date YYYY-MM-DD; jsonb the source's text; bytea standard
base64; text[] an array of NFC strings); a null column is not written. A table's hash is the SHA-256 of its rows
sorted by their canonical primary key's bytes, each an 8-byte big-endian length and the row; the unit's digest is the
domain-separated digest of {class, schema_version, tables: [{name, rows, sha256}]}. It depends on the content only,
never on the route, the files or the column order."""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import struct
import uuid
from decimal import Decimal
from typing import Any

from besdk import canon

TEXT, NUMERIC, JSONB, BYTEA, TEXT_ARRAY = "text", "numeric", "jsonb", "bytea", "text[]"
INT2, INT4, INT8, BOOL, UUID, TIMESTAMPTZ, DATE = "int2", "int4", "int8", "bool", "uuid", "timestamptz", "date"
TYPES = (TEXT, NUMERIC, JSONB, BYTEA, TEXT_ARRAY, INT2, INT4, INT8, BOOL, UUID, TIMESTAMPTZ, DATE)
DIGEST_VERSION = 1  # P15.10
UNIT_RECORD = "be.tier.unit"


def _integer(v: Any) -> str:
    if isinstance(v, bool):
        raise canon.Refused(f"integer {v!r}")
    if isinstance(v, int):
        return str(v)
    s = canon.decimal(str(v))
    if "." in s:
        raise canon.Refused(f"integer {v!r} has a fraction")
    return s


def value(typ: str, v: Any) -> Any:
    """One column's canonical value; raises canon.Refused for a value the type does not take."""
    if typ == TEXT:
        return canon.text(v)
    if typ in (INT2, INT4, INT8):
        return _integer(v)
    if typ == NUMERIC:
        return canon.decimal(format(v, "f") if isinstance(v, Decimal) else str(v))
    if typ == BOOL:
        if not isinstance(v, bool):
            raise canon.Refused(f"boolean {v!r}")
        return v
    if typ == UUID:
        return str(v if isinstance(v, uuid.UUID) else uuid.UUID(str(v)))
    if typ == TIMESTAMPTZ:
        if isinstance(v, dt.datetime):
            if v.tzinfo is None:
                raise canon.Refused(f"instant {v!r} without an offset")
            return canon.time(v)
        return canon.instant(str(v))
    if typ == DATE:
        return (v if isinstance(v, dt.date) else dt.date.fromisoformat(str(v))).isoformat()
    if typ == JSONB:
        return v if isinstance(v, str) else canon.text(str(v))
    if typ == BYTEA:
        return base64.b64encode(bytes(v)).decode()
    if typ == TEXT_ARRAY:
        if any(x is None for x in v):
            raise canon.Refused("a text array with a null element has no canonical form")
        return [canon.text(x) for x in v]
    raise canon.Refused(f"no canonical form for the type {typ!r}")


def row(columns: dict[str, str], r: dict[str, Any]) -> dict[str, Any]:
    """The canonical row as a value: null columns left out."""
    return {c: value(t, r[c]) for c, t in columns.items() if r.get(c) is not None}


def row_bytes(columns: dict[str, str], r: dict[str, Any]) -> bytes:
    return canon.encode(row(columns, r))


def pk_bytes(columns: dict[str, str], pk: list[str], r: dict[str, Any]) -> bytes:
    """The primary key's canonical form: its columns' values in key order, as an array."""
    return canon.encode([value(columns[c], r[c]) for c in pk])


class TableHash:
    """A table's hash built from its rows in any order (P15.9): sorted by the canonical primary key's bytes."""

    def __init__(self, columns: dict[str, str], pk: list[str]):
        self.columns, self.pk = columns, pk
        self._rows: list[tuple[bytes, bytes]] = []

    def add(self, r: dict[str, Any]) -> None:
        self._rows.append((pk_bytes(self.columns, self.pk, r), row_bytes(self.columns, r)))

    def add_canonical(self, key: bytes, raw: bytes) -> None:
        """A row whose canonical key and bytes are known already (read back from a route)."""
        self._rows.append((key, raw))

    @property
    def rows(self) -> int:
        return len(self._rows)

    def hexdigest(self) -> str:
        h = hashlib.sha256()
        for _, raw in sorted(self._rows, key=lambda x: x[0]):
            h.update(struct.pack(">Q", len(raw)))
            h.update(raw)
        return h.hexdigest()


def table_hash(columns: dict[str, str], pk: list[str], rows: list[dict[str, Any]]) -> str:
    t = TableHash(columns, pk)
    for r in rows:
        t.add(r)
    return t.hexdigest()


def unit_digest(cls: str, schema_version: str, tables: list[tuple[str, int, str]]) -> str:
    """The unit's digest over its tables' (name, rows, sha256), the tables sorted by name (P15.9)."""
    body = {"class": cls, "schema_version": schema_version,
            "tables": [{"name": n, "rows": str(k), "sha256": h} for n, k, h in sorted(tables)]}
    return canon.digest(UNIT_RECORD, DIGEST_VERSION, body)


_RANGES = {INT2: 2**15, INT4: 2**31, INT8: 2**63}


def from_canonical(columns: dict[str, str], canonical: dict[str, Any]) -> dict[str, Any]:
    """A canonical row read back from a route as the values a copy writes again (the driver's: numeric and jsonb as
    text), so writing it gives the same canonical bytes (P15.8, P15.31, P15.35). Raises canon.Refused for a column the
    table lacks or a value out of the column's range."""
    out: dict[str, Any] = {}
    for c, v in canonical.items():
        typ = columns.get(c)
        if typ is None:
            raise canon.Refused(f"column {c!r} is not the table's")
        if typ in _RANGES:
            n = int(v)
            if not -_RANGES[typ] <= n < _RANGES[typ]:
                raise canon.Refused(f"{c} {v} is out of the range of {typ}")
            out[c] = n
        elif typ == UUID:
            out[c] = uuid.UUID(v)
        elif typ == TIMESTAMPTZ:
            out[c] = dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
        elif typ == DATE:
            out[c] = dt.date.fromisoformat(v)
        elif typ == BYTEA:
            out[c] = base64.b64decode(v)
        elif typ == TEXT_ARRAY:
            out[c] = list(v)
        else:  # text, numeric, jsonb as their text; bool as it is
            out[c] = v
    return out
