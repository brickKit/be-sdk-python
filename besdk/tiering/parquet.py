"""The cold route's files (format parquet-1, be-protocol P15.15): one table's rows in a Parquet file whose schema is
built from the class's column types, every column optional, in name order, zstd; numeric and jsonb keep the source's
text, text[] its canonical JSON text (JSON-annotated, as jsonb); uuid, timestamptz (UTC microseconds) and date carry
their logical types. The same schema as be-sdk-go's tiering/parquet.go, so either SDK reads what the other wrote and
a third-party reader sees the same values. Reading back gives the values the canonical row takes, so the digest of
what was read equals the digest of what was sealed.

pyarrow (T2b: the candidate that writes the same logical types as parquet-go; polars writes neither UUID nor JSON) is
the SDK's optional extra be-sdk[tiering]: a member with a class on the object route installs it."""
from __future__ import annotations

import datetime as dt
import io
import json
import uuid
from typing import Any

from besdk import canon
from besdk.tiering.classes import Table
from besdk.tiering.rows import (BOOL, BYTEA, DATE, INT2, INT4, INT8, JSONB, NUMERIC, TEXT, TEXT_ARRAY, TIMESTAMPTZ,
                                UUID)

READ_BATCH = 1024  # the rows a read turns into Python values per call into pyarrow (see read)


def _pa():
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:  # the extra is not installed
        raise RuntimeError("tiering: the object route needs pyarrow (install be-sdk[tiering])") from None
    return pa, pq


def schema(t: Table):
    pa, _ = _pa()
    types = {TEXT: pa.string(), NUMERIC: pa.string(), JSONB: pa.json_(), TEXT_ARRAY: pa.json_(), INT2: pa.int16(),
             INT4: pa.int32(), INT8: pa.int64(), BOOL: pa.bool_(), UUID: pa.uuid(),
             TIMESTAMPTZ: pa.timestamp("us", tz="UTC"), DATE: pa.date32(), BYTEA: pa.binary()}
    return pa.schema([pa.field(n, types[t.columns[n]], nullable=True) for n in sorted(t.columns)])


def _stored(typ: str, v: Any) -> Any:
    """One value as the column stores it (from the driver's value)."""
    if v is None:
        return None
    if typ == TEXT_ARRAY:
        if any(x is None for x in v):
            raise canon.Refused("a text array with a null element has no canonical form")
        return canon.encode([canon.text(x) for x in v]).decode()
    if typ == UUID:
        return (v if isinstance(v, uuid.UUID) else uuid.UUID(str(v))).bytes
    if typ in (TEXT, NUMERIC, JSONB) and not isinstance(v, str):
        raise canon.Refused(f"{typ} {v!r}: the driver gives it as text")
    return v


def _back(typ: str, v: Any) -> Any:
    """A stored value back as the canonical row takes it."""
    if v is None:
        return None
    if typ == TEXT_ARRAY:
        return json.loads(v)
    if typ == UUID:
        return v if isinstance(v, uuid.UUID) else uuid.UUID(bytes=v)
    if typ == TIMESTAMPTZ:
        return v.astimezone(dt.timezone.utc)
    return v


def write(t: Table, rows: list[dict[str, Any]]) -> bytes:
    """One table's rows (each column to the driver's value, None for null) as a Parquet file."""
    pa, pq = _pa()
    names = sorted(t.columns)
    data = {n: [_stored(t.columns[n], r.get(n)) for r in rows] for n in names}
    buf = io.BytesIO()
    pq.write_table(pa.table(data, schema=schema(t)), buf, compression="zstd")
    return buf.getvalue()


def read(t: Table, raw: bytes) -> list[dict[str, Any]]:
    """One table's rows back; the file's columns must be the table's (names and order). Raises ValueError."""
    _, pq = _pa()
    f = pq.ParquetFile(io.BytesIO(raw))
    names = sorted(t.columns)
    if f.schema_arrow.names != names:
        raise ValueError(f"tiering: {t.name}'s columns are {names}, the file's {f.schema_arrow.names}")
    out: list[dict[str, Any]] = []
    # a batch at a time: turning a whole column into Python values holds the GIL for the whole call, and a shell's
    # other members wait (T2c: p99 stall 34 ms for 20,000 rows in a thread; 11 ms a batch at a time, the time the same)
    for b in f.iter_batches(batch_size=READ_BATCH):
        cols = {n: [_back(t.columns[n], v) for v in b.column(n).to_pylist()] for n in names}
        out += [{n: cols[n][i] for n in names} for i in range(b.num_rows)]
    return out
