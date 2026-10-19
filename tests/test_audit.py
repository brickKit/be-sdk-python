"""Audit record bodies without a database."""
from datetime import datetime, timezone

import pytest

from besdk import records
from besdk.audit import Record, _body


def test_changes_keep_integers_and_leave_out_nulls():
    """A record's state often holds integers (a version, a minor unit): they are allowed in the canonical encoding;
    a fraction is refused (decimals are strings), as in the Go SDK."""
    r = Record(category="business_change", operation="demo.currency.manage", outcome="succeeded",
               actor="0190f3a1-0000-7000-8000-0000000000a1",
               changes={"after": {"code": "KWD", "minor_unit": 3, "version": 2, "rate": "7.65", "cash_unit": None}})
    raw, _ = records.seal("be.audit.record", _body(r, "0190f3a1-0000-7000-8000-0000000000c1", datetime.now(timezone.utc)))
    assert b'"minor_unit":3' in raw and b'"version":2' in raw
    assert b"cash_unit" not in raw, "a null member is left out"
    r.changes = {"after": {"ratio": 0.5}}
    with pytest.raises(Exception):
        records.seal("be.audit.record", _body(r, "0190f3a1-0000-7000-8000-0000000000c2", datetime.now(timezone.utc)))
