"""The canonical row, the primary key, the table hash and the unit's digest against be-protocol's vector tier-digest
(P15.8-P15.10)."""
import pytest

from besdk import canon, protocol
from besdk.tiering import rows


def native(columns, r):
    """The vector writes bytea as hex; the database gives bytes."""
    return {c: (bytes.fromhex(v) if columns.get(c) == rows.BYTEA and isinstance(v, str) else v) for c, v in r.items()}


@pytest.mark.parametrize("c", protocol.vector("tier-digest")["cases"], ids=lambda c: c["id"])
def test_the_tier_digest_vectors(c):
    i, want = c["input"], c["expected"]
    if want.get("refused"):
        with pytest.raises((canon.Refused, ValueError)):
            rows.row_bytes(i["columns"], native(i["columns"], i["row"]))
    elif "canonical_row" in want:
        assert rows.row_bytes(i["columns"], native(i["columns"], i["row"])).decode() == want["canonical_row"]
    elif "canonical_pk" in want:
        assert rows.pk_bytes(i["columns"], i["pk"], native(i["columns"], i["row"])).decode() == want["canonical_pk"]
    elif "sha256" in want:
        assert rows.table_hash(i["columns"], i["pk"], [native(i["columns"], r) for r in i["rows"]]) == want["sha256"]
    else:
        tables = [(t["name"], len(t["rows"]), rows.table_hash(t["columns"], t["pk"],
                                                               [native(t["columns"], r) for r in t["rows"]]))
                  for t in i["tables"]]
        assert rows.unit_digest(i["class"], i["schema_version"], tables) == want["digest"]
