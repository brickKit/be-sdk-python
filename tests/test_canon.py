import json

import pytest

from besdk import canon, protocol


def from_json(raw):
    return json.loads(json.dumps(raw), parse_float=float)


def test_canonical_vectors():
    for c in protocol.vector("canonical")["cases"]:
        kind, x = c["kind"], c["expect"]
        if kind == "segment":
            continue  # segment normalization is the authz package's
        try:
            got = {"decimal": lambda: canon.decimal(c["in"]), "instant": lambda: canon.instant(c["in"]),
                   "text": lambda: canon.text(c["in"]), "json": lambda: canon.encode(c["in"]).decode(),
                   "digest": lambda: canon.digest(c["record_type"], c["major"], c["in"])}[kind]()
        except canon.Refused:
            assert x.get("refused"), c["id"]
            continue
        assert not x.get("refused"), (c["id"], got)
        assert got == x["out"], c["id"]


def test_floats_and_nulls_are_refused():
    for bad in (1.5, None, {"a": None}, [1.0]):
        with pytest.raises(canon.Refused):
            canon.encode(bad)
