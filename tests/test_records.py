"""Security records on the wire (D76, P8.8): the generated schemas through fastjsonschema, canonical bytes, digests."""
import pytest

from besdk import protocol, records


def test_security_record_vectors():
    cases = protocol.vector("security-records")["cases"]
    assert cases
    for c in cases:
        if not c["valid"]:
            with pytest.raises((records.Invalid, ValueError)):
                raw, digest = records.seal(c["record_type"], c["record"])
                records.open_(c["record_type"], raw, digest)
            continue
        raw, digest = records.seal(c["record_type"], c["record"])
        assert raw.decode() == c["canonical"], c["id"]
        assert digest == c["digest"], c["id"]
        records.open_(c["record_type"], raw, digest)


def test_open_refuses_what_is_not_exactly_canonical():
    rec = {"component": "demo/order", "version": "3.0.0", "definitions": []}
    raw, digest = records.seal("be.security.definition_set", rec)
    assert records.open_("be.security.definition_set", raw, digest) == rec
    for bad, why in ((b'{"component": "demo/order","definitions":[],"version":"3.0.0"}', "whitespace"),
                     (b'{"version":"3.0.0","component":"demo/order","definitions":[]}', "key order"),
                     ('{"component":"demo/order","definitions":[],"version":"é"}'.encode(), "text not in NFC"),
                     (b'{"component":"demo/order","component":"demo/order","definitions":[],"version":"3.0.0"}', "a duplicate key"),
                     (b'{"component":"demo/order","definitions":[],"version":1.5}', "floating point")):
        with pytest.raises(records.Invalid):
            records.open_("be.security.definition_set", bad, digest)
        assert why
    with pytest.raises(records.Invalid):
        records.open_("be.security.definition_set", raw, "sha256:" + "0" * 64)
