"""Idempotency keys against the protocol's vector idempotency (P4.21, P4.22): which keys are valid, and the request
fingerprint."""
import pytest

from besdk import idem, protocol

CASES = protocol.vector("idempotency")["cases"]


@pytest.mark.parametrize("c", [c for c in CASES if c["kind"] == "key"], ids=lambda c: c["id"])
def test_keys(c):
    assert idem.valid_key(c["key"]) is c["valid"]


@pytest.mark.parametrize("c", [c for c in CASES if c["kind"] == "fingerprint"], ids=lambda c: c["id"])
def test_fingerprints(c):
    r = idem.Request(c["principal"], "k", c["operation"], c["target"], c["body"].encode())
    assert r.fingerprint() == c["fingerprint"]


def test_every_case_kind_is_covered():
    assert {c["kind"] for c in CASES} == {"key", "fingerprint"}
