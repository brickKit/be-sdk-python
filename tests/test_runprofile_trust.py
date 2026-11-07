"""The trust history on certificates (P12.24-P12.26; be-sdk-go's TestTheTrustHistoryOnCertificates): a rotation
counts only with both signatures (the previous CA's over the record, the next CA's acceptance over the body); a
revocation counts only when its own signer was trusted when it was received; a chain is judged at its receipt."""
import datetime as dt
import pathlib
import uuid

from cryptography import x509
from cryptography.hazmat.primitives import serialization

from besdk.runprofile import trust
from besdk.testing import World

T0 = dt.datetime.now(dt.timezone.utc)


def der(c: x509.Certificate) -> bytes:
    return c.public_bytes(serialization.Encoding.DER)


def signed(body: dict, key, chain: list[bytes]) -> dict:
    r = {"record_type": "be.ops.run_profile_record", "schema": "1.0", "deployment": "d", "kind": "trust.ca",
         "id": str(uuid.uuid7()), "written_at": "2026-11-07T00:00:00.000000Z",
         "writer": {"component": "ops/issue", "instance": "i"}, "body": body}
    r["signature"] = {**trust.sign_bytes(trust.signed_bytes(r), key), "cert_chain": trust.pem_chain(chain)}
    return r


def leaf_chain(w: World, component: str) -> list[bytes]:
    cert, _, _ = w.certs(component)
    return [der(x509.load_pem_x509_certificate(pathlib.Path(cert).read_bytes()))]


def rotation(a: World, b: World, accepted: bool) -> dict:
    ca_a, key_a = a.ca()
    ca_b, key_b = b.ca()
    body = {"event": trust.ROTATION, "previous": trust.fingerprint(der(ca_a)), "next": trust.fingerprint(der(ca_b)),
            "ca": ca_b.public_bytes(serialization.Encoding.PEM).decode()}
    if accepted:
        body = trust.accept(body, key_b)
    else:
        body = trust.accept(body, key_a)  # "accepted" by the wrong key: the next CA never agreed
    return signed(body, key_a, [der(ca_a)])


def test_a_rotation_needs_both_signatures(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a, b = World(tmp_path / "a"), World(tmp_path / "b")
    chain = leaf_chain(a, "infra/authz")
    roots = [b.ca()[0]]
    assert trust.build(roots, []).holds(chain, T0) == "untrusted_ca", "no rotation: A is foreign to B"
    good = trust.build(roots, [trust.TrustRecord(rotation(a, b, True), T0)])
    assert good.holds(chain, T0) == "" and good.problems == []
    bad = trust.build(roots, [trust.TrustRecord(rotation(a, b, False), T0)])
    assert bad.holds(chain, T0) == "untrusted_ca" and bad.problems, "the acceptance does not hold"


def test_a_revocation_counts_only_from_a_trusted_signer(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "x").mkdir()
    a, stranger = World(tmp_path / "a"), World(tmp_path / "x")
    chain = leaf_chain(a, "infra/authz")
    ca_a, key_a = a.ca()
    ca_x, key_x = stranger.ca()
    revoke = {"event": trust.REVOKE, "fingerprint": trust.fingerprint(chain[0])}
    at = T0 - dt.timedelta(minutes=1)
    h = trust.build([ca_a], [trust.TrustRecord(signed(revoke, key_a, [der(ca_a)]), at)])
    assert h.holds(chain, at - dt.timedelta(seconds=10)) == "", "received before the revocation"
    assert h.holds(chain, at - dt.timedelta(seconds=1)) == "revoked", "within the skew"
    assert h.holds(chain, T0) == "revoked", "after the revocation"
    forged = trust.build([ca_a], [trust.TrustRecord(signed(revoke, key_x, [der(ca_x)]), at)])
    assert forged.holds(chain, T0) == "" and forged.problems, "a revocation by an untrusted signer does not count"
    assert h.holds(chain, T0 + dt.timedelta(days=30)) == "untrusted_ca", "judged at its receipt: expired by then"
