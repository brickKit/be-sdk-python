"""The run profile's and the trust history's vectors (P12.1-P12.5, P12.24-P12.26): the signed bytes and the key, each
kind's retention, a recovery generation's current value and its advance; the trusted CAs along the rotations, the
revocations within the skew, the writer as the signing certificate's identity."""
import datetime as dt

import pytest
from cryptography import x509
from cryptography.x509.oid import NameOID

from besdk import protocol
from besdk.runprofile import current_of, next_generation, object_key, retention, writer_is_signer
from besdk.runprofile.trust import Revocation, Rotation, revoked_at, signed_bytes, trusted_cas


def cases(topic):
    return protocol.vector(topic)["cases"]


@pytest.mark.parametrize("c", cases("run-profile"), ids=lambda c: c["id"])
def test_the_run_profile_vectors(c):
    i, want = c["input"], c["expected"]
    if "signed_bytes" in want:
        r = i["record"]
        assert signed_bytes(r).decode() == want["signed_bytes"]
        if "key" in want:
            assert object_key(r["deployment"], r["kind"], r["written_at"], r["id"]) == want["key"]
    elif "min_retention_days" in want:
        assert retention(i["kind"]) >= dt.timedelta(days=want["min_retention_days"])
    elif "current" in want:
        assert current_of([r for r in i["records"] if r["signature_holds"]]) == want["current"]
    elif "next" in want:
        assert next_generation(i["run_profile"], i["evidence"], *i["databases"]) == want["next"]
    else:
        pytest.fail(f"a case this test does not know: {c['id']}")


def certificate_of(deployment: str, component: str) -> x509.Certificate:
    """A certificate naming a component's service identity, enough for writer_is_signer (unsigned fields only)."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, component)])
    now = dt.datetime.now(dt.timezone.utc)
    return (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(1).not_valid_before(now).not_valid_after(now + dt.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.UniformResourceIdentifier(
                f"spiffe://{deployment}/component/{component}")]), critical=False)
            .sign(key, hashes.SHA256()))


@pytest.mark.parametrize("c", cases("trust-history"), ids=lambda c: c["id"])
def test_the_trust_history_vectors(c):
    i, want = c["input"], c["expected"]
    at = lambda s: dt.datetime.fromtimestamp(s, dt.timezone.utc)  # noqa: E731
    rot = [Rotation(r["previous"], r["next"], r["signed_by_previous"], r["accepted_by_next"]) for r in i["rotations"]]
    rev = [Revocation(r["fingerprint"], at(r["received"])) for r in i["revocations"] if r["signer_trusted"]]
    rec, chain = i["record"], i["record"]["chain"]
    reason = ""
    if chain[-1] not in trusted_cas(i["configured"], rot):
        reason = "untrusted_ca"
    elif revoked_at(chain, at(rec["received"]), rev, dt.timedelta(seconds=i["skew"])):
        reason = "revoked"
    elif rec.get("writer") is not None and not writer_is_signer(
            {"deployment": "d", "writer": {"component": rec["writer"]}}, certificate_of("d", rec["signer"])):
        reason = "writer_not_signer"
    assert (reason == "", reason) == (want["holds"], want.get("reason", ""))
