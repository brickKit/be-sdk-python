"""The trust material's history (P12.24-P12.27): what a signed record is checked against is not the current trust root
alone. A CA rotated out after T still vouches for the records signed under it (a rotation is signed by the previous CA
and accepted by the next one), and a credential revoked after T no longer vouches for what it signed after its
revocation (less the declared clock skew). The decisions (trusted_cas, revoked_at) are pure and follow the vector
trust-history; TrustHistory applies them to certificates."""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519

from besdk import canon

KIND_CA, KIND_CREDENTIAL = "trust.ca", "trust.credential"
ANCHOR, ROTATION, ISSUE, REVOKE = "anchor", "rotation", "issue", "revoke"
DEFAULT_SKEW = timedelta(seconds=2)  # the declared clock skew between the channels (assembly protocol 15)


def fingerprint(der: bytes) -> str:
    """A certificate's SHA-256 over its DER, lower-case hex (P12.24)."""
    return hashlib.sha256(der).hexdigest()


@dataclass(frozen=True)
class Rotation:
    """What the history knows of a rotation: which CA to which, and whether both signatures hold."""

    previous: str
    next: str
    signed: bool
    accepted: bool


@dataclass(frozen=True)
class Revocation:
    """A revocation that counts (its own signer was trusted): which certificate, received when."""

    fingerprint: str
    at: datetime


def trusted_cas(configured: list[str], rotations: list[Rotation]) -> set[str]:
    """The configured roots, and walking back along every rotation whose next CA is trusted and whose two signatures
    hold (P12.25)."""
    trusted = set(configured)
    changed = True
    while changed:
        changed = False
        for r in rotations:
            if r.next in trusted and r.signed and r.accepted and r.previous not in trusted:
                trusted.add(r.previous)
                changed = True
    return trusted


def revoked_at(chain: list[str], t: datetime, revocations: list[Revocation], skew: timedelta) -> bool:
    """A certificate of the chain was revoked at r and the record was received at t > r - skew (P12.26)."""
    return any(r.fingerprint == fp and t > r.at - skew for fp in chain for r in revocations)


def canonical(v: dict) -> bytes:
    """The canonical bytes of a JSON value (integers only: the canonical form refuses floating point)."""
    return canon.encode(json.loads(json.dumps(v)))


def sign_bytes(msg: bytes, key) -> dict:
    """{"alg", "value"} over msg: ES256 (ASN.1 DER over SHA-256) or EdDSA."""
    if isinstance(key, ec.EllipticCurvePrivateKey):
        return {"alg": "ES256", "value": base64.b64encode(key.sign(msg, ec.ECDSA(hashes.SHA256()))).decode()}
    if isinstance(key, ed25519.Ed25519PrivateKey):
        return {"alg": "EdDSA", "value": base64.b64encode(key.sign(msg)).decode()}
    raise ValueError(f"run profile: a {type(key).__name__} cannot sign records")


def verify_bytes(msg: bytes, sig: dict | None, public) -> bool:
    if not sig:
        return False
    try:
        raw = base64.b64decode(sig.get("value", ""), validate=True)
        if sig.get("alg") == "ES256" and isinstance(public, ec.EllipticCurvePublicKey):
            public.verify(raw, msg, ec.ECDSA(hashes.SHA256()))
            return True
        if sig.get("alg") == "EdDSA" and isinstance(public, ed25519.Ed25519PublicKey):
            public.verify(raw, msg)
            return True
    except (InvalidSignature, ValueError):
        return False
    return False


def pem_chain(chain: list[bytes]) -> str:
    """A chain (DER, leaf first) as PEM."""
    return "".join(x509.load_der_x509_certificate(d).public_bytes(serialization.Encoding.PEM).decode() for d in chain)


def chain_of(record: dict) -> list[bytes]:
    """A record's signer chain (DER, leaf first); raises ValueError."""
    sig = record.get("signature") or {}
    if not sig:
        raise ValueError("no signature")
    try:
        certs = x509.load_pem_x509_certificates((sig.get("cert_chain") or "").encode())
    except ValueError:
        certs = []
    if not certs:
        raise ValueError("no certificate chain")
    return [c.public_bytes(serialization.Encoding.DER) for c in certs]


def signed_bytes(record: dict) -> bytes:
    """A record's bytes to sign: the canonical encoding without the signature (P12.2)."""
    return canonical({k: v for k, v in record.items() if k != "signature"})


def verify_signature(record: dict, signer: x509.Certificate | None) -> bool:
    return signer is not None and verify_bytes(signed_bytes(record), record.get("signature"), signer.public_key())


def acceptance_bytes(body: dict) -> bytes:
    """What the next CA signs in a rotation: the body without its acceptance, canonical."""
    return canonical({k: v for k, v in body.items() if k != "acceptance"})


def accept(body: dict, next_key) -> dict:
    """A rotation's body signed by the next CA's key (P12.24), for the issuer."""
    return {**body, "acceptance": sign_bytes(acceptance_bytes(body), next_key)}


def _valid_at(c: x509.Certificate, t: datetime) -> bool:
    return c.not_valid_before_utc <= t <= c.not_valid_after_utc


def _issued_by(c: x509.Certificate, issuer: x509.Certificate) -> bool:
    try:
        c.verify_directly_issued_by(issuer)
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False


@dataclass
class TrustRecord:
    """One trust record read from the run profile with the time the store received it (P12.7)."""

    record: dict
    received: datetime


@dataclass
class TrustHistory:
    """The trust material as the run profile records it, read against the configured roots."""

    skew: timedelta = DEFAULT_SKEW
    trusted: dict[str, x509.Certificate] = field(default_factory=dict)  # by fingerprint
    revocations: list[Revocation] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)  # records not used and why (P12.25)

    def anchor_of(self, chain: list[bytes], t: datetime) -> str:
        """The trusted CA the chain (DER, leaf first) verifies to at t, by fingerprint; "" when none. Each certificate
        on the path must be valid at t."""
        try:
            certs = [x509.load_der_x509_certificate(d) for d in chain]
        except ValueError:
            return ""
        cur, rest = certs[0], certs[1:]
        for _ in range(len(certs) + 1):
            fp = fingerprint(cur.public_bytes(serialization.Encoding.DER))
            if fp in self.trusted:  # a CA itself signed it (a trust record), or the path reached a trusted CA
                return fp if _valid_at(cur, t) else ""
            if not _valid_at(cur, t):
                return ""
            for fp, ca in self.trusted.items():
                if _issued_by(cur, ca) and _valid_at(ca, t):
                    return fp
            nxt = next((c for c in rest if _issued_by(cur, c)), None)
            if nxt is None:
                return ""
            cur, rest = nxt, [c for c in rest if c is not nxt]
        return ""

    def holds(self, chain: list[bytes], t: datetime) -> str:
        """Why a signer's chain did not vouch for a record received at t (P12.26): "untrusted_ca", "revoked", or ""
        when it did."""
        anchor = self.anchor_of(chain, t)
        if not anchor:
            return "untrusted_ca"
        fps = [fingerprint(d) for d in chain] + [anchor]
        return "revoked" if revoked_at(fps, t, self.revocations, self.skew) else ""


def build(roots: list[x509.Certificate], records: list[TrustRecord], skew: timedelta = DEFAULT_SKEW) -> TrustHistory:
    """The trust records read against the configured roots (P12.25): the CAs trusted, and the revocations whose own
    signer held when they were received (P12.26). Records that do not hold are listed in problems."""
    h = TrustHistory(skew)
    known: dict[str, x509.Certificate] = {}
    configured = []
    for c in roots:
        fp = fingerprint(c.public_bytes(serialization.Encoding.DER))
        known[fp] = c
        configured.append(fp)
    rotations, revokes = [], []
    for tr in records:
        b, rid = tr.record.get("body"), tr.record.get("id", "")
        if not isinstance(b, dict):
            h.problems.append(f"{rid}: not a trust record")
            continue
        event = b.get("event")
        if event == ANCHOR:
            try:
                c = x509.load_pem_x509_certificate((b.get("ca") or "").encode())
                known[fingerprint(c.public_bytes(serialization.Encoding.DER))] = c
            except ValueError:
                pass
        elif event == ROTATION:
            try:
                nxt = x509.load_pem_x509_certificate((b.get("ca") or "").encode())
            except ValueError:
                nxt = None
            if nxt is None or fingerprint(nxt.public_bytes(serialization.Encoding.DER)) != b.get("next"):
                h.problems.append(f"{rid}: a rotation whose next CA is not the one it names")
                continue
            known[b["next"]] = nxt
            signed = False
            try:
                chain = chain_of(tr.record)
                if fingerprint(chain[-1]) == b.get("previous"):
                    prev = x509.load_der_x509_certificate(chain[-1])
                    known[b["previous"]] = prev
                    signed = verify_signature(tr.record, prev)
            except ValueError:
                pass
            accepted = verify_bytes(acceptance_bytes(b), b.get("acceptance"), nxt.public_key())
            if not (signed and accepted):
                h.problems.append(f"{rid}: a rotation without both signatures")
            rotations.append(Rotation(b.get("previous", ""), b["next"], signed, accepted))
        elif event == REVOKE:
            revokes.append((b.get("fingerprint", ""), tr))
    for fp in trusted_cas(configured, rotations):
        if (c := known.get(fp)) is not None:
            h.trusted[fp] = c
    for fp, tr in revokes:  # a revocation counts when its signer chains to a trusted CA when it was received
        try:
            chain = chain_of(tr.record)
            ok = bool(h.anchor_of(chain, tr.received)) and verify_signature(
                tr.record, x509.load_der_x509_certificate(chain[0]))
        except ValueError:
            ok = False
        if ok:
            h.revocations.append(Revocation(fp, tr.received))
        else:
            h.problems.append(f"{tr.record.get('id', '')}: a revocation whose signer is not trusted")
    return h
