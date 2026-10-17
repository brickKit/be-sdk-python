"""User access tokens against vector jwt-verify (P8.1-P8.3) and the key set's refresh rules (P8.2, CK-ID-01)."""
import httpx
import pytest

from besdk import errs, protocol
from besdk.identity import KeySet, TokenError, Verifier, refusal


def jwks_client(jwks, hits, status=200):
    def handler(request):
        hits.append(request.url)
        return httpx.Response(status[0] if isinstance(status, list) else status, json=jwks)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def load():
    v = protocol.vector("jwt-verify")
    return v["cases"][0], v["cases"][1:]


def fixture_verifier(keys, f):
    return Verifier(f["issuer"], f["audience"], keys, now=lambda: f["now"])


async def test_jwt_verify_vectors():
    f, cases = load()
    hits = []
    keys = KeySet("http://iam/jwks", jwks_client(f["jwks"], hits))
    await keys.refresh()
    v = fixture_verifier(keys, f)
    for c in cases:
        keys.last_unknown = None  # each case may refresh once
        want = c["expect"]
        if want["valid"]:
            u = await v.verify(c["token"])
            assert u.subject == "0190f3a1-0000-7000-8000-0000000000a1", c["id"]
            continue
        with pytest.raises(TokenError) as e:
            await v.verify(c["token"])
        assert e.value.problem == want["problem"], c["id"]
        r = refusal(e.value)
        assert errs.is_(r, "be", "TOKEN_INVALID") and r.detail == "the credential is missing or not valid", c["id"]
        assert r.problem("") == refusal(TokenError("other")).problem(""), "the reason stays out of the answer"


async def test_an_unknown_kid_refreshes_at_most_every_30_seconds():  # CK-ID-01
    f, cases = load()
    unknown = next(c["token"] for c in cases if c["id"] == "kid-unknown")
    hits, clock = [], [1000.0]
    keys = KeySet("http://iam/jwks", jwks_client(f["jwks"], hits), now=lambda: clock[0])
    await keys.refresh()
    v = fixture_verifier(keys, f)
    for _ in range(5):
        with pytest.raises(TokenError):
            await v.verify(unknown)
    assert len(hits) == 2, "one fetch at start, one refresh for the unknown kid"
    clock[0] += 31
    with pytest.raises(TokenError):
        await v.verify(unknown)
    assert len(hits) == 3


async def test_stale_keys_are_used_for_an_hour_then_the_answer_is_unavailable():  # P8.2
    f, cases = load()
    valid = next(c["token"] for c in cases if c["id"] == "valid-es256")
    status, clock = [200], [1000.0]

    def handler(request):
        return httpx.Response(status[0], json=f["jwks"])

    keys = KeySet("http://iam/jwks", httpx.AsyncClient(transport=httpx.MockTransport(handler)), now=lambda: clock[0])
    await keys.refresh()
    v = fixture_verifier(keys, f)
    status[0] = 503  # the identity member is down from now on
    clock[0] += 3500
    assert (await v.verify(valid)).subject
    clock[0] += 200
    with pytest.raises(errs.Error) as e:
        await v.verify(valid)
    assert errs.is_(e.value, "be", "DEPENDENCY_UNAVAILABLE") and e.value.metadata["dependency"] == "iam"


async def test_keys_never_fetched_are_unavailable_not_a_refusal():
    f, cases = load()

    def handler(request):
        raise httpx.ConnectError("down")

    keys = KeySet("http://iam/jwks", httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    with pytest.raises(errs.Error) as e:
        await fixture_verifier(keys, f).verify(cases[0]["token"])
    assert errs.is_(e.value, "be", "DEPENDENCY_UNAVAILABLE")


def own_issuer():
    import base64

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    key = ed25519.Ed25519PrivateKey.generate()
    x = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    jwk = {"kty": "OKP", "crv": "Ed25519", "kid": "k1", "alg": "EdDSA", "x": base64.urlsafe_b64encode(x).rstrip(b"=").decode()}
    return key, jwk


def sign(key, claims, kid="k1"):
    import jwt

    return jwt.encode(claims, key, algorithm="EdDSA", headers={"typ": "at+jwt", "kid": kid})


CLAIMS = {"iss": "https://iam/", "aud": "be:deployment:d", "sub": "0190f3a1-0000-7000-8000-0000000000a1"}


async def test_keys_this_code_does_not_understand_are_skipped():  # P8.2, RFC 7517 section 5
    key, jwk = own_issuer()
    jwks = {"keys": [{"kty": "XYZ", "kid": "future-1"}, {"kty": "RSA", "kid": "broken"}, jwk]}
    keys = KeySet("http://iam/jwks", jwks_client(jwks, []))
    await keys.refresh()
    v = Verifier("https://iam/", "be:deployment:d", keys, now=lambda: 1000)
    assert (await v.verify(sign(key, {**CLAIMS, "exp": 2000}))).subject == CLAIMS["sub"]


async def test_a_timed_out_request_does_not_cancel_the_unknown_kid_refresh():  # P8.2
    import asyncio

    key, jwk = own_issuer()
    served, hits = [{"keys": []}], []

    async def handler(request):
        hits.append(1)
        await asyncio.sleep(0.3)
        return httpx.Response(200, json=served[0])

    keys = KeySet("http://iam/jwks", httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    await keys.refresh()
    served[0] = {"keys": [jwk]}  # the issuer rotated to a new key
    v = Verifier("https://iam/", "be:deployment:d", keys, now=lambda: 1000)
    token = sign(key, {**CLAIMS, "exp": 2000})
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.05):
            await v.verify(token)
    await asyncio.sleep(0.4)
    assert (await v.verify(token)).subject == CLAIMS["sub"], "the refresh finished although its request gave up"
    assert len(hits) == 2


async def test_a_time_claim_that_is_not_a_finite_number_is_refused():
    key, jwk = own_issuer()
    keys = KeySet("http://iam/jwks", jwks_client({"keys": [jwk]}, []))
    await keys.refresh()
    v = Verifier("https://iam/", "be:deployment:d", keys, now=lambda: 1000)
    with pytest.raises(TokenError) as e:
        await v.verify(sign(key, {**CLAIMS, "exp": float("nan")}))
    assert e.value.problem == "format"
