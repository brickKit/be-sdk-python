"""Access-token verification (P5): allow-listed alg bound to the JWKS key, required claims, typ, skew,
JWKS cache with one refetch for an unknown kid, fail-static."""
import base64
import json
import time

import httpx
import pytest

from besdk.auth.jwt import JwksCache, Verifier
from besdk.errors import Error
from tests.unit._tokens import ISSUER, TENANT, FakeIAM


@pytest.fixture
def iam():
    return FakeIAM()


@pytest.fixture
def verifier(iam):
    client = httpx.AsyncClient(transport=iam.transport(), base_url="http://iam:8200")
    return Verifier(issuer=ISSUER, tenant=TENANT, jwks=JwksCache("http://iam:8200", client))


async def _reason(verifier, token):
    with pytest.raises(Error) as ei:
        await verifier.verify(token)
    return ei.value.reason


async def test_valid_rs256_and_es256(verifier, iam):
    c = await verifier.verify(iam.token())
    assert c["sub"] == "u_me" and c["roles"] == ["rep"]
    assert (await verifier.verify(iam.token("k-ec")))["sub"] == "u_me"


@pytest.mark.parametrize("claims", [
    {"typ": "refresh"}, {"typ": None}, {"iss": "urn:be:other:iam"}, {"aud": "t2"}, {"aud": ["t2", "t3"]},
    {"sub": ""}, {"jti": None}, {"exp": None}, {"iat": None}, {"roles": "rep"}, {"dept_path": 7},
    {"act": {"kind": "robot", "sub": "x"}}, {"exp": int(time.time()) - 120}, {"nbf": int(time.time()) + 120},
])
async def test_claim_failures_are_token_invalid(verifier, iam, claims):
    assert await _reason(verifier, iam.token(**claims)) == "TOKEN_INVALID"


async def test_audience_array_containing_tenant(verifier, iam):
    assert (await verifier.verify(iam.token(aud=["x", TENANT])))["sub"] == "u_me"


async def test_skew_tolerance_60s(verifier, iam):
    assert (await verifier.verify(iam.token(exp=int(time.time()) - 30)))["sub"] == "u_me"


def _forge(header: dict, payload: dict, sig: bytes = b"") -> str:
    enc = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
    return ".".join([enc(json.dumps(header).encode()), enc(json.dumps(payload).encode()), enc(sig)])


async def test_alg_none_hmac_missing_kid_and_mismatched_alg(verifier, iam):
    now = int(time.time())
    p = {"iss": ISSUER, "aud": TENANT, "sub": "u", "typ": "access", "iat": now, "exp": now + 60, "jti": "j"}
    assert await _reason(verifier, _forge({"alg": "none", "kid": "k-rsa"}, p)) == "TOKEN_INVALID"
    assert await _reason(verifier, _forge({"alg": "HS256", "kid": "k-rsa"}, p, b"x")) == "TOKEN_INVALID"
    assert await _reason(verifier, _forge({"alg": "RS256"}, p, b"x")) == "TOKEN_INVALID"
    # an RS256 token whose header claims the EC key's kid: alg must equal the selected key's alg
    t = iam.token("k-rsa", headers={"kid": "k-ec"})
    assert await _reason(verifier, t) == "TOKEN_INVALID"
    assert await _reason(verifier, "not-a-jwt") == "TOKEN_INVALID"


async def test_unknown_kid_refetches_at_most_once_per_30s(verifier, iam):
    await verifier.verify(iam.token())
    assert iam.fetches == 1
    _, key = iam.keys["k-rsa"]
    iam.keys["k-new"] = ("RS256", key)
    assert (await verifier.verify(iam.token("k-new")))["sub"] == "u_me"  # rotation: one refetch
    assert iam.fetches == 2
    iam.keys["k-newer"] = ("RS256", key)
    assert await _reason(verifier, iam.token("k-newer")) == "TOKEN_INVALID"  # within 30 s: no refetch
    assert iam.fetches == 2


async def test_fail_static_when_jwks_unreachable(iam):
    state = {"up": True}

    def handle(req):
        if not state["up"]:
            raise httpx.ConnectError("down")
        return httpx.Response(200, json=iam.jwks())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="http://iam:8200")
    cache = JwksCache("http://iam:8200", client, max_age=0)
    v = Verifier(issuer=ISSUER, tenant=TENANT, jwks=cache)
    await v.verify(iam.token())
    state["up"] = False
    assert (await v.verify(iam.token()))["sub"] == "u_me"
