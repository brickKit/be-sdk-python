"""The route decision chain (P6.2, P1.5, P5.6): Public → token → (bundle) → token checks → Authenticated →
key. Errors carry the reasons the protocol names."""
import time

import httpx
import pytest

from besdk.auth.access import AUTHENTICATED, PUBLIC, Authorizer, PermKey
from besdk.auth.bundle import Bundle
from besdk.auth.jwt import JwksCache, Verifier
from besdk.errors import Error
from tests.unit._tokens import ISSUER, TENANT, FakeIAM

BUNDLE = {"contract": "authz/2.0", "revision": "7", "capabilities": {"core": True, "delegation": False},
          "roles": {"rep": ["erp.sales.view"]}, "grants": {}, "stale_since": {"u_stale": int(time.time())}}


class _Src:
    def __init__(self, doc):
        self.bundle = Bundle.accept(doc) if doc else None


@pytest.fixture
def iam():
    return FakeIAM()


def authorizer(iam, doc=BUNDLE):
    client = httpx.AsyncClient(transport=iam.transport())
    return Authorizer(Verifier(issuer=ISSUER, tenant=TENANT, jwks=JwksCache("http://iam", client)), _Src(doc))


async def _deny(a, guard, header):
    with pytest.raises(Error) as ei:
        await a.decide(guard, header)
    return ei.value


async def test_public_needs_nothing(iam):
    assert await authorizer(iam, None).decide(PUBLIC, None) is None


async def test_missing_or_bad_header_is_token_invalid(iam):
    a = authorizer(iam)
    for h in (None, "", "Basic abc", "Bearer", "Bearer not.a.jwt"):
        e = await _deny(a, PermKey("erp.sales.view"), h)
        assert (e.http, e.reason) == (401, "TOKEN_INVALID")


async def test_no_bundle_yet_is_503_for_protected_routes(iam):
    e = await _deny(authorizer(iam, None), PermKey("erp.sales.view"), "Bearer " + iam.token())
    assert (e.http, e.reason) == (503, "AUTHZ_NOT_READY")
    e = await _deny(authorizer(iam, None), AUTHENTICATED, "Bearer " + iam.token())
    assert e.reason == "AUTHZ_NOT_READY"


async def test_stale_token(iam):
    e = await _deny(authorizer(iam), AUTHENTICATED, "Bearer " + iam.token(sub="u_stale", iat=int(time.time()) - 60))
    assert (e.http, e.reason) == (401, "TOKEN_STALE")


async def test_delegation_without_capability(iam):
    e = await _deny(authorizer(iam), AUTHENTICATED, "Bearer " + iam.token(ceil=["ro"]))
    assert (e.http, e.reason) == (401, "UNSUPPORTED_DELEGATION")


async def test_authenticated_and_key(iam):
    a = authorizer(iam)
    acc = await a.decide(AUTHENTICATED, "Bearer " + iam.token())
    assert acc.user.sub == "u_me" and acc.user.has_dept and acc.user.dept_path == "/1/12/"
    acc = await a.decide(PermKey("erp.sales.view"), "Bearer " + iam.token())
    assert acc.has("erp.sales.view") and not acc.has("erp.sales.cancel") and acc.key == "erp.sales.view"
    e = await _deny(a, PermKey("erp.sales.cancel"), "Bearer " + iam.token())
    assert (e.http, e.reason, e.metadata) == (403, "MISSING_PERMISSION", {"permission": "erp.sales.cancel"})


async def test_malformed_department_is_no_department(iam):
    acc = await authorizer(iam).decide(AUTHENTICATED, "Bearer " + iam.token(dept_path="1/12"))
    assert not acc.user.has_dept
