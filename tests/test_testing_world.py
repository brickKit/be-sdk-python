"""The test deployment's identity side (foundations item 2, T6): tokens at the account's security version, security
changes into the view, a rotatable key set that counts its fetches."""
import json
import urllib.request

import jwt

from besdk import canon
from besdk.testing import World

ALICE, BOB = "0190f3a1-0000-7000-8000-0000000000a1", "0190f3a1-0000-7000-8000-0000000000b2"


def claims(tok: str) -> dict:
    return jwt.decode(tok, options={"verify_signature": False})


def test_tokens_carry_the_accounts_security_version(tmp_path):
    w = World(tmp_path)
    assert claims(w.token(ALICE))["be_sv"] == 1
    w.security_change(ALICE)
    assert claims(w.token(ALICE))["be_sv"] == 2
    assert claims(w.token(ALICE, sv=1))["be_sv"] == 1, "a token from before the change"
    assert claims(w.token(BOB))["be_sv"] == 1


def test_only_changed_accounts_carry_the_version_in_the_view(tmp_path):
    w = World(tmp_path)
    w.grant(BOB, {"resource_type": "demo.x", "operation": "demo.x.read", "atoms": {}, "read_fields": [],
                  "valid": {"from": "2026-01-01T00:00:00Z"}})
    before = w.gen
    w.security_change(ALICE)
    assert w.gen == before + 1, "the next view is a new one"
    raw, _ = w.view()
    subjects = canon.parse(raw)["subjects"]
    assert subjects[ALICE]["security_version"] == 2
    assert "security_version" not in subjects[BOB], "a component whose schema predates the field is not sent it"


async def test_the_key_set_rotates_and_counts_its_fetches(tmp_path):
    w = await World(tmp_path).start()
    try:
        first = w.kid
        get = lambda: json.loads(urllib.request.urlopen(w.jwks_url, timeout=5).read())  # noqa: E731
        assert [k["kid"] for k in get()["keys"]] == [first]
        new = w.rotate()
        assert new != first and claims(w.token(ALICE)) and jwt.get_unverified_header(w.token(ALICE))["kid"] == new
        assert sorted(k["kid"] for k in get()["keys"]) == sorted([first, new]), "the old key stays published"
        assert w.jwks_fetches == 2
        assert jwt.get_unverified_header(w.token(ALICE, kid="ghost"))["kid"] == "ghost"
    finally:
        await w.stop()
