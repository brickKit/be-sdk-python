"""contract-infra-authz decision vectors, the core part the runtime decides per route (E1–E6, P6.2):
bundle acceptance, token checks, has(K) and level(K). Scope parameters and single-record decisions
(E6–E12) belong to the Access task (P6) and are not checked here."""
import json

import pytest

from besdk.auth import bundle as B
from tests.unit.vectors._load import ROOT

FILES = sorted((ROOT / "authz-vectors" / "decision").glob("*.json"))


@pytest.mark.parametrize("path", FILES, ids=[p.stem for p in FILES])
def test_core(path):
    v = json.loads(path.read_text())
    i, want = v["input"], v["expected"]
    try:
        b = B.Bundle.accept(i["bundle"])
    except B.BundleRefused:
        assert want["bundle"] == "refused"
        return
    assert want["bundle"] == "accepted"
    token = b.check_token(i["claims"])
    assert (token or "OK") == want["token"]
    if token:
        return
    p = b.principal(i["claims"], now=i["now"])
    assert p.has(i["key"]) == want["has_key"]
    assert p.level(i["key"]) == want["level"]
