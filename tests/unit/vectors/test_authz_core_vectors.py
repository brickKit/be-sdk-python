"""contract-infra-authz decision vectors (EVALUATION.md E1–E12, be-protocol P6): bundle acceptance, token
checks, has(K), level(K), the canonical scope parameters, degradation, field masks, the single-record
decision and the explain facts — every member of `expected`, for all vectors."""
import json

import pytest

from besdk.auth import bundle as B
from besdk.auth import evaluate as E
from tests.unit.vectors._load import ROOT

FILES = sorted((ROOT / "authz-vectors" / "decision").glob("*.json"))


def test_all_62_vectors_are_present():
    assert len(FILES) == 62


@pytest.mark.parametrize("path", FILES, ids=[p.stem for p in FILES])
def test_decision_vector(path):
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
    key, rt = i["key"], E.ResourceType.of(i["resource_type"])
    graph = i.get("graph_ids")
    assert p.has(key) == want["has_key"]
    assert p.level(key) == want["level"]
    sp = E.scope_params(p, key, rt, graph_ids=graph)
    assert sp.as_dict() == want["scope_params"]
    assert E.degraded(p, rt) == want["degraded"]
    if "fields" in want:
        assert E.fields(p, rt) == want["fields"]
    if "decision" in want:
        row = E.Row.of(i["row"])
        acl = [E.AclRow.of(a) for a in i.get("acl") or ()]
        d = E.decide(p, key, rt, row, acl=acl, graph_ids=graph)
        assert {"visible": d.visible, "allowed": d.allowed, "reason": d.reason} == want["decision"]
        assert E.explain(p, key, rt, row, acl=acl, graph_ids=graph) == want["explain"]
