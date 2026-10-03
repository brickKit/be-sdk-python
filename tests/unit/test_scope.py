"""The canonical list predicate (P6.5): a static parameterised fragment whose parameters come only from the
evaluated access; column names come from the resource declaration, never from the request."""
import pytest

from besdk.auth import evaluate as E
from besdk.auth.bundle import Bundle
from besdk.auth.scope import Columns, Scope

RT = E.ResourceType.of({"type": "conformance.widget.widget", "view_key": "conformance.widget.view",
                        "dimensions": ["owner", "org", "region"], "derivation": "direct",
                        "relations": {"viewer": {"grants": ["conformance.widget.view"]}}})
BUNDLE = {"contract": "authz/2.0", "revision": "1", "capabilities": {"core": True, "sharing": True},
          "roles": {"rep": ["conformance.widget.view"]},
          "grants": {"rep": {"levels": {"conformance.widget.view": "subtree"}, "values": {"region": ["east"]}}}}
CLAIMS = {"sub": "u1", "roles": ["rep"], "dept_path": "/1/12/", "iat": 0}


def scope(cols=None) -> Scope:
    p = Bundle.accept(BUNDLE).principal(CLAIMS, now=1)
    return Scope.of(p, "conformance.widget.view", RT, cols or Columns(dims={"region": "region"}))


def test_predicate_shape_and_parameters():
    sql, args = scope().predicate("w", start=3)
    assert sql.startswith("(") and "$3" in sql and "w.owner_id = ANY($5::text[])" in sql
    assert "w.dept_path LIKE ANY($7::text[])" in sql and "w.region::text = ANY($9::text[])" in sql
    assert "FROM besdk_authz_acl" in sql and "w.id::text = ANY(" in sql
    assert args[:7] == [True, False, ["u1"], [], ["/1/12/%"], False, ["east"]]
    assert args[7:10] == [True, "conformance.widget.widget", ["viewer"]]
    assert sql.count("$") == len(args)


def test_type_without_identity_dimensions():
    rt = E.ResourceType.of({"type": "erp.inventory.balance", "view_key": "conformance.widget.view",
                            "dimensions": ["warehouse"], "relations": {}})
    p = Bundle.accept(BUNDLE).principal(CLAIMS, now=1)
    sql, _ = Scope.of(p, "conformance.widget.view", rt, Columns(dims={"warehouse": "warehouse_id"})).predicate("b")
    assert "owner_id" not in sql and "dept_path" not in sql and "b.warehouse_id::text = ANY(" in sql


@pytest.mark.parametrize("bad", ["w; DROP", "1w", "w.x"])
def test_alias_and_columns_are_identifiers(bad):
    with pytest.raises(ValueError):
        scope().predicate(bad)
    with pytest.raises(ValueError):
        Columns(owner=bad)
