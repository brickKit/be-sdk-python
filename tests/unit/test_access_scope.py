"""Access over declared resources (P6.3–P6.9): scopes per route key, dimension parameters, field masks,
sorting and writing masked fields, decisions that answer 404 for an invisible record."""
import httpx
import pytest
import yaml

import besdk
from besdk import Module, PermKey
from besdk.auth.evaluate import Row
from tests.unit._rt import build, manifest

VIEW = PermKey("conformance.widget.view")
ASSEMBLY = {
    "data_scopes": [{"dimension": "owner", "column": "owner_id", "mode": "equals", "tables": ["widgets"]},
                    {"dimension": "org", "column": "dept_path", "mode": "prefix", "tables": ["widgets"]},
                    {"dimension": "region", "column": "region", "mode": "in", "tables": ["widgets"]}],
    "resources": [{"type": "conformance.widget.widget", "table": "widgets", "view_key": "conformance.widget.view",
                   "keys": ["conformance.widget.view", "conformance.widget.approve"],
                   "dimensions": ["owner", "org", "region"], "derivation": "direct",
                   "relations": {"viewer": {"grants": ["conformance.widget.view"]}},
                   "fields": [{"set": "conformance.widget.price", "columns": ["price", "amount"],
                               "read": "conformance.widget.price.read", "edit": "conformance.widget.price.edit"}]}]}
BUNDLE = {"contract": "authz/2.0", "revision": "7", "capabilities": {"core": True},
          "roles": {"rep": ["conformance.widget.view", "conformance.widget.approve"],
                    "pricer": ["conformance.widget.view", "conformance.widget.price.read"]},
          "grants": {"rep": {"levels": {"conformance.widget.view": "dept"}, "values": {"region": ["east"]}},
                     "pricer": {"values": {"region": ["*"]}}}, "stale_since": {}}
T = "conformance.widget.widget"
SEEN = {}


def routes(r: besdk.Router):
    @r.get("/widgets", guard=VIEW)
    async def list_widgets(region: str | None = None, sort: str | None = None):
        a = besdk.access()
        if region:
            a.check_dimension(T, "region", region)
        if sort:
            a.check_sortable(T, sort)
        sql, args = a.scope(T).predicate("w")
        SEEN["sql"], SEEN["args"] = sql, args
        return {"items": [a.mask(T, {"id": "w1", "price": "9.90", "amount": "19.80", "name": "A"})]}

    @r.get("/widgets/{wid}", guard=VIEW)
    async def get_widget(wid: str):
        d = await besdk.access().can(VIEW, T, Row(wid, owner="u_other", dept_path="/9/", values={"region": "west"}))
        if not d.allowed:
            raise d.err()
        return {"id": wid}

    @r.patch("/widgets/{wid}", guard=VIEW)
    async def patch_widget(wid: str, body: dict):
        besdk.access().check_writable(T, list(body))
        return {"ok": True}


@pytest.fixture
async def client(tmp_path):
    async def create(rt):
        return Module(http=routes)

    (tmp_path / "assembly.yaml").write_text(yaml.safe_dump(ASSEMBLY))
    rt, fakes, _ = build(tmp_path, create)
    fakes.bundle = BUNDLE
    app = rt.http_app(await rt.spec.create(rt))
    await rt.shared.bundle_source.fetch()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w") as c:
        yield c, fakes


B = "/conformance/widget-py"


def h(fakes, *roles):
    return {"Authorization": "Bearer " + fakes.iam.token(roles=list(roles))}


async def test_scope_parameters_follow_the_route_key(client):
    c, f = client
    assert (await c.get(f"{B}/widgets", headers=h(f, "rep"))).status_code == 200
    assert "w.owner_id = ANY" in SEEN["sql"]
    assert SEEN["args"][:7] == [True, False, ["u_me"], ["/1/12/"], [], False, ["east"]]


async def test_dimension_parameter_outside_scope_is_403(client):
    c, f = client
    assert (await c.get(f"{B}/widgets?region=east", headers=h(f, "rep"))).status_code == 200
    r = await c.get(f"{B}/widgets?region=west", headers=h(f, "rep"))
    assert (r.status_code, r.json()["reason"]) == (403, "OUT_OF_SCOPE")
    assert (await c.get(f"{B}/widgets?region=west", headers=h(f, "pricer"))).status_code == 200


async def test_masked_fields_are_null_and_listed(client):
    c, f = client
    item = (await c.get(f"{B}/widgets", headers=h(f, "rep"))).json()["items"][0]
    assert item["price"] is None and item["amount"] is None and item["_masked"] == ["amount", "price"]
    item = (await c.get(f"{B}/widgets", headers=h(f, "pricer"))).json()["items"][0]
    assert item["price"] == "9.90" and item["_masked"] == []


async def test_sort_and_write_on_masked_or_read_only_fields(client):
    c, f = client
    r = await c.get(f"{B}/widgets?sort=price", headers=h(f, "rep"))
    assert (r.status_code, r.json()["reason"]) == (400, "SORT_FORBIDDEN")
    assert (await c.get(f"{B}/widgets?sort=price", headers=h(f, "pricer"))).status_code == 200
    r = await c.patch(f"{B}/widgets/w1", json={"price": "1"}, headers=h(f, "pricer"))
    assert (r.status_code, r.json()["reason"]) == (403, "FIELD_FORBIDDEN")
    assert (await c.patch(f"{B}/widgets/w1", json={"name": "x"}, headers=h(f, "pricer"))).status_code == 200


async def test_invisible_record_is_404(client):
    c, f = client
    r = await c.get(f"{B}/widgets/w1", headers=h(f, "rep"))
    assert (r.status_code, r.json()["reason"]) == (404, "NOT_FOUND")
