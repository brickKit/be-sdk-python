"""The resource contract (P6.10, P6.11, CP-SCOPE-12…15): _authz/check, _authz/explain, _shares over a
real projection; shares written through the provider's WriteTuples (gRPC, AUTHZ_GRPC_URL) and answered only
after the projection caught up; 501 without the sharing capability; a consistency token."""
import io
import json
import uuid

import grpc
import httpx
import pytest
import yaml

import besdk
from besdk import Module, PermKey
from besdk.auth import provider
from besdk.auth.evaluate import Row
from besdk.runtime import Runtime, Shared, Spec
from tests.integration.conftest import component_dir
from tests.integration.test_migrate import migrator
from tests.integration.test_projection import FakeAuthz
from tests.unit._tokens import ISSUER, TENANT, FakeIAM

T = "conformance.widget.widget"
VIEW, APPROVE, SHARE = "conformance.widget.view", "conformance.widget.approve", "conformance.widget.share"
ASSEMBLY = {"data_scopes": [{"dimension": "owner", "column": "owner_id", "tables": ["widgets"]},
                            {"dimension": "org", "column": "dept_path", "tables": ["widgets"]}],
            "resources": [{"type": T, "table": "widgets", "view_key": VIEW, "keys": [VIEW, APPROVE],
                           "dimensions": ["owner", "org"], "derivation": "direct",
                           "relations": {"viewer": {"grants": [VIEW]},
                                         "editor": {"includes": ["viewer"], "grants": [APPROVE]}},
                           "share": {"key": SHARE, "relations": ["viewer", "editor"], "subjects": ["user", "role"]}}]}
PROPS = {k: {"type": "string"} for k in ("AUTHZ_URL", "AUTHZ_GRPC_URL", "IAM_URL", "IAM_ISSUER", "TENANT_ID")}


class FakeProvider:
    """WriteTuples of infra.authz.v2.AuthzProvider: appends to the fake changefeed, returns the revision."""

    def __init__(self, feed: FakeAuthz):
        self.feed, self.requests = feed, []

    async def write(self, req, ctx):
        self.requests.append((req, dict(ctx.invocation_metadata())))
        for op, ts in (("upsert", req.writes), ("delete", req.deletes)):
            for t in ts:
                self.feed.change(op, t.object.id, relation=t.relation, subject=t.subject, rtype=t.object.type)
        return provider.message("WriteTuplesResponse")(revision=str(len(self.feed.log)))


@pytest.fixture
async def env(tmp_path, ident):
    root = component_dir(tmp_path, props=PROPS)
    (root / "assembly.yaml").write_text(yaml.safe_dump(ASSEMBLY))
    migrator(root, ident, AUTHZ_URL="http://authz:1", AUTHZ_GRPC_URL="http://x:1", IAM_URL="http://iam:1",
             IAM_ISSUER=ISSUER, TENANT_ID=TENANT).up()
    ident.sql(f'CREATE TABLE "{ident.schema}".widgets (id text PRIMARY KEY, owner_id text, dept_path text)')
    ident.sql(f'GRANT SELECT ON "{ident.schema}".widgets TO "{ident.user}"')
    ident.sql(f"INSERT INTO \"{ident.schema}\".widgets VALUES ('mine', 'u_me', '/1/'), ('theirs', 'u2', '/2/')")
    feed, iam = FakeAuthz(), FakeIAM()
    feed.status = 200
    bundle = {"contract": "authz/2.0", "revision": "1", "capabilities": {"core": True, "sharing": True},
              "roles": {"rep": [VIEW, APPROVE], "sharer": [VIEW, SHARE]}, "grants": {}, "stale_since": {}}

    def handle(req):
        if req.url.host == "iam":
            return iam.transport().handle_request(req)
        if req.url.path == "/authz/v2/bundle":
            return httpx.Response(200, json=bundle)
        return feed.handle(req)

    fake = FakeProvider(feed)
    server = grpc.aio.server()
    server.add_generic_rpc_handlers([grpc.method_handlers_generic_handler("infra.authz.v2.AuthzProvider", {
        "WriteTuples": grpc.unary_unary_rpc_method_handler(
            fake.write, request_deserializer=provider.message("WriteTuplesRequest").FromString,
            response_serializer=lambda m: m.SerializeToString())})])
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()

    async def load(tx, wid):
        r = await tx.fetchrow("SELECT id, owner_id, dept_path FROM widgets WHERE id = $1", wid)
        return Row(r["id"], r["owner_id"], r["dept_path"]) if r else None

    async def create(rt):
        return Module(sharing=[besdk.SharingLoader(T, load)])

    spec = Spec(id="conformance/widget-py", migrations=root / "migrations", contracts=root / "contracts", create=create)
    shared = Shared.standalone(spec_id=spec.id, http_transport=httpx.MockTransport(handle))
    rt = Runtime(spec, ident.env(AUTHZ_URL="http://authz:8223", AUTHZ_GRPC_URL=f"http://127.0.0.1:{port}",
                                 IAM_URL="http://iam:8200", IAM_ISSUER=ISSUER, TENANT_ID=TENANT), shared,
                 log_stream=io.StringIO())
    app = rt.http_app(await spec.create(rt))
    await shared.bundle_source.fetch()
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://w")
    yield rt, client, iam, bundle, feed, fake
    await client.aclose()
    await server.stop(None)
    await rt.outbound().close()
    await rt.store().close()
    await shared.close()


B = "/conformance/widget-py"


def h(iam, roles, sub="u_me"):
    return {"Authorization": "Bearer " + iam.token(roles=roles, sub=sub, dept_path="/1/")}


async def test_check_and_its_limit(env):
    rt, c, iam, *_ = env
    checks = [{"key": VIEW, "type": T, "id": "mine"}, {"key": VIEW, "type": T, "id": "theirs"},
              {"key": VIEW, "type": T, "id": "nope"}, {"key": SHARE, "type": T, "id": "mine"},
              {"key": VIEW, "type": "other.x.y", "id": "mine"}]
    r = await c.post(f"{B}/_authz/check", json={"checks": checks}, headers=h(iam, ["rep"]))
    assert r.status_code == 200
    assert r.json()["results"] == [
        {"visible": True, "allowed": True, "reason": ""}, {"visible": False, "allowed": False, "reason": "NOT_FOUND"},
        {"visible": False, "allowed": False, "reason": "NOT_FOUND"},
        {"visible": True, "allowed": False, "reason": "MISSING_PERMISSION"},
        {"visible": False, "allowed": False, "reason": "NOT_FOUND"}]
    r = await c.post(f"{B}/_authz/check", json={"checks": checks * 101}, headers=h(iam, ["rep"]))
    assert (r.status_code, r.json()["reason"]) == (400, "BATCH_TOO_LARGE")
    assert (await c.post(f"{B}/_authz/check", json={"checks": []})).status_code == 401


async def test_explain_visible_and_invisible(env):
    rt, c, iam, *_ = env
    r = await c.get(f"{B}/_authz/explain", params={"key": APPROVE, "type": T, "id": "mine"}, headers=h(iam, ["rep"]))
    assert r.status_code == 200 and r.json()["decision"] == "allowed"
    assert {"kind": "role_key", "source": "rep", "detail": APPROVE} in r.json()["reasons"]
    r = await c.get(f"{B}/_authz/explain", params={"key": VIEW, "type": T, "id": "theirs"}, headers=h(iam, ["rep"]))
    assert r.status_code == 404
    r2 = await c.get(f"{B}/_authz/explain", params={"key": VIEW, "type": T, "id": "nope"}, headers=h(iam, ["rep"]))
    assert r2.status_code == 404 and r2.json()["reason"] == r.json()["reason"] == "NOT_FOUND"


async def test_share_lifecycle(env):
    rt, c, iam, bundle, feed, fake = env
    path = f"{B}/_shares/{T}/mine"
    body = {"subject": "user:u2", "relation": "viewer"}
    r = await c.post(path, json=body, headers=h(iam, ["rep"]))
    assert (r.status_code, r.json()["reason"]) == (403, "MISSING_PERMISSION")
    r = await c.post(f"{B}/_shares/{T}/theirs", json=body, headers=h(iam, ["sharer"]))
    assert r.status_code == 404
    r = await c.post(path, json={"subject": "dept:/1/", "relation": "viewer"}, headers=h(iam, ["sharer"]))
    assert (r.status_code, r.json()["reason"]) == (403, "SHARE_NOT_ALLOWED")
    key = str(uuid.uuid4())
    r = await c.post(path, json=body, headers={**h(iam, ["sharer"]), "Idempotency-Key": key})
    assert r.status_code == 200, rt.logger.handlers[0].stream.getvalue()[-2500:]
    share, rev = r.json()["share"], r.json()["revision"]
    assert share["subject"] == "user:u2" and share["relation"] == "viewer" and rev == "1"
    req, md = fake.requests[0]
    assert req.idempotency_key == key and req.source == "conformance/widget-py" and req.actor.sub == "u_me"
    assert md["be-caller"] == "conformance/widget-py"
    # the projection already holds it: the shared user sees the record without waiting for the job
    r = await c.post(f"{B}/_authz/check", json={"checks": [{"key": VIEW, "type": T, "id": "mine"}]},
                     headers=h(iam, ["rep"], sub="u2"))
    assert r.json()["results"][0]["visible"] is True
    lst = (await c.get(path, headers=h(iam, ["sharer"]))).json()["shares"]
    assert [(s["subject"], s["relation"]) for s in lst] == [("user:u2", "viewer")]
    r = await c.delete(f"{path}/{share['share_id']}", headers=h(iam, ["sharer"]))
    assert r.status_code == 200 and r.json()["revision"] == "2"
    assert (await c.get(path, headers=h(iam, ["sharer"]))).json()["shares"] == []


async def test_sharing_capability_absent_is_501(env):
    rt, c, iam, bundle, *_ = env
    bundle["capabilities"] = {"core": True}
    await rt.shared.bundle_source.fetch()
    for m, path in (("get", f"{B}/_shares/{T}/mine"), ("post", f"{B}/_shares/{T}/mine")):
        kw = {"json": {"subject": "user:u2", "relation": "viewer"}} if m == "post" else {}
        r = await getattr(c, m)(path, headers=h(iam, ["sharer"]), **kw)
        assert (r.status_code, r.json()["reason"], r.json()["metadata"]) == (
            501, "CAPABILITY_UNAVAILABLE", {"capability": "sharing"})


async def test_consistency_token(env):
    rt, c, iam, bundle, feed, _ = env
    feed.change("upsert", "theirs", subject="user:u_me")
    r = await c.post(f"{B}/_authz/check", json={"checks": [{"key": VIEW, "type": T, "id": "theirs"}]},
                     headers={**h(iam, ["rep"]), "X-Authz-Revision": "1"})
    assert r.json()["results"][0]["visible"] is True and "x-authz-consistency" not in r.headers
    r = await c.post(f"{B}/_authz/check", json={"checks": [{"key": VIEW, "type": T, "id": "theirs"}]},
                     headers={**h(iam, ["rep"]), "X-Authz-Revision": "99"})
    assert r.headers["x-authz-consistency"] == "stale", rt.logger.handlers[0].stream.getvalue()[-2500:]
