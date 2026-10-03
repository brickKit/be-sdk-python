"""The ACL projection (P6.11, P6.12): direct tuples pulled from GET {AUTHZ_URL}/authz/v2/changes into
besdk_authz_acl, the cursor advanced to the watermark, 410 rebuilt from /authz/v2/tuples, 501 a no-op,
a consistency token caught up synchronously."""
import io

import httpx
import pytest
import yaml

from besdk import Module
from besdk.auth.projection import Projection
from besdk.runtime import Runtime, Shared, Spec
from tests.integration.conftest import component_dir
from tests.integration.test_migrate import migrator

T = "conformance.widget.widget"
RES = {"resources": [{"type": T, "view_key": "conformance.widget.view", "relations": {}, "derivation": "direct",
                      "dimensions": ["owner"], "inherits": [{"from": "prj.project.project", "via": "project_id",
                                                             "relation": "member", "as": "viewer"}]}]}


class FakeAuthz:
    def __init__(self):
        self.log: list[dict] = []  # changes with revision
        self.floor = 0
        self.snapshot: dict[str, list[dict]] = {}
        self.snapshot_rev = "0"
        self.status = 200
        self.calls: list[str] = []

    def change(self, op, rid, relation="viewer", subject="user:u_me", expires_at=None, rtype=T):
        t = {"object": {"type": rtype, "id": rid}, "relation": relation, "subject": subject}
        if expires_at:
            t["expires_at"] = expires_at
        self.log.append({"revision": str(len(self.log) + 1), "op": op, "tuple": t})

    def handle(self, req: httpx.Request) -> httpx.Response:
        self.calls.append(req.url.path)
        assert req.headers.get("be-caller") == "conformance/widget-py"
        if self.status != 200:
            return httpx.Response(self.status, json={"reason": "CAPABILITY_UNAVAILABLE"})
        q = req.url.params
        if req.url.path == "/authz/v2/changes":
            types, after, limit = q["types"].split(","), int(q["after"]), int(q.get("limit", "500"))
            self.types_seen = sorted(types)
            if after < self.floor:
                return httpx.Response(410, json={"reason": "CHANGES_EXPIRED"})
            page = [c for c in self.log if int(c["revision"]) > after and c["tuple"]["object"]["type"] in types][:limit]
            nxt = page[-1]["revision"] if page else str(after)
            return httpx.Response(200, json={"changes": page, "next": nxt, "watermark": str(len(self.log))})
        if req.url.path == "/authz/v2/tuples":
            ts = self.snapshot.get(q["type"], [])
            return httpx.Response(200, json={"tuples": ts, "next_cursor": "", "revision": self.snapshot_rev})
        return httpx.Response(404)


async def _empty(rt):
    return Module()


@pytest.fixture
async def setup(tmp_path, ident):
    root = component_dir(tmp_path, props={"AUTHZ_URL": {"type": "string"}})
    (root / "assembly.yaml").write_text(yaml.safe_dump(RES))
    migrator(root, ident).up()
    fake = FakeAuthz()
    spec = Spec(id="conformance/widget-py", migrations=root / "migrations", contracts=root / "contracts", create=_empty)
    shared = Shared.standalone(spec_id=spec.id, http_transport=httpx.MockTransport(fake.handle))
    rt = Runtime(spec, ident.env(AUTHZ_URL="http://authz:8223", PG_POOL_MIN_IDLE="0"), shared,
                 log_stream=io.StringIO())
    yield rt, fake, Projection(rt, page=2)
    await rt.store().close()
    await shared.close()


def acl(ident):
    return sorted(tuple(r) for r in ident.sql(
        f'SELECT rid, relation, subject, expires_at IS NOT NULL FROM "{ident.schema}".besdk_authz_acl'))


async def test_pull_applies_changes_in_pages_and_advances(setup, ident):
    rt, fake, pr = setup
    fake.change("upsert", "w1")
    fake.change("upsert", "w2", subject="role:rep", expires_at="2030-01-01T00:00:00Z")
    fake.change("upsert", "w3")
    fake.change("delete", "w1")
    fake.change("upsert", "p1", relation="member", rtype="prj.project.project")
    await pr.pull()
    assert acl(ident) == [("p1", "member", "user:u_me", False), ("w2", "viewer", "role:rep", True),
                          ("w3", "viewer", "user:u_me", False)]
    assert await pr.watermark() == 5
    assert fake.types_seen == ["conformance.widget.widget", "prj.project.project"]  # own and inherited types
    fake.log.append({"revision": "6", "op": "upsert", "tuple": {"object": {"type": "other.x.y", "id": "z"},
                                                                 "relation": "r", "subject": "user:x"}})
    await pr.pull()
    assert await pr.watermark() == 6  # no change of our types still advances to the watermark


async def test_410_rebuilds_from_the_snapshot(setup, ident):
    rt, fake, pr = setup
    fake.change("upsert", "old")
    await pr.pull()
    fake.floor, fake.snapshot_rev = 5, "7"
    fake.log += [{"revision": str(i), "op": "upsert", "tuple": {"object": {"type": "x.y.z", "id": "n"},
                                                                "relation": "r", "subject": "user:x"}} for i in range(2, 8)]
    fake.snapshot = {T: [{"object": {"type": T, "id": "w9"}, "relation": "viewer", "subject": "dept:/1/"}]}
    await pr.pull()
    assert acl(ident) == [("w9", "viewer", "dept:/1/", False)]
    assert await pr.watermark() == 7 and "/authz/v2/tuples" in fake.calls


async def test_capability_absent_is_a_no_op(setup, ident):
    rt, fake, pr = setup
    fake.status = 501
    await pr.pull()
    assert acl(ident) == [] and await pr.watermark() == 0


async def test_catch_up_to_a_consistency_token(setup, ident):
    rt, fake, pr = setup
    fake.change("upsert", "w1")
    assert await pr.catch_up(1, budget=0.3) is True
    assert await pr.catch_up(9, budget=0.3) is False  # the provider never reaches 9
