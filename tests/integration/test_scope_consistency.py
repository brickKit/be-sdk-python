"""List/Can consistency (P6.7, CP-SCOPE-10) on PostgreSQL: under random bundles, claims, rows and shares, the
canonical predicate selects exactly the rows whose single-record decision (E10) is visible for K."""
import random
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from besdk.auth import evaluate as E
from besdk.auth.bundle import Bundle
from besdk.auth.scope import Columns, Scope
from tests.integration.conftest import admin_dsn

K = "conformance.widget.view"
RT = E.ResourceType.of({"type": "conformance.widget.widget", "view_key": K, "dimensions": ["owner", "org", "region"],
                        "derivation": "direct", "relations": {
                            "viewer": {"grants": [K]}, "member": {"grants": [K], "owned_by": "component"}}})
DEPTS = ["/1/", "/1/12/", "/1/12/7/", "/1/13/", "/2/", "/a_b/", "/a%b/", ""]
USERS = ["u_me", "u2", "u3"]
REGIONS = ["east", "west", "north"]
NOW = datetime(2026, 10, 3, tzinfo=timezone.utc)


def scenario(rnd: random.Random):
    roles = {}
    grants = {}
    for r in ("r1", "r2", "r3"):
        roles[r] = [K] if rnd.random() < 0.7 else []
        g = {}
        if rnd.random() < 0.7:
            g["levels"] = {K: rnd.choice(["own", "dept", "subtree", "all"])}
        if rnd.random() < 0.3:
            g["default_level"] = rnd.choice(["own", "dept", "subtree", "all"])
        vals = {"region": rnd.sample(REGIONS + ["*"], rnd.randint(0, 2))}
        if rnd.random() < 0.3:
            vals["org"] = rnd.sample(["/1/12/", "/2/", "*", "bad"], rnd.randint(1, 2))
        g["values"] = vals
        if rnd.random() < 0.1:
            g["until"] = 1
        grants[r] = g
    caps = {"core": True, "sharing": rnd.random() < 0.7, "relation_sync": rnd.random() < 0.7}
    bundle = {"contract": "authz/2.0", "revision": "1", "capabilities": caps, "roles": roles, "grants": grants}
    claims = {"sub": "u_me", "roles": rnd.sample(list(roles), rnd.randint(0, 3)), "dept_path": rnd.choice(DEPTS),
              "iat": 0}
    rows = [E.Row(f"w{i}", rnd.choice(USERS), rnd.choice(DEPTS[:-1]), {"region": rnd.choice(REGIONS)})
            for i in range(40)]
    acl = []
    for _ in range(rnd.randint(0, 6)):
        exp = rnd.choice([None, NOW + timedelta(days=1), NOW - timedelta(days=1)])
        acl.append(E.AclRow(RT.type, rnd.choice(rows).id, rnd.choice(["viewer", "member", "other"]),
                            rnd.choice(["user:u_me", "user:u2", "role:r1", "dept:/1/12/", "dept_tree:/1/"]), exp))
    return bundle, claims, rows, acl


@pytest.fixture
async def conn(pg16):
    c = await asyncpg.connect(admin_dsn(pg16))
    await c.execute("CREATE TEMP TABLE widgets (id text PRIMARY KEY, owner_id text, dept_path text, region text)")
    await c.execute("CREATE TEMP TABLE besdk_authz_acl (rtype text, rid text, relation text, subject text, "
                    "expires_at timestamptz, revision bigint, PRIMARY KEY (rtype, rid, relation, subject))")
    yield c
    await c.close()


@pytest.mark.parametrize("seed", range(60))
async def test_list_equals_can(conn, seed):
    rnd = random.Random(seed)
    bundle, claims, rows, acl = scenario(rnd)
    await conn.execute("TRUNCATE widgets, besdk_authz_acl")
    await conn.executemany("INSERT INTO widgets VALUES ($1, $2, $3, $4)",
                           [(r.id, r.owner, r.dept_path, r.values["region"]) for r in rows])
    await conn.executemany("INSERT INTO besdk_authz_acl VALUES ($1, $2, $3, $4, $5, 1)",
                           [(a.rtype, a.rid, a.relation, a.subject, a.expires_at) for a in acl])
    p = Bundle.accept(bundle).principal(claims, now=NOW.timestamp())
    sql, args = Scope.of(p, K, RT, Columns(dims={"region": "region"})).predicate("w")
    await conn.execute("SET TimeZone = 'UTC'")
    listed = {r["id"] for r in await conn.fetch(
        f"SELECT id FROM widgets w WHERE {sql}".replace("now()", f"'{NOW.isoformat()}'::timestamptz"), *args)}
    can = {r.id for r in rows if E.vis(p, K, RT, r, acl=acl)}
    assert listed == can
