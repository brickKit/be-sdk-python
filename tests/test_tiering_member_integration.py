"""A component with data classes in the launcher (P15.1, P15.3, P15.13, P15.29, P15.30, P15.40), the same cases as
be-sdk-go's launcher/tiering_integration_test.go: the migrate entry installs the write barrier, again on every run; a
residence below N refuses the start; a component with a class it can destroy starts (its hold and approval operations
declared, the destructions' replay registered), runs the SDK's tiering jobs and serves the holds and approvals."""
import asyncio
import io
import uuid

import httpx
import pytest

from besdk import launcher
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.testing import ALICE, BOB, defs
from besdk.tiering import member
from besdk.tiering.classes import COMPLETED, POSTGRES, Child, Class, Retention, Table
from besdk.tiering.rows import TIMESTAMPTZ, UUID
from tests import pgtest
from tests.test_events_interop import free_port, member_env
from tests.test_recovery_phases_integration import finish, start

pytestmark = pytest.mark.integration
NOTES = {"0001_notes.up.sql": "CREATE TABLE notes (id uuid PRIMARY KEY, closed_at timestamptz, created_at timestamptz "
                              "NOT NULL); CREATE TABLE note_lines (id uuid PRIMARY KEY, note_id uuid NOT NULL "
                              "REFERENCES notes (id));"}


class NoRoutes:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        pass


def tier_class() -> Class:
    return Class("demo.notes", COMPLETED, Table("notes", {"id": UUID, "closed_at": TIMESTAMPTZ,
                                                          "created_at": TIMESTAMPTZ}, pk="id"),
                 children=(Child(Table("note_lines", {"id": UUID, "note_id": UUID}, pk="id"), "note_id"),),
                 anchor="closed_at", created="created_at", terminal="r.closed_at IS NOT NULL", residence_days=365,
                 retention=Retention(365, 3650, "test"), routes=(POSTGRES,), schema_version="1",
                 hold_operation="demo.tierl.hold", approve_operation="demo.tierl.approve")


def definition(cid: str, *ops: str, **kw) -> Definition:
    return Definition(cid, "3.0.0", NoRoutes, migrations=NOTES, grpc_port=free_port(), recovery="demo.tierl.recover",
                      authz=defs(cid, "demo.tierl", "demo.tierl.read", "demo.tierl.recover", "demo.tierl.hold",
                                 "demo.tierl.approve", *ops),
                      tiering=[tier_class()], **kw)


async def test_the_migrate_entry_installs_the_barrier(world):  # P15.13, P15.3
    cid = "demo/tierl"
    d = definition(cid)
    env = {**await member_env("py_tierl", {}), **world.env(cid)}
    out = io.StringIO()
    for _ in range(2):  # idempotent
        assert await launcher.run(d, launcher.Options(args=["migrate"], env=EnvSource(lookup_env=env.get), stdout=out,
                                                      stderr=out), asyncio.Event()) == 0, out.getvalue()
    a = await pgtest.admin()
    try:
        triggers = [r[0] for r in await a.fetch(
            "SELECT c.relname || ':' || t.tgname FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN "
            "pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'py_tierl' AND t.tgname LIKE 'be_tier_%' "
            "ORDER BY 1")]
    finally:
        await a.close()
    assert triggers == ["note_lines:be_tier_barrier_demo_notes", "notes:be_tier_barrier_demo_notes"]

    short = {**env, member.residence_key("demo.notes"): "30", "COMPONENT_ID": cid, "PORT": str(free_port())}
    out = io.StringIO()
    code = await launcher.run(d, launcher.Options(env=EnvSource(lookup_env=short.get), stdout=out, stderr=out,
                                                  host="127.0.0.1"), asyncio.Event())
    assert code != 0
    assert "TIERING_RESIDENCE_DEMO_NOTES" in out.getvalue(), out.getvalue()


async def test_a_component_with_classes_serves(world):  # P15.1, P15.29, P15.30, P15.40
    cid = "demo/tiers"
    d = definition(cid, "demo.tierl.replay", replay="demo.tierl.replay")
    world.grant_all(ALICE, d.authz)
    env = {**await member_env("py_tiers", {}), **world.env(cid)}
    a = await pgtest.admin()
    try:
        url, stop, task, out = await start(d, env)
        try:
            jobs: list[str] = []
            for _ in range(200):
                jobs = [r[0] for r in await a.fetch("SELECT job FROM py_tiers.be_work_item WHERE job LIKE "
                                                    "'be.tiering.%' ORDER BY job")]
                if len(jobs) >= 5:
                    break
                await asyncio.sleep(0.05)
            assert jobs == ["be.tiering.cleanup", "be.tiering.destroy", "be.tiering.export.cleanup",
                            "be.tiering.release", "be.tiering.transfer"], \
                "the singletons (the candidates are a cron job, the export a queue)\n" + out.getvalue()

            async with httpx.AsyncClient(base_url=url) as c:
                async def send(path, who, body=None):
                    return await c.post(path, json=body, headers={"X-Requested-With": "test",
                                                                  "Authorization": world.bearer(who)})

                hold = {"scope_kind": "class", "scope": "demo.notes", "source": "legal", "reason": "a court order"}
                r = await send("/be/tiering/demo.notes/holds", BOB, hold)
                assert r.status_code in (403, 404), f"bob holds no hold operation: {r.status_code} {r.text}"
                r = await send("/be/tiering/demo.notes/holds", ALICE, hold)
                assert r.status_code == 201, r.text
                hid = r.json()["id"]
                assert await a.fetchval("SELECT count(*) FROM py_tiers.be_tier_hold WHERE id = $1 AND authorized_by = "
                                        "$2 AND released_at IS NULL", uuid.UUID(hid), ALICE) == 1
                r = await send(f"/be/tiering/demo.notes/holds/{hid}/lift", ALICE)
                assert r.status_code == 204, r.text
                r = await send("/be/tiering/demo.notes/approvals", ALICE,
                               {"scope_kind": "class", "scope": "demo.notes", "reason": "the retention has run"})
                assert r.status_code == 201, r.text
                r = await send(f"/be/tiering/demo.notes/approvals/{r.json()['id']}/withdraw", ALICE)
                assert r.status_code == 204, r.text
        finally:
            await finish(stop, task, out)
    finally:
        await a.close()
