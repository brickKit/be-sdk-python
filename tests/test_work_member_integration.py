"""Background work in a running member (chapter 07): a module declares its jobs and enqueues through the runtime in
its business transaction; the SDK's own jobs are registered with every database (P4.25, P5.22, P6.12, P7.19); a module
with jobs and no database does not start."""
import asyncio
import io
import uuid

import httpx
import pytest
from fastapi.responses import JSONResponse

from besdk import launcher
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.http import Route
from besdk.testing import recovering
from besdk.work import LOCAL, QUEUE, Job
from tests import pgtest
from tests.test_events_interop import free_port, member_env
from tests.test_recovery_phases_integration import finish, start

pytestmark = pytest.mark.integration


class Worker:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def order(c):
            oid = str(uuid.uuid7())

            async def body(tx):
                await tx.execute("INSERT INTO orders VALUES ($1)", oid)
                await self.rt.work.enqueue(tx, "demo.confirm", {"order": oid})

            await self.rt.db.tx(body)
            return JSONResponse({"id": oid}, status_code=201)

        r.add(Route("POST", "/api/orders", order, public=True))

    def jobs(self, j):
        async def confirm(run):
            await run.complete(lambda tx: tx.execute("UPDATE orders SET confirmed = true WHERE id = $1",
                                                     run.args["order"]))

        j.add(Job("demo.confirm", QUEUE, confirm, LOCAL))


ORDERS = {"0001_orders.up.sql": "CREATE TABLE IF NOT EXISTS orders (id text PRIMARY KEY, confirmed bool NOT NULL "
                                "DEFAULT false);"}


async def test_a_member_runs_its_jobs_and_the_sdks(world):
    cid = "demo/wkm" + uuid.uuid4().hex[:6]
    env = {**await member_env("py_wkm", {}), **world.env(cid)}
    d = Definition(cid, "3.0.0", Worker, migrations=ORDERS, grpc_port=free_port(), **recovering(cid))
    a = await pgtest.admin()
    try:
        url, stop, task, out = await start(d, env)
        try:
            async with httpx.AsyncClient() as c:
                oid = (await c.post(url + "/api/orders")).json()["id"]
            for _ in range(200):
                if await a.fetchval("SELECT confirmed FROM py_wkm.orders WHERE id = $1", oid):
                    break
                await asyncio.sleep(0.02)
            assert await a.fetchval("SELECT confirmed FROM py_wkm.orders WHERE id = $1", oid), out.getvalue()
            singletons = {r["job"] for r in await a.fetch("SELECT job FROM py_wkm.be_work_item WHERE kind = 'singleton'")}
            assert singletons >= {"be.work.cleanup", "be.outbox.purge", "be.partitions.maintain",
                                  "be.idempotency.cleanup"}, singletons
        finally:
            await finish(stop, task, out)
    finally:
        await a.close()


async def test_a_module_with_jobs_needs_a_database():
    out = io.StringIO()
    d = Definition("demo/wknodb", "3.0.0", Worker)
    env = {"COMPONENT_ID": d.id, "COMPONENT_VERSION": "3.0.0", "PORT": str(free_port())}
    code = await launcher.run(d, launcher.Options(env=EnvSource(lookup_env=env.get), stdout=out, stderr=out),
                              asyncio.Event())
    assert code != 0 and "a module with jobs has a database" in out.getvalue(), out.getvalue()
