"""The recovery phases (P1.22-P1.24; be-sdk-go's TestRecoveryPhases is the same walk): a member started with
RECOVERY_PHASE is recovery_ready (its readiness answers 200 with the phase, its self-description and ready line say the
phase); frozen runs no forwarding and no subscription and answers MAINTENANCE on the user surface; redelivering,
replaying and verifying run forwarding and subscriptions and still answer MAINTENANCE; read_only serves reads and the
writes a route declares open; no phase runs a job (the SDK's own included); without the phase everything runs again.
A phase it does not know is a configuration error."""
import asyncio
import io
import uuid

import httpx
import pytest
from fastapi.responses import JSONResponse

from besdk import launcher
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.events.consumer import Sub
from besdk.events.core import Event, Projection, Stream
from besdk.http import Route
from besdk.work import EVERY, Job
from tests import pgtest
from tests.test_events_interop import bus, free_port, member_env

pytestmark = pytest.mark.integration
RC = Stream("demo.rc", 2)
PHASES = ("frozen", "redelivering", "replaying", "verifying", "read_only")


class Producer:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def create(c):
            rid = str(uuid.uuid7())
            await self.rt.db.tx(lambda tx: self.rt.events.emit(tx, RC.name, Event(
                "demo.rc.created.v1", rid, 1, Projection.FULL, {"id": rid})))
            return JSONResponse({"id": rid}, status_code=201)

        async def ticks(c):
            return JSONResponse({})

        async def code(c):
            return JSONResponse({})

        r.add(Route("POST", "/api/r", create, public=True))
        r.add(Route("GET", "/api/ticks", ticks, public=True))
        r.add(Route("POST", "/api/code", code, public=True, read_only_open=True))

    def jobs(self, j):
        async def tick(run):
            await self.rt.db.background_tx(lambda tx: tx.execute("INSERT INTO ticks DEFAULT VALUES"))

        j.add(Job("demo.tick", EVERY, tick, interval=0.1))


class Consumer:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        pass

    def subscriptions(self, s):
        async def handle(tx, e):
            await tx.execute("INSERT INTO rseen VALUES ($1)", uuid.UUID(e.id))

        s.add(Sub("rc", [RC.name], handle, types=["demo.rc.created.v1"]))


async def start(d, env, phase=""):
    """Migrates, then runs the member in the phase; returns its URL, the stop event, the task and its output."""
    out = io.StringIO()
    if phase:
        env = {**env, "RECOVERY_PHASE": phase}
    assert await launcher.run(d, launcher.Options(args=["migrate"], env=EnvSource(lookup_env=env.get), stdout=out,
                                                  stderr=out), asyncio.Event()) == 0, out.getvalue()
    port, stop = free_port(), asyncio.Event()
    env = {**env, "COMPONENT_ID": d.id, "PORT": str(port)}
    task = asyncio.create_task(launcher.run(d, launcher.Options(env=EnvSource(lookup_env=env.get), stdout=out,
                                                                stderr=out, host="127.0.0.1"), stop))
    url = f"http://127.0.0.1:{port}"
    async with httpx.AsyncClient() as c:
        for _ in range(1000):
            try:
                if (await c.get(url + "/readyz")).status_code == 200:
                    return url, stop, task, out
            except httpx.ConnectError:
                pass
            assert not task.done(), out.getvalue()
            await asyncio.sleep(0.02)
    raise AssertionError("never ready\n" + out.getvalue())


async def finish(stop, task, out):
    stop.set()
    assert await task == 0, out.getvalue()


async def eventually(f, want, seconds, what):
    for _ in range(int(seconds / 0.05)):
        if await f() == want:
            return
        await asyncio.sleep(0.05)
    assert await f() == want, what


async def test_the_recovery_phases():
    sfx = uuid.uuid4().hex[:6]
    async with bus("pgqueue") as url:
        prod_env = await member_env("py_rc_prod", {"BUS_URL": url, "IMPLEMENTATION_INSTANCE": str(uuid.uuid4())})
        cons_env = await member_env("py_rc_cons", {"BUS_URL": url})
        prod = Definition("demo/rcprod" + sfx, "3.0.0", Producer, streams=[RC],
                          migrations={"0001_m.up.sql": "CREATE TABLE IF NOT EXISTS marker (id int);",
                                      "0002_ticks.up.sql": "CREATE TABLE IF NOT EXISTS ticks "
                                                           "(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY);"})
        cons = Definition("demo/rccons" + sfx, "3.0.0", Consumer, subscribes=True,
                          migrations={"0001_rseen.up.sql": "CREATE TABLE IF NOT EXISTS rseen (event_id uuid PRIMARY KEY);"})
        a = await pgtest.admin()

        async def count(q):
            return await a.fetchval(q)

        async def seen():
            return await count("SELECT count(*) FROM py_rc_cons.rseen")

        async def ticks():
            return await count("SELECT count(*) FROM py_rc_prod.ticks")

        async def unforwarded():
            return await count("SELECT count(*) FROM py_rc_prod.be_outbox WHERE seq IS NULL")

        async def copy_outbox_row():
            await a.execute("""INSERT INTO py_rc_prod.be_outbox (event_id, stream, partition, type, occurred_at,
                record_id, version, projection, hops, payload, digest) SELECT gen_random_uuid(), stream, partition,
                type, occurred_at, gen_random_uuid()::text, version, projection, hops, payload, digest
                FROM py_rc_prod.be_outbox LIMIT 1""")

        try:
            await a.execute("TRUNCATE py_rc_cons.rseen") if await count(
                "SELECT to_regclass('py_rc_cons.rseen') IS NOT NULL") else None
            # running: two events go through
            purl, pstop, ptask, pout = await start(prod, prod_env)
            curl, cstop, ctask, cout = await start(cons, cons_env)
            async with httpx.AsyncClient() as c:
                for _ in range(2):
                    assert (await c.post(purl + "/api/r")).status_code == 201
            await eventually(seen, 2, 10, "two events received while running")
            for _ in range(200):
                if await ticks() > 0:
                    break
                await asyncio.sleep(0.05)
            assert await ticks() > 0, "the job ticks while running"
            # one more on the bus while the consumer is away: a frozen consumer must leave it there
            await finish(cstop, ctask, cout)
            async with httpx.AsyncClient() as c:
                assert (await c.post(purl + "/api/r")).status_code == 201
            await eventually(unforwarded, 0, 10, "the third event is on the bus")
            await finish(pstop, ptask, pout)

            # frozen: recovery_ready with the phase; MAINTENANCE on the user surface; nothing forwarded or received
            await copy_outbox_row()
            before = await ticks()
            purl, pstop, ptask, pout = await start(prod, prod_env, "frozen")
            curl, cstop, ctask, cout = await start(cons, cons_env, "frozen")
            async with httpx.AsyncClient() as c:
                r = await c.get(purl + "/readyz")
                assert r.status_code == 200 and r.json() == {"state": "recovery_ready", "phase": "frozen"}
                assert (await c.get(purl + "/.well-known/be-component")).json()["recovery_phase"] == "frozen"
                r = await c.post(purl + "/api/r")
                assert r.status_code == 503, r.text
                assert r.json()["reason"] == "MAINTENANCE" and r.json()["metadata"]["phase"] == "frozen", r.text
                assert (await c.get(purl + "/api/ticks")).status_code == 503, "no reads while frozen"
            assert '"phase": "frozen"' in pout.getvalue() or '"phase":"frozen"' in pout.getvalue(), "the ready line"
            # past the forwarding leases the run before held (10 s, P6.11) and the forwarder's poll many times over
            await asyncio.sleep(11)
            assert await unforwarded() == 1, "not forwarded while frozen"
            assert await seen() == 2, "not received while frozen"
            assert await ticks() == before, "no job while frozen"
            await finish(cstop, ctask, cout)
            await finish(pstop, ptask, pout)

            # redelivering, replaying, verifying: forwarding and subscriptions run; the user surface does not
            want = 4  # the event left on the bus and the row written while frozen
            for phase in ("redelivering", "replaying", "verifying"):
                if phase != "redelivering":
                    await copy_outbox_row()
                purl, pstop, ptask, pout = await start(prod, prod_env, phase)
                curl, cstop, ctask, cout = await start(cons, cons_env, phase)
                async with httpx.AsyncClient() as c:
                    assert (await c.get(curl + "/readyz")).json()["phase"] == phase
                    # the run before held the partitions' forwarding leases (P6.11): this run may wait them out
                    await eventually(seen, want, 25, f"forwarded and received while {phase}")
                    r = await c.post(purl + "/api/r")
                    assert r.status_code == 503 and r.json()["metadata"]["phase"] == phase, r.text
                    assert (await c.get(purl + "/api/ticks")).status_code == 503, f"no reads while {phase}"
                    assert (await c.post(purl + "/api/code")).status_code == 503
                    assert await ticks() == before, f"no job while {phase}"
                want += 1
                await finish(cstop, ctask, cout)
                await finish(pstop, ptask, pout)

            # read_only: reads and the writes a route declares open; other writes MAINTENANCE
            purl, pstop, ptask, pout = await start(prod, prod_env, "read_only")
            async with httpx.AsyncClient() as c:
                assert (await c.get(purl + "/api/ticks")).status_code == 200, "a read"
                assert (await c.post(purl + "/api/code")).status_code == 200, "a write declared read_only_open"
                r = await c.post(purl + "/api/r")
                assert r.status_code == 503 and r.json()["metadata"]["phase"] == "read_only", r.text
            await asyncio.sleep(0.5)
            assert await ticks() == before, "no job while open read-only"
            await finish(pstop, ptask, pout)

            # running again: everything, and no recovery_phase in the self-description
            purl, pstop, ptask, pout = await start(prod, prod_env)
            async with httpx.AsyncClient() as c:
                r = await c.get(purl + "/readyz")
                assert r.status_code == 200 and r.text == "ok"
                assert "recovery_phase" not in (await c.get(purl + "/.well-known/be-component")).json()
                assert (await c.post(purl + "/api/r")).status_code == 201
            for _ in range(200):
                if await ticks() > before:
                    break
                await asyncio.sleep(0.05)
            assert await ticks() > before, "the job runs again"
            await finish(pstop, ptask, pout)

            # a phase this version does not know: a configuration error before any port opens (P1.9, P2.6)
            out = io.StringIO()
            bad = {**prod_env, "RECOVERY_PHASE": "thawing", "COMPONENT_ID": prod.id, "PORT": str(free_port())}
            assert await launcher.run(prod, launcher.Options(env=EnvSource(lookup_env=bad.get), stdout=out, stderr=out),
                                      asyncio.Event()) == 78, out.getvalue()
        finally:
            await a.close()


def test_every_phase_is_a_value_of_the_key():
    from besdk import host
    key = next(k for k in host.sdk_keys() if k.name == "RECOVERY_PHASE")
    assert tuple(key.values) == PHASES and key.process
