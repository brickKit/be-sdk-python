"""The simulation's members sim/a and sim/b: each takes orders (a business row, a work item of one of three effect
classes, an event and a payment, an external command, in one transaction), consumes the other's events, runs a
singleton in steps and a cron job. Every effect leaves a row whose count the simulation checks: an effect twice, or
never, is a failure. The payments' external system is the table pay_taken, written outside the SDK's transactions; it
answers queries and does not deduplicate."""
from __future__ import annotations

import os
import uuid

from fastapi.responses import JSONResponse

from besdk import errs
from besdk.component import Definition
from besdk.events.consumer import Sub
from besdk.events.core import Event, Projection, Stream
from besdk.extcmd.commands import Answer, Result
from besdk.extcmd.state import SUCCEEDED, Capabilities
from besdk.http import Route
from besdk.testing import defs
from besdk.work import CRON, IDEMPOTENT_EXTERNAL, LOCAL, NON_IDEMPOTENT_EXTERNAL, QUEUE, SINGLETON, Job

IDS = ("sim/a", "sim/b")
STREAMS = {"sim/a": Stream("sim.a.orders", 2), "sim/b": Stream("sim.b.orders", 2)}
ORDER_TYPE = "sim.order.placed.v1"
KINDS = {"local": ("sim.local", LOCAL), "idem": ("sim.idem", IDEMPOTENT_EXTERNAL),
         "nonidem": ("sim.nonidem", NON_IDEMPOTENT_EXTERNAL)}
MIGRATIONS = {"0001_sim.up.sql": """
CREATE TABLE IF NOT EXISTS orders (id uuid PRIMARY KEY, kind text NOT NULL, at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS effects (item text NOT NULL, order_id uuid NOT NULL, operation_id text NOT NULL);
CREATE TABLE IF NOT EXISTS far_calls (key text NOT NULL, order_id uuid NOT NULL, idempotent boolean NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS far_calls_once ON far_calls (key) WHERE idempotent;
CREATE TABLE IF NOT EXISTS seen (event_id uuid NOT NULL, order_id uuid NOT NULL);
CREATE TABLE IF NOT EXISTS steps_task (id int PRIMARY KEY CHECK (id = 1), n int NOT NULL);
CREATE TABLE IF NOT EXISTS step_effects (operation_id text NOT NULL, step int NOT NULL);
CREATE TABLE IF NOT EXISTS cron_runs (slot text NOT NULL);
-- the external system the payments go to (outside the SDK's transactions): one row per payment it took
CREATE TABLE IF NOT EXISTS pay_taken (reference text NOT NULL, at timestamptz NOT NULL DEFAULT now());
"""}


def prefix(cid: str) -> str:
    return cid.replace("/", ".")


def peer(cid: str) -> str:
    return IDS[1] if cid == IDS[0] else IDS[0]


class Sim:
    def __init__(self, rt, cid: str):
        self.rt, self.cid = rt, cid

    def routes(self, r) -> None:
        async def order(c):
            body = await c.json()
            kind = body.get("kind", "local") if isinstance(body, dict) else ""
            if kind not in KINDS:
                raise errs.new("REQUEST_INVALID", "kind local, idem or nonidem")
            oid = uuid.uuid7()

            async def fn(tx) -> None:
                await tx.execute("INSERT INTO orders (id, kind) VALUES ($1, $2)", oid, kind)
                await self.rt.work.enqueue(tx, KINDS[kind][0], {"order": str(oid)})
                await self.rt.events.emit(tx, STREAMS[self.cid].name,
                                          Event(ORDER_TYPE, str(oid), 1, Projection.FULL, {"id": str(oid)}))
                await self.rt.external.request(tx, self.cid, str(oid), "pay", {"order": str(oid)})

            await self.rt.db.tx(fn)
            return JSONResponse({"id": str(oid)}, status_code=201)

        async def steps(c):
            n = int((await c.json()).get("n", 5))
            await self.rt.db.tx(lambda tx: tx.execute(
                "INSERT INTO steps_task (id, n) VALUES (1, $1) ON CONFLICT (id) DO NOTHING", n))
            return JSONResponse({"n": n}, status_code=202)

        r.add(Route("POST", "/api/order", order, public=True))
        r.add(Route("POST", "/api/steps", steps, public=True))

    def external_commands(self):
        return Ledger(self.rt), {"pay": Capabilities(query=True)}

    def subscriptions(self, s) -> None:
        async def handle(tx, e) -> None:
            await tx.execute("INSERT INTO seen (event_id, order_id) VALUES ($1, $2)", uuid.UUID(e.id),
                             uuid.UUID(e.decode()["id"]))

        s.add(Sub("peer-orders", [STREAMS[peer(self.cid)].name], handle, types=[ORDER_TYPE]))

    def jobs(self, j) -> None:
        def effect(run):
            async def fn(tx) -> None:
                await tx.execute("INSERT INTO effects (item, order_id, operation_id) VALUES ($1, $2, $3)", run.id,
                                 uuid.UUID(run.args["order"]), run.operation_id)
            return fn

        async def local(run) -> None:
            if run.attempts == 1 and uuid.UUID(run.args["order"]).int % 3 == 0:
                raise RuntimeError("a first attempt that fails (the retry path)")
            await run.complete(effect(run))

        async def idem(run) -> None:
            await self.rt.db.background_tx(lambda tx: tx.execute(
                "INSERT INTO far_calls (key, order_id, idempotent) VALUES ($1, $2, true) ON CONFLICT DO NOTHING",
                run.sub_key("call"), uuid.UUID(run.args["order"])))
            await run.complete(effect(run))

        async def nonidem(run) -> None:
            await run.before_call()
            await self.rt.db.background_tx(lambda tx: tx.execute(
                "INSERT INTO far_calls (key, order_id, idempotent) VALUES ($1, $2, false)", run.operation_id,
                uuid.UUID(run.args["order"])))
            await run.complete(effect(run))

        async def steps(run) -> None:
            n = await self.rt.db.background_tx(lambda tx: tx.fetchval("SELECT n FROM steps_task WHERE id = 1"))
            if n is None:
                await run.complete()
                return
            done = run.progress if isinstance(run.progress, int) else 0
            for s in range(done + 1, n + 1):
                async def fx(tx, s=s, op=run.operation_id) -> None:
                    await tx.execute("INSERT INTO step_effects (operation_id, step) VALUES ($1, $2)", op, s)
                if s == n:
                    async def last(tx, fx=fx) -> None:
                        await fx(tx)
                        await tx.execute("DELETE FROM steps_task WHERE id = 1")
                    await run.complete(last)
                else:
                    await run.checkpoint(s, fx)

        async def minutely(run) -> None:
            await run.complete(lambda tx: tx.execute("INSERT INTO cron_runs (slot) VALUES ($1)", run.slot))

        for kind, (name, effect_class) in KINDS.items():
            j.add(Job(name, QUEUE, {"local": local, "idem": idem, "nonidem": nonidem}[kind], effect_class))
        j.add(Job("sim.steps", SINGLETON, steps, LOCAL, interval=1.0))
        j.add(Job("sim.minutely", CRON, minutely, LOCAL, cron="* * * * *", time_zone="UTC"))


class Ledger:
    """The payments' external system: takes every payment it is sent (no deduplication), answers by reference."""

    def __init__(self, rt):
        self.rt = rt

    async def send(self, c):
        await self.rt.db.background_tx(lambda tx: tx.execute("INSERT INTO pay_taken (reference) VALUES ($1)",
                                                             c.reference))
        return Result(SUCCEEDED, response="taken")

    async def query(self, c):
        n = await self.rt.db.background_tx(lambda tx: tx.fetchval(
            "SELECT count(*) FROM pay_taken WHERE reference = $1", c.reference))
        return Answer(n > 0, SUCCEEDED if n else "")


def definition(cid: str) -> Definition:
    p = prefix(cid)
    return Definition(cid, "3.0.0", lambda rt: Sim(rt, cid), migrations=MIGRATIONS, streams=[STREAMS[cid]],
                      subscribes=True, resend=p + ".resend", grpc_port=int(os.environ.get("SIM_GRPC_" + cid.replace("/", "_").upper(), "0")),
                      authz=defs(cid, p, p + ".read", p + ".resend", p + ".recover"), external_commands=True,
                      recovery=p + ".recover")
