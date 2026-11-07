"""The long simulation (foundations item 6, T6; the fifth item's retro S1: it runs as soon as the first end-to-end path
works, and each later task adds its crash points). `make simulation` with the environment of test-integration;
SIM_MINUTES (default 30) is how long the crashes go on.

Two members, sim/a and sim/b (tests/sim/member.py), each taking orders that write a business row, a work item of one
of three effect classes and an event in one transaction, each consuming the other's events, running a singleton in
steps and a cron job. Three replicas on a real PostgreSQL and the PostgreSQL queue: a shell hosting both members, and
each member alone. Each replica runs with a crash point taken in turn from besdk.crash's points and ends itself
there; the parent starts it again with the next one, and kills one at random now and then. Meanwhile the parent
places orders and gives the singletons tasks. Then the crashes stop, everything runs until nothing is left to do, and
the final state is checked: every order's job took effect exactly once (a non-idempotent call at most once, and an
item whose call may have been sent paused, never sent again); every event was handled exactly once by the other
member, or (its last delivery met a crash) dead-lettered with the cause "no outcome" and not handled (P6.20); every
payment was taken by the external system once and succeeded, or (a crash between its claim and its send) never and is
manual; the outbox's positions have no gap and no duplicate; no step of a singleton segment and no cron slot ran
twice; no item is dead or stuck; every crash point was reached."""
import asyncio
import io
import json
import os
import pathlib
import random
import signal
import subprocess
import sys
import time
import uuid

import httpx
import pytest

from besdk import launcher
from besdk.config import EnvSource
from besdk.events.consumer import NO_OUTCOME
from tests import pgtest
from tests.sim.member import IDS, KINDS, definition
from tests.test_events_interop import bus, free_port

pytestmark = pytest.mark.simulation
MINUTES = float(os.environ.get("SIM_MINUTES", "30"))
ROOT = pathlib.Path(__file__).resolve().parents[2]
SHELL_ROLE, SHELL_PW = "sdk_sim_shell", "sim-shell-pw"
SCHEMA = {"sim/a": "sim_a", "sim/b": "sim_b"}
POINTS = [f"{p}/{w}" for p in ("work.claim", "work.complete", "work.before_call", "work.checkpoint", "work.retry",
                               "events.number", "events.published", "events.inbox") for w in ("commit", "done")]
POINTS.append("events.publish/done")
POINTS += [f"extcmd.{p}" for p in ("claim/commit", "claim/done", "send/done", "result/commit", "result/done",
                                   "query/done")]
KILL_EVERY, ORDER_EVERY, DRAIN_LIMIT = 20.0, 0.15, 300.0


def say(msg: str) -> None:
    print(f"SIM {time.strftime('%H:%M:%S')} {msg}", flush=True)


class Replica:
    """One replica process; its crash point rotates through POINTS on every start."""

    def __init__(self, name: str, mode: str, env: dict, urls: dict, logs: pathlib.Path, offset: int):
        self.name, self.mode, self.env, self.urls, self.logs = name, mode, env, urls, logs
        self.turn, self.proc, self.starts, self.reached = offset, None, 0, set()
        self.log = None

    def start(self, crashing: bool) -> None:
        point = POINTS[self.turn % len(POINTS)] if crashing else ""
        self.turn += 1
        self.starts += 1
        self.log = self.logs / f"{self.name}-{self.starts:04d}.log"
        self.proc = subprocess.Popen([sys.executable, "-m", "tests.sim.child", self.mode], cwd=ROOT,
                                     env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", **self.env, "SIM_CRASH": point},
                                     stdout=self.log.open("w"), stderr=subprocess.STDOUT)

    def ended(self) -> bool:
        if self.proc is None or self.proc.poll() is None:
            return False
        for line in self.log.read_text().splitlines():
            if line.startswith("SIM CRASH "):
                self.reached.add(line.removeprefix("SIM CRASH ").strip())
        return True

    def kill(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def stop(self) -> int:
        if self.proc is None or self.proc.poll() is not None:
            return self.proc.returncode if self.proc else 0
        self.proc.send_signal(signal.SIGTERM)
        try:
            return self.proc.wait(30)
        except subprocess.TimeoutExpired:
            self.kill()
            return -9


async def provision() -> dict[str, dict]:
    a = await pgtest.admin()
    try:
        await pgtest.role(a, SHELL_ROLE, SHELL_PW)
        for schema in SCHEMA.values():
            await a.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    finally:
        await a.close()
    envs = {}
    for cid, schema in SCHEMA.items():
        await pgtest.member(schema, shell_role=SHELL_ROLE)
        e = {**pgtest.member_env(schema), "COMPONENT_VERSION": "3.0.0", "STOP_DRAIN_DELAY": "50ms"}
        out = io.StringIO()
        assert await launcher.run(definition(cid), launcher.Options(args=["migrate"], env=EnvSource(lookup_env=e.get),
                                                                    stdout=out, stderr=out), asyncio.Event()) == 0, out.getvalue()
        envs[cid] = {k: v for k, v in e.items() if not k.startswith("PG_OWNER")}
    return envs


# an order the peer neither saw nor dead-lettered: its event has no outcome yet. An event whose last delivery met a
# crash is dead-lettered with the cause "no outcome" (P6.20): an outcome, re-driven by an operator in a real system
UNSEEN = """SELECT count(*) FROM {peer}.orders o WHERE NOT EXISTS (SELECT 1 FROM {s}.seen x WHERE x.order_id = o.id)
    AND NOT EXISTS (SELECT 1 FROM {s}.be_dead_letter d WHERE d.headers->>'ce-subject' = o.id::text)"""


async def pending(c) -> str:
    """What is left to do; empty when the members have drained."""
    left = []
    for cid, s in SCHEMA.items():
        busy = await c.fetchval(f"""SELECT count(*) FROM {s}.be_work_item WHERE kind = 'queue'
            AND state IN ('pending', 'running', 'retry_wait')""")
        unpublished = await c.fetchval(f"SELECT count(*) FROM {s}.be_outbox WHERE published_at IS NULL")
        peer = SCHEMA[IDS[1] if cid == IDS[0] else IDS[0]]
        unseen = await c.fetchval(UNSEEN.format(peer=peer, s=s))
        steps = await c.fetchval(f"SELECT count(*) FROM {s}.steps_task")
        paying = await c.fetchval(f"""SELECT count(*) FROM {s}.be_external_command
            WHERE state IN ('prepared', 'retry_wait', 'sending', 'unknown', 'accepted')""")
        if busy or unpublished or unseen or steps or paying:
            left.append(f"{cid}: {busy} queue items, {unpublished} unpublished, {unseen} unseen, {steps} step tasks, "
                        f"{paying} payments")
    return "; ".join(left)


async def check(c) -> list[str]:
    problems = []
    for cid, s in SCHEMA.items():
        peer = SCHEMA[IDS[1] if cid == IDS[0] else IDS[0]]
        rows = await c.fetch(f"""SELECT o.id, o.kind, w.state, w.pause_reason,
                (SELECT count(*) FROM {s}.effects e WHERE e.order_id = o.id) AS effects,
                (SELECT count(*) FROM {s}.far_calls f WHERE f.order_id = o.id) AS calls
            FROM {s}.orders o LEFT JOIN {s}.be_work_item w ON w.args->>'order' = o.id::text""")
        for r in rows:
            where = f"{cid} order {r['id']} ({r['kind']}, {r['state']})"
            if r["state"] is None:
                problems.append(f"{where}: no work item")
            elif r["kind"] == "nonidem":
                if r["calls"] > 1:
                    problems.append(f"{where}: the non-idempotent call was made {r['calls']} times")
                if r["state"] == "done" and (r["effects"], r["calls"]) != (1, 1):
                    problems.append(f"{where}: done with {r['effects']} effects and {r['calls']} calls")
                if r["state"] == "paused" and (r["pause_reason"] != "outcome_unknown" or r["effects"]):
                    problems.append(f"{where}: paused ({r['pause_reason']}) with {r['effects']} effects")
                if r["state"] not in ("done", "paused"):
                    problems.append(f"{where}: ended {r['state']}")
            else:
                if r["state"] != "done" or r["effects"] != 1:
                    problems.append(f"{where}: {r['effects']} effects")
                if r["kind"] == "idem" and r["calls"] != 1:
                    problems.append(f"{where}: {r['calls']} idempotent calls taken")
        seen = await c.fetch(f"SELECT order_id, count(*) AS n FROM {s}.seen GROUP BY order_id HAVING count(*) > 1")
        problems += [f"{cid} saw order {r['order_id']} {r['n']} times" for r in seen]
        missing = await c.fetchval(UNSEEN.format(peer=peer, s=s))
        if missing:
            problems.append(f"{cid}: {missing} of the other member's orders neither seen nor dead-lettered")
        dead = await c.fetch(f"""SELECT d.event_id, d.error, EXISTS (SELECT 1 FROM {s}.seen x
                WHERE x.order_id::text = d.headers->>'ce-subject') AS seen FROM {s}.be_dead_letter d""")
        for r in dead:
            if r["seen"]:
                problems.append(f"{cid}: event {r['event_id']} both handled and dead-lettered")
            elif r["error"] != NO_OUTCOME:
                problems.append(f"{cid}: event {r['event_id']} dead-lettered: {r['error']}")
        if dead:
            say(f"{cid}: {len(dead)} events dead-lettered after their last delivery met a crash")
        gaps = await c.fetch(f"""SELECT stream, partition, epoch_generation, epoch_timeline, count(*) AS n,
                count(DISTINCT seq) AS d, min(seq) AS lo, max(seq) AS hi
            FROM {s}.be_outbox WHERE seq IS NOT NULL GROUP BY 1, 2, 3, 4""")
        for g in gaps:
            if g["n"] != g["d"] or g["hi"] - g["lo"] + 1 != g["n"]:
                problems.append(f"{cid} outbox {g['stream']}/{g['partition']}: positions {g['lo']}..{g['hi']} for "
                                f"{g['n']} rows ({g['d']} distinct)")
        twice = await c.fetchval(f"""SELECT count(*) FROM (SELECT operation_id, step FROM {s}.step_effects
            GROUP BY 1, 2 HAVING count(*) > 1) x""")
        if twice:
            problems.append(f"{cid}: {twice} singleton steps ran twice in one segment")
        steps = await c.fetchval(f"SELECT count(*) FROM {s}.step_effects")
        if not steps:
            problems.append(f"{cid}: the singleton never ran a step")
        slots = await c.fetchval(f"SELECT count(*) FROM (SELECT slot FROM {s}.cron_runs GROUP BY 1 HAVING count(*) > 1) x")
        if slots:
            problems.append(f"{cid}: {slots} cron slots ran twice")
        # the payments (chapter 13): never taken twice; succeeded when taken once; manual only when never taken (a
        # crash between the claim and the send: it may still land as far as the SDK knows, P13.6)
        pays = await c.fetch(f"""SELECT o.id, x.state, (SELECT count(*) FROM {s}.pay_taken t
                WHERE t.reference = o.id::text) AS taken
            FROM {s}.orders o LEFT JOIN {s}.be_external_command x ON x.reference = o.id::text""")
        manual = 0
        for r in pays:
            where = f"{cid} payment of order {r['id']} ({r['state']}, taken {r['taken']} times)"
            if r["state"] == "manual":
                manual += 1
            if r["taken"] > 1 or (r["state"] == "succeeded") != (r["taken"] == 1) or r["state"] not in (
                    "succeeded", "manual") or (r["state"] == "manual" and r["taken"]):
                problems.append(where)
        if manual:
            say(f"{cid}: {manual} payments manual (a crash between their claim and their send)")
        bad = await c.fetch(f"""SELECT job, state, count(*) AS n FROM {s}.be_work_item
            WHERE state IN ('dead', 'running') OR (state = 'paused' AND job <> 'sim.nonidem') GROUP BY 1, 2""")
        problems += [f"{cid}: {r['n']} items of {r['job']} {r['state']}" for r in bad]
    return problems


async def test_the_long_simulation(world, tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    say(f"logs in {logs}; {MINUTES} minutes of crashes; {len(POINTS)} crash points")
    async with bus("pgqueue") as url:
        envs = await provision()
        impl = {cid: str(uuid.uuid4()) for cid in IDS}

        def member_config(cid: str) -> dict:
            return {**envs[cid], **world.env(cid), "BUS_URL": url, "IMPLEMENTATION_INSTANCE": impl[cid]}

        replicas, urls = [], {}
        # the shell hosting both members
        ports = {cid: (free_port(), free_port()) for cid in IDS}
        served = [{"componentId": cid, "version": "3.0.0", "httpPort": ports[cid][0],
                   "extraPorts": [{"name": "grpc", "port": ports[cid][1]}], "config": member_config(cid)} for cid in IDS]
        shell_env = {"COMPONENT_ID": "be/py-sim", "COMPONENT_VERSION": "3.0.0", "PORT": str(free_port()),
                     "STOP_DRAIN_DELAY": "50ms", "BRICKKIT_SERVED_MEMBERS_CONFIG": json.dumps(served),
                     "PG_SHELL_USER": SHELL_ROLE, "PG_SHELL_PASSWORD": SHELL_PW}
        replicas.append(Replica("shell", "shell", shell_env, {cid: f"http://127.0.0.1:{ports[cid][0]}" for cid in IDS},
                                logs, 0))
        for k, cid in enumerate(IDS):
            http, grpc_ = free_port(), free_port()
            env = {**member_config(cid), "COMPONENT_ID": cid, "PORT": str(http),
                   "SIM_GRPC_" + cid.replace("/", "_").upper(): str(grpc_)}
            replicas.append(Replica(cid.split("/")[1], cid.split("/")[1], env, {cid: f"http://127.0.0.1:{http}"},
                                    logs, 6 * (k + 1)))
        for r in replicas:
            r.start(crashing=True)
        placed, failed, kills = 0, 0, 0
        end, next_kill, next_steps, next_say = time.monotonic() + MINUTES * 60, time.monotonic() + KILL_EVERY, 0.0, 0.0
        async with httpx.AsyncClient(timeout=5) as c:
            try:
                while time.monotonic() < end:
                    for r in replicas:
                        if r.ended():
                            r.start(crashing=True)
                    if time.monotonic() >= next_kill:
                        random.choice(replicas).kill()
                        kills += 1
                        next_kill = time.monotonic() + KILL_EVERY
                    cid = random.choice(IDS)
                    targets = [r.urls[cid] for r in replicas if cid in r.urls]
                    try:
                        got = await c.post(random.choice(targets) + "/api/order",
                                           json={"kind": random.choice(list(KINDS))})
                        placed += got.status_code == 201
                        failed += got.status_code != 201
                        if time.monotonic() >= next_steps:
                            await c.post(random.choice(targets) + "/api/steps", json={"n": 5})
                            next_steps = time.monotonic() + 3
                    except httpx.HTTPError:
                        failed += 1  # a replica down: the order may or may not have committed; the check reads the orders
                    if time.monotonic() >= next_say:
                        reached = set().union(*(r.reached for r in replicas))
                        say(f"progress: {placed} orders placed, {failed} refused or lost, {kills} kills, "
                            f"{sum(r.starts for r in replicas)} starts, {len(reached)}/{len(POINTS)} crash points "
                            f"reached, {int((end - time.monotonic()) / 60)} min left")
                        next_say = time.monotonic() + 30
                    await asyncio.sleep(ORDER_EVERY)
                say("the crashes stop; draining")
                for r in replicas:
                    r.ended()
                    r.kill()
                    r.start(crashing=False)
                a = await pgtest.admin()
                try:
                    limit = time.monotonic() + DRAIN_LIMIT
                    while left := await pending(a):
                        assert time.monotonic() < limit, f"not drained in time: {left}"
                        await asyncio.sleep(2)
                    say("drained; checking the final state")
                    for r in replicas:
                        r.stop()
                    problems = await check(a)
                finally:
                    await a.close()
            finally:
                for r in replicas:
                    r.kill()
        reached = set().union(*(r.reached for r in replicas))
        missing = [p for p in POINTS if p not in reached]
        say(f"final: {placed} orders placed, {kills} kills, {len(reached)}/{len(POINTS)} crash points reached, "
            f"{len(problems)} problems")
        for p in problems[:50]:
            say("PROBLEM " + p)
        assert not missing, f"crash points never reached: {missing}"
        assert not problems, f"{len(problems)} problems; logs in {logs}"
        say("ALL-CHECKS-PASSED")
