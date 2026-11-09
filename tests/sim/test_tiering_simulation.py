"""The tiering's long simulation (foundations item 6, T10; be-sdk-go's tiering/sim_test.go, the assembly protocol 11's
replacement checks), run with `uv run pytest -m simulation tests/sim/test_tiering_simulation.py -s` and the
environment of test-integration; SIM_MINUTES (default 30) is how long the crashes go on.

Two members on a real PostgreSQL and object store, member a on the object route and b on the warm one, their routes
swapped halfway (the route change and the old versions' release). Four replicas (tests/sim/tierchild.py): two host
both members (as a shell does), one each alone. Each replica runs with a crash point taken in turn from every state
transition of the SDK and ends itself there; the parent starts it again, and kills one at random now and then.
Meanwhile the parent writes new documents, changes open ones, closes some into the past, writes to closed ones
(refused once sealed), erases (as a request, and as the queued redo a recovery's replay leaves), holds old ones, records
backups and reads documents by their ID, each against what it last wrote. Then the crashes stop, the jobs run until
nothing is left to do, and the final state is checked: every document reads as written (or is destroyed, past its
retention and not held); every published copy verifies; the recovery check passes (no copy missing, no orphan, no
released unit with online rows); no work item is stuck; every crash point was reached."""
import asyncio
import datetime as dt
import io
import json
import os
import pathlib
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal

import pytest

from besdk import errs, obs
from besdk._gen.be.recovery.v1 import check_pb2
from besdk.recovery import PASS, Checks, CheckServer
from besdk.tiering import check, erase, manifest, member, reads, store, transfer
from besdk.tiering.barrier import statements
from besdk.work.runner import Runner
from tests import pgtest
from tests.sim.tierchild import (CLEANUP_EVERY, ORPHAN_AGE, SCHEMA_SQL, SIM_CLASS, aggregate, catalog_of, erased,
                                 read_online, sim_class)
from tests.test_db_integration import migrated
from tests.test_tiering_transfer_integration import ensure_bucket, s3

pytestmark = pytest.mark.simulation
MINUTES = float(os.environ.get("SIM_MINUTES", "30"))
ROOT = pathlib.Path(__file__).resolve().parents[2]
BUCKET = "be-tiering-sim"
# every background transaction that changes state, before and after its commit, and every object written or deleted
POINTS = ["seal/commit", "seal/done", "claim/commit", "claim/done", "copy.rows/commit", "copy.rows/done",
          "copy.object/done", "copy.manifest/done", "publish/commit", "publish/done", "release.decide/commit",
          "release.decide/done", "release.delete/commit", "release.delete/done", "destroy.decide/commit",
          "destroy.decide/done", "destroy.purge/commit", "destroy.purge/done", "destroy.object/done",
          "erase.find/done", "erase.sealed/commit", "erase.sealed/done", "rewrite.route/done",
          "releaseold.decide/done", "releaseold.delete/commit", "releaseold.delete/done", "releaseold.object/done",
          "orphans.object/done", "orphans.delete/commit", "orphans.delete/done"]
QUIET = obs.new_logger(obs.Member("test/sim", "3.0.0"), "error", io.StringIO())


def say(msg: str) -> None:
    print(f"SIM {time.strftime('%H:%M:%S')} {msg}", flush=True)


@dataclass
class Doc:
    agg: str = ""  # as last written (aggregate)
    open: bool = False
    old: bool = False  # closed past the retention: may be destroyed
    held: bool = False
    busy: bool = False  # a queued erasure not done yet: not compared
    destroyed: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class Member:
    name: str
    prefix: str
    env: dict  # the replicas' configuration (the route changes halfway)
    d: object = None
    cat: object = None  # the parent's own: reads and requested erasures
    queue: object = None  # never started: it only queues the erasures' redo
    pools: list = field(default_factory=list)
    docs: dict = field(default_factory=dict)
    ids: list = field(default_factory=list)

    def spec(self) -> dict:
        return {"name": self.name, "prefix": self.prefix, "env": dict(self.env)}

    def pick(self, want=None):
        for _ in range(20):
            if not self.ids:
                break
            oid = random.choice(self.ids)
            if want is None or want(self.docs[oid]):
                return oid, self.docs[oid]
        return None, None


class Run:
    def __init__(self):
        self.members: list[Member] = []
        self.failures: list[str] = []
        self.reached: dict[str, int] = {}
        self.stats = dict.fromkeys(("writes", "refused", "reads", "erasures", "queued", "holds", "crashes", "kills",
                                    "transient"), 0)
        self.waiting: set[asyncio.Task] = set()

    def fail(self, msg: str) -> None:
        self.failures.append(msg)
        if len(self.failures) <= 20:
            print(f"SIM FAILURE {msg}", flush=True)

    def member(self) -> Member:
        return random.choice(self.members)

    async def read(self, m: Member, oid: str) -> tuple[str, str]:
        """A document as a component reads it: from its archive when it has left the online tables, else online. The
        two reads are not one snapshot, so a document published and released in between is read again. ("", ""):
        found nowhere."""
        for _ in range(5):
            try:
                a = await reads.get(m.cat, SIM_CLASS, oid)
            except errs.Error as e:
                if e.reason == "NOT_FOUND":  # destroyed: gone from the decision on (P15.22, P15.29)
                    return "", ""
                if e.reason == "DEPENDENCY_UNAVAILABLE":
                    self.stats["transient"] += 1
                    await asyncio.sleep(0.05)
                    continue
                raise
            if a is not None:
                return aggregate(a.root, a.children), a.route
            online = await read_online(m.d, uuid.UUID(oid))
            if online is not None:
                return online, "online"
            await asyncio.sleep(0.05)
        return "", ""

    async def check(self, m: Member, oid: str, doc: Doc) -> None:
        """Reads a document and compares it with what was last written; doc is locked by the caller."""
        if doc.busy or doc.destroyed:
            return
        self.stats["reads"] += 1
        try:
            got, where = await self.read(m, oid)
        except Exception as e:  # noqa: BLE001
            self.fail(f"{m.name} {oid}: read: {e!r}")
            return
        if where == "":
            await self.gone(m, oid, doc)
        elif got != doc.agg:
            self.fail(f"{m.name} {oid}: read from {where} differs:\n got  {got}\n want {doc.agg}")

    async def gone(self, m: Member, oid: str, doc: Doc) -> None:
        """A document read nowhere: destroyed, which only one past its retention and not held may be."""
        state = await m.d.background_tx(lambda tx: tx.fetchval(
            "SELECT u.state FROM be_tier_member b JOIN be_tier_unit u ON u.id = b.unit WHERE b.class = $1 AND "
            "b.root_id = $2", SIM_CLASS, oid))
        if state != "destroyed":
            self.fail(f"{m.name} {oid}: not found (unit {state!r})")
        elif not doc.old or doc.held:
            self.fail(f"{m.name} {oid}: destroyed though old={doc.old} held={doc.held}")
        else:
            doc.destroyed = True

    async def reread(self, m: Member, oid: str, doc: Doc) -> None:
        """Takes a document's form after a committed write; doc is locked by the caller."""
        try:
            got, where = await self.read(m, oid)
        except Exception as e:  # noqa: BLE001
            self.fail(f"{m.name} {oid}: reading back a write: {e!r}")
            return
        if where == "":  # destroyed meanwhile (an old document)
            await self.gone(m, oid, doc)
        else:
            doc.agg = got

    async def write(self, m: Member) -> None:
        """Makes a document: open, closed into the past (due for the archive), or closed past the retention."""
        oid, kind = uuid.uuid7(), random.randrange(10)
        closed = None
        if kind < 5:
            closed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=400 + random.randrange(240))
        elif kind < 6:
            closed = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=3700 + random.randrange(40))
        day = None if random.randrange(3) == 0 else dt.date(2020 + random.randrange(6), 1 + random.randrange(12),
                                                            1 + random.randrange(28))

        async def body(tx):
            await tx.execute("""INSERT INTO docs (id, grp, rev, code, note, amount, qty, small, flag, day, attrs, blob,
                tags, closed_at, created_at) VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                now() - interval '4000 days')""", oid, f"g{random.randrange(4)}", "D-" + str(oid)[:8],
                             f"call +34 600 {random.randrange(10**6)}",
                             Decimal(f"{random.randrange(10**6)}.{random.randrange(10**4):04d}"),
                             random.randrange(1 << 53), random.randrange(65536) - 32768, random.randrange(2) == 0, day,
                             '{"n": [1, "é"]}', random.randbytes(random.randrange(2048)),
                             [f"t{random.randrange(9)}", "Ä"], closed)
            for line_no in range(1 + random.randrange(5)):
                line = uuid.uuid7()
                await tx.execute("INSERT INTO doc_lines VALUES ($1, $2, $3, $4, $5)", line, oid, line_no,
                                 f"line {line_no}", f"person {random.randrange(1000)}")
                for s in range(random.randrange(4)):
                    await tx.execute("INSERT INTO line_marks VALUES ($1, $2, 'mark')", line, s)

        try:
            await m.d.background_tx(body)
        except Exception as e:  # noqa: BLE001
            self.fail(f"{m.name}: write: {e!r}")
            return
        self.stats["writes"] += 1
        doc = Doc(open=kind >= 6, old=kind == 5)
        async with doc.lock:
            m.docs[str(oid)] = doc
            m.ids.append(str(oid))
            await self.reread(m, str(oid), doc)

    async def change(self, m: Member) -> None:
        """Edits an open document, closes one into the past, or writes to a closed one (refused once sealed)."""
        oid, doc = m.pick(lambda d: not d.destroyed and not d.busy)
        if doc is None:
            return
        async with doc.lock:
            if doc.destroyed or doc.busy:
                return
            days = 0
            if doc.open and random.randrange(3) == 0:
                days = 3700 + random.randrange(40) if random.randrange(6) == 0 else 400 + random.randrange(240)
                stmt, args = ("UPDATE docs SET rev = rev + 1, closed_at = now() - make_interval(days => $2) "
                              "WHERE id = $1"), (uuid.UUID(oid), days)
            else:
                stmt, args = ("UPDATE docs SET rev = rev + 1, amount = amount + 1, note = $2 WHERE id = $1",
                              (uuid.UUID(oid), "changed " + str(uuid.uuid7())[:8]))

            async def body(tx):
                await tx.execute(stmt, *args)
                await tx.execute("UPDATE doc_lines SET label = label || '+' WHERE doc_id = $1 AND line = 0",
                                 uuid.UUID(oid))

            try:
                await m.d.background_tx(body)
            except errs.Error as e:
                if e.reason == "RECORD_SEALED":
                    self.stats["refused"] += 1
                    if doc.open:
                        self.fail(f"{m.name} {oid}: an open document refused a write")
                    return
                self.fail(f"{m.name} {oid}: change: {e!r}")
                return
            self.stats["writes"] += 1
            if days:
                doc.open, doc.old = False, days >= 3650
            await self.reread(m, oid, doc)

    async def erase(self, m: Member) -> None:
        """Erases a closed document: as a request (the parent's catalog), or queued as a recovery's redo leaves it (a
        replica runs it, with the crash points). A quarter take a document of a unit sealed now: a unit is sealed for
        about a second, and the replicas would hardly ever erase one otherwise."""
        oid, doc = m.pick(lambda d: not d.open and not d.destroyed and not d.busy)
        from_sealed = False
        if random.randrange(4) == 0:
            sealed = await m.d.background_tx(lambda tx: tx.fetchval(
                "SELECT b.root_id FROM be_tier_member b JOIN be_tier_unit u ON u.id = b.unit WHERE u.state = 'sealed' "
                "ORDER BY random() LIMIT 1"))
            d = m.docs.get(sealed) if sealed else None
            if d is not None and not d.busy and not d.destroyed:
                oid, doc, from_sealed = sealed, d, True
        if doc is None:
            return
        async with doc.lock:
            if doc.busy or doc.destroyed:
                return
            if (from_sealed or random.randrange(2) == 0) and not doc.held:
                await self._queue_erasure(m, oid, doc)
                return
            for _ in range(5):
                try:
                    out = await erase.erase(m.cat, SIM_CLASS, oid, "sim")
                except Exception as e:  # noqa: BLE001
                    self.fail(f"{m.name} {oid}: erase: {e!r}")
                    return
                if out == erase.DEFERRED:
                    return
                if out == erase.ONLINE:  # not in a unit past open: the component erases its online rows
                    async def online(tx):
                        await tx.execute("UPDATE docs SET note = NULL WHERE id = $1", uuid.UUID(oid))
                        await tx.execute("UPDATE doc_lines SET person = NULL WHERE doc_id = $1", uuid.UUID(oid))

                    try:
                        await m.d.background_tx(online)
                    except errs.Error as e:
                        if e.reason == "RECORD_SEALED":
                            continue  # sealed meanwhile: the catalog erases it now
                        self.fail(f"{m.name} {oid}: erasing online: {e!r}")
                        return
                self.stats["erasures"] += 1
                want = erased(doc.agg)
                await self.reread(m, oid, doc)
                if doc.agg != want and not doc.destroyed:
                    self.fail(f"{m.name} {oid}: after the erasure ({out}):\n got  {doc.agg}\n want {want}")
                return

    async def _queue_erasure(self, m: Member, oid: str, doc: Doc) -> None:
        """A recovery's replay queues the redo for a unit published or released (an online or sealed one it erases in
        the replay's own transaction); queued for a sealed one too here, so that the replicas reach the sealed
        erasure's crash points (the job erases whatever state the unit is in when it runs). doc is locked."""
        async def body(tx):
            state = await tx.fetchval("SELECT coalesce((SELECT u.state FROM be_tier_member b JOIN be_tier_unit u ON "
                                      "u.id = b.unit WHERE b.class = $1 AND b.root_id = $2 FOR SHARE OF u), 'none')",
                                      SIM_CLASS, oid)
            if state not in ("sealed", "published", "released"):
                return ""
            return await m.queue.enqueue(tx, erase.JOB, {"class": SIM_CLASS, "root": oid, "basis": "sim"})

        try:
            job = await m.d.background_tx(body)
        except Exception as e:  # noqa: BLE001
            self.fail(f"{m.name} {oid}: queueing an erasure: {e!r}")
            return
        if not job:
            return
        self.stats["queued"] += 1
        doc.busy = True
        t = asyncio.create_task(self.await_erasure(m, oid, doc, job, erased(doc.agg)))
        self.waiting.add(t)
        t.add_done_callback(self.waiting.discard)

    async def await_erasure(self, m: Member, oid: str, doc: Doc, job: str, want: str) -> None:
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            state = await m.d.background_tx(lambda tx: tx.fetchval("SELECT state FROM be_work_item WHERE id = $1",
                                                                   job))
            if state == "done":
                async with doc.lock:
                    doc.busy = False
                    self.stats["erasures"] += 1
                    await self.reread(m, oid, doc)
                    if doc.agg != want and not doc.destroyed:
                        self.fail(f"{m.name} {oid}: after the queued erasure:\n got  {doc.agg}\n want {want}")
                        doc.agg = want
                return
            await asyncio.sleep(0.2)
        self.fail(f"{m.name} {oid}: the queued erasure {job} did not finish in 5 minutes")

    async def hold(self, m: Member) -> None:
        """A legal hold on an old document (never lifted: it must outlive its retention)."""
        oid, doc = m.pick(lambda d: d.old and not d.held and not d.destroyed and not d.busy)
        if doc is None:
            return
        async with doc.lock:
            async def body(tx):
                # a hold on a document already destroyed protects nothing: the test holds only what is still there
                state = await tx.fetchval("SELECT coalesce((SELECT u.state FROM be_tier_member b JOIN be_tier_unit u "
                                          "ON u.id = b.unit WHERE b.class = $1 AND b.root_id = $2 FOR SHARE OF u), "
                                          "'none')", SIM_CLASS, oid)
                if state != "destroyed":
                    await tx.execute("INSERT INTO be_tier_hold (id, scope_kind, scope, source, authorized_by, reason) "
                                     "VALUES ($1, 'record', $2, 'legal', 'sim', 'the simulation')", uuid.uuid7(),
                                     f"{SIM_CLASS}/{oid}")
                return state

            try:
                state = await m.d.background_tx(body)
            except Exception as e:  # noqa: BLE001
                if "40P01" in repr(e) or "deadlock" in repr(e).lower():  # a destruction deciding at the same moment
                    return
                self.fail(f"{m.name} {oid}: hold: {e!r}")
                return
            if state != "destroyed":
                doc.held = True
                self.stats["holds"] += 1

    async def progress(self, start: float) -> None:
        parts = []
        for m in self.members:
            states = await m.d.background_tx(lambda tx: tx.fetch(
                "SELECT state || ':' || count(*) FROM be_tier_unit GROUP BY state ORDER BY state"))
            parts.append(f"{m.name} docs={len(m.ids)} units={','.join(r[0] for r in states)}")
        s = self.stats
        say(f"{int((time.monotonic() - start) / 60)}m {' '.join(parts)} writes={s['writes']} refused={s['refused']} "
            f"reads={s['reads']} erasures={s['erasures']} queued={s['queued']} holds={s['holds']} "
            f"crashes={s['crashes']} points={len(self.reached)}/{len(POINTS)} kills={s['kills']} "
            f"transient={s['transient']} failures={len(self.failures)}")


class Replica:
    """One slot of the four: its members, and the child process running now."""

    def __init__(self, members: list[Member], logs: pathlib.Path):
        self.members, self.logs, self.proc = members, logs, None

    async def spawn(self, crash: str, after: int):
        spec = {"id": str(uuid.uuid7()), "crash": crash, "after": after, "members": [m.spec() for m in self.members]}
        self.proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "tests.sim.tierchild", cwd=ROOT, stdout=asyncio.subprocess.PIPE,
            stderr=self.logs.open("a"), env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1",
                                             "SIM_TIER_CHILD": json.dumps(spec)})
        return self.proc

    def kill(self) -> bool:
        if self.proc is not None and self.proc.returncode is None:
            self.proc.kill()
            return True
        return False


async def supervise(run: Run, rp: Replica, stop: asyncio.Event, done: asyncio.Event, turn: list[int]) -> None:
    """Runs a replica with crash points in turn until stop, then without until done."""
    while not done.is_set():
        crash, after = "", 0
        if not stop.is_set():
            turn[0] += 1
            crash, after = POINTS[turn[0] % len(POINTS)], 1 + random.randrange(3)
        proc = await rp.spawn(crash, after)
        limit = time.monotonic() + 90  # a point this replica does not reach soon: another one
        crashing = bool(crash)
        while True:
            try:
                line = await asyncio.wait_for(proc.stdout.readline(), 0.5)
            except TimeoutError:
                line = None
            if line == b"":  # ended
                break
            if line and line.startswith(b"CRASH "):
                p = line.decode().removeprefix("CRASH ").strip()
                run.reached[p] = run.reached.get(p, 0) + 1
                run.stats["crashes"] += 1
            if crashing and (time.monotonic() > limit or stop.is_set()):
                rp.kill()
                crashing = False
            if done.is_set():
                rp.kill()
                await proc.wait()
                return
        await proc.wait()
        await asyncio.sleep(0.1)


async def every(stop: asyncio.Event, period: float, fn) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), period)
            return
        except TimeoutError:
            pass
        try:
            await fn()
        except Exception as e:  # noqa: BLE001 - a workload step that fails is a finding, not the end of the run
            print(f"SIM FAILURE workload: {e!r}", flush=True)


async def settle(run: Run, limit: float) -> bool:
    """Waits until nothing is left for the jobs: nothing due unsealed, no unit sealed or published, every unit on its
    class's route with no older version kept, no queued erasure, nothing destroyable left; then past the orphans'
    window."""
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        idle = True
        for m in run.members:
            if await m.cat.groupings(SIM_CLASS):
                idle = False
                continue
            route = m.cat.classes[SIM_CLASS].routes[0]
            pending = await m.d.background_tx(lambda tx: tx.fetchval(
                """SELECT (SELECT count(*) FROM be_tier_unit WHERE state IN ('sealed', 'published'))
                + (SELECT count(*) FROM be_tier_unit WHERE state = 'released' AND route <> $1)
                + (SELECT count(*) FROM be_tier_manifest f JOIN be_tier_unit u ON u.id = f.unit WHERE
                   f.version < u.manifest_version AND u.state <> 'destroyed' AND f.superseded_at IS NULL)
                + (SELECT count(*) FROM be_work_item WHERE job = $2 AND state <> 'done')
                + (SELECT count(*) FROM be_tier_unit u WHERE u.state = 'released' AND
                   u.anchor_max < now() - interval '3650 days' AND NOT EXISTS (SELECT 1 FROM be_tier_hold h JOIN
                   be_tier_member b ON h.scope = $3 || '/' || b.root_id WHERE b.unit = u.id AND h.released_at IS NULL))
                """, route, erase.JOB, SIM_CLASS))
            if pending:
                idle = False
        if idle:
            await asyncio.sleep(ORPHAN_AGE.total_seconds() + CLEANUP_EVERY + 15)  # past the window, and a cleanup since
            return True
        await asyncio.sleep(3)
    return False


async def final_checks(run: Run) -> None:
    """Every document, every published copy, the recovery check, the work items."""
    for m in run.members:
        for oid in list(m.ids):
            doc = m.docs[oid]
            async with doc.lock:
                if doc.busy:
                    run.fail(f"{m.name} {oid}: a queued erasure never finished")
                await run.check(m, oid, doc)
        copies = await m.d.background_tx(lambda tx: tx.fetch(
            "SELECT u.id::text, f.manifest FROM be_tier_unit u JOIN be_tier_manifest f ON f.unit = u.id AND "
            "f.version = u.manifest_version WHERE u.state IN ('published', 'released')"))
        for unit, raw in copies:
            try:
                await transfer.verify(m.cat, manifest.parse(bytes(raw)))
            except Exception as e:  # noqa: BLE001
                run.fail(f"{m.name} unit {unit}: the published copy does not verify: {e!r}")
        reg = Checks()
        reg.add(check.recovery_check(m.cat))
        resp = await CheckServer(m.d, False, reg).Check(check_pb2.CheckRequest(recovery="sim"), None)
        for res in resp.results:
            if res.check == check.NAME and res.outcome != PASS:
                run.fail(f"{m.name}: be.tiering {res.outcome}: {list(res.differences)[:20]}")
        stuck = await m.d.background_tx(lambda tx: tx.fetch(
            "SELECT job || ' ' || state FROM be_work_item WHERE state NOT IN ('pending', 'running', 'done')"))
        if stuck:
            run.fail(f"{m.name}: work items left: {[r[0] for r in stuck]}")
        say(f"{m.name}: {len(m.ids)} documents checked, {len(copies)} copies verified")


async def setup(run: Run) -> None:
    await ensure_bucket(BUCKET, False)
    url, access, secret = s3()
    tag = str(uuid.uuid7())[:8]
    for name, route in (("py_tier_sim_a", "object-parquet"), ("py_tier_sim_b", "postgres")):
        await migrated(name, {"0001_sim.up.sql": SCHEMA_SQL})
        a = await pgtest.admin()
        try:
            await a.execute(f"SET search_path = {name}")
            for s in statements([sim_class()]):
                await a.execute(s)
            await a.execute("INSERT INTO be_tier_approval (id, scope_kind, scope, approved_by, reason) VALUES ($1, "
                            "'class', $2, 'sim', 'the retention has run')", uuid.uuid7(), SIM_CLASS)
        finally:
            await a.close()
        env = {k: v for k, v in pgtest.member_env(name).items() if not k.startswith("PG_OWNER")}
        env.update({store.STORE_URL: url, store.BUCKET: BUCKET, member.route_key(SIM_CLASS): route,
                    member.GRACE: "0s"})
        for role in store.ROLES:
            ak, sk = store.credential_keys(role)
            env[ak], env[sk] = access, secret
        m = Member(name, f"sim-{tag}-{name}", env)
        await renew_catalog(m)
        run.members.append(m)


async def renew_catalog(m: Member) -> None:
    """The parent's catalog of a member from its configuration now (a request in flight keeps the old one)."""
    d, pool, cat = await catalog_of(m.spec(), "parent", QUIET)
    m.d, m.cat = d, cat
    m.pools.append(pool)
    m.queue = Runner(d, "test/" + m.name, QUIET)
    m.queue.add(erase.erase_job(cat, QUIET))


async def test_the_tiering_long_simulation(tmp_path):
    run = Run()
    await setup(run)
    a, b = run.members
    logs = pathlib.Path(os.environ.get("SIM_REPLICA_LOG") or tmp_path / "replicas.log")
    replicas = [Replica([a, b], logs), Replica([a, b], logs), Replica([a], logs), Replica([b], logs)]
    say(f"{MINUTES} minutes of crashes, {len(POINTS)} crash points, replica logs in {logs}")
    start = time.monotonic()
    stop_crashes, done, stop_work, stop_bg = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
    turn = [0]
    supervisors = [asyncio.create_task(supervise(run, rp, stop_crashes, done, turn)) for rp in replicas]

    async def check_one():
        m = run.member()
        oid, doc = m.pick()
        if doc is not None:
            async with doc.lock:
                await run.check(m, oid, doc)

    async def backup():
        for m in run.members:
            await m.d.background_tx(lambda tx: tx.execute("INSERT INTO sim_backup VALUES (now())"))

    async def kill():  # a kill at no particular point
        if random.randrange(2) == 0 and random.choice(replicas).kill():
            run.stats["kills"] += 1

    workers = [asyncio.create_task(every(stop_work, p, f)) for p, f in (
        (0.06, lambda: run.write(run.member())), (0.04, lambda: run.change(run.member())),
        (0.7, lambda: run.erase(run.member())), (3.0, lambda: run.hold(run.member())),
        (0.005, check_one), (0.005, check_one), (30.0, kill))]
    background = [asyncio.create_task(every(stop_bg, 5.0, backup)),
                  asyncio.create_task(every(stop_bg, 60.0, lambda: run.progress(start)))]

    half = MINUTES * 60 / 2
    await asyncio.sleep(half)
    say("the routes swap: a to the warm route, b to the object store")
    for m in run.members:
        m.env[member.route_key(SIM_CLASS)] = "postgres" if m is a else "object-parquet"
        await renew_catalog(m)
    for rp in replicas:  # every replica starts again with the new configuration
        rp.kill()
    await asyncio.sleep(half)

    say("the crashes and the workload stop; the jobs finish")
    stop_work.set()
    await asyncio.gather(*workers)
    await asyncio.gather(*run.waiting)
    stop_crashes.set()
    if not await settle(run, 30 * 60):
        run.fail("the jobs did not finish within 30 minutes of the end")
    await final_checks(run)  # the replicas still run: nothing is left for them to do
    await run.progress(start)
    done.set()
    await asyncio.gather(*supervisors)
    stop_bg.set()
    await asyncio.gather(*background)
    for m in run.members:
        for p in m.pools:
            await p.close()

    missing = [p for p in POINTS if not run.reached.get(p)]
    say("crash points reached: " + " ".join(f"{p}={n}" for p, n in sorted(run.reached.items())))
    if missing:
        run.fail(f"crash points never reached: {missing}")
    say(f"RESULT failures={len(run.failures)}")
    assert run.failures == [], "\n".join(run.failures[:50])
