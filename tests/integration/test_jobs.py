"""Background jobs against PostgreSQL (P14, CP-JOBS-01…06): two replicas of one member share the tables
of its schema; a cron slot runs once, a singleton has one holder and is taken over after the lease TTL, a
queued job runs once and only after commit, failures retry and die, reconcilers claim, cleanup deletes."""
import asyncio
import io
import json

import pytest
from prometheus_client import generate_latest

import besdk
from besdk import Module, context
from besdk.jobs.model import Job, JobKind, Reconciler, Worker
from besdk.jobs.runner import JobsRuntime
from besdk.runtime import Runtime, Shared, Spec
from tests.integration.conftest import component_dir
from tests.integration.test_migrate import migrator

PROPS = {"JOBS_OVERRIDES": {"type": "string", "default": ""},
         "BUSINESS_TIMEZONE": {"type": "string", "default": "Asia/Shanghai"}}


def replica(root, ident, module, **env) -> tuple[Runtime, JobsRuntime]:
    async def create(rt):
        return module

    spec = Spec(id="conformance/widget-py", migrations=root / "migrations", contracts=root / "contracts", create=create)
    rt = Runtime(spec, ident.env(PG_POOL_MAX="4", **env), Shared.standalone(spec_id=spec.id),
                 log_stream=io.StringIO())
    return rt, JobsRuntime(rt, module, lease_ttl=1.5)


@pytest.fixture
def root(tmp_path, ident):
    r = component_dir(tmp_path, props=PROPS)
    migrator(r, ident).up()
    return r


async def stop(*pairs):
    for rt, jr in pairs:
        await jr.stop()
        await rt.supervisor.stop(timeout=2)
        await rt.store().close()


async def test_cron_slot_runs_once_across_replicas(root, ident):
    runs = []

    async def tick():
        runs.append(context.current().job)

    m = Module(jobs=[Job("widget.tick", JobKind.CRON, timeout=5, cron="@every 1s", run=tick)])
    a, b = replica(root, ident, m), replica(root, ident, m)
    await a[1].start()
    await b[1].start()
    await asyncio.sleep(3.6)
    await stop(a, b)
    slots = ident.sql(f'SELECT count(*), count(DISTINCT slot_at) FROM "{ident.schema}".besdk_job_slot '
                     f"WHERE name = 'widget.tick'")[0]
    assert slots[0] == slots[1] == len(runs) and 3 <= len(runs) <= 5
    assert set(runs) == {"widget.tick"}


async def test_singleton_has_one_holder_and_is_taken_over(root, ident):
    active, peak, holders = [0], [0], []

    async def work():
        active[0] += 1
        peak[0] = max(peak[0], active[0])
        holders.append(context.current().job_epoch)
        try:
            await asyncio.sleep(0.2)
        finally:
            active[0] -= 1

    m = Module(jobs=[Job("widget.sweep", JobKind.SINGLETON, timeout=5, interval=0.1, run=work)])
    a, b = replica(root, ident, m), replica(root, ident, m)
    await a[1].start()
    await b[1].start()
    await asyncio.sleep(1.5)
    assert peak[0] == 1 and holders
    first = holders[-1]
    held = ident.sql(f"SELECT holder FROM \"{ident.schema}\".besdk_job_lease WHERE name = 'widget.sweep'")[0][0]
    holder, other = (a, b) if held == a[1].holder else (b, a)
    await holder[1].stop()  # releases the lease; the other replica takes over with a new epoch
    await holder[0].supervisor.stop(timeout=2)
    await asyncio.sleep(2.0)
    assert holders[-1] == first + 1 and peak[0] == 1
    await stop(other)
    await holder[0].store().close()


async def test_queue_runs_once_after_commit_retries_and_dies(root, ident):
    done, dead = [], []

    async def run(j):
        if j.args.get("fail"):
            raise RuntimeError("boom")
        done.append(j.args["n"])

    async def on_dead(tx, j):
        dead.append((j.args, j.attempts, j.last_error))

    m = Module(workers=[Worker("widget.mail", run, timeout=5, max_attempts=2, backoff=(0.1,), concurrency=2,
                               on_dead=on_dead)])
    a, b = replica(root, ident, m), replica(root, ident, m)
    store = a[0].store()
    for n in range(10):
        await store.tx(lambda tx, n=n: tx.enqueue("widget.mail", {"n": n}))
    assert await store.tx(lambda tx: tx.enqueue("widget.mail", {"n": 99}, unique_key="u1")) is True
    assert await store.tx(lambda tx: tx.enqueue("widget.mail", {"n": 98}, unique_key="u1")) is False

    async def rolled_back(tx):
        await tx.enqueue("widget.mail", {"n": 1000})
        raise besdk.Error(besdk.Code.FAILED_PRECONDITION, "NOPE")

    with pytest.raises(besdk.Error):
        await store.tx(rolled_back)
    await store.tx(lambda tx: tx.enqueue("widget.mail", {"fail": True}))
    with pytest.raises(besdk.Error):
        await store.tx(lambda tx: tx.enqueue("widget.unknown", {}))
    await a[1].start()
    await b[1].start()
    await asyncio.sleep(3.0)
    await stop(a, b)
    assert sorted(done) == list(range(10)) + [99]
    assert dead and dead[0][0] == {"fail": True} and dead[0][1] == 2 and "boom" in dead[0][2]
    states = dict(ident.sql(f'SELECT state, count(*) FROM "{ident.schema}".besdk_job_queue GROUP BY state'))
    assert states == {"done": 11, "dead": 1}
    text = generate_latest(a[0].registry).decode() + generate_latest(b[0].registry).decode()
    assert 'be_queue_depth{component="conformance/widget-py",kind="widget.mail",state="dead"} 1.0' in text


async def test_reconciler_claims_applies_and_gives_up(root, ident):
    ident.sql(f'CREATE TABLE "{ident.schema}".proc (id text PRIMARY KEY, state text NOT NULL)')
    ident.sql(f'GRANT SELECT, UPDATE ON "{ident.schema}".proc TO "{ident.user}"')
    ident.sql(f"INSERT INTO \"{ident.schema}\".proc VALUES ('ok1', 'PENDING'), ('ok2', 'PENDING'), ('bad', 'PENDING')")
    handled = []

    async def candidates(tx, limit):
        return [r["id"] for r in await tx.fetch("SELECT id FROM proc WHERE state = 'PENDING' LIMIT $1", limit)]

    async def handle(item):
        handled.append(item)
        if item == "bad":
            raise RuntimeError("upstream says no")
        return "DONE"

    async def apply(tx, item, outcome):
        await tx.execute("UPDATE proc SET state = $2 WHERE id = $1", item, outcome)

    async def give_up(tx, item):
        await tx.execute("UPDATE proc SET state = 'SUSPENDED' WHERE id = $1", item)

    async def still_due(tx, item):
        return await tx.fetchval("SELECT state = 'PENDING' FROM proc WHERE id = $1", item)

    rec = Reconciler("widget.recon", every=0.2, timeout=5, candidates=candidates, id=lambda x: x, handle=handle,
                     apply=apply, max_attempts=2, backoff=(0.1,), give_up=give_up, still_due=still_due)
    m = Module(reconcilers=[rec])
    a, b = replica(root, ident, m), replica(root, ident, m)
    await a[1].start()
    await b[1].start()
    await asyncio.sleep(2.0)
    await stop(a, b)
    assert dict(ident.sql(f'SELECT id, state FROM "{ident.schema}".proc')) == {
        "ok1": "DONE", "ok2": "DONE", "bad": "SUSPENDED"}
    assert handled.count("ok1") == 1 and handled.count("bad") == 2
    # applied items leave no row; the given-up one stays suspended so a stale replica cannot claim it again
    assert ident.sql(f'SELECT item_id, next_at = \'infinity\' FROM "{ident.schema}".besdk_reconcile') == [("bad", True)]


async def test_overrides_disable_and_reschedule(root, ident):
    runs = []

    async def tick():
        runs.append(1)

    m = Module(jobs=[Job("widget.tick", JobKind.EVERY, timeout=5, interval=60, run=tick),
                     Job("widget.off", JobKind.EVERY, timeout=5, interval=0.1, run=tick)])
    rt, jr = replica(root, ident, m, JOBS_OVERRIDES=json.dumps(
        {"widget.tick": {"interval": "100ms"}, "widget.off": {"enabled": False}}))
    await jr.start()
    await asyncio.sleep(0.75)
    await stop((rt, jr))
    assert 4 <= len(runs) <= 8


async def test_run_once_by_kind(root, ident):
    runs = []

    async def work():
        runs.append(context.current().job)

    async def fail():
        raise RuntimeError("bad")

    m = Module(jobs=[Job("widget.daily", JobKind.CRON, timeout=5, cron="0 3 * * *", run=work),
                     Job("widget.sweep", JobKind.SINGLETON, timeout=5, interval=60, run=work),
                     Job("widget.broken", JobKind.EVERY, timeout=5, interval=60, run=fail)])
    rt, jr = replica(root, ident, m)
    one = JobsRuntime(rt, m, holder_suffix="job-run:test")
    assert (await one.run_once("widget.daily")) == "ok"
    assert (await one.run_once("widget.daily")).startswith("noop")  # the slot is taken
    assert (await one.run_once("widget.sweep")) == "ok"
    with pytest.raises(RuntimeError):
        await one.run_once("widget.broken")
    with pytest.raises(KeyError):
        await one.run_once("widget.nope")
    holder = ident.sql(f'SELECT holder FROM "{ident.schema}".besdk_job_slot')[0][0]
    assert holder.startswith("conformance/widget-py/job-run:")
    assert runs == ["widget.daily", "widget.sweep"]
    await rt.store().close()


async def test_cleanup_deletes_expired_rows(root, ident):
    s = ident.schema
    ident.sql(f"INSERT INTO \"{s}\".besdk_idempotency (caller, idempotency_key, command, request_hash, expires_at) "
              f"VALUES ('system', 'old', 'c', '\\x00', now() - interval '1 day'), ('system', 'new', 'c', '\\x00', now() + interval '1 day')")
    ident.sql(f"INSERT INTO \"{s}\".besdk_job_queue (id, kind, args, max_attempts, state, finished_at) VALUES "
              f"(gen_random_uuid(), 'k', '{{}}', 1, 'done', now() - interval '8 days'), "
              f"(gen_random_uuid(), 'k', '{{}}', 1, 'done', now()), (gen_random_uuid(), 'k', '{{}}', 1, 'dead', now() - interval '60 days')")
    ident.sql(f"INSERT INTO \"{s}\".besdk_job_slot (name, slot_at, holder) VALUES ('j', now() - interval '31 days', 'h'), ('j', now(), 'h')")
    ident.sql(f"INSERT INTO \"{s}\".besdk_event_cursor (aggregate_type, aggregate_id, version, event_id, seen_at) VALUES "
              f"('a', '1', 1, 'e', now() - interval '31 days'), ('a', '2', 1, 'e', now())")
    rt, jr = replica(root, ident, Module())
    assert await jr.run_once("be.cleanup") == "ok"
    assert [r[0] for r in ident.sql(f'SELECT idempotency_key FROM "{s}".besdk_idempotency')] == ["new"]
    assert sorted(r[0] for r in ident.sql(f'SELECT state FROM "{s}".besdk_job_queue')) == ["dead", "done"]
    assert ident.sql(f'SELECT count(*) FROM "{s}".besdk_job_slot WHERE name = \'j\'')[0][0] == 1
    assert ident.sql(f'SELECT count(*) FROM "{s}".besdk_event_cursor')[0][0] == 1
    await rt.store().close()
