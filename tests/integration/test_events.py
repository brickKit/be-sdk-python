"""Events end to end on PostgreSQL 16 + NATS 2.12 (P12): outbox in the business transaction, the pump and
PubAck, envelope headers, durables created only when absent, cursor dedup, SDK-side backoff and dead
letters (repro r1-07), causation and hop, Apply vs Run, InProgress."""
import asyncio
import inspect
import io
import json

import nats
import nats.js.errors
import pytest
from nats.js.api import ConsumerConfig

from besdk import Event, Events, Module, Subscription, context, ids, permanent
from besdk.events import consumer as consumer_mod
from besdk.events.envelope import durable_name
from besdk.runtime import Runtime, Shared, Spec
from tests.integration.conftest import component_dir
from tests.integration.test_migrate import migrator

CONTRACT = {"events": [
    {"subject": "conformance.widget.created.v1", "x-aggregate-type": "conformance.widget.widget",
     "x-consumption": "state", "x-transaction-document": True,
     "payload": {"type": "object", "required": ["widget_id", "legal_entity_id"],
                 "properties": {"widget_id": {"type": "string"}, "legal_entity_id": {"type": "string"}}}},
    {"subject": "conformance.widget.noted.v1", "x-aggregate-type": "conformance.widget.widget",
     "x-consumption": "state", "payload": {"type": "object"}},
]}
EV_PROPS = {"NATS_URL": {"type": "string"}, "EVENTS_MAX_DELIVER": {"type": "integer", "default": 8},
            "EVENTS_BACKOFF": {"type": "string", "default": "1s,10s,1m,5m,15m,30m,1h"}}


class Seen:
    def __init__(self):
        self.applied: list[tuple[str, int]] = []
        self.fail = 0
        self.mode = "ok"
        self.in_tx = None
        self.slow = 0.0


def make_module(seen: Seen, subject="conformance.widget.created.v1", run=False):
    async def apply(tx, ev: Event):
        if seen.slow:
            await asyncio.sleep(seen.slow)
        if seen.mode == "permanent":
            raise permanent("cannot parse")
        if seen.mode == "error":
            raise RuntimeError("handler failed")
        seen.applied.append((ev.aggregate_id, ev.version))
        seen.in_tx = context.current().tx is not None
        if ev.subject == "conformance.widget.created.v1" and ev.json().get("echo"):
            await tx.publish(Event("conformance.widget.noted.v1", ev.aggregate_id, ev.version, {"from": ev.id}))

    async def run_(ev: Event):
        await apply(None, ev)

    sub = Subscription(subject, run=run_) if run else Subscription(subject, apply=apply)

    async def create(rt):
        return Module(events=Events(publishes=["conformance.widget.created.v1", "conformance.widget.noted.v1"],
                                    subscribe=[sub]))

    return create


def new_cid() -> str:
    import uuid
    return "conformance/w" + uuid.uuid4().hex[:10]


async def start(tmp_path, ident, nats_url, create, cid=None, **env) -> Runtime:
    cid = cid or new_cid()
    root = component_dir(tmp_path, cid, props=EV_PROPS)
    (root / "contracts" / "events" / "widget.events.json").write_text(json.dumps(CONTRACT))
    migrator(root, ident).up()
    spec = Spec(id=cid, migrations=root / "migrations", contracts=root / "contracts", create=create)
    rt = Runtime(spec, ident.env(NATS_URL=nats_url, **env), Shared.standalone(spec_id=spec.id),
                 log_stream=io.StringIO())
    module = await spec.create(rt)
    await rt.store().open()
    await rt.start_events(module)
    await asyncio.wait_for(rt.events.started.wait(), 15)
    return rt


async def stop(rt):
    await rt.stop_events()
    await rt.supervisor.stop()
    await rt.shared.close()


def payload(wid, **kw):
    return {"widget_id": wid, "legal_entity_id": "LE01", **kw}


async def eventually(cond, timeout=10.0):
    for _ in range(int(timeout / 0.05)):
        if await cond() if inspect.iscoroutinefunction(cond) else cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not reached")


async def stream_msgs(nats_url, stream, subject):
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    sub = await js.pull_subscribe(subject, stream=stream)
    try:
        msgs = await sub.fetch(50, timeout=1)
    except Exception:  # noqa: BLE001
        msgs = []
    await nc.close()
    return msgs


async def test_publish_through_outbox_with_envelope(tmp_path, ident, nats_url):
    seen = Seen()
    rt = await start(tmp_path, ident, nats_url, make_module(seen, subject="conformance.widget.noted.v1"))
    wid = str(ids.new_id())
    with rt.tracer.start_as_current_span("request"):
        await rt.store().tx(lambda tx: tx.publish(Event("conformance.widget.created.v1", wid, 1, payload(wid))))

    async def published():
        return await rt.store().tx(lambda tx: tx.fetchval(
            "SELECT count(*) FROM besdk_outbox WHERE status = 'PUBLISHED' AND aggregate_id = $1", wid)) == 1

    await eventually(published)
    row = await rt.store().tx(lambda tx: tx.fetchrow(
        "SELECT id, created_at, hop_count, causation_id, traceparent FROM besdk_outbox WHERE aggregate_id = $1", wid))
    assert ids.id_time(row["id"]) == row["created_at"] and row["hop_count"] == 0 and row["traceparent"]
    msgs = [m for m in await stream_msgs(nats_url, "BE_CONFORMANCE", "conformance.widget.created.v1")
            if m.headers.get("ce-subject") == wid]
    h = msgs[0].headers
    assert h["ce-id"] == str(row["id"]) == h["Nats-Msg-Id"] and h["ce-legalentity"] == "LE01"
    assert h["ce-dataschema"] == f"{rt.id}@1.0.0/contracts/events/widget.events.json#conformance.widget.created.v1"
    assert h["ce-aggregatetype"] == "conformance.widget.widget" and h["ce-hopcount"] == "0"
    await stop(rt)


async def test_publish_refuses_contract_violations(tmp_path, ident, nats_url):
    rt = await start(tmp_path, ident, nats_url, make_module(Seen()))
    from besdk import Error
    for ev, reason in [(Event("conformance.widget.created.v1", "w", 1, {"widget_id": "w"}), "LEGAL_ENTITY_MISSING"),
                       (Event("conformance.widget.created.v1", "w", 1, {"legal_entity_id": "LE01"}), "PAYLOAD_INVALID"),
                       (Event("conformance.other.thing.v1", "w", 1, {}), "SUBJECT_NOT_DECLARED")]:
        with pytest.raises(Error) as ei:
            await rt.store().tx(lambda tx, ev=ev: tx.publish(ev))
        assert ei.value.internal_message.startswith(reason), ei.value.internal_message
    await stop(rt)


async def test_durable_created_once_never_updated(tmp_path, ident, nats_url):
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    try:
        await js.stream_info("BE_CONFORMANCE")
    except nats.js.errors.NotFoundError:
        await js.add_stream(name="BE_CONFORMANCE", subjects=["conformance.>"])
    cid = new_cid()
    d = durable_name(cid, "conformance.widget.created.v1")
    await js.add_consumer("BE_CONFORMANCE", ConsumerConfig(durable_name=d, filter_subject="conformance.widget.created.v1",
                                                           ack_wait=30, max_ack_pending=100))
    rt = await start(tmp_path, ident, nats_url, make_module(Seen()), cid=cid)
    info = await js.consumer_info("BE_CONFORMANCE", d)
    assert info.config.max_ack_pending == 100  # an operator's change stays
    await stop(rt)
    await js.delete_consumer("BE_CONFORMANCE", d)
    (tmp_path / "2").mkdir()
    rt = await start(tmp_path / "2", ident, nats_url, make_module(Seen()), cid=cid)
    info = await js.consumer_info("BE_CONFORMANCE", d)
    assert (info.config.ack_wait, info.config.max_ack_pending, info.config.max_deliver) == (30, 256, -1)
    assert not info.config.backoff
    await stop(rt)
    await nc.close()


async def test_apply_dedups_by_cursor_and_derives_causation(tmp_path, ident, nats_url):
    seen = Seen()
    rt = await start(tmp_path, ident, nats_url, make_module(seen))
    wid = str(ids.new_id())
    for v in (1, 2, 1):
        await rt.store().tx(lambda tx, v=v: tx.publish(Event("conformance.widget.created.v1", wid, v,
                                                              payload(wid, echo=v == 2))))
    await eventually(lambda: (wid, 2) in seen.applied)
    await asyncio.sleep(0.5)
    assert [x for x in seen.applied if x[0] == wid] in ([(wid, 1), (wid, 2)], [(wid, 2)])
    assert seen.in_tx is True
    noted = await rt.store().tx(lambda tx: tx.fetchrow(
        "SELECT causation_id, hop_count FROM besdk_outbox WHERE subject = 'conformance.widget.noted.v1' AND aggregate_id = $1",
        wid))
    created2 = await rt.store().tx(lambda tx: tx.fetchval(
        "SELECT id::text FROM besdk_outbox WHERE aggregate_id = $1 AND aggregate_version = 2", wid))
    assert noted["causation_id"] == created2 and noted["hop_count"] == 1
    await stop(rt)


async def test_errors_back_off_then_dead_letter(tmp_path, ident, nats_url):
    seen = Seen()
    seen.mode = "error"
    rt = await start(tmp_path, ident, nats_url, make_module(seen), EVENTS_MAX_DELIVER="2", EVENTS_BACKOFF="200ms,300ms")
    wid = str(ids.new_id())
    await rt.store().tx(lambda tx: tx.publish(Event("conformance.widget.created.v1", wid, 1, payload(wid))))
    d = durable_name(rt.id, "conformance.widget.created.v1")

    async def dead():
        return [m for m in await stream_msgs(nats_url, "BE_DLQ", f"dlq.{d}.>") if m.headers.get("ce-subject") == wid]

    for _ in range(100):
        got = await dead()
        if got:
            break
        await asyncio.sleep(0.1)
    h = got[0].headers
    assert h["be-dlq-reason"] == "MAX_DELIVER" and h["be-dlq-delivery"] == "3" and h["be-dlq-consumer"] == d
    assert h["Nats-Msg-Id"].startswith(f"dlq:{d}:")
    await stop(rt)


async def test_permanent_and_invalid_envelope_go_to_dead_letters_at_once(tmp_path, ident, nats_url):
    seen = Seen()
    seen.mode = "permanent"
    rt = await start(tmp_path, ident, nats_url, make_module(seen))
    wid = str(ids.new_id())
    await rt.store().tx(lambda tx: tx.publish(Event("conformance.widget.created.v1", wid, 1, payload(wid))))
    nc = await nats.connect(nats_url)
    await nc.jetstream().publish("conformance.widget.created.v1", b"{}", headers={"X-Aggregate-Id": "legacy"})
    await nc.close()
    d = durable_name(rt.id, "conformance.widget.created.v1")
    reasons = set()
    for _ in range(100):
        reasons = {m.headers["be-dlq-reason"] for m in await stream_msgs(nats_url, "BE_DLQ", f"dlq.{d}.>")}
        if {"PERMANENT", "ENVELOPE_INVALID"} <= reasons:
            break
        await asyncio.sleep(0.1)
    assert {"PERMANENT", "ENVELOPE_INVALID"} <= reasons
    await stop(rt)


async def test_run_handler_outside_transaction(tmp_path, ident, nats_url):
    seen = Seen()
    rt = await start(tmp_path, ident, nats_url, make_module(seen, run=True))
    wid = str(ids.new_id())
    await rt.store().tx(lambda tx: tx.publish(Event("conformance.widget.created.v1", wid, 1, payload(wid))))
    await eventually(lambda: (wid, 1) in seen.applied)
    assert seen.in_tx is False
    await stop(rt)


async def test_slow_handler_reports_progress_and_runs_once(tmp_path, ident, nats_url, monkeypatch):
    monkeypatch.setattr(consumer_mod, "ACK_WAIT", 2.0)  # a fresh durable gets ack wait 2 s
    monkeypatch.setattr(consumer_mod, "HANDLER_MARGIN", -3.0)  # let the handler outlive the ack wait
    seen = Seen()
    seen.slow = 3.0
    rt = await start(tmp_path, ident, nats_url, make_module(seen))
    wid = str(ids.new_id())
    await rt.store().tx(lambda tx: tx.publish(Event("conformance.widget.created.v1", wid, 1, payload(wid))))
    await eventually(lambda: (wid, 1) in seen.applied, timeout=15)
    await asyncio.sleep(2.5)
    assert seen.applied.count((wid, 1)) == 1
    await stop(rt)
