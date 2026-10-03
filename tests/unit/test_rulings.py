"""Stage-B review rulings that align the three SDKs (stageB-review.md): reason names, check order,
payload limit, metric labels, partition names."""
import asyncio
import json
import logging
from datetime import date

import pytest

from besdk import errors
from besdk.events.model import Event, Subscription


def test_request_invalid_and_dependency_unavailable_are_be_reasons():
    e = errors.be_error("REQUEST_INVALID")
    assert (e.code, e.http, e.domain) == (errors.Code.INVALID_ARGUMENT, 400, "be")
    e = errors.be_error("DEPENDENCY_UNAVAILABLE", {"dependency": "db"})
    assert (e.code, e.http, e.metadata) == (errors.Code.UNAVAILABLE, 503, {"dependency": "db"})
    p = errors.problem(e, path="/x", request_id="r", trace_id="t", locale="en")
    assert p.body["domain"] == "be" and p.body["reason"] == "DEPENDENCY_UNAVAILABLE"


class _DownPool:
    ready = False
    server_version = 160000

    async def open(self):
        raise ConnectionRefusedError("connect refused")


async def test_unreachable_database_is_dependency_unavailable():
    from besdk.metrics import BeMetrics, ComponentRegistry
    from besdk.store.store import Store

    s = Store.for_member(_DownPool(), member="c/x", role="r", schema="s", owner="o", budget=2,
                         logger=logging.getLogger("t"), metrics=BeMetrics(ComponentRegistry("c/x")))
    with pytest.raises(errors.Error) as ei:
        await s.tx(lambda tx: tx.fetchval("SELECT 1"))
    assert (ei.value.reason, ei.value.domain, ei.value.metadata) == ("DEPENDENCY_UNAVAILABLE", "be", {"dependency": "db"})


def test_tx_retries_metric_is_labelled_by_sqlstate():
    from besdk.metrics import BeMetrics, ComponentRegistry

    m = BeMetrics(ComponentRegistry("c/x"))
    m.tx_retries.labels(sqlstate="40001").inc()


class _Tx:
    def __init__(self, pub):
        self.store = type("S", (), {"publisher": pub})()
        self.rows = []

    async def execute(self, sql, *args):
        self.rows.append(args)


def _publisher(tmp_path):
    from opentelemetry.propagate import get_global_textmap

    from besdk.events.contract import Contract
    from besdk.events.outbox import Publisher

    (tmp_path / "events").mkdir()
    (tmp_path / "events" / "w.events.json").write_text(json.dumps({"events": [
        {"subject": "conformance.widget.created.v1", "x-aggregate-type": "conformance.widget", "payload": {"type": "object"}}]}))
    return Publisher("conformance/widget", "1.0.0", Contract.load(tmp_path), ["conformance.widget.created.v1"],
                     get_global_textmap(), logging.getLogger("t"))


async def test_payload_above_64_kib_is_rejected(tmp_path):
    from besdk.events.outbox import write

    tx = _Tx(_publisher(tmp_path))
    await write(tx, Event("conformance.widget.created.v1", "w1", 1, {"d": "x" * 1000}))
    with pytest.raises(errors.Error) as ei:
        await write(tx, Event("conformance.widget.created.v1", "w1", 2, {"d": "x" * (64 * 1024)}))
    assert "PAYLOAD_TOO_LARGE" in ei.value.internal_message
    assert len(tx.rows) == 1


def test_subscription_may_name_its_aggregate_type():
    s = Subscription("erp.sales.order.confirmed.v1", aggregate_type="erp.sales.order", run=lambda ev: None)
    assert s.aggregate_type == "erp.sales.order"


def test_inbound_aggregate_type_order():
    """contract declaration → Subscription.aggregate_type → ce-aggregatetype header."""
    from besdk.events.consumer import Consumer
    from besdk.events.contract import Contract

    c = Consumer.__new__(Consumer)
    c.member, c.contract = "c/x", Contract({})
    c.sub = Subscription("a.b.c.v1", aggregate_type="a.b", run=lambda ev: None)
    assert c._inbound({"ce-aggregatetype": "z.z"}).aggregate_type == "a.b"
    c.sub = Subscription("a.b.c.v1", run=lambda ev: None)
    assert c._inbound({"ce-aggregatetype": "z.z"}).aggregate_type == "z.z"


def test_outbox_partition_name_is_isoyear_w_week():
    from besdk.migrate import outbox_partition_name

    assert outbox_partition_name(date(2026, 10, 3)) == "besdk_outbox_2026w40"
    assert outbox_partition_name(date(2027, 1, 1)) == "besdk_outbox_2026w53"


def test_access_log_line_for_ok_is_info():
    """rc.2 errors access_log_level: every access-log line is at least info; OK and CANCELLED are info."""
    assert errors.access_log_level(errors.Code.OK) == "info"
    assert errors.access_log_level(errors.Code.CANCELLED) == "info"
    assert errors.access_log_level(errors.Code.INTERNAL) == "error"


def test_cancellation_is_request_cancelled():
    e = errors.to_error(asyncio.CancelledError())
    assert (e.reason, e.domain, e.http) == ("REQUEST_CANCELLED", "be", 499)


async def test_outbox_row_carries_tracestate(tmp_path):
    from besdk.events.outbox import write

    tx = _Tx(_publisher(tmp_path))
    await write(tx, Event("conformance.widget.created.v1", "w1", 1, {"d": 1}))
    assert len(tx.rows[0]) == 13  # … traceparent, tracestate, causation, hop, headers, payload
