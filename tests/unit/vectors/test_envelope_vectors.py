"""be-protocol vectors `envelope` (P11.5, P12): ids, headers, derivation, acceptance, cursor, redelivery, names."""
import pytest

from besdk import ids
from besdk.config_values import parse_duration_ns
from besdk.events import envelope as env
from tests.unit.vectors._load import cases, expect


@pytest.mark.parametrize("case", cases("envelope", "ids"))
def test_uuid7(case):
    def run():
        u = ids.parse_id(case["input"]["id"])
        return {"canonical": str(u), "unix_ms": ids.id_unix_ms(u), "created_at": ids.rfc3339_ms(ids.id_time(u))}

    expect(case, run)


@pytest.mark.parametrize("case", cases("envelope", "headers"))
def test_envelope_headers(case):
    i = case["input"]
    p, r = i["producer"], i["row"]
    row = env.OutboxRow(id=r["id"], subject=r["subject"], aggregate_type=r["aggregate_type"],
                        aggregate_id=r["aggregate_id"], aggregate_version=r["aggregate_version"],
                        occurred_at=r["occurred_at"], traceparent=r["traceparent"], causation_id=r["causation_id"],
                        hop_count=r["hop_count"], payload_json=r["payload_json"],
                        tracestate=r.get("tracestate", ""))
    td = bool((i.get("contract") or {}).get("transaction_document"))
    expect(case, lambda: {"headers": env.headers_of(row, component_id=p["component_id"], version=p["version"],
                                                    events_file=p["events_file"], transaction_document=td)})


def _ctx(c):
    h = c.get("handled")
    j = c.get("job")
    return env.derive(c["kind"], handled=(h["id"], h["hop_count"]) if h else None,
                      job=(j["causation_id"], j["hop_count"]) if j else None)


@pytest.mark.parametrize("case", cases("envelope", "derive", "derive"))
def test_derive(case):
    def run():
        cid, hop = _ctx(case["input"]["context"])
        return {"causation_id": cid, "hop_count": hop}

    expect(case, run)


@pytest.mark.parametrize("case", cases("envelope", "derive", "enqueue_context"))
def test_enqueue_context(case):
    def run():
        cid, hop = _ctx(case["input"]["context"])
        return {"job_causation_id": cid, "job_hop_count": hop}

    expect(case, run)


@pytest.mark.parametrize("case", cases("envelope", "inbound"))
def test_accept(case):
    i = case["input"]
    s = i["subscription"]
    sub = env.Inbound(component_id=s["component_id"], subject=s["subject"], aggregate_type=s["aggregate_type"],
                      transaction_document=s["transaction_document"])

    def run():
        out = env.accept(sub, i["headers"], i["payload_json"].encode(), i["delivery"])
        if out.dlq_reason:
            return {"action": "dlq", "dlq_subject": out.dlq_subject, "added_headers": out.dlq_headers}
        e = out.event
        return {"action": "handle", "event": {
            "id": e.id, "subject": e.subject, "source": e.source, "aggregate_type": e.aggregate_type,
            "aggregate_id": e.aggregate_id, "version": e.version, "hop_count": e.hop_count,
            "causation_id": e.causation_id, "occurred_at": env.ce_time(e.occurred_at),
            "legal_entity": e.legal_entity, "delivery": e.delivery}}

    expect(case, run)


@pytest.mark.parametrize("case", cases("envelope", "cursor", "cursor_sequence"))
def test_cursor_sequence(case):
    i = case["input"]

    def run():
        cur, applied = i["start"], []
        for v in i["versions"]:
            if env.cursor_applies(cur, v):
                applied.append(v)
                cur = v
        return {"applied": applied, "final": cur}

    expect(case, run)


@pytest.mark.parametrize("case", cases("envelope", "cursor", "redelivery"))
def test_redelivery(case):
    i = case["input"]
    backoff = [parse_duration_ns(b) for b in i["backoff"]]

    def run():
        d = env.redelivery(i["delivery"], i["max_deliver"], backoff, i["outcome"], i["durable"], i["stream_seq"])
        out = {"action": d.action, "handled": d.handled}
        if d.action == "nak":
            out["delay"] = i["backoff"][backoff.index(d.delay_ns)]
        if d.action == "dlq":
            out.update(reason=d.reason, dlq_msg_id=d.dlq_msg_id)
        return out

    expect(case, run)


@pytest.mark.parametrize("case", cases("envelope", "names", "stream"))
def test_stream(case):
    s = case["input"]["subject"]
    expect(case, lambda: {"stream": env.stream_of(s), "filter": env.stream_filter(s)})


@pytest.mark.parametrize("case", cases("envelope", "names", "durable"))
def test_durable(case):
    i = case["input"]

    def run():
        d = env.durable_name(i["component_id"], i["subject"])
        return {"durable": d, "dlq_subject": env.dlq_subject(d, i["subject"])}

    expect(case, run)
