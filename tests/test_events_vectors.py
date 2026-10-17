from datetime import datetime

from besdk import protocol
from besdk.events.core import Row, Stream, digest, subject


def test_partition_vectors():
    for c in protocol.vector("partition")["cases"]:
        assert Stream("s", c["partitions"]).partition_of(c["record_id"]) == c["expect"], c["id"]


def test_envelope_vectors():
    for c in protocol.vector("envelope")["cases"]:
        e, x = c["event"], c["expect"]
        payload = e["payload"].encode()
        r = Row(0, e["event_id"], e["stream"], e["partition"], e["type"],
                datetime.fromisoformat(e["time"].replace("Z", "+00:00")), e["record_id"], e["version"], e["projection"],
                payload, digest(e["event_id"], payload), e["seq"], tuple(e["epoch"]), e.get("legal_entity", ""),
                e.get("subjects"), e.get("causation", ""), e.get("hops", 0), claim=e.get("claim", False))
        if e.get("traceparent"):
            r.trace = {"traceparent": e["traceparent"]}
        assert r.digest == x["digest"], c["id"]
        assert r.headers(e["source"], e["impl"]) == x["headers"], c["id"]
        assert subject(r.stream, r.partition, r.type) == x["subject"], c["id"]
