"""The producer's redelivery (P6.38) and the purge's position (P6.12): the published rows after the asked position
again, in position order, the event's identity kept and a message identity of their own; another implementation
instance's position or none starts at the first row kept; a start past the counter or in an epoch the partition never
had is ahead; rows purged after the start are beyond the retention."""
import pytest

from besdk import errs
from besdk._gen.be.source.v1 import resend_pb2, watermark_pb2
from besdk.events.outbox import Forwarder, purge
from besdk.events.resend import Resender
from tests import pgtest
from tests.test_events_integration import LOG, emit, source

pytestmark = pytest.mark.integration
IMPL = "impl-a"


class Recording:
    """A transport that keeps what it is given."""

    def __init__(self):
        self.sent = []

    async def ensure_stream(self, s):
        return None

    async def publish(self, m):
        self.sent.append(m)


def request(after=None, stream="demo.stock", rid="r-1"):
    req = resend_pb2.ResendRequest(stream=stream, request_id=rid)
    for n, (impl, epoch, seq) in (after or {}).items():
        getattr(req, "from").append(resend_pb2.PartitionFrom(partition=n, after=watermark_pb2.Position(
            impl=impl, epoch=epoch, seq=seq)))
    return req


async def published(name, d, p, n=6):
    a = await pgtest.admin()
    await a.execute(f"DELETE FROM {name}.be_outbox; DELETE FROM {name}.be_outbox_partition; "
                    f"DELETE FROM {name}.be_epoch_history")
    await a.close()
    await emit(d, p, n)
    first = Recording()
    await Forwarder(d, p, first, "demo/" + name, IMPL, LOG).once()
    return first.sent


async def epoch_of(name):
    a = await pgtest.admin()
    try:
        await a.execute(f"SET search_path = {name}")  # the function reads the schema's tables by name
        r = await a.fetchrow("SELECT generation, timeline FROM be_current_db_epoch()")
        return f"{r[0]}.{r[1]}"
    finally:
        await a.close()


async def test_a_redelivery_sends_after_the_start_in_order():  # P6.38
    d, pool, p = await source("py_rs1")
    try:
        original = await published("py_rs1", d, p)
        by_part = {}
        for m in original:
            by_part.setdefault(int(m.headers["ce-bepartition"]), []).append(m)
        assert set(by_part) == {0, 1} and all(len(v) >= 1 for v in by_part.values()), "both partitions have rows"
        e = await epoch_of("py_rs1")
        out = Recording()
        r = Resender(d, p, out, "demo/py_rs1", IMPL, LOG)
        resp = await r.Resend(request({0: (IMPL, e, 1)}), None)
        want = [m for m in by_part[0] if int(m.headers["ce-beseq"]) > 1] + by_part[1]
        assert resp.resent == len(want) and not resp.ahead and not resp.beyond_retention
        assert [m.id for m in out.sent] == [m.id for m in want], "partition by partition, in position order"
        assert all(m.msg_id == f"{m.id}/resend/r-1" and m.dedup_id != m.id for m in out.sent), "a message ID of their own"
        assert [m.headers for m in out.sent] == [m.headers for m in want], "the event's headers unchanged"

        out = Recording()
        resp = await Resender(d, p, out, "demo/py_rs1", IMPL, LOG).Resend(request({0: ("another-impl", e, 99)}), None)
        assert resp.resent == len(original), "another implementation instance's position: from the first row kept"
    finally:
        await pool.close()


async def test_ahead_and_beyond_the_retention():  # P6.38, P6.12
    d, pool, p = await source("py_rs2")
    try:
        original = await published("py_rs2", d, p)
        e = await epoch_of("py_rs2")
        last0 = max(int(m.headers["ce-beseq"]) for m in original if m.headers["ce-bepartition"] == "0")
        out = Recording()
        r = Resender(d, p, out, "demo/py_rs2", IMPL, LOG)
        resp = await r.Resend(request({0: (IMPL, e, last0 + 5), 1: (IMPL, "999.1", 1)}), None)
        assert list(resp.ahead) == [0, 1], "past the counter; an epoch the partition never had"
        assert resp.resent == 0 and out.sent == []

        a = await pgtest.admin()
        await a.execute("UPDATE py_rs2.be_outbox SET published_at = now() - interval '30 days' WHERE partition = 0 "
                        "AND seq <= 2")
        await a.close()
        assert await purge(d, retention=3600) == 2
        a = await pgtest.admin()
        purged = await a.fetchval("SELECT purged_seq FROM py_rs2.be_outbox_partition WHERE stream = 'demo.stock' "
                                  "AND partition = 0")
        await a.close()
        assert purged == 2, "the purge notes the largest position it deleted"
        resp = await r.Resend(request({0: (IMPL, e, 1)}), None)
        assert 0 in resp.beyond_retention, "rows after the start were purged"
        resp = await r.Resend(request({0: (IMPL, e, 2)}), None)
        assert 0 not in resp.beyond_retention, "a start at the purged position misses nothing"
        resp = await r.Resend(request(), None)
        assert list(resp.beyond_retention) == [0], "no start, and the partition was purged"
        with pytest.raises(errs.Error) as ex:
            await r.Resend(request(stream="demo.missing"), None)
        assert ex.value.reason == "REQUEST_INVALID"
    finally:
        await pool.close()
