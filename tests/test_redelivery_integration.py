"""The consumer side of a redelivery (P6.36, P6.37, P6.39, P6.40): the progress read from the inbox's source positions
and the subscription's runs equals the vector's rule; the first start records the database epoch; after the database
went back the consumer asks each producer it has seen, from the smallest progress of its subscriptions, records the
partitions a redelivery cannot cover and settles only once every producer answered; a producer without an address is
logged."""
import datetime as dt
import io

import pytest

from besdk import obs
from besdk._gen.be.source.v1 import resend_pb2
from besdk.events.progress import Handled, Point, SourceKey, progress, progress_of
from besdk.events.redelivery import RESEND_METHOD, Redelivery
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
T0 = dt.datetime(2026, 11, 1, 8, 0, tzinfo=dt.UTC)


async def consumer_db(name):
    v, _ = await migrated(name, {"0001_m.up.sql": "CREATE TABLE IF NOT EXISTS marker (id int);"})
    d, pool = await open_db(name, v)
    a = await pgtest.admin()
    await a.execute(f"DELETE FROM {name}.be_inbox; DELETE FROM {name}.be_subscription_run; "
                    f"DELETE FROM {name}.be_consumer_epoch; DELETE FROM {name}.be_resend_gap")
    return d, pool, a


async def handled(a, name, sub, component, part, seq, minutes, impl="impl-1", stream="demo.stock"):
    await a.execute(f"""INSERT INTO {name}.be_inbox (subscription, event_id, processed_at, source_component, source_impl,
        source_stream, source_partition, epoch_generation, epoch_timeline, seq)
        VALUES ($1, gen_random_uuid(), $2, $3, $4, $5, $6, 1, 1, $7)""", sub, T0 + dt.timedelta(minutes=minutes),
                    component, impl, stream, part, seq)


class Channel:
    """A producer's system surface that answers Resend and keeps the requests."""

    def __init__(self, beyond=(), ahead=(), fail=False):
        self.got, self.beyond, self.ahead, self.fail = [], list(beyond), list(ahead), fail

    def unary_unary(self, method, request_serializer, response_deserializer):
        assert method == RESEND_METHOD

        async def call(req, metadata=None):
            if self.fail:
                raise RuntimeError("unavailable")
            self.got.append(req)
            return resend_pb2.ResendResponse(resent=3, beyond_retention=self.beyond, ahead=self.ahead)
        return call


async def test_the_progress_read_from_the_inbox():  # P6.36, P6.37
    d, pool, a = await consumer_db("py_rd1")
    try:
        await a.execute("INSERT INTO py_rd1.be_subscription_run (subscription, started_at) VALUES ('s1', $1)", T0)
        rows = []
        for i in range(1, 16):  # one handling a minute for 15 minutes; L = 10 minutes
            await handled(a, "py_rd1", "s1", "demo/src", 0, i, i)
            rows.append(Handled(SourceKey("demo/src", "impl-1", "demo.stock", 0), Point(1, 1, i),
                                T0 + dt.timedelta(minutes=i)))
        got = await progress(d, "s1", 600)
        assert got == progress_of([T0], rows, 600) == {SourceKey("demo/src", "impl-1", "demo.stock", 0): Point(1, 1, 5)}
    finally:
        await a.close()
        await pool.close()


async def test_after_the_database_went_back_every_producer_is_asked():  # P6.39, P6.40
    d, pool, a = await consumer_db("py_rd2")
    try:
        await a.execute("INSERT INTO py_rd2.be_subscription_run (subscription, started_at) VALUES ('s1', $1), ('s2', $1)",
                        T0)
        for i in range(1, 16):
            await handled(a, "py_rd2", "s1", "demo/src", 0, i, i)
            if i > 2:
                await handled(a, "py_rd2", "s2", "demo/src", 0, i - 2, i)  # s2 two positions behind s1
        for i in range(1, 16):
            await handled(a, "py_rd2", "s1", "demo/src", 1, i, i)
        await handled(a, "py_rd2", "s2", "demo/other", 0, 1, 1)
        log = io.StringIO()
        src, other = Channel(beyond=[1]), None
        r = Redelivery(d, lambda c: {"demo/src": src, "demo/other": other}[c],
                       obs.new_logger(obs.Member("demo/rd2", "3.0.0"), "info", log), lifetime=600)
        assert (await r.went_back())[0] is False, "the first start only records the epoch"
        assert await a.fetchval("SELECT count(*) FROM py_rd2.be_consumer_epoch") == 1
        await a.execute("UPDATE py_rd2.be_recovery_generation SET generation = generation + 1")
        try:
            back, cur = await r.went_back()
            assert back, "the database went back"
            await r.run()
            assert len(src.got) == 1 and src.got[0].stream == "demo.stock"
            starts = {f.partition: (f.after.impl, f.after.seq) for f in getattr(src.got[0], "from")}
            assert starts == {0: ("impl-1", 3)}, "partition 0: the smaller of s1's 5 and s2's 3; 1: s2 has none there"
            gaps = await a.fetch("SELECT subscription, source_partition, kind, progress FROM py_rd2.be_resend_gap "
                                 "ORDER BY subscription")
            assert [(g[0], g[1], g[2]) for g in gaps] == [("s1", 1, "beyond_retention"), ("s2", 1, "beyond_retention")]
            assert gaps[0][3] == "impl-1/1.1/5" and gaps[1][3] is None
            assert "event_resend_unreachable" in log.getvalue(), "demo/other has no address"
            assert (await r.went_back())[0] is False, "settled once every producer answered"
        finally:
            await a.execute("UPDATE py_rd2.be_recovery_generation SET generation = generation - 1")
    finally:
        await a.close()
        await pool.close()


async def test_a_producer_that_does_not_answer_is_asked_again_before_settling():  # P6.39
    d, pool, a = await consumer_db("py_rd3")
    try:
        await handled(a, "py_rd3", "s1", "demo/src", 0, 1, 1)
        r = Redelivery(d, lambda c: Channel(fail=True), obs.new_logger(obs.Member("demo/rd3", "3.0.0"), "error",
                                                                       io.StringIO()))
        await r.went_back()
        await a.execute("UPDATE py_rd3.be_recovery_generation SET generation = generation + 1")
        try:
            with pytest.raises(RuntimeError, match="unavailable"):
                await r.request()
            assert (await r.went_back())[0], "not settled: the next start asks again"
        finally:
            await a.execute("UPDATE py_rd3.be_recovery_generation SET generation = generation - 1")
    finally:
        await a.close()
        await pool.close()
