import io
import os
import time
import uuid
from urllib.parse import urlsplit

import pytest

from besdk import obs
from besdk.events import jetstream, pgqueue
from besdk.events.core import Outcome
from tests import pgtest, transport_suite

pytestmark = pytest.mark.integration
LOG_BUS = obs.new_logger(obs.Member("bus", "0"), "error", io.StringIO())


def bus_url() -> str:
    u = urlsplit(pgtest.admin_dsn(pgtest.DATABASE))
    return u._replace(scheme="pgqueue", query="schema=py_bus_test&sslmode=disable").geturl()


async def pg_transport():
    await pgtest.ensure_database(pgtest.DATABASE)
    a = await pgtest.admin()
    try:
        await a.execute("CREATE SCHEMA IF NOT EXISTS py_bus_test")
    finally:
        await a.close()
    await pgqueue.migrate_bus(bus_url(), LOG_BUS)
    return await pgqueue.Transport.connect(bus_url(), "be-sdk-python-test")


async def js_transport():
    url = os.environ.get("BE_SDK_TEST_NATS_URL", "")
    assert url, "BE_SDK_TEST_NATS_URL is not set: the JetStream tests need a NATS server"
    return await jetstream.Transport.connect(url, "be-sdk-python-test")


class LosesAcks:
    """A broken adapter: it reports every handler outcome as acknowledged but never settles a nak (P6.24)."""

    def __init__(self, inner):
        self.inner = inner

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def subscribe(self, spec, h):
        async def swallow(d):
            await h(d)
            return Outcome.ACK  # the outcome is lost: a nak is never redelivered

        return await self.inner.subscribe(spec, swallow)


@pytest.mark.parametrize("case", transport_suite.CASES, ids=lambda c: c.__name__)
@pytest.mark.parametrize("adapter", ["jetstream", "pgqueue"])
async def test_transport_suite(adapter, case):  # P6.16-P6.24, CK-BUS-01..08
    tr = await (js_transport() if adapter == "jetstream" else pg_transport())
    stream = "pysuite.s" + uuid.uuid4().hex[:10]
    try:
        if adapter == "jetstream" and case is not transport_suite.a_subscriber_may_start_before_any_producer:
            from besdk.events.core import Stream

            await tr.ensure_stream(Stream(stream, 4))
        await transport_suite.run(case, tr, stream)
    finally:
        if adapter == "jetstream":
            try:
                await tr.delete_stream(stream)
            except Exception:  # noqa: BLE001
                pass
        await tr.close()


async def test_the_suite_fails_an_adapter_that_loses_acks():  # P6.24
    tr = await pg_transport()
    try:
        with pytest.raises(AssertionError):
            await transport_suite.run(transport_suite.a_nak_is_redelivered_after_the_backoff, LosesAcks(tr),
                                      "pysuite.s" + uuid.uuid4().hex[:10])
    finally:
        await tr.close()


async def backlog(n: int):
    """A new subscription with n due deliveries of real messages, written directly."""
    from besdk.events.core import SubscriptionSpec

    s = SubscriptionSpec("backlog_" + uuid.uuid4().hex[:8], ["x"], max_deliver=3).defaults()
    a = await pgtest.admin()
    try:
        await a.execute("""INSERT INTO py_bus_test.be_bus_subscription (durable, streams, filters, max_deliver,
            ack_wait_ms, backoff_ms) VALUES ($1, '{x}', '{}', 3, 30000, '{1000}')""", s.durable)
        await a.execute("""WITH m AS (INSERT INTO py_bus_test.be_bus_message (subject, headers, data)
                SELECT 'be.x.0.t.v1', '{}'::jsonb, '\\x7b7d'::bytea FROM generate_series(1, $2) RETURNING seq)
            INSERT INTO py_bus_test.be_bus_delivery (durable, seq) SELECT $1, seq FROM m""", s.durable, n)
    finally:
        await a.close()
    return s


async def forget(durable: str, subject: str) -> None:
    """Remove what a test wrote straight into the shared test bus: every later subscription would read past it."""
    a = await pgtest.admin()
    try:
        await a.execute("DELETE FROM py_bus_test.be_bus_subscription WHERE durable = $1", durable)
        await a.execute("DELETE FROM py_bus_test.be_bus_message WHERE subject = $1", subject)
    finally:
        await a.close()


async def median_claim(tr, s) -> float:
    took = []
    for _ in range(15):
        started = time.perf_counter()
        got = await tr._claim(s)
        took.append(time.perf_counter() - started)
        assert got is not None
    return sorted(took)[len(took) // 2]


async def test_a_backlog_does_not_slow_each_claim():  # P6.23
    # every claim used to scan the subscription's whole backlog first: the more behind, the slower each message,
    # so a subscriber that once fell behind could not catch up
    tr = await pg_transport()
    subs = []
    try:
        subs = [await backlog(20), await backlog(20000)]
        small, large = await median_claim(tr, subs[0]), await median_claim(tr, subs[1])
        assert large < 3 * small + 0.002, f"claims with 20 000 waiting ({large * 1000:.1f} ms) against 20 ({small * 1000:.1f} ms)"
    finally:
        await tr.close()
        for s in subs:
            await forget(s.durable, "be.x.0.t.v1")


async def test_a_claim_whose_message_was_purged_is_dropped():  # P6.27
    tr = await pg_transport()
    try:
        s = await backlog(2)
        a = await pgtest.admin()
        cleanup = s.durable
        try:
            first = await a.fetchval("SELECT min(seq) FROM py_bus_test.be_bus_delivery WHERE durable = $1", s.durable)
            await a.execute("DELETE FROM py_bus_test.be_bus_message WHERE seq = $1", first)
            got = None
            for _ in range(3):  # the orphan is dropped on the way; the real message follows
                if (got := await tr._claim(s)) is not None:
                    break
            assert got is not None, "the delivery with a message comes through"
            assert await a.fetchval("SELECT count(*) FROM py_bus_test.be_bus_delivery WHERE durable = $1 AND seq = $2",
                                    s.durable, first) == 0, "the delivery of the purged message is gone"
        finally:
            await a.close()
            await forget(cleanup, "be.x.0.t.v1")
    finally:
        await tr.close()


async def test_a_subscription_crosses_other_streams_without_waiting():  # P6.23, P6.28
    # the queue is shared by every stream: a subscription reads past every message to keep its position, at most a
    # batch per round; waiting a poll after each round that delivered nothing capped that at batch/poll a second
    import asyncio

    from besdk.events.core import Stream, SubscriptionSpec

    tr = await pg_transport()
    stream = "cross" + uuid.uuid4().hex[:8]
    other = f"be.other{stream}.0.t.v1"
    a = await pgtest.admin()
    try:
        await a.execute("""INSERT INTO py_bus_test.be_bus_message (subject, headers, data)
            SELECT $1, '{}'::jsonb, '\\x7b7d'::bytea FROM generate_series(1, 20000)""", other)
    finally:
        await a.close()
    durable = "cross_" + stream
    try:
        await tr.ensure_stream(Stream(stream, 1))
        got = asyncio.Event()

        async def h(d):
            got.set()
            return Outcome.ACK

        started = time.perf_counter()
        sub = await tr.subscribe(SubscriptionSpec(durable, [stream]), h)
        from besdk.events.core import Message

        await tr.publish(Message(f"be.{stream}.0.x.v1", {"ce-id": str(uuid.uuid4())}, b"{}"))
        await asyncio.wait_for(got.wait(), 10)
        took = time.perf_counter() - started
        await sub.stop()
        assert took < 2, f"{took:.1f} s past 20 000 messages of another stream"
    finally:
        await tr.close()
        await forget(durable, other)


async def test_a_publish_wakes_an_idle_reader_without_waiting_for_the_poll():  # P6.30
    import asyncio

    from besdk.events.core import Message, Stream, SubscriptionSpec

    tr = await pg_transport()
    tr.poll = 3600.0  # only the wake can pick the message up
    stream = "wake" + uuid.uuid4().hex[:8]
    subject = f"be.{stream}.0.x.v1"
    durable = "wake_" + stream
    try:
        await tr.ensure_stream(Stream(stream, 1))
        got: asyncio.Queue[float] = asyncio.Queue()

        async def h(d):
            got.put_nowait(time.perf_counter())
            return Outcome.ACK

        sub = await tr.subscribe(SubscriptionSpec(durable, [stream]), h)
        await asyncio.sleep(0.5)  # listening, and the reader has read past the bus and is idle
        for i in range(2):
            published = time.perf_counter()
            await tr.publish(Message(subject, {"ce-id": str(uuid.uuid4())}, b"{}"))
            try:
                at = await asyncio.wait_for(got.get(), 10)
            except TimeoutError:
                pytest.fail(f"message {i} was not delivered without a poll")
            assert at - published < 2, f"message {i}: delivered on the wake"
        await sub.stop()
    finally:
        await tr.close()
        await forget(durable, subject)
