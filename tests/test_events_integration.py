import asyncio
import io
import os
import uuid

import pytest

from besdk import errs, obs
from besdk.events.consumer import Consumer, Sub
from besdk.events.core import Delivery, Event, Outcome, Projection, Stream
from besdk.events.objectstore import HTTPClaims, S3Store
from besdk.events.outbox import Forwarder, Producer, caused_by, number, purge
from besdk.config import Secret
from tests import pgtest
from tests.test_db_integration import migrated, open_db
from tests.test_transports import pg_transport

pytestmark = pytest.mark.integration
LOG = obs.new_logger(obs.Member("demo/ev", "3.0.0"), "error", io.StringIO())
STOCK = Stream("demo.stock", 2)
ITEMS = {"0001_items.up.sql": "CREATE TABLE items (id uuid PRIMARY KEY);"}


async def source(name, claims=None, stream=STOCK):
    v, _ = await migrated(name, ITEMS)
    d, pool = await open_db(name, v)
    return d, pool, Producer("demo/" + name, [stream], claims)


async def emit(d, p, n=1, data=None, fail=False):
    stream = next(iter(p.streams))
    for _ in range(n):
        async def write(tx):
            rid = uuid.uuid7()
            await tx.execute("INSERT INTO items VALUES ($1)", rid)
            await p.emit(tx, stream, Event("demo.stock.moved.v1", str(rid), 1, Projection.FULL,
                                               data if data is not None else {"id": str(rid)}))
            if fail:
                raise errs.new("REQUEST_INVALID", "rolled back")

        await d.tx(write)


async def positions(name, partition):
    a = await pgtest.admin()
    try:
        return [r[0] for r in await a.fetch(f"SELECT seq FROM {name}.be_outbox WHERE partition = $1 AND seq IS NOT NULL "
                                            "ORDER BY seq", partition)]
    finally:
        await a.close()


async def test_emit_only_with_the_business_commit():  # P6.2, CK-EV-01
    d, pool, p = await source("py_ev1")
    try:
        with pytest.raises(errs.Error):
            await emit(d, p, fail=True)
        await emit(d, p)
        a = await pgtest.admin()
        try:
            assert await a.fetchval("SELECT count(*) FROM py_ev1.be_outbox") == 1
        finally:
            await a.close()
    finally:
        await pool.close()


async def test_two_numberers_never_share_a_position():  # P6.5, P6.6, CK-EV-02
    d, pool, p = await source("py_ev2")
    try:
        async def writer():
            await emit(d, p, 15)

        async def numberer():
            for _ in range(40):
                for part in range(STOCK.partitions):
                    await number(d, STOCK.name, part, 7)
                await asyncio.sleep(0.01)

        await asyncio.gather(*(writer() for _ in range(4)), numberer(), numberer())
        for part in range(STOCK.partitions):
            await number(d, STOCK.name, part, 500)
        total = 0
        for part in range(STOCK.partitions):
            seqs = await positions("py_ev2", part)
            assert seqs == list(range(1, len(seqs) + 1)), f"partition {part}: no gap, no repeat"
            total += len(seqs)
        assert total == 60
    finally:
        await pool.close()


async def test_a_new_epoch_restarts_positions_and_records_the_fork():  # P6.7, P6.8, CK-EV-03
    d, pool, p = await source("py_ev3")
    try:
        await emit(d, p, 6)
        for part in range(2):
            await number(d, STOCK.name, part, 500)
        a = await pgtest.admin()
        try:
            await a.execute("UPDATE py_ev3.be_recovery_generation SET generation = generation + 1")
            await emit(d, p, 6)
            for part in range(2):
                await number(d, STOCK.name, part, 500)
            hist = await a.fetch("SELECT partition, old_generation, fork_seq, new_generation FROM py_ev3.be_epoch_history "
                                 "ORDER BY partition")
            assert [(h["old_generation"], h["new_generation"]) for h in hist] == [(1, 2), (1, 2)]
            news = await a.fetch("SELECT partition, min(seq) AS first FROM py_ev3.be_outbox WHERE epoch_generation = 2 "
                                 "GROUP BY partition")
            assert all(r["first"] == 1 for r in news), "the new epoch numbers from 1"
        finally:
            await a.close()
    finally:
        await pool.close()


async def forwarded(d, p, tr, name):
    f = Forwarder(d, p, tr, "demo/" + name, str(uuid.uuid4()), LOG)
    for s in p.streams.values():
        await tr.ensure_stream(s)
    return await f.once()


async def test_the_forwarder_publishes_in_order_once_numbered():  # P6.10
    own = Stream("demo.order" + uuid.uuid4().hex[:6], 2)  # its own stream: the shared queue holds earlier messages
    d, pool, p = await source("py_ev4", stream=own)
    tr = await pg_transport()
    stream = own.name
    try:
        await emit(d, p, 8)
        seen: list[tuple[int, int]] = []

        async def h(dl: Delivery) -> Outcome:
            seen.append((int(dl.headers["ce-bepartition"]), int(dl.headers["ce-beseq"])))
            return Outcome.ACK

        from besdk.events.core import SubscriptionSpec

        sub = await tr.subscribe(SubscriptionSpec("py_ev4_" + uuid.uuid4().hex[:8], [stream]), h)
        assert await forwarded(d, p, tr, "py_ev4") == 8
        assert await forwarded(d, p, tr, "py_ev4") == 0, "published rows are not published again"
        for _ in range(100):
            if len(seen) >= 8:
                break
            await asyncio.sleep(0.05)
        await sub.stop()
        for part in range(2):
            mine = [s for pp, s in seen if pp == part]
            assert mine == sorted(mine) == list(range(1, len(mine) + 1))
        assert await purge(d, 0) >= 8, "published rows past the retention go"
    finally:
        await tr.close()
        await pool.close()


async def consumer_db(name):
    v, _ = await migrated(name, {"0001_seen.up.sql": "CREATE TABLE seen (event_id uuid PRIMARY KEY, size int NOT NULL);"})
    return await open_db(name, v)


async def test_redelivery_once_dead_letters_and_hops():  # P6.15, P6.20, P6.25, CK-EV-07, CK-EV-08
    d, pool = await consumer_db("py_evc1")
    fail = {"on": False}

    async def handle(tx, r):
        if fail["on"]:
            raise RuntimeError("cannot apply")
        await tx.execute("INSERT INTO seen VALUES ($1, $2)", uuid.UUID(r.id), len(r.data))

    c = Consumer("demo/py_evc1", Sub("moves", [STOCK.name], handle), d, LOG)
    eid = str(uuid.uuid7())
    hdr = {"ce-id": eid, "ce-type": "demo.stock.moved.v1", "ce-behops": "0", "ce-beversion": "1", "ce-bepartition": "0",
           "ce-beseq": "1"}
    try:
        for attempt in (1, 2):
            assert await c.handle(Delivery("be.demo.stock.0.x", dict(hdr), b"{}", attempt)) == Outcome.ACK
        a = await pgtest.admin()
        try:
            assert await a.fetchval("SELECT count(*) FROM py_evc1.seen") == 1, "a redelivery takes effect once"
        finally:
            await a.close()
        fail["on"] = True
        other = {**hdr, "ce-id": str(uuid.uuid7())}
        assert await c.handle(Delivery("s", dict(other), b"{}", 1)) == Outcome.NAK
        assert await c.handle(Delivery("s", dict(other), b"{}", 3)) == Outcome.TERM
        assert await c.handle(Delivery("s", dict(other), b"{}", 3)) == Outcome.TERM
        fail["on"] = False
        loop = {**hdr, "ce-id": str(uuid.uuid7()), "ce-behops": "11"}
        assert await c.handle(Delivery("s", dict(loop), b"{}", 3)) == Outcome.TERM, "more than 10 hops is a failure"
        a = await pgtest.admin()
        try:
            assert await a.fetchval("SELECT count(*) FROM py_evc1.be_dead_letter") == 2, "one dead letter per event"
        finally:
            await a.close()
    finally:
        await pool.close()


async def test_emitting_while_handling_counts_hops():  # P6.15
    d, pool, p = await source("py_ev5")
    try:
        with caused_by(str(uuid.uuid7()), 10):
            with pytest.raises(errs.Error, match="INTERNAL"):
                await emit(d, p)
        with caused_by(cause := str(uuid.uuid7()), 2):
            await emit(d, p)
        a = await pgtest.admin()
        try:
            row = await a.fetchrow("SELECT causation::text AS c, hops FROM py_ev5.be_outbox")
            assert (row["c"], row["hops"]) == (cause, 3)
        finally:
            await a.close()
    finally:
        await pool.close()


async def test_large_payloads_travel_by_claim_check():  # P6.14, CK-EV-06
    url, access, secret = (os.environ.get(k, "") for k in ("BE_SDK_TEST_S3_URL", "BE_SDK_TEST_S3_ACCESS_KEY",
                                                           "BE_SDK_TEST_S3_SECRET_KEY"))
    assert url and access and secret, "BE_SDK_TEST_S3_* must name object storage"
    store = S3Store(url, "us-east-1", "be-sdk-test", "py-claims/", access, Secret(secret))
    own = Stream("demo.claims" + uuid.uuid4().hex[:6], 1)  # its own stream: the shared queue holds other tests' messages
    d, pool, p = await source("py_ev6", store, own)
    cd, cpool = await consumer_db("py_evc6")
    tr = await pg_transport()
    claims = HTTPClaims()
    try:
        big = "x" * (64 * 1024 + 10)
        await emit(d, p, data={"blob": big})
        got: list[bytes] = []

        async def handle(tx, r):
            got.append(r.data)
            await tx.execute("INSERT INTO seen VALUES ($1, $2)", uuid.UUID(r.id), len(r.data))

        c = Consumer("demo/py_evc6", Sub("moves" + uuid.uuid4().hex[:6], [own.name], handle), cd, LOG, claims)
        small: list[int] = []

        async def h(dl):
            assert dl.headers.get("ce-beclaim") == "true"
            small.append(len(dl.data))
            return await c.handle(dl)

        sub = await tr.subscribe(c.spec(), h)
        assert await forwarded(d, p, tr, "py_ev6") == 1
        for _ in range(100):
            if got:
                break
            await asyncio.sleep(0.05)
        await sub.stop()
        assert small and small[0] < 1000, "the message carries the reference only"
        assert got and b'"blob":"' + big.encode() in got[0]
    finally:
        await asyncio.gather(claims.close(), tr.close(), pool.close(), cpool.close())


class SlowClaims:
    """Object storage that takes a while per upload and records how many uploads overlapped."""

    def __init__(self, delay: float):
        self.delay, self.now, self.peak = delay, 0, 0

    async def put(self, event_id: str, data: bytes) -> str:
        self.now += 1
        self.peak = max(self.peak, self.now)
        await asyncio.sleep(self.delay)
        self.now -= 1
        return "s3://claims/" + event_id


async def test_claim_uploads_overlap_and_publishing_keeps_its_order():  # P6.10, P6.14
    from besdk.events.outbox import CLAIM_CONCURRENCY

    claims, n = SlowClaims(0.04), 12
    own = Stream("demo.slowclaims" + uuid.uuid4().hex[:6], 1)
    d, pool, p = await source("py_ev9", claims, own)
    tr = await pg_transport()
    try:
        for _ in range(n):
            await emit(d, p, data={"blob": "x" * (64 * 1024 + 10)})
        seen: list[int] = []

        async def h(dl: Delivery) -> Outcome:
            seen.append(int(dl.headers["ce-beseq"]))
            return Outcome.ACK

        from besdk.events.core import SubscriptionSpec

        sub = await tr.subscribe(SubscriptionSpec("py_ev9_" + uuid.uuid4().hex[:8], [own.name]), h)
        started = asyncio.get_running_loop().time()
        assert await forwarded(d, p, tr, "py_ev9") == n
        took = asyncio.get_running_loop().time() - started
        for _ in range(100):
            if len(seen) >= n:
                break
            await asyncio.sleep(0.05)
        await sub.stop()
        assert 1 < claims.peak <= CLAIM_CONCURRENCY, "uploads of one batch overlap, within the bound"
        assert took < n * claims.delay / 2, "one upload at a time caps a partition at 1/delay events a second"
        assert seen == sorted(seen) == list(range(1, n + 1)), "published in position order"
    finally:
        await tr.close()
        await pool.close()


async def test_one_s3_client_while_the_secret_stays():  # P6.14
    url, access, secret = (os.environ.get(k, "") for k in ("BE_SDK_TEST_S3_URL", "BE_SDK_TEST_S3_ACCESS_KEY",
                                                           "BE_SDK_TEST_S3_SECRET_KEY"))
    assert url and access and secret, "BE_SDK_TEST_S3_* must name object storage"

    class Rotating:
        def __init__(self, v):
            self.v = v

        def value(self):
            return self.v

    s = Rotating(secret)
    store = S3Store(url, "us-east-1", "be-sdk-test", "py-clients/", access, s)
    try:
        for _ in range(3):
            await store.put(str(uuid.uuid4()), b"{}")
        assert store.clients_built == 1, "building a client per upload cost more than the upload"
        s.v = secret + " "  # a rotated secret file: a new client (that this one is wrong does not matter here)
        await store._s3()
        assert store.clients_built == 2
    finally:
        await store.close()


async def test_the_commit_wakes_the_forwarder():  # P6.29
    # at a low rate every event used to wait for the next poll (about 120 ms at 50/s with a 250 ms poll); the commit
    # that wrote it now wakes the member's own forwarder
    from besdk.events.core import SubscriptionSpec

    own = Stream("demo.wake" + uuid.uuid4().hex[:6], 1)
    d, pool, p = await source("py_ev10", stream=own)
    tr = await pg_transport()
    f = Forwarder(d, p, tr, "demo/py_ev10", str(uuid.uuid4()), LOG)
    f.poll = 10.0
    got = asyncio.Event()

    async def h(dl: Delivery) -> Outcome:
        got.set()
        return Outcome.ACK

    sub = await tr.subscribe(SubscriptionSpec("py_ev10_" + uuid.uuid4().hex[:8], [own.name]), h)
    await f.start()
    try:
        await asyncio.sleep(0.3)  # the forwarder's first round is over; the next poll is 10 s away
        written = asyncio.get_running_loop().time()
        await emit(d, p, 1)
        await asyncio.wait_for(got.wait(), 5)
        assert asyncio.get_running_loop().time() - written < 1, "published right after the commit"
    finally:
        await f.stop()
        await sub.stop()
        await tr.close()
        await pool.close()
