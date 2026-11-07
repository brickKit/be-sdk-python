"""The watermark source (P9.13-P9.18) and its record reads (P9.30-P9.33), the same cases as be-sdk-go's
events/watermark_integration_test.go and events/records_integration_test.go: only the declared pullers and readers;
a pull's echo comes after the changes committed before it, a long transaction delays only itself, the pages end with
the echo; the row at the consumer's position is checked; the step before the echo runs first; a read answers every
ID in order with the partitions' high points in its snapshot; a snapshot pages by ID, its page token bound to the
epoch; a big record is a claim check. The servicers are called with the caller the system surface would set (its
caller check is the chain's, tested in test_system)."""
import json
import secrets
import uuid

import pytest

from besdk import errs, system
from besdk._gen.be.source.v1 import records_pb2, watermark_pb2
from besdk.events.core import MAX_PAYLOAD, Event, Projection, Stream
from besdk.events.objectstore import sha256_hex
from besdk.events.outbox import Producer, number
from besdk.events.records import RecordSource
from besdk.events.sources import EXISTS, PURGED, RECORD_PAGE, TOMBSTONE, ReadResult, Record, Records, Watermark
from besdk.events.watermark import ECHO_TYPE, PULL_PAGE, WatermarkSource
from tests import pgtest
from tests.test_db_integration import migrated, open_db
from tests.test_events_integration import LOG

pytestmark = pytest.mark.integration
ROWS = {"0001_rows.up.sql": """CREATE TABLE rows (id text PRIMARY KEY, version bigint NOT NULL, data jsonb NOT NULL,
    deleted boolean NOT NULL);"""}


def table_records(*readers):
    """Record reads over the rows table; an ID that is not there is purged."""
    async def read(tx, ids):
        out = []
        for i in ids:
            r = await tx.fetchrow("SELECT version, data::text AS data, deleted FROM rows WHERE id = $1", i)
            out.append(ReadResult(i, PURGED) if r is None else ReadResult(i, TOMBSTONE, r["version"]) if r["deleted"]
                       else ReadResult(i, EXISTS, r["version"], json.loads(r["data"])))
        return out

    async def page(tx, after, limit):
        rows = await tx.fetch("""SELECT id, version, data::text AS data FROM rows WHERE NOT deleted
            AND id COLLATE "C" > $1 ORDER BY id COLLATE "C" LIMIT $2""", after, limit)
        return [Record(r["id"], r["version"], json.loads(r["data"])) for r in rows]

    return Records("demo/wmsrc.read", list(readers), read, page)


def people(before=None):
    return Stream("demo.people", 1, watermark=Watermark("demo/wmsrc.pull", ["demo/consumer"], before),
                  records=table_records("demo/consumer"))


class Claims:
    def __init__(self):
        self.objs = {}

    async def put(self, key, data):
        uri = f"s3://claims/{key}"
        self.objs[uri] = data
        return uri


async def setup(name, *streams, claims=None):
    v, _ = await migrated(name, ROWS)
    d, pool = await open_db(name, v)
    p = Producer("demo/wmsrc", list(streams), claims)
    return d, pool, p


def as_caller(who):
    return system._caller.set(who)


def nonce():
    return secrets.token_hex(16)


async def pull(src, who, req):
    """Calls the source as who and follows its pages."""
    token = as_caller(who)
    try:
        pages = []
        while True:
            resp = await src.Pull(req, None)
            pages.append(resp)
            if not resp.next_page_token:
                return pages
            req = watermark_pb2.PullRequest(page_token=resp.next_page_token)
    finally:
        system._caller.reset(token)


def pulled(pages):
    """Partition 0's records and types over the pages, its echo and what it needs."""
    recs, types, echo, need = [], [], None, watermark_pb2.NEED_SNAPSHOT_UNSPECIFIED
    for pg in pages:
        for r in pg.partitions:
            for e in r.events:
                recs.append(e.headers["ce-subject"])
                types.append(e.headers["ce-type"])
            if r.HasField("echo"):
                echo = r.echo
            if r.need_snapshot:
                need = r.need_snapshot
    return recs, types, echo, need


async def emit_person(tx, p):
    rid = str(uuid.uuid7())
    await p.emit(tx, "demo.people", Event("demo.person.changed.v1", rid, 1, Projection.FULL, {"id": rid},
                                          subjects=["person:" + rid]))
    return rid


async def emit_people(d, p, n):
    out = []
    for _ in range(n):
        async def write(tx):
            out.append(await emit_person(tx, p))
        await d.tx(write)
    return out


def source(d, p):
    return WatermarkSource(d, p, "demo/wmsrc", "impl-1", "dep-1", LOG)


def at(echo, seq, row):
    return watermark_pb2.PullRequest(stream="demo.people", nonce=nonce(), partitions=[watermark_pb2.PartitionPosition(
        partition=0, position=watermark_pb2.Position(impl=echo.impl, epoch=echo.epoch, seq=seq), row=row)])


def record_row(rid, version, digest):
    return watermark_pb2.RowIdentity(record=watermark_pb2.RowIdentity.Record(record_id=rid, version=version,
                                                                             digest=digest))


def echo_row(n):
    return watermark_pb2.RowIdentity(echo_nonce=n)


def test_a_watermark_stream_declares_its_record_reads():  # P9.30
    with pytest.raises(ValueError, match="declares its record reads"):
        Stream("demo.a", 1, watermark=Watermark("demo/x.pull", ["demo/c"])).validate()
    with pytest.raises(ValueError, match="not among the record readers"):
        Stream("demo.a", 1, watermark=Watermark("demo/x.pull", ["demo/c"]), records=table_records("demo/d")).validate()
    with pytest.raises(ValueError, match="who may pull"):
        Stream("demo.a", 1, watermark=Watermark("", ["demo/c"]), records=table_records("demo/c")).validate()
    with pytest.raises(ValueError, match="both read functions"):
        Stream("demo.a", 1, records=Records("demo/x.read", ["demo/c"], None, None)).validate()
    people().validate()


async def test_only_the_declared_pullers_may_pull():  # CK-WM-01, P9.13
    teams = Stream("demo.teams", 1, watermark=Watermark("demo/wmsrc.pull", ["demo/other"]),
                   records=table_records("demo/other"))
    d, pool, p = await setup("py_wm1", people(), teams)
    try:
        src = source(d, p)
        with pytest.raises(errs.Error) as e:
            await pull(src, "demo/other", watermark_pb2.PullRequest(stream="demo.people", nonce=nonce()))
        assert e.value.reason == "PERMISSION_DENIED", "a component the stream does not list"
        with pytest.raises(errs.Error) as e:
            await pull(src, "demo/consumer", watermark_pb2.PullRequest(stream="demo.teams", nonce=nonce()))
        assert e.value.reason == "PERMISSION_DENIED", "a puller of another stream"
        with pytest.raises(errs.Error) as e:
            await pull(src, "demo/consumer", watermark_pb2.PullRequest(stream="demo.people", nonce="ab"))
        assert e.value.reason == "REQUEST_INVALID", "a nonce of fewer than 128 bits"
        pages = await pull(src, "demo/consumer", watermark_pb2.PullRequest(stream="demo.people", nonce=nonce()))
        decl = pages[0].source
        assert (decl.primary, decl.deployment, decl.impl) == (True, "dep-1", "impl-1")
        assert decl.system_id and decl.epoch == "1.1"

        async def no_subjects(tx):
            await p.emit(tx, "demo.people", Event("demo.person.changed.v1", "r", 1, Projection.FULL))
        with pytest.raises(errs.Error, match="lists its subjects"):
            await d.tx(no_subjects)
    finally:
        await pool.close()


async def test_a_pull_echoes_after_the_changes_before_it():  # CK-WM-02, P9.16, PJ23, PJ25, PJ29
    import asyncio
    d, pool, p = await setup("py_wm2", people())
    try:
        src = source(d, p)
        before = await emit_people(d, p, 3)

        async def rolled_back(tx):
            await emit_person(tx, p)
            raise errs.new("REQUEST_INVALID", "rolled back")
        with pytest.raises(errs.Error):
            await d.tx(rolled_back)
        held, release = asyncio.Event(), asyncio.Event()
        long_id = []

        async def long(tx):
            long_id.append(await emit_person(tx, p))
            held.set()
            await release.wait()
        long_task = asyncio.create_task(d.tx(long))
        await held.wait()

        n1 = nonce()
        _, types, echo, _ = pulled(await pull(src, "demo/consumer",
                                              watermark_pb2.PullRequest(stream="demo.people", nonce=n1)))
        assert echo is not None and types == [ECHO_TYPE], "echo only returns the echo"
        assert echo.seq == 4, "the three committed changes are 1-3, before the echo"

        release.set()
        await long_task
        after = await emit_people(d, p, 2)
        req = at(echo, echo.seq, echo_row(n1))
        recs, types, echo2, need = pulled(await pull(src, "demo/consumer", req))
        assert need == watermark_pb2.NEED_SNAPSHOT_UNSPECIFIED
        assert recs[:3] == [long_id[0], *after], "the long transaction's row only delays itself"
        assert len(recs) == 4 and types[3] == ECHO_TYPE and echo2.seq == 8
        assert len(before) == 3
    finally:
        await pool.close()


async def test_a_pull_pages_with_the_echo_on_the_last_page():  # CK-WM-02, P9.16, PJ29
    d, pool, p = await setup("py_wm3", people())
    try:
        src = source(d, p)
        n1 = nonce()
        _, _, echo, _ = pulled(await pull(src, "demo/consumer", watermark_pb2.PullRequest(stream="demo.people", nonce=n1)))

        async def many(tx):
            for _ in range(2 * PULL_PAGE + 100):
                await emit_person(tx, p)
        await d.tx(many)
        pages = await pull(src, "demo/consumer", at(echo, echo.seq, echo_row(n1)))
        assert len(pages) == 3
        for i, pg in enumerate(pages):
            r = pg.partitions[0]
            if i < 2:
                assert len(r.events) == PULL_PAGE and not r.HasField("echo"), "no echo before the last page"
            else:
                assert len(r.events) == 101 and r.HasField("echo"), "the rest and the echo"
            assert pg.source.epoch == pages[0].source.epoch, "every page declares the source"
        seqs = [int(e.headers["ce-beseq"]) for pg in pages for e in pg.partitions[0].events]
        assert seqs == list(range(echo.seq + 1, echo.seq + 1 + len(seqs))), "every position after the asked, in order"
    finally:
        await pool.close()


async def test_the_pull_checks_the_row_at_the_position():  # CK-WM-03, P9.15, SK17, PJ44, PJ48
    d, pool, p = await setup("py_wm4", people())
    a = await pgtest.admin()
    try:
        src = source(d, p)
        await emit_people(d, p, 3)
        _, _, echo, _ = pulled(await pull(src, "demo/consumer",
                                          watermark_pb2.PullRequest(stream="demo.people", nonce=nonce())))
        assert echo.seq == 4
        r = await a.fetchrow("SELECT record_id, version, digest FROM py_wm4.be_outbox WHERE seq = 2")
        row = record_row(r["record_id"], r["version"], r["digest"])

        async def outcome(req):
            recs, _, _, need = pulled(await pull(src, "demo/consumer", req))
            return need, recs

        need, recs = await outcome(at(echo, 2, row))
        assert need == watermark_pb2.NEED_SNAPSHOT_UNSPECIFIED, "the row at the position is the one it names"
        assert len(recs) == 3, "after seq 2 up to the new echo: the third change, the first echo, the new echo"
        await a.execute("UPDATE py_wm4.be_outbox SET version = version + 1 WHERE seq = 2")
        need, recs = await outcome(at(echo, 2, row))
        assert need == watermark_pb2.NEED_SNAPSHOT_ROLLBACK_DETECTED and recs == [], "the row there is another one"
        need, _ = await outcome(at(echo, 99, row))
        assert need == watermark_pb2.NEED_SNAPSHOT_ROLLBACK_DETECTED, "a position past the source's counter"
        await a.execute("DELETE FROM py_wm4.be_outbox WHERE seq = 1")
        need, _ = await outcome(at(echo, 1, record_row("gone", 1, "x")))
        assert need == watermark_pb2.NEED_SNAPSHOT_BEYOND_RETENTION, "the row was purged by the retention"
        other = watermark_pb2.Position(impl=echo.impl, epoch="0.1", seq=2)
        need, recs = await outcome(watermark_pb2.PullRequest(stream="demo.people", nonce=nonce(), partitions=[
            watermark_pb2.PartitionPosition(partition=0, position=other, row=row)]))
        assert need == watermark_pb2.NEED_SNAPSHOT_UNSPECIFIED and len(recs) == 1, "another epoch: echo only"
    finally:
        await a.close()
        await pool.close()


async def test_the_step_before_the_echo_runs_first():  # P9.16
    """A source mirroring an external product pulls the product's changes into the outbox before the echo; when that
    fails, no echo is written and the pull answers DEPENDENCY_UNAVAILABLE."""
    state = {"fail": False, "mirrored": []}
    box = {}

    async def before():
        if state["fail"]:
            raise RuntimeError("the product is down")

        async def write(tx):
            state["mirrored"].append(await emit_person(tx, box["p"]))
        await box["d"].tx(write)

    d, pool, p = await setup("py_wm5", people(before))
    box.update(d=d, p=p)
    a = await pgtest.admin()
    try:
        src = source(d, p)
        _, _, echo, _ = pulled(await pull(src, "demo/consumer",
                                          watermark_pb2.PullRequest(stream="demo.people", nonce=nonce())))
        assert len(state["mirrored"]) == 1 and echo.seq == 2, "the product's change (1) is before the echo (2)"
        state["fail"] = True
        echoes = await a.fetchval("SELECT count(*) FROM py_wm5.be_outbox WHERE type = $1", ECHO_TYPE)
        with pytest.raises(errs.Error) as e:
            await pull(src, "demo/consumer", watermark_pb2.PullRequest(stream="demo.people", nonce=nonce()))
        assert e.value.reason == "DEPENDENCY_UNAVAILABLE"
        assert await a.fetchval("SELECT count(*) FROM py_wm5.be_outbox WHERE type = $1", ECHO_TYPE) == echoes, \
            "no echo when the step failed"
    finally:
        await a.close()
        await pool.close()


ROWS_STREAM = Stream("demo.rows", 2, records=table_records("demo/consumer"))


async def read(src, who, stream, *ids):
    token = as_caller(who)
    try:
        return await src.Read(records_pb2.ReadRequest(stream=stream, record_ids=list(ids)), None)
    finally:
        system._caller.reset(token)


async def snapshot(src, who, req):
    token = as_caller(who)
    try:
        return await src.Snapshot(req, None)
    finally:
        system._caller.reset(token)


def record_source(d, p):
    return RecordSource(d, p, "impl-1", "dep-1", LOG)


async def test_only_the_declared_readers_may_read():  # CK-PJ-04, P9.30
    other = Stream("demo.other", 1, records=table_records("demo/other"))
    d, pool, p = await setup("py_rec1", ROWS_STREAM, other)
    try:
        src = record_source(d, p)
        for call in (read(src, "demo/other", "demo.rows", "a"),
                     snapshot(src, "demo/other", records_pb2.SnapshotRequest(stream="demo.rows"))):
            with pytest.raises(errs.Error) as e:
                await call
            assert e.value.reason == "PERMISSION_DENIED", "a reader of another stream"
        await read(src, "demo/consumer", "demo.rows", "a")
    finally:
        await pool.close()


async def test_a_read_answers_every_id_in_order():  # CK-PJ-04, P9.31, P9.33
    d, pool, p = await setup("py_rec2", ROWS_STREAM)
    a = await pgtest.admin()
    try:
        await a.execute("""INSERT INTO py_rec2.rows VALUES ('live', 3, '{"n":1}', false), ('gone', 4, '{}', true)""")
        for _ in range(3):  # numbered events on the partition of "live": the high point is the last of them
            await d.tx(lambda tx: p.emit(tx, "demo.rows", Event("demo.row.changed.v1", "live", 3, Projection.FULL,
                                                                   {"n": 1})))
        part = ROWS_STREAM.partition_of("live")
        await number(d, "demo.rows", part, 500)
        src = record_source(d, p)
        resp = await read(src, "demo/consumer", "demo.rows", "never", "gone", "live")
        got = [(r.record_id, r.state, r.version, r.payload) for r in resp.results]
        assert got == [("never", records_pb2.READ_STATE_PURGED, 0, b""),
                       ("gone", records_pb2.READ_STATE_TOMBSTONE, 4, b""),
                       ("live", records_pb2.READ_STATE_EXISTS, 3, b'{"n":1}')], "encoded as the producer encodes"
        assert {h.partition: h.seq for h in resp.highs} == {part: 3, 1 - part: 0}, "every partition, one snapshot"
        system_id = await a.fetchval("SELECT system_identifier::text FROM pg_control_system()")
        s = resp.source
        assert (s.deployment, s.impl, s.system_id, s.epoch, s.primary) == ("dep-1", "impl-1", system_id, "1.1", True)
        for ids in ((), [str(i) for i in range(RECORD_PAGE + 1)]):
            with pytest.raises(errs.Error) as e:
                await read(src, "demo/consumer", "demo.rows", *ids)
            assert e.value.reason == "REQUEST_INVALID", "no ID, or more than one read takes"
    finally:
        await a.close()
        await pool.close()


async def test_a_read_sees_one_snapshot():  # P9.31: the high points describe exactly what the read returned
    """A row committed by another transaction after the read's first statement (the high points) is not seen by the
    read function: the whole read runs in one snapshot (REPEATABLE READ)."""
    a = await pgtest.admin()
    inner = table_records("demo/consumer")

    async def late_read(tx, ids):
        await a.execute("""INSERT INTO py_rec6.rows VALUES ('late', 1, '{}', false)""")  # committed meanwhile
        return await inner.read(tx, ids)

    stream = Stream("demo.rows", 2, records=Records(inner.operation, inner.readers, late_read, inner.page))
    d, pool, p = await setup("py_rec6", stream)
    try:
        resp = await read(record_source(d, p), "demo/consumer", "demo.rows", "late")
        assert resp.results[0].state == records_pb2.READ_STATE_PURGED, "committed after the snapshot: not seen"
        assert await a.fetchval("SELECT count(*) FROM py_rec6.rows WHERE id = 'late'") == 1, "the row is there"
    finally:
        await a.close()
        await pool.close()


async def test_a_snapshot_pages_by_record_id():  # CK-PJ-04, P9.32
    d, pool, p = await setup("py_rec3", ROWS_STREAM)
    a = await pgtest.admin()
    try:
        await a.execute("""INSERT INTO py_rec3.rows SELECT 'r' || lpad(g::text, 5, '0'), g, jsonb_build_object('g', g),
            g % 10 = 0 FROM generate_series(1, 1300) g""")
        src = record_source(d, p)
        ids, sizes = [], []
        req = records_pb2.SnapshotRequest(stream="demo.rows")
        while True:
            resp = await snapshot(src, "demo/consumer", req)
            assert len(resp.highs) == 2, "every page carries the high points"
            sizes.append(len(resp.records))
            ids += [r.record_id for r in resp.records]
            if not resp.next_page_token:
                break
            req = records_pb2.SnapshotRequest(page_token=resp.next_page_token)
        assert sizes == [500, 500, 170], "1 170 records that exist (every tenth is deleted)"
        assert ids == sorted(ids) and len(set(ids)) == len(ids) and "r00010" not in ids
    finally:
        await a.close()
        await pool.close()


async def test_a_snapshot_page_token_is_bound_to_the_epoch():  # CK-PJ-04, P9.32
    d, pool, p = await setup("py_rec4", ROWS_STREAM)
    a = await pgtest.admin()
    try:
        await a.execute("""INSERT INTO py_rec4.rows SELECT 'r' || lpad(g::text, 5, '0'), g, '{}', false
            FROM generate_series(1, 600) g""")
        src = record_source(d, p)
        first = await snapshot(src, "demo/consumer", records_pb2.SnapshotRequest(stream="demo.rows"))
        assert first.next_page_token, "the precondition: a second page"
        await a.execute("UPDATE py_rec4.be_recovery_generation SET generation = generation + 1")
        with pytest.raises(errs.Error) as e:
            await snapshot(src, "demo/consumer", records_pb2.SnapshotRequest(page_token=first.next_page_token))
        assert e.value.reason == "QUERY_PLAN_CHANGED", "align again"
        again = await snapshot(src, "demo/consumer", records_pb2.SnapshotRequest(stream="demo.rows"))
        assert again.source.epoch == "2.1", "a new first page declares the new epoch"
    finally:
        await a.close()
        await pool.close()


async def test_a_big_record_is_a_claim_check():  # P9.30, P6.14
    claims = Claims()
    d, pool, p = await setup("py_rec5", ROWS_STREAM, claims=claims)
    a = await pgtest.admin()
    try:
        big = json.dumps({"text": "x" * MAX_PAYLOAD})
        await a.execute("INSERT INTO py_rec5.rows VALUES ('big', 1, $1::jsonb, false), ('small', 1, '{}', false)", big)
        src = record_source(d, p)
        resp = await read(src, "demo/consumer", "demo.rows", "big", "small")
        r = resp.results[0]
        assert r.claim and not resp.results[1].claim, "over 64 KiB: a claim check"
        ref = json.loads(r.payload)["claim"]
        stored = claims.objs[ref["uri"]]
        assert ref["size"] == len(stored) and ref["sha256"] == sha256_hex(stored)
        assert json.loads(stored) == json.loads(big), "the object is the record's content"
        snap = await snapshot(src, "demo/consumer", records_pb2.SnapshotRequest(stream="demo.rows"))
        assert snap.records[0].claim, "the snapshot too"
    finally:
        await a.close()
        await pool.close()
