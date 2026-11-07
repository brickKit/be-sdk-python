"""A full recovery's checks (P14.1-P14.5), the same cases as be-sdk-go's recovery/server_integration_test.go: the
SDK's checks and the component's, every read rolled back, a check that cannot run unprovable; the presence of audited
objects (present, older, absent; an unregistered type, a version that is no revision); the continuous series read and
raised, never lowered, the skipped numbers voided in the same transaction, a series with gaps refused."""
import pytest

from besdk import errs
from besdk._gen.be.recovery.v1 import check_pb2
from besdk.db.docno import Series, next_gapped_number, next_number
from besdk.recovery import (ABSENT, AUDIT_BACKLOG, CONSUMER_EPOCH, DIFF, OLDER, PASS, PROJECTIONS, UNPROVABLE, Check,
                            Checks, CheckServer, Result, in_tx, table_presence)
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration


async def schema(name, files=None):
    v, _ = await migrated(name, files or {"0001_x.up.sql": "CREATE TABLE x (id int);"})
    return await open_db(name, v)


async def outcomes(s):
    return {r.check: r for r in (await s.Check(check_pb2.CheckRequest(recovery="rec-1"), None)).results}


async def test_checks():  # P14.1, P14.2
    d, pool = await schema("py_rcv_checks")
    a = await pgtest.admin()
    try:
        reg = Checks()

        async def writes(tx):
            await tx.execute("INSERT INTO be_doc_series (series, last_number, gapless) VALUES ('x', 1, true)")
            return Result(PASS)

        reg.add(Check("demo.writes", "test", in_tx(writes)))
        reads = []

        async def twice(read):
            for _ in range(2):  # a read, a call to the other side in between, a read
                async def body(tx):
                    reads.append(1)
                    await tx.execute("INSERT INTO be_doc_series (series, last_number, gapless) VALUES ('y', 1, true)")
                await read(body)
            return Result(PASS)

        reg.add(Check("demo.twice", "test", twice))

        async def broken(read):
            raise RuntimeError("cannot tell")

        reg.add(Check("demo.broken", "test", broken))
        with pytest.raises(ValueError):
            reg.add(Check("demo.broken", "test", broken))
        s = CheckServer(d, True, reg)
        got = await outcomes(s)
        assert got[PROJECTIONS].outcome == PASS and got[AUDIT_BACKLOG].outcome == PASS
        assert got[CONSUMER_EPOCH].outcome == DIFF, "never recorded in this epoch"
        assert got["demo.writes"].outcome == PASS and got["demo.twice"].outcome == PASS and len(reads) == 2
        assert got["demo.broken"].outcome == UNPROVABLE and "cannot tell" in got["demo.broken"].disposal
        assert await a.fetchval("SELECT count(*) FROM py_rcv_checks.be_doc_series") == 0, "every read rolled back"
        assert CONSUMER_EPOCH not in await outcomes(CheckServer(d, False)), "only a member that subscribes"

        sql = lambda q: a.execute(q.replace("{s}", "py_rcv_checks"))  # noqa: E731
        await a.execute("SET search_path = py_rcv_checks")  # be_current_db_epoch reads the schema's tables by name
        await a.execute("INSERT INTO be_consumer_epoch (epoch_generation, epoch_timeline) "
                        "SELECT generation, timeline FROM be_current_db_epoch()")
        await a.execute("RESET search_path")
        await sql("INSERT INTO {s}.be_projection_source (projection, status) VALUES ('p.ok', 'aligned'), "
                  "('p.late', 'stalled')")
        await sql("""INSERT INTO {s}.be_watermark_quarantine (projection, subject, impl, epoch, align_gen, partition, seq,
            event) VALUES ('p.ok', 'customer-7', 'i', 'e', 1, 0, 9, '{}')""")
        await sql("""INSERT INTO {s}.be_audit_log (id, occurred_at, category, operation, outcome, digest)
            VALUES (gen_random_uuid(), now(), 'business_change', 'demo.op', 'succeeded', 'd')""")
        got = await outcomes(s)
        assert got[CONSUMER_EPOCH].outcome == PASS, "recorded in the current epoch"
        for edit, why in (("epoch_timeline = epoch_timeline + 1", "another timeline"),
                          ("epoch_timeline = epoch_timeline - 1, epoch_generation = epoch_generation + 1",
                           "another generation")):
            await sql("UPDATE {s}.be_consumer_epoch SET " + edit)
            assert (await outcomes(s))[CONSUMER_EPOCH].outcome == DIFF, why
        await sql("UPDATE {s}.be_consumer_epoch SET epoch_generation = epoch_generation - 1")
        assert [f"{x.object_type}:{x.object_id}" for x in got[PROJECTIONS].differences] == [
            "projection:p.late", "projection_subject:p.ok/customer-7"]
        assert "1 records not transferred" in got[AUDIT_BACKLOG].differences[0].detail
        await sql("UPDATE {s}.be_audit_log SET transfer_state = 'conflict'")
        diffs = (await outcomes(s))[AUDIT_BACKLOG].differences
        assert [x.object_id for x in diffs] == ["conflict"]
        await sql("UPDATE {s}.be_audit_log SET transfer_state = 'received'")
        assert (await outcomes(s))[AUDIT_BACKLOG].outcome == PASS, "transferred"
        await sql("""INSERT INTO {s}.be_resend_gap (subscription, source_component, source_stream, source_partition,
            kind) VALUES ('s', 'erp/sales', 'orders', 0, 'beyond_retention')""")
        assert (await outcomes(s))[CONSUMER_EPOCH].outcome == DIFF, "a gap the redelivery could not cover"
    finally:
        await a.close()
        await pool.close()


async def test_present():  # P14.3
    d, pool = await schema("py_rcv_present", {"0001_invoice.up.sql": """CREATE TABLE invoice
        (id uuid PRIMARY KEY, version bigint NOT NULL); INSERT INTO invoice VALUES
        ('0190f3a1-0000-7000-8000-0000000000b1', 3);"""})
    try:
        reg = Checks()
        reg.add_presence("invoice", table_presence("invoice", "id", "version"))

        async def orders(tx, ids):
            return {i: 5 for i in ids if i != "o-gone"}

        reg.add_presence("order", orders)
        s = CheckServer(d, False, reg)

        async def ask(*objs):
            r = await s.Present(check_pb2.PresentRequest(recovery="rec-1", objects=[
                check_pb2.ObjectRef(type=t, id=i, version=v) for t, i, v in objs]), None)
            return [(x.object.id, x.answer, x.version) for x in r.answers]

        assert await ask(("order", "o-1", "5"), ("order", "o-2", "7"), ("order", "o-gone", "1"), ("order", "o-3", "")) \
            == [("o-1", "present", "5"), ("o-2", OLDER, "5"), ("o-gone", ABSENT, ""), ("o-3", "present", "5")]
        assert await ask(("invoice", "0190f3a1-0000-7000-8000-0000000000b1", "4"), ("invoice", "not-a-uuid", "")) \
            == [("0190f3a1-0000-7000-8000-0000000000b1", OLDER, "3"), ("not-a-uuid", ABSENT, "")]
        for objs, reason in (((("shipment", "i-1", ""),), "CAPABILITY_UNSUPPORTED"),
                             ((("order", "o-1", "v5"),), "REQUEST_INVALID")):
            with pytest.raises(errs.Error) as e:
                await ask(*objs)
            assert e.value.reason == reason
    finally:
        await pool.close()


async def test_numbers():  # P14.5
    d, pool = await schema("py_rcv_numbers")
    a = await pgtest.admin()
    try:
        inv = Series("le-1", "INV", "2026")

        async def three(tx):
            for _ in range(3):
                await next_number(tx, inv)

        await d.tx(three)
        await next_gapped_number(d, Series("le-1", "DRAFT"))
        void = []
        reg = Checks()

        async def voiding(tx, s, start, end):
            void.append((s.code, start, end))

        reg.set_void(voiding)
        s = CheckServer(d, False, reg)
        nums = (await s.Numbers(check_pb2.NumbersRequest(recovery="rec-1"), None)).series
        assert [(x.code, x.period, x.max_used) for x in nums] == [("INV", "2026", 3)], "only the continuous series"

        async def raise_(srv, *series):
            r = await srv.AdvanceNumbers(check_pb2.AdvanceNumbersRequest(recovery="rec-1", series=[
                check_pb2.Series(legal_entity=le, code=c, period=p, max_used=m) for le, c, p, m in series]), None)
            return [x.max_used for x in r.series]

        assert await raise_(s, ("le-1", "INV", "2026", 7), ("le-2", "INV", "2026", 2)) == [7, 2]
        assert void == [("INV", 4, 7), ("INV", 1, 2)]
        assert await raise_(s, ("le-1", "INV", "2026", 5)) == [7], "never lowered"
        assert len(void) == 2, "nothing skipped"
        for srv, series, reason in ((s, ("le-1", "DRAFT", "", 9), "REQUEST_INVALID"),
                                    (CheckServer(d, False), ("le-1", "INV", "2026", 9), "CAPABILITY_UNSUPPORTED")):
            with pytest.raises(errs.Error) as e:
                await raise_(srv, series)
            assert e.value.reason == reason
        assert await a.fetchval("SELECT last_number FROM py_rcv_numbers.be_doc_series WHERE series = 'le-1/INV/2026'") \
            == 7, "a refused raise writes nothing"
    finally:
        await a.close()
        await pool.close()
