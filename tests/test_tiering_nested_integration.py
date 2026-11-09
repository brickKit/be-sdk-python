"""An aggregate of three levels (P15.1, P15.13, P15.19, P15.22, P15.31, A155), the same case as be-sdk-go's
tiering/nested_integration_test.go: it is sealed with every level, a write to a tax of a sealed invoice is refused,
the unit is copied, read back by key with every level as it was online, released in batches of one row (the deepest
level first), and a tax's remark is erased in the archived copy."""
import datetime as dt
import uuid

import pytest

from besdk import canon, errs
from besdk.evidence import Writer
from besdk.tiering import barrier, erase, reads, release, transfer
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import COMPLETED, OBJECT, POSTGRES, Child, Class, Retention, Table
from besdk.tiering.rows import NUMERIC, TEXT, TEXT_ARRAY, TIMESTAMPTZ, UUID
from tests import pgtest
from tests.test_db_integration import migrated, open_db
from tests.test_tiering_destroy_integration import QUIET, Keeper
from tests.test_tiering_release_integration import backup_at
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, store_for

pytestmark = pytest.mark.integration
NESTED_SQL = """CREATE TABLE invoices (id uuid PRIMARY KEY, entity uuid NOT NULL, closed_at timestamptz,
  created_at timestamptz NOT NULL, status text NOT NULL);
CREATE TABLE invoice_lines (id uuid PRIMARY KEY, invoice_id uuid NOT NULL REFERENCES invoices (id), amount numeric(19,4) NOT NULL);
CREATE TABLE line_taxes (id uuid PRIMARY KEY, line_id uuid NOT NULL REFERENCES invoice_lines (id), rate numeric(9,6) NOT NULL,
  remark text);
CREATE TABLE line_tags (line_id uuid NOT NULL REFERENCES invoice_lines (id), tag text NOT NULL, serials text[] NOT NULL,
  PRIMARY KEY (line_id, tag));"""


def nested_class(route: str) -> Class:
    return Class("billing.invoices", COMPLETED,
                 Table("invoices", {"id": UUID, "entity": UUID, "closed_at": TIMESTAMPTZ, "created_at": TIMESTAMPTZ,
                                    "status": TEXT}, pk="id"),
                 anchor="closed_at", created="created_at", terminal="r.status = 'closed'", residence_days=365,
                 retention=Retention(3650, 3650, "test"), schema_version="1",
                 children=(
                     # declared before its parent: the order of the declaration does not matter
                     Child(Table("line_taxes", {"id": UUID, "line_id": UUID, "rate": NUMERIC, "remark": TEXT},
                                 pk="id"), "line_id", "invoice_lines"),
                     Child(Table("invoice_lines", {"id": UUID, "invoice_id": UUID, "amount": NUMERIC}, pk="id"),
                           "invoice_id"),
                     # a leaf with a key of two columns and a text array (P15.1, P15.8)
                     Child(Table("line_tags", {"line_id": UUID, "tag": TEXT, "serials": TEXT_ARRAY},
                                 key=("line_id", "tag")), "line_id", "invoice_lines")),
                 group=("entity",), locate=("entity",), erasable=("remark",), routes=(route,),
                 hold_operation="billing.invoice.hold", approve_operation="billing.invoice.approve")


async def invoice(a, entity: str, closed: bool) -> tuple[str, str]:
    """An invoice (closed 400 days ago, or open) with two lines of two taxes each; its id and one tax row's."""
    iid, tax = str(uuid.uuid7()), ""
    status, at = ("closed", "now() - interval '400 days'") if closed else ("open", "NULL")
    await a.execute(f"INSERT INTO invoices VALUES ('{iid}', '{entity}', {at}, now() - interval '500 days', '{status}')")
    for n in range(2):
        line = str(uuid.uuid7())
        await a.execute(f"INSERT INTO invoice_lines VALUES ('{line}', '{iid}', {n + 1}.5)")
        await a.execute(f"INSERT INTO line_tags VALUES ('{line}', 'lot', ARRAY['S-{n}', 'S-{n + 10}']), "
                        f"('{line}', 'colour', '{{}}')")
        for _ in range(2):
            tax = str(uuid.uuid7())
            await a.execute(f"INSERT INTO line_taxes VALUES ('{tax}', '{line}', 0.21, 'Mr Example, +34 600')")
    return iid, tax


@pytest.mark.parametrize("route", [POSTGRES, OBJECT])
async def test_an_aggregate_of_three_levels(route):
    name = "py_tier_nested_" + route.replace("-", "_")
    v, _ = await migrated(name, {"0001_billing.up.sql": NESTED_SQL})
    d, pool = await open_db(name, v)
    a = await pgtest.admin()
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        await a.execute(f"SET search_path = {name}")
        cl = nested_class(route)
        assert cl.check(90) == []
        for s in barrier.statements([cl]):
            await a.execute(s)
        cat = Catalog(d, {cl.name: cl}, store=st, batch=1, grace=dt.timedelta(0),
                      backups=backup_at(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)),
                      evidence=Writer(d, "test/billing", "t-1", QUIET, store=Keeper()))
        entity = str(uuid.uuid7())
        ia, tax = await invoice(a, entity, True)
        ib, _ = await invoice(a, entity, True)
        open_, open_tax = await invoice(a, entity, False)

        async def online(iid: str) -> str:  # the invoice's lower levels as one sorted text, values canonical
            out = []
            for r in await a.fetch("SELECT l.id::text, l.amount::text FROM invoice_lines l WHERE l.invoice_id = $1",
                                   uuid.UUID(iid)):
                out.append(f"line {r[0]} {canon.decimal(r[1])}")
            for r in await a.fetch("SELECT x.id::text, x.line_id::text, x.rate::text, coalesce(x.remark, '-') FROM "
                                   "line_taxes x JOIN invoice_lines l ON l.id = x.line_id WHERE l.invoice_id = $1",
                                   uuid.UUID(iid)):
                out.append(f"tax {r[0]} {r[1]} {canon.decimal(r[2])} {r[3]}")
            return "|".join(sorted(out))

        before = await online(ia)
        groups = await cat.groupings(cl.name)
        res = await cat.seal(cl.name, groups[0])
        assert res.members == 2

        async def write(tid: str) -> None:
            await d.tx(lambda tx: tx.execute("UPDATE line_taxes SET rate = 0.1 WHERE id = $1", uuid.UUID(tid)))

        with pytest.raises(errs.Error) as x:
            await write(tax)
        assert x.value.reason == "RECORD_SEALED", "a tax of a sealed invoice"
        await write(open_tax)  # the open invoice's tax

        await transfer.transfer(cat, res.unit)
        agg = await reads.get(cat, cl.name, ia)
        assert agg is not None
        assert len(agg.children["invoice_lines"]) == 2
        assert len(agg.children["line_taxes"]) == 4, "the third level under its root"
        assert len(agg.children["line_tags"]) == 4, "a leaf of two key columns"
        serials = sorted(g["serials"] for g in agg.children["line_tags"] if g["tag"] == "lot")
        assert serials == [["S-0", "S-10"], ["S-1", "S-11"]], "a text array as it was"
        got = [f"line {r['id']} {r['amount']}" for r in agg.children["invoice_lines"]]
        got += [f"tax {r['id']} {r['line_id']} {r['rate']} {r['remark']}" for r in agg.children["line_taxes"]]
        assert "|".join(sorted(got)) == before, "as it was online"

        assert await release.release(cat, res.unit) == []
        for q in ("SELECT count(*) FROM invoices WHERE id IN ($1, $2)",
                  "SELECT count(*) FROM line_tags g JOIN invoice_lines l ON l.id = g.line_id WHERE l.invoice_id IN ($1, $2)",
                  "SELECT count(*) FROM invoice_lines WHERE invoice_id IN ($1, $2)",
                  "SELECT count(*) FROM line_taxes x JOIN invoice_lines l ON l.id = x.line_id "
                  "WHERE l.invoice_id IN ($1, $2)"):
            assert await a.fetchval(q, uuid.UUID(ia), uuid.UUID(ib)) == 0, q
        assert await a.fetchval("SELECT count(*) FROM line_taxes x JOIN invoice_lines l ON l.id = x.line_id "
                                "WHERE l.invoice_id = $1", uuid.UUID(open_)) == 4, "the open invoice keeps its taxes"

        assert await erase.erase(cat, cl.name, ia, "request ER-7") == erase.ERASED
        agg = await reads.get(cat, cl.name, ia)
        assert len(agg.children["line_taxes"]) == 4
        assert all("remark" not in r for r in agg.children["line_taxes"]), "erased on the third level"
        other = await reads.get(cat, cl.name, ib)
        assert other.children["line_taxes"][0]["remark"] == "Mr Example, +34 600"
    finally:
        await st.close()
        await a.close()
        await pool.close()
