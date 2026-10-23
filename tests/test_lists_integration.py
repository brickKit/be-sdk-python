"""Lists against PostgreSQL (P9.1-P9.5): the list-pages vector page by page along the cursors, cursors bound to their
filter and plan, restricted fields refused where not readable everywhere. The same cases as be-sdk-go's list tests."""
import io
from datetime import datetime, timezone

import pytest

from besdk import errs, protocol
from besdk.authz import Definitions, Field, Holder, Operation, Resource, Subject
from besdk.lists import DATE, DECIMAL, TEXT, UUID, Column, Filter, Order, Request, SearchColumn, Spec, build, parse_limit
from besdk.obs import Member, new_logger
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration

ALICE = "0190f3a1-0000-7000-8000-0000000000a1"

DEFS = Definitions("demo/items", [Resource("demo.item", "demo.item.read", {}, {
    "id": Field("ordinary"), "name": Field("ordinary"), "amount": Field("ordinary"), "day": Field("ordinary"),
    "cost": Field("restricted", read_key="demo.item.cost")})],
    [Operation("demo.item.read", "demo.item", "read", "low")])

SPEC = Spec("demo.item.read", "i", "id", UUID, [
    Column("name", "name", TEXT, True, True), Column("amount", "amount", DECIMAL, True, True),
    Column("day", "day", DATE, True, True), Column("cost", "cost", DECIMAL, True, True)])


def seg(*fields):
    return {"resource_type": "demo.item", "operation": "demo.item.read", "atoms": {}, "read_fields": list(fields),
            "valid": {"from": "2026-01-01T00:00:00Z"}}


class Grants:
    def __init__(self):
        self.segs = [seg("id", "name", "amount", "day", "cost")]

    async def verify(self, req):
        view = {"shard": "s", "applicability": ["demo.item"], "digest": "d", "subjects": {ALICE: {"segments": self.segs}}}
        return {"nonce": req["nonce"], "recovery_generation": {"domain": "authz.recovery", "value": "1"},
                "shards": [{"shard": "s", "status": "changed", "view": view}]}


async def setup(name):
    rows = protocol.vector("list-pages")["cases"][0]["rows"]
    v, _ = await migrated(name, {"0001_items.up.sql": "CREATE TABLE items (id uuid PRIMARY KEY, name text NOT NULL, "
                                                      "amount numeric NOT NULL, day date NOT NULL, cost numeric NOT NULL);"})
    d, pool = await open_db(name, v)

    async def fill(tx):
        await tx.execute("DELETE FROM items")
        for r in rows:
            await tx.execute("INSERT INTO items VALUES ($1::text::uuid, $2, $3::text::numeric, $4::text::date, 1)",
                             r["id"], r["name"], r["amount"], r["day"])
    await d.tx(fill)
    g = Grants()
    h = Holder("demo/items", g, new_logger(Member("demo/items", "3.0.0"), "error", io.StringIO()))
    await h.verify()
    DEFS.validate()
    return d, pool, Subject(ALICE, DEFS, h, datetime.now(timezone.utc)), g


async def page(d, s, req):
    """One page: its IDs and the next cursor ("" at the end)."""
    q = build(s, SPEC, req)
    sel = ", ".join(["i.id::text"] + [f"i.{f}::text" for f in q.sort_fields()])

    async def run(tx):
        return await tx.fetch(f"SELECT {sel} FROM items i WHERE {q.where} ORDER BY {q.order_by} LIMIT {q.limit + 1}",
                              *q.args)
    rows = await d.tx(run)
    ids = [r[0] for r in rows[:q.limit]]
    if len(rows) <= q.limit:
        return ids, ""
    last = rows[q.limit - 1]
    return ids, q.next([last[i] for i in range(1, len(last))], last[0])


async def test_list_pages_vectors():  # P9.1-P9.3
    d, pool, s, _ = await setup("py_lists1")
    try:
        for c in protocol.vector("list-pages")["cases"][1:]:
            if c["kind"] == "refused":
                with pytest.raises(errs.Error) as e:
                    parse_limit(str(c["page_size"]))
                assert errs.is_(e.value, "be", "REQUEST_INVALID"), c["id"]
                continue
            req = Request(sort=[Order(o["field"], o["dir"] == "desc") for o in c.get("sort") or []], limit=c["page_size"])
            got = []
            while True:
                ids, nxt = await page(d, s, req)
                got.append(ids)
                if not nxt:
                    break
                req.cursor = nxt
            assert got == c["expect"], c["id"]
    finally:
        await pool.close()


async def test_cursors_bind_their_filter_and_plan():  # P9.3, CK-LS-01
    d, pool, s, g = await setup("py_lists2")
    try:
        req = Request(sort=[Order("name")], limit=3)
        _, nxt = await page(d, s, req)
        for other, why in ((Request(sort=[Order("amount")], limit=3, cursor=nxt), "another sort"),
                           (Request(sort=req.sort, limit=3, cursor="not-a-cursor"), "unreadable"),
                           (Request(sort=req.sort, limit=3, cursor=nxt,
                                    filters=[Filter("amount", "gte", ["1"])]), "another filter")):
            with pytest.raises(errs.Error) as e:
                await page(d, s, other)
            assert errs.is_(e.value, "be", "CURSOR_INVALID"), why
        g.segs = [seg("id", "name")]  # the grants changed
        await s.holder.verify()
        with pytest.raises(errs.Error) as e:
            await page(d, s, Request(sort=req.sort, limit=3, cursor=nxt))
        assert errs.is_(e.value, "be", "QUERY_PLAN_CHANGED")
    finally:
        await pool.close()


async def test_restricted_fields_filters_and_keywords():  # P9.4, P9.5, CK-LS-02
    d, pool, s, g = await setup("py_lists3")
    try:
        ids, _ = await page(d, s, Request(sort=[Order("cost")]))
        assert len(ids) == 8, "cost is readable in every segment"
        g.segs = [seg("id", "name", "amount", "day")]
        await s.holder.verify()
        for req in (Request(sort=[Order("cost")]), Request(filters=[Filter("cost", "gte", ["1"])])):
            with pytest.raises(errs.Error) as e:
                await page(d, s, req)
            assert errs.is_(e.value, "be", "FIELD_NOT_QUERYABLE")
        assert len((await page(d, s, Request(filters=[Filter("amount", "in", ["10", "5"])])))[0]) == 5
        assert len((await page(d, s, Request(filters=[Filter("day", "gte", ["2026-10-03"])])))[0]) == 2
        assert (await page(d, s, Request(q="alpha")))[0] == [], "no search column: a keyword matches nothing"
        with pytest.raises(errs.Error) as e:
            build(s, Spec(SPEC.operation, "i", "id", UUID, SPEC.columns, [SearchColumn("names", "search", ["name"])]),
                  Request(q="alpha"))
        assert errs.is_(e.value, "be", "CAPABILITY_UNSUPPORTED")
        g.segs = []  # no grant: an empty page, not a refusal (security 07)
        await s.holder.verify()
        assert (await page(d, s, Request()))[0] == []
    finally:
        await pool.close()
