"""Sealing and the write barrier (P15.6, P15.11-P15.13), the same cases as be-sdk-go's
tiering/seal_integration_test.go: only the terminal aggregates past the residence are sealed and then refused to every
write; a seal and a write serialize; a blocked grouping stays open; a child's write holds its root; the barrier holds
from another search path."""
import asyncio
import uuid

import pytest

from besdk import errs
from besdk.tiering import barrier
from besdk.tiering.catalog import EVENTS_PENDING, Catalog
from besdk.tiering.classes import COMPLETED, OBJECT, Child, Class, Retention, Table
from besdk.tiering.rows import INT4, INT8, JSONB, NUMERIC, TEXT, TIMESTAMPTZ, UUID
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
SCHEMA = """CREATE TABLE orders (
  id uuid PRIMARY KEY, legal_entity uuid NOT NULL, code text NOT NULL, note text, total numeric(19,4) NOT NULL,
  status text NOT NULL, closed_at timestamptz, created_at timestamptz NOT NULL, attrs jsonb);
CREATE TABLE order_lines (
  id uuid PRIMARY KEY, order_id uuid NOT NULL REFERENCES orders (id), line integer NOT NULL, qty bigint NOT NULL);"""


def orders_class() -> Class:
    return Class("sales.orders", COMPLETED,
                 Table("orders", {"id": UUID, "legal_entity": UUID, "code": TEXT, "note": TEXT, "total": NUMERIC,
                                  "status": TEXT, "closed_at": TIMESTAMPTZ, "created_at": TIMESTAMPTZ, "attrs": JSONB},
                       pk="id"),
                 anchor="closed_at", created="created_at", terminal="r.status = 'closed'", residence_days=365,
                 retention=Retention(3650, 3650, "test"), schema_version="1",
                 children=(Child(Table("order_lines", {"id": UUID, "order_id": UUID, "line": INT4, "qty": INT8},
                                       pk="id"), "order_id"),),
                 group=("legal_entity",), locate=("legal_entity",), routes=(OBJECT,),
                 hold_operation="sales.order.hold", approve_operation="sales.order.approve_destruction")


class Env:
    def __init__(self, name):
        self.name, self.le = name, str(uuid.uuid7())

    async def __aenter__(self):
        v, _ = await migrated(self.name, {"0001_sales.up.sql": SCHEMA})
        self.d, self.pool = await open_db(self.name, v)
        self.a = await pgtest.admin()
        await self.a.execute(f"SET search_path = {self.name}")  # the barrier keeps the search path it is created under
        for s in barrier.statements([orders_class()]):
            await self.a.execute(s)
        c = orders_class()
        self.cat = Catalog(self.d, {c.name: c})
        return self

    async def __aexit__(self, *exc):
        await self.a.close()
        await self.pool.close()

    async def order(self, closed_days_ago: int) -> str:
        """An order closed that many days ago (open when negative) with two lines; answers its id."""
        oid = str(uuid.uuid7())
        status, closed = ("open", "NULL") if closed_days_ago < 0 else ("closed", f"now() - interval '{closed_days_ago} days'")
        await self.a.execute(f"""INSERT INTO orders VALUES ('{oid}', '{self.le}', 'SO-{oid[:8]}', NULL, 12.5, '{status}',
            {closed}, now() - interval '500 days', '{{"a": 1}}')""")
        await self.a.execute(f"INSERT INTO order_lines VALUES ('{uuid.uuid7()}', '{oid}', 1, 3), ('{uuid.uuid7()}', '{oid}', 2, 4)")
        return oid

    async def write(self, sql, *args):
        await self.d.tx(lambda tx: tx.execute(sql, *args))


async def sealed_refusal(coro):
    with pytest.raises(errs.Error) as e:
        await coro
    assert e.value.reason == "RECORD_SEALED", e.value


async def test_sealing_and_the_write_barrier():  # TR01, TR02, TR28, P15.13
    async with Env("py_tier_seal") as e:
        sealed1, sealed2 = await e.order(400), await e.order(400)
        open_ = await e.order(-1)
        recent = await e.order(150)  # closed five months ago, residence 12 months (TR28)
        groups = await e.cat.groupings("sales.orders")
        assert len(groups) == 1, "one legal entity, one month"
        res = await e.cat.seal("sales.orders", groups[0])
        assert res.blocked == {} and res.members == 2, "only the closed orders past the residence (TR01)"
        assert res.digest.startswith("sha256:") and len(res.digest) == 71
        assert await e.a.fetchval("SELECT state FROM be_tier_unit WHERE id = $1", uuid.UUID(res.unit)) == "sealed"
        await sealed_refusal(e.write("UPDATE orders SET note = 'late' WHERE id = $1", uuid.UUID(sealed1)))
        await sealed_refusal(e.write("DELETE FROM order_lines WHERE order_id = $1", uuid.UUID(sealed2)))
        await sealed_refusal(e.write("INSERT INTO order_lines VALUES ($1, $2, 9, 1)", uuid.uuid7(), uuid.UUID(sealed1)))
        await sealed_refusal(e.write("UPDATE order_lines SET order_id = $1 WHERE order_id = $2", uuid.UUID(sealed1),
                                     uuid.UUID(open_)))  # moved into it
        await e.write("UPDATE orders SET note = 'still open' WHERE id = $1", uuid.UUID(open_))
        await e.write("UPDATE orders SET note = 'not yet' WHERE id = $1", uuid.UUID(recent))
        await e.write("INSERT INTO order_lines VALUES ($1, $2, 9, 1)", uuid.uuid7(), uuid.UUID(open_))
        assert await e.cat.groupings("sales.orders") == [], "the sealed orders are members; the rest are not candidates"


async def test_a_seal_and_a_write_serialize():  # TR03
    async with Env("py_tier_race") as e:
        oid = await e.order(400)
        groups = await e.cat.groupings("sales.orders")
        w = await pgtest.admin()
        try:
            await w.execute("SET search_path = py_tier_race")
            tr = w.transaction()
            await tr.start()
            await w.execute("UPDATE orders SET note = 'first' WHERE id = $1", uuid.UUID(oid))  # holds the row
            seal = asyncio.create_task(e.cat.seal("sales.orders", groups[0]))
            await asyncio.sleep(0.5)
            assert not seal.done(), "the seal waits for the open write"
            await tr.commit()
            res = await seal
            assert res.members == 1
        finally:
            await w.close()
        assert await e.a.fetchval("SELECT note FROM orders WHERE id = $1", uuid.UUID(oid)) == "first"
        await sealed_refusal(e.write("UPDATE orders SET note = 'second' WHERE id = $1", uuid.UUID(oid)))


async def test_a_blocked_grouping_stays_open():  # TR23, TR30, P15.12
    async with Env("py_tier_block") as e:
        held, other = await e.order(400), await e.order(400)

        async def reservation(tx, cls, roots):
            return {held: ["reservation_open"]}

        e.cat.registry.block("sales.orders", reservation)
        await e.a.execute(f"""INSERT INTO be_outbox (event_id, stream, partition, type, occurred_at, record_id, version,
            projection, hops, payload, digest) VALUES ('{uuid.uuid7()}', 's', 0, 't', now(), '{other}', 1, 'full', 0,
            '\\x00', 'd')""")
        groups = await e.cat.groupings("sales.orders")
        res = await e.cat.seal("sales.orders", groups[0])
        assert res.blocked == {held: ["reservation_open"], other: [EVENTS_PENDING]} and res.members == 0
        r = await e.a.fetchrow("SELECT state, blocked::text AS b, (SELECT count(*) FROM be_tier_member) AS n "
                               "FROM be_tier_unit WHERE id = $1", uuid.UUID(res.unit))
        assert r["state"] == "open" and "reservation_open" in r["b"] and r["n"] == 0
        await e.write("UPDATE orders SET note = 'still writable' WHERE id = $1", uuid.UUID(held))


async def test_a_seal_waits_for_a_child_write():  # TR03 for a child
    async with Env("py_tier_child") as e:
        oid = await e.order(400)
        groups = await e.cat.groupings("sales.orders")
        w = await pgtest.admin()
        try:
            await w.execute("SET search_path = py_tier_child")
            tr = w.transaction()
            await tr.start()
            await w.execute("UPDATE order_lines SET qty = 99 WHERE order_id = $1 AND line = 1", uuid.UUID(oid))
            seal = asyncio.create_task(e.cat.seal("sales.orders", groups[0]))
            await asyncio.sleep(0.5)
            assert not seal.done(), "the line's write holds the order's row (the barrier's FOR SHARE)"
            await tr.commit()
            res = await seal
        finally:
            await w.close()
        assert res.members == 1
        await sealed_refusal(e.write("UPDATE order_lines SET qty = 1 WHERE order_id = $1", uuid.UUID(oid)))


async def test_the_barrier_holds_from_another_search_path():  # P15.13
    async with Env("py_tier_path") as e:
        oid = await e.order(400)
        groups = await e.cat.groupings("sales.orders")
        await e.cat.seal("sales.orders", groups[0])
        await e.a.execute("SET search_path = public")
        with pytest.raises(Exception) as x:
            await e.a.execute("UPDATE py_tier_path.orders SET note = 'x' WHERE id = $1", uuid.UUID(oid))
        assert "sealed archive unit" in str(x.value), "an operator naming the schema meets the barrier too"
