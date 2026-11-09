"""G4 (assembly protocol 11's replacement checks: every read implementation answers the same rows), the same case as
be-sdk-go's tiering/consistency_integration_test.go: the same history read answers the same orders, field for field,
online, sealed, published, released, moved to the other route and with the old version released; the archived orders
read by their key are the same from either route."""
import datetime as dt
from dataclasses import replace

import pytest

from besdk.tiering import history, reads, release, reroute, transfer
from besdk.tiering.classes import OBJECT, POSTGRES
from tests.test_tiering_history_integration import ALICE, ORDER_DEFS, Grants, line, order_reader
from tests.test_tiering_reads_integration import by_line
from tests.test_tiering_release_integration import backup_at
from tests.test_tiering_seal_integration import Env
from tests.test_tiering_transfer_integration import BUCKET, ensure_bucket, store_for, with_route

pytestmark = pytest.mark.integration


async def test_the_same_read_answers_the_same_at_every_step():
    from besdk.authz import Subject

    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_same_read") as e:
            e.cat.store = st
            with_route(e, OBJECT)
            g = Grants()
            g.entities = [e.le]
            ORDER_DEFS.validate()
            sub = Subject(ALICE, ORDER_DEFS, g, dt.datetime.now(dt.timezone.utc))  # type: ignore[arg-type]
            e.slow = 0
            history.register(e.cat, "sales.orders", order_reader(e))
            await e.order(-1)
            archived = [await e.order(400), await e.order(400)]
            await e.order(150)  # closed, not yet past the residence

            async def read(step: str) -> list[str]:
                out, pending = await history.history(e.cat, sub, "sales.orders")
                assert pending is None, step
                return sorted(line(o) for o in out)

            async def by_key(route: str) -> dict:
                out = {}
                for oid in archived:
                    a = await reads.get(e.cat, "sales.orders", oid)
                    assert a is not None and a.route == route
                    a = replace(a, unit="", route="",
                                children={**a.children, "order_lines": by_line(a.children["order_lines"])})
                    out[oid] = a
                return out

            want = await read("online")
            assert len(want) == 4
            groups = await e.cat.groupings("sales.orders")
            assert len(groups) == 1
            sealed = await e.cat.seal("sales.orders", groups[0])
            assert sealed.members == 2
            assert await read("sealed") == want
            await transfer.transfer(e.cat, sealed.unit)
            assert await read("published") == want
            from_objects = await by_key(OBJECT)

            e.cat.grace = dt.timedelta(0)
            e.cat.backups = backup_at(dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1))
            assert await release.release(e.cat, sealed.unit) == []
            assert await read("released") == want

            await reroute.reroute(e.cat, sealed.unit, POSTGRES)
            assert await read("moved to the warm route") == want
            assert await by_key(POSTGRES) == from_objects, "the same orders by key from either route"
            assert await reroute.release_old(e.cat, sealed.unit) == []
            assert await read("the old version released") == want
            assert await by_key(POSTGRES) == from_objects
    finally:
        await st.close()
