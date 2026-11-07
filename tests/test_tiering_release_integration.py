"""The release (P15.13, P15.19, P15.21), the same case as be-sdk-go's tiering/release_integration_test.go: it waits for
the grace, a backup after the publish, every lease taken before the route changed and the unit's newest record to be N
days old; then only the unit's rows go (children first), the unit is released and the catalog version moves; another
unit's name lets no write through."""
import datetime as dt
import uuid

import pytest

from besdk import errs
from besdk.tiering import release, transfer
from besdk.tiering.classes import POSTGRES
from besdk.tiering.plan import GRACE, LEASE, NO_BACKUP, RECENT
from tests.test_tiering_seal_integration import Env
from tests.test_tiering_transfer_integration import sealed_unit, with_route

pytestmark = pytest.mark.integration


def backup_at(at):
    async def latest():
        return at
    return latest


async def test_the_release_conditions():  # P15.19, TR06, TR25
    async with Env("py_tier_release") as e:
        with_route(e, POSTGRES)
        e.cat.n, e.cat.grace = dt.timedelta(days=90), dt.timedelta(days=7)
        open_ = await e.order(-1)
        unit = await sealed_unit(e)
        await transfer.transfer(e.cat, unit)
        assert await release.release(e.cat, unit) == [GRACE, NO_BACKUP], "published now, no run profile"
        now = dt.datetime.now(dt.timezone.utc)
        e.cat.grace, e.cat.backups = dt.timedelta(0), backup_at(now - dt.timedelta(hours=1))  # before the publish
        assert await release.release(e.cat, unit) == [NO_BACKUP], "the backup finished before the publish"
        e.cat.backups = backup_at(now + dt.timedelta(minutes=1))
        await e.a.execute(f"INSERT INTO be_tier_lease VALUES ('{uuid.uuid7()}', 0, 'export', now() + interval '15 minutes')")
        assert await release.release(e.cat, unit) == [LEASE], "an export reading under the catalog before the publish"
        await e.a.execute("UPDATE be_tier_lease SET expires_at = now() - interval '1 second'")
        await e.a.execute("UPDATE be_tier_unit SET newest_created = now() - interval '30 days' WHERE id = $1",
                          uuid.UUID(unit))
        assert await release.release(e.cat, unit) == [RECENT], "a record created 30 days ago (TR25)"
        await e.a.execute("UPDATE be_tier_unit SET newest_created = now() - interval '91 days' WHERE id = $1",
                          uuid.UUID(unit))

        async def foreign(tx):  # the SDK's own work names its unit: another unit's name lets nothing through
            await tx.execute("SELECT set_config('be.tier_unit', $1, true)", str(uuid.uuid7()))
            await tx.execute("DELETE FROM order_lines WHERE order_id IN (SELECT root_id::uuid FROM be_tier_member "
                             "WHERE unit = $1)", uuid.UUID(unit))
        with pytest.raises(errs.Error) as x:
            await e.d.tx(foreign)
        assert x.value.reason == "RECORD_SEALED"
        e.cat.batch = 1  # a line per transaction: the orders go only once no line is left
        before = await e.a.fetchval("SELECT version FROM be_tier_catalog")
        assert await release.release(e.cat, unit) == []
        assert await e.a.fetchval("SELECT version FROM be_tier_catalog") == before + 1
        assert await e.a.fetchval("SELECT state FROM be_tier_unit WHERE id = $1", uuid.UUID(unit)) == "released"
        assert await e.a.fetchval("SELECT count(*) FROM orders o JOIN be_tier_member m ON m.root_id = o.id::text") == 0
        assert await e.a.fetchval("SELECT count(*) FROM order_lines l JOIN be_tier_member m "
                                  "ON m.root_id = l.order_id::text") == 0
        assert await e.a.fetchval("SELECT count(*) FROM orders WHERE id = $1", uuid.UUID(open_)) == 1, "the open stays"
        assert await e.a.fetchval("SELECT count(*) FROM order_lines WHERE order_id = $1", uuid.UUID(open_)) == 2
        assert await e.a.fetchval("SELECT count(*) FROM be_tier_member WHERE unit = $1", uuid.UUID(unit)) == 2, \
            "the locate index stays"
