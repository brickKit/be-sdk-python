"""Monthly partitions (P5.21-P5.23, CK-DB-08): a component's migration declares a table partitioned by business date;
the SDK's job, as the member's runtime role, creates the missing months ahead through be_maintain_partitions(); a
detached partition of an expected name fails it. The SQL functions are the protocol's migrations, shared with the Go
SDK; this checks the Python side reaches them as the protocol says."""
import datetime as dt
import io

import pytest

from besdk import obs
from besdk.db.partitions import maintain_job, maintain_partitions
from besdk.work import Runner
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
PARTITIONED = {"0001_orders.up.sql": """
CREATE TABLE orders (id uuid NOT NULL, business_date date NOT NULL, amount numeric NOT NULL CHECK (amount >= 0),
    PRIMARY KEY (id, business_date)) PARTITION BY RANGE (business_date);
SELECT be_declare_monthly_partitions('orders', 2);"""}
LOG = obs.new_logger(obs.Member("demo/part", "3.0.0"), "error", io.StringIO())


def months(first: dt.date, n: int) -> list[str]:
    out, y, m = [], first.year, first.month
    for _ in range(n + 1):
        out.append(f"orders_p{y:04d}{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


async def partitions(a, schema):
    return [r[0] for r in await a.fetch("""SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = $1::regclass ORDER BY 1""", f"{schema}.orders")]


async def test_the_job_keeps_the_months_ahead():
    a = await pgtest.admin()
    await a.execute("DROP SCHEMA IF EXISTS py_part1 CASCADE")
    v, _ = await migrated("py_part1", PARTITIONED)
    d, pool = await open_db("py_part1", v)
    try:
        month = (await a.fetchval("SELECT date_trunc('month', now() AT TIME ZONE 'UTC')")).date()
        assert await partitions(a, "py_part1") == months(month, 2), "declaring creates this month and two ahead"
        assert await d.background_tx(maintain_partitions) == [], "maintaining again creates nothing"

        last = months(month, 2)[2]
        await a.execute(f"DROP TABLE py_part1.{last}")
        w = Runner(d, "demo/part1", LOG)
        w.add(maintain_job(LOG))
        await w._ensure_singleton(w.jobs["be.partitions.maintain"])
        await w.run(await w.claim())
        assert await partitions(a, "py_part1") == months(month, 2), "the job, as the runtime role, made it again"
        assert await a.fetchval("SELECT state FROM py_part1.be_work_item WHERE id = 'be.partitions.maintain'") == "pending"

        await a.execute(f"ALTER TABLE py_part1.orders DETACH PARTITION py_part1.{last}")
        with pytest.raises(Exception, match="detached"):
            await d.background_tx(maintain_partitions)
    finally:
        await pool.close()
        await a.close()
