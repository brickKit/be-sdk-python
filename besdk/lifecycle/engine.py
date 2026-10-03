"""The lifecycle engine, P0 (be-protocol P16.2, P16.5–P16.7, P16.10): the singleton job ``be.lifecycle``.

Each run keeps every range-partitioned table's window ahead (the tables ``lifecycle.yaml`` declares with a
``grain`` and the platform's ``besdk_outbox``), records the units in ``besdk_lifecycle_units`` and drops
outbox partitions whose rows are all published and whose week ended 14 days ago. The runtime role has no
DDL: partitions are created and dropped only through the platform's SECURITY DEFINER functions. One step is
one transaction holding the step lock ``(be.lifecycle, table)`` under a short ``lock_timeout``. ``DATA_LIFECYCLE``
``mode`` ``off`` does nothing, ``dry-run`` logs the plan. Hot and warm tiers only: no cold store in 1.0.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from besdk.lifecycle.decl import Declaration, units

if TYPE_CHECKING:
    from besdk.runtime import Runtime
    from besdk.store.tx import Tx

UTC = timezone.utc
OUTBOX_AHEAD, OUTBOX_KEEP_DAYS, STEP_LOCK_TIMEOUT = 2, 14, 2.0

_ENSURE = "SELECT besdk_ensure_range_partition($1, $2, $3, $4)"
_UNIT = ("INSERT INTO besdk_lifecycle_units (table_name, unit_key, range_from, range_to, state) "
         "VALUES ($1, $2, $3, $4, 'ACTIVE') ON CONFLICT (table_name, unit_key) DO NOTHING")
_LOG = ("INSERT INTO besdk_lifecycle_log (table_name, unit_key, action, actor, detail) "
        "VALUES ($1, $2, $3, $4, $5::jsonb)")
_OLD_OUTBOX = ("SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bound FROM pg_inherits i "
               "JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'besdk_outbox'::regclass")
_DROP = "SELECT besdk_drop_partition('besdk_outbox', $1)"
_SEAL = "SELECT besdk_seal_table($1)"
_SEALED = ("INSERT INTO besdk_lifecycle_units (table_name, unit_key, state, sealed_at) VALUES ($1, $2, 'SEALED', now()) "
           "ON CONFLICT (table_name, unit_key) DO UPDATE SET state = 'SEALED', sealed_at = now(), "
           "version = besdk_lifecycle_units.version + 1, updated_at = now()")


def mode_of(rt: "Runtime") -> str:
    if "DATA_LIFECYCLE" not in rt.config.declared():
        return "on"
    v = rt.config.json("DATA_LIFECYCLE", {"mode": "on"}) or {}
    return str(v.get("mode", "on"))


async def log(tx: "Tx", table: str, unit: str, action: str, actor: str, detail: dict) -> None:
    await tx.execute(_LOG, table, unit, action, actor, json.dumps(detail, default=str))


class Engine:
    def __init__(self, rt: "Runtime", decl: Declaration):
        self.rt, self.decl = rt, decl

    def windows(self, now: datetime) -> list[tuple[str, str, datetime, datetime]]:
        """(table, unit, lower, upper) the window must hold now."""
        out = [("besdk_outbox", n, lo, hi) for n, lo, hi in units("besdk_outbox", "week", now, ahead=OUTBOX_AHEAD)]
        for name in self.decl.partitioned():
            t = self.decl.tables[name]
            out += [(name, n, lo, hi) for n, lo, hi in units(name, t.grain, now, ahead=t.ahead)]
        return out

    async def run(self) -> None:
        mode = mode_of(self.rt)
        if mode == "off":
            return
        now = datetime.now(UTC)
        plan = self.windows(now)
        if mode == "dry-run":
            self.rt.logger.info("lifecycle_plan", extra={"units": ",".join(u for _, u, _, _ in plan)})
            return
        for table in sorted({t for t, *_ in plan}):
            await self.rt.store().tx(lambda tx, table=table: self._window(tx, table, plan),
                                     lock_timeout=STEP_LOCK_TIMEOUT)
        await self.rt.store().tx(lambda tx: self._drop_old_outbox(tx, now), lock_timeout=STEP_LOCK_TIMEOUT)

    async def _window(self, tx: "Tx", table: str, plan: list) -> None:
        await tx.lock("be.lifecycle", table)
        for t, unit, lo, hi in plan:
            if t != table:
                continue
            if await tx.fetchval(_ENSURE, t, unit, lo, hi):
                await log(tx, t, unit, "partition_created", "be.lifecycle", {"from": lo, "to": hi})
            if t != "besdk_outbox":
                await tx.execute(_UNIT, t, unit, lo, hi)

    async def _drop_old_outbox(self, tx: "Tx", now: datetime) -> None:
        await tx.lock("be.lifecycle", "besdk_outbox")
        for r in await tx.fetch(_OLD_OUTBOX):
            hi = _upper(r["bound"])
            if hi is None or (now - hi).days < OUTBOX_KEEP_DAYS:
                continue
            pending = await tx.fetchval(f"SELECT count(*) FROM {_ident(r['relname'])} WHERE status <> 'PUBLISHED'")
            if pending:
                continue
            await tx.fetchval(_DROP, r["relname"])
            await log(tx, "besdk_outbox", r["relname"], "partition_dropped", "be.lifecycle", {"to": hi})


async def seal(tx: "Tx", table: str, unit: str, actor: str) -> None:
    """Install the seal guard on a unit (a partition, or the table itself when ``unit`` is empty) (P16.5)."""
    await tx.lock("be.lifecycle", table)
    await tx.fetchval(_SEAL, unit or table)
    await tx.execute(_SEALED, table, unit or table)
    await log(tx, table, unit or table, "sealed", actor, {})


def _upper(bound: str) -> datetime | None:
    """``FOR VALUES FROM ('…') TO ('…')`` → the upper bound."""
    try:
        text = bound.split(" TO ('", 1)[1].split("')", 1)[0]
        return datetime.fromisoformat(text.replace(" ", "T")).astimezone(UTC)
    except (IndexError, ValueError):
        return None


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'
