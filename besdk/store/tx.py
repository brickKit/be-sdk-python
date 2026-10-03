"""``Tx``: one transaction on one connection (apis §3). ``fetch`` / ``fetchrow`` / ``fetchval`` / ``execute`` /
``executemany`` pass through to asyncpg with the member's ``/* be:<schema> */`` prefix (P10.2, repro r1-04);
``publish`` writes the outbox (P12.1); ``lock`` / ``try_lock`` take transaction-level advisory locks (P10.8)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    from besdk.events.model import Event
    from besdk.idem import Command, Prior
    from besdk.store.store import Store

_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':' || $1), hashtext($2))"
_TRY_SQL = "SELECT pg_try_advisory_xact_lock(hashtext(current_schema() || ':' || $1), hashtext($2))"


class Tx:
    def __init__(self, store: "Store", conn: Any, req: dict | None):
        self.store = store
        self.conn = conn
        self._prefix = f"/* be:{store.identity.schema} */ "
        self._req = req

    async def _run(self, method: str, sql: str, *args: Any, **kw: Any) -> Any:
        if self._req is not None:
            self._req["db_active"] = True
        try:
            return await getattr(self.conn, method)(self._prefix + sql, *args, **kw)
        finally:
            if self._req is not None:
                self._req["db_active"] = False

    async def fetch(self, sql: str, *args: Any, **kw: Any) -> list:
        return await self._run("fetch", sql, *args, **kw)

    async def fetchrow(self, sql: str, *args: Any, **kw: Any) -> Any:
        return await self._run("fetchrow", sql, *args, **kw)

    async def fetchval(self, sql: str, *args: Any, **kw: Any) -> Any:
        return await self._run("fetchval", sql, *args, **kw)

    async def execute(self, sql: str, *args: Any, **kw: Any) -> str:
        return await self._run("execute", sql, *args, **kw)

    async def executemany(self, sql: str, args: Sequence[Sequence[Any]], **kw: Any) -> None:
        return await self._run("executemany", sql, args, **kw)

    async def lock(self, name: str, *parts: str) -> None:
        """Wait for the transaction-level advisory lock ``(schema:name, parts joined by '|')``."""
        await self._run("execute", _LOCK_SQL, name, "|".join(parts))

    async def try_lock(self, name: str, *parts: str) -> bool:
        return bool(await self._run("fetchval", _TRY_SQL, name, "|".join(parts)))

    async def enqueue(self, kind: str, args: Any, *, run_at: Any = None, unique_key: str | None = None) -> bool:
        """Queue a job in this transaction (P14 queue); False when ``unique_key`` already has a live job."""
        from besdk.jobs.queue import enqueue

        return await enqueue(self, kind, args, run_at=run_at, unique_key=unique_key)

    async def publish(self, ev: "Event") -> None:
        """Write the event to the outbox in this transaction (P12.1); the pump publishes it after commit."""
        from besdk.events.outbox import write

        await write(self, ev)

    # --- idempotency (P13): the same statements as besdk.idempotent, step by step -------------------

    async def idem_lookup(self, cmd: "Command") -> "Prior":
        from besdk import idem

        return await idem.lookup(self, cmd)

    async def idem_claim(self, cmd: "Command") -> "Prior":
        from besdk import idem

        return await idem.claim(self, cmd)

    async def idem_complete(self, cmd: "Command", result: Any) -> None:
        from besdk import idem

        await idem.complete(self, cmd, result)

    async def idem_release(self, cmd: "Command") -> None:
        from besdk import idem

        await idem.release(self, cmd)
