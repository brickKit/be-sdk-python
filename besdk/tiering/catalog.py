"""The owner's catalog: candidates and sealing (be-protocol P15.6, P15.11, P15.12). A unit groups a class's aggregates
by its group columns and the anchor's UTC month; it is created open at its first try, and sealed in one local
transaction holding FOR UPDATE on the roots it seals, so the write barrier (P15.13) serializes with it. The digest's
CPU work (every row's canonical form and the tables' hashes) goes through the catalog's compute, off the event loop:
in a shell the members share one loop (T2c)."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from besdk import canon, crash
from besdk.db.pool import DB, Tx
from besdk.tiering import rows as canonical
from besdk.tiering.classes import NEVER, Class, Table, ident

EVENTS_PENDING = "events_pending"  # the SDK's own blocker: an aggregate whose events are not published yet
DEFAULT_MAX_MEMBERS = 100_000  # one unit's bound (its digest is sorted in memory); the rest go to the next unit

# answers, for roots of a class about to be sealed, why each cannot be sealed yet (an absent root has no reason); it
# runs in the seal's transaction
Blocker = Callable[[Tx, str, list[str]], Awaitable[dict[str, list[str]]]]
# runs a CPU-heavy function off the event loop and answers its result
Compute = Callable[..., Awaitable[Any]]


async def in_thread(fn, *args):
    """The default compute: a thread (the assembly project's T2c, measured with 20,000 rows: a shell's request waits
    at most about 10 ms, the GIL's switch interval twice, while a worker process costs 18-20% more time and CPU to
    pickle the rows over and stalls no less; a Parquet read turns the file into values a batch at a time for that)."""
    return await asyncio.to_thread(fn, *args)


class Registry:
    """What a module registers with the tiering: its classes' blockers (P15.12)."""

    def __init__(self) -> None:
        self.blockers: dict[str, list[Blocker]] = {}

    def block(self, cls: str, b: Blocker) -> None:
        self.blockers.setdefault(cls, []).append(b)


@dataclass(frozen=True)
class Grouping:
    key: str  # canonical (P15.6)
    values: dict[str, str]  # the group columns' values (text)
    period: str  # YYYY-MM of the anchor in UTC


@dataclass
class SealResult:
    unit: str = ""
    members: int = 0
    digest: str = ""
    blocked: dict[str, list[str]] = field(default_factory=dict)  # root -> reasons; empty when sealed


def group_key(values: dict[str, str], period: str) -> str:
    """A grouping's canonical key (P15.6)."""
    return canon.encode({"period": period, **{"g:" + k: v for k, v in values.items()}}).decode()


def select_list(t: Table, alias: str) -> tuple[list[str], str]:
    """A table's columns in name order, each as the driver should give it for its type (numeric and jsonb as their
    text)."""
    names = sorted(t.columns)
    exprs = [f"{alias}.{ident(n)}" + ("::text" if t.columns[n] in (canonical.NUMERIC, canonical.JSONB) else "")
             for n in names]
    return names, ", ".join(exprs)


def table_sum(columns: dict[str, str], key: list[str], rows: list[dict]) -> tuple[int, str]:
    """A table's row count and hash from its rows (P15.9): the CPU-heavy part, a plain function so it can run in a
    thread or another process."""
    h = canonical.TableHash(columns, key)
    for r in rows:
        h.add(r)
    return h.rows, h.hexdigest()


@dataclass
class Catalog:
    """One member's catalog over its database."""

    db: DB
    classes: dict[str, Class]
    registry: Registry = field(default_factory=Registry)
    residence: dict[str, int] = field(default_factory=dict)  # TIERING_RESIDENCE_<class>, NEVER for never
    max_members: int = DEFAULT_MAX_MEMBERS
    compute: Compute = in_thread
    n: dt.timedelta = dt.timedelta(days=90)  # the lists' default range (TIERING_LIST_DAYS)
    grace: dt.timedelta = dt.timedelta(days=7)  # the wait after a publish (TIERING_GRACE)
    store: Any = None  # the cold route's bucket (store.Store); None without TIERING_STORE_URL
    store_locked: bool = False  # the store has object lock: no erasable class, no exports
    backups: Any = None  # when the deployment's latest backup finished (release.Backups); None: never released
    batch: int = 0  # the rows a release deletes per table and transaction (0: release.BATCH)
    orphan_age: dt.timedelta = dt.timedelta(0)  # how old an unnamed object must be before the cleanup (0: the window)
    parts: int = 0  # the rows of one Parquet object of a table (0: transfer.PART_ROWS)
    cold_sync_limit: float = 10.0  # a synchronous history read's limit, seconds
    cold_concurrency: int = 4  # the member's cold reads at once
    enqueue: Any = None  # queues a job's work item in a transaction (the runner's enqueue); None: no exports
    evidence: Any = None  # the destructions' and erasures' evidence (besdk.evidence.Writer); None: neither is done
    subject: Any = None  # a principal's authorization now, for the exports; None: none
    log: Any = None  # the catalog's alerts
    readers: dict = field(default_factory=dict)  # class -> history.Reader
    converters: dict = field(default_factory=dict)  # "<class>@<structure version>" -> reroute.Converter
    slots: Any = None  # the member's cold-read slots (asyncio.Semaphore of cold_concurrency), made on first use

    def tierable(self, name: str) -> tuple[Class, int]:
        cl = self.classes.get(name)
        if cl is None or not cl.tierable():
            raise ValueError(f"tiering: no tierable class {name!r}")
        return cl, self.residence.get(name, cl.residence_days)

    async def tx(self, point: str, fn: Callable[[Tx], Awaitable[Any]], options=None) -> Any:
        """A background transaction of the jobs with its crash points (the long simulation's)."""
        async def body(tx: Tx):
            out = await fn(tx)
            crash.point(f"tiering.{point}/commit")
            return out

        out = await self.db.background_tx(body, options)
        crash.point(f"tiering.{point}/done")
        return out

    @staticmethod
    def _candidates(cl: Class) -> str:
        """The WHERE over the root (alias r) that selects a class's sealable candidates: terminal, past the residence
        ($1 days), not a member yet ($2 the class)."""
        return (f"({cl.terminal}) AND r.{ident(cl.anchor)} <= now() - make_interval(days => $1) AND NOT EXISTS "
                f"(SELECT 1 FROM be_tier_member m WHERE m.class = $2 AND m.root_id = r.{ident(cl.root.pk)}::text)")

    @staticmethod
    def _period(cl: Class) -> str:
        return f"to_char(r.{ident(cl.anchor)} AT TIME ZONE 'UTC', 'YYYY-MM')"

    async def groupings(self, name: str) -> list[Grouping]:
        """The groupings of a class with candidates now (none when its residence is never)."""
        cl, days = self.tierable(name)
        if days == NEVER:
            return []
        sel = [f"r.{ident(g)}::text" for g in cl.group] + [self._period(cl)]
        q = (f"SELECT DISTINCT {', '.join(sel)} FROM {ident(cl.root.name)} r WHERE {self._candidates(cl)} "
             f"ORDER BY {len(sel)}")
        rows = await self.tx("groupings", lambda tx: tx.fetch(q, days, name))
        out = []
        for r in rows:
            vals = {g: r[i] for i, g in enumerate(cl.group) if r[i] is not None}
            out.append(Grouping(group_key(vals, r[len(sel) - 1]), vals, r[len(sel) - 1]))
        return out

    async def seal(self, name: str, g: Grouping) -> SealResult:
        """Tries to seal one grouping's candidates (P15.12) in one transaction. Blocked: nothing is sealed, the
        reasons are written on the open unit."""
        cl, days = self.tierable(name)
        if days == NEVER:
            raise ValueError("tiering: the class stays online")

        async def body(tx: Tx) -> SealResult:
            res = SealResult()
            await tx.execute("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", f"{name}/{g.key}")
            res.unit = await open_unit(tx, name, g.key)
            args: list[Any] = [days, name, g.period]
            where = [self._candidates(cl), f"{self._period(cl)} = $3"]
            for col in cl.group:
                if col in g.values:
                    args.append(g.values[col])
                    where.append(f"r.{ident(col)}::text = ${len(args)}")
                else:
                    where.append(f"r.{ident(col)} IS NULL")
            args.append(self.max_members or DEFAULT_MAX_MEMBERS)
            roots = [r[0] for r in await tx.fetch(
                f"SELECT r.{ident(cl.root.pk)}::text FROM {ident(cl.root.name)} r WHERE {' AND '.join(where)} "
                f"ORDER BY r.{ident(cl.root.pk)} LIMIT ${len(args)} FOR UPDATE OF r", *args)]
            if not roots:
                return res
            blocked = await self._blockers(tx, name, roots)
            if blocked:
                res.blocked = blocked
                # blocked since it came due: the latest anchor among the candidates plus the residence (the alert
                # counts 30 days from there, protocol 11)
                await tx.execute(
                    f"UPDATE be_tier_unit SET blocked = $2::jsonb, blocked_since = coalesce(blocked_since, "
                    f"(SELECT max(r.{ident(cl.anchor)}) FROM {ident(cl.root.name)} r WHERE r.{ident(cl.root.pk)}::text "
                    f"= ANY($3)) + make_interval(days => $4)) WHERE id = $1",
                    uuid.UUID(res.unit), json.dumps(blocked), roots, days)
                return res
            await tx.execute("INSERT INTO be_tier_member (class, root_id, unit) SELECT $1, unnest($2::text[]), $3",
                             name, roots, uuid.UUID(res.unit))
            res.digest = await self.digest(tx, cl, roots)
            res.members = len(roots)
            await tx.execute(
                f"UPDATE be_tier_unit SET state = 'sealed', members = $2, digest = $3, sealed_at = now(), blocked = NULL, "
                f"blocked_since = NULL, anchor_max = (SELECT max(r.{ident(cl.anchor)}) FROM {ident(cl.root.name)} r "
                f"WHERE r.{ident(cl.root.pk)}::text = ANY($4)), newest_created = (SELECT max(r.{ident(cl.created)}) "
                f"FROM {ident(cl.root.name)} r WHERE r.{ident(cl.root.pk)}::text = ANY($4)) WHERE id = $1",
                uuid.UUID(res.unit), len(roots), res.digest, roots)
            return res

        return await self.tx("seal", body)

    async def _blockers(self, tx: Tx, name: str, roots: list[str]) -> dict[str, list[str]]:
        """The SDK's own (events not published yet) and the module's, merged per root."""
        out: dict[str, list[str]] = {}
        for r in await tx.fetch("SELECT DISTINCT record_id FROM be_outbox WHERE published_at IS NULL "
                                "AND record_id = ANY($1)", roots):
            out.setdefault(r[0], []).append(EVENTS_PENDING)
        for b in self.registry.blockers.get(name, []):
            for root, why in (await b(tx, name, roots)).items():
                out.setdefault(root, []).extend(why)
        return {r: sorted(set(w)) for r, w in out.items()}

    async def digest(self, tx: Tx, cl: Class, roots: list[str]) -> str:
        """The sealed content's digest (P15.9) from the roots and their children, in the seal's transaction; the
        hashing through compute."""
        sums = []

        async def add(t: Table, where: str) -> None:
            names, cols = select_list(t, "t")
            fetched = await tx.fetch(f"SELECT {cols} FROM {ident(t.name)} t WHERE {where}", roots)
            data = [{n: r[i] for i, n in enumerate(names)} for r in fetched]
            k, h = await self.compute(table_sum, t.columns, t.key_columns(), data)
            sums.append((t.name, k, h))

        await add(cl.root, f"t.{ident(cl.root.pk)}::text = ANY($1)")
        for ch in cl.children:
            await add(ch.table, f"{cl.root_expr(ch, 't')} = ANY($1)")
        return canonical.unit_digest(cl.name, cl.schema_version, sums)


async def open_unit(tx: Tx, name: str, grouping: str) -> str:
    """The grouping's open unit, created (with the next sequence number) when there is none."""
    got = await tx.fetchval("SELECT id::text FROM be_tier_unit WHERE class = $1 AND grouping = $2 AND state = 'open'",
                            name, grouping)
    if got is not None:
        return got
    uid = uuid.uuid7()
    await tx.execute("""INSERT INTO be_tier_unit (id, class, grouping, seq, state) VALUES ($1, $2, $3,
        (SELECT coalesce(max(seq), 0) + 1 FROM be_tier_unit WHERE class = $2 AND grouping = $3), 'open')""",
                     uid, name, grouping)
    return str(uid)
