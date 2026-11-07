"""A member's tiering: its configuration keys, its catalog and the candidates job (be-protocol P15.1, P15.3, P15.4,
P15.11, P15.35): the deployment may lengthen a class's residence (or keep it online), give its retention at go-live
and choose its route among the ones the class allows; the class's tables are checked against the database at start
(every column declared with its type, none missing, none more)."""
from __future__ import annotations

import datetime as dt
from dataclasses import replace

from besdk.config import Key, Type
from besdk.db.pool import DB, Tx
from besdk.obs import Logger
from besdk.tiering.catalog import Catalog
from besdk.tiering.classes import NEVER, OBJECT, Class, Retention
from besdk.tiering.plan import BLOCKED_ALERT_AFTER
from besdk.tiering.rows import (BOOL, BYTEA, DATE, INT2, INT4, INT8, JSONB, NUMERIC, TEXT, TEXT_ARRAY, TIMESTAMPTZ,
                                UUID)

WINDOW, LIST_DAYS, GRACE = "TIERING_WINDOW", "TIERING_LIST_DAYS", "TIERING_GRACE"
COLD_SYNC_LIMIT, COLD_CONCURRENCY = "TIERING_COLD_SYNC_LIMIT", "TIERING_COLD_CONCURRENCY"
# the database's types a column type stands for (information_schema's data_type; a text array is ARRAY of _text)
SQL_TYPES = {TEXT: ("text", "character varying", "character"), INT2: ("smallint",), INT4: ("integer",),
             INT8: ("bigint",), NUMERIC: ("numeric",), BOOL: ("boolean",), UUID: ("uuid",),
             TIMESTAMPTZ: ("timestamp with time zone",), DATE: ("date",), JSONB: ("jsonb",), BYTEA: ("bytea",),
             TEXT_ARRAY: ("ARRAY:_text",)}


def _suffix(cls: str) -> str:
    return cls.replace(".", "_").replace("-", "_").upper()


def residence_key(cls: str) -> str:
    """Overrides a class's residence: days, or "never" (P15.3)."""
    return "TIERING_RESIDENCE_" + _suffix(cls)


def retention_key(cls: str) -> str:
    """A class's retention given at deployment, "<shortest days>/<longest days>/<basis>" (P15.4)."""
    return "TIERING_RETENTION_" + _suffix(cls)


def route_key(cls: str) -> str:
    """A class's route chosen at deployment among the ones it allows (P15.35)."""
    return "TIERING_ROUTE_" + _suffix(cls)


def retention_of(raw: str) -> Retention:
    parts = [p.strip() for p in raw.split("/", 2)]
    try:
        if len(parts) != 3:
            raise ValueError
        lo, hi = int(parts[0]), int(parts[1])
        if lo <= 0 or hi < lo or not parts[2]:
            raise ValueError
    except ValueError:
        raise ValueError(f"{raw!r}: <shortest days>/<longest days>/<basis>, the shortest above 0, the longest not "
                         "below it, and a basis") from None
    return Retention(lo, hi, parts[2])


def keys(classes: list[Class]) -> list[Key]:
    """The keys of a member that declares classes (the cold store's are the store's, store.keys)."""
    from besdk.tiering import store

    out = [Key(WINDOW, Type.STRING, default="0 2 * * *"), Key(LIST_DAYS, Type.INTEGER, default=90, min=1),
           Key(GRACE, Type.DURATION, default=dt.timedelta(days=7)),
           Key(COLD_SYNC_LIMIT, Type.DURATION, default=dt.timedelta(seconds=10)),
           Key(COLD_CONCURRENCY, Type.INTEGER, default=4, min=1)]
    for c in classes:
        if c.tierable():
            out += [Key(residence_key(c.name), Type.STRING), Key(retention_key(c.name), Type.STRING),
                    Key(route_key(c.name), Type.STRING)]
    return out + store.keys()


async def check_columns(d: DB, cl: Class) -> list[str]:
    """A class's tables against the database's (P15.1): a column the class does not declare is not copied, and the
    release deletes the whole row."""
    problems: list[str] = []

    async def read(tx: Tx) -> None:
        for t in cl.tables():
            have = {r[0]: r[1] for r in await tx.fetch(
                """SELECT column_name, CASE WHEN data_type = 'ARRAY' THEN 'ARRAY:' || udt_name ELSE data_type END
                FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1""", t.name)}
            if not have:
                problems.append(f"table {t.name} is not in the database")
                continue
            for name, typ in have.items():
                if name not in t.columns:
                    problems.append(f"{t.name}.{name} ({typ}) is not declared: it would not be copied, and the "
                                    "release deletes the row")
                elif typ not in SQL_TYPES[t.columns[name]]:
                    problems.append(f"{t.name}.{name} is {typ} in the database, declared {t.columns[name]}")
            problems.extend(f"{t.name}.{n} is declared but not in the database" for n in t.columns if n not in have)

    await d.background_tx(read)
    return sorted(problems)


async def new_catalog(d: DB, classes: list[Class], values, prefix: str, log: Logger | None = None) -> Catalog:
    """Checks the classes (P15.1-P15.5; the SDK's own tables, be_*, are never a class's) and the configured
    residences against N, and builds the member's catalog with its cold store (prefix: the component's in the
    bucket). A class whose route is the object store needs one; an erasable class may not go to a locked bucket
    (P15.32). Raises ValueError."""
    from besdk.tiering import store as cold

    n = values.int(LIST_DAYS)
    st = cold.Store.from_values(values, prefix)
    cat = Catalog(d, {}, n=dt.timedelta(days=n), grace=values.duration(GRACE), store=st,
                  cold_sync_limit=values.duration(COLD_SYNC_LIMIT).total_seconds(),
                  cold_concurrency=values.int(COLD_CONCURRENCY), log=log)
    locked = await st.locked() if st is not None else False
    cat.store_locked = locked
    for cl in classes:
        if cl.tierable() and (raw := values.string(route_key(cl.name))):  # the deployment's route (P15.35)
            if raw not in cl.routes:
                raise ValueError(f"{route_key(cl.name)}: {raw!r} is not one of the routes {cl.name} allows {list(cl.routes)}")
            cl = replace(cl, routes=(raw, *(r for r in cl.routes if r != raw)))
        if cl.tierable() and (raw := values.string(retention_key(cl.name))):  # the deployment's retention (P15.4)
            try:
                cl = replace(cl, retention=retention_of(raw))
            except ValueError as e:
                raise ValueError(f"{retention_key(cl.name)}: {e}") from None
        if problems := cl.check(n, tuple(t.name for t in cl.tables() if t.name.startswith("be_"))):
            raise ValueError("; ".join(problems))
        if cl.tierable() and (problems := await check_columns(d, cl)):
            raise ValueError(f"{cl.name}: the declaration is not the tables (P15.1): {'; '.join(problems)}")
        if cl.name in cat.classes:
            raise ValueError(f"tiering: class {cl.name} declared twice")
        cat.classes[cl.name] = cl
        if not cl.tierable():
            continue
        if OBJECT in cl.routes:
            if st is None and cl.routes[0] == OBJECT:
                raise ValueError(f"{cl.name}: its route is the object store but {cold.STORE_URL} is not set")
            if locked and cl.erasable:
                raise ValueError(f"{cl.name}: erasable columns may not go to the object-locked bucket "
                                 f"{st.bucket} (P15.32)")  # type: ignore[union-attr]
        raw = values.string(residence_key(cl.name))
        if raw == "never":
            cat.residence[cl.name] = NEVER
        elif raw:
            if not raw.isdigit() or int(raw) < n:
                raise ValueError(f"{residence_key(cl.name)}: {raw!r} is not a residence of at least N = {n} days or "
                                 "never (P15.3)")
            cat.residence[cl.name] = int(raw)
    return cat


def pending_retentions(cat: Catalog) -> list[str]:
    """The tierable classes whose retention is pending: never destroyed, listed at go-live (P15.4, TR24)."""
    return sorted(n for n, c in cat.classes.items() if c.tierable() and c.retention.pending)


async def seal_due(cat: Catalog, log: Logger) -> None:
    """Seals every grouping that has come due, and alerts on the units blocked too long (P15.11, protocol 11)."""
    for name, cl in cat.classes.items():
        if not cl.tierable():
            continue
        for g in await cat.groupings(name):
            res = await cat.seal(name, g)
            if not res.blocked:
                log.info("an archive unit is sealed", event="tiering_sealed", **{"class": name}, unit=res.unit,
                         members=res.members)
    for r in await cat.tx("blocked", lambda tx: tx.fetch(
            """SELECT id::text AS id, class, grouping, blocked::text AS blocked, blocked_since FROM be_tier_unit
            WHERE state = 'open' AND blocked IS NOT NULL AND blocked_since <= now() - make_interval(days => $1)""",
            BLOCKED_ALERT_AFTER.days)):
        log.error("an archive unit is blocked past its residence", event="tiering_blocked", **{"class": r["class"]},
                  unit=r["id"], grouping=r["grouping"], due_since=r["blocked_since"].isoformat(), blocked=r["blocked"])


def candidates_job(cat: Catalog, schedule: str, log: Logger):
    """be.tiering.candidates: seals what has come due, in the deployment's low-peak window (P15.11)."""
    from besdk.work.job import CRON, LOCAL, UNLIMITED, Job

    async def handle(run) -> None:
        await seal_due(cat, log)
        await run.complete()

    return Job("be.tiering.candidates", CRON, handle, LOCAL, cron=schedule, time_zone="UTC", max_attempts=UNLIMITED,
               timeout=3600.0)
