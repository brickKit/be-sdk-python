"""The tiering simulation's data and its replica (foundations item 6, T10; be-sdk-go's tiering/sim_data_test.go and
sim_child_test.go): one class of three levels and every column type, one erasable column at two levels, its route
chosen by the deployment. A replica is `python -m tests.sim.tierchild` with SIM_TIER_CHILD (JSON: its members, the
crash point and after how many hits): it hosts one member, or two as a shell does, each with the catalog the host
builds from the deployment's configuration and every tiering job on a runner of its own, their intervals in seconds.
With a crash point it ends itself (exit 3, "CRASH <point>" on stdout) the after-th time it gets there."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import sys
from dataclasses import replace

from besdk import config, crash, db, obs
from besdk.config import MapSource
from besdk.db.pool import DB, Pool
from besdk.evidence import Writer
from besdk.tiering import destroy, erase, member, release, transfer
from besdk.tiering import rows as canonical
from besdk.tiering.classes import COMPLETED, OBJECT, POSTGRES, Child, Class, Retention, Table
from besdk.tiering.rows import (BOOL, BYTEA, DATE, INT2, INT4, INT8, JSONB, NUMERIC, TEXT, TEXT_ARRAY, TIMESTAMPTZ,
                                UUID)
from besdk.work import SINGLETON
from besdk.work.runner import Runner

SIM_CLASS = "sim.docs"
SCHEMA_SQL = """CREATE TABLE docs (
  id uuid PRIMARY KEY, grp text NOT NULL, rev integer NOT NULL, code text NOT NULL, note text,
  amount numeric(19,4) NOT NULL, qty bigint NOT NULL, small smallint NOT NULL, flag boolean NOT NULL, day date,
  attrs jsonb, blob bytea, tags text[], closed_at timestamptz, created_at timestamptz NOT NULL);
CREATE TABLE doc_lines (
  id uuid PRIMARY KEY, doc_id uuid NOT NULL REFERENCES docs (id), line integer NOT NULL, label text, person text);
CREATE TABLE line_marks (
  line_id uuid NOT NULL REFERENCES doc_lines (id), seq integer NOT NULL, mark text, PRIMARY KEY (line_id, seq));
CREATE TABLE sim_backup (finished_at timestamptz NOT NULL);"""
# the verification window before an orphan goes (P15.20): longer than the longest transfer, or the cleanup takes a
# transfer's objects before it verifies them (be-sdk-go found 20 s too short under the simulation's load)
ORPHAN_AGE = dt.timedelta(seconds=90)
CLEANUP_EVERY = 30.0  # the replicas' cleanup interval: each run lists every object of the class
PARTS = 40  # a unit's tables span several Parquet objects (the data volume reaches the boundaries)


def sim_class() -> Class:
    return Class(SIM_CLASS, COMPLETED,
                 Table("docs", {"id": UUID, "grp": TEXT, "rev": INT4, "code": TEXT, "note": TEXT, "amount": NUMERIC,
                                "qty": INT8, "small": INT2, "flag": BOOL, "day": DATE, "attrs": JSONB, "blob": BYTEA,
                                "tags": TEXT_ARRAY, "closed_at": TIMESTAMPTZ, "created_at": TIMESTAMPTZ}, pk="id"),
                 anchor="closed_at", created="created_at", terminal="r.closed_at IS NOT NULL", residence_days=365,
                 retention=Retention(3650, 4000, "the simulation"), schema_version="1",
                 children=(Child(Table("doc_lines", {"id": UUID, "doc_id": UUID, "line": INT4, "label": TEXT,
                                                     "person": TEXT}, pk="id"), "doc_id"),
                           Child(Table("line_marks", {"line_id": UUID, "seq": INT4, "mark": TEXT},
                                       key=("line_id", "seq")), "line_id", "doc_lines")),
                 group=("grp",), locate=("grp",), erasable=("note", "person"), routes=(POSTGRES, OBJECT),
                 hold_operation="sim.hold", approve_operation="sim.approve")


class Keeper:
    """A replica's evidence store: it takes every record (the simulation checks the data, not the store)."""

    async def append(self, recs):
        return ["accepted"] * len(recs)


def backups_of(d: DB):
    """The deployment's latest backup: the newest row of sim_backup (the parent records one every few seconds)."""
    async def latest():
        return await d.background_tx(lambda tx: tx.fetchval("SELECT max(finished_at) FROM sim_backup"))
    return latest


async def catalog_of(m: dict, instance: str, log):
    """A member's database and catalog as the host builds them, with the simulation's backups and evidence store."""
    classes = [sim_class()]
    values = config.parse([*db.serve_keys(), *member.keys(classes)], MapSource(m["env"]))
    s = db.settings_from(values)
    pool = await Pool.open(s.target, s.user, s.password, "test/" + m["name"], 8, s.prepared)
    d = DB("test/" + m["name"], pool, s)
    cat = await member.new_catalog(d, classes, values, m["prefix"], log)
    cat.backups = backups_of(d)
    cat.evidence = Writer(d, "test/" + m["name"], instance, log, store=Keeper())
    cat.batch, cat.orphan_age, cat.parts = 7, ORPHAN_AGE, PARTS
    return d, pool, cat


def fast(job, every: float):
    """A job of the SDK with the simulation's interval: a cron job becomes a singleton."""
    return replace(job, kind=SINGLETON, cron="", time_zone="", interval=every)


async def canonical_rows(tx, t: Table, where: str, *args) -> list[dict]:
    """Rows of a class table as canonical rows (P15.8), as the copy holds them."""
    names = sorted(t.columns)
    exprs = [f"t.{n}::text AS {n}" if t.columns[n] in (NUMERIC, JSONB) else f"t.{n}" for n in names]
    return [canonical.row(t.columns, dict(r))
            for r in await tx.fetch(f"SELECT {', '.join(exprs)} FROM {t.name} t WHERE {where}", *args)]


def _text(v) -> str:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def aggregate(root: dict, children: dict[str, list[dict]]) -> str:
    """One document as JSON: its root and every child table's rows in a fixed order (empty tables left out)."""
    kids = {name: sorted(_text(r) for r in rs) for name, rs in children.items() if rs}
    return _text({"root": root, "children": kids})


async def read_online(d: DB, oid) -> str | None:
    """A document from the online tables; None: not there."""
    cl = sim_class()

    async def body(tx):
        roots = await canonical_rows(tx, cl.root, "t.id = $1", oid)
        if not roots:
            return None
        lines = await canonical_rows(tx, cl.children[0].table, "t.doc_id = $1", oid)
        marks = await canonical_rows(tx, cl.children[1].table,
                                     "t.line_id IN (SELECT id FROM doc_lines WHERE doc_id = $1)", oid)
        return aggregate(roots[0], {"doc_lines": lines, "line_marks": marks})

    return await d.background_tx(body)


def erased(agg: str) -> str:
    """A document's expected form after its erasure: the erasable columns null (a canonical row leaves a null out), at
    both levels."""
    a = json.loads(agg)
    a["root"].pop("note", None)
    children = {}
    for name, rows in a.get("children", {}).items():
        rs = [json.loads(r) for r in rows]
        if name == "doc_lines":
            for r in rs:
                r.pop("person", None)
        children[name] = rs
    return aggregate(a["root"], children)


async def child(spec: dict) -> None:
    hits = 0

    def hook(p: str) -> None:
        nonlocal hits
        if p == "tiering." + spec["crash"]:
            hits += 1
            if hits >= spec["after"]:
                sys.stdout.write(f"CRASH {spec['crash']}\n")
                sys.stdout.flush()
                os._exit(3)

    if spec["crash"]:
        crash.set_hook(hook)
    for m in spec["members"]:
        log = obs.new_logger(obs.Member("test/" + m["name"], "3.0.0"), "warn", sys.stderr)
        d, _, cat = await catalog_of(m, spec["id"], log)
        r = Runner(d, "test/" + m["name"], log, lease=3.0, poll=0.1, concurrency=4)
        cat.enqueue = r.enqueue
        for job in (fast(member.candidates_job(cat, "* * * * *", log), 1.0), fast(transfer.transfer_job(cat, log), 1.0),
                    fast(release.release_job(cat, log), 2.0), fast(destroy.destroy_job(cat, log), 3.0),
                    erase.erase_job(cat, log), fast(transfer.cleanup_job(cat, log), CLEANUP_EVERY)):
            r.add(job)
        await r.start()
    sys.stdout.write("READY\n")
    sys.stdout.flush()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(child(json.loads(os.environ["SIM_TIER_CHILD"])))
