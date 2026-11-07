"""A data class a component declares (be-protocol P15.1-P15.5): its category, its root table and children (children
may nest: a child's parent is the root or another child; a leaf no one points at may have a key of several columns),
the columns a unit groups by, the anchor its residence and retention count from, its terminal condition, residence,
retention, the columns reads need to find and authorize a record, the erasable columns, its routes and its structure
version. check() answers every rule that needs no database."""
from __future__ import annotations

import re
from dataclasses import dataclass

from besdk.tiering.rows import TIMESTAMPTZ, TYPES

# categories (P15.2): only completed history and audit evidence are tiered
ONLINE_MUTABLE, LONG_TERM_EVIDENCE, COMPLETED, AUDIT, REBUILDABLE = (
    "online_mutable", "long_term_evidence", "completed_history", "audit_evidence", "rebuildable")
CATEGORIES = (ONLINE_MUTABLE, LONG_TERM_EVIDENCE, COMPLETED, AUDIT, REBUILDABLE)
ONLINE, POSTGRES, OBJECT = "online", "postgres", "object-parquet"  # the routes (A150)
NEVER = -1  # a residence that keeps a class online
_PLAIN = re.compile(r"[a-z_][a-z0-9_]*")


def ident(name: str) -> str:
    """A plain identifier (checked by check()) quoted for SQL."""
    return '"' + name + '"'


@dataclass(frozen=True)
class Table:
    """A class's table: its name, its primary key (pk, or key for a leaf with several columns) and every column's
    type (P15.8)."""

    name: str
    columns: dict[str, str]
    pk: str = ""
    key: tuple[str, ...] = ()

    def key_columns(self) -> list[str]:
        return list(self.key) if self.key else [self.pk]


@dataclass(frozen=True)
class Child:
    """A table whose rows belong to a root: fk holds its parent's primary key, the parent being the root (parent
    empty) or another child."""

    table: Table
    fk: str
    parent: str = ""

    @property
    def name(self) -> str:
        return self.table.name


@dataclass(frozen=True)
class Retention:
    """The shortest and longest days from the unit's latest anchor, and the basis; pending when no law or contract
    gives one (never destroyed) (P15.4)."""

    min_days: int = 0
    max_days: int = 0
    basis: str = ""
    pending: bool = False


@dataclass(frozen=True)
class Class:
    name: str
    category: str
    root: Table
    anchor: str  # the root's timestamptz column the residence and the retention count from
    created: str  # the root's creation, timestamptz
    terminal: str  # a SQL boolean over the root row (alias r) that holds for an aggregate in its final state
    residence_days: int  # NEVER: stays online
    retention: Retention
    schema_version: str  # the tables' structure version, in each manifest (P15.33)
    children: tuple[Child, ...] = ()
    group: tuple[str, ...] = ()  # the root's columns a unit groups by, with the anchor's UTC month (P15.6)
    locate: tuple[str, ...] = ()  # the columns reads need to find and authorize a record
    erasable: tuple[str, ...] = ()
    routes: tuple[str, ...] = ()
    hold_operation: str = ""  # sets or lifts a hold (P15.28): when the class can be destroyed or erased
    approve_operation: str = ""  # gives or withdraws a destruction approval (P15.29): when it can be destroyed

    def tierable(self) -> bool:
        return self.category in (COMPLETED, AUDIT)

    def tables(self) -> list[Table]:
        return [self.root, *(ch.table for ch in self.children)]

    def child(self, name: str) -> Child | None:
        return next((ch for ch in self.children if ch.name == name), None)

    def parent(self, ch: Child) -> Table | None:
        """A child's parent table: the root, or the child it names."""
        if not ch.parent or ch.parent == self.root.name:
            return self.root
        o = self.child(ch.parent)
        return o.table if o is not None and o.name != ch.name else None

    def depth(self, ch: Child) -> int:
        """A child's level: 1 under the root; -1 for a parent chain that does not end at the root."""
        for d in range(1, len(self.children) + 1):
            if not ch.parent or ch.parent == self.root.name:
                return d
            nxt = self.child(ch.parent)
            if nxt is None:
                return -1
            ch = nxt
        return -1

    def by_depth(self, deepest_first: bool = False) -> list[Child]:
        """The children, the shallowest first (reads and copies) or the deepest first (deletes)."""
        return sorted(self.children, key=lambda ch: -self.depth(ch) if deepest_first else self.depth(ch))

    def root_expr(self, ch: Child, alias: str) -> str:
        """The SQL expression (text) of the root key of a child's row under alias: its key column under the root, a
        lookup up the parent chain below it."""
        if not ch.parent or ch.parent == self.root.name:
            return f"{alias}.{ident(ch.fk)}::text"
        parent = self.child(ch.parent)
        p = alias + "p"
        return (f"(SELECT {self.root_expr(parent, p)} FROM {ident(parent.name)} {p} "  # type: ignore[arg-type]
                f"WHERE {p}.{ident(parent.table.pk)} = {alias}.{ident(ch.fk)})")  # type: ignore[union-attr]

    def check(self, n: int, registries: tuple[str, ...] = ()) -> list[str]:
        """The rules that need no database (P15.1-P15.5); registries are the tables the component names as unique
        registries; n is the list's default range in days. Answers the problems, none when it holds."""
        out: list[str] = []

        def bad(msg: str) -> None:
            out.append(f"{self.name}: {msg}")

        if not self.name or " " in self.name or "/" in self.name:
            bad("a class needs a name without spaces or slashes")
        if self.category not in CATEGORIES:
            bad(f"unknown category {self.category!r}")
        if not self.tierable() and (self.residence_days != 0 or self.routes):
            bad(f"category {self.category} is never tiered: no residence or route (P15.2)")
        if self.tierable():
            if self.residence_days != NEVER and self.residence_days < n:
                bad(f"residence {self.residence_days} days is below N = {n} (P15.3)")
            if not self.routes:
                bad("a tierable class names its routes")
            for r in self.routes:
                if r not in (POSTGRES, OBJECT):
                    bad(f"unknown route {r!r}")
            if not self.terminal:
                bad("a tierable class needs its terminal condition")
        rt = self.retention
        if not rt.pending and (rt.min_days <= 0 or rt.max_days < rt.min_days or not rt.basis):
            bad("a retention needs its shortest and longest days and its basis, or pending (P15.4)")
        if not self.schema_version:
            bad("a class needs its schema version")
        if not self.hold_operation and (not rt.pending or self.erasable):
            bad("a class that can be destroyed or erased declares its hold operation (P15.28)")
        if not self.approve_operation and not rt.pending:
            bad("a class that can be destroyed declares its destruction-approval operation (P15.29)")
        for t in self.tables():
            if t.name in registries:
                bad(f"{t.name} is a unique registry: never a class's table (P15.5)")
            if not _PLAIN.fullmatch(t.name):
                bad(f"table {t.name!r} is not a plain identifier")
            if bool(t.pk) == bool(t.key):
                bad(f"table {t.name}: a key, either pk or key")
            for k in t.key_columns():
                if k not in t.columns or not _PLAIN.fullmatch(k or ""):
                    bad(f"table {t.name}: its key column {k!r} has no type")
            for col, typ in t.columns.items():
                if not _PLAIN.fullmatch(col) or typ not in TYPES:
                    bad(f"table {t.name}: column {col!r} of type {typ!r}")
        if not self.root.pk:
            bad(f"the root {self.root.name} has a single-column key (pk)")
        for ch in self.children:
            if ch.fk not in ch.table.columns:
                bad(f"child {ch.name}: its key to its parent {ch.fk} has no type")
            parent = self.parent(ch)
            if parent is None:
                bad(f"child {ch.name}: its parent {ch.parent!r} is not the root or another child")
            else:
                if not parent.pk:
                    bad(f"child {ch.name}: its parent {parent.name} is a parent, so it has a single-column key (pk)")
                if self.depth(ch) < 0:
                    bad(f"child {ch.name}: its parents go round in a cycle")
        root = self.root.columns
        for col in (self.anchor, self.created, *self.group, *self.locate):
            if col not in root:
                bad(f"root {self.root.name} has no column {col!r}")
        if root.get(self.anchor) != TIMESTAMPTZ or root.get(self.created) != TIMESTAMPTZ:
            bad("the anchor and the creation are timestamptz columns")
        for col in self.erasable:
            if not any(col in t.columns for t in self.tables()):
                bad(f"erasable column {col!r} is in no table")
        return out
