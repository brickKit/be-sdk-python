"""Component lists (protocol P9.1-P9.5): keyset pages with the record ID as the last key, opaque cursors bound to the
filter and plan digests, declared filters and sorts, all in one SQL statement with the subject's authorization
predicate. The same rules as be-sdk-go's list package. List search on normalized search columns (P9.6-P9.8) is not in
the Python subset yet: a keyword on a list that declares search columns answers CAPABILITY_UNSUPPORTED."""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, TypeVar

from besdk import errs
from besdk.authz.defs import ORDINARY
from besdk.authz.plan import ident
from besdk.authz.subject import Subject

# Page sizes (P9.2).
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

# Kinds of columns: how cursor values and filter values are cast.
TEXT, DECIMAL, DATE, TIME, UUID = "text", "numeric", "date", "timestamptz", "uuid"
BOOLEAN = "boolean"  # filter only (eq: true or false); not a sort key


@dataclass
class Column:
    """A field a list may filter or sort by. Sortable columns are not nullable."""

    field: str
    column: str
    kind: str
    sortable: bool = False
    filterable: bool = False


@dataclass
class SearchColumn:
    name: str
    column: str
    fields: list[str]


@dataclass
class Spec:
    operation: str  # the list's operation (the view operation for ordinary lists)
    alias: str  # the table alias the SQL uses
    id_column: str
    id_kind: str
    columns: list[Column] = field(default_factory=list)
    search: list[SearchColumn] = field(default_factory=list)


@dataclass
class Order:
    field: str
    desc: bool = False


@dataclass
class Filter:
    field: str
    op: str  # eq, in, gte, gt, lte, lt
    values: list[str]


@dataclass
class Request:
    sort: list[Order] = field(default_factory=list)
    filters: list[Filter] = field(default_factory=list)
    q: str = ""
    cursor: str = ""
    limit: int = 0


def _bad(name: str, why: str) -> errs.Error:
    return errs.new("REQUEST_INVALID", why).with_violations(errs.Violation(name, "INVALID", why))


def _digest(v: Any) -> str:
    return hashlib.sha256(json.dumps(v, separators=(",", ":"), sort_keys=True).encode()).hexdigest()[:32]


def parse_limit(v: str) -> int:
    """A page size given by a caller: absent is the default; 0 or more than MAX_LIMIT is refused (P9.2)."""
    if v == "":
        return DEFAULT_LIMIT
    try:
        n = int(v)
    except ValueError:
        n = 0
    if n < 1 or n > MAX_LIMIT:
        raise _bad("limit", f"the page size must be 1-{MAX_LIMIT}")
    return n


@dataclass
class Query:
    """A compiled page: SELECT the columns you need FROM your table AS the spec's alias, WHERE where, ORDER BY
    order_by, LIMIT limit + 1 (one more row tells whether there is a next page)."""

    where: str
    args: list
    order_by: str
    alias: str
    limit: int
    hits: list[str]
    sorts: list[Order]
    filter_hash: str
    plan_hash: str

    def next(self, keys: list[str], id: str) -> str:
        """The cursor that continues after a row, given its sort values (as text, in sort order) and ID."""
        b = json.dumps({"v": 1, "k": keys, "id": id, "f": self.filter_hash, "p": self.plan_hash}, separators=(",", ":"))
        return base64.urlsafe_b64encode(b.encode()).decode().rstrip("=")

    def sort_fields(self) -> list[str]:
        return [o.field for o in self.sorts]


class _Builder:
    def __init__(self, spec: Spec, args: list):
        self.spec, self.args = spec, args
        self.cols = {c.field: c for c in spec.columns}

    def arg(self, v: Any) -> str:
        self.args.append(v)
        return f"${len(self.args)}"

    def col(self, name: str) -> str:
        return ident(self.spec.alias, name)

    def typed(self, c: Column, v: str) -> str:
        return f"{self.arg(v)}::text::{c.kind}"

    def filter(self, c: Column, f: Filter) -> str:
        col = self.col(c.column)
        if f.op == "in":
            return f"{col} = ANY({self.arg(list(f.values))}::text[]::{c.kind}[])" if f.values else "FALSE"
        ops = {"eq": "=", "gte": ">=", "gt": ">", "lte": "<=", "lt": "<"}
        if f.op not in ops or len(f.values) != 1:
            raise _bad("filter." + f.field, "operator must be eq, in, gte, gt, lte or lt with one value (in: a list)")
        return f"{col} {ops[f.op]} {self.typed(c, f.values[0])}"

    def resume(self, raw: str, sorts: list[Order], filter_hash: str, plan_hash: str) -> str:
        """The keyset condition: rows strictly after the cursor's row in the order, each key in its own direction."""
        try:
            c = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
            ok = c.get("v") == 1 and isinstance(c.get("k"), list) and len(c["k"]) == len(sorts)
        except (ValueError, TypeError, AttributeError):
            ok = False
        if not ok:
            raise errs.new("CURSOR_INVALID", "the cursor cannot be read")
        if c.get("f") != filter_hash:
            raise errs.new("CURSOR_INVALID", "the cursor belongs to another filter, search or sort")
        if c.get("p") != plan_hash:
            raise errs.new("QUERY_PLAN_CHANGED", "the authorization changed; query again")
        ors = []
        for i in range(len(sorts) + 1):
            ands = [f"{self.col(self.cols[sorts[j].field].column)} = {self.typed(self.cols[sorts[j].field], c['k'][j])}"
                    for j in range(i)]
            if i < len(sorts):
                col = self.cols[sorts[i].field]
                ands.append(f"{self.col(col.column)} {'<' if sorts[i].desc else '>'} {self.typed(col, c['k'][i])}")
            else:
                ands.append(f"{self.col(self.spec.id_column)} > {self.arg(c['id'])}::text::{self.spec.id_kind}")
            ors.append("(" + " AND ".join(ands) + ")")
        return "(" + " OR ".join(ors) + ")"


def build(subj: Subject, spec: Spec, req: Request) -> Query:
    """Compiles one page for the request's subject."""
    limit = req.limit or DEFAULT_LIMIT
    if limit < 1 or limit > MAX_LIMIT:
        raise _bad("limit", f"the page size must be 1-{MAX_LIMIT}")
    plan = subj.list(spec.operation, spec.alias, 0)
    b = _Builder(spec, list(plan.args))
    where = [plan.where]
    o = subj.defs.operation(spec.operation)
    r = subj.defs.resource(o.resource_type) if o else None

    def restricted(name: str) -> bool:
        return r is not None and name in r.fields and r.fields[name].cls != ORDINARY

    for f in req.filters:
        c = b.cols.get(f.field)
        if c is None or not c.filterable:
            raise _bad("filter." + f.field, "not a filterable field")
        if restricted(f.field) and not plan.readable_everywhere(f.field):
            raise errs.new("FIELD_NOT_QUERYABLE", "this field cannot be used to filter here").with_("field", f.field)
        where.append(b.filter(c, f))
    q = " ".join(req.q.split())
    if q:
        if spec.search:
            raise errs.new("CAPABILITY_UNSUPPORTED", "list search is not in the Python SDK yet")
        where.append("FALSE")  # no column this list may search: nothing matches, nothing leaks
    for s in req.sort:
        c = b.cols.get(s.field)
        if c is None or not c.sortable:
            raise _bad("sort." + s.field, "not a sortable field")
        if restricted(s.field) and not plan.readable_everywhere(s.field):
            raise errs.new("FIELD_NOT_QUERYABLE", "this field cannot be used to sort here").with_("field", s.field)
    filter_hash = _digest({"filters": [asdict(f) for f in req.filters], "q": q, "sort": [asdict(s) for s in req.sort]})
    if req.cursor:
        where.append(b.resume(req.cursor, req.sort, filter_hash, plan.digest))
    order = [f"{b.col(b.cols[s.field].column)} {'DESC' if s.desc else 'ASC'}" for s in req.sort]
    order.append(f"{b.col(spec.id_column)} ASC")
    return Query(" AND ".join(where), b.args, ", ".join(order), spec.alias, limit, plan.hits, list(req.sort), filter_hash, plan.digest)


T = TypeVar("T")


async def page(db: Any, q: Query, table: str, cols: str, scan: Callable[[Any], T],
               keys: Callable[[T], tuple[list[str], str]]) -> tuple[list[T], str]:
    """Runs one compiled page in its own transaction: SELECT cols FROM table AS the spec's alias, one row more than the
    page to know whether another follows. keys gives a row's sort values (as text, in sort order) and its ID; the
    next cursor is empty on the last page. Every field of the rows must be readable wherever a row is listed."""
    async def run(tx) -> list:
        return await tx.fetch(f"SELECT {cols} FROM {table} AS {ident(q.alias)} WHERE {q.where} ORDER BY {q.order_by} "
                              f"LIMIT {q.limit + 1}", *q.args)
    rows = await db.tx(run)
    out = [scan(r) for r in rows[:q.limit]]
    if len(rows) <= q.limit:
        return out, ""
    k, id = keys(out[-1])
    return out, q.next(k, id)
