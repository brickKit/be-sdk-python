"""A class declaration (P15.1-P15.5), the same cases as be-sdk-go's tiering/class_test.go: what a declaration may not
say, and what it may."""
from dataclasses import replace

import pytest

from besdk.tiering import classes as k
from besdk.tiering.rows import TEXT, TIMESTAMPTZ, UUID

LINES = k.Child(k.Table("order_lines", {"id": UUID, "order_id": UUID}, pk="id"), "order_id")
NOTES_UNDER = lambda parent: k.Child(k.Table("notes", {"id": UUID, "line_id": UUID}, pk="id"), "line_id", parent)  # noqa: E731


def orders() -> k.Class:
    return k.Class("sales.orders", k.COMPLETED,
                   k.Table("orders", {"id": UUID, "legal_entity": UUID, "closed_at": TIMESTAMPTZ,
                                      "created_at": TIMESTAMPTZ, "email": TEXT}, pk="id"),
                   anchor="closed_at", created="created_at", terminal="r.status = 'closed'", residence_days=365,
                   retention=k.Retention(3650, 3650, "commercial law"), schema_version="1", children=(LINES,),
                   group=("legal_entity",), locate=("legal_entity",), erasable=("email",), routes=(k.OBJECT,),
                   hold_operation="sales.order.hold", approve_operation="sales.order.approve_destruction")


def root_with(**columns):
    c = orders()
    return replace(c.root, columns={**c.root.columns, **columns})


BAD = {
    "residence below N (P15.3, TR29)": lambda c: replace(c, residence_days=30),
    "a residence on a never-tiered kind (P15.2)": lambda c: replace(c, category=k.ONLINE_MUTABLE),
    "routes on evidence kept online (P15.2, TR22)": lambda c: replace(c, category=k.LONG_TERM_EVIDENCE, residence_days=0),
    "a unique registry as a table (P15.5, TR21)":
        lambda c: replace(c, children=(replace(LINES, table=replace(LINES.table, name="doc_numbers")),)),
    "an unknown route": lambda c: replace(c, routes=("iceberg",)),
    "no routes": lambda c: replace(c, routes=()),
    "no terminal condition": lambda c: replace(c, terminal=""),
    "a retention without a basis (P15.4)": lambda c: replace(c, retention=replace(c.retention, basis="")),
    "the longest retention below the shortest": lambda c: replace(c, retention=replace(c.retention, max_days=10)),
    "an anchor that is not a column": lambda c: replace(c, anchor="closed"),
    "an anchor that is not an instant": lambda c: replace(c, anchor="legal_entity"),
    "a locate column the root lacks": lambda c: replace(c, locate=("region",)),
    "a child's key without a type": lambda c: replace(c, children=(replace(LINES, fk="parent"),)),
    "an erasable column in no table": lambda c: replace(c, erasable=("phone",)),
    "a column type the encoding lacks": lambda c: replace(c, root=root_with(email="citext")),
    "no schema version": lambda c: replace(c, schema_version=""),
    "no hold operation (P15.28)": lambda c: replace(c, hold_operation=""),
    "no approval operation (P15.29)": lambda c: replace(c, approve_operation=""),
    "a child under an unknown parent": lambda c: replace(c, children=(LINES, NOTES_UNDER("lines"))),
    "a root with a key of two columns": lambda c: replace(c, root=replace(c.root, pk="", key=("id", "legal_entity"))),
    "a table with both a pk and a key":
        lambda c: replace(c, children=(replace(LINES, table=replace(LINES.table, key=("id", "order_id"))),)),
    "a parent with a key of two columns": lambda c: replace(c, children=(
        replace(LINES, table=replace(LINES.table, pk="", key=("id", "order_id"))), NOTES_UNDER("order_lines"))),
    "a key column without a type":
        lambda c: replace(c, children=(replace(LINES, table=replace(LINES.table, pk="", key=("id", "nope"))),)),
    "children in a cycle": lambda c: replace(c, children=(
        k.Child(k.Table("a", {"id": UUID, "b_id": UUID}, pk="id"), "b_id", "b"),
        k.Child(k.Table("b", {"id": UUID, "a_id": UUID}, pk="id"), "a_id", "a"))),
    "pending but erasable, no hold operation":
        lambda c: replace(c, retention=k.Retention(pending=True), hold_operation="", approve_operation=""),
}


def test_a_valid_declaration():
    assert orders().check(90, ("doc_numbers",)) == []


@pytest.mark.parametrize("edit", list(BAD.values()), ids=list(BAD))
def test_what_a_declaration_may_not_say(edit):
    assert edit(orders()).check(90, ("doc_numbers",)) != []


def test_what_a_declaration_may_say():
    unknown = replace(orders(), children=(LINES, NOTES_UNDER("lines")))
    assert any("its parent 'lines' is not the root or another child" in p for p in unknown.check(90))
    assert replace(orders(), children=(LINES, k.Child(k.Table("line_notes", {"id": UUID, "line_id": UUID}, pk="id"),
                                                      "line_id", "order_lines"))).check(90) == [], "nested (P15.1)"
    assert replace(orders(), children=(replace(LINES, table=replace(LINES.table, pk="", key=("order_id", "id"))),)) \
        .check(90) == [], "a leaf with a key of two columns"
    assert replace(orders(), residence_days=k.NEVER).check(90) == [], "a class may stay online"
    assert replace(orders(), retention=k.Retention(pending=True)).check(90) == [], "pending (P15.4, TR24)"
    assert replace(orders(), retention=k.Retention(pending=True), erasable=(), hold_operation="",
                   approve_operation="").check(90) == [], "never destroyed nor erased: no hold or approval operation"
    assert replace(orders(), category=k.LONG_TERM_EVIDENCE, residence_days=0, routes=()).check(90) == [], \
        "a never-tiered kind with a retention only"


def test_a_tiering_class_names_declared_operations():  # P15.1: hold and approval operations are the component's
    from besdk.authz import Definitions, Operation
    from besdk.component import Definition
    from besdk.host import _tiering_operations

    authz = Definitions("demo/notes", [], [Operation("demo.hold", "demo.note", "manage", "low"),
                                           Operation("demo.approve", "demo.note", "manage", "low")])
    cl = k.Class("demo.notes", k.COMPLETED, k.Table("notes", {"id": UUID}, pk="id"), "", "", "", 365,
                 k.Retention(), "1", hold_operation="demo.hold", approve_operation="demo.approve")
    _tiering_operations(Definition("demo/notes", "3.0.0", None, authz=authz, tiering=[cl]))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="demo.destroy"):
        _tiering_operations(Definition("demo/notes", "3.0.0", None, authz=authz,  # type: ignore[arg-type]
                                       tiering=[replace(cl, approve_operation="demo.destroy")]))
    with pytest.raises(ValueError, match="Definition.authz"):  # no definitions at all
        _tiering_operations(Definition("demo/notes", "3.0.0", None, tiering=[cl]))  # type: ignore[arg-type]
