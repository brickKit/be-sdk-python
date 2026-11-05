"""Compiled lists against PostgreSQL and vector segment-eval (P8.17, P8.18; CK-AZ-05): the view operation's list and
an operation's list give the vector's records and per-row fields, the same as the single-object evaluation; a filter
outside the grants narrows, never errs, on every dimension. The same test as be-sdk-go's
authz.TestCompiledListsMatchTheVectors."""
from datetime import datetime

import pytest

from besdk.authz import Segment
from besdk.authz.plan import compile_plan
from tests.test_authz import facts, fixture
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration


async def test_compiled_lists_match_the_vectors():  # P8.17, P8.18, CK-AZ-05
    f, r, cases = fixture()
    at = datetime.fromisoformat(f["at"])
    v, _ = await migrated("py_authzsql", {"0001_orders.up.sql": "CREATE TABLE demo_order (id text PRIMARY KEY, "
                                                                "legal_entity text, dept_path text, owner_id uuid, "
                                                                "amount numeric, currency text);"})
    d, pool = await open_db("py_authzsql", v)
    try:
        async def fill(tx):
            await tx.execute("DELETE FROM demo_order")
            for rec in f["records"]:
                fa = facts(r, rec)
                if "unobtained" in (fa["legal_entity"].state, fa["amount"].state):
                    continue  # single-object only: a table has no "not obtained"

                def val(dim):
                    return fa[dim].value if fa[dim].state == "value" else None
                await tx.execute("INSERT INTO demo_order VALUES ($1, $2, $3, $4::text::uuid, $5::text::numeric, $6)",
                                 rec["id"], val("legal_entity"), val("dept"), val("owner"), val("amount"),
                                 fa["amount"].currency if fa["amount"].state == "value" else None)
        await d.tx(fill)

        async def listed(c, op, flt=None):
            p = compile_plan(r, [Segment.from_dict(s) for s in c["segments"]], op, c["principal"], at, "o", 0)
            where, args = p.where, list(p.args)
            for col, value in (flt or {}).items():
                args.append(value)
                where += f" AND o.{col}::text = ${len(args)}::text"
            sel = ", ".join(["o.id"] + p.hits)

            async def run(tx):
                return await tx.fetch(f"SELECT {sel} FROM demo_order o WHERE {where} ORDER BY o.id", *args)
            rows = await d.tx(run)
            for row in rows:  # each bit a boolean, never NULL (P8.17): a component reads them as they are
                assert all(isinstance(x, bool) for x in row[1:]), (op, row[0], list(row[1:]))
            return {row[0]: p.fields(list(row[1:])) for row in rows}

        n = 0
        for c in cases:
            if c["kind"] == "evaluate":
                got = await listed(c, r.view_operation)
                want = c["expect"]["list"]
                assert sorted(got) == sorted(want["ids"]), c["id"]
                assert got == {i: want["fields"][i] for i in want["ids"]}, (c["id"], "list and single object agree")
                # the list of an operation: visible AND permitted, with the operation's own fields (security 01)
                got = await listed(c, c["action"])
                want = c["expect"]["action_list"]
                assert got == {i: want["fields"][i] for i in want["ids"]}, (c["id"], "the operation's list")
                n += 1
            elif c["kind"] == "list":
                assert sorted(await listed(c, r.view_operation, c["filter"])) == c["expect"]["ids"], c["id"]
                n += 1
        assert n > 0
    finally:
        await pool.close()
