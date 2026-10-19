"""P5.30: statements that would leave state on a pooled connection are named and refused."""
import pytest

from besdk.db.session import guard, session_state

CASES = {
    "SET search_path = other": "SET without LOCAL",
    "  set ROLE someone": "SET without LOCAL",
    "/* demo/a */ SET SESSION statement_timeout = 0": "SET without LOCAL",
    "-- a comment\nRESET ALL": "RESET",
    "discard all": "DISCARD",
    "LISTEN be_bus_wake": "LISTEN",
    "UNLISTEN *": "UNLISTEN",
    "SELECT pg_advisory_lock(42)": "a session-level advisory lock",
    "SELECT PG_TRY_ADVISORY_LOCK_SHARED(1, 2)": "a session-level advisory lock",
    "SET LOCAL search_path = demo": "",
    "set local role demo_rw": "",
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE": "",
    "SET CONSTRAINTS ALL DEFERRED": "",
    "SELECT set_config('search_path', 'x', true)": "",
    "SELECT set_config('search_path', 'x', false)": "set_config(…, false)",
    "select SET_CONFIG($1, $2, FALSE)": "set_config(…, false)",
    "SELECT set_config('a', 'b', true), coalesce(x, false)": "",
    "SELECT pg_advisory_xact_lock(42)": "",
    "UPDATE settings SET value = 1": "",
    "INSERT INTO t (reset) VALUES (1)": "",
    "/* unterminated": "",
}


@pytest.mark.parametrize("sql", list(CASES))
def test_statements_that_would_stay_on_the_pooled_connection_are_named(sql):  # P5.30
    assert session_state(sql) == CASES[sql]
    if CASES[sql]:
        with pytest.raises(RuntimeError, match="P5.30"):
            guard(sql)
    else:
        guard(sql)


DO_CASES = {
    "DO $$ BEGIN PERFORM set_config('TimeZone', 'UTC', false); END $$": "set_config(…, false) in a DO block",
    "DO $d$ BEGIN SET search_path = x; END $d$": "SET without LOCAL in a DO block",
    "do $$ begin perform pg_advisory_lock(1); end $$": "a session-level advisory lock in a DO block",
    "DO $$ BEGIN SET LOCAL lock_timeout = '1s'; UPDATE t SET a = 1; END $$": "",
}


@pytest.mark.parametrize("sql", list(DO_CASES))
def test_do_blocks_are_checked_like_statements(sql):  # P5.31
    assert session_state(sql) == DO_CASES[sql]
