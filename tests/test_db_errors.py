import socket

import asyncpg
import pytest

from besdk import errs, protocol
from besdk.db.pool import map_error


async def connect_error():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    with pytest.raises(OSError) as e:
        await asyncpg.connect(host="127.0.0.1", port=port, user="u", database="d", timeout=2)
    return e.value


async def test_db_errors_vectors():
    for c in protocol.vector("db-errors")["cases"]:
        err = await connect_error() if c["sqlstate"] == "connect" else asyncpg.PostgresError.new({"C": c["sqlstate"], "M": "x"})
        e = map_error(err)
        assert e.reason == c["reason"], c["id"]
        if c["reason"] == "DEPENDENCY_UNAVAILABLE":
            assert e.metadata == {"dependency": "db"}, c["id"]
        retried = isinstance(err, (asyncpg.SerializationError, asyncpg.DeadlockDetectedError))
        assert retried == c["retried"], c["id"]


def test_module_errors_pass_through():
    own = errs.new("NOT_FOUND", "x")
    assert map_error(own) is own
