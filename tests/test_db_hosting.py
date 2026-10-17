import asyncio
import io
import json
import socket

import httpx
import pytest
from fastapi.responses import JSONResponse

from besdk import launcher, shell
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.http import Route
from tests import pgtest
from tests.test_shell import installed

pytestmark = pytest.mark.integration
FILES = {"0001_notes.up.sql": "CREATE TABLE notes (id int PRIMARY KEY);"}


class Notes:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def who(c):
            row = await self.rt.db.tx(lambda tx: tx.fetchrow(
                "SELECT current_user AS u, current_schema() AS s, (SELECT count(*) FROM notes) AS n"))
            return JSONResponse(dict(row))

        r.add(Route("GET", "/api/who", who, public=True))


def definition(cid="demo/notes", version="3.0.0"):
    return Definition(cid, version, Notes, migrations=FILES)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def env(values):
    return EnvSource(lookup_env=values.get)


async def wait_ready(url):
    async with httpx.AsyncClient() as c:
        for _ in range(500):
            try:
                if (await c.get(url + "/readyz")).status_code == 200:
                    return
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.01)
    raise AssertionError("never ready")


async def test_a_component_with_a_database_migrates_and_serves():  # P1.1, P1.2, P5.10, CK-DB-04
    await pgtest.member("py_host1")
    e = pgtest.member_env("py_host1")
    for _ in range(2):
        out = io.StringIO()
        assert await launcher.run(definition(), launcher.Options(args=["migrate"], env=env(e), stdout=out,
                                                                 stderr=io.StringIO()), asyncio.Event()) == 0, out.getvalue()
    port, stop = free_port(), asyncio.Event()
    serve = {**e, "COMPONENT_ID": "demo/notes", "COMPONENT_VERSION": "3.0.0", "PORT": str(port), "STOP_DRAIN_DELAY": "50ms"}
    run = asyncio.create_task(launcher.run(definition(), launcher.Options(env=env(serve), stdout=io.StringIO(),
                                                                         stderr=io.StringIO(), host="127.0.0.1"), stop))
    await wait_ready(f"http://127.0.0.1:{port}")
    async with httpx.AsyncClient() as c:
        assert (await c.get(f"http://127.0.0.1:{port}/api/who")).json() == {"u": "sdk_py_host1_rw", "s": "py_host1", "n": 0}
    stop.set()
    assert await run == 0

    for bad, code in [({"PG_PASSWORD": "wrong"}, 78), ({"PG_DATABASE": "be_sdk_no_such_db"}, 69)]:
        out = io.StringIO()
        got = await launcher.run(definition(), launcher.Options(env=env({**serve, **bad}), stdout=out,
                                                                stderr=io.StringIO(), host="127.0.0.1"), asyncio.Event())
        assert got == code, (bad, out.getvalue())


async def test_members_share_one_pool_and_never_each_others_schema():  # P5.1, CK-DB-01
    a = await pgtest.admin()
    try:
        await pgtest.role(a, "sdk_py_shell2", "shell2-pw")
    finally:
        await a.close()
    configs = {}
    for name in ("py_sh_a", "py_sh_b"):
        await pgtest.member(name, shell_role="sdk_py_shell2")
        e = pgtest.member_env(name)
        assert await launcher.run(definition("demo/" + name), launcher.Options(
            args=["migrate"], env=env(e), stdout=io.StringIO(), stderr=io.StringIO()), asyncio.Event()) == 0
        configs[name] = {k: v for k, v in e.items() if not k.startswith("PG_OWNER")}
    pa, pb, ps = free_port(), free_port(), free_port()
    members = json.dumps([{"componentId": "demo/py_sh_a", "version": "3.0.0", "httpPort": pa, "config": configs["py_sh_a"]},
                          {"componentId": "demo/py_sh_b", "version": "3.0.0", "httpPort": pb, "config": configs["py_sh_b"]}])
    base = {"COMPONENT_ID": "be/py-render", "COMPONENT_VERSION": "3.0.0", "PORT": str(ps), "STOP_DRAIN_DELAY": "50ms",
            "BRICKKIT_SERVED_MEMBERS_CONFIG": members}
    defs = [definition("demo/py_sh_a"), definition("demo/py_sh_b")]
    lookup = installed({"demo/py_sh_a": "3.0.0", "demo/py_sh_b": "3.0.0"})
    self = shell.Identity("be/py-render", "3.0.0")

    err = io.StringIO()
    assert await shell.run(self, defs, shell.Options(env=env(base), stdout=io.StringIO(), stderr=err,
                                                     installed_of=lookup), asyncio.Event()) == 78
    assert "PG_SHELL_USER" in err.getvalue(), "a member with a database needs the shell's login role"

    stop = asyncio.Event()
    full = {**base, "PG_SHELL_USER": "sdk_py_shell2", "PG_SHELL_PASSWORD": "shell2-pw"}
    run = asyncio.create_task(shell.run(self, defs, shell.Options(env=env(full), stdout=io.StringIO(), stderr=io.StringIO(),
                                                                  host="127.0.0.1", installed_of=lookup), stop))
    await wait_ready(f"http://127.0.0.1:{ps}")
    async with httpx.AsyncClient() as c:
        assert (await c.get(f"http://127.0.0.1:{pa}/api/who")).json() == {"u": "sdk_py_sh_a_rw", "s": "py_sh_a", "n": 0}
        assert (await c.get(f"http://127.0.0.1:{pb}/api/who")).json() == {"u": "sdk_py_sh_b_rw", "s": "py_sh_b", "n": 0}
    a = await pgtest.admin()
    try:
        users = {r[0] for r in await a.fetch("SELECT DISTINCT usename FROM pg_stat_activity WHERE application_name LIKE "
                                             "'be/py-render%' OR application_name LIKE 'demo/py_sh_%'")}
        assert users == {"sdk_py_shell2"}, "the database sees only the shell's login role"
    finally:
        await a.close()
    stop.set()
    assert await run == 0

    configs["py_sh_b"]["PG_POOLER_PREPARED"] = "false"
    members = json.dumps([{"componentId": "demo/py_sh_a", "version": "3.0.0", "httpPort": pa, "config": configs["py_sh_a"]},
                          {"componentId": "demo/py_sh_b", "version": "3.0.0", "httpPort": pb, "config": configs["py_sh_b"]}])
    err = io.StringIO()
    assert await shell.run(self, defs, shell.Options(env=env({**full, "BRICKKIT_SERVED_MEMBERS_CONFIG": members}),
                                                     stdout=io.StringIO(), stderr=err, installed_of=lookup), asyncio.Event()) == 78
    assert "demo/py_sh_b:PG_POOLER_PREPARED" in err.getvalue()
