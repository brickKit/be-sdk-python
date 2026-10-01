import pytest

from besdk.shell_runner import parse_served_members

PEM = "-----BEGIN PRIVATE KEY-----\nMIIB$abc\"q\\x\n-----END PRIVATE KEY-----\n"


def test_unset_is_error():
    with pytest.raises(RuntimeError, match="未设置"):
        parse_served_members(None)


def test_empty_string_is_error():
    with pytest.raises(RuntimeError):
        parse_served_members("  ")


def test_zero_members():
    assert parse_served_members("[]") == []


def test_fields_and_secret_verbatim():
    raw = ('[{"componentId":"infra/print","version":"2.0.0","httpPort":8402,'
           '"extraPorts":[{"name":"grpc","port":9402}],'
           '"config":{"PG_SCHEMA":"infra_print","K":"-----BEGIN PRIVATE KEY-----\\nMIIB$abc\\"q\\\\x\\n-----END PRIVATE KEY-----\\n"}}]')
    [m] = parse_served_members(raw)
    assert (m.component_id, m.version, m.http_port) == ("infra/print", "2.0.0", 8402)
    assert m.extra_ports == {"grpc": 9402}
    assert m.config["PG_SCHEMA"] == "infra_print"
    assert m.config["K"] == PEM


# ---- build_shell_config / run / supervise（不需要真实 DB、NATS）----

import asyncio
import json
import logging
from types import SimpleNamespace

import asyncpg
import nats

import besdk
from besdk import shell_runner
from besdk.shell_runner import build_shell_config

SHELL_ENV = {
    "PG_HOST": "db", "PG_PORT": "5432", "PG_DATABASE": "d", "PG_USER": "u", "PG_PASSWORD": "p",
    "NATS_URL": "nats://n:4222", "IAM_JWKS_URL": "http://iam/jwks", "AUTHZ_BUNDLE_URL": "http://authz/bundle",
    "SHELL_HEALTH_PORT": "0",
}


def _members(*ids):
    return json.dumps([
        {"componentId": i, "version": "1.0.0", "httpPort": 8000 + n, "extraPorts": [], "config": {}}
        for n, i in enumerate(ids)
    ])


async def _noop_module(rt):
    return SimpleNamespace(asgi_app=None, register_grpc=None, start=None, stop=None, migrations_dir=None)


def test_build_shell_config_ok():
    cfg = build_shell_config("py", {"a/b": _noop_module}, {**SHELL_ENV, "BRICKKIT_SERVED_MEMBERS_CONFIG": _members("a/b")})
    assert [m.member.component_id for m in cfg.modules] == ["a/b"]
    assert cfg.authz_bundle_url == "http://authz/bundle"
    assert cfg.otel_base_url == ""


@pytest.mark.parametrize("key", ["IAM_JWKS_URL", "AUTHZ_BUNDLE_URL"])
def test_build_shell_config_missing_shell_key_names_it_no_member_fallback(key):
    env = {k: v for k, v in SHELL_ENV.items() if k != key}
    raw = json.dumps([{"componentId": "a/b", "version": "1", "httpPort": 1, "config": {key: "http://from-member"}}])
    with pytest.raises(RuntimeError, match=key):
        build_shell_config("py", {"a/b": _noop_module}, {**env, "BRICKKIT_SERVED_MEMBERS_CONFIG": raw})


def test_build_shell_config_missing_pg_key_names_it():
    env = {k: v for k, v in SHELL_ENV.items() if k != "PG_HOST"}
    with pytest.raises(ValueError, match="PG_HOST"):
        build_shell_config("py", {}, {**env, "BRICKKIT_SERVED_MEMBERS_CONFIG": "[]"})


def test_unregistered_component_id_names_it():
    with pytest.raises(RuntimeError, match="x/unknown"):
        build_shell_config("py", {"a/b": _noop_module}, {**SHELL_ENV, "BRICKKIT_SERVED_MEMBERS_CONFIG": _members("x/unknown")})


class _FakeClosable:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


@pytest.fixture
def fakes(monkeypatch):
    db, nc = _FakeClosable(), _FakeClosable()

    async def create_pool(dsn):
        return db

    async def connect(url):
        return nc

    async def bootstrap(name, otel):
        async def shutdown():
            return None
        return shutdown

    monkeypatch.setattr(asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(nats, "connect", connect)
    monkeypatch.setattr(besdk, "bootstrap", bootstrap)
    monkeypatch.setattr(besdk, "init_shell_authz", lambda *a, **k: None)
    monkeypatch.setattr(besdk, "new_shell_runtime", lambda cfg, db, nc: SimpleNamespace(cfg=cfg))
    started = []

    async def serve(*args):
        started.append(args[0])
        await args[-1].wait() if isinstance(args[-1], asyncio.Event) else None

    monkeypatch.setattr(besdk, "serve_http", lambda port, app, ev: serve(port, ev))
    monkeypatch.setattr(besdk, "serve_extra_port", lambda n, p, r, ev: serve(p, ev))
    return SimpleNamespace(db=db, nc=nc, started=started)


def _cfg(*specs):
    return shell_runner.ShellConfig(shell_name="t", modules=list(specs))


def _spec(cid, new_module, port=8000):
    return shell_runner.ModuleSpec(
        member=shell_runner.ServedMember(cid, "1", port, {}, {}), new_module=new_module)


async def test_run_zero_members_starts_nothing_and_stops_on_event(fakes):
    stop = asyncio.Event()
    t = asyncio.create_task(shell_runner.run(_cfg(), stop))
    await asyncio.sleep(0.05)
    assert not t.done()
    assert fakes.started == []
    stop.set()
    await asyncio.wait_for(t, 2)
    assert fakes.db.closed and fakes.nc.closed


async def test_one_member_start_raising_does_not_stop_the_others(fakes, caplog):
    other_stopped = asyncio.Event()

    async def bad_module(rt):
        async def start():
            raise ValueError("boom")
        return SimpleNamespace(asgi_app=None, register_grpc=None, start=start, stop=None)

    async def good_module(rt):
        async def start():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                other_stopped.set()
                raise
        return SimpleNamespace(asgi_app=None, register_grpc=None, start=start, stop=None)

    stop = asyncio.Event()
    with caplog.at_level(logging.ERROR):
        t = asyncio.create_task(shell_runner.run(_cfg(_spec("bad/m", bad_module, 8001), _spec("good/m", good_module, 8002)), stop))
        await asyncio.sleep(0.1)
        assert not t.done(), "一个成员 start() 失败不应让外壳退出"
        assert not other_stopped.is_set()
        assert 8002 in fakes.started
        assert any(getattr(r, "module_component_id", "") == "bad/m" for r in caplog.records)
        stop.set()
        await asyncio.wait_for(t, 2)
    assert other_stopped.is_set()


async def test_new_module_failure_closes_pool_and_nats(fakes):
    async def broken(rt):
        raise RuntimeError("ctor failed")

    with pytest.raises(RuntimeError, match="ctor failed"):
        await shell_runner.run(_cfg(_spec("a/b", broken)), asyncio.Event())
    assert fakes.db.closed and fakes.nc.closed


async def test_external_cancel_of_run_cleans_up_everything(fakes):
    async def mod_new(rt):
        async def start():
            await asyncio.Event().wait()
        return SimpleNamespace(asgi_app=None, register_grpc=None, start=start, stop=None)

    before = set(asyncio.all_tasks())
    t = asyncio.create_task(shell_runner.run(_cfg(_spec("a/b", mod_new, 8001), _spec("c/d", mod_new, 8002)), asyncio.Event()))
    await asyncio.sleep(0.1)
    assert not t.done()
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert fakes.db.closed and fakes.nc.closed
    leftover = {x for x in asyncio.all_tasks() if x not in before and x is not t and not x.done()}
    assert leftover == set()


async def test_member_serve_failure_exits_loudly_after_cleanup(fakes, monkeypatch):
    async def bad_serve(port, app, ev):
        raise OSError("address already in use")

    monkeypatch.setattr(besdk, "serve_http", bad_serve)
    started = asyncio.Event()

    async def mod_new(rt):
        async def start():
            started.set()
            await asyncio.Event().wait()
        return SimpleNamespace(asgi_app=None, register_grpc=None, start=start, stop=None)

    with pytest.raises(shell_runner.ShellMemberServeError, match="a/b"):
        await asyncio.wait_for(shell_runner.run(_cfg(_spec("a/b", mod_new)), asyncio.Event()), 3)
    assert fakes.db.closed and fakes.nc.closed
