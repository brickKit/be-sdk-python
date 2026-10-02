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
}


@pytest.fixture
def own_yaml(tmp_path):
    f = tmp_path / "component.yaml"
    f.write_text("deployment:\n  port: 18123\n")
    return str(f)


def _members(*ids):
    return json.dumps([
        {"componentId": i, "version": "1.0.0", "httpPort": 8000 + n, "extraPorts": [], "config": {}}
        for n, i in enumerate(ids)
    ])


async def _noop_module(rt):
    return SimpleNamespace(asgi_app=None, register_grpc=None, start=None, stop=None, migrations_dir=None)


def test_build_shell_config_ok(own_yaml):
    cfg = build_shell_config("py", {"a/b": _noop_module}, {**SHELL_ENV, "BRICKKIT_SERVED_MEMBERS_CONFIG": _members("a/b")}, own_yaml)
    assert cfg.health_port == 18123
    assert [m.member.component_id for m in cfg.modules] == ["a/b"]
    assert cfg.authz_bundle_url == "http://authz/bundle"
    assert cfg.otel_base_url == ""


@pytest.mark.parametrize("key", ["IAM_JWKS_URL", "AUTHZ_BUNDLE_URL"])
def test_build_shell_config_missing_shell_key_names_it_no_member_fallback(key, own_yaml):
    env = {k: v for k, v in SHELL_ENV.items() if k != key}
    raw = json.dumps([{"componentId": "a/b", "version": "1", "httpPort": 1, "config": {key: "http://from-member"}}])
    with pytest.raises(RuntimeError, match=key):
        build_shell_config("py", {"a/b": _noop_module}, {**env, "BRICKKIT_SERVED_MEMBERS_CONFIG": raw}, own_yaml)


def test_build_shell_config_missing_pg_key_names_it(own_yaml):
    env = {k: v for k, v in SHELL_ENV.items() if k != "PG_HOST"}
    with pytest.raises(ValueError, match="PG_HOST"):
        build_shell_config("py", {}, {**env, "BRICKKIT_SERVED_MEMBERS_CONFIG": "[]"}, own_yaml)


def test_unregistered_component_id_names_it(own_yaml):
    with pytest.raises(RuntimeError, match="x/unknown"):
        build_shell_config("py", {"a/b": _noop_module}, {**SHELL_ENV, "BRICKKIT_SERVED_MEMBERS_CONFIG": _members("x/unknown")}, own_yaml)


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


@pytest.mark.parametrize("body", ["deployment: {}\n", "name: x\n", "deployment:\n  port: 0\n",
                                  "deployment:\n  port: abc\n", "deployment:\n  port: 70000\n"])
def test_health_port_missing_or_invalid_is_error(tmp_path, body):
    f = tmp_path / "component.yaml"
    f.write_text(body)
    with pytest.raises(RuntimeError, match="deployment.port"):
        build_shell_config("py", {}, {**SHELL_ENV, "SHELL_HEALTH_PORT": "9999", "BRICKKIT_SERVED_MEMBERS_CONFIG": "[]"}, str(f))


def test_health_port_missing_file_is_error(tmp_path):
    with pytest.raises(RuntimeError, match="component.yaml"):
        build_shell_config("py", {}, {**SHELL_ENV, "BRICKKIT_SERVED_MEMBERS_CONFIG": "[]"}, str(tmp_path / "nope.yaml"))


# ---- 额外端口无 register_grpc（与 Go R19 同一判据）----

async def test_member_extra_port_without_register_grpc_fails_startup_naming_member_and_port(fakes):
    stopped = []

    async def no_grpc(rt):
        async def stop():
            stopped.append("a/b")
        return SimpleNamespace(asgi_app=None, register_grpc=None, start=None, stop=stop)

    spec = shell_runner.ModuleSpec(
        member=shell_runner.ServedMember("a/b", "1", 8001, {"grpc": 9400}, {}), new_module=no_grpc)
    with pytest.raises(RuntimeError) as ei:
        await asyncio.wait_for(shell_runner.run(_cfg(spec), asyncio.Event()), 3)
    msg = str(ei.value)
    assert "a/b" in msg and "grpc" in msg and "9400" in msg and "register_grpc" in msg
    assert fakes.started == [], "启动期校验失败时不应开始监听任何端口"
    assert fakes.db.closed and fakes.nc.closed
    assert stopped == ["a/b"], "已构造的成员也要被收尾"


# ---- 真实 uvicorn 绑定失败（SystemExit）也必须归到成员名下 ----

async def test_real_uvicorn_bind_conflict_is_attributed_to_member(fakes, monkeypatch, caplog):
    import socket

    from besdk.standalone import _serve_http

    # 真实的 serve_http：uvicorn 端口被占时在 startup 里 sys.exit(1)，绕过 except Exception。
    monkeypatch.setattr(besdk, "serve_http", _serve_http)
    blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    blocker.bind(("0.0.0.0", 0))
    blocker.listen(1)
    port = blocker.getsockname()[1]

    async def mod_new(rt):
        async def app(scope, receive, send):
            return None
        return SimpleNamespace(asgi_app=app, register_grpc=None, start=None, stop=None)

    try:
        with caplog.at_level(logging.ERROR):
            with pytest.raises(shell_runner.ShellMemberServeError, match="a/b"):
                await asyncio.wait_for(shell_runner.run(_cfg(_spec("a/b", mod_new, port)), asyncio.Event()), 10)
    finally:
        blocker.close()
    assert fakes.db.closed and fakes.nc.closed
    assert any(getattr(r, "module_component_id", "") == "a/b" for r in caplog.records), \
        "绑定失败的 ERROR 日志必须带 module_component_id"


# ---- 取消路径与 clean stop_event 路径下，成员 stop 都恰好被调用一次，且不抛异常 ----

def _stoppable_module(calls, cid):
    async def mod_new(rt):
        async def start():
            await asyncio.Event().wait()

        async def stop():
            calls.append(cid)
        return SimpleNamespace(asgi_app=None, register_grpc=None, start=start, stop=stop)
    return mod_new


async def test_external_cancel_calls_every_member_stop_once(fakes):
    calls: list[str] = []
    t = asyncio.create_task(shell_runner.run(
        _cfg(_spec("a/b", _stoppable_module(calls, "a/b"), 8001), _spec("c/d", _stoppable_module(calls, "c/d"), 8002)),
        asyncio.Event()))
    await asyncio.sleep(0.1)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    assert sorted(calls) == ["a/b", "c/d"]
    assert fakes.db.closed and fakes.nc.closed


async def test_clean_stop_event_with_members_does_not_raise_and_stops_each_once(fakes):
    calls: list[str] = []
    stop = asyncio.Event()
    t = asyncio.create_task(shell_runner.run(
        _cfg(_spec("a/b", _stoppable_module(calls, "a/b"), 8001), _spec("c/d", _stoppable_module(calls, "c/d"), 8002)),
        stop))
    await asyncio.sleep(0.1)
    assert sorted(fakes.started) == [8001, 8002]
    stop.set()
    await asyncio.wait_for(t, 3)  # 不抛 ShellMemberServeError 等任何异常
    assert t.exception() is None
    assert sorted(calls) == ["a/b", "c/d"]
    assert fakes.db.closed and fakes.nc.closed
