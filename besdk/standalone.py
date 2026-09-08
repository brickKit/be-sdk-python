"""bootstrap / run_standalone——对应 be-sdk-go 的 standalone.go。

``run_standalone`` 是单跑形态的全部装配，也是全项目唯一允许读进程环境
变量的地方（设计书 §12.5.3）。它做：bootstrap（OTel/日志，进程级一次）
→ 填 Runtime → 调 ``new_module`` → 起 HTTP 与全部 extra_ports →
装信号处理器 → 优雅关停。

于是每个组件的 ``main.py`` 只有几行：

    if __name__ == "__main__":
        besdk.run_standalone(module.new)
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import Awaitable, Callable

import asyncpg
import uvicorn
from grpc import aio as grpc_aio

from besdk.manifest import load_own_ports
from besdk.module import Module
from besdk.runtime import Config, Runtime

_SHUTDOWN_TIMEOUT_SECONDS = 30


def _exitf(component_id: str, message: str) -> None:
    prefix = f"[{component_id}] " if component_id else ""
    print(prefix + message, file=sys.stderr)
    sys.exit(1)


def _must_getenv(key: str, component_id: str = "") -> str:
    """全项目唯一允许读进程环境变量的地方之一（另一处是 ``_env_snapshot``）。
    模块代码里出现 ``os.environ`` 就是违规。
    """
    v = os.environ.get(key)
    if v is None:
        _exitf(component_id or os.environ.get("COMPONENT_ID", ""), f"必需的环境变量 {key} 未设置")
    return v  # type: ignore[return-value]  # _exitf 不返回，mypy 看不出来


def _env_snapshot() -> dict[str, str]:
    """把当前进程环境变量拍成一份快照灌进 Config。

    ⚠️ 合并态下这个函数不会被调用——外壳启动器会给每个模块构造自己那一份
    env map（§13.8.2），不是从共享的 ``os.environ`` 里读，否则就是
    "N 个模块的 PG_SCHEMA 互相顶掉"那条雷（§12.5.3）。
    """
    return dict(os.environ)


def _build_pg_dsn(component_id: str) -> str:
    """从平台注入的 ``DATABASE_*`` 前缀变量拼出 asyncpg 认得的 DSN。

    ⚠️ 没有单个 ``PG_DSN`` 这种东西——资源注入是分开的五个变量
    （HOST/PORT/USER/PASSWORD/NAME）。sslmode 走 asyncpg 的默认（禁用），
    TLS 需求留给未来客户按需求提。
    """
    host = _must_getenv("DATABASE_HOST", component_id)
    port = _must_getenv("DATABASE_PORT", component_id)
    user = _must_getenv("DATABASE_USER", component_id)
    password = _must_getenv("DATABASE_PASSWORD", component_id)
    name = _must_getenv("DATABASE_NAME", component_id)
    return f"postgres://{user}:{password}@{host}:{port}/{name}"


def _build_nats_url() -> str:
    """从 ``MQ_*`` 前缀变量拼出 nats-py 认得的 URL。

    ⚠️ 同样没有单个 ``NATS_URL``。``MQ_USER``/``MQ_PASSWORD`` 是否存在
    取决于这个部署的 nats 资源有没有配认证，不能假设一定有。
    """
    host = os.environ.get("MQ_HOST", "")
    port = os.environ.get("MQ_PORT", "")
    user = os.environ.get("MQ_USER")
    password = os.environ.get("MQ_PASSWORD", "")
    if user:
        return f"nats://{user}:{password}@{host}:{port}"
    return f"nats://{host}:{port}"


async def bootstrap(component_id: str, otel_base_url: str) -> Callable[[], Awaitable[None]]:
    """做进程级、只能有一份的那些初始化（OTel provider、日志根）。
    调用方（``run_standalone`` 或外壳）调它恰好一次；模块一律不许碰
    （设计书 §12.5.2）。

    ⚠️ 依赖 ``besdk.otel.init_otel``，那个函数目前只有签名（阶段三 Task 1
    还没补），调用会抛 ``NotImplementedError``——这是预期行为，不是 bug，
    随后的 TDD 任务会补上。**这个函数本身的结构现在就要钉死**，因为它决定
    了外壳启动器合并多个模块时的装配顺序（总纲 SOP-L L-1）。
    """
    from besdk.otel import init_otel  # 延迟 import：otel.py 现在只是 stub

    return await init_otel(component_id, otel_base_url)


def run_standalone(new_module: Callable[["Runtime"], Awaitable[Module]]) -> None:
    asyncio.run(_run_standalone_async(new_module))


async def _run_standalone_async(new_module: Callable[["Runtime"], Awaitable[Module]]) -> None:
    component_id = _must_getenv("COMPONENT_ID")
    component_version = _must_getenv("COMPONENT_VERSION")

    # ⚠️ 端口不是平台注入的（§13.8.1）：环境变量表里只有"别人在哪"
    # （*_ENDPOINT），没有"我该监听哪"。唯一权威来源是组件自己的
    # component.yaml——be-sdk-go v0.1.0 曾经等一个从来不存在的 HTTP_PORT
    # 环境变量，同样的坑本仓库从第一版就避开。
    try:
        ports = load_own_ports("component.yaml")
    except Exception as exc:  # noqa: BLE001 - 启动期失败必须整体退出
        _exitf(component_id, f"读自己的 component.yaml 失败：{exc}")
        return

    shutdown_otel = await bootstrap(component_id, os.environ.get("OTEL_BASE_URL", ""))

    try:
        pg_dsn = _build_pg_dsn(component_id)
    except SystemExit:
        raise
    db_pool = await asyncpg.create_pool(pg_dsn)

    # ⚠️ NATS 连接同样等 events.py/otel.py 补上真实实现后才真正被消费；
    # 这里先把 Runtime 的形状钉死。nats-py 的 connect 目前用占位 URL，
    # 阶段三 Task 1 的 TDD 任务里补真实错误处理。
    import nats  # noqa: PLC0415 - 避免顶层强依赖 nats-py 的连接副作用

    nc = await nats.connect(_build_nats_url())

    from besdk.logging import new_logger  # noqa: PLC0415 - logging.py 现在只是 stub
    from besdk.metrics import new_registry  # noqa: PLC0415 - 同上
    from besdk.otel import get_meter, get_tracer  # noqa: PLC0415 - 同上，延迟到确有 provider 后再取

    rt = Runtime(
        component_id=component_id,
        component_version=component_version,
        config=Config(_env_snapshot()),
        db=db_pool,
        nats=nc,
        logger=new_logger(component_id),
        tracer=get_tracer(component_id),
        meter=get_meter(component_id),
        registry=new_registry(),
        http_port=ports.http_port,
        extra_ports=ports.extra_ports,
    )

    # ⚠️ 权限判定的进程级状态在这里装配一次，同 Tracer/Meter 那一类
    # "只能有一份、模块不许自己碰"的东西（§12.5.2）。iam_jwks_url/
    # authz_bundle_url 任一没配都保持阶段一的 fail-closed stub 行为。
    from besdk.authz import _set_authz_runtime, setup_authz_runtime  # noqa: PLC0415 - 避免顶层循环 import

    iam_jwks_url, _ = rt.config.string("iamJwksUrl")
    authz_bundle_url, _ = rt.config.string("authzBundleUrl")
    verifier, bundle = setup_authz_runtime(iam_jwks_url, authz_bundle_url, rt.logger)
    _set_authz_runtime(verifier, bundle)

    mod = await new_module(rt)

    # ⚠️ 全拆态迁移不在这里跑：平台为每个组件单独生成一次性迁移容器，
    # run_standalone 服务的是应用进程本身。mod.migrations_dir 只被合并态
    # 的外壳启动器消费（§13.3 铁律五，阶段四）。

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    tasks = [asyncio.create_task(_serve_http(rt.http_port, mod.asgi_app, stop_event))]
    for name, port in rt.extra_ports.items():
        tasks.append(asyncio.create_task(_serve_extra_port(name, port, mod.register_grpc, stop_event)))
    if mod.start is not None:
        tasks.append(asyncio.create_task(mod.start()))

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in done:
        exc = task.exception()
        if exc is not None:
            rt.logger.error("服务异常退出：%s", exc)

    stop_event.set()
    for task in pending:
        task.cancel()

    if mod.stop is not None:
        await asyncio.wait_for(mod.stop(), timeout=_SHUTDOWN_TIMEOUT_SECONDS)

    await db_pool.close()
    await nc.close()
    await shutdown_otel()


async def _serve_http(port: int, app, stop_event: asyncio.Event) -> None:
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")  # noqa: S104 - 容器内监听所有网卡是预期行为
    server = uvicorn.Server(config)

    async def _watch_stop() -> None:
        await stop_event.wait()
        server.should_exit = True

    watcher = asyncio.create_task(_watch_stop())
    try:
        await server.serve()
    finally:
        watcher.cancel()


async def _serve_extra_port(
    name: str,
    port: int,
    register: Callable[[grpc_aio.Server], None] | None,
    stop_event: asyncio.Event,
) -> None:
    if register is None:
        return
    server = grpc_aio.server()
    register(server)
    server.add_insecure_port(f"[::]:{port}")
    await server.start()

    async def _watch_stop() -> None:
        await stop_event.wait()
        await server.stop(grace=10)

    watcher = asyncio.create_task(_watch_stop())
    try:
        await server.wait_for_termination()
    finally:
        watcher.cancel()
