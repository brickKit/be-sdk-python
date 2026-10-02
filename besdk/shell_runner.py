"""外壳启动器——Python 外壳进程的全部装配逻辑，从已退役的 shells/python 仓库迁入 SDK。

brickKit v1 起，平台在外壳启动前用每个成员自己的镜像跑完该成员的迁移，所以这里
**不跑迁移**。成员清单来自 ``BRICKKIT_SERVED_MEMBERS_CONFIG``（v1 JSON 数组，每项
``{componentId, version, httpPort, extraPorts:[{name,port}], config:{KEY: 已求值的值}}``）；
成员的依赖地址（``*_ENDPOINT``）在它自己的 ``config`` 里，不在进程环境里。零成员时平台给 ``[]``。

装配复用 run_standalone 已验证过的构件（``new_shell_runtime`` / ``init_shell_authz`` /
``serve_http`` / ``serve_extra_port``），不重新实现一遍。

⚠️ 外壳失败契约（与 Go 外壳一致）：
- 成员的 HTTP/gRPC 监听或服务任务以异常结束（如端口绑定失败，含 uvicorn 绑定失败时的
  ``SystemExit``）-> 收尾后外壳非零退出（``ShellMemberServeError``，日志带成员 component_id）；
  健康检查绿着而成员端口已死是静默故障，必须响亮。
- 成员声明了额外端口而 ``new_module`` 没有返回 ``register_grpc`` -> 启动即失败，点名成员与端口。
- 成员 ``start()``/后台任务启动后抛异常 -> 只记日志（带 component_id），其余成员继续服务。
- 启动期成员 ``new_module`` 失败 -> 先收掉共享池与 NATS 再中止。
- 外壳 /healthz 监听自己 ``./component.yaml`` 的 ``deployment.port``（缺失/非法/0 直接报错，不读环境变量、无默认端口）。
- 其余退出：``stop_event`` 被 set（信号）或外壳自己的 /healthz 服务退出。
编排不用 ``asyncio.TaskGroup``：``serve_*`` 靠 ``stop_event`` 优雅退出，``start()`` 靠 ``.cancel()``。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import asyncpg
import nats

import besdk
from besdk.connection import nats_url, pg_dsn
from besdk.manifest import load_own_http_port
from besdk.runtime import Config
from besdk.shell import ShellModuleConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from besdk import Module, Runtime

_SHUTDOWN_TIMEOUT_SECONDS = 30


@dataclass
class ServedMember:
    component_id: str
    version: str
    http_port: int
    extra_ports: dict[str, int]
    config: dict[str, str]


def parse_served_members(raw: str | None) -> list[ServedMember]:
    """raw 是 os.environ.get("BRICKKIT_SERVED_MEMBERS_CONFIG")。
    None（未设置）与空串都报错；"[]" 是零成员。绝不退化成"启动全部编译进来的模块"。"""
    if raw is None:
        raise RuntimeError("BRICKKIT_SERVED_MEMBERS_CONFIG 未设置：这个进程看起来不是由 brickkit 作为外壳启动的")
    if raw.strip() == "":
        raise RuntimeError("BRICKKIT_SERVED_MEMBERS_CONFIG 为空字符串：零成员时平台给的是 []")
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"解析 BRICKKIT_SERVED_MEMBERS_CONFIG 失败：{e}") from e
    return [
        ServedMember(
            component_id=i["componentId"], version=i["version"], http_port=int(i["httpPort"]),
            extra_ports={p["name"]: int(p["port"]) for p in i.get("extraPorts") or []},
            config={k: str(v) for k, v in (i.get("config") or {}).items()},
        )
        for i in items
    ]


@dataclass
class ModuleSpec:
    """外壳要装的一个模块：成员数据 + 静态登记的 ``new_module``。"""

    member: ServedMember
    new_module: "Callable[[Runtime], Awaitable[Module]]"


@dataclass
class ShellConfig:
    """外壳进程级的全部输入。连接串由外壳自己的进程环境（``Config(dict(os.environ))``）
    经 ``pg_dsn`` / ``nats_url`` 得到。"""

    shell_name: str
    otel_base_url: str = ""
    pg_dsn: str = ""
    nats_url: str = ""
    iam_jwks_url: str = ""
    authz_bundle_url: str = ""
    health_port: int = 0
    modules: list[ModuleSpec] = field(default_factory=list)


@dataclass
class _Built:
    spec: ModuleSpec
    rt: "Runtime"
    mod: "Module"


async def run(cfg: ShellConfig, stop_event: asyncio.Event | None = None) -> None:
    """装配整个外壳：``bootstrap`` 一次 → 共享连接池/NATS 连接 → ``init_shell_authz`` 一次
    → 逐模块 ``new_module`` → 起全部服务任务 → 任一任务结束或 ``stop_event`` 被 set 后
    优雅关停。不跑迁移（平台已用成员自己的镜像跑完）。

    ``stop_event`` 留空时自己建一个，测试时外部 set 即可触发优雅关闭。
    """
    if stop_event is None:
        stop_event = asyncio.Event()

    logger = logging.getLogger(cfg.shell_name)

    shutdown_otel = await besdk.bootstrap(cfg.shell_name, cfg.otel_base_url)

    db = await asyncpg.create_pool(cfg.pg_dsn)
    nc = await nats.connect(cfg.nats_url)

    besdk.init_shell_authz(cfg.iam_jwks_url, cfg.authz_bundle_url, logger)

    built: list[_Built] = []
    try:
        for spec in cfg.modules:
            m = spec.member
            rt = besdk.new_shell_runtime(
                ShellModuleConfig(
                    component_id=m.component_id,
                    component_version=m.version,
                    env=m.config,
                    http_port=m.http_port,
                    extra_ports=m.extra_ports,
                ),
                db,
                nc,
            )
            mod = await spec.new_module(rt)
            built.append(_Built(spec=spec, rt=rt, mod=mod))
            # 声明了额外端口却没有 register_grpc：serve_extra_port 对 None 直接返回，那个端口
            # 没人监听，外壳 /healthz 却是绿的——与 Go 外壳同一判据，启动即失败并点名成员与端口。
            if m.extra_ports and getattr(mod, "register_grpc", None) is None:
                ports = ", ".join(f"{n}(:{p})" for n, p in sorted(m.extra_ports.items()))
                raise RuntimeError(f"成员 {m.component_id} 声明了额外端口 {ports}，但 new_module 没有返回 register_grpc")
    except BaseException:
        # 启动期某成员构造失败：允许整体中止，但必须先收掉共享池与 NATS（及已构造成员）。
        await _shutdown(built, db, nc, shutdown_otel, logger)
        raise

    try:
        await supervise(built, cfg.health_port, stop_event, logger)
    finally:
        # 任何退出路径（信号、自身 healthz 失败、外部取消、意外异常）都恰好收尾一次。
        await _shutdown(built, db, nc, shutdown_otel, logger)


async def _shutdown(built: "list[_Built]", db, nc, shutdown_otel, logger: logging.Logger) -> None:
    for b in built:
        if b.mod.stop is not None:
            try:
                await asyncio.wait_for(b.mod.stop(), timeout=_SHUTDOWN_TIMEOUT_SECONDS)
            except Exception:  # noqa: BLE001 - 单成员停止失败不影响其余成员收尾
                logger.exception("模块停止失败", extra={"module_component_id": b.spec.member.component_id})
    await db.close()
    await nc.close()
    await shutdown_otel()


async def _guard(component_id: str, what: str, coro, logger: logging.Logger) -> None:
    """成员级 panic 隔离：成员任务里的异常只记日志（带 component_id），不外抛，其余成员继续服务。
    取消（CancelledError）照常向上传播。"""
    try:
        await coro
    except Exception:  # noqa: BLE001 - 隔离边界：任何成员异常都不得拖垮外壳
        logger.exception("成员任务异常退出，其余成员继续运行：%s", what, extra={"module_component_id": component_id})


class ShellMemberServeError(RuntimeError):
    """某成员的 HTTP/gRPC 监听或服务任务以异常结束（如端口绑定失败）。外壳收尾后以非零退出：
    健康检查绿着而成员端口已死是静默故障，必须响亮。"""


async def _serve_guard(
    component_id: str, what: str, coro, stop_event: asyncio.Event, failures: list[str], logger: logging.Logger
) -> None:
    """成员的监听/服务任务：异常 = 致命。记日志、登记失败并触发整体优雅关停，由 supervise 收尾后抛出。

    ⚠️ 也要接住 ``SystemExit``：uvicorn 端口绑定失败时在 startup 里 ``sys.exit(1)``，它不是
    ``Exception``，不接住就会从 Task 里直接冲出事件循环——退出码虽非零，但日志不带成员归属，
    收尾也被跳过。"""
    try:
        await coro
    except (Exception, SystemExit) as exc:  # noqa: BLE001
        logger.exception("成员监听/服务失败：%s", what, extra={"module_component_id": component_id})
        detail = f"SystemExit({exc.code})（uvicorn 启动失败，多为端口被占）" if isinstance(exc, SystemExit) else str(exc)
        failures.append(f"{component_id} {what}: {detail}")
        stop_event.set()


async def supervise(
    built: "list[_Built]", health_port: int, stop_event: asyncio.Event, logger: logging.Logger
) -> None:
    """起全部成员任务并监督。失败契约：成员的 HTTP/gRPC 监听或服务任务以异常结束 -> 优雅关停
    全部并抛 ``ShellMemberServeError``（外壳非零退出）；成员 ``start()`` 后台异常 -> 只记日志，
    其余成员继续；外壳自己的 /healthz 服务退出或 stop_event 被 set -> 正常收尾。
    """
    member_tasks: list[asyncio.Task] = []
    serve_failures: list[str] = []
    for b in built:
        m = b.spec.member
        cid = m.component_id
        member_tasks.append(asyncio.create_task(
            _serve_guard(cid, "http", besdk.serve_http(m.http_port, b.mod.asgi_app, stop_event),
                         stop_event, serve_failures, logger)))
        for name, port in m.extra_ports.items():
            member_tasks.append(asyncio.create_task(
                _serve_guard(cid, f"grpc:{name}", besdk.serve_extra_port(name, port, b.mod.register_grpc, stop_event),
                             stop_event, serve_failures, logger)))
        if b.mod.start is not None:
            member_tasks.append(asyncio.create_task(_guard(cid, "start", _watch_and_run_start(b, stop_event), logger)))

    stop_waiter = asyncio.create_task(stop_event.wait())
    waiters = [stop_waiter]
    health_task: asyncio.Task | None = None
    try:
        if health_port:
            health_task = asyncio.create_task(_serve_health(health_port, stop_event))
            waiters.append(health_task)

        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        if health_task is not None and health_task.done() and not health_task.cancelled() and health_task.exception():
            logger.error("外壳健康检查服务异常退出：%s", health_task.exception())
    finally:
        # 无论正常退出、外部取消还是异常：取消并等待全部任务，不留孤儿任务。
        stop_event.set()
        everything = [*member_tasks, *waiters]
        for t in everything:
            if not t.done():
                t.cancel()
        await asyncio.gather(*everything, return_exceptions=True)
    if serve_failures:
        raise ShellMemberServeError("成员监听/服务失败，外壳退出：" + "; ".join(serve_failures))


async def _watch_and_run_start(b: _Built, stop_event: asyncio.Event) -> None:
    """``Module.start`` 约定"取消时必须返回"（靠 ``Task.cancel()``）——额外起一个 watcher，
    ``stop_event`` 被 set 时取消 ``start()``，对齐 HTTP/gRPC 走 ``stop_event`` 的路径。
    start() 抛出的异常原样外抛，由外层 ``_guard`` 记日志并隔离。
    """
    watcher = asyncio.create_task(stop_event.wait())
    task = asyncio.create_task(b.mod.start())
    try:
        done, _ = await asyncio.wait({watcher, task}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            task.result()
        else:
            task.cancel()
    finally:
        watcher.cancel()
        if not task.done():
            task.cancel()
        await asyncio.gather(watcher, task, return_exceptions=True)


async def _serve_health(port: int, stop_event: asyncio.Event) -> None:
    """外壳进程自己对外的健康检查端口：只答"进程活着"，不查任何模块/依赖。

    ⚠️ 路径必须是 ``/healthz``，且显式同时注册 GET 与 HEAD：平台生成的健康检查用
    ``wget --spider``（发 HEAD），只注册 GET 会让每次探测都 404/405
    （见 ``besdk/fastapi_app.py`` 顶部同一个坑）。
    """
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/healthz")
    @app.head("/healthz")
    async def _healthz() -> dict[str, bool]:
        return {"ok": True}

    await besdk.serve_http(port, app, stop_event)


def build_shell_config(
    shell_name: str,
    registry: "dict[str, Callable[[Runtime], Awaitable[Module]]]",
    environ: "dict[str, str] | None" = None,
    component_yaml: str = "component.yaml",
) -> ShellConfig:
    """从进程环境 + ``BRICKKIT_SERVED_MEMBERS_CONFIG`` 构造 ``ShellConfig``。

    ``registry`` 是静态登记的 componentId -> new_module（不能凭字符串反射着 import）。
    成员清单里出现 registry 没登记的组件直接报错。
    """
    env = dict(os.environ) if environ is None else environ
    members = parse_served_members(env.get("BRICKKIT_SERVED_MEMBERS_CONFIG"))

    specs: list[ModuleSpec] = []
    for m in members:
        new_module = registry.get(m.component_id)
        if new_module is None:
            raise RuntimeError(
                f"组件 {m.component_id} 在 BRICKKIT_SERVED_MEMBERS_CONFIG 里，"
                "但 registry 没有登记它的 new_module——是不是漏了给它加 import"
            )
        specs.append(ModuleSpec(member=m, new_module=new_module))

    # 外壳自己的配置只来自外壳进程环境（它自己的 configSchema 声明了这些键），
    # 绝不从成员 config 里兜底。缺必需键直接报错并点名；OTEL_BASE_URL 缺省 = 不导出。
    shell_cfg = Config(env)
    return ShellConfig(
        shell_name=shell_name,
        otel_base_url=shell_cfg.string_or("OTEL_BASE_URL", ""),
        pg_dsn=pg_dsn(shell_cfg),
        nats_url=nats_url(shell_cfg),
        iam_jwks_url=shell_cfg.must_string("IAM_JWKS_URL"),
        authz_bundle_url=shell_cfg.must_string("AUTHZ_BUNDLE_URL"),
        health_port=load_own_http_port(component_yaml),
        modules=specs,
    )


async def _main(shell_name: str, registry: "dict[str, Callable[[Runtime], Awaitable[Module]]]") -> None:
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)
    await run(build_shell_config(shell_name, registry), stop_event)


def main(shell_name: str, registry: "dict[str, Callable[[Runtime], Awaitable[Module]]]") -> None:
    """外壳进程入口：``besdk.shell_runner.main("py-render", {"infra/print": create_module})``。
    任何装配或服务失败（含 new_module 抛出的任意异常类型）都打印 ``[shell] 消息`` 并以 1 退出。"""
    try:
        asyncio.run(_main(shell_name, registry))
    except Exception as exc:  # noqa: BLE001 - 入口兜底：任何装配/服务失败都同一格式、退出码 1
        detail = str(exc) if isinstance(exc, (RuntimeError, ValueError)) else f"{type(exc).__name__}: {exc}"
        print(f"[{shell_name}] {detail}", file=sys.stderr)
        sys.exit(1)
