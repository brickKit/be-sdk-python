"""外壳级构件——对应 be-sdk-go 的 shell.go。

阶段四 Task 3：``be-shell-python`` 需要复用 ``run_standalone`` 已经
验证过的构造/Listen 逻辑，不是重新实现一遍——理由与 Task 2 给
``be-sdk-go`` 补同类构件完全一致（阶段三踩坑记录 A4g：只有小范围调用
方走过的构造路径容易藏 bug，外壳自己重写等于把这类风险放大到 N 倍
爆炸半径）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from besdk.authz import _set_authz_runtime, setup_authz_runtime
from besdk.logging import new_logger
from besdk.metrics import new_registry
from besdk.otel import get_meter, get_tracer
from besdk.runtime import Config, Runtime
from besdk.standalone import _serve_extra_port, _serve_http

if TYPE_CHECKING:
    import asyncpg
    import nats.aio.client


@dataclass
class ShellModuleConfig:
    """外壳装配单个模块 Runtime 所需的全部输入——``run_standalone`` 从
    进程环境变量 + 自己的 ``component.yaml`` 推导出这些值，外壳不能这样
    做：一个进程只有一份 ``os.environ``，N 个模块各自的
    ``DATABASE_*``/``COMPONENT_ID``/config 项互相顶掉，不报错，模块按
    别人的 schema 建表写数据（设计书 §12.5.3、决策 110）。每个字段都
    必须来自 ``be-ops`` 产出 4/7（合并清单 + 每外壳环境变量表，设计书
    §13.8.2），不能从外壳进程自己的 ``os.environ`` 读。
    """

    component_id: str
    component_version: str
    env: dict[str, str]
    http_port: int
    extra_ports: dict[str, int] = field(default_factory=dict)


def new_shell_runtime(
    cfg: ShellModuleConfig, db: "asyncpg.Pool", nc: "nats.aio.client.Client"
) -> Runtime:
    """为外壳里的一个模块构造 Runtime，字段逐一对应
    ``_run_standalone_async`` 自己组装 Runtime 的那一段（standalone.py），
    区别只在 db/nats 由外壳传入共享实例，不是各自新开一个连接——设计书
    §13.3 铁律二：一个外壳一个连接池，模块通过 ``besdk.with_tx`` 的
    ``SET LOCAL ROLE`` 切身份，从不持有自己的池。

    tracer/meter 仍然按模块各自的 component_id 取，只是从共享的
    provider 上取一个按名字区分的 instrumentation scope，不是重新
    初始化一份——真正"只能有一份"的 provider 由 ``bootstrap`` 在外壳
    启动最开始装配一次，这里不重复调。
    """
    return Runtime(
        component_id=cfg.component_id,
        component_version=cfg.component_version,
        config=Config(cfg.env),
        db=db,
        nats=nc,
        logger=new_logger(cfg.component_id),
        tracer=get_tracer(cfg.component_id),
        meter=get_meter(cfg.component_id),
        registry=new_registry(),
        http_port=cfg.http_port,
        extra_ports=cfg.extra_ports,
    )


def init_shell_authz(iam_jwks_url: str, authz_bundle_url: str, logger: logging.Logger) -> None:
    """给整个外壳进程装配**恰好一份** JWT 验签器 + bundle 轮询——不是
    每个模块各调一次。

    ⚠️ ``besdk.authz`` 的 ``_verifier``/``_bundle`` 是模块级全局状态，
    ``run_standalone`` 假设"一个进程一个模块"所以调一次没有问题；外壳
    如果照搬"每个模块各自调 setup_authz_runtime"，N 次调用里只有最后
    一次真正生效（前面模块的 ``require_permission`` 判定全部会被换成
    最后一个模块的配置），且前面启动的 bundle 轮询任务从此没人再持有
    引用、没人能取消——这正是导读第 17 条"最后一个 init 的赢"那类问题
    在权限判定状态上的翻版。本项目目前全部业务组件的
    iam_jwks_url/authz_bundle_url 都指向同一个 infra-iam-casdoor/
    infra-authz 实例，所以外壳只需要用任意一个模块的配置调一次本函数
    即可对齐全部模块的判定行为——调用时机：``bootstrap`` 之后、任何
    模块的 HTTP/gRPC server 开始接请求之前。
    """
    verifier, bundle = setup_authz_runtime(iam_jwks_url, authz_bundle_url, logger)
    _set_authz_runtime(verifier, bundle)


# serve_http/serve_extra_port 是 _serve_http/_serve_extra_port 的公开
# 别名，供外壳按模块循环调用——不是重新实现一遍。
serve_http = _serve_http
serve_extra_port = _serve_extra_port
