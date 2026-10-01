"""Config 与 Runtime——对应 be-sdk-go 的 runtime.go。

Config 是模块读配置的唯一入口（设计书 §12.5.3）。模块代码里零 ``os.environ``。

数据来源不由 Config 自己决定：
    单跑：``run_standalone`` 从进程环境变量读一份快照灌进来
    合并：外壳启动器给每个模块一份只属于它自己的成员 config（BRICKKIT_SERVED_MEMBERS_CONFIG）
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg
    import nats.aio.client
    import opentelemetry.metrics
    import opentelemetry.trace
    import prometheus_client


class Config:
    """模块读配置的唯一入口。所有值都是字符串（平台把 configSchema 的
    每一项都渲染成环境变量），类型转换在这里做一次，业务代码不重复解析。
    """

    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    def string(self, key: str) -> tuple[str, bool]:
        """按键名精确取值。v1 起键名即环境变量名，原样注入，不做任何转换。"""
        v = self._values.get(key)
        if v is None:
            return "", False
        return v, True

    def endpoint(self, dep: str, extra: str = "") -> tuple[str, bool]:
        """从 Config 读 <ID>[_<PORT>]_ENDPOINT 并剥掉 scheme。外壳成员的依赖地址只在它
        自己的成员 config 里，绝不回落到进程环境。键不存在或值为空都视为缺失。"""
        from besdk.endpoint import env_name

        v, ok = self.string(env_name(dep, extra))
        if not ok or v == "":
            return "", False
        for p in ("http://", "https://"):
            if v.startswith(p):
                v = v[len(p):]
        return v.rstrip("/"), True

    def must_endpoint(self, dep: str, extra: str = "") -> str:
        from besdk.endpoint import env_name

        v, ok = self.endpoint(dep, extra)
        if not ok:
            raise RuntimeError(f"强依赖 {dep} 的 {env_name(dep, extra)} 未注入")
        return v

    def s3_url(self) -> tuple[str, bool]:
        v, ok = self.string("S3_URL")
        return (v, True) if ok and v else ("", False)

    def string_or(self, key: str, default: str) -> str:
        v, ok = self.string(key)
        return v if ok else default

    def must_string(self, key: str) -> str:
        """用于 configSchema 里没写 default 的必填项：拿不到直接抛异常，
        因为这类配置缺失属于部署错误，不该让模块带着一个空字符串跑起来。
        """
        v, ok = self.string(key)
        if not ok:
            raise RuntimeError(f"必填配置项 {key!r} 未注入")
        return v

    def int(self, key: str) -> tuple[int, bool]:
        v, ok = self.string(key)
        if not ok:
            return 0, False
        try:
            return int(v), True
        except ValueError:
            return 0, False

    def int_or(self, key: str, default: int) -> int:
        n, ok = self.int(key)
        return n if ok else default

    def bool(self, key: str) -> tuple[bool, bool]:
        v, ok = self.string(key)
        if not ok:
            return False, False
        lowered = v.strip().lower()
        if lowered in ("true", "1", "t", "yes"):
            return True, True
        if lowered in ("false", "0", "f", "no"):
            return False, True
        return False, False

    def bool_or(self, key: str, default: bool) -> bool:
        b, ok = self.bool(key)
        return b if ok else default


@dataclass
class Runtime:
    """调用方交给模块的一切。模块自己不去取任何一样（设计书 §12.5、
    §13.3 铁律七，总纲 SOP-L 的 L-1）。

        单跑：由 run_standalone 填（读进程环境变量、自己开池、自己连 NATS）
        合并：由外壳启动器填（本模块那一份 env map、外壳的唯一全局池、
              共用的 NATS 连接）
    """

    component_id: str
    component_version: str
    config: Config
    db: "asyncpg.Pool"  # noqa: UP037 - 前向引用，见 TYPE_CHECKING
    nats: "nats.aio.client.Client"
    logger: logging.Logger
    tracer: "opentelemetry.trace.Tracer"
    meter: "opentelemetry.metrics.Meter"
    registry: "prometheus_client.CollectorRegistry"  # ⭐ 每模块一个，不是默认全局那个
    http_port: int
    extra_ports: dict[str, int] = field(default_factory=dict)
