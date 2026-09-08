"""OTel 初始化——对应 be-sdk-go 的 otel.go。目前只有签名，TDD 补实现。"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from opentelemetry.metrics import Meter
from opentelemetry.trace import Tracer


async def init_otel(service_name: str, otel_base_url: str) -> Callable[[], Awaitable[None]]:
    """初始化 OTel SDK（TracerProvider + MeterProvider）。

    ⚠️ ``otel_base_url`` 为空时装 Blackhole Exporter，不是报错、不是阻塞
    业务线程（设计书 §7.5：连不上必须静默丢弃）。这是 ``bootstrap`` 唯一
    调用它的地方——调用方（``run_standalone`` 或外壳）恰好调一次，模块
    自己永远不碰（§12.5.2）。

    实现随后用 TDD 补：最要紧的一条属性测试是"``otel_base_url`` 为空时，
    返回的 shutdown 协程必须能正常调用且不抛异常、不阻塞"。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")


def get_tracer(component_id: str) -> Tracer:
    """从已初始化的 provider 上取一个真 tracer。

    ⚠️ 必须在 ``init_otel`` 之后调用，否则拿到的是 no-op tracer——这正是
    be-sdk-go 真实踩过的坑（``docs/dev/实测踩坑记录.md`` A4g）：Runtime
    的 Tracer 字段若带着零值传下去，中间件一收到请求就 panic，包括
    ``/healthz`` 本身，容器因此永远不健康。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")


def get_meter(component_id: str) -> Meter:
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")
