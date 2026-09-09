"""OTel 初始化——对应 be-sdk-go 的 otel.go。"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from opentelemetry import metrics as _metrics_api
from opentelemetry import trace as _trace_api
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Meter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Tracer


async def init_otel(service_name: str, otel_base_url: str) -> Callable[[], Awaitable[None]]:
    """初始化 OTel SDK（当前只接 TracerProvider；指标那半靠
    ``metrics.py`` 的 Prometheus Registry 单独覆盖，不重复——同
    be-sdk-go 的既有判据：``Runtime.meter`` 因此恒是 no-op meter，这是
    刻意的，不是漏做）。

    ⚠️ ``otel_base_url`` 为空时装 Blackhole：``TracerProvider`` 一个
    ``SpanProcessor`` 都不挂，span 该怎么创建就怎么创建（业务代码零
    感知），但没有任何导出器消费它们——不联网、不重试、不阻塞，创建即
    丢弃（设计书 §7.5：连不上必须静默丢弃）。这是 ``bootstrap`` 唯一
    调用它的地方，恰好一次，模块自己永远不碰（§12.5.2）。
    """
    resource = Resource.create({"service.name": service_name})

    if not otel_base_url:
        provider = TracerProvider(resource=resource)
        _trace_api.set_tracer_provider(provider)
        return _shutdown_of(provider)

    # ⚠️ 导出器用异步批处理（BatchSpanProcessor），不是每个 span 同步
    # 发送——otlp exporter 的连接失败会在后台重试/丢弃，不会让
    # tracer.start_span()/span.end() 阻塞或报错，这正是"连不上必须静默
    # 丢弃"在代码层面的落点（同 be-sdk-go InitOTel 的既有判据）。
    exporter = OTLPSpanExporter(endpoint=otel_base_url)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(exporter, schedule_delay_millis=5000))
    _trace_api.set_tracer_provider(provider)
    return _shutdown_of(provider)


def _shutdown_of(provider: TracerProvider) -> Callable[[], Awaitable[None]]:
    async def _shutdown() -> None:
        # provider.shutdown() 是同步方法，真实导出器场景下会做最后一次
        # flush（可能有网络 I/O）——丢进线程池，不阻塞事件循环。
        await asyncio.to_thread(provider.shutdown)

    return _shutdown


def get_tracer(component_id: str) -> Tracer:
    """从已初始化的全局 provider 上取一个真 tracer。

    ⚠️ 必须在 ``init_otel`` 之后调用，否则拿到的是 no-op tracer——这正是
    be-sdk-go 真实踩过的坑（``docs/dev/实测踩坑记录.md`` A4g）：Runtime
    的 Tracer 字段若带着零值传下去，中间件一收到请求就出问题，包括
    ``/healthz`` 本身，容器因此永远不健康。
    """
    return _trace_api.get_tracer(component_id)


def get_meter(component_id: str) -> Meter:
    """同 be-sdk-go 的既有判据：本项目从不调用
    ``opentelemetry.metrics.set_meter_provider``，所以这里恒是 SDK 默认
    的 no-op meter——指标完全靠 ``metrics.py`` 的 Prometheus Registry，
    这个字段只是为了跟 Go 版 ``Runtime`` 形状保持一致，不是遗漏。
    """
    return _metrics_api.get_meter(component_id)
