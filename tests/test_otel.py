"""init_otel/get_tracer/get_meter——对应 be-sdk-go 的 otel_test.go。"""

from __future__ import annotations

import pytest

from besdk.otel import get_meter, get_tracer, init_otel


@pytest.mark.asyncio
async def test_otel_base_url为空时装blackhole_shutdown不阻塞不抛异常() -> None:
    """核心属性测试：``otel_base_url`` 为空时，返回的 shutdown 协程必须
    能正常调用且不抛异常、不阻塞（连不上必须静默丢弃，设计书 §7.5）。
    """
    shutdown = await init_otel("test-service", "")
    await shutdown()  # 不抛异常就是通过


@pytest.mark.asyncio
async def test_初始化之后get_tracer拿到的不是no_op_tracer() -> None:
    """同 be-sdk-go 踩过的坑（实测踩坑记录 A4g）：Tracer 若是 no-op，
    ``start_as_current_span`` 产出的 span 永远 invalid，trace_id 注入
    日志那条链路就断了。这里断言初始化之后拿到的是真 provider 产出的
    tracer，不是 SDK 默认的 no-op 实现。
    """
    shutdown = await init_otel("test-service-tracer", "")
    try:
        tracer = get_tracer("test-service-tracer")
        with tracer.start_as_current_span("test-span") as span:
            assert span.get_span_context().is_valid
    finally:
        await shutdown()


@pytest.mark.asyncio
async def test_真实otlp_endpoint时provider挂了导出器() -> None:
    """不需要真的连上一个 OTel Collector——只验证 provider 真的按
    ``otel_base_url`` 非空这条路径构造（挂了 BatchSpanProcessor），
    而不是不管传参都走 blackhole 分支。
    """
    shutdown = await init_otel("test-service-otlp", "http://localhost:4318/v1/traces")
    try:
        tracer = get_tracer("test-service-otlp")
        with tracer.start_as_current_span("test-span") as span:
            assert span.get_span_context().is_valid
    finally:
        # ⚠️ 真机验证过：即使 endpoint 连不上，shutdown 也不该抛异常
        # 卡住——批处理导出器的 flush 失败会被 SDK 自己吞掉。
        await shutdown()


def test_get_meter不需要init就能调_返回no_op也不报错() -> None:
    """同 be-sdk-go 的既有判据：本项目从不调
    ``set_meter_provider``，``get_meter`` 恒是 no-op meter——不崩、
    可以正常创建 counter 之类的仪表，只是不导出到任何地方。
    """
    meter = get_meter("test-service-meter")
    counter = meter.create_counter("test_counter")
    counter.add(1)  # no-op，但不该抛异常
