"""new_logger——对应 be-sdk-go 的 logging_test.go。"""

from __future__ import annotations

import io
import json

from opentelemetry.sdk.trace import TracerProvider

from besdk.logging import _build_logger


def test_输出是合法json_且带component_id() -> None:
    stream = io.StringIO()
    logger = _build_logger(stream, "test-component")
    logger.info("hello world")

    line = stream.getvalue().strip()
    entry = json.loads(line)  # 不是合法 JSON 这里直接抛异常
    assert entry["component_id"] == "test-component"
    assert entry["msg"] == "hello world"
    assert entry["level"] == "info"


def test_extra字段被合并进json而不是丢失() -> None:
    stream = io.StringIO()
    logger = _build_logger(stream, "test-component")
    logger.info("with extra", extra={"order_id": "SO-123"})

    entry = json.loads(stream.getvalue().strip())
    assert entry["order_id"] == "SO-123"


def test_有span时自动带trace_id_span_id() -> None:
    """守 new_logger 的第二条：每条日志能拿到当前 span 就自动带
    trace_id/span_id，不需要业务代码手写 ``logger.info(..., trace_id=...)``。
    """
    stream = io.StringIO()
    logger = _build_logger(stream, "test-component")
    tracer = TracerProvider().get_tracer("test-tracer")

    with tracer.start_as_current_span("test-span"):
        logger.info("inside span")

    entry = json.loads(stream.getvalue().strip())
    assert "trace_id" in entry
    assert "span_id" in entry
    assert len(entry["trace_id"]) == 32  # 128-bit trace id 的十六进制表示
    assert len(entry["span_id"]) == 16  # 64-bit span id 的十六进制表示


def test_没有span时不带trace_id字段() -> None:
    stream = io.StringIO()
    logger = _build_logger(stream, "test-component")
    logger.info("no span here")

    entry = json.loads(stream.getvalue().strip())
    assert "trace_id" not in entry


def test_已知敏感字段被脱敏() -> None:
    stream = io.StringIO()
    logger = _build_logger(stream, "test-component")
    logger.info("联系方式", extra={"phone": "13800000000", "safe_field": "not secret"})

    entry = json.loads(stream.getvalue().strip())
    assert entry["phone"] == "[REDACTED]"
    assert entry["safe_field"] == "not secret"


def test_超过2kb的内容被截断() -> None:
    stream = io.StringIO()
    logger = _build_logger(stream, "test-component")
    logger.info("big payload", extra={"payload": "x" * 5000})

    line = stream.getvalue().strip()
    assert len(line.encode("utf-8")) <= 2048
    assert line.endswith("...[TRUNCATED]")


def test_两次new_logger同名不会互相污染handler() -> None:
    """守 ``_build_logger`` 顶部注释的那条判据：用 ``logging.Logger(name)``
    直接构造，不走 ``logging.getLogger`` 的全局缓存——同一个 component_id
    调两次不该让日志重复打印两遍。
    """
    stream1 = io.StringIO()
    stream2 = io.StringIO()
    logger1 = _build_logger(stream1, "same-name")
    logger2 = _build_logger(stream2, "same-name")

    logger1.info("only in stream1")
    logger2.info("only in stream2")

    assert "only in stream1" in stream1.getvalue()
    assert "only in stream2" not in stream1.getvalue()
    assert "only in stream2" in stream2.getvalue()
    assert "only in stream1" not in stream2.getvalue()
