"""结构化日志——对应 be-sdk-go 的 logging.go。

⚠️ 一处刻意的设计差异，别照 Go 版字面搬：Go 把 PII 脱敏/截断做成一个
业务代码要记得去调的公开函数 ``RedactPII``；这里改成 ``new_logger``
构造出的 Formatter 对**每一条日志的最终 JSON 文本**自动做同样的处理，
不依赖业务代码记得调用——`new_logger` 自己的文档就要求"这三条"是这个
logger 实例与生俱来的属性，而不是"用了才有"的可选项，自动挡比"业务代码
记得调"更符合这个项目一贯的 fail-closed 风格（同 §14.2 数据权限默认
不给、要显式开的判据）。
"""

from __future__ import annotations

import json
import logging as _stdlib_logging
import re
import sys
import time
from typing import IO

from opentelemetry import trace as _trace_api

_PII_FIELD_RE = re.compile(r'"(phone|mobile|id_card|password|bank_card|email)"\s*:\s*"[^"]*"')
_MAX_LOGGED_PAYLOAD = 2048  # §7.3：2KB 截断

# LogRecord 自带的属性名——格式化时要排除，只把业务代码通过
# `extra={...}` 传进来的字段当"额外字段"合并进 JSON。
_RESERVED_RECORD_ATTRS = frozenset(vars(_stdlib_logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {
    "message",
    "asctime",
}


def new_logger(component_id: str) -> _stdlib_logging.Logger:
    """构造已注入 component_id 与 trace 上下文的结构化 JSON 日志根
    （设计书 §7.3）。``bootstrap`` 调用它填 ``Runtime.logger``，恰好一次
    ——模块自己不许 ``logging.basicConfig()`` 式的重新初始化（§12.5.2：
    最后一个 init 的赢，N 个模块的日志格式被某个模块顶掉，而一路全绿）。

    守三条：① 输出是合法 JSON；② 每条日志能拿到当前 span 就自动带
    trace_id/span_id；③ 大字段截断到 2KB，已知敏感字段自动脱敏。
    """
    return _build_logger(sys.stdout, component_id)


def _build_logger(stream: IO[str], component_id: str) -> _stdlib_logging.Logger:
    """可测的核心构造——``new_logger`` 只是把输出钉死在 ``sys.stdout``。

    ⚠️ 用 ``logging.Logger(name)`` 直接构造一个独立实例，不用
    ``logging.getLogger(name)``——后者会命中全局 logger 字典的缓存：
    同一个 ``component_id`` 被调用两次（比如测试里反复 new_logger 同一个
    名字）会拿到同一个 Logger 对象、叠加出两份 handler，日志重复打印一遍。
    每个模块一个独立实例，同 ``metrics.new_registry`` 不用默认全局
    Registry 是同一个道理（设计书 §12.5.2）。
    """
    logger = _stdlib_logging.Logger(component_id)
    logger.setLevel(_stdlib_logging.INFO)
    logger.propagate = False
    handler = _stdlib_logging.StreamHandler(stream)
    handler.setFormatter(_JSONFormatter(component_id))
    logger.addHandler(handler)
    return logger


class _JSONFormatter(_stdlib_logging.Formatter):
    def __init__(self, component_id: str) -> None:
        super().__init__()
        self._component_id = component_id

    def format(self, record: _stdlib_logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + "Z",
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
            "component_id": self._component_id,
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_ATTRS:
                entry[key] = value

        span = _trace_api.get_current_span()
        ctx = span.get_span_context()
        if ctx.is_valid:
            entry["trace_id"] = format(ctx.trace_id, "032x")
            entry["span_id"] = format(ctx.span_id, "016x")

        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        text = json.dumps(entry, default=str, ensure_ascii=False)
        return _redact_and_truncate(text)


def _redact_and_truncate(text: str) -> str:
    def _redact(m: re.Match[str]) -> str:
        key_part = m.group(0).split(":", 1)[0]
        return f'{key_part}: "[REDACTED]"'

    redacted = _PII_FIELD_RE.sub(_redact, text)
    encoded = redacted.encode("utf-8")
    if len(encoded) > _MAX_LOGGED_PAYLOAD:
        marker = b"...[TRUNCATED]"
        cut = _MAX_LOGGED_PAYLOAD - len(marker)  # 截断标记本身也算在 2KB 预算内
        encoded = encoded[:cut] + marker
    return encoded.decode("utf-8", errors="ignore")
