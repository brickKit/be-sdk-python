"""Each member's own logger, metrics registry and tracer provider (protocol 17's shell invariant: everything per
member). Nothing here touches process-wide state: no logging.basicConfig, no root or named global loggers, no
prometheus_client.REGISTRY, no opentelemetry.trace.set_tracer_provider.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass
from typing import IO, Any

from opentelemetry.propagators.textmap import TextMapPropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from prometheus_client import CollectorRegistry

# The W3C trace-context propagator (traceparent, tracestate): a value, not the global one.
PROPAGATOR: TextMapPropagator = TraceContextTextMapPropagator()

LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}
_NAMES = {logging.DEBUG: "DEBUG", logging.INFO: "INFO", logging.WARNING: "WARN", logging.ERROR: "ERROR"}


@dataclass(frozen=True)
class Member:
    """Whose logs, metrics and traces these are."""

    id: str  # component ID, e.g. erp/sales
    version: str


# The upper bounds of every duration histogram, the same in every language (P10.6).
DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line = {"time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
                + f".{int(record.msecs):03d}Z", "level": _NAMES.get(record.levelno, record.levelname),
                "msg": record.getMessage()}
        line.update(getattr(record, "attrs", {}))
        return json.dumps(line, ensure_ascii=False, separators=(",", ":"), default=str)


class Logger:
    """A structured JSON logger: every line carries the component ID and version plus the attributes given with
    with_() and at the call."""

    def __init__(self, base: logging.Logger, attrs: dict[str, Any]):
        self._base, self._attrs = base, attrs

    def with_(self, **attrs: Any) -> Logger:
        return Logger(self._base, {**self._attrs, **attrs})

    def enabled(self, level: int) -> bool:
        return self._base.isEnabledFor(level)

    def log(self, level: int, msg: str, **attrs: Any) -> None:
        if self._base.isEnabledFor(level):
            self._base.log(level, msg, extra={"attrs": {**self._attrs, **attrs}})

    def debug(self, msg: str, **attrs: Any) -> None:
        self.log(logging.DEBUG, msg, **attrs)

    def info(self, msg: str, **attrs: Any) -> None:
        self.log(logging.INFO, msg, **attrs)

    def warn(self, msg: str, **attrs: Any) -> None:
        self.log(logging.WARNING, msg, **attrs)

    def error(self, msg: str, **attrs: Any) -> None:
        self.log(logging.ERROR, msg, **attrs)


def new_logger(m: Member, level: str = "info", out: IO[str] | None = None) -> Logger:
    """The member's JSON logger. The underlying logging.Logger is constructed directly, so it is not registered in
    the logging module's global manager and never propagates to the root logger."""
    base = logging.Logger(m.id, LEVELS.get(level, logging.INFO))
    base.propagate = False
    handler = logging.StreamHandler(out or sys.stdout)
    handler.setFormatter(_JSONFormatter())
    base.addHandler(handler)
    return Logger(base, {"component": m.id, "version": m.version})


def new_registry() -> CollectorRegistry:
    """The member's own metrics registry; the launcher or the shell serves it."""
    return CollectorRegistry(auto_describe=True)


def new_tracer_provider(m: Member, otlp_url: str = "") -> TracerProvider:
    """The member's tracer provider, service name = component ID. Without a URL nothing is exported (the basic tier);
    spans still carry trace IDs for the logs."""
    provider = TracerProvider(resource=Resource.create({"service.name": m.id, "service.version": m.version}))
    if otlp_url:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_url)))
    return provider
