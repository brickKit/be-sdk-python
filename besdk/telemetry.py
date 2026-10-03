"""Traces and the OpenTelemetry meter (be-protocol P18.1, P19.3, P19.4; repro r1-01).

Process-wide (``Platform``): the exporter and the propagator. Per member (``MemberTelemetry``): a tracer
provider with its own batch processor and resource, and a meter provider read into the member's
Prometheus registry. The shared exporter sits behind a wrapper whose ``shutdown`` does nothing, so a
member that stops flushes only its own queue; only the platform closes the real exporter, last.
Every SDK instrumentation is handed the member's tracer and the platform's propagator explicitly.
"""

from __future__ import annotations

import os
import socket
from typing import Sequence

from opentelemetry import propagate, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Meter
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from prometheus_client import REGISTRY, CollectorRegistry

_global_set = False


class _SharedExporter(SpanExporter):
    """The members' view of the shared exporter: export passes through, shutdown is the platform's job."""

    def __init__(self, inner: SpanExporter):
        self.inner = inner

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self.inner.export(spans)

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def otlp_exporter(base_url: str) -> SpanExporter | None:
    """OTLP/HTTP to ``{OTEL_BASE_URL}/v1/traces``; empty base = no export (P18.1)."""
    if not base_url:
        return None
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    return OTLPSpanExporter(endpoint=base_url.rstrip("/") + "/v1/traces", timeout=5)


class Platform:
    """The process-wide half: one exporter, one propagator, the global fallback provider."""

    def __init__(self, process_id: str, *, exporter: SpanExporter | None):
        global _global_set
        self.exporter = exporter
        self.propagator = CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
        self.fallback = self._provider(process_id, "")
        if not _global_set:  # set once per process; a missed instrumentation shows up under this name
            propagate.set_global_textmap(self.propagator)
            trace.set_tracer_provider(self.fallback)
            _global_set = True

    def _provider(self, service: str, version: str) -> TracerProvider:
        attrs = {"service.name": service, "service.instance.id": os.environ.get("HOSTNAME") or socket.gethostname()}
        if version:
            attrs["service.version"] = version
        tp = TracerProvider(resource=Resource.create(attrs))
        if self.exporter is not None:
            tp.add_span_processor(BatchSpanProcessor(_SharedExporter(self.exporter)))
        return tp

    def member(self, component_id: str, version: str, registry: CollectorRegistry) -> "MemberTelemetry":
        return MemberTelemetry(self, component_id, version, registry)

    def shutdown(self) -> None:
        """After every member stopped: flush the fallback and close the real exporter."""
        self.fallback.shutdown()
        if self.exporter is not None:
            self.exporter.shutdown()


class _MemberReader(PrometheusMetricReader):
    """The OTel → Prometheus bridge, registered in the member's registry instead of the global one."""

    def __init__(self, registry: CollectorRegistry):
        super().__init__(disable_target_info=True)
        REGISTRY.unregister(self._collector)
        self._registry = registry

    def attach(self) -> None:
        """Register in the member's registry once the meter provider owns the reader."""
        self._registry.register(self._collector)

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        try:
            self._registry.unregister(self._collector)
        except KeyError:
            pass


class MemberTelemetry:
    """A member's own tracer provider and meter provider (P19.4)."""

    def __init__(self, platform: Platform, component_id: str, version: str, registry: CollectorRegistry):
        self.platform = platform
        self.tracer_provider = platform._provider(component_id, version)
        self.tracer = self.tracer_provider.get_tracer("besdk")
        reader = _MemberReader(registry)
        self.meter_provider = MeterProvider(metric_readers=[reader],
                                            resource=Resource.create({"service.name": component_id}))
        reader.attach()
        self.meter: Meter = self.meter_provider.get_meter("besdk")

    @property
    def propagator(self) -> CompositePropagator:
        return self.platform.propagator

    def shutdown(self) -> None:
        """Flush this member's own span queue; the shared exporter stays open."""
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()
