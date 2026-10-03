"""Per-member providers over one shared exporter (P18.1, P19.3, repro r1-01) and per-member metrics (P18.3)."""
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import Counter, generate_latest

from besdk import metrics, telemetry


def test_stopping_one_member_never_drops_another_members_spans():
    exp = InMemorySpanExporter()
    plat = telemetry.Platform("be/py-shell", exporter=exp)
    a = plat.member("conformance/widget-a", "1.0.0", metrics.ComponentRegistry("conformance/widget-a"))
    b = plat.member("conformance/widget-b", "1.0.0", metrics.ComponentRegistry("conformance/widget-b"))
    with a.tracer.start_as_current_span("a1"):
        pass
    a.shutdown()  # r1-01 S1: a naive shared exporter would be closed here
    with b.tracer.start_as_current_span("b1"):
        pass
    with b.tracer.start_as_current_span("b2"):
        pass
    b.shutdown()
    plat.shutdown()
    names = sorted((s.resource.attributes["service.name"], s.name) for s in exp.get_finished_spans())
    assert names == [("conformance/widget-a", "a1"), ("conformance/widget-b", "b1"), ("conformance/widget-b", "b2")]


def test_spans_have_valid_trace_ids_without_export():
    plat = telemetry.Platform("conformance/widget", exporter=None)
    m = plat.member("conformance/widget", "1.0.0", metrics.ComponentRegistry("conformance/widget"))
    with m.tracer.start_as_current_span("x") as s:
        assert s.get_span_context().is_valid


def test_propagator_is_trace_context_and_baggage():
    plat = telemetry.Platform("conformance/widget", exporter=None)
    m = plat.member("conformance/widget", "1.0.0", metrics.ComponentRegistry("conformance/widget"))
    carrier: dict[str, str] = {}
    with m.tracer.start_as_current_span("x"):
        plat.propagator.inject(carrier)
    assert carrier["traceparent"].startswith("00-")
    ctx = plat.propagator.extract({"traceparent": carrier["traceparent"]})
    with m.tracer.start_as_current_span("child", context=ctx) as child:
        assert f"{child.get_span_context().trace_id:032x}" == carrier["traceparent"].split("-")[1]


def test_every_series_carries_the_component_label():
    reg = metrics.ComponentRegistry("erp/sales")
    c = Counter("erp_sales_orders_total", "orders", registry=reg)
    c.inc()
    be = metrics.BeMetrics(reg)
    be.http_server_requests.labels(method="GET", route="/erp/sales/orders", status_code="200").inc()
    text = generate_latest(reg).decode()
    assert 'erp_sales_orders_total{component="erp/sales"} 1.0' in text
    assert ('be_http_server_requests_total{component="erp/sales",method="GET",route="/erp/sales/orders",'
            'status_code="200"} 1.0') in text


def test_otel_meter_lands_in_the_member_registry():
    plat = telemetry.Platform("erp/sales", exporter=None)
    reg = metrics.ComponentRegistry("erp/sales")
    m = plat.member("erp/sales", "3.0.0", reg)
    m.meter.create_counter("erp_sales_things").add(2)
    assert 'erp_sales_things_total{component="erp/sales"} 2.0' in generate_latest(reg).decode()


def test_member_resource_attributes():
    """P18.1 (rc.2): service.namespace = the domain, deployment.environment.name = DEPLOY_ENV."""
    exp = InMemorySpanExporter()
    plat = telemetry.Platform("conformance/widget", exporter=exp)
    m = plat.member("erp/sales", "3.0.0", metrics.ComponentRegistry("erp/sales"), environment="prod")
    with m.tracer.start_as_current_span("x"):
        pass
    m.shutdown()
    a = exp.get_finished_spans()[0].resource.attributes
    assert (a["service.name"], a["service.version"], a["service.namespace"], a["deployment.environment.name"]) == (
        "erp/sales", "3.0.0", "erp", "prod")
