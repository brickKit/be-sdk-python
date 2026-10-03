"""Metrics (be-protocol P18.3): one Prometheus registry per member, every series labelled
``component=<member ID>``, and the protocol's ``be_*`` metrics."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, disable_created_metrics
from prometheus_client.metrics_core import Metric

disable_created_metrics()  # no *_created series: they double the series count and nobody reads them

_RPC_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)


class ComponentRegistry(CollectorRegistry):
    """A registry that adds ``component=<id>`` to every sample it exposes, the component's own too."""

    def __init__(self, component_id: str):
        super().__init__(auto_describe=True)
        self.component_id = component_id

    def collect(self):
        for m in super().collect():
            out = Metric(m.name, m.documentation, m.type, m.unit)
            for s in m.samples:
                out.add_sample(s.name, {"component": self.component_id, **s.labels}, s.value, s.timestamp,
                               s.exemplar)
            yield out


class BeMetrics:
    """The protocol metrics a runtime maintains (P18.3); created once per member registry."""

    def __init__(self, reg: CollectorRegistry):
        c, g, h = (lambda n, d, ls=(): Counter(n, d, ls, registry=reg),
                   lambda n, d, ls=(): Gauge(n, d, ls, registry=reg),
                   lambda n, d, ls=(): Histogram(n, d, ls, registry=reg, buckets=_RPC_BUCKETS))
        self.http_server_requests = c("be_http_server_requests", "HTTP requests served",
                                      ("method", "route", "status_code"))
        self.http_server_duration = h("be_http_server_duration_seconds", "HTTP server latency", ("method", "route"))
        self.http_client_requests = c("be_http_client_requests", "outbound HTTP requests",
                                      ("target", "method", "status_code"))
        self.http_client_duration = h("be_http_client_duration_seconds", "outbound HTTP latency", ("target", "method"))
        self.grpc_server_handled = c("be_grpc_server_handled", "gRPC calls served", ("service", "method", "code"))
        self.grpc_server_duration = h("be_grpc_server_duration_seconds", "gRPC server latency", ("service", "method"))
        self.grpc_client_handled = c("be_grpc_client_handled", "outbound gRPC calls", ("target", "method", "code"))
        self.grpc_client_duration = h("be_grpc_client_duration_seconds", "outbound gRPC latency", ("target", "method"))
        self.outbound_inflight = g("be_outbound_inflight", "outbound calls in flight", ("target",))
        self.db_pool_in_use = g("be_db_pool_in_use", "database connections in use by this member")
        self.db_pool_wait = h("be_db_pool_wait_seconds", "time waiting for a database connection")
        self.tx_retries = c("be_tx_retries", "transaction bodies re-run", ("reason",))
        self.db_identity_ok = g("be_db_identity_ok", "1 when the database identity probe passed")
        self.secret_reload_failures = c("be_secret_reload_failures", "secret files that could not be re-read",
                                        ("key",))
        self.outbox_pending = g("be_outbox_pending", "outbox rows not yet published")
        self.outbox_oldest_age = g("be_outbox_oldest_age_seconds", "age of the oldest unpublished outbox row")
        self.events_published = c("be_events_published", "events confirmed by the bus", ("subject",))
        self.consumer_handled = c("be_consumer_handled", "deliveries handled", ("subject", "result"))
        self.consumer_lag = g("be_consumer_lag_seconds", "age of the last handled event", ("subject",))
        self.dlq_messages = c("be_dlq_messages", "messages dead-lettered", ("subject",))
        self.authz_bundle_age = g("be_authz_bundle_age_seconds", "age of the bundle in use")
        self.authz_denied = c("be_authz_denied", "requests refused by the route decision", ("reason",))
