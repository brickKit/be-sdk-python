[English](README.md) · [中文](README.zh.md)

# be-sdk-python

The official Python implementation of the BrickEnterprise component protocol, **be-protocol 1.0** (pinned at `v1.0.0-rc.1`). A component written with it is a complete brickKit component: one image, one entry point, the protocol's HTTP, gRPC, database, event and observability behaviour, standalone or inside a Python shell. Package `besdk`, version **0.6.0** (in progress on branch `stage-b`); `/_be/info` reports `sdk: be-sdk-python`, `protocol: "1.0"`.

The protocol text is the source of truth: [be-protocol](https://github.com/brickKit/be-protocol) `spec/`. This README says how to use the SDK and which requirements it implements so far.

## Writing a component

```python
# <pkg>/__init__.py
from pathlib import Path
import besdk
from . import authzgen  # generated permission keys

HERE = Path(__file__).parent

async def create(rt: besdk.Runtime) -> besdk.Module:   # declarations only; no loops, no connections
    store = rt.store()

    def routes(r: besdk.Router):                    # prefix /{domain}/{name}, every route declares one guard
        @r.get("/templates/{tid}", guard=authzgen.PRINT_VIEW, timeout=10)
        async def get_template(tid: str):
            return await store.tx(lambda tx: tx.fetchrow("SELECT id, name FROM template WHERE id = $1", tid))

    return besdk.Module(http=routes, events=besdk.Events(publishes=["infra.print.rendered.v1"]))

spec = besdk.Spec(id="infra/print", migrations=HERE / "migrations", contracts=HERE / "contracts", create=create)

# <pkg>/__main__.py
besdk.main(spec)
```

- `component.yaml` sits beside `contracts/` (or pass `Spec(manifest=…)`); the SDK reads its `configSchema` at start.
- `migration.command` is `[python, -m, <pkg>, migrate, up]`; the serving command is `[python, -m, <pkg>]`.
- Configuration only through `rt.config` (`require`, `string`, `int`, `bool`, `duration`, `durations`, `json`, `secret`); a key not declared in `configSchema` is refused.
- A transaction is a function: `await rt.store().tx(fn)`; `fn(tx)` may be re-run on 40001 / 40P01, so it touches only `tx`.
- Errors: `raise besdk.Error(besdk.Code.FAILED_PRECONDITION, "TEMPLATE_ARCHIVED", {"id": tid})`; every reason is in `contracts/errors.yaml`.

## Entry point

| Command | Does | Exit |
|---|---|---|
| *(none)* | serve: ports first, dependencies in the background, `SIGTERM` drains within `SHUTDOWN_GRACE` | 0; 1 fatal |
| `migrate up` | component migrations, then the platform migration, as `PG_OWNER_USER` | 0; 1 failed |
| `migrate down <n>` | roll back the last `n` component migrations | 0 / 1 |
| `migrate status` | one JSON line: applied, pending, platform version | 0 / 1 |
| `job run <name>` | run one declared job once (P14.8): **not available yet**, every name is unknown | 64 |
| anything else | usage error, before the configuration is read | 64 |
| bad configuration | one JSON log line per key | 78 |

## What is implemented

| Area | Requirements | Module |
|---|---|---|
| Process, entry point, readiness, shutdown | P1.1–P1.8, P1.10, P1.13 | `besdk.main`, `besdk.serve` |
| Configuration, secret files re-read | P2.1–P2.3, P2.5–P2.7, P2.9, P2.10, P2.12 | `besdk.config`, `besdk.config_values` |
| HTTP surface | P3.1–P3.6, P3.10, P3.12, P3.13 | `besdk.http` |
| Errors, problem+json, gRPC details | P4.1–P4.3, P4.6–P4.8 | `besdk.errors`, `besdk.rpc.status` |
| Token verification | P5.1–P5.6, P5.8, P5.9 | `besdk.auth.jwt` |
| Bundle and the route decision (keys, levels) | P6.1, P6.2, P1.5 | `besdk.auth` |
| System plane (gRPC) | P7.2–P7.10, P7.12 | `besdk.rpc` |
| Outbound HTTP, no network in a transaction | P8.1–P8.4 | `besdk.outbound` |
| Deadlines, retries, bulkheads | P9 | (all of the above) |
| Database store | P10.1–P10.8, P10.12 | `besdk.store` |
| Migrations and the platform migration | P11.1–P11.3, P11.5 (ids), P16.6 (outbox window) | `besdk.migrate` |
| Events: outbox, pump, consumers, dead letters | P12.1–P12.10, P12.13, P12.14 | `besdk.events` |
| Logs, metrics, traces per member | P18.1–P18.4 | `besdk.logs`, `besdk.metrics`, `besdk.telemetry` |
| Self-description | P20.3, P20.4 | `besdk.http.app` |

Not yet (later tasks): command idempotency (P13), jobs, queues and reconcilers (P14, and `job run`), Access scopes, resource contract and projection (P6.3–P6.15), the lifecycle engine and business partition windows (P16), calendar, money, numbering, search, blob, caches, snapshots (P11.6–P11.10, P15, P17), the test package `besdk.testing`, the shell launcher (P19), the PostgreSQL bus adapter (P12.12), the `examples/widget` fixture.

## Internal guarantees (INTERNAL rows)

- Every database access runs in a transaction that sets role, `search_path`, `application_name` and three timeouts with `set_config(…, true)` (the same effect as `SET LOCAL`); no session-level `SET` on a pooled connection; every statement starts with `/* be:<PG_SCHEMA> */` so asyncpg's statement cache never crosses members (repro r1-04).
- A transaction inside a transaction raises `NESTED_TX`; gRPC, user-plane HTTP and third-party HTTP inside a transaction raise `NETWORK_IN_TX` (both answered as `INTERNAL`).
- The raw token lives only in the request's context and is forwarded only by `rt.user_http`.
- Each member has its own logger (never the root logger), metrics registry, tracer provider and meter provider; the exporter is shared and closed last (repro r1-01).
- Durables are created only when absent: `consumer_info` first, `add_consumer` only on not-found, never an update (repro r1-07).

## Stack (exact pins)

CPython 3.14 · FastAPI 0.118.0 on uvicorn 0.38.0 (httptools) · grpcio 1.76.0 · asyncpg 0.31.0 · yoyo-migrations 9.0.0 with psycopg[binary] 3.3.6 · nats-py 2.16.0 · PyJWT 2.13.0 · httpx 0.28.1 · OpenTelemetry 1.38.0 · prometheus-client 0.23.1 · jsonschema 4.26.0 · tzdata 2026.5. The package also ships `be/v1/limits_pb2.py` (top-level package `be`), generated from be-protocol's `proto/be/v1/limits.proto`.

## Development

```sh
make venv            # .venv with Python 3.14 and the pins
make test            # unit tests and the protocol vectors (no containers)
make itest           # integration tests against throwaway PostgreSQL 16 / 14 and NATS 2.12 (prefix sdkb-py-)
make sync-protocol   # copy ddl, schemas and vectors from be-protocol's pinned tag (BE_PROTOCOL_REPO=../be-protocol)
make gen-limits      # regenerate be/v1/limits_pb2.py
```

The protocol data is copied, not fetched at run time: `besdk/_protocol/` (reference DDL, `errors-be.yaml`, `config-keys.yaml`, shipped in the wheel) and `tests/protocol/` (the vectors, checked against `SHA256SUMS`, plus contract-infra-authz's decision vectors). Both are committed and change only through `make sync-protocol`.
