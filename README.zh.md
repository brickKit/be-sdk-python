[English](README.md) · [中文](README.zh.md)

# be-sdk-python

BrickEnterprise 组件协议 **be-protocol 1.0**（钉在 `v1.0.0-rc.1`）的官方 Python 实现。用它写的组件就是一个完整的 brickKit 组件：一个镜像、一个入口，协议规定的 HTTP、gRPC、数据库、事件和可观测行为都在，单跑和进 Python 外壳一样。包名 `besdk`，版本 **0.6.0**（在 `stage-b` 分支上进行中）；`/_be/info` 报 `sdk: be-sdk-python`、`protocol: "1.0"`。

以协议正文为准：[be-protocol](https://github.com/brickKit/be-protocol) 的 `spec/`。本文件只讲怎么用这个 SDK，以及目前实现了哪些条款。

## Writing a component

```python
# <pkg>/__init__.py
from pathlib import Path
import besdk
from . import authzgen  # 生成的权限键

HERE = Path(__file__).parent

async def create(rt: besdk.Runtime) -> besdk.Module:   # 只做声明，不起循环、不建连接
    store = rt.store()

    def routes(r: besdk.Router):                    # 前缀 /{domain}/{name}，每条路由声明一个守卫
        @r.get("/templates/{tid}", guard=authzgen.PRINT_VIEW, timeout=10)
        async def get_template(tid: str):
            return await store.tx(lambda tx: tx.fetchrow("SELECT id, name FROM template WHERE id = $1", tid))

    return besdk.Module(http=routes, events=besdk.Events(publishes=["infra.print.rendered.v1"]))

spec = besdk.Spec(id="infra/print", migrations=HERE / "migrations", contracts=HERE / "contracts", create=create)

# <pkg>/__main__.py
besdk.main(spec)
```

- `component.yaml` 和 `contracts/` 放在同一层（或者传 `Spec(manifest=…)`）；SDK 启动时读它的 `configSchema`。
- `migration.command` 写 `[python, -m, <pkg>, migrate, up]`；服务命令是 `[python, -m, <pkg>]`。
- 配置只经 `rt.config` 读（`require`、`string`、`int`、`bool`、`duration`、`durations`、`json`、`secret`）；`configSchema` 没声明的键一律拒读。
- 事务是一个函数：`await rt.store().tx(fn)`；遇到 40001 / 40P01 时 `fn(tx)` 会被重跑，所以里面只碰 `tx`。
- 错误：`raise besdk.Error(besdk.Code.FAILED_PRECONDITION, "TEMPLATE_ARCHIVED", {"id": tid})`；每个 reason 都要在 `contracts/errors.yaml` 里。

## Entry point

| 命令 | 做什么 | 退出码 |
|---|---|---|
| （无参数） | 服务：先开端口，依赖在后台连接，`SIGTERM` 后在 `SHUTDOWN_GRACE` 内处理完 | 0；致命错误 1 |
| `migrate up` | 组件迁移，然后平台迁移，以 `PG_OWNER_USER` 登录 | 0；失败 1 |
| `migrate down <n>` | 回滚最近 `n` 个组件迁移 | 0 / 1 |
| `migrate status` | 一行 JSON：已应用、待应用、平台版本 | 0 / 1 |
| `job run <name>` | 把声明过的一个任务跑一次（P14.8）：**本版还没有**，任何名字都按不存在处理 | 64 |
| 其它参数 | 用法错误，在读配置之前 | 64 |
| 配置错误 | 每个键一行 JSON 日志 | 78 |

## What is implemented

| 方面 | 条款 | 模块 |
|---|---|---|
| 进程、入口、就绪、停机 | P1.1–P1.8、P1.10、P1.13 | `besdk.main`、`besdk.serve` |
| 配置、密钥文件重读 | P2.1–P2.3、P2.5–P2.7、P2.9、P2.10、P2.12 | `besdk.config`、`besdk.config_values` |
| HTTP 面 | P3.1–P3.6、P3.10、P3.12、P3.13 | `besdk.http` |
| 错误、problem+json、gRPC 详情 | P4.1–P4.3、P4.6–P4.8 | `besdk.errors`、`besdk.rpc.status` |
| 令牌校验 | P5.1–P5.6、P5.8、P5.9 | `besdk.auth.jwt` |
| bundle 与路由判定（键、档位） | P6.1、P6.2、P1.5 | `besdk.auth` |
| 系统面（gRPC） | P7.2–P7.10、P7.12 | `besdk.rpc` |
| 出站 HTTP、事务内不许走网络 | P8.1–P8.4 | `besdk.outbound` |
| 截止时间、重试、舱壁 | P9 | （以上各处） |
| 数据库 Store | P10.1–P10.8、P10.12 | `besdk.store` |
| 迁移与平台迁移 | P11.1–P11.3、P11.5（标识）、P16.6（outbox 窗口） | `besdk.migrate` |
| 事件：outbox、泵、消费者、死信 | P12.1–P12.10、P12.13、P12.14 | `besdk.events` |
| 每成员的日志、指标、trace | P18.1–P18.4 | `besdk.logs`、`besdk.metrics`、`besdk.telemetry` |
| 自描述 | P20.3、P20.4 | `besdk.http.app` |

还没有（后续任务）：命令幂等（P13）；后台任务、队列和对账器（P14，以及 `job run`）；Access 的数据范围、资源契约和投影（P6.3–P6.15）；生命周期引擎和业务表分区窗口（P16）；日历、金额、单据编号、搜索、对象存储、缓存、快照（P11.6–P11.10、P15、P17）；测试包 `besdk.testing`；外壳启动器（P19）；PostgreSQL 总线适配器（P12.12）；`examples/widget` 夹具。

## Internal guarantees (INTERNAL rows)

- 每次数据库访问都在事务里，开头用 `set_config(…, true)` 设 role、`search_path`、`application_name` 和三个超时（效果与 `SET LOCAL` 相同）；池化连接上从不发会话级 `SET`；每条语句以 `/* be:<PG_SCHEMA> */` 开头，asyncpg 的语句缓存不会跨成员串用（复现 r1-04）。
- 事务里再开事务报 `NESTED_TX`；事务里调 gRPC、用户面 HTTP、第三方 HTTP 报 `NETWORK_IN_TX`（对外都是 `INTERNAL`）。
- 原始 token 只放在请求上下文里，只有 `rt.user_http` 会转发它。
- 每个成员有自己的 logger（从不用根 logger）、指标 registry、tracer provider 和 meter provider；导出器共用、最后才关（复现 r1-01）。
- durable 只在不存在时创建：先 `consumer_info`，查不到才 `add_consumer`，从不更新（复现 r1-07）。

## Stack (exact pins)

CPython 3.14 · FastAPI 0.118.0 跑在 uvicorn 0.38.0（httptools）上 · grpcio 1.76.0 · asyncpg 0.31.0 · yoyo-migrations 9.0.0 + psycopg[binary] 3.3.6 · nats-py 2.16.0 · PyJWT 2.13.0 · httpx 0.28.1 · OpenTelemetry 1.38.0 · prometheus-client 0.23.1 · jsonschema 4.26.0 · tzdata 2026.5。包里还附带 `be/v1/limits_pb2.py`（顶层包 `be`），由 be-protocol 的 `proto/be/v1/limits.proto` 生成。

## Development

```sh
make venv            # 建 .venv：Python 3.14 + 钉死的版本
make test            # 单元测试和协议向量（不需要容器）
make itest           # 集成测试：一次性 PostgreSQL 16 / 14 和 NATS 2.12 容器（前缀 sdkb-py-）
make sync-protocol   # 从 be-protocol 钉住的 tag 复制 ddl、schemas、vectors（BE_PROTOCOL_REPO=../be-protocol）
make gen-limits      # 重新生成 be/v1/limits_pb2.py
```

协议数据是复制进来的，运行期不去拉：`besdk/_protocol/`（参考 DDL、`errors-be.yaml`、`config-keys.yaml`，随 wheel 发布）和 `tests/protocol/`（向量，按 `SHA256SUMS` 校验，外加 contract-infra-authz 的决策向量）。两者都入库，只经 `make sync-protocol` 改动。
