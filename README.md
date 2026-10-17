# be-sdk-python

组件协议（[be-protocol](https://github.com/brickKit/be-protocol)）的官方 Python 实现。旧版本（0.x，最新代码在 `stage-b` 分支）在分支 `legacy/v0`，标签保留；本主分支按终局设计从零开始（D59），版本从 `3.0.0` 起（A110；Python 包写作 `3.0.0a1`，即 `3.0.0-alpha.1`）。设计依据在装配项目 [be-assembly-standard](https://github.com/brickKit/be-assembly-standard) 的协议 17、18 与组件实现约定；规则正文在 `be-protocol`。

首批只实现代表部署实验用到的子集（D75），其余能力在功能矩阵中如实标为未实现；子集的范围见装配项目的实验计划与第 3 步计划。

## 依赖

Python 3.14；依赖由 `uv` 管理，`uv.lock` 锁定精确版本。技术栈按装配项目的“锁定的技术栈”：FastAPI 跑在 uvicorn 上（单进程、单事件循环）、`grpc.aio`、`asyncpg` 与手写 SQL、`nats-py`、PyJWT、`aioboto3`、标准 `logging`、每个成员一个 Prometheus 注册表、OpenTelemetry；迁移入口由 SDK 自带（A111），与 Go SDK 读写同一份迁移状态。

## 协议的机器可读部分

`besdk/_protocol/` 是 `be-protocol` 在一个精确标签上的 `vectors/`、`migrations/`、`migrations-bus/` 与 `capabilities.json`，来源的标签与提交记在 `PROTOCOL.json`。它随包分发（SDK 自己的迁移从这里执行），从不手改：换版本用 `python3 -I scripts/protocol.py fetch <标签>`，`make protocol-check` 重新取一次并逐字节比较。

## 测试

`make test`：单元与向量测试（`uv run pytest`）。需要真实 PostgreSQL、NATS 与对象存储的测试标为 `integration`，`make test-integration` 运行，环境与 Go SDK 相同（`BE_SDK_TEST_PG_ADMIN` 等）；缺环境时失败，不跳过。
