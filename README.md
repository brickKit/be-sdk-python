# be-sdk-python

组件协议（[be-protocol](https://github.com/brickKit/be-protocol)）的官方 Python 实现。旧版本（0.x，最新代码在 `stage-b` 分支）在分支 `legacy/v0`，标签保留；本主分支按终局设计从零开始（D59），版本从 `3.0.0` 起（A110；Python 包写作 PEP 440 的 `3.0.0a6`，即 `3.0.0-alpha.6`）。设计依据在装配项目 [be-assembly-standard](https://github.com/brickKit/be-assembly-standard) 的协议 17、18 与组件实现约定；规则正文在 `be-protocol`。

首批只实现代表部署实验用到的子集（D75），其余能力在功能矩阵中如实标为未实现；子集的范围见装配项目的实验计划与第 3 步计划。

## 在组件里使用

不发到包索引；组件按标签从 Git 取，版本由组件自己的 `uv.lock` 锁定：

```toml
[project]
dependencies = ["be-sdk"]

[tool.uv.sources]
be-sdk = { git = "https://github.com/brickKit/be-sdk-python", tag = "v3.0.0a6" }
```

标签用 PEP 440 的写法（例如 `v3.0.0a6`），与分发版本一致；自描述里报告的 SDK 版本是 `be-sdk-python/3.0.0-alpha.6`。组件自己的测试可以用 `besdk.testing`：测试部署的签发方与公钥、信任根与组件证书、假的授权与审计组件、成员数据库的准备；只用于测试，组件代码不导入它。

## 功能矩阵

`features.json` 列出 `be-protocol` 能力清单中 SDK 负责的每项能力的状态与证明它的测试（`tests/test_features.py` 核对它与协议副本一一对应、证据是真实存在的测试）。

## 依赖

Python 3.14；依赖由 `uv` 管理，`uv.lock` 锁定精确版本。技术栈按装配项目的“锁定的技术栈”：FastAPI 跑在 uvicorn 上（单进程、单事件循环）、`grpc.aio`、`asyncpg` 与手写 SQL、`nats-py`、PyJWT、`aioboto3`、标准 `logging`、每个成员一个 Prometheus 注册表、OpenTelemetry；迁移入口由 SDK 自带（A111），与 Go SDK 读写同一份迁移状态。

## 协议的机器可读部分

`besdk/_protocol/` 是 `be-protocol` 在一个精确标签上的 `vectors/`、`migrations/`、`migrations-bus/` 与 `capabilities.json`，来源的标签与提交记在 `PROTOCOL.json`。它随包分发（SDK 自己的迁移从这里执行），从不手改：换版本用 `python3 -I scripts/protocol.py fetch <标签>`，`make protocol-check` 重新取一次并逐字节比较。

## 测试

`make test`：单元与向量测试（`uv run pytest`）。需要真实 PostgreSQL、NATS 与对象存储的测试标为 `integration`，`make test-integration` 运行，环境与 Go SDK 相同（`BE_SDK_TEST_PG_ADMIN` 等）；缺环境时失败，不跳过。
