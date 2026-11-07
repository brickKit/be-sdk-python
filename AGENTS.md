# be-sdk-python

组件协议（`be-protocol`）的官方 Python 实现。组件怎样用 SDK 写在 `README.md`；本文件写给要修改 SDK、或要查某个机制在 SDK 里怎样实现的人：包地图、从哪开始、怎样测试、哪些不能弄坏。规则正文是 `be-protocol` 的 `spec/`（`P<章>.<序号>` 编号）；能力的实现状态在 `features.json`（首批只实现代表组件用到的子集，其余如实标“未实现”，写明理由）。Go 的实现是 `be-sdk-go`，两边按同一份条款与向量实现，不照抄对方的代码；行为不同时，不符合向量的一方是缺陷。

## 代码地图

| 路径 | 管什么 | 规则 |
| --- | --- | --- |
| `component.py` | 组件交给启动器或外壳的定义：身份、版本、配置键、迁移、事件流、授权定义、模块的入口 | 第 01 章 |
| `launcher.py` | 单独运行一个组件：`serve` 与 `migrate` 入口、配置、就绪 | 第 01、02 章 |
| `shell.py` | 外壳启动器：一个进程承载装进来的成员；SDK 版本不低于成员声明的最低版本（P1.21）；进程级的键各成员一致（P2.11） | 协议 17 的外壳义务 |
| `host.py` | 启动器与外壳共用的组装：建一个成员（运行时、用户面、系统面、转发与订阅、审计、授权视图）、启动与停止、就绪与自述 | 第 01 章 |
| `runtime.py` | 模块对外的唯一入口：配置、依赖地址、数据库、调用、事件、审计、日志与指标 | 第 01 章 |
| `config.py` | 读取声明的配置键：严格类型，空即未设 | 第 02 章 |
| `errs.py`、`errwire.py` | 唯一的错误对象与原因目录；系统面上的编码 | 第 03 章 |
| `calls/` | 出站调用：并发上限、熔断、重试预算、逐跳缩短的时限、幂等键与转发用户凭据 | 第 04 章 |
| `reqctx.py` | 出站调用要转发的请求级值 | 第 04 章 |
| `db/` | 进入数据库的唯一方式：连接池与事务入口（`pool.py`：角色、`search_path`、超时都是事务级；数据库错误到原因的映射 `map_error`）、拒绝把状态留在连接上的语句（`session.py`）、迁移与迁移检查（`migrate.py`、`lint.py`）、单号（`docno.py`）、统计（`statistics.py`） | 第 05 章 |
| `events/` | 发件表与转发（`outbox.py`）、订阅与收件表（`consumer.py`）、信封与流（`core.py`）、运输的选择（`bus.py`）与两个适配器（`jetstream.py`、`pgqueue.py`）、大载荷的凭证检查（`objectstore.py`，SigV4 在 `sigv4.py`） | 第 06 章 |
| `identity/`、`guard.py`、`authz/`、`audit.py`、`records.py`、`canon.py` | 身份核验与服务身份、用户面准入（含令牌陈旧）、授权（定义、视图、求值、列表的计划）、审计、安全记录的封装与规范编码 | 第 08 章 |
| `lists.py` | 列表的游标与筛选 | 第 09 章 |
| `obs.py` | 每个成员自己的日志、指标与追踪 | 第 10 章 |
| `http.py`、`system.py` | 用户面（FastAPI）与系统面（`grpc.aio`）的服务器与中间件链 | 第 04、08 章 |
| `clock.py` | 系统挂起时也计时的时钟 | P1.11 |
| `mergesafety.py` | 找出合进外壳会坏的代码（进程级的全局状态、事件循环上的阻塞） | 协议 17 |
| `manifest.py` | 组件自己的测试拿来核对文档的：`configSchema` 等 | 协议 19 |
| `testing.py` | 测试用的安全部署：令牌、信任根、授权与审计的替身（只给测试） | — |
| `protocol.py`、`_protocol/` | 协议副本（迁移、向量、schema、能力表），由 `scripts/protocol.py` 从 `be-protocol` 的标签整份复制，不手改 | 协议 18 |
| `features.json` | 每项能力的实现状态、证据与“未实现”的理由 | 协议 18 |

| 想查或想改 | 从这里开始 | 然后 |
| --- | --- | --- |
| 一个机制怎样实现 | 上表找到模块，读模块开头的说明 | `tests/` 里同名的测试；`*_integration.py` 是可运行的用法 |
| 换协议版本 | `python3 -I scripts/protocol.py fetch <标签>` | `make test` 找出要跟着改的；`features.json` 的 `protocol`；`tests/interop/` 的 Go 小程序钉同一个协议的 be-sdk-go |
| 新加一条配置键或改启动行为 | `host.py` 的 `sdk_keys`、`keys` | `launcher.py`、`shell.py` |
| 新加一个错误原因 | `errs.py` 的目录 | 协议的 `errors` 向量 |
| 一个能力的实现状态 | `features.json` | `tests/test_features.py` |

## 构建与测试

- `make test`（`uv run pytest -q`）：单元测试与协议向量。
- `make test-integration`（`uv run pytest -q -m integration`）：要 `BE_SDK_TEST_PG_ADMIN`、`BE_SDK_TEST_NATS_URL` 与 `BE_SDK_TEST_S3_*`（装配项目的 `itenv.sh` 设置）；缺环境就失败，不跳过。互通测试会构建 `tests/interop/` 下的 Go 小程序。
- `make protocol-check`：协议副本与它记下的标签逐字节相同。
- 运行 Python 时设 `PYTHONDONTWRITEBYTECODE=1`，不在仓库里留字节码。

## 设计取舍

- 规则在 `be-protocol`，SDK 只是一个实现：行为变了先改协议的条款与向量，再改 SDK。
- 每个机制的入口只有一个（事务只经 `db`，出站只经 `calls`，事件只经 `events`），组件不能绕过。
- 外壳里的成员共用一个事件循环：CPU 重活与阻塞调用不上事件循环（交给进程池或线程），否则同一外壳的成员一起被卡住。

## 易错点

| 不许 | 症状 | 原因 |
| --- | --- | --- |
| 手改 `_protocol/` 里的文件 | `make protocol-check` 失败；与 Go 的行为对不上 | 副本必须与标签逐字节相同 |
| 在 SDK 里用进程级的全局状态（默认指标注册表、全局追踪、日志的全局配置） | 合进外壳后成员互相覆盖 | 一个进程承载多个成员；`mergesafety` 会找出来 |
| 改了行为不改 `features.json` | 能力的状态与实际不符 | 状态是两种 SDK 对齐的依据 |
| 协议副本换了版本，互通测试的 Go 小程序没跟着换 | `test_the_go_interop_helpers_pin_the_same_protocol` 失败 | 互通测的是同一份协议的两个实现 |

## 改代码前自查

1. 这个改动对应 `be-protocol` 的哪条条款？条款或向量要不要先改？
2. `features.json` 的状态要不要变？
3. 同一模块的集成测试跑过了吗？
4. 升了版本吗（`pyproject.toml` 的 `version`）？
