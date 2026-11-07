# be-sdk-python

组件协议（`be-protocol`）的官方 Python 实现。组件怎样用 SDK 写在 `README.md`；本文件写给要修改 SDK、或要查某个机制在 SDK 里怎样实现的人：包地图、从哪开始、怎样测试、哪些不能弄坏。规则正文是 `be-protocol` 的 `spec/`（`P<章>.<序号>` 编号）；能力的实现状态在 `features.json`（首批只实现代表组件用到的子集，其余如实标“未实现”，写明理由）。Go 的实现是 `be-sdk-go`，两边按同一份条款与向量实现，不照抄对方的代码；行为不同时，不符合向量的一方是缺陷。

## 代码地图

| 路径 | 管什么 | 规则 |
| --- | --- | --- |
| `component.py` | 组件交给启动器或外壳的定义：身份、版本、配置键、迁移、事件流、授权定义、模块的入口 | 第 01 章 |
| `launcher.py` | 单独运行一个组件：`serve` 与 `migrate` 入口、配置、就绪 | 第 01、02 章 |
| `shell.py` | 外壳启动器：一个进程承载装进来的成员；SDK 版本不低于成员声明的最低版本（P1.21）；进程级的键各成员一致（P2.11） | 协议 17 的外壳义务 |
| `host.py` | 启动器与外壳共用的组装：建一个成员（运行时、用户面、系统面、转发与订阅、补发、作业、审计、授权视图）、启动与停止、就绪与自述、恢复阶段（P1.22–P1.24） | 第 01 章 |
| `sdkjobs.py` | SDK 自带的作业与它们在恢复阶段怎样（没写规则的作业宿主拒绝登记；`tests/test_sdkjobs.py` 核对宿主登记了每个造作业的函数） | P1.23 |
| `runtime.py` | 模块对外的唯一入口：配置、依赖地址、数据库、调用、事件、审计、日志与指标 | 第 01 章 |
| `config.py` | 读取声明的配置键：严格类型，空即未设 | 第 02 章 |
| `errs.py`、`errwire.py` | 唯一的错误对象与原因目录；系统面上的编码 | 第 03 章 |
| `calls/` | 出站调用：并发上限、熔断、重试预算、逐跳缩短的时限、幂等键与转发用户凭据 | 第 04 章 |
| `reqctx.py` | 出站调用要转发的请求级值 | 第 04 章 |
| `db/` | 进入数据库的唯一方式：连接池与事务入口（`pool.py`：角色、`search_path`、超时都是事务级；数据库错误到原因的映射 `map_error`）、拒绝把状态留在连接上的语句（`session.py`）、迁移与迁移检查（`migrate.py`、`lint.py`）、单号（`docno.py`）、统计（`statistics.py`）、按月分区的维护（`partitions.py`） | 第 05 章 |
| `events/` | 发件表、转发与清理（`outbox.py`）、订阅与收件表（`consumer.py`，收件表记来源位置）、信封与流（`core.py`）、运输的选择（`bus.py`）与两个适配器（`jetstream.py`、`pgqueue.py`）、大载荷的凭证检查（`objectstore.py`，SigV4 在 `sigv4.py`）、订阅的进度（`progress.py`）、生产者的补发 `ResendService`（`resend.py`）、消费者库回退后的补发（`redelivery.py`）；水位来源的补取 `WatermarkService`（`watermark.py`）与记录读取 `RecordService`（`records.py`），流在 `Stream.watermark`、`Stream.records` 里声明（`sources.py`），只有来源一侧（A164） | 第 06、09 章 |
| `work/` | 后台工作项：作业的声明与失败的分类（`job.py`）、一次认领与带核对的提交（`run.py`）、成员的轮询、工作者、调度与定时（`runner.py`）、cron 的时间槽（`cron.py`）、运维操作与清理（`operator.py`） | 第 07 章 |
| `evidence/` | 证据：记录者一侧（`writer.py`：意图、结论、一般记录、变更历史与提交序号、双通道失效）、转运到证据存储（`transfer.py`，SDK 自带作业 `be.evidence.transfer`，恢复阶段里是宿主的循环）、属主的重放（`replay.py`，`be.evidence.v1.ReplayService`） | P12.9–P12.23 |
| `runprofile/` | 运行档案与告警渠道（`store.py`：开了对象锁的桶里签名的记录、恢复代次、告警）；信任材料历史（`trust.py`：沿双签的轮换受信的 CA、按接收时刻判吊销） | P12.1–P12.8、P12.24–P12.27 |
| `extcmd/` | 对外命令：状态表（`state.py`，纯函数，对向量）、命令的写入与作业 `be.extcmd.process`（`commands.py`）、整套恢复第 7 步的匹配（`reconcile.py`，纯函数）与 `be.external.v1.ReconcileService`（`server.py`） | 第 13 章 |
| `recovery/` | 整套恢复的核对 `be.recovery.v1.CheckService`：组件登记的检查、对象是否还在、单号的处理（`checks.py`）与 SDK 自带的三项检查；服务（`server.py`）。每个有数据库的成员都要声明 `Definition.recovery` | 第 14 章 |
| `idem.py` | 命令的幂等键：认领、命令与结果同一事务，清理作业 | P4.21–P4.25 |
| `identity/`、`guard.py`、`authz/`、`audit.py`、`records.py`、`canon.py` | 身份核验与服务身份、用户面准入（含令牌陈旧）、授权（定义、视图、求值、列表的计划）、审计、安全记录的封装与规范编码 | 第 08 章 |
| `lists.py` | 列表的游标与筛选 | 第 09 章 |
| `obs.py` | 每个成员自己的日志、指标与追踪 | 第 10 章 |
| `http.py`、`system.py` | 用户面（FastAPI）与系统面（`grpc.aio`）的服务器与中间件链 | 第 04、08 章 |
| `clock.py` | 系统挂起时也计时的时钟 | P1.11 |
| `crash.py` | 崩溃点：状态迁移处的 `crash.point(名字)`，平时什么也不做，长时间推演挂上钩子在那里结束进程 | — |
| `mergesafety.py` | 找出合进外壳会坏的代码（进程级的全局状态、事件循环上的阻塞） | 协议 17 |
| `manifest.py` | 组件自己的测试拿来核对文档的：`configSchema` 等 | 协议 19 |
| `testing.py` | 测试用的安全部署：令牌、信任根、授权与审计的替身（只给测试） | — |
| `protocol.py`、`_protocol/` | 协议副本（迁移、向量、schema、proto、能力表），由 `scripts/protocol.py` 从 `be-protocol` 的标签整份复制，不手改 | 协议 18 |
| `_gen/` | 协议的 gRPC 生成代码（`be.source.v1`、`be.evidence.v1`、`be.external.v1`、`be.recovery.v1`、`be.session.v1`），`scripts/gen.py` 从副本的 `proto/` 生成并提交，导入改写到 `besdk._gen.be.…`；不手改 | 协议 18 |
| `features.json` | 每项能力的实现状态、证据与“未实现”的理由 | 协议 18 |

| 想查或想改 | 从这里开始 | 然后 |
| --- | --- | --- |
| 一个机制怎样实现 | 上表找到模块，读模块开头的说明 | `tests/` 里同名的测试；`*_integration.py` 是可运行的用法 |
| 换协议版本 | `python3 -I scripts/protocol.py fetch <标签>`，再 `make gen` | `make test` 找出要跟着改的；`features.json` 的 `protocol`；`tests/interop/` 的 Go 小程序钉同一个协议的 be-sdk-go |
| 加 SDK 自带的作业 | `sdkjobs.py` 写它在恢复阶段怎样 | 在 `host.py` 登记；`tests/test_sdkjobs.py` |
| 新加一条配置键或改启动行为 | `host.py` 的 `sdk_keys`、`keys` | `launcher.py`、`shell.py` |
| 新加一个错误原因 | `errs.py` 的目录 | 协议的 `errors` 向量 |
| 一个能力的实现状态 | `features.json` | `tests/test_features.py` |

## 构建与测试

- `make test`（`uv run pytest -q`）：单元测试与协议向量。
- `make test-integration`（`uv run pytest -q -m integration`）：要 `BE_SDK_TEST_PG_ADMIN`、`BE_SDK_TEST_NATS_URL` 与 `BE_SDK_TEST_S3_*`（装配项目的 `itenv.sh` 设置）；缺环境就失败，不跳过。互通测试会构建 `tests/interop/` 下的 Go 小程序。
- `make simulation`（`tests/sim`）：长时间推演：两个成员的几个副本（其中一个是承载两者的外壳）轮流在每个崩溃点结束自己、不时被杀掉，之后核对终态（每条事件进了收件表或死信、作业都有结局）；要 `make test-integration` 的环境，`SIM_MINUTES` 是注入崩溃的时长（默认 30），按长任务放后台。
- `make protocol-check`：协议副本与它记下的标签逐字节相同。
- 运行 Python 时设 `PYTHONDONTWRITEBYTECODE=1`，不在仓库里留字节码。

## 设计取舍

- 规则在 `be-protocol`，SDK 只是一个实现：行为变了先改协议的条款与向量，再改 SDK。
- 每个机制的入口只有一个（事务只经 `db`，出站只经 `calls`，事件只经 `events`），组件不能绕过。
- 外壳里的成员共用一个事件循环：CPU 重活与阻塞调用不上事件循环（交给进程池或线程），否则同一外壳的成员一起被卡住。

## 易错点

| 不许 | 症状 | 原因 |
| --- | --- | --- |
| 手改 `_protocol/` 或 `_gen/` 里的文件 | `make protocol-check` 或 `make gen-check`（单元测试里也查）失败；与 Go 的行为对不上 | 副本必须与标签逐字节相同，生成代码必须是副本生成的 |
| 有流的组件不声明 `Definition.resend` | 成员拒绝构建 | 有流就要在系统面答补发（P6.38），与 be-sdk-go 相同 |
| 在 SDK 里用进程级的全局状态（默认指标注册表、全局追踪、日志的全局配置） | 合进外壳后成员互相覆盖 | 一个进程承载多个成员；`mergesafety` 会找出来 |
| 改了行为不改 `features.json` | 能力的状态与实际不符 | 状态是两种 SDK 对齐的依据 |
| 协议副本换了版本，互通测试的 Go 小程序没跟着换 | `test_the_go_interop_helpers_pin_the_same_protocol` 失败 | 互通测的是同一份协议的两个实现 |

## 改代码前自查

1. 这个改动对应 `be-protocol` 的哪条条款？条款或向量要不要先改？
2. `features.json` 的状态要不要变？
3. 同一模块的集成测试跑过了吗？
4. 升了版本吗（`pyproject.toml` 的 `version`）？
