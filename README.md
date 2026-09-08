# be-sdk-python

Python 横切基础库（总纲 §4 SOP-L 十四项能力）。**不是 brickKit 组件**，也**不是公共 model 包**——零业务逻辑、零组件 model、零组件间引用。它是 `be-acceptance` 铁律六 import 扫描的唯一白名单之一（另一个是 `be-sdk-go`、`be-sdk-ts`）。

⚠️ **与 `be-sdk-go` 的关系不是"照着抄一遍"，是"逐个能力对应着搬"**（总纲 §3.5.1、决策 100）：两份 SDK 的公开 API 要同名、同参数顺序、同语义。`besdk.Endpoint()` 对应 `besdk.endpoint()`，`besdk.WithTx()` 对应 `besdk.with_tx()`，`besdk.RunStandalone()` 对应 `besdk.run_standalone()`——每一处对应关系都是刻意的，不是巧合。

## 它替所有 Python 组件挡住的坑

| 能力 | 文件 | 挡住的坑 |
|---|---|---|
| 组件地址剥 scheme | `endpoint.py` | `grpc.aio.insecure_channel("http://host:9094")` 连不上，报错指向名称解析（导读第 1 条） |
| `SET LOCAL` 事务 | `tx.py` | 不带 `LOCAL` 的 `SET` 之后连接还回池，下一个借用者原样继承，悄悄读写别人的数据（导读第 2 条） |
| 单跑统一入口 | `standalone.py`、`module.py`、`runtime.py`、`fastapi_app.py` | 每个组件各发明一个入口，合并那天全部重写（导读第 18 条） |
| `Config` 的 camelCase 查询 | `runtime.py` | `be-sdk-go` v0.1.0–v0.1.8 真实存在过的 bug：查询用的 camelCase key 从来没转成平台真实注入的 SCREAMING_SNAKE_CASE，四个已发布组件因默认值恰好等于真实值而未暴露五个版本。**本仓库从第一个提交起就是对的**，不重犯 |

## 现状（阶段三 Task 1）

`Runtime` / `Module` / `run_standalone` / `new_fastapi_app` / `bootstrap` / `endpoint` / `with_tx` / `Config`（含 camelCase 转换）已经是**真实实现**——这是"结构三件套"，决定外壳能不能把模块挂进来，必须先钉死（总纲 SOP-L L-1）。

`otel.py` / `logging.py` / `metrics.py` / `query.py` / `archive.py` / `outbox.py` / `events.py` / `authz.py` / `scope.py` / `client.py` 现在只有签名 + 文档注释 + `raise NotImplementedError`。**调用它们会抛异常，这是预期行为**，不是 bug——随后用 TDD 逐个补上。

## 为什么是 FastAPI + `asyncpg`，不是别的（决策 108，物理原因不是偏好）

| 不选 | 为什么 |
|---|---|
| Flask / Django（WSGI，同步） | 同步 handler 拿不到共享的 `asyncpg` 连接池——池是 asyncio 原生的，同步框架没有事件循环去 `await` 它 |
| 同步 `grpc` | 要在每个方法里 `run_coroutine_threadsafe` 桥一次，且合并部署下 22 个模块共用一个事件循环，混进同步阻塞调用会拖垮全部 |
| gunicorn 多 worker | 多 worker = 多进程，Outbox 推送线程会跑 N 遍；外壳形态只有一个进程（§13.3 铁律七） |

**所以**：`uvicorn` 单进程单事件循环、`grpc.aio` 而不是同步 `grpc`、`asyncpg` 而不是 `psycopg2`。

## 用法

```python
# module.py
async def new(ctx: Context, rt: Runtime) -> Module:
    app = new_fastapi_app(rt)

    @app.get("/orders")
    async def list_orders():
        ...

    return Module(asgi_app=app)
```

```python
# main.py 只有一行
if __name__ == "__main__":
    run_standalone(new)
```

## 依赖

`fastapi` / `uvicorn` / `asyncpg`（不用 `psycopg2`）/ `nats-py` / `grpcio` + `grpcio-tools`（`grpc.aio`，不用同步 `grpc`）/ `opentelemetry-api` + `opentelemetry-sdk` / `prometheus-client`。版本精确锁定（`pyproject.toml` 里没有裸的 `>=`），Python `>=3.12`——理由同 `be-sdk-go` 用 `go 1.25`：整个生态在过去一年逐步抬高门槛，换旧版换不来任何好处。
