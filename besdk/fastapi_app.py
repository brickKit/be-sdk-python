"""new_fastapi_app——对应 be-sdk-go 的 gin.go 里的 NewGinEngine。

发一个已挂好全部中间件的 FastAPI app：OTel、request-id、error 映射、
结构化访问日志、RED 指标，并已挂 ``/healthz`` 与 ``/metrics``。

⚠️ 组件不许自己 ``FastAPI()``——中间件漏一条不会报错，只是那个组件从此
没有 trace、没有 RED 指标，而 Grafana 上看起来只是"这个组件流量低"。

⚠️ ``/healthz`` 只检查本进程存活，不查依赖、不查数据库（设计书 §12.3.6：
一个下游抖动会让所有上游同时被判不健康并重启，合并态下更狠）。

⚠️ **与 Gin 同一个坑，Python 也会踩——这条曾经在这里写反过**：Gin 的
路由不会让 GET 处理器顺带接住 HEAD 请求（be-sdk-go 自己 v0.1.4→v0.1.5
真实踩过，见 `docs/dev/实测踩坑记录.md` A4h——平台的健康检查用
`wget --spider` 发的是 HEAD）。这里曾经写着"Starlette 默认会自动应答
HEAD，所以不需要再注册"——**那是错的**，`infra-print` 第一次真机
`brickkit up` 时容器直接 unhealthy，实测确认：纯 Starlette 的
`Route.__init__` 确实有 `if "GET" in methods: methods.add("HEAD")`，
但 **FastAPI 的 `APIRoute.__init__` 整个不调用 `super().__init__()`，
自己重新赋值 `self.methods`，那一步"GET 自动带上 HEAD"的逻辑没有被
带过来**——`@app.get(...)` 在 FastAPI 下对 HEAD 请求会直接 405，这是
FastAPI 本身的行为，不是这个 SDK 装配错了什么。同 Gin 版一样，必须
显式再注册一次 HEAD。
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse
from opentelemetry.trace import Status, StatusCode
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

if TYPE_CHECKING:
    from besdk.runtime import Runtime


def new_fastapi_app(rt: "Runtime") -> FastAPI:
    req_total = Counter(
        "http_requests_total",
        "HTTP 请求总数（RED 的 Rate + Errors）",
        ["method", "route", "status"],
        registry=rt.registry,
    )
    req_duration = Histogram(
        "http_request_duration_seconds",
        "HTTP 请求耗时（RED 的 Duration）",
        ["method", "route"],
        registry=rt.registry,
    )

    app = FastAPI()

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        req_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex
        request.state.request_id = req_id
        response = await call_next(request)
        response.headers["X-Request-Id"] = req_id
        return response

    @app.middleware("http")
    async def tracing_middleware(request: Request, call_next):
        route = request.scope.get("route")
        route_path = route.path if route else request.url.path
        with rt.tracer.start_as_current_span(f"{request.method} {route_path}") as span:
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.route", route_path)
            response = await call_next(request)
            if response.status_code >= 500:
                span.set_status(Status(StatusCode.ERROR, str(response.status_code)))
            return response

    @app.middleware("http")
    async def red_metrics_middleware(request: Request, call_next):
        route = request.scope.get("route")
        route_path = route.path if route else request.url.path
        start = time.monotonic()
        response = await call_next(request)
        elapsed = time.monotonic() - start
        req_total.labels(method=request.method, route=route_path, status=response.status_code).inc()
        req_duration.labels(method=request.method, route=route_path).observe(elapsed)
        return response

    @app.middleware("http")
    async def access_log_middleware(request: Request, call_next):
        response = await call_next(request)
        # ⚠️ 结构化日志、trace 上下文自动注入、PII 脱敏都在 logging.py 里，
        # 那里现在只有签名——这一行是占位，logging.py 补上真实实现后这里
        # 改调 rt.logger 的结构化方法，不再用裸 print。
        rt.logger.info(
            "%s %s %s",
            request.method,
            request.url.path,
            response.status_code,
            extra={"request_id": getattr(request.state, "request_id", "")},
        )
        return response

    @app.get("/healthz")
    @app.head("/healthz")  # 平台健康检查用 wget --spider 发 HEAD，见上方模块文档
    async def healthz() -> Response:
        return Response(status_code=200)

    @app.get("/metrics")
    async def metrics() -> Response:
        return PlainTextResponse(
            generate_latest(rt.registry), media_type=CONTENT_TYPE_LATEST
        )

    return app
