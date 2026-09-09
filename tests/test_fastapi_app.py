"""new_fastapi_app——对应 be-sdk-go 的 gin_test.go。

⚠️ 这个文件在此前是全空的：``/healthz`` 对 HEAD 返回 405 这个真实 bug
（FastAPI 的 ``APIRoute.__init__`` 不像 Starlette 的 ``Route.__init__``
那样自动把 GET 路由的 HEAD 请求接住）一直没被测试挡住，直到
``infra-print`` 第一次真机 ``brickkit up`` 时容器 unhealthy 才暴露——
这里补上回归测试，不再靠"肉眼看代码觉得对"。
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry

from besdk.fastapi_app import new_fastapi_app
from besdk.otel import get_tracer, init_otel
from besdk.runtime import Config, Runtime


@pytest.fixture
async def rt() -> Runtime:
    shutdown = await init_otel("test-component", "")
    runtime = Runtime(
        component_id="test/component",
        component_version="0.0.0",
        config=Config({}),
        db=MagicMock(),
        nats=MagicMock(),
        logger=MagicMock(),
        tracer=get_tracer("test-component"),
        meter=MagicMock(),
        registry=CollectorRegistry(),
        http_port=0,
    )
    yield runtime
    await shutdown()


@pytest.mark.asyncio
async def test_healthz对GET与HEAD都返回200(rt: Runtime) -> None:
    app = new_fastapi_app(rt)
    client = TestClient(app)

    get_resp = client.get("/healthz")
    assert get_resp.status_code == 200

    # ⚠️ 平台健康检查用 wget --spider，发的是 HEAD——真机验证过 FastAPI
    # 对纯 @app.get 注册的路由会给 HEAD 返回 405，这条是那个真实 bug 的
    # 回归测试。
    head_resp = client.head("/healthz")
    assert head_resp.status_code == 200


@pytest.mark.asyncio
async def test_metrics返回prometheus格式且含RED指标(rt: Runtime) -> None:
    app = new_fastapi_app(rt)
    client = TestClient(app)

    client.get("/healthz")  # 先打一次，确认 RED 中间件真的记了这次请求
    resp = client.get("/metrics")

    assert resp.status_code == 200
    assert "http_requests_total" in resp.text
    assert 'route="/healthz"' in resp.text
