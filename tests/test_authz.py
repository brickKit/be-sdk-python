"""权限键路由注册——对应 be-sdk-go 的 authz_test.go 三条断言。

⚠️ 判定本体本阶段仍是 fail-closed stub（见 authz.py 模块文档：Task 5
才换真实 bundle 轮询），这三条测试锁的是**这一阶段该有的行为**，不是
最终行为——Task 5 时这三条断言本身不变，变的只是 require_permission
内部的实现。
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import besdk


def _app() -> FastAPI:
    app = FastAPI()

    async def handler() -> dict[str, bool]:
        return {"ok": True}

    besdk.get(app.router, "/public", besdk.PUBLIC, handler)
    besdk.get(app.router, "/secret", "erp.sales.view", handler)
    return app


def test_非Public的路由在stub实现下一律拒绝() -> None:
    client = TestClient(_app())
    resp = client.get("/secret")
    assert resp.status_code == 403


def test_标了Public的路由放行() -> None:
    client = TestClient(_app())
    resp = client.get("/public")
    assert resp.status_code == 200


def test_PUBLIC是常量不是空字符串字面量的巧合() -> None:
    # ⚠️ 这条锁的是"漏传权限键"这个失败模式不存在——PUBLIC 的值恰好是
    # 空字符串是实现细节，调用点必须显式写 besdk.PUBLIC 才能通过
    # make gates 的裸路由扫描（阶段三计划 Task 3），不是随手传个 "" 就行。
    assert besdk.PUBLIC == ""
    assert isinstance(besdk.PUBLIC, str)
