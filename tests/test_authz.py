"""权限键路由注册与判定——对应 be-sdk-go 的 authz_test.go。

阶段三 Task 5：``require_permission`` 从 fail-closed stub 换成真实判定
后，"没配 iam_jwks_url 时保持 fail-closed"这条既有断言原样保留（下面
三条），新增的状态机断言覆盖真实判定的全部分支。
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import besdk
import besdk.authz as authz_module
from besdk.bundle import BundleCache
from besdk.jwt_verify import JWTVerifier
from tests.helpers import FakeBundleServer, FakeJWKSServer


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


# ── 阶段三 Task 5：真实判定上线后的状态机 ─────────────────────────


@pytest.fixture
def _restore_authz_runtime() -> Iterator[None]:
    """authz_module._verifier/_bundle 是进程级全局状态（同 otel provider
    那一类"进程只能有一份"的东西，见 authz.py 注释）——测试之间不这样
    隔离会互相污染：一条测试装好真实判定后，前面那三条验"stub 下一律
    拒绝"的既有测试会突然表现不一样。
    """
    prev_verifier, prev_bundle = authz_module._verifier, authz_module._bundle  # noqa: SLF001
    yield
    authz_module._verifier, authz_module._bundle = prev_verifier, prev_bundle  # noqa: SLF001


@pytest.fixture
def jwks() -> Iterator[FakeJWKSServer]:
    srv = FakeJWKSServer()
    yield srv
    srv.close()


def _app_with(perm: str) -> FastAPI:
    app = FastAPI()

    async def handler() -> dict[str, bool]:
        return {"ok": True}

    besdk.get(app.router, "/x", perm, handler)
    return app


async def test_配了验签器但没带token返回401(jwks: FakeJWKSServer, _restore_authz_runtime: None) -> None:
    authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
    client = TestClient(_app_with(besdk.AUTHENTICATED))

    resp = client.get("/x")

    assert resp.status_code == 401


async def test_token签名不合法返回401(jwks: FakeJWKSServer, _restore_authz_runtime: None) -> None:
    authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
    client = TestClient(_app_with(besdk.AUTHENTICATED))

    resp = client.get("/x", headers={"Authorization": "Bearer this.is.not.a.jwt"})

    assert resp.status_code == 401


async def test_Authenticated放行任何合法登录不查权限键(
    jwks: FakeJWKSServer, _restore_authz_runtime: None
) -> None:
    authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
    authz_module._bundle = None  # noqa: SLF001 - 连 bundle 都没配，AUTHENTICATED 不该关心它
    token = jwks.sign("u_zhangsan")
    client = TestClient(_app_with(besdk.AUTHENTICATED))

    resp = client.get("/x", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 200, resp.text


async def test_bundle从没连上过返回503(jwks: FakeJWKSServer, _restore_authz_runtime: None) -> None:
    """§14.1.9 明确要求的那一档：启动时始终拿不到第一份 bundle → 业务
    请求返 503（不是 403）——与"连上了但这个角色没这条权限"（403）必须
    是两个不同的状态码。
    """
    authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
    authz_module._bundle = BundleCache()  # noqa: SLF001 - 造出来但从没 fetch 过
    token = jwks.sign("u_zhangsan", roles=["sales_manager"])
    client = TestClient(_app_with("erp.sales.view"))

    resp = client.get("/x", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 503


async def test_权限键够200不够403(jwks: FakeJWKSServer, _restore_authz_runtime: None) -> None:
    fake_server = FakeBundleServer()
    try:
        fake_server.set_bundle({"sales_manager": ["erp.sales.view"]}, {}, '"v1"')
        from besdk.bundle import start_bundle_poller

        bundle = start_bundle_poller(fake_server.url, __import__("logging").getLogger("t"))
        import asyncio

        deadline = asyncio.get_event_loop().time() + 2
        while not bundle.has_ever_fetched() and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.02)

        authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
        authz_module._bundle = bundle  # noqa: SLF001
        token = jwks.sign("u_zhangsan", roles=["sales_manager"])

        app = FastAPI()

        async def handler() -> dict[str, bool]:
            return {"ok": True}

        besdk.get(app.router, "/view", "erp.sales.view", handler)
        besdk.get(app.router, "/approve", "erp.sales.approve", handler)
        client = TestClient(app)

        resp_ok = client.get("/view", headers={"Authorization": f"Bearer {token}"})
        resp_forbidden = client.get("/approve", headers={"Authorization": f"Bearer {token}"})

        assert resp_ok.status_code == 200, resp_ok.text
        assert resp_forbidden.status_code == 403
    finally:
        fake_server.close()


async def test_token早于stale_since返回401token_stale(
    jwks: FakeJWKSServer, _restore_authz_runtime: None
) -> None:
    """§14.1.6 判定链第 3 步的真实验证：一个人被踢出角色之后，他手上
    那份"旧快照"token 必须在下一次请求时就失效，不用等 TTL 到期。
    """
    import time

    # ⚠️ token 的自然 TTL 是 10 分钟（FakeJWKSServer.sign 写死），签发
    # 时间不能早到让 PyJWT 自己的"exp 过期"判定先手（同 be-sdk-go 踩过
    # 的同一处测试设计坑）——要测的是"在 TTL 有效期内、但比 stale_since
    # 更早签发"这个专属场景，不是自然过期。
    token_issued_at = time.time() - 120  # 2 分钟前
    token = jwks.sign("u_zhangsan", issued_at=token_issued_at)

    authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
    bundle = BundleCache()
    bundle._data.stale_since["u_zhangsan"] = int(time.time()) - 60  # noqa: SLF001 - 比 token 签发晚
    bundle._data.ever_fetched = True  # noqa: SLF001
    authz_module._bundle = bundle  # noqa: SLF001

    client = TestClient(_app_with(besdk.AUTHENTICATED))
    resp = client.get("/x", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 401, resp.text
    assert "token_stale" in resp.headers.get("WWW-Authenticate", "")
    assert "token_stale" in resp.text


async def test_token晚于stale_since不受影响(jwks: FakeJWKSServer, _restore_authz_runtime: None) -> None:
    """确认时间比较方向没有写反——这是最容易把 401 判定反过来的一条断言。"""
    import time

    token = jwks.sign("u_zhangsan", roles=["sales_manager"])  # 刚刚重新登录，比 stale_since 晚

    authz_module._verifier = JWTVerifier(jwks.url)  # noqa: SLF001
    bundle = BundleCache()
    bundle._data.stale_since["u_zhangsan"] = int(time.time()) - 3600  # noqa: SLF001
    bundle._data.roles["sales_manager"] = ["erp.sales.view"]  # noqa: SLF001
    bundle._data.ever_fetched = True  # noqa: SLF001
    authz_module._bundle = bundle  # noqa: SLF001

    client = TestClient(_app_with("erp.sales.view"))
    resp = client.get("/x", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 200, resp.text
