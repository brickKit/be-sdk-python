"""权限键路由注册与判定——对应 be-sdk-go 的 authz.go。

阶段三 Task 5：``require_permission`` 从 Task 1 的 fail-closed stub
换成真实判定（JWT 本地验签 + bundle 轮询），与 ``be-sdk-go``/
``be-sdk-ts`` 同一批上线，形状逐字对应——Task 1 模块文档里记的那条
"先写 stub、Task 5 一起补"的偏离到这里兑现。
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from fastapi import APIRouter, Depends, HTTPException, Request, status

from besdk.bundle import BundleCache, start_bundle_poller
from besdk.jwt_verify import Claims, JWTVerifier
from besdk.scope import ScopeFilter, _set_current_scope

# PermKey 是权限键——assembly.yaml 的 permissions 段声明的那些
# （设计书 §14.1.1）。Python 没有 Go 那种"漏传参数编译不过"的机制，
# 强制靠 make gates 的裸路由扫描（阶段三计划 Task 3），不靠类型系统。
PermKey = str

# PUBLIC 是"显式公开"，不是"省略"。调用点必须显式传 besdk.PUBLIC 才能
# 通过 make gates 的扫描，不存在"忘了传权限键"这种失败模式
# （设计书 §14.1.7、导读第 23 条）。
PUBLIC: PermKey = ""

# AUTHENTICATED 是"已登录即可，不需要具体权限键"这一档（阶段三 Task 4
# 实现 infra-authz 时发现的真实缺口：GET /api/me/permissions 这类端点
# 任何登录用户都该能查，套一个具体权限键反而是画蛇添足）。⚠️ 仍然会验
# JWT 签名与 stale_since，只是跳过 bundle map 的权限键查找这一步。
AUTHENTICATED: PermKey = "__authenticated__"

_HandlerT = TypeVar("_HandlerT", bound=Callable[..., Awaitable[object]])

# 权限判定的进程级状态——由 run_standalone 在启动时装配一次（同
# otel provider 那一类"只能有一份"的东西，设计书 §12.5.2）。两者任一
# 为 None 都代表这个组件没配 iam_jwks_url/authz_bundle_url，此时任何
# 非 PUBLIC 权限键一律 fail-closed 403——这是阶段一遗留的默认状态，
# 阶段三给这两项配置赋值之前，行为不变。
_verifier: JWTVerifier | None = None
_bundle: BundleCache | None = None

_STALE_TIME_SKEW_SECONDS = 5  # jwt.iat 与 stale_since 比较时的容忍余量（设计书 §14.1.6）


def _set_authz_runtime(verifier: JWTVerifier | None, bundle: BundleCache | None) -> None:
    """只应由 ``run_standalone`` 调用一次。"""
    global _verifier, _bundle  # noqa: PLW0603 - 进程级单例赋值，同 be-sdk-go 的 setAuthzRuntime
    _verifier, _bundle = verifier, bundle


def setup_authz_runtime(
    iam_jwks_url: str, authz_bundle_url: str, logger: logging.Logger
) -> tuple[JWTVerifier | None, BundleCache | None]:
    """从两项配置装配 JWT 验签器与 bundle 轮询——``run_standalone`` 专用，
    模块代码不调用。两项配置任一缺失都返回 ``None``，调用方
    （``require_permission``）据此退化成 fail-closed stub，不阻断组件
    启动（§14.1.9：authz 不可达不该拖累组件本身）。
    """
    verifier: JWTVerifier | None = None
    if iam_jwks_url:
        try:
            verifier = JWTVerifier(iam_jwks_url)
        except Exception:
            logger.exception("初始化 JWT 验签器失败，非 PUBLIC/AUTHENTICATED 权限键将 fail-closed")
    else:
        logger.info("未配置 iamJwksUrl，非 PUBLIC/AUTHENTICATED 权限键将 fail-closed（阶段一遗留行为）")

    bundle: BundleCache | None = None
    if authz_bundle_url:
        bundle = start_bundle_poller(authz_bundle_url, logger)
    else:
        logger.info("未配置 authzBundleUrl，具体权限键判定将始终 503")

    return verifier, bundle


def _bearer_token(request: Request) -> str | None:
    header = request.headers.get("Authorization", "")
    prefix = "Bearer "
    if not header.startswith(prefix):
        return None
    token = header[len(prefix) :].strip()
    return token or None


def _is_stale(claims: Claims, bundle: BundleCache | None) -> bool:
    if bundle is None:
        return False
    since = bundle.stale_since_for(claims.sub)
    if since == 0:
        return False
    return claims.issued_at.timestamp() < since - _STALE_TIME_SKEW_SECONDS


def require_permission(perm: PermKey) -> Callable[[Request], Awaitable[None]]:
    """判定本体，返回一个 FastAPI 依赖。判定链（设计书 §14.1.6 第 3 步、
    §14.1.9）：

    1. ``PUBLIC``：直接放行，不验签——``/healthz`` 这类必须匿名可达的
       端点靠这条。
    2. 验签 JWT（本地，JWKS 从 ``iamJwksUrl`` 来）；没配时退化成阶段一
       的 fail-closed stub：非 PUBLIC 一律 403。
    3. ``jwt.iat < stale_since[sub]`` → 401 ``token_stale``（有界列表，
       §14.1.6）。
    4. ``AUTHENTICATED``：验签过、不 stale 就放行，不查权限键。
    5. 具体权限键：bundle 从没连上过 authz → 503（不是 403，语义更准，
       §14.1.9）；连上过就在纯并集展开后的权限键集合里查，查不到 403。
    """

    async def _dependency(request: Request) -> None:
        if perm == PUBLIC:
            return

        verifier = _verifier
        if verifier is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限判定尚未配置（iamJwksUrl 未注入）",
            )

        token = _bearer_token(request)
        if token is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="缺少或格式不对的 Authorization",
            )
        try:
            claims = await verifier.verify(token)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"token 无效: {exc}",
            ) from exc

        if _is_stale(claims, _bundle):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="token_stale",
                headers={"WWW-Authenticate": 'Bearer error="token_stale"'},
            )

        # ⚠️ 塞进 ScopeFilter 的 ContextVar，不是 request.state——scope_of()
        # 收的是零参数（同一份 ContextVar），跟 UserClient/SystemClient
        # 一样显式区分参数风格是 client.py 那边的事，这里延续既有约定。
        _set_current_scope(
            ScopeFilter(
                all=claims.dept_path == "",
                prefix=claims.dept_path,
                exact=claims.dept_path,
                owner=claims.sub,
            )
        )

        if perm == AUTHENTICATED:
            return

        bundle = _bundle
        if bundle is None or not bundle.has_ever_fetched():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="权限判定尚未就绪（authz 从启动到现在还没能连上过）",
            )
        if not bundle.has_permission(claims.roles, perm):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="无权限")

    return _dependency


def _register(router: APIRouter, method: str, path: str, perm: PermKey, handler: _HandlerT) -> None:
    router.add_api_route(path, handler, methods=[method], dependencies=[Depends(require_permission(perm))])


def get(router: APIRouter, path: str, perm: PermKey, handler: _HandlerT) -> None:
    """注册一条带权限键的 GET 路由。

    ⚠️ 业务代码不许再碰 ``router.get(...)``/``app.get(...)`` 原生装饰器
    ——那样绕开的不是一层封装，是权限键的强制：漏写权限键在 Go 版编译
    不过，在这里则完全不需要权限键，那个接口就从此无人鉴权且没有任何
    症状（设计书 §14.1.7、导读第 23 条）。这条约束在 Python 里只能靠
    ``make gates`` 的裸路由扫描守，不能靠类型系统。
    """
    _register(router, "GET", path, perm, handler)


def post(router: APIRouter, path: str, perm: PermKey, handler: _HandlerT) -> None:
    _register(router, "POST", path, perm, handler)


def put(router: APIRouter, path: str, perm: PermKey, handler: _HandlerT) -> None:
    _register(router, "PUT", path, perm, handler)


def patch(router: APIRouter, path: str, perm: PermKey, handler: _HandlerT) -> None:
    _register(router, "PATCH", path, perm, handler)


def delete(router: APIRouter, path: str, perm: PermKey, handler: _HandlerT) -> None:
    _register(router, "DELETE", path, perm, handler)
