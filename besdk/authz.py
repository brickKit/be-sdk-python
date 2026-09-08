"""权限键路由注册与判定——对应 be-sdk-go 的 authz.go。

⚠️ **与阶段三计划 Task 1 字面要求的一处偏离，写清楚不藏起来**：该任务
原文要求 `RequirePermission`/`ScopeOf`/`UserClient`/`SystemClient` "本阶段
第一次实现就要带真实判定，不像阶段二 be-sdk-go 先立 stub 形状"。但
`be-sdk-go` 自己**现在**（阶段三开工时）仍然是 fail-closed stub——真实的
bundle 轮询是 Task 5（`infra-authz` 建成之后）才会给三个 `be-sdk-*`
一起补的。若本文件现在就写真实判定，会出现"Python 版比 Go 版先进"的
临时不对称，且需要为一个还没有真实服务器可对照测试的契约设计并锁死
实现——这是与 Task 1 其余内容（结构三件套）不同量级的工作。

**权衡后的决定**：本文件先按 be-sdk-go 当前的 fail-closed stub 形状写
（签名真实、判定是 stub），Task 5 时与 `be-sdk-go`/`be-sdk-ts` 一起升级
成真实的 bundle 轮询——三者当时"同一天"变成真实判定，不留 Python 单独
抢跑又要在 Task 5 回头对齐的返工。这条偏离已经记进阶段三计划的执行
笔记，不是没读任务清单，是读了之后判断这样代价更小。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from fastapi import APIRouter, Depends, HTTPException, status

# PermKey 是权限键——assembly.yaml 的 permissions 段声明的那些
# （设计书 §14.1.1）。Python 没有 Go 那种"漏传参数编译不过"的机制，
# 强制靠 make gates 的裸路由扫描（阶段三计划 Task 3），不靠类型系统。
PermKey = str

# PUBLIC 是"显式公开"，不是"省略"。调用点必须显式传 besdk.PUBLIC 才能
# 通过 make gates 的扫描，不存在"忘了传权限键"这种失败模式
# （设计书 §14.1.7、导读第 23 条）。
PUBLIC: PermKey = ""

_HandlerT = TypeVar("_HandlerT", bound=Callable[..., Awaitable[object]])


def require_permission(perm: PermKey) -> Callable[[], Awaitable[None]]:
    """判定本体，返回一个 FastAPI 依赖。阶段三 Task 1 是 fail-closed stub：
    PUBLIC 放行，其余一律拒绝——真实的权限判定要等 Task 5（`infra-authz`
    上线后）换成进程内 bundle map 查找，签名不变，调用方不用跟着改。
    """

    async def _dependency() -> None:
        if perm == PUBLIC:
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="权限判定尚未实现（阶段三 Task 5 之前，非 PUBLIC 路由一律拒绝）",
        )

    return _dependency


def _register(router: APIRouter, method: str, path: str, perm: PermKey, handler: _HandlerT) -> None:
    router.add_api_route(path, handler, methods=[method], dependencies=[Depends(require_permission(perm))])


def get(router: APIRouter, path: str, perm: PermKey, handler: _HandlerT) -> None:
    """注册一条带权限键的 GET 路由。

    ⚠️ 业务代码不许再碰 ``router.get(...)``/``app.get(...)`` 原生装饰器
    ——那样绕开的不是一层封装，是权限键的强制：漏写权限键在 Go 版编译
    不过，在这里则完全不需要权限键，那个接口就从此无人鉴权且没有任何
    症状（设计书 §14.1.7、导读第 23 条）。**这条约束在 Python 里只能靠
    `make gates` 的裸路由扫描守，不能靠类型系统**——阶段三计划 Task 3
    已经点明"这条不是照抄 Go 版能过的，要重新设计判据"。
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
