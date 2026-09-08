"""GET /authz/bundle 轮询——对应 be-sdk-go 的 bundle.go。

"组件里没有任何一张权限表"这条在这里成立：这只是一份进程内缓存，不落
库、不进迁移（设计书 §14.1.4）。⚠️ 这是全组件唯一一份、由
``run_standalone`` 在启动时创建一次，``require_permission`` 只读它。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

import httpx

BUNDLE_POLL_INTERVAL_SECONDS = 15.0


@dataclass
class _BundleData:
    roles: dict[str, list[str]] = field(default_factory=dict)
    stale_since: dict[str, int] = field(default_factory=dict)
    etag: str = ""
    ever_fetched: bool = False  # 区分"还没连上过 authz"与"连上了只是没有这条权限"


class BundleCache:
    """⚠️ 不用 ``asyncio.Lock`` 保护 ``_data``——asyncio 是单线程协作式
    调度，`self._data = _BundleData(...)` 这个整体替换是一条没有
    ``await`` 的语句，不可能被打断到一半，天然原子。加锁只会多一层
    没有必要的间接（同 SOP-P：逻辑本身简单却硬套机制是更糟的结果）。
    """

    def __init__(self) -> None:
        self._data = _BundleData()

    def has_ever_fetched(self) -> bool:
        return self._data.ever_fetched

    def has_permission(self, roles: list[str], perm: str) -> bool:
        """纯并集展开（设计书 §14.1.3：纯并集，无 Deny）。"""
        for role in roles:
            if perm in self._data.roles.get(role, ()):
                return True
        return False

    def stale_since_for(self, sub: str) -> int:
        """不存在返回 0（永不 stale）。"""
        return self._data.stale_since.get(sub, 0)

    async def _fetch_once(self, client: httpx.AsyncClient, url: str, logger: logging.Logger) -> None:
        """单次失败只记日志、沿用内存里最后一份 bundle 继续跑——这是
        §14.1.9 的 fail-static：一个授权服务抖动不该让使用它的组件同时
        拒绝所有请求。
        """
        headers = {"If-None-Match": self._data.etag} if self._data.etag else {}
        try:
            resp = await client.get(url, headers=headers, timeout=10.0)
        except httpx.HTTPError as exc:
            logger.warning("拉取 authz bundle 失败，沿用内存里已有的旧版本: %s", exc)
            return

        if resp.status_code == 304:
            return  # ETag 命中，未变化，沿用旧的
        if resp.status_code != 200:
            logger.warning(
                "拉取 authz bundle 收到非预期状态码 %s，沿用内存里已有的旧版本", resp.status_code
            )
            return

        try:
            body = resp.json()
        except ValueError as exc:
            logger.error("解析 authz bundle 失败，沿用内存里已有的旧版本: %s", exc)
            return

        self._data = _BundleData(
            roles=body.get("roles") or {},
            stale_since=body.get("stale_since") or {},
            etag=resp.headers.get("ETag", ""),
            ever_fetched=True,
        )

    async def _loop(self, url: str, logger: logging.Logger) -> None:
        async with httpx.AsyncClient() as client:
            await self._fetch_once(client, url, logger)
            while True:
                await asyncio.sleep(BUNDLE_POLL_INTERVAL_SECONDS)
                await self._fetch_once(client, url, logger)


def start_bundle_poller(url: str, logger: logging.Logger) -> BundleCache:
    """立刻拉一次，之后每 15 秒条件 GET 一次（设计书 §14.1.4/§14.1.6：
    生效时延全部 ~15 秒）。返回的 task 不需要显式持有引用去取消——
    ``run_standalone`` 进程退出时事件循环停止，这个协程自然结束，
    不需要单独的 Stop（同 be-sdk-go 用 ctx 取消的判据，这里靠进程生命周期）。
    """
    cache = BundleCache()
    asyncio.create_task(cache._loop(url, logger))  # noqa: SLF001 - 同模块内部协作，不算破坏封装
    return cache
