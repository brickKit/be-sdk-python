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

# 首次拉取失败后的第一次重试间隔。首次成功之前按它翻倍退避（封顶
# BUNDLE_POLL_INTERVAL_SECONDS），见 _loop。
BUNDLE_FIRST_RETRY_DELAY_SECONDS = 0.5


def _next_bundle_retry_delay(d: float) -> float:
    """把退避间隔翻倍，封顶轮询间隔。"""
    return min(d * 2, BUNDLE_POLL_INTERVAL_SECONDS)


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

    async def _fetch_once(self, client: httpx.AsyncClient, url: str, logger: logging.Logger) -> bool:
        """拉一次，返回这次是否拿到了可用的 bundle（200 解析成功，或 304
        沿用已有的）。

        单次失败只记日志、沿用内存里最后一份 bundle 继续跑——这是
        §14.1.9 的 fail-static：一个授权服务抖动不该让使用它的组件同时
        拒绝所有请求。
        """
        headers = {"If-None-Match": self._data.etag} if self._data.etag else {}
        try:
            resp = await client.get(url, headers=headers, timeout=10.0)
        except httpx.HTTPError as exc:
            logger.warning("拉取 authz bundle 失败，沿用内存里已有的旧版本: %s", exc)
            return False

        if resp.status_code == 304:
            return True  # ETag 命中，未变化，沿用旧的
        if resp.status_code != 200:
            logger.warning(
                "拉取 authz bundle 收到非预期状态码 %s，沿用内存里已有的旧版本", resp.status_code
            )
            return False

        try:
            body = resp.json()
        except ValueError as exc:
            logger.error("解析 authz bundle 失败，沿用内存里已有的旧版本: %s", exc)
            return False

        self._data = _BundleData(
            roles=body.get("roles") or {},
            stale_since=body.get("stale_since") or {},
            etag=resp.headers.get("ETag", ""),
            ever_fetched=True,
        )
        return True

    async def _loop(self, url: str, logger: logging.Logger) -> None:
        async with httpx.AsyncClient() as client:
            # ⚠️ 首次成功之前不等满 15 秒：组件常和 authz 同时启动，第一次
            # 拉取时 authz 多半还没起来。旧实现要等一个完整轮询周期才重试，
            # 这期间每个受保护的路由都答 503（"还不知道"），启动后大约 20 秒
            # 不可用。现在按 0.5 秒起翻倍退避（封顶轮询间隔）重试，成功之后
            # 才进入 15 秒的条件轮询。
            delay = BUNDLE_FIRST_RETRY_DELAY_SECONDS
            while not await self._fetch_once(client, url, logger):
                await asyncio.sleep(delay)
                delay = _next_bundle_retry_delay(delay)
            while True:
                await asyncio.sleep(BUNDLE_POLL_INTERVAL_SECONDS)
                await self._fetch_once(client, url, logger)


def start_bundle_poller(url: str, logger: logging.Logger) -> BundleCache:
    """立刻拉一次，之后每 15 秒条件 GET 一次（设计书 §14.1.4/§14.1.6：
    生效时延全部 ~15 秒）；首次成功之前按 0.5 秒起翻倍、封顶 15 秒退避
    重试。返回的 task 不需要显式持有引用去取消——
    ``run_standalone`` 进程退出时事件循环停止，这个协程自然结束，
    不需要单独的 Stop（同 be-sdk-go 用 ctx 取消的判据，这里靠进程生命周期）。
    """
    cache = BundleCache()
    asyncio.create_task(cache._loop(url, logger))  # noqa: SLF001 - 同模块内部协作，不算破坏封装
    return cache
