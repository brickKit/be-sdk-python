from __future__ import annotations

import asyncio
import logging
import time

import pytest

from besdk.bundle import BUNDLE_POLL_INTERVAL_SECONDS, start_bundle_poller
from tests.helpers import FakeBundleServer

_LOGGER = logging.getLogger("test_bundle")


@pytest.fixture
def fake_server() -> FakeBundleServer:
    srv = FakeBundleServer()
    yield srv
    srv.close()


async def _wait_until(cond, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("等待条件超时")


async def test_轮询后能查到权限(fake_server: FakeBundleServer) -> None:
    fake_server.set_bundle({"sales_manager": ["erp.sales.view", "erp.sales.approve"]}, {}, '"v1"')

    cache = start_bundle_poller(fake_server.url, _LOGGER)
    await _wait_until(cache.has_ever_fetched)

    assert cache.has_permission(["sales_manager"], "erp.sales.view")
    assert not cache.has_permission(["sales_manager"], "erp.sales.export")


async def test_ETag未变化返回304不清空内容(fake_server: FakeBundleServer) -> None:
    fake_server.set_bundle({"r1": ["perm.a"]}, {}, '"v1"')
    cache = start_bundle_poller(fake_server.url, _LOGGER)
    await _wait_until(cache.has_ever_fetched)

    # 手动再触发一次拉取（不改内容，ETag 不变，服务器应该回 304）。
    import httpx

    async with httpx.AsyncClient() as client:
        await cache._fetch_once(client, fake_server.url, _LOGGER)  # noqa: SLF001

    assert fake_server.not_match_count > 0, "第二次拉取应该命中 If-None-Match 拿到 304"
    assert cache.has_permission(["r1"], "perm.a"), "304 之后旧内容不该被清空"


async def test_单次拉取失败不清空旧内容(fake_server: FakeBundleServer) -> None:
    fake_server.set_bundle({"r1": ["perm.a"]}, {}, '"v1"')
    cache = start_bundle_poller(fake_server.url, _LOGGER)
    await _wait_until(cache.has_ever_fetched)

    import httpx

    async with httpx.AsyncClient() as client:
        await cache._fetch_once(client, "http://127.0.0.1:1/nope", _LOGGER)  # noqa: SLF001

    assert cache.has_permission(["r1"], "perm.a"), "fail-static：单次拉取失败不该清空内存里已有的 bundle"


@pytest.mark.slow
async def test_15秒后角色变更真的生效(fake_server: FakeBundleServer) -> None:
    """阶段三 Task 5 计划明确要求的断言：改角色分配后不重启组件，等 15
    秒左右重新请求，断言权限跟着变——这条要真等 15 秒，不是 mock 时钟。
    """
    fake_server.set_bundle({"sales_rep": []}, {}, '"v1"')
    cache = start_bundle_poller(fake_server.url, _LOGGER)
    await _wait_until(cache.has_ever_fetched)
    assert not cache.has_permission(["sales_rep"], "erp.sales.approve")

    fake_server.set_bundle({"sales_rep": ["erp.sales.approve"]}, {}, '"v2"')
    await asyncio.sleep(BUNDLE_POLL_INTERVAL_SECONDS + 2)  # 真睡，不 mock 时钟

    assert cache.has_permission(["sales_rep"], "erp.sales.approve")
