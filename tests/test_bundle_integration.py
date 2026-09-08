"""真机对接 infra-authz——对应 be-sdk-go 的 bundle_integration_test.go。

不是 mock：直接打真实跑着的 infra-authz 容器的 GET /authz/bundle，
确认 be-sdk-python 的轮询客户端认得它实际吐出来的 JSON 形状（两边是
本项目自己分两次写的，最容易出现"字段名各写各的"这类耦合裂缝）。
设了 TEST_AUTHZ_BUNDLE_URL 才跑，同 TEST_PG_DSN 的约定。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import pytest

from besdk.bundle import start_bundle_poller

_LOGGER = logging.getLogger("test_bundle_integration")


async def test_真机对接infra_authz() -> None:
    url = os.environ.get("TEST_AUTHZ_BUNDLE_URL", "")
    if not url:
        pytest.skip("未设置 TEST_AUTHZ_BUNDLE_URL，跳过（本地至少跑一次真的）")

    cache = start_bundle_poller(url, _LOGGER)
    deadline = time.monotonic() + 5
    while not cache.has_ever_fetched() and time.monotonic() < deadline:
        await asyncio.sleep(0.05)

    # infra-authz 的迁移种了 authz_admin/infra.authz.admin 这条真实的
    # 自举数据（003_seed_bootstrap_admin_role.up.sql）——用它做断言，
    # 不用测试自己造的数据，这样即使全新环境第一次跑也能通过。
    assert cache.has_permission(["authz_admin"], "infra.authz.admin")
