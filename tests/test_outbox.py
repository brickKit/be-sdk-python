"""publish_outbox/start_outbox_pump——对应 be-sdk-go 的 outbox_test.go。
需要真实 PostgreSQL + NATS，未设置 TEST_PG_DSN 时跳过。

⚠️ 故意不用任何真实组件的 schema（如 infra_workflow）——那些 schema
背后可能有一个真实部署的容器正在用自己的 be-sdk-go outbox pump 轮询
同一张表（每 200ms 一次），会跟这里测试用的 pump 产生竞态：两边都可能
选中同一条 PENDING 行、都真的发布到 NATS、都把它标记
PUBLISHED——本项目在 integration-im-dingtalk 那次真机验证时已经踩过一次
"测试事件被真实容器广播消费"的坑（同 events.py consume() 文档里提到的
阶段二踩坑记录 E1 是同一类问题）。这里改用 ``public``/``postgres``——
同 test_tx.py 的既有先例，是 SDK 自己的测试沙盒，不会有任何真实组件的
后台任务盯着它。
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import asyncpg
import nats
import pytest

from besdk.events import Event
from besdk.outbox import publish_outbox, start_outbox_pump
from besdk.tx import with_tx

_TEST_SCHEMA = "public"
_TEST_ROLE = "postgres"


def _dsn() -> str:
    dsn = os.environ.get("TEST_PG_DSN")
    if not dsn:
        pytest.skip("未设置 TEST_PG_DSN，跳过（CI 里必须设）")
    return dsn


def _nats_url() -> str:
    return os.environ.get("TEST_NATS_URL", "nats://localhost:4222")


def _unique(prefix: str) -> str:
    return f"{prefix}-{time.time_ns()}"


async def _ensure_event_outbox_table(pool: asyncpg.Pool) -> None:
    """SDK 自己的测试沙盒表——不分区（真实组件的 event_outbox 按周分区，
    这里测的是 publish_outbox/start_outbox_pump 的逻辑本身，不是分区
    维护，不需要那份复杂度）。字段与真实迁移的 002_create_outbox 逐字
    对应，只是去掉了 PARTITION BY。
    """
    await pool.execute(
        """
        CREATE TABLE IF NOT EXISTS public.event_outbox (
            id           BIGSERIAL PRIMARY KEY,
            subject      TEXT        NOT NULL,
            aggregate_id TEXT        NOT NULL,
            version      BIGINT      NOT NULL,
            trace_id     TEXT        NOT NULL DEFAULT '',
            causation_id TEXT        NOT NULL DEFAULT '',
            hop_count    INT         NOT NULL DEFAULT 0,
            payload      JSONB       NOT NULL,
            published_at TIMESTAMPTZ,
            attempts     INT         NOT NULL DEFAULT 0,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            status       TEXT        NOT NULL DEFAULT 'PENDING'
        )
        """
    )


@pytest.mark.asyncio
async def test_非法schema名被拒绝() -> None:
    pool = await asyncpg.create_pool(_dsn())
    try:
        ev = Event(subject="test.x.v1", aggregate_id="1", version=1, payload=b"{}")
        with pytest.raises(ValueError, match="schema"):
            async with pool.acquire() as conn, conn.transaction():
                await publish_outbox(conn, "public; --", ev)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_publish_outbox真的写进了event_outbox表() -> None:
    pool = await asyncpg.create_pool(_dsn())
    aggregate_id = _unique("agg")
    try:
        await _ensure_event_outbox_table(pool)
        ev = Event(
            subject="besdk.outbox_test.created.v1",
            aggregate_id=aggregate_id,
            version=1,
            trace_id="trace-1",
            causation_id="cause-1",
            hop_count=0,
            payload=b'{"a":1}',
        )

        async def _write(conn: asyncpg.Connection) -> None:
            await publish_outbox(conn, _TEST_SCHEMA, ev)

        await with_tx(pool, _TEST_ROLE, _TEST_SCHEMA, _write)

        row = await pool.fetchrow(
            "SELECT subject, aggregate_id, version, payload, status "
            "FROM public.event_outbox WHERE aggregate_id = $1",
            aggregate_id,
        )
        assert row is not None
        assert row["subject"] == "besdk.outbox_test.created.v1"
        assert row["status"] == "PENDING"
        assert row["payload"] == '{"a": 1}'
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_start_outbox_pump真的把pending事件发到nats并标记published() -> None:
    """端到端：写一条 PENDING 事件 → 起 pump → 真订阅 NATS 收到消息 →
    表里状态变成 PUBLISHED。真实 Postgres + 真实 NATS，不是 mock。
    """
    pool = await asyncpg.create_pool(_dsn())
    nc = await nats.connect(_nats_url())
    aggregate_id = _unique("pump-agg")
    subject = f"besdk.outbox_pump_test.created.v1.{aggregate_id}"

    try:
        await _ensure_event_outbox_table(pool)
        received: list[bytes] = []

        async def _handler(msg: nats.aio.msg.Msg) -> None:
            received.append(msg.data)

        sub = await nc.subscribe(subject, cb=_handler)

        ev = Event(subject=subject, aggregate_id=aggregate_id, version=1, payload=b'{"ok":true}')

        async def _write(conn: asyncpg.Connection) -> None:
            await publish_outbox(conn, _TEST_SCHEMA, ev)

        await with_tx(pool, _TEST_ROLE, _TEST_SCHEMA, _write)

        pump_task = asyncio.create_task(start_outbox_pump(pool, _TEST_SCHEMA, nc))
        try:
            for _ in range(50):
                if received:
                    break
                await asyncio.sleep(0.1)
        finally:
            # ⚠️ start_outbox_pump 内部 catch 了 CancelledError 并直接
            # return（干净关停，见其函数文档），所以取消后 await 这个
            # task 会正常返回 None，不会重新抛出 CancelledError——那是
            # 设计意图，不是需要用 pytest.raises 断言的异常路径。
            pump_task.cancel()
            await pump_task

        # ⚠️ 不能按字节原样比较——PostgreSQL 存 JSONB 时会按自己的规范化
        # 格式重新序列化（冒号后面加空格），"透传"指的是语义层面的 JSON
        # 值不变，不是字节数组不变（同 infra-workflow repo_test.go 已经
        # 记过的既有事实）。
        assert len(received) == 1
        assert json.loads(received[0]) == {"ok": True}

        row = await pool.fetchrow(
            "SELECT status, published_at FROM public.event_outbox WHERE aggregate_id = $1",
            aggregate_id,
        )
        assert row["status"] == "PUBLISHED"
        assert row["published_at"] is not None

        await sub.unsubscribe()
    finally:
        await nc.close()
        await pool.close()


@pytest.mark.asyncio
async def test_pump单轮失败不会让协程退出(monkeypatch: pytest.MonkeyPatch) -> None:
    """同 be-sdk-go A4i 的既有判据：查询失败只记日志、循环继续，不能让
    整个协程带着异常退出——否则 run_standalone 会把这当成"服务异常退出"
    带崩整个进程。这里故意让第一轮 _pump_once 抛异常，断言协程仍然存活、
    第二轮正常跑。
    """
    import besdk.outbox as outbox_module

    pool = await asyncpg.create_pool(_dsn())
    nc = await nats.connect(_nats_url())
    await _ensure_event_outbox_table(pool)
    call_count = 0
    real_pump_once = outbox_module._pump_once

    async def _flaky_pump_once(p: asyncpg.Pool, schema: str, conn: nats.aio.client.Client) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            msg = "模拟数据库暂时不可达"
            raise RuntimeError(msg)
        await real_pump_once(p, schema, conn)

    monkeypatch.setattr(outbox_module, "_pump_once", _flaky_pump_once)

    try:
        task = asyncio.create_task(start_outbox_pump(pool, _TEST_SCHEMA, nc))
        await asyncio.sleep(0.6)  # 至少跨过两轮 200ms 的 poll interval
        assert not task.done(), "第一轮失败后协程不该退出"
        assert call_count >= 2, "协程应该在第一轮失败后继续跑第二轮"
        task.cancel()
        await task  # 内部已捕获 CancelledError 并 return，正常完成不重新抛出
    finally:
        await nc.close()
        await pool.close()
