"""Outbox 写入与推送——对应 be-sdk-go 的 outbox.go，逻辑逐字对应。"""

from __future__ import annotations

import asyncio
import logging
import re

import asyncpg
import nats

from besdk.events import Event

_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# NATS Header 里承载信封字段的 key——同 events.py（consume，阶段三 Task 1
# 后续 TDD 补）共用同一套名字，两边不能各写一份，否则生产者/消费者的
# 信封字段对不上。
HEADER_AGGREGATE_ID = "X-Aggregate-Id"
HEADER_VERSION = "X-Version"
HEADER_TRACE_ID = "X-Trace-Id"
HEADER_CAUSATION_ID = "X-Causation-Id"
HEADER_HOP_COUNT = "X-Hop-Count"

_OUTBOX_POLL_INTERVAL_SECONDS = 0.2

_logger = logging.getLogger("besdk.outbox")


async def publish_outbox(conn: asyncpg.Connection, schema: str, ev: Event) -> None:
    """在同一事务里把事件写进 ``event_outbox``（设计书 §3.10 Outbox
    Pattern）。

    ⚠️ 生产者必须走这条路，不许直接往 NATS publish——那样业务变更与事件
    发布不在同一事务里，进程崩在两者之间就是"改了但没发"或"发了但没改"。
    表结构见 §11.2.2（本函数假定表已经建好，不负责建表）。
    """
    if not _IDENT_RE.match(schema):
        msg = f"非法 schema 名：{schema!r}"
        raise ValueError(msg)
    await conn.execute(
        f"""
        INSERT INTO {schema}.event_outbox
            (subject, aggregate_id, version, trace_id, causation_id, hop_count, payload)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        """,
        ev.subject,
        ev.aggregate_id,
        ev.version,
        ev.trace_id,
        ev.causation_id,
        ev.hop_count,
        # ⚠️ 真机验证过的一处 Go/Python 差异：`payload` 列是 JSONB，
        # be-sdk-go 直接把 `[]byte` 传给 pgx 就能用（driver 层做了隐式
        # 转换）；asyncpg 对 JSONB 参数**只接受 str**，传 bytes 会报
        # "invalid input for query argument: expected str, got bytes"。
        # `Event.payload` 的类型仍然定成 `bytes`（跟 Go 版 `Event.Payload`
        # 保持同一个心智模型），这里解一次码，不改公开类型。
        ev.payload.decode("utf-8"),
    )


async def start_outbox_pump(pool: asyncpg.Pool, schema: str, nc: nats.aio.client.Client) -> None:
    """起后台推送协程，轮询 outbox 发往 NATS。

    ⚠️ 这是 ``Module.start`` 的典型用法——必须能被取消、取消时干净返回，
    不许自己装信号处理器（§13.3 铁律七）。``asyncio.CancelledError`` 会在
    ``asyncio.sleep``/``pool.acquire`` 等任意 await 点被抛出，不需要额外
    包装。

    发送成功立刻标记 ``PUBLISHED``；发送失败只累加 ``attempts``，状态留在
    ``PENDING`` 等下一轮重试——NATS 抖动不该让事件永久丢失，也不该让 pump
    自己崩掉。

    ⚠️ 同 be-sdk-go v0.1.5→v0.1.6 真实踩过的坑（``实测踩坑记录.md``
    A4i）：单轮查询失败（连接抖动/短暂不可用）只能记日志、留到下一轮，
    **绝不能让整个协程退出**——那样会让 ``run_standalone`` 判定这个任务
    "异常退出"，进而带崩整个进程，Docker 立刻重启又撞上同一个还没恢复的
    连接，陷入几百毫秒一次的重启死循环，这段时间里 HTTP/gRPC 完全没人
    能连。
    """
    if not _IDENT_RE.match(schema):
        msg = f"非法 schema 名：{schema!r}"
        raise ValueError(msg)

    while True:
        try:
            await asyncio.sleep(_OUTBOX_POLL_INTERVAL_SECONDS)
            await _pump_once(pool, schema, nc)
        except asyncio.CancelledError:
            return
        except Exception:  # noqa: BLE001 - 单轮失败只记日志，循环必须继续
            _logger.exception("outbox pump 单轮失败")


async def _pump_once(pool: asyncpg.Pool, schema: str, nc: nats.aio.client.Client) -> None:
    """处理一批 PENDING 事件。单条失败只累加 attempts，不中断整批——
    一条坏数据不该卡住同一批里的其他事件。
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, subject, aggregate_id, version, trace_id, causation_id, hop_count, payload
            FROM {schema}.event_outbox
            WHERE status = 'PENDING' ORDER BY id LIMIT 100
            """
        )

        for row in rows:
            headers = {
                HEADER_AGGREGATE_ID: row["aggregate_id"],
                HEADER_VERSION: str(row["version"]),
                HEADER_TRACE_ID: row["trace_id"] or "",
                HEADER_CAUSATION_ID: row["causation_id"] or "",
                HEADER_HOP_COUNT: str(row["hop_count"] or 0),
            }
            # ⚠️ 同 publish_outbox 那处 Go/Python 差异的反向：asyncpg 把
            # JSONB 列读回来是 str（不是 bytes），而 nats-py 的 publish()
            # 只接受 bytes——这里编一次码。
            payload_bytes = row["payload"].encode("utf-8") if row["payload"] else b""
            try:
                await nc.publish(row["subject"], payload_bytes, headers=headers)
            except Exception:  # noqa: BLE001 - 发布失败只累加重试计数，不是硬错误
                await conn.execute(
                    f"UPDATE {schema}.event_outbox SET attempts = attempts + 1, updated_at = now() WHERE id = $1",
                    row["id"],
                )
                continue
            await conn.execute(
                f"""UPDATE {schema}.event_outbox
                    SET status = 'PUBLISHED', published_at = now(), updated_at = now()
                    WHERE id = $1""",
                row["id"],
            )
