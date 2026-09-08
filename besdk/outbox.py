"""Outbox 写入与推送——对应 be-sdk-go 的 outbox.go。
目前只有签名，TDD 补实现。
"""

from __future__ import annotations

import asyncpg
import nats

from besdk.events import Event


async def publish_outbox(conn: asyncpg.Connection, schema: str, ev: Event) -> None:
    """在同一事务里把事件写进 ``event_outbox``（设计书 §3.10 Outbox
    Pattern）。

    ⚠️ 生产者必须走这条路，不许直接往 NATS publish——那样业务变更与事件
    发布不在同一事务里，进程崩在两者之间就是"改了但没发"或"发了但没改"。

    实现随后用 TDD 补。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")


async def start_outbox_pump(pool: asyncpg.Pool, schema: str, nc: nats.aio.client.Client) -> None:
    """起后台推送协程，轮询 outbox 发往 NATS。

    ⚠️ 这是 ``Module.start`` 的典型用法——必须能被取消、取消时干净返回，
    不许自己装信号处理器（§13.3 铁律七）。

    ⚠️ 参考 be-sdk-go v0.1.5→v0.1.6 真实踩过的坑（``实测踩坑记录.md``
    A4i）：查询失败（如数据库暂时不可达）只能记日志、继续下一轮 ticker，
    **绝不能让整个协程退出**——那样会连累整个进程被判定"异常退出"、被
    重启策略反复拉起又立刻失败，而这段时间内 HTTP/gRPC 完全没人能连，
    是比"依赖检查写进 /healthz"更隐蔽的第二条故障传播路径。

    实现随后用 TDD 补。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")
