"""事件信封与幂等消费——对应 be-sdk-go 的 events.go。
目前只有签名，TDD 补实现。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import asyncpg
import nats


@dataclass
class Event:
    """事件的统一信封。三个字段由 SDK 自动填/校验（设计书 §3.10）。"""

    subject: str  # {domain}.{aggregate}.{action}.v{n}
    aggregate_id: str
    version: int  # 消费侧只允许严格大于本地当前值才更新（§3.10）
    trace_id: str = ""
    causation_id: str = ""
    hop_count: int = 0  # > 5 直接丢弃进 DLQ
    payload: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)


async def consume(
    nc: nats.aio.client.Client,
    pool: asyncpg.Pool,
    schema: str,
    subject: str,
    fn: Callable[[asyncpg.Connection, Event], Awaitable[None]],
) -> None:
    """注册幂等消费者：自动做 inbox 去重、hop_count 防环、version 单调
    校验。

    ⚠️ 守的是 §3.10 事件纪律与 §4.6 幂等性铁律——业务代码里只写 fn 的
    内容，去重/防环/单调校验全部由这一层负责，不许业务代码自己再判一遍。

    ⚠️ 用的是 ``nc.subscribe``（NATS 核心发布订阅），**不是** queue group
    ——同一 subject 的多个订阅者会各收到一份广播，不是竞争消费。这条已经
    真实造成过一次测试假阳性（阶段二踩坑记录 E1：本地测试进程与真实运行
    的容器同时订阅同一个 subject，容器"帮忙"处理了测试发的消息）。写这个
    组件自己的消费者测试前，先确认没有另一个真实实例订阅着同一个 subject。

    实现随后用 TDD 补（总纲 SOP-W W-1：契约先于测试）。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")
