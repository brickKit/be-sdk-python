"""batchGet 冷热自动路由——对应 be-sdk-go 的 archive.go。
目前只有签名，TDD 补实现。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg

T = TypeVar("T")


async def batch_get_routed(
    conn: asyncpg.Connection,
    schema: str,
    table: str,
    ids: list[str],
    scan: Callable[[asyncpg.Record], Awaitable[T]],
) -> list[T]:
    """先查热表，缺失的再查 ``{schema}_archive``（设计书 §11.6.1）。

    ⚠️ 每个聚合根必须提供的 batchGet（§3.8）走这一层实现冷热自动路由——
    业务代码里不许出现"若已归档则……"这种分支，归档与否对业务完全透明。

    实现随后用 TDD 补：核心不变量是"传入 N 个 ID，无论各自在热表还是
    归档表，返回集合与传入顺序无关、且不重不漏"。

    ⚠️ 参考 be-sdk-go v0.1.2→v0.1.3 真实踩过的坑（``实测踩坑记录.md``
    A4e）：归档表不存在时不能"先查、失败了再判断能不能忽略"——一条语句
    在事务里真的执行失败后，PostgreSQL 会把整个事务标记成 aborted，Python
    这层的错误处理挽不回。要用 ``to_regclass`` 之类不会报错的方式先确认
    归档表存在，再决定查不查它。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")
