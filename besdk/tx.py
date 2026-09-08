"""事务 + role/schema 切换——对应 be-sdk-go 的 tx.go，逻辑逐字对应。"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg

_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")

T = TypeVar("T")


async def with_tx(
    pool: asyncpg.Pool,
    role: str,
    schema: str,
    fn: Callable[[asyncpg.Connection], Awaitable[T]],
) -> T:
    """开事务，切到本组件的 role 与 schema，跑 fn，然后 COMMIT。

    ⚠️⚠️ 绝不能用不带 LOCAL 的 SET。``SET search_path`` / ``SET ROLE`` 之后
    把连接还回共享池，下一个借用者会原样继承它——A 组件的查询打在 B 组件
    的表上，不报错、不崩，只是悄悄读写了别人的数据。这是这套写法唯一的
    雷，也是最难查的一个（设计书决策 3、§13.3 铁律二）。

    ``SET LOCAL`` 在 COMMIT/ROLLBACK 时自动还原，连接干净地回到池里。

    ⚠️ **一条真机验证过的、Go 版没有的细节**：``asyncpg.Pool`` 在
    ``release()`` 时会自动调用 ``Connection.reset()``（等价于
    ``DISCARD ALL``/``RESET ALL``），**把所有会话级配置重置为默认值**——
    这与 Go ``database/sql`` 的池**不一样**，后者不做任何自动重置，
    ``SET LOCAL`` 是唯一的防线。真机测试证实：即使故意把这里改回不带
    ``LOCAL`` 的 ``SET``，"下一个从池里借到同一条连接的人拿到脏
    search_path"这个具体场景在 asyncpg 下也不会重现，因为池自己已经
    在 release 时清理过一遍（``tests/test_tx.py`` 里那条"直接 spy 语句
    本身"的测试就是这样发现"看结果"这条路测不出来的）。**这不代表
    SET LOCAL 在这里可有可无**——仍然保留它，理由有二：① 防御纵深，
    不依赖 asyncpg 池实现细节的"免费"保护（万一将来改用不做自动 reset
    的连接管理方式）；② 与 be-sdk-go 保持同一套心智模型，两份 SDK 的
    公开语义要一致，不能一个显式防护、一个隐式依赖运行时行为。真正会被
    SET LOCAL 挡住、而 asyncpg 的池级 reset 挡不住的场景是：**同一次
    acquire 期间**（还没 release）连续处理多个 role/schema 不同的请求
    ——那种情况下 release 还没发生，池的自动重置也就还没触发。
    """
    # role 与 schema 来自 registry，不是用户输入；但它们要拼进 SQL
    # （SET LOCAL ROLE 不接受占位符/参数化），所以仍然白名单校验。
    if not _IDENT_RE.match(role):
        raise ValueError(f"非法 role 名：{role!r}")
    if not _IDENT_RE.match(schema):
        raise ValueError(f"非法 schema 名：{schema!r}")

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL ROLE {role}")
            # 归档 schema 也要在 search_path 里，batchGet 的冷热路由才不用
            # 写限定名。
            await conn.execute(f"SET LOCAL search_path TO {schema}, {schema}_archive")
            return await fn(conn)
