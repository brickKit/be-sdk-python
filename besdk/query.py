"""List 查询的时间窗口/游标注入——对应 be-sdk-go 的 query.go。
目前只有签名，TDD 补实现。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Query:
    """List 类查询的参数信封，字段留给后续按需扩（时间窗口、cursor、
    排序、过滤）。现在只钉签名，不钉字段——字段属于"横切函数的输入形状"，
    不属于"结构三件套"，改起来代价小得多。
    """


def list_window(q: Query) -> Query:
    """给 List 查询自动注入时间窗口（默认最近 90 天）与 Cursor 分页
    （设计书 §11.4）。

    ⚠️ 守的是决策 53：List 契约里没有 offset 字段，深分页在契约层面就
    不可表达；业务代码里永远只写类似 ``SELECT * FROM sales_orders`` 的
    查询，时间窗口与游标由这一层注入，不许业务代码自己拼。

    实现随后用 TDD 补。
    """
    raise NotImplementedError("阶段三 Task 1 后续 TDD 补")
