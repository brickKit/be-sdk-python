"""数据范围过滤——对应 be-sdk-go 的 scope.go。同 authz.py，先按阶段二
be-sdk-go 当前的形状写（签名真实、恒不限），Task 5 一起换成真实求解
（理由见 authz.py 模块文档）。
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field


@dataclass
class ScopeFilter:
    """查仓储时用的数据范围过滤条件——设计书 §14.2.3 的五档求解
    （all / dept_and_below / self_dept / self / custom）压平成一个结构体，
    仓储方法按哪个字段非空决定怎么拼 WHERE。
    """

    all: bool = False  # all：不限
    prefix: str = ""  # dept_and_below：dept_path LIKE prefix || '%'
    exact: str = ""  # self_dept：dept_path = exact
    owner: str = ""  # self：owner_id = owner
    in_: list[str] = field(default_factory=list)  # custom：dept_path LIKE ANY(...)


_current_scope: ContextVar[ScopeFilter] = ContextVar("besdk_scope", default=ScopeFilter(all=True))


def scope_of() -> ScopeFilter:
    """取当前请求的数据范围。阶段三 Task 1 恒返回不限——JWT 里的
    dept_path/sub 要等 Task 5（`infra-authz` 上线）才有真实的身份链路
    可解。签名先定型，调用方现在就按它接线；Task 5 只改这一个函数的
    实现，不改调用点。
    """
    return _current_scope.get()
