"""数据范围过滤——对应 be-sdk-go 的 scope.go。

阶段三 Task 5：``scope_of`` 从 Task 1 的"恒不限"换成真实求解——与
``authz.py`` 的 ``require_permission`` 同一批上线（后者验签成功后把
算好的 ``ScopeFilter`` 塞进这里的 ``ContextVar``）。
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field


@dataclass
class ScopeFilter:
    """查仓储时用的数据范围过滤条件——设计书 §14.2.3 的五档求解
    （all / dept_and_below / self_dept / self / custom）压平成一个结构体，
    仓储方法按哪个字段非空决定怎么拼 WHERE。

    ⚠️ 这是一个"纯函数"的输出（§14.2.4 明文）：五档不是 ``scope_of``
    自己判断出来的，``prefix``/``exact``/``owner`` 三个字段**始终**从
    同一份 JWT 的 ``dept_path``/``sub`` 填，"这次查询该用哪一档"是
    调用方（具体某条 SQL 查询）的静态选择——它只取自己关心的那个字段，
    其余字段的存在与否不影响它。``in_`` 字段本次（阶段三 Task 5）不填：
    它对应 ``mode: in`` 的"自定义列表"档（如 ``erp-inventory`` 的仓库
    维度），列表从哪来是业务组件自己的数据（不是 JWT 字段），要由业务
    组件自己的仓储层查出来后再组装，不归 ``scope_of`` 管。
    """

    all: bool = False  # all：不限。dept_path 为空（如坐在部门树根节点）时天然成立，不是特判出来的
    prefix: str = ""  # dept_and_below：dept_path LIKE prefix || '%'
    exact: str = ""  # self_dept：dept_path = exact
    owner: str = ""  # self：owner_id = owner
    in_: list[str] = field(default_factory=list)  # custom：调用方自己填，scope_of 不填（见上）


_current_scope: ContextVar[ScopeFilter | None] = ContextVar("besdk_scope", default=None)


def _set_current_scope(f: ScopeFilter) -> None:
    """只应由 ``require_permission`` 的依赖调用——同一个 ASGI 请求在
    Starlette 里独立起一个 asyncio task，``ContextVar.set`` 的效果不会
    漏到别的并发请求里，不需要显式 ``reset``（同一份 task 结束后这份
    context 直接被丢弃）。
    """
    _current_scope.set(f)


def scope_of() -> ScopeFilter:
    """取当前请求的数据范围。ctx 必须是经过 ``require_permission`` 处理
    过的请求（它把算好的 ``ScopeFilter`` 塞了进去）——除 PUBLIC/
    AUTHENTICATED 外的路由，这个前提总是成立。

    ⚠️ 取不到时**不能**返回一个"看起来安全"的默认值：§14.2.4 的 SQL
    约定是"空字符串表示不限"（``@scope_prefix = ''`` 表示不限），
    ``ScopeFilter()`` 的零值 ``prefix``/``owner`` 都是空字符串——那会被
    下游 SQL 解读成"放行一切"，方向反了，是 fail-**open** 不是
    fail-closed。这种调用只可能是编程错误（在 ``Module.start``/事件
    handler 里调 ``scope_of``，那些地方应该用 ``system_client`` 且不
    经过 ``require_permission``）——**抛异常**，让它在测试/联调阶段就
    现形，而不是安静地多返回几行数据（这是全项目第三条"悄悄读到别人
    数据"路径的同一类风险，§14.2.6）。
    """
    f = _current_scope.get()
    if f is None:
        msg = (
            "besdk.scope_of: 当前上下文里没有 ScopeFilter——只能在 require_permission "
            "已经验过签的请求路径上调用；Module.start/事件 handler 里查数据请用 "
            "system_client，不经过这里"
        )
        raise RuntimeError(msg)
    return f
