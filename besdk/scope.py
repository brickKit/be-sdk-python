"""数据范围过滤——对应 be-sdk-go 的 scope.go。

阶段三 Task 5：``scope_of`` 从 Task 1 的"恒不限"换成真实求解——与
``authz.py`` 的 ``require_permission`` 同一批上线（后者验签成功后把
``scope_from_claims`` 算好的 ``ScopeFilter`` 塞进这里的 ``ContextVar``）。

v0.5.0（R60）：空 ``dept_path`` 不再是"不限"。authz 签发的真实路径恒以
``/`` 开头（根部门也是 ``/<根id>/``），空串只出现在没分部门的人身上；
旧实现把它直接当前缀，``LIKE '' || '%'`` 匹配所有行，是 fail-open。
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field

from besdk.jwt_verify import Claims

NO_DEPT_PATH = "!no-dept"
"""没分部门（或 ``dept_path`` 格式异常）时 ``prefix``/``exact`` 的取值。

⚠️ 它是值层面的 fail-closed：不以 ``/`` 开头，任何真实路径都不会以它
开头；不含 ``%`` ``_``，绑进 ``LIKE $n || '%'`` 不会变成通配符。所以没
改过代码的下游——``LIKE`` 前缀匹配、``startswith``——什么都不命中，
``owner OR org`` 退化成只剩本人。三份 SDK 用同一个值。

⚠️ 只能用来**查**，不能**写进行里**：建单时要把调用方的部门快照进
``dept_path`` 列的，``has_dept`` 为假时写空串，不要写这个哨兵。
"""


@dataclass
class ScopeFilter:
    """查仓储时用的数据范围过滤条件——设计书 §14.2.3 的五档求解
    （all / dept_and_below / self_dept / self / custom）压平成一个结构体，
    仓储方法按哪个字段决定怎么拼 WHERE。

    ⚠️ 这是一个"纯函数"的输出（§14.2.4 明文，见 ``scope_from_claims``）：
    五档不是 ``scope_of`` 自己判断出来的，``prefix``/``exact``/``owner``
    三个字段**始终**从同一份 JWT 的 ``dept_path``/``sub`` 填，"这次查询
    该用哪一档"是调用方（具体某条 SQL 查询）的静态选择——它只取自己关心
    的那个字段，其余字段的存在与否不影响它。

    SDK 保证 ``prefix``/``exact`` 永不为空串：没有部门时是
    ``NO_DEPT_PATH``。仓储层收到空串前缀只可能是编程错误，不能当成"全部"。

    ``in_`` 字段本次（阶段三 Task 5）不填：它对应 ``mode: in`` 的"自定义
    列表"档（如 ``erp-inventory`` 的仓库维度），列表从哪来是业务组件自己
    的数据（不是 JWT 字段），要由业务组件自己的仓储层查出来后再组装，
    不归 ``scope_of`` 管。
    """

    all: bool = False  # all：整棵树。只有显式根标记 dept_path == "/" 时为真
    prefix: str = NO_DEPT_PATH  # dept_and_below：dept_path LIKE prefix || '%'
    exact: str = NO_DEPT_PATH  # self_dept：dept_path = exact
    owner: str = ""  # self：owner_id = owner
    in_: list[str] = field(default_factory=list)  # custom：调用方自己填，scope_of 不填（见上）
    has_dept: bool = False  # 这个人有没有部门归属；为假时 prefix/exact 是 NO_DEPT_PATH


def scope_from_claims(claims: Claims) -> ScopeFilter:
    """从一份已验签的 Claims 求 ``ScopeFilter``（纯函数，判定链第 9 步）。

    - ``dept_path`` 为空或不以 ``/`` 开头：没有部门。``prefix``/``exact``
      取 ``NO_DEPT_PATH``，``has_dept``/``all`` 为假，org 维什么都不命中，
      只剩 owner 维。格式异常的路径前缀语义不可预期，一样处理。
    - ``"/"``：整棵树的显式根标记。``all``/``has_dept`` 为真，``prefix``
      为 ``"/"``，作为普通前缀天然匹配所有真实路径（不匹配 ``dept_path``
      为空串的行）。
    - 真实路径（如 ``/1/12/``）：``has_dept`` 为真，原样进 ``prefix``/``exact``。
    """
    dept = claims.dept_path
    if not dept.startswith("/"):
        return ScopeFilter(all=False, prefix=NO_DEPT_PATH, exact=NO_DEPT_PATH, owner=claims.sub, has_dept=False)
    return ScopeFilter(all=dept == "/", prefix=dept, exact=dept, owner=claims.sub, has_dept=True)


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

    ⚠️ 取不到时**不能**返回一个默认值：取不到身份就没有 owner，任何
    默认值都是在替调用方猜一个范围（v0.5.0 之前 ``prefix`` 的零值是空串，
    会被下游 ``LIKE '' || '%'`` 解读成"放行一切"，是 fail-**open**）。
    这种调用只可能是编程错误（在 ``Module.start``/事件
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
