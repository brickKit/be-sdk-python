"""数据范围过滤——对应 be-sdk-go 的 scope_test.go。"""

from __future__ import annotations

import pytest

from besdk.scope import ScopeFilter, _current_scope, _set_current_scope, scope_of


@pytest.fixture(autouse=True)
def _reset_scope():
    token = _current_scope.set(None)
    yield
    _current_scope.reset(token)


def test_按JWT字段填三个可选字段() -> None:
    """§14.2.4 的核心断言：五档不是 scope_of 自己判断出来的，
    prefix/exact/owner 始终从同一份 Claims 填，调用方（某条 SQL 查询）
    自己决定用哪一个。
    """
    _set_current_scope(ScopeFilter(all=False, prefix="/root/china/east/sh-sales", exact="/root/china/east/sh-sales", owner="u_zhangsan"))

    f = scope_of()

    assert f.all is False
    assert f.prefix == "/root/china/east/sh-sales"
    assert f.exact == "/root/china/east/sh-sales"
    assert f.owner == "u_zhangsan"


def test_部门树根节点自然得到All() -> None:
    """"五档退化成纯函数"这条设计的直接验证：坐在根部门（dept_path 为
    空）的人，前缀匹配天然覆盖全部，不需要任何特判分支。
    """
    _set_current_scope(ScopeFilter(all=True, prefix="", exact="", owner="u_ceo"))

    f = scope_of()

    assert f.all is True


def test_没有设置过ScopeFilter时抛异常() -> None:
    """本次实现最容易被反过来搞错方向的一处：§14.2.4 的 SQL 约定是
    "空字符串表示不限"，如果 scope_of 在取不到值时返回默认的
    ScopeFilter()，等于把"取不到身份"解读成"放行一切"——方向反了，是
    fail-open 不是 fail-closed。必须抛异常，不能安静地返回一个看似
    收紧、实际在下游 SQL 里被解读成不限的值。
    """
    with pytest.raises(RuntimeError, match="require_permission"):
        scope_of()
