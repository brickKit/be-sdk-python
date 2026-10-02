"""数据范围过滤——对应 be-sdk-go 的 scope_test.go。"""

from __future__ import annotations

import pytest

from besdk.jwt_verify import Claims
from besdk.scope import ScopeFilter, _current_scope, _set_current_scope, scope_from_claims, scope_of


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


def test_斜杠是整棵树的显式根标记() -> None:
    """看整棵部门树的显式标记是 ``"/"``，不是空串：authz 签发的真实路径
    恒以 ``/`` 开头（根部门也是 ``/<根id>/``），``"/"`` 作为普通前缀天然
    覆盖所有真实路径。空 dept_path 只表示"没分部门"，不再是根节点。
    """
    f = scope_from_claims(Claims(sub="u_ceo", dept_path="/"))

    assert f.all is True
    assert f.has_dept is True
    assert f.prefix == "/"
    assert "/1/12/".startswith(f.prefix)


def test_没有设置过ScopeFilter时抛异常() -> None:
    """本次实现最容易被反过来搞错方向的一处：§14.2.4 的 SQL 约定是
    "空字符串表示不限"，如果 scope_of 在取不到值时返回默认的
    ScopeFilter()，等于把"取不到身份"解读成"放行一切"——方向反了，是
    fail-open 不是 fail-closed。必须抛异常，不能安静地返回一个看似
    收紧、实际在下游 SQL 里被解读成不限的值。
    """
    with pytest.raises(RuntimeError, match="require_permission"):
        scope_of()
