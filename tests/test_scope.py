"""数据范围过滤——对应 be-sdk-go 的 scope_test.go。"""

from __future__ import annotations

import pytest

import besdk
from besdk.jwt_verify import Claims
from besdk.scope import (
    NO_DEPT_PATH,
    ScopeFilter,
    _current_scope,
    _set_current_scope,
    scope_from_claims,
    scope_of,
)


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
    """本次实现最容易被反过来搞错方向的一处：scope_of 取不到值时如果
    返回一个默认的 ScopeFilter()，等于替调用方猜了一个范围——v0.5.0
    之前零值的 prefix 是空串，下游 SQL 会解读成"放行一切"，是 fail-open
    不是 fail-closed。必须抛异常，让编程错误现形。
    """
    with pytest.raises(RuntimeError, match="require_permission"):
        scope_of()


# ── R60：没分部门的人 org 维落空，只剩本人 ─────────────────────────


def test_dept_path为空时org维落空只剩本人() -> None:
    """authz 对没分部门的人签 ``dept_path = ""``。旧实现把它当成"不限"：
    ``prefix == ""`` 让 ``LIKE '' || '%'`` 匹配所有行。现在 prefix/exact
    取一个任何真实路径都不会以它开头的哨兵，owner 维照旧。
    """
    f = scope_from_claims(Claims(sub="u_x", dept_path=""))

    assert f.all is False
    assert f.has_dept is False
    assert f.prefix == NO_DEPT_PATH
    assert f.exact == NO_DEPT_PATH
    assert f.owner == "u_x"
    # 哨兵在前缀匹配里落空：真实路径、空串路径都不以它开头
    assert not "/1/12/".startswith(f.prefix)
    assert not "".startswith(f.prefix)


def test_不以斜杠开头的dept_path按无部门处理() -> None:
    """格式异常的路径前缀语义不可预期（``"1/12/"`` 会匹配 ``"1/123/"``
    之类的东西），一律当成没有部门。
    """
    f = scope_from_claims(Claims(sub="u_x", dept_path="1/12/"))

    assert f.all is False
    assert f.has_dept is False
    assert f.prefix == NO_DEPT_PATH
    assert f.exact == NO_DEPT_PATH
    assert f.owner == "u_x"


def test_真实部门路径原样进prefix和exact() -> None:
    f = scope_from_claims(Claims(sub="u_zhangsan", dept_path="/1/12/"))

    assert f.all is False
    assert f.has_dept is True
    assert f.prefix == "/1/12/"
    assert f.exact == "/1/12/"
    assert f.owner == "u_zhangsan"


def test_NO_DEPT_PATH不以斜杠开头且不含LIKE通配符() -> None:
    """哨兵会被下游直接绑进 ``LIKE $n || '%'``：以 ``/`` 开头就会命中
    真实路径，含 ``%`` / ``_`` 就会被 LIKE 当成通配符。值本身也是三份
    SDK 共用的协议常量。
    """
    assert NO_DEPT_PATH == "!no-dept"
    assert NO_DEPT_PATH != ""
    assert not NO_DEPT_PATH.startswith("/")
    assert "%" not in NO_DEPT_PATH
    assert "_" not in NO_DEPT_PATH
    assert besdk.NO_DEPT_PATH == NO_DEPT_PATH
    assert besdk.scope_from_claims is scope_from_claims
