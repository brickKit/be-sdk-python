"""with_tx——对应 be-sdk-go 的 tx_test.go。需要真实 PostgreSQL，
未设置 TEST_PG_DSN 时跳过（不是放宽断言，CI 里必须设）。
"""

from __future__ import annotations

import os

import asyncpg
import pytest

from besdk.tx import with_tx


def _dsn() -> str:
    dsn = os.environ.get("TEST_PG_DSN")
    if not dsn:
        pytest.skip("未设置 TEST_PG_DSN，跳过（CI 里必须设）")
    return dsn


@pytest.mark.asyncio
async def test_非法role名被拒绝() -> None:
    pool = await asyncpg.create_pool(_dsn())
    try:
        with pytest.raises(ValueError, match="role"):
            await with_tx(pool, "erp_sales_rw; DROP TABLE x", "erp_sales", lambda c: None)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_非法schema名被拒绝() -> None:
    pool = await asyncpg.create_pool(_dsn())
    try:
        with pytest.raises(ValueError, match="schema"):
            await with_tx(pool, "postgres", "erp_sales; --", lambda c: None)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_with_tx内部真的切到了指定的schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠️ 验证 with_tx 真的把 search_path 切过去了（正面场景，不是只测
    "没抛异常"）。
    """
    pool = await asyncpg.create_pool(_dsn())
    try:

        async def _read_search_path(conn: asyncpg.Connection) -> str:
            return await conn.fetchval("SELECT current_setting('search_path')")

        search_path_inside = await with_tx(pool, "postgres", "public", _read_search_path)
        assert search_path_inside == "public, public_archive"
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_with_tx发的语句真的带LOCAL_不是普通SET(monkeypatch: pytest.MonkeyPatch) -> None:
    """⚠️⚠️ 这条测试锁的是导读第 2 条那个坑——但要先说清一件真机验证过的事：
    ``asyncpg.Pool`` 在 ``release()`` 时会自动 ``RESET ALL``（见 tx.py 的
    模块内详细说明），这与 Go ``database/sql`` 的池**不一样**（那边不做
    任何自动重置）。所以"改回不带 LOCAL 的 SET、看下一个借用者是否读到
    脏值"这个断言方式在 asyncpg 下**测不出来**——真机试过，池的自动重置
    会把两种写法的外部可观察结果拉平，让这条本该失败的测试误报通过
    （曾经真的这样改坏过 tx.py 来验证，测试仍然绿）。

    正确的断法是**直接检查 with_tx 发给数据库的语句本身**：在类级别
    monkeypatch ``asyncpg.Connection.execute``，真的调用 ``with_tx``，
    断言它内部真的发了 ``SET LOCAL ROLE``/``SET LOCAL search_path``，
    不是退化成不带 LOCAL 的版本。这是本仓库第一次遇到"两种语言的连接池
    语义不同，导致同一种黑盒测试方法在一边有效、另一边失效"——记进
    `docs/dev/实测踩坑记录.md` 类别 A（第三方库的实际行为与直觉不一致）。
    """
    executed: list[str] = []
    real_execute = asyncpg.Connection.execute

    async def _spy_execute(self: asyncpg.Connection, query: str, *args: object, **kwargs: object) -> object:
        executed.append(query)
        return await real_execute(self, query, *args, **kwargs)

    monkeypatch.setattr(asyncpg.Connection, "execute", _spy_execute)

    pool = await asyncpg.create_pool(_dsn())
    try:

        async def _noop(conn: asyncpg.Connection) -> None:
            return None

        await with_tx(pool, "postgres", "public", _noop)
    finally:
        await pool.close()

    assert any(q.startswith("SET LOCAL ROLE") for q in executed), executed
    assert any(q.startswith("SET LOCAL search_path") for q in executed), executed
    assert not any(
        q.startswith("SET ROLE") or q.startswith("SET search_path") for q in executed
    ), "退化成了不带 LOCAL 的 SET，导读第 2 条的坑重现"
