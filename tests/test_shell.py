"""外壳级构件——对应 be-sdk-go 的 shell_test.go。"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

import besdk
import besdk.authz as authz_module
from besdk.jwt_verify import JWTVerifier
from besdk.shell import ShellModuleConfig
from tests.helpers import FakeJWKSServer


def test_new_shell_runtime_多模块共享DB与NATS但各自持有独立字段() -> None:
    """阶段四外壳工程最核心的一条断言：设计书 §13.3 铁律二要求一个外壳
    一个连接池，模块通过 SET LOCAL ROLE 切身份而不是各自开池——这里验证
    new_shell_runtime 传入同一个 db/nc 时，两个模块的 Runtime 确实拿到
    同一个对象（池真的被共享），而 component_id/config/logger/端口这些
    逐模块的字段确实各自独立。
    """
    shared_db = object()
    shared_nc = object()

    rt1 = besdk.new_shell_runtime(
        ShellModuleConfig(
            component_id="mdm/customer",
            component_version="1.0.5",
            env={"FOO_BAR": "customer-value"},
            http_port=8080,
            extra_ports={"grpc": 9090},
        ),
        shared_db,  # type: ignore[arg-type]
        shared_nc,  # type: ignore[arg-type]
    )
    rt2 = besdk.new_shell_runtime(
        ShellModuleConfig(
            component_id="erp/sales",
            component_version="1.0.19",
            env={"FOO_BAR": "sales-value"},
            http_port=8084,
            extra_ports={"grpc": 9094},
        ),
        shared_db,  # type: ignore[arg-type]
        shared_nc,  # type: ignore[arg-type]
    )

    assert rt1.db is rt2.db
    assert rt1.nats is rt2.nats

    assert rt1.component_id == "mdm/customer"
    assert rt2.component_id == "erp/sales"
    assert rt1.http_port == 8080
    assert rt2.http_port == 8084
    assert rt1.extra_ports == {"grpc": 9090}
    assert rt2.extra_ports == {"grpc": 9094}

    assert rt1.config.string("fooBar") == ("customer-value", True)
    assert rt2.config.string("fooBar") == ("sales-value", True)

    assert rt1.registry is not rt2.registry


@pytest.fixture
def _restore_authz_runtime() -> Iterator[None]:
    """同 test_authz.py 的既有判据：_verifier/_bundle 是模块级全局状态，
    测试之间不这样隔离会互相污染。
    """
    prev_verifier, prev_bundle = authz_module._verifier, authz_module._bundle  # noqa: SLF001
    yield
    authz_module._verifier, authz_module._bundle = prev_verifier, prev_bundle  # noqa: SLF001


def test_init_shell_authz_只需调一次不需要每个模块各调(
    _restore_authz_runtime: None,
) -> None:
    """用一个哨兵状态预置 _verifier/_bundle，验证 init_shell_authz 真的
    调用到了 _set_authz_runtime（不是签名对但函数体是空的）——两项配置
    都缺失时 setup_authz_runtime 按既有判据返回 (None, None)，调完之后
    哨兵应该被换成 None，证明确实执行到底，不是被短路跳过。
    """
    authz_module._verifier = object()  # type: ignore[assignment]  # noqa: SLF001
    authz_module._bundle = object()  # type: ignore[assignment]  # noqa: SLF001

    besdk.init_shell_authz("", "", logging.getLogger("test"))

    assert authz_module._verifier is None  # noqa: SLF001
    assert authz_module._bundle is None  # noqa: SLF001


def test_init_shell_authz_配了jwks真的装配验签器(
    _restore_authz_runtime: None,
) -> None:
    """与上一条互补：确认真的配了 iam_jwks_url 时 init_shell_authz 也能
    走通真实装配路径，不是只测得出"空配置退化成 None"这一半。
    """
    srv = FakeJWKSServer()
    try:
        besdk.init_shell_authz(srv.url, "", logging.getLogger("test"))
        assert isinstance(authz_module._verifier, JWTVerifier)  # noqa: SLF001
    finally:
        srv.close()


def test_serve_http_导出别名与私有实现一致() -> None:
    assert besdk.serve_http is besdk.shell._serve_http  # type: ignore[attr-defined]  # noqa: SLF001


def test_serve_extra_port_导出别名与私有实现一致() -> None:
    assert besdk.serve_extra_port is besdk.shell._serve_extra_port  # type: ignore[attr-defined]  # noqa: SLF001


def test_build_pg_dsn与build_nats_url_导出别名与私有实现一致() -> None:
    assert besdk.build_pg_dsn is besdk.shell._build_pg_dsn  # type: ignore[attr-defined]  # noqa: SLF001
    assert besdk.build_nats_url is besdk.shell._build_nats_url  # type: ignore[attr-defined]  # noqa: SLF001
