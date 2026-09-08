"""endpoint() 剥 scheme + 二值返回——对应 be-sdk-go 的 endpoint_test.go。"""

from __future__ import annotations

import pytest

from besdk.endpoint import endpoint, must_endpoint, storage_endpoint


def test_剥掉http前缀(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MDM_CUSTOMER_ENDPOINT", "http://mdm-customer-1-0-1:8080")
    v, ok = endpoint("mdm/customer")
    assert ok is True
    assert v == "mdm-customer-1-0-1:8080"


def test_额外端口的变量名带extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "INTEGRATION_IM_DINGTALK_GRPC_ENDPOINT", "http://integration-im-dingtalk-1-0-0:9207"
    )
    v, ok = endpoint("integration/im-dingtalk", "grpc")
    assert ok is True
    assert v == "integration-im-dingtalk-1-0-0:9207"


def test_弱依赖缺失时键根本不存在不是空串(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INFRA_WORKFLOW_ENDPOINT", raising=False)
    v, ok = endpoint("infra/workflow")
    assert ok is False
    assert v == ""


def test_MustEndpoint缺失时抛异常(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MDM_PRODUCT_ENDPOINT", raising=False)
    with pytest.raises(RuntimeError, match="mdm/product"):
        must_endpoint("mdm/product")


def test_storage_endpoint方向相反_加scheme而不是剥(monkeypatch: pytest.MonkeyPatch) -> None:
    # ⚠️ STORAGE_ENDPOINT 是唯一一个名字带 ENDPOINT、值却是裸 host:port
    # 的变量（导读第 12 条）——这条测试锁死方向不会被改反。
    monkeypatch.setenv("STORAGE_ENDPOINT", "rustfs:9000")
    v, ok = storage_endpoint(secure=False)
    assert ok is True
    assert v == "http://rustfs:9000"

    v_secure, _ = storage_endpoint(secure=True)
    assert v_secure == "https://rustfs:9000"
