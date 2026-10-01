"""Config 精确匹配取值（v1 起键名即环境变量名，不做任何转换）。"""

from __future__ import annotations

from besdk.runtime import Config


def test_config_string_is_exact_match() -> None:
    c = Config({"PG_SCHEMA": "infra_print"})
    assert c.string("PG_SCHEMA") == ("infra_print", True)
    assert c.string("pgSchema") == ("", False)


def test_MustString拿不到必填项直接抛异常() -> None:
    cfg = Config({})
    try:
        cfg.must_string("DEFAULT_WAREHOUSE_ID")
        raise AssertionError("应该抛异常")
    except RuntimeError as exc:
        assert "DEFAULT_WAREHOUSE_ID" in str(exc)


def test_StringOr查不到用default() -> None:
    cfg = Config({})
    assert cfg.string_or("OTEL_BASE_URL", "") == ""
    assert cfg.string_or("OTEL_BASE_URL", "http://otel:4318") == "http://otel:4318"


def test_IntOr与BoolOr的类型转换() -> None:
    cfg = Config({"RETRY_MAX": "3", "FEATURE_ENABLED": "true"})
    assert cfg.int_or("RETRY_MAX", 0) == 3
    assert cfg.bool_or("FEATURE_ENABLED", False) is True
    # 转换失败时用 default，不抛异常——平台不校验 config 的值（导读第 5 条同类精神）
    cfg2 = Config({"RETRY_MAX": "not-a-number"})
    assert cfg2.int_or("RETRY_MAX", 5) == 5


def test_两个Config各持一份互不干扰() -> None:
    c1 = Config({"PG_SCHEMA": "erp_sales"})
    c2 = Config({"PG_SCHEMA": "erp_inventory"})
    assert c1.string_or("PG_SCHEMA", "") == "erp_sales"
    assert c2.string_or("PG_SCHEMA", "") == "erp_inventory"
