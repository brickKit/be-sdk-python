"""Config 的 camelCase 查询——阶段三计划 Task 1 明确要求的回归测试。

⚠️ 这条测试是照抄 be-sdk-go 的教训写的：``Config`` 从 v0.1.0 到 v0.1.8
都没有把 camelCase 配置键转成平台真实注入的 SCREAMING_SNAKE_CASE，四个
已发布组件因为默认值恰好等于真实值而未暴露五个版本。**be-sdk-python
从第一次提交起就要有这条测试**，不重犯同一个坑。
"""

from __future__ import annotations

from besdk.runtime import Config, _config_env_var_name


def test_camelCase查询能找到平台真实注入的环境变量() -> None:
    # 模拟平台真实的注入行为：configSchema 写的是 camelCase（pgSchema），
    # 而平台装配阶段把它转成 SCREAMING_SNAKE_CASE 才注入进程环境变量。
    cfg = Config({"PG_SCHEMA": "erp_sales"})

    v, ok = cfg.string("pgSchema")

    assert ok is True
    assert v == "erp_sales"


def test_camelCase转换算法与Go版逐字对应() -> None:
    cases = {
        "pgSchema": "PG_SCHEMA",
        "otelBaseUrl": "OTEL_BASE_URL",
        "defaultWarehouseId": "DEFAULT_WAREHOUSE_ID",
        "enabledComponents": "ENABLED_COMPONENTS",
        "iamJwksUrl": "IAM_JWKS_URL",
    }
    for key, want in cases.items():
        assert _config_env_var_name(key) == want


def test_已经是SCREAMING_SNAKE_CASE的输入是幂等的() -> None:
    # 下划线本身既非大写也非小写也非数字，不会被误判成词边界。
    assert _config_env_var_name("PG_SCHEMA") == "PG_SCHEMA"
    assert _config_env_var_name("DEFAULT_WAREHOUSE_ID") == "DEFAULT_WAREHOUSE_ID"


def test_MustString拿不到必填项直接抛异常() -> None:
    cfg = Config({})
    try:
        cfg.must_string("defaultWarehouseId")
        raise AssertionError("应该抛异常")
    except RuntimeError as exc:
        assert "defaultWarehouseId" in str(exc)


def test_StringOr查不到用default() -> None:
    cfg = Config({})
    assert cfg.string_or("otelBaseUrl", "") == ""
    assert cfg.string_or("otelBaseUrl", "http://otel:4318") == "http://otel:4318"


def test_IntOr与BoolOr的类型转换() -> None:
    cfg = Config({"RETRY_MAX": "3", "FEATURE_ENABLED": "true"})
    assert cfg.int_or("retryMax", 0) == 3
    assert cfg.bool_or("featureEnabled", False) is True
    # 转换失败时用 default，不抛异常——平台不校验 config 的值（导读第 5 条同类精神）
    cfg2 = Config({"RETRY_MAX": "not-a-number"})
    assert cfg2.int_or("retryMax", 5) == 5


def test_两个Config各持一份互不干扰() -> None:
    c1 = Config({"PG_SCHEMA": "erp_sales"})
    c2 = Config({"PG_SCHEMA": "erp_inventory"})
    assert c1.string_or("pgSchema", "") == "erp_sales"
    assert c2.string_or("pgSchema", "") == "erp_inventory"
