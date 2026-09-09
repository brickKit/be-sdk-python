"""new_registry——对应 be-sdk-go 的 metrics_test.go。"""

from __future__ import annotations

from prometheus_client import Counter

from besdk.metrics import new_registry


def test_每次调用返回独立的registry不是默认全局的() -> None:
    """核心场景：合并态下 N 个模块各自调一次，互不冲突——同一个指标名
    在两个不同的 Registry 里各注册一次不该报 ``Duplicated timeseries``。
    """
    r1 = new_registry()
    r2 = new_registry()
    assert r1 is not r2

    Counter("same_metric_name_total", "测试用", registry=r1)
    # 如果 new_registry() 退化成返回默认全局 registry，这一行会因为
    # "same_metric_name_total" 已经注册过而抛 ValueError。
    Counter("same_metric_name_total", "测试用", registry=r2)
