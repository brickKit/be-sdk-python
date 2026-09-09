"""每模块一个 Prometheus Registry——对应 be-sdk-go 的 metrics.go。"""

from __future__ import annotations

from prometheus_client import CollectorRegistry


def new_registry() -> CollectorRegistry:
    """给每个模块建一个独立的 Prometheus Registry（不是默认全局那个）。

    用默认全局 registry 的症状：Go ``MustRegister`` panic、Python 抛
    ``Duplicated timeseries``——单跑 100% 正常，进外壳第二个模块起来就崩
    （设计书 §12.5.2）。``run_standalone`` 调用它填 ``Runtime.registry``，
    恰好一次。
    """
    return CollectorRegistry()
