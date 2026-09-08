"""组件地址解析——对应 be-sdk-go 的 endpoint.go，逻辑逐字对应，不许分叉。"""

from __future__ import annotations

import os
import re

_NON_ALNUM = re.compile(r"[/-]")


def _env_name(dep: str, extra: str) -> str:
    """把组件 ID 推导成平台注入的变量名前缀。

    ⚠️ 规则必须与平台的 manifest.EnvPrefix 逐字一致：只把 / 与 - 换成 _，
    然后全大写。不要多加任何替换规则——组件 ID 的正则里不允许出现点号，
    多写一条会让读的人以为 ID 里可能有点号。

        "mdm/customer" + ""                 -> "MDM_CUSTOMER_ENDPOINT"
        "integration/im-dingtalk" + "grpc"  -> "INTEGRATION_IM_DINGTALK_GRPC_ENDPOINT"
    """
    p = _NON_ALNUM.sub("_", dep).upper()
    if not extra:
        return f"{p}_ENDPOINT"
    return f"{p}_{extra.upper()}_ENDPOINT"


def endpoint(dep: str, extra: str = "") -> tuple[str, bool]:
    """读 ``*_ENDPOINT`` 并剥掉 scheme。

    ⚠️ 平台注入的值恒为 ``http://`` 开头，额外端口也一样——没有 ``grpc://``
    这种东西。直接把带 scheme 的值传给 gRPC 客户端会连不上，而报错信息
    指向名称解析，非常难联想到是这里。所以剥 scheme 这件事全项目只写在
    这一个函数里（导读第 1 条）。

    ⚠️ 必须用 ``os.environ`` 的成员测试而不是 ``os.environ.get()`` 后判空：
    弱依赖缺失时那个变量根本不存在，不是空字符串——这是平台刻意的设计
    （§3.6）。返回二值 ``(value, ok)``，调用方必须显式判断 ``ok``。
    """
    v = os.environ.get(_env_name(dep, extra))
    if not v:
        return "", False
    v = v.removeprefix("http://").removeprefix("https://")
    return v.rstrip("/"), True


def must_endpoint(dep: str, extra: str = "") -> str:
    """用于强依赖：缺失即抛异常（强依赖缺失时平台本来就会阻断启动）。"""
    v, ok = endpoint(dep, extra)
    if not ok:
        raise RuntimeError(f"强依赖 {dep} 的 {_env_name(dep, extra)} 未注入")
    return v


def storage_endpoint(secure: bool = False) -> tuple[str, bool]:
    """读 ``STORAGE_ENDPOINT`` 并**加上** scheme，返回完整 URL。

    ⚠️ 与 :func:`endpoint` 方向相反。``STORAGE_ENDPOINT`` 是平台注入的资源
    变量，值是裸 ``host:port``；而 S3 SDK 要一个完整 URL。两个函数必须
    分开——共用一个必然有一边错（导读第 12 条）。
    """
    v = os.environ.get("STORAGE_ENDPOINT")
    if not v:
        return "", False
    scheme = "https://" if secure else "http://"
    return scheme + v, True
