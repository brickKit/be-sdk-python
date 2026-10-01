"""读自己的 component.yaml 拿端口——对应 be-sdk-go 的 manifest.go。

⚠️ 端口不是平台注入的（§13.8.1）：环境变量表里只有"别人在哪"
（``*_ENDPOINT``），没有"我该监听哪"。全拆态下唯一权威来源是组件自己的
``component.yaml``——镜像里必须把它跟解释器/代码放在一起。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class OwnPorts:
    http_port: int
    extra_ports: dict[str, int] = field(default_factory=dict)


def load_own_ports(path: str | Path = "component.yaml") -> OwnPorts:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    deployment = data.get("deployment", {})
    extra_ports = {p["name"]: p["port"] for p in deployment.get("extraPorts", [])}
    return OwnPorts(http_port=deployment["port"], extra_ports=extra_ports)


def load_own_http_port(path: str | Path = "component.yaml") -> int:
    """只读自己的 ``deployment.port``（对应 be-sdk-go 的 ``LoadOwnHTTPPort``）。
    文件缺失、字段缺失/非整数/不在 1..65535（含 0）都响亮报错，不退化成默认端口。"""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except OSError as exc:
        raise RuntimeError(f"读自己的 component.yaml 失败：{exc}") from exc
    port = (data.get("deployment") or {}).get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise RuntimeError(f"{path} 的 deployment.port 缺失或非法（{port!r}）：外壳 /healthz 必须监听这个端口")
    return port
