"""Module——对应 be-sdk-go 的 module.go。

模块交回去的一切。模块自己不 Listen、不注册全局、不装信号处理器
（设计书 §12.5、§13.3 铁律七）。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI
    from grpc.aio import Server


@dataclass
class Module:
    asgi_app: "FastAPI"  # ⭐ 外壳对 FastAPI 完全无感，它只 serve 一个 ASGI app
    register_grpc: Callable[["Server"], None] | None = None
    migrations_dir: Path | None = None  # 外壳按拓扑顺序跑（§13.3 铁律五）
    start: Callable[[], Awaitable[None]] | None = None  # 后台循环，取消时必须返回
    stop: Callable[[], Awaitable[None]] | None = None
