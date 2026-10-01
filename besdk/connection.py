"""统一连接键 → 驱动连接串。v1 起平台不再注入 DATABASE_*/MQ_*，连接信息是组件
configSchema 里的普通配置项（docs/conventions/configuration.md）。"""
from __future__ import annotations

import urllib.parse

from besdk.runtime import Config


def pg_dsn(cfg: Config) -> str:
    missing: list[str] = []

    def need(k: str) -> str:
        v, ok = cfg.string(k)
        if not ok or v == "":
            missing.append(k)
        return v

    host, port, db, user = need("PG_HOST"), need("PG_PORT"), need("PG_DATABASE"), need("PG_USER")
    pw, ok = cfg.string("PG_PASSWORD")
    if not ok:
        missing.append("PG_PASSWORD")
    if missing:
        raise ValueError("缺少数据库连接配置：" + ", ".join(missing))
    q = urllib.parse.quote
    return f"postgresql://{q(user, safe='')}:{q(pw, safe='')}@{host}:{port}/{q(db, safe='')}"


def nats_url(cfg: Config) -> str:
    v, ok = cfg.string("NATS_URL")
    if not ok or not v:
        raise ValueError("缺少 NATS_URL")
    return v
