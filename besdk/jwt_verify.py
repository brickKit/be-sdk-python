"""JWT 本地验签——对应 be-sdk-go 的 jwt.go。

⚠️ ``PyJWT`` 的 ``PyJWKClient`` 是同步实现（内部用 ``urllib`` 拉
JWKS，不是 asyncio 原生）。``verify()`` 整体包一层 ``asyncio.to_thread``，
避免 JWK Set 缓存过期那一刻的网络请求把事件循环卡住——这个项目对
"Python 侧的同步/异步边界"本来就比其他语言敏感（决策 108：WSGI 同步
handler 拿不到共享 asyncpg 池是同一类物理理由，只是这里换成了网络
调用而不是数据库调用）。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime

import jwt


@dataclass
class Claims:
    """本地验签后从 JWT 里取出的身份信息（设计书 §14.1.5：JWT 只带
    身份，权限键一个都不进）。
    """

    sub: str
    roles: list[str] = field(default_factory=list)
    dept_path: str = ""
    org_id: str = ""
    issued_at: datetime = field(default_factory=lambda: datetime.fromtimestamp(0, tz=UTC))


class JWTVerifier:
    """包一层 ``jwt.PyJWKClient``——它自带 JWK Set 缓存与刷新，不需要
    自己再写一份（同 be-sdk-go 用 ``MicahParks/keyfunc`` 的理由，决策 32：
    有现成的就用现成的）。
    """

    def __init__(self, jwks_url: str) -> None:
        self._client = jwt.PyJWKClient(jwks_url, cache_jwk_set=True)

    async def verify(self, token: str) -> Claims:
        return await asyncio.to_thread(self._verify_sync, token)

    def _verify_sync(self, token: str) -> Claims:
        # ⚠️ 只认 RS256——Casdoor（`infra-iam-casdoor` 的 slot:iam 默认
        # 实现）与绝大多数 JWKS 发布方的默认算法，显式白名单防
        # "alg: none" 之类的算法混淆攻击（同 be-sdk-go 的判据）。
        signing_key = self._client.get_signing_key_from_jwt(token)
        # ⚠️ options={"require": [...]} 已经会在 sub/iat 缺失时抛
        # jwt.exceptions.MissingRequiredClaimError——不需要 decode 之后
        # 自己再手写一遍"if not sub: raise"，那是死代码（真机测试跑出来
        # 才发现：手写的 ValueError 分支永远走不到，PyJWT 自己先抛了）。
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={"require": ["sub", "iat"]},
        )
        iat = payload["iat"]
        return Claims(
            sub=payload["sub"],
            roles=list(payload.get("roles") or []),
            dept_path=payload.get("dept_path") or "",
            org_id=payload.get("org_id") or "",
            issued_at=datetime.fromtimestamp(iat, tz=UTC),
        )
