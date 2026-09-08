from __future__ import annotations

import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from besdk.jwt_verify import JWTVerifier
from tests.helpers import FakeJWKSServer


@pytest.fixture
def jwks() -> FakeJWKSServer:
    srv = FakeJWKSServer()
    yield srv
    srv.close()


async def test_验签合法token拿到正确claims(jwks: FakeJWKSServer) -> None:
    v = JWTVerifier(jwks.url)
    token = jwks.sign("u_zhangsan", roles=["sales_manager"], dept_path="/root/china/east", org_id="org1")

    claims = await v.verify(token)

    assert claims.sub == "u_zhangsan"
    assert claims.dept_path == "/root/china/east"
    assert claims.org_id == "org1"
    assert claims.roles == ["sales_manager"]


async def test_签名被篡改会验签失败(jwks: FakeJWKSServer) -> None:
    """最基本的安全断言：换一把不相关的私钥签同样的 payload，验签必须失败。"""
    v = JWTVerifier(jwks.url)
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    forged = jwt.encode(
        {"sub": "u_attacker", "iat": int(time.time())},
        other_key,
        algorithm="RS256",
        headers={"kid": jwks.kid},  # 冒充已知的 kid，但用的是另一把私钥
    )

    with pytest.raises(Exception, match=".*"):
        await v.verify(forged)


async def test_algNone攻击被拒绝(jwks: FakeJWKSServer) -> None:
    v = JWTVerifier(jwks.url)
    unsigned = jwt.encode({"sub": "u_attacker", "iat": int(time.time())}, key="", algorithm="none")

    with pytest.raises(Exception, match=".*"):
        await v.verify(unsigned)


async def test_缺少sub报错(jwks: FakeJWKSServer) -> None:
    """⚠️ 不是自己手写的校验在报错——PyJWT 的 options={"require": [...]}
    在 sub/iat 缺失时会先抛 MissingRequiredClaimError（真机测试跑出来
    才发现：本来在 jwt_verify.py 里手写了一遍同样的检查，从没被走到过，
    是死代码，已删除，见该文件的注释）。"""
    v = JWTVerifier(jwks.url)
    priv = jwks._priv  # noqa: SLF001 - 测试内部直接复用同一把私钥，制造缺字段的 token
    token = jwt.encode({"iat": int(time.time())}, priv, algorithm="RS256", headers={"kid": jwks.kid})

    with pytest.raises(jwt.exceptions.MissingRequiredClaimError, match="sub"):
        await v.verify(token)
