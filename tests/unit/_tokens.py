"""A small fake identity provider for tests: keys, a JWKS document and a token signer."""
import json
import time
import uuid

import jwt
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from jwt.algorithms import ECAlgorithm, RSAAlgorithm

ISSUER = "urn:be:t1:iam"
TENANT = "t1"


class FakeIAM:
    def __init__(self):
        self.keys = {"k-rsa": ("RS256", rsa.generate_private_key(public_exponent=65537, key_size=2048)),
                     "k-ec": ("ES256", ec.generate_private_key(ec.SECP256R1()))}
        self.fetches = 0

    def jwks(self) -> dict:
        out = []
        for kid, (alg, key) in self.keys.items():
            algo = RSAAlgorithm if alg == "RS256" else ECAlgorithm
            jwk = json.loads(algo.to_jwk(key.public_key()))
            jwk.update(kid=kid, alg=alg, use="sig")
            out.append(jwk)
        return {"keys": out}

    def token(self, kid="k-rsa", headers=None, **claims) -> str:
        alg, key = self.keys[kid]
        now = int(time.time())
        body = {"iss": ISSUER, "aud": TENANT, "sub": "u_me", "typ": "access", "iat": now, "exp": now + 600,
                "jti": str(uuid.uuid4()), "roles": ["rep"], "dept_path": "/1/12/", "tenant_id": TENANT}
        body.update(claims)
        body = {k: v for k, v in body.items() if v is not None}
        return jwt.encode(body, key, algorithm=alg, headers={"kid": kid, **(headers or {})})

    def transport(self):
        import httpx

        def handle(req: httpx.Request) -> httpx.Response:
            if req.url.path == "/.well-known/jwks.json":
                self.fetches += 1
                return httpx.Response(200, json=self.jwks())
            return httpx.Response(404)

        return httpx.MockTransport(handle)
