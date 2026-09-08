"""测试夹具：真实的本地 JWKS/bundle 服务器——不是 mock。

``infra-iam-casdoor`` 要到阶段三 Task 7 才建仓库，现在没有真实签发方
可用；这里的加密运算是真实的（真 RSA 密钥对、真 JWKS 端点、真 RS256
签名与验签，都跑在一个真实绑定端口的 ``http.server``），只是身份是
测试用的，不是"假装验证成功"的 mock。
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm


class FakeJWKSServer:
    def __init__(self) -> None:
        self._priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.kid = "test-key-1"
        jwk = RSAAlgorithm.to_jwk(self._priv.public_key(), as_dict=True)
        jwk["kid"] = self.kid
        jwk["use"] = "sig"
        jwk["alg"] = "RS256"
        body = json.dumps({"keys": [jwk]}).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - http.server 的约定命名
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:  # 静默测试输出
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/.well-known/jwks.json"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def sign(
        self,
        sub: str,
        roles: list[str] | None = None,
        dept_path: str = "",
        org_id: str = "",
        issued_at: float | None = None,
    ) -> str:
        iat = issued_at if issued_at is not None else time.time()
        payload: dict[str, Any] = {
            "sub": sub,
            "iat": int(iat),
            "exp": int(iat) + 600,
            "roles": roles or [],
            "dept_path": dept_path,
            "org_id": org_id,
        }
        return jwt.encode(payload, self._priv, algorithm="RS256", headers={"kid": self.kid})


class FakeBundleServer:
    """可以在运行中改内容的假 ``/authz/bundle``——模拟"角色分配变了"
    这个真实场景，同时校验 ``If-None-Match`` 有没有真的被发送。
    """

    def __init__(self) -> None:
        self._body: dict[str, Any] = {"roles": {}, "stale_since": {}}
        self._etag = '"v1"'
        self.not_match_count = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.headers.get("If-None-Match") == outer._etag:  # noqa: SLF001
                    outer.not_match_count += 1
                    self.send_response(304)
                    self.send_header("ETag", outer._etag)  # noqa: SLF001
                    self.end_headers()
                    return
                body = json.dumps(outer._body).encode()  # noqa: SLF001
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("ETag", outer._etag)  # noqa: SLF001
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/authz/bundle"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def set_bundle(self, roles: dict[str, list[str]], stale_since: dict[str, int], etag: str) -> None:
        self._body = {"roles": roles, "stale_since": stale_since or {}}
        self._etag = etag
