"""Access-token verification against the identity provider's JWKS (be-protocol P5).

``Verifier`` is process-wide in a shell (P19.3): every member shares the JWKS cache. Verification runs
in a worker thread (PyJWT is synchronous). Every failure is ``401 TOKEN_INVALID``; the parser's own
message goes to the log only.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Mapping

import httpx
import jwt
from jwt.algorithms import ECAlgorithm, OKPAlgorithm, RSAAlgorithm

from besdk.errors import Error, be_error

ALLOWED_ALGS = ("RS256", "ES256", "EdDSA")
SKEW = 60
FETCH_TIMEOUT = 3.0
MAX_AGE = 3600.0
REFETCH_GAP = 30.0
_STRING_CLAIMS = ("dept_path", "dg", "azp", "locale", "tenant_id", "org_id")
_LIST_CLAIMS = ("roles", "ceil")


def _invalid(why: str) -> Error:
    err = be_error("TOKEN_INVALID")
    err.internal_message = why
    return err


def _alg_of(jwk: Mapping[str, Any]) -> str | None:
    if jwk.get("alg"):
        return jwk["alg"]
    return {("RSA", None): "RS256", ("EC", "P-256"): "ES256", ("OKP", "Ed25519"): "EdDSA"}.get(
        (jwk.get("kty"), jwk.get("crv")))


class JwksCache:
    """``{IAM_URL}/.well-known/jwks.json``: ≤ 1 h cache, one refetch per unknown kid at most every 30 s,
    3 s per fetch, fail-static (P5.4)."""

    def __init__(self, iam_url: str, client: httpx.AsyncClient, *, max_age: float = MAX_AGE):
        self.url = iam_url.rstrip("/") + "/.well-known/jwks.json"
        self.client = client
        self.max_age = max_age
        self._keys: dict[str, tuple[str, Any]] = {}
        self._fetched = 0.0
        self._last_refetch = -REFETCH_GAP  # refetches caused by an unknown kid
        self._lock = asyncio.Lock()

    async def _fetch(self) -> None:
        try:
            r = await self.client.get(self.url, timeout=FETCH_TIMEOUT)
            r.raise_for_status()
            keys = {}
            for jwk in r.json().get("keys", ()):
                alg = _alg_of(jwk)
                if jwk.get("kid") and alg in ALLOWED_ALGS:
                    algo = {"RS256": RSAAlgorithm, "ES256": ECAlgorithm, "EdDSA": OKPAlgorithm}[alg]
                    keys[jwk["kid"]] = (alg, algo.from_jwk(json.dumps(jwk)))
            self._keys, self._fetched = keys, time.monotonic()
        except (httpx.HTTPError, ValueError, KeyError):
            pass  # fail-static: keep what we hold

    async def key(self, kid: str) -> tuple[str, Any] | None:
        async with self._lock:
            now = time.monotonic()
            if not self._fetched or now - self._fetched > self.max_age:
                await self._fetch()
            elif kid not in self._keys and now - self._last_refetch >= REFETCH_GAP:
                self._last_refetch = now
                await self._fetch()
            return self._keys.get(kid)

    @property
    def loaded(self) -> bool:
        return bool(self._keys)


class Verifier:
    def __init__(self, *, issuer: str, tenant: str, jwks: JwksCache):
        self.issuer, self.tenant, self.jwks = issuer, tenant, jwks

    async def verify(self, token: str) -> dict[str, Any]:
        """Verify a compact JWS access token; returns its claims or raises 401 ``TOKEN_INVALID``."""
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as e:
            raise _invalid(f"malformed token: {e}") from None
        alg, kid = header.get("alg"), header.get("kid")
        if alg not in ALLOWED_ALGS or not isinstance(kid, str) or not kid:
            raise _invalid(f"alg {alg!r} or kid {kid!r} refused")
        found = await self.jwks.key(kid)
        if found is None or found[0] != alg:
            raise _invalid(f"no key {kid!r} for {alg}")
        try:
            claims = await asyncio.to_thread(
                jwt.decode, token, found[1], algorithms=[alg], audience=self.tenant, issuer=self.issuer,
                leeway=SKEW, options={"require": ["exp", "iat", "sub", "jti", "iss", "aud"]})
        except jwt.PyJWTError as e:
            raise _invalid(str(e)) from None
        _check_claims(claims)
        return claims


def _check_actor(act: Any) -> None:
    while act is not None:
        if not isinstance(act, dict) or not isinstance(act.get("sub"), str) or not act["sub"] \
                or act.get("kind") not in ("user", "agent", "svc"):
            raise _invalid("act is malformed")
        act = act.get("act")


def _check_claims(c: Mapping[str, Any]) -> None:
    """P5.3, P5.9: typ, non-empty sub and jti, and the JSON type of every claim the runtime reads."""
    if c.get("typ") != "access":
        raise _invalid(f"typ {c.get('typ')!r}")
    for name in ("sub", "jti"):
        if not isinstance(c.get(name), str) or not c[name]:
            raise _invalid(f"{name} missing")
    for name in _STRING_CLAIMS:
        if name in c and not isinstance(c[name], str):
            raise _invalid(f"{name} is not a string")
    for name in _LIST_CLAIMS:
        if name in c and not (isinstance(c[name], list) and all(isinstance(x, str) for x in c[name])):
            raise _invalid(f"{name} is not an array of strings")
    _check_actor(c.get("act"))
