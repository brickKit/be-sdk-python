"""User access tokens (protocol P8.1-P8.3): the checks of vector jwt-verify, keys only from the configured JWKS URL
with the refresh rules of P8.2, and refusals that say nothing about why."""
from __future__ import annotations

import asyncio
import base64
import contextlib
import contextvars
import json
import math
import re
import time
from dataclasses import dataclass
from typing import Callable, Iterator

import aiohttp
import jwt
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, rsa

from besdk import errs

TOKEN_TYPE = "at+jwt"
LEEWAY = 60.0
ALGORITHMS = ("RS256", "ES256", "EdDSA")
KEY_REFRESH = 600.0  # periodic refresh
UNKNOWN_KID_REFRESH = 30.0  # an unknown kid refreshes at most this often
KEY_MAX_AGE = 3600.0  # keys older than this are not used
MAX_JWKS = 1 << 20

_PRINCIPAL = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")


@dataclass(frozen=True)
class User:
    """A verified user credential."""

    subject: str  # the platform principal
    expiry: float  # the credential's own end (UNIX seconds): part of the request's cutoff


class TokenError(Exception):
    """A refused token: TOKEN_INVALID to the caller, the problem (vector jwt-verify) in the logs only."""

    def __init__(self, problem: str):
        super().__init__("token invalid: " + problem)
        self.problem = problem


class KeysUnavailable(Exception):
    """No usable keys: never fetched, or older than KEY_MAX_AGE."""


class _UnknownKID(Exception):
    pass


def refusal(err: BaseException) -> errs.Error:
    """What the caller gets for a refused token: TOKEN_INVALID, with nothing about why."""
    if isinstance(err, TokenError):
        return errs.new("TOKEN_INVALID", "the credential is missing or not valid").wrap(err)
    return errs.from_exception(err)  # type: ignore[return-value]


class KeySet:
    """The issuer's public keys, fetched only from the configured URL. One per process (protocol 17)."""

    def __init__(self, url: str, now: Callable[[], float] = time.monotonic):
        self.url, self._now = url, now
        self._client: aiohttp.ClientSession | None = None  # made at the first fetch, inside the running loop
        self._keys: dict[str, jwt.PyJWK] = {}
        self._fetched_at: float | None = None
        self.last_unknown: float | None = None
        self._unknown: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    @staticmethod
    async def open(url: str) -> tuple[KeySet, Exception | None]:
        """Creates the set and fetches the keys once; a failing first fetch is returned, not raised (the identity
        member may start after the component). run() keeps refreshing."""
        k = KeySet(url)
        try:
            await k.refresh()
            return k, None
        except Exception as e:  # noqa: BLE001 - reported by the caller
            return k, e

    async def run(self) -> None:
        while True:
            await asyncio.sleep(KEY_REFRESH)
            try:
                await self.refresh()
            except Exception:  # noqa: BLE001 - the previous keys stay until KEY_MAX_AGE
                pass

    async def refresh(self) -> None:
        async with self._lock:
            if self._client is None:
                self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False)
            try:
                async with self._client.get(self.url) as resp:
                    if resp.status != 200:
                        raise OSError(f"fetching the issuer's keys: status {resp.status}")
                    body = b""
                    async for chunk in resp.content.iter_chunked(64 << 10):
                        body += chunk
                        if len(body) > MAX_JWKS:
                            raise OSError("fetching the issuer's keys: the answer is too large")
            except (aiohttp.ClientError, TimeoutError) as e:
                raise OSError(f"fetching the issuer's keys: {e}") from e
            try:
                entries = json.loads(body)["keys"]
            except (ValueError, KeyError, TypeError) as e:
                raise OSError(f"parsing the issuer's keys: {e}") from e
            keys = {}
            for entry in entries if isinstance(entries, list) else []:
                try:
                    k = jwt.PyJWK(entry)
                except (jwt.PyJWKError, jwt.InvalidKeyError, TypeError, ValueError):
                    continue  # a key this code does not understand is skipped, the rest are used (P8.2, RFC 7517 §5)
                if k.key_id:
                    keys[k.key_id] = k
            self._keys, self._fetched_at = keys, self._now()

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.close()

    async def key(self, kid: str) -> jwt.PyJWK:
        """A token's key: an unknown kid refreshes the set at most once per 30 s; keys older than an hour are not
        used."""
        now = self._now()
        if kid not in self._keys and (self.last_unknown is None or now - self.last_unknown >= UNKNOWN_KID_REFRESH):
            self.last_unknown = now
            # the refresh is the key set's, not the request's: a request that times out does not cancel it
            self._unknown = asyncio.get_running_loop().create_task(self.refresh())
        if self._unknown is not None and not self._unknown.done():
            try:
                await asyncio.shield(self._unknown)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - judged below by the keys' age
                pass
        if self._fetched_at is None or self._now() - self._fetched_at > KEY_MAX_AGE:
            raise KeysUnavailable()
        if kid not in self._keys:
            raise _UnknownKID()
        return self._keys[kid]


def _b64json(part: str) -> dict:
    raw = base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))
    v = json.loads(raw)
    if not isinstance(v, dict):
        raise ValueError("not an object")
    return v


def _fits(alg: str, key: object) -> bool:
    if alg == "RS256":
        return isinstance(key, rsa.RSAPublicKey)
    if alg == "ES256":
        return isinstance(key, ec.EllipticCurvePublicKey) and key.curve.name == "secp256r1"
    return isinstance(key, ed25519.Ed25519PublicKey)


class Verifier:
    """Verifies user access tokens for one deployment."""

    def __init__(self, issuer: str, audience: str, keys: KeySet, now: Callable[[], float] = time.time):
        self.issuer, self.audience, self.keys, self.now = issuer, audience, keys, now

    async def verify(self, token: str) -> User:
        """A refused token raises TokenError; keys that cannot be obtained raise DEPENDENCY_UNAVAILABLE."""
        parts = token.split(".")
        if len(parts) != 3:
            raise TokenError("format")
        try:
            h = _b64json(parts[0])
        except ValueError:
            raise TokenError("format") from None
        alg, typ, kid = h.get("alg"), h.get("typ"), h.get("kid")
        if any(v is not None and not isinstance(v, str) for v in (alg, typ, kid)):
            raise TokenError("format")
        if alg not in ALGORITHMS:
            raise TokenError("alg")
        if typ != TOKEN_TYPE:
            raise TokenError("typ")
        if not kid:
            raise TokenError("kid")
        try:
            jwk = await self.keys.key(kid)
        except KeysUnavailable as e:
            raise errs.new("DEPENDENCY_UNAVAILABLE", "the identity keys are unavailable").with_(
                "dependency", "iam").wrap(e) from e
        except _UnknownKID:
            raise TokenError("kid_unknown") from None
        if not _fits(alg, jwk.key) or (jwk.algorithm_name and jwk.algorithm_name != alg):
            raise TokenError("alg")  # the key does not fit the algorithm
        claims = self._decode(token, jwk, alg)
        self._times(claims)
        sub = claims.get("sub")
        if not isinstance(sub, str) or not _PRINCIPAL.fullmatch(sub):
            raise TokenError("sub")
        return User(sub, float(claims["exp"]))

    def _decode(self, token: str, jwk: jwt.PyJWK, alg: str) -> dict:
        """Signature, issuer and audience; the times are checked against the injectable clock below."""
        try:
            return jwt.decode(token, jwk.key, algorithms=[alg], issuer=self.issuer, audience=self.audience,
                              options={"verify_exp": False, "verify_nbf": False, "verify_iat": False,
                                       "verify_sub": False, "verify_jti": False, "require": ["iss", "aud"]})
        except jwt.InvalidSignatureError:
            raise TokenError("signature") from None
        except jwt.InvalidIssuerError:
            raise TokenError("iss") from None
        except jwt.InvalidAudienceError:
            raise TokenError("aud") from None
        except jwt.MissingRequiredClaimError as e:
            raise TokenError("aud" if e.claim == "aud" else "iss") from None
        except (jwt.InvalidAlgorithmError, jwt.InvalidKeyError):
            raise TokenError("alg") from None
        except jwt.InvalidTokenError:
            raise TokenError("format") from None

    def _times(self, claims: dict) -> None:
        now = self.now()

        def num(name: str) -> float | None:
            v = claims.get(name)
            if v is None:
                return None
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
                raise TokenError("format")
            return float(v)

        exp, nbf, iat = num("exp"), num("nbf"), num("iat")
        if exp is None or now >= exp + LEEWAY:
            raise TokenError("exp")
        if nbf is not None and now < nbf - LEEWAY:
            raise TokenError("nbf")
        if iat is not None and now < iat - LEEWAY:
            raise TokenError("iat")


_user: contextvars.ContextVar[User | None] = contextvars.ContextVar("be_user", default=None)


def user() -> User | None:
    """The request's verified user (bound by the guard for the handler)."""
    return _user.get()


@contextlib.contextmanager
def user_scope(u: User) -> Iterator[None]:
    """Binds the verified user for a block (the guard's admission)."""
    token = _user.set(u)
    try:
        yield
    finally:
        _user.reset(token)
