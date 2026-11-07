"""The cold route's object store (A153): one bucket, the component's prefix, and three credentials: the transfer
writes, reads read, and destruction and the orphan cleanup delete. Objects are created, never overwritten (P15.15).
The SDK signs its own requests (besdk.events.sigv4, as the run profile and the large events)."""
from __future__ import annotations

import base64
import datetime as dt
import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import aiohttp

from besdk.config import Key, Type
from besdk.events.sigv4 import UNSIGNED, Signer, escape

STORE_URL, BUCKET = "TIERING_STORE_URL", "TIERING_BUCKET"
ROLES = ("WRITE", "READ", "DELETE")
_S3 = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def credential_keys(role: str) -> tuple[str, str]:
    return f"TIERING_{role}_ACCESS_KEY", f"TIERING_{role}_SECRET_KEY"


def keys() -> list[Key]:
    """The object store's keys; all optional (without TIERING_STORE_URL the cold route is unavailable)."""
    out = [Key(STORE_URL, Type.STRING), Key(BUCKET, Type.STRING, default="be-tiering")]
    for r in ROLES:
        a, s = credential_keys(r)
        out += [Key(a, Type.STRING), Key(s, Type.STRING, secret=True)]
    return out


class Missing(Exception):
    """An object that is not there."""


class Exists(Exception):
    """A create of a key that is already there."""


class StoreError(Exception):
    pass


@dataclass(frozen=True)
class Listed:
    key: str
    size: int
    last_modified: dt.datetime


class Store:
    """One member's view of the cold bucket."""

    def __init__(self, url: str, bucket: str, prefix: str, credentials: dict[str, tuple[str, object]]):
        self.bucket, self.prefix = bucket, prefix
        self._roles = {r: (Signer(url, "us-east-1", a), s) for r, (a, s) in credentials.items()}
        self._client: aiohttp.ClientSession | None = None

    @staticmethod
    def from_values(values, prefix: str) -> Store | None:
        """None when TIERING_STORE_URL is not set; raises ValueError for a missing credential."""
        if not (url := values.string(STORE_URL)):
            return None
        creds = {}
        for r in ROLES:
            a, s = credential_keys(r)
            secret = values.secret(s)
            if not values.string(a) or secret is None:
                raise ValueError(f"tiering: {a} and {s} are required with {STORE_URL}")
            creds[r] = (values.string(a), secret)
        return Store(url, values.string(BUCKET), prefix, creds)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()

    async def _do(self, role: str, method: str, key: str, query: str = "", extra: dict | None = None,
                  body: bytes | None = None) -> tuple[int, bytes]:
        signer, secret = self._roles[role]
        payload = hashlib.sha256(body).hexdigest() if body is not None else UNSIGNED
        url, headers = signer.request(secret.value(), method, self.bucket, key, query, extra or {}, payload)  # type: ignore[attr-defined]
        if self._client is None:
            self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60), trust_env=False)
        async with self._client.request(method, url, data=body, headers=headers) as resp:
            return resp.status, await resp.read()

    @staticmethod
    def _fail(op: str, key: str, status: int, body: bytes) -> StoreError:
        return StoreError(f"tiering: {op} {key}: the object store answered {status}: {body[:200]!r}")

    async def put(self, key: str, body: bytes) -> None:
        """Creates an object (the writer's credential); Exists when the key is taken (P15.15)."""
        status, out = await self._do("WRITE", "PUT", key, extra={
            "if-none-match": "*", "content-md5": base64.b64encode(hashlib.md5(body).digest()).decode(),
            "content-type": "application/octet-stream"}, body=body)
        if status == 412:
            raise Exists(key)
        if status // 100 != 2:
            raise self._fail("put", key, status, out)

    async def get(self, key: str) -> bytes:
        """Reads an object (the reader's credential); Missing when it is not there."""
        status, out = await self._do("READ", "GET", key)
        if status == 404:
            raise Missing(key)
        if status // 100 != 2:
            raise self._fail("get", key, status, out)
        return out

    def presign_get(self, key: str, expires: int) -> str:
        """An address that reads one object for expires seconds without credentials (the reader's signs it)."""
        signer, secret = self._roles["READ"]
        return signer.presign_get(secret.value(), self.bucket, key, expires)  # type: ignore[attr-defined]

    async def delete(self, key: str) -> None:
        """Removes an object (the deleter's credential); a missing object is not an error."""
        status, out = await self._do("DELETE", "DELETE", key)
        if status // 100 != 2 and status != 404:
            raise self._fail("delete", key, status, out)

    async def list(self, prefix: str) -> list[Listed]:
        """The objects under a prefix (the reader's credential)."""
        out, token = [], ""
        while True:
            q = {"list-type": "2", "prefix": prefix, **({"continuation-token": token} if token else {})}
            status, body = await self._do("READ", "GET", "", "&".join(f"{escape(k)}={escape(v)}"
                                                                      for k, v in sorted(q.items())))
            if status // 100 != 2:
                raise self._fail("list", prefix, status, body)
            root = ET.fromstring(body)
            ns = _S3 if root.tag.startswith(_S3) else ""
            for c in root.findall(f"{ns}Contents"):
                out.append(Listed(c.findtext(f"{ns}Key", ""), int(c.findtext(f"{ns}Size", "0")),
                                  dt.datetime.fromisoformat(c.findtext(f"{ns}LastModified", "").replace("Z", "+00:00"))))
            if root.findtext(f"{ns}IsTruncated", "false") != "true":
                return out
            token = root.findtext(f"{ns}NextContinuationToken", "")

    async def locked(self) -> bool:
        """Whether the bucket has object lock enabled: its objects cannot be erased in place (P15.32)."""
        status, body = await self._do("READ", "GET", "", "object-lock=")
        if status == 404 or (status // 100 != 2 and b"ObjectLockConfigurationNotFound" in body):
            return False
        if status // 100 != 2:
            raise self._fail("object lock of", self.bucket, status, body)
        return b"<ObjectLockEnabled>Enabled</ObjectLockEnabled>" in body
