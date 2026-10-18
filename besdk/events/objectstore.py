"""The claim check's two ends (P6.14): the producer puts a large payload under its own prefix and sends a presigned
read-only GET; the subscriber fetches it with a plain GET, holding no storage credentials, and checks size and
digest."""
from __future__ import annotations

import hashlib
import json

import aiohttp

from besdk.config import Key, Type, Values
from besdk.events.core import H_CLAIM, TRANSPORT_RETENTION, Message
from besdk.events.sigv4 import Signer

CLAIM_TTL = int(TRANSPORT_RETENTION)  # the transport retention, also SigV4's maximum


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def store_keys() -> list[Key]:
    """The member-level keys of the SDK's storage port (chapter 02)."""
    return [Key("OBJECT_STORE_URL", Type.STRING), Key("OBJECT_STORE_REGION", Type.STRING, default="us-east-1"),
            Key("OBJECT_STORE_BUCKET", Type.STRING), Key("OBJECT_STORE_PREFIX", Type.STRING),
            Key("OBJECT_STORE_ACCESS_KEY", Type.STRING), Key("OBJECT_STORE_SECRET_KEY", Type.STRING, secret=True)]


class S3Store:
    """The producer side: PUT the payload under the member's prefix and presign a GET of it, signed with SigV4
    (besdk.events.sigv4) over the member's own HTTP client. The secret is read at every upload, so a rotated secret
    file is used from the next one."""

    def __init__(self, url: str, region: str, bucket: str, prefix: str, access: str, secret):
        self.bucket, self.prefix, self.secret = bucket, prefix, secret
        self._signer = Signer(url, region, access)
        self._client: aiohttp.ClientSession | None = None  # made at the first upload, inside the running loop

    @staticmethod
    def from_values(v: Values) -> S3Store | None:
        """None without OBJECT_STORE_URL."""
        if not v.string("OBJECT_STORE_URL"):
            return None
        secret = v.secret("OBJECT_STORE_SECRET_KEY")
        if not v.string("OBJECT_STORE_BUCKET") or not v.string("OBJECT_STORE_ACCESS_KEY") or secret is None:
            raise ValueError("object storage: OBJECT_STORE_BUCKET, OBJECT_STORE_ACCESS_KEY and OBJECT_STORE_SECRET_KEY "
                             "go with OBJECT_STORE_URL")
        return S3Store(v.string("OBJECT_STORE_URL"), v.string("OBJECT_STORE_REGION"), v.string("OBJECT_STORE_BUCKET"),
                       v.string("OBJECT_STORE_PREFIX"), v.string("OBJECT_STORE_ACCESS_KEY"), secret)

    async def put(self, event_id: str, data: bytes) -> str:
        key = f"{self.prefix}events/{event_id}"
        secret = self.secret.value()
        url, headers = self._signer.put_headers(secret, self.bucket, key, sha256_hex(data), "application/json")
        if self._client is None:
            self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), trust_env=False)
        async with self._client.put(url, data=data, headers=headers) as resp:
            if resp.status != 200:
                # the answer says why (a wrong secret, a missing bucket); it never carries the secret
                raise OSError(f"object storage answered {resp.status} to the upload of {key}: "
                              f"{(await resp.text())[:300]}")
        return self._signer.presign_get(secret, self.bucket, key, CLAIM_TTL)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()


class HTTPClaims:
    """The subscriber side: a plain GET of the presigned address, with the member's own client."""

    def __init__(self) -> None:
        self._client: aiohttp.ClientSession | None = None  # made at the first fetch, inside the running loop

    async def get(self, uri: str) -> bytes:
        if not uri.startswith(("http://", "https://")):
            raise ValueError("a claim reference is not an http(s) address")
        if self._client is None:
            self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), trust_env=False)
        async with self._client.get(uri) as resp:
            if resp.status != 200:
                raise ValueError(f"the claimed object answered {resp.status}")
            return await resp.read()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()


async def resolve_claim(m: Message, reader: HTTPClaims | None) -> bytes:
    """The real payload of a delivery: as is, or fetched and checked against the reference's size and digest; a
    mismatch is a processing failure (P6.14)."""
    if m.headers.get(H_CLAIM) != "true":
        return m.data
    try:
        ref = json.loads(m.data)["claim"]
        uri, size, digest = ref["uri"], int(ref["size"]), ref["sha256"]
    except (ValueError, KeyError, TypeError):
        raise ValueError(f"event {m.id}: the claim reference cannot be read") from None
    if reader is None:
        raise ValueError(f"event {m.id}: a claim-checked payload and no object storage reader")
    data = await reader.get(uri)
    if len(data) != size or sha256_hex(data) != digest:
        raise ValueError(f"event {m.id}: the claimed object does not match its size and digest")
    return data
