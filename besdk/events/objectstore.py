"""The claim check's two ends (P6.14): the producer puts a large payload under its own prefix and sends a presigned
read-only GET; the subscriber fetches it with a plain GET, holding no storage credentials, and checks size and
digest."""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json

import aioboto3
import httpx
from botocore.config import Config

from besdk.config import Key, Type, Values
from besdk.events.core import H_CLAIM, TRANSPORT_RETENTION, Message

CLAIM_TTL = int(TRANSPORT_RETENTION)  # the transport retention, also SigV4's maximum


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def store_keys() -> list[Key]:
    """The member-level keys of the SDK's storage port (chapter 02)."""
    return [Key("OBJECT_STORE_URL", Type.STRING), Key("OBJECT_STORE_REGION", Type.STRING, default="us-east-1"),
            Key("OBJECT_STORE_BUCKET", Type.STRING), Key("OBJECT_STORE_PREFIX", Type.STRING),
            Key("OBJECT_STORE_ACCESS_KEY", Type.STRING), Key("OBJECT_STORE_SECRET_KEY", Type.STRING, secret=True)]


class S3Store:
    """The producer side. One client is kept while the secret stays the same: building one loads the service model
    and cost about 17 ms per upload against 9 ms for the upload itself; the secret is read at every request, so a
    rotated file gets a new client."""

    def __init__(self, url: str, region: str, bucket: str, prefix: str, access: str, secret):
        self.url, self.region, self.bucket, self.prefix, self.access, self.secret = url, region, bucket, prefix, access, secret
        self._session = aioboto3.Session()
        self._lock = asyncio.Lock()
        self._current: tuple[str, object] | None = None  # (the secret it was built with, the client)
        self._stack = contextlib.AsyncExitStack()  # every client built, closed by close()
        self.clients_built = 0

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

    def _client(self):
        return self._session.client("s3", endpoint_url=self.url, region_name=self.region,
                                    aws_access_key_id=self.access, aws_secret_access_key=self.secret.value(),
                                    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))

    async def _s3(self):
        secret = self.secret.value()
        async with self._lock:
            if self._current is None or self._current[0] != secret:
                # the client built with the previous secret is closed with the store: a request may still use it
                self._current = (secret, await self._stack.enter_async_context(self._client()))
                self.clients_built += 1
            return self._current[1]

    async def put(self, event_id: str, data: bytes) -> str:
        key = f"{self.prefix}events/{event_id}"
        s3 = await self._s3()
        await s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentLength=len(data),
                            ContentType="application/json")
        return await s3.generate_presigned_url("get_object", Params={"Bucket": self.bucket, "Key": key},
                                               ExpiresIn=CLAIM_TTL)

    async def close(self) -> None:
        self._current = None
        await self._stack.aclose()


class HTTPClaims:
    """The subscriber side: a plain GET of the presigned address, with the member's own client."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(trust_env=False, timeout=30)

    async def get(self, uri: str) -> bytes:
        if not uri.startswith(("http://", "https://")):
            raise ValueError("a claim reference is not an http(s) address")
        resp = await self._client.get(uri)
        if resp.status_code != 200:
            raise ValueError(f"the claimed object answered {resp.status_code}")
        return resp.content

    async def close(self) -> None:
        await self._client.aclose()


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
