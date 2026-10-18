"""AWS Signature Version 4 for the two S3 requests the claim check makes (P6.14): an object PUT signed in its
headers, and a presigned GET. Path-style addressing (`<endpoint>/<bucket>/<key>`), as every S3-compatible store
accepts. Measured on the probes' 100 KiB claims: about 1.1 ms of CPU an upload with presigning, against 4.3 ms
through aioboto3 (botocore builds and validates a whole request model, hooks and checksums for each call). The
signatures equal botocore's for the same inputs (tests/test_sigv4.py)."""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit

ALGORITHM = "AWS4-HMAC-SHA256"
UNSIGNED = "UNSIGNED-PAYLOAD"


def _hmac(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


class Signer:
    """Signs for one endpoint, region and access key; the secret is passed at every call, so a rotated secret is
    used from the next request."""

    def __init__(self, endpoint: str, region: str, access: str):
        u = urlsplit(endpoint)
        self.scheme, self.host, self.base = u.scheme, u.netloc, u.path.rstrip("/")
        self.region, self.access = region, access
        self._keys: dict[tuple[str, str], bytes] = {}  # (secret, date) -> signing key

    def _key(self, secret: str, date: str) -> bytes:
        k = self._keys.get((secret, date))
        if k is None:
            k = _hmac(_hmac(_hmac(_hmac(("AWS4" + secret).encode(), date), self.region), "s3"), "aws4_request")
            self._keys = {(secret, date): k}  # one day's key per secret is enough
        return k

    def path(self, bucket: str, key: str) -> str:
        """The canonical path: each segment percent-encoded once (S3 does not encode twice); `/` stays."""
        return f"{self.base}/{quote(bucket, safe='')}/{quote(key, safe='/~')}"

    def _sign(self, secret: str, when: datetime, method: str, path: str, query: str, headers: dict[str, str],
              payload_hash: str) -> tuple[str, str, str]:
        amz, date = when.strftime("%Y%m%dT%H%M%SZ"), when.strftime("%Y%m%d")
        names = sorted(headers)
        canonical = "\n".join([method, path, query, "".join(f"{n}:{headers[n]}\n" for n in names), ";".join(names),
                               payload_hash])
        scope = f"{date}/{self.region}/s3/aws4_request"
        to_sign = "\n".join([ALGORITHM, amz, scope, hashlib.sha256(canonical.encode()).hexdigest()])
        return hmac.new(self._key(secret, date), to_sign.encode(), hashlib.sha256).hexdigest(), scope, ";".join(names)

    def put_headers(self, secret: str, bucket: str, key: str, payload_sha256: str, content_type: str,
                    when: datetime | None = None) -> tuple[str, dict[str, str]]:
        """The URL and headers of a signed PUT of an object whose body has the given SHA-256."""
        when = when or datetime.now(timezone.utc)
        amz = when.strftime("%Y%m%dT%H%M%SZ")
        path = self.path(bucket, key)
        signed = {"host": self.host, "x-amz-content-sha256": payload_sha256, "x-amz-date": amz,
                  "content-type": content_type}
        sig, scope, names = self._sign(secret, when, "PUT", path, "", signed, payload_sha256)
        headers = {"x-amz-content-sha256": payload_sha256, "x-amz-date": amz, "content-type": content_type,
                   "authorization": f"{ALGORITHM} Credential={self.access}/{scope}, SignedHeaders={names}, Signature={sig}"}
        return f"{self.scheme}://{self.host}{path}", headers

    def presign_get(self, secret: str, bucket: str, key: str, expires: int, when: datetime | None = None) -> str:
        """A read-only GET of the object, valid for expires seconds, that needs no credentials to use."""
        when = when or datetime.now(timezone.utc)
        amz, date = when.strftime("%Y%m%dT%H%M%SZ"), when.strftime("%Y%m%d")
        path = self.path(bucket, key)
        params = {"X-Amz-Algorithm": ALGORITHM, "X-Amz-Credential": f"{self.access}/{date}/{self.region}/s3/aws4_request",
                  "X-Amz-Date": amz, "X-Amz-Expires": str(expires), "X-Amz-SignedHeaders": "host"}
        query = "&".join(f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}" for k, v in sorted(params.items()))
        sig, _, _ = self._sign(secret, when, "GET", path, query, {"host": self.host}, UNSIGNED)
        return f"{self.scheme}://{self.host}{path}?{query}&X-Amz-Signature={sig}"
