"""The claim check's SigV4 against botocore's signer at a fixed time (P6.14): a presigned GET and a signed PUT, for an
ordinary event key and for one that needs every kind of encoding."""
import datetime as dt
import hashlib

import botocore.auth
import pytest
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from besdk.events.sigv4 import Signer

WHEN = dt.datetime(2026, 10, 18, 14, 30, 5, tzinfo=dt.timezone.utc)
CREDS = Credentials("AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY")
KEYS = ["test_probe_py/events/0190f3a1-0000-7000-8000-0000000000a1", "a b/ü+x=y&z~%"]


@pytest.fixture
def frozen(monkeypatch):
    class Frozen(dt.datetime):
        @classmethod
        def utcnow(cls):
            return WHEN.replace(tzinfo=None)

        @classmethod
        def now(cls, tz=None):
            return WHEN if tz else WHEN.replace(tzinfo=None)

    monkeypatch.setattr(botocore.auth.datetime, "datetime", Frozen)


@pytest.mark.parametrize("key", KEYS)
def test_a_presigned_get_equals_botocores(frozen, key):
    s = Signer("http://127.0.0.1:9000", "us-east-1", CREDS.access_key)
    req = AWSRequest(method="GET", url="http://127.0.0.1:9000" + s.path("be-claims", key))
    botocore.auth.S3SigV4QueryAuth(CREDS, "s3", "us-east-1", expires=604800).add_auth(req)
    assert s.presign_get(CREDS.secret_key, "be-claims", key, 604800, WHEN) == req.url


@pytest.mark.parametrize("key", KEYS)
def test_a_signed_put_equals_botocores(frozen, key):
    s = Signer("https://store.example:9443/base", "eu-west-1", CREDS.access_key)
    body = b"x" * 1000
    digest = hashlib.sha256(body).hexdigest()
    url, headers = s.put_headers(CREDS.secret_key, "be-claims", key, digest, "application/json", WHEN)
    req = AWSRequest(method="PUT", url=url, data=body,
                     headers={"Content-Type": "application/json", "X-Amz-Content-SHA256": digest})
    botocore.auth.S3SigV4Auth(CREDS, "s3", "eu-west-1").add_auth(req)
    assert url.startswith("https://store.example:9443/base/be-claims/")
    assert headers["authorization"] == req.headers["Authorization"]
    assert headers["x-amz-date"] == req.headers["X-Amz-Date"]


def test_the_signing_key_follows_a_rotated_secret():
    s = Signer("http://127.0.0.1:9000", "us-east-1", CREDS.access_key)
    a = s.presign_get("secret-one", "b", "k", 60, WHEN)
    b = s.presign_get("secret-two", "b", "k", 60, WHEN)
    assert a != b and s.presign_get("secret-one", "b", "k", 60, WHEN) == a
