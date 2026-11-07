"""The run profile against real object storage with object lock (P12.1-P12.8, P12.24-P12.26), the same cases as
be-sdk-go's runprofile/runprofile_integration_test.go: a record signed, written once under compliance-mode lock and
read back; a tampered, foreign or spoofed record not used; a generation advanced only through the run profile; an alert
written and a double failure logged; the trust history in the store (a rotation keeps the old CA's records, a
revocation stops what was received after it). Each run writes under its own deployment and the records expire in
minutes, so the shared bucket does not keep them for years."""
import asyncio
import base64
import datetime as dt
import hashlib
import io
import json
import os
import pathlib
import socket
import time
import uuid
from aiohttp import web

import aiohttp
import pytest
from cryptography.hazmat.primitives import serialization

from besdk import obs
from besdk.events.sigv4 import Signer
from besdk.identity.service import ServiceTLS
from besdk.runprofile import ALERT, ALERT_TIMEOUT, AUTHZ_GENERATION, DEPLOYMENT_GENERATION, RunProfile, StoreError, object_key
from besdk.runprofile import trust
from besdk.testing import World

pytestmark = pytest.mark.integration
BUCKET = "be-sdk-test-run-profile"  # object-locked, created once (shared with be-sdk-go's tests)


def s3():
    url, access, secret = (os.environ.get(k, "") for k in ("BE_SDK_TEST_S3_URL", "BE_SDK_TEST_S3_ACCESS_KEY",
                                                           "BE_SDK_TEST_S3_SECRET_KEY"))
    assert url and access and secret, "BE_SDK_TEST_S3_* must name object storage"
    return url, access, secret


async def admin(method, key="", query="", extra=None, body=b"") -> tuple[int, bytes]:
    """One request with the test's own credentials, outside the store under test."""
    url, access, secret = s3()
    sha = hashlib.sha256(body).hexdigest()
    u, headers = Signer(url, "us-east-1", access).request(secret, method, BUCKET, key, query, extra or {}, sha)
    async with aiohttp.ClientSession() as c, c.request(method, u, data=body or None, headers=headers) as r:
        return r.status, await r.read()


async def lock_bucket():
    status, out = await admin("PUT", extra={"x-amz-bucket-object-lock-enabled": "true"})
    assert status == 200 or b"BucketAlreadyOwnedByYou" in out or b"BucketAlreadyExists" in out, out


def tls_of(world: World, component: str, deployment: str | None = None) -> ServiceTLS:
    return ServiceTLS(deployment or world.deployment, component, *world.certs(component, deployment),
                      lambda p: pathlib.Path(p).read_bytes())


def store(world: World, component: str, identity=None, deployment=None, url=None, log=None, webhook="", now=None):
    u, access, secret = s3()
    return RunProfile(url or u, "us-east-1", BUCKET, access, lambda: secret, deployment or world.deployment, component,
                      component + "-1", identity or world.tls(component), webhook, log, now,
                      keep=lambda kind: dt.timedelta(minutes=2))


def closed_port() -> str:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return f"http://127.0.0.1:{port}"


@pytest.fixture
async def env(tmp_path):
    await lock_bucket()
    w = World(tmp_path, deployment=str(uuid.uuid7()))  # this run's own records
    stores = []
    yield w, stores
    for s in stores:
        await s.close()


async def test_a_record_is_signed_locked_and_read_back(env):  # P12.1-P12.3, P12.6
    w, stores = env
    s = store(w, "infra/authz")
    stores.append(s)
    r = await s.append(AUTHZ_GENERATION, {"value": 3})
    key = object_key(w.deployment, AUTHZ_GENERATION, r["written_at"], r["id"])
    status, out = await admin("GET", key, "retention=")
    assert status == 200 and b"COMPLIANCE" in out, out
    until = dt.datetime.fromisoformat(out.split(b"<RetainUntilDate>")[1].split(b"<")[0].decode().replace("Z", "+00:00"))
    assert abs((until - dt.datetime.now(dt.timezone.utc)) - dt.timedelta(minutes=2)) < dt.timedelta(seconds=30)
    recs, problems = await s.list(AUTHZ_GENERATION)
    assert problems == [] and [x["id"] for x in recs] == [r["id"]]
    assert recs[0]["writer"]["component"] == "infra/authz" and recs[0]["signature"]["alg"] == "ES256"
    # the same key is never written twice: a writer that produced the same identity and instant is refused
    at = dt.datetime.now(dt.timezone.utc)
    same = store(w, "infra/authz", now=lambda: at)
    stores.append(same)
    fixed = "01a1ab2d-0000-7000-8000-00000000c0de"
    orig = uuid.uuid7
    uuid.uuid7 = lambda: uuid.UUID(fixed)  # type: ignore[assignment]
    try:
        await same.append(AUTHZ_GENERATION, {"value": 1})
        with pytest.raises(StoreError) as e:
            await same.append(AUTHZ_GENERATION, {"value": 2})
        assert e.value.status == 412
    finally:
        uuid.uuid7 = orig  # type: ignore[assignment]
    _, raw = await admin("GET", key)
    assert s3()[2].encode() not in raw and b"PRIVATE KEY" not in raw, "no secret in the record (P12.6)"


async def test_a_tampered_or_foreign_record_is_not_used(env, tmp_path):  # P12.4
    w, stores = env
    s = store(w, "infra/authz")
    stores.append(s)
    r = await s.append(AUTHZ_GENERATION, {"value": 4})
    tampered = {**r, "id": str(uuid.uuid7()), "body": {"value": 99}}
    raw = json.dumps(tampered).encode()
    status, out = await admin("PUT", object_key(w.deployment, AUTHZ_GENERATION, r["written_at"], tampered["id"]),
                              extra={"content-md5": base64.b64encode(hashlib.md5(raw).digest()).decode()}, body=raw)
    assert status == 200, out
    (tmp_path / "foreign").mkdir()
    foreign = World(tmp_path / "foreign", deployment=w.deployment)  # another trust root, the same prefix
    fs = store(foreign, "infra/authz")
    stores.append(fs)
    await fs.append(AUTHZ_GENERATION, {"value": 50})
    # the audit component's own (valid) credential signs a record naming the authorization member as its writer
    other = store(w, "infra/authz", identity=w.tls("infra/audit"))
    stores.append(other)
    spoofed = await other.append(AUTHZ_GENERATION, {"value": 60})
    hist = await s.trust()
    assert "writer" in s.verify(spoofed, hist, dt.datetime.now(dt.timezone.utc))
    # the same CA's credential of the same component in another deployment
    elsewhere = store(w, "infra/authz", identity=tls_of(w, "infra/authz", "another-deployment"))
    stores.append(elsewhere)
    await elsewhere.append(AUTHZ_GENERATION, {"value": 70})

    recs, problems = await s.list(AUTHZ_GENERATION)
    assert [x["id"] for x in recs] == [r["id"]], "only the genuine record"
    assert len(problems) == 4
    assert (await s.current(AUTHZ_GENERATION))[0] == 4, "the tampered 99 and the foreign 50 are not the generation"
    listed = await s.list_all(AUTHZ_GENERATION)
    assert all(x.read for x in listed), "an untrusted record comes with its content (6.2 reviews it)"
    assert {x.record["body"]["value"]: not x.err for x in listed} == {4: True, 99: False, 50: False, 60: False,
                                                                       70: False}


async def test_a_generation_advances_only_through_the_run_profile(env):  # P12.4, P12.5, AT62
    w, stores = env
    s = store(w, "infra/authz")
    stores.append(s)
    assert (await s.current(DEPLOYMENT_GENERATION))[0] == 0, "no record: 0"
    assert await s.advance(DEPLOYMENT_GENERATION, 4, 2) == 5, "the evidence store saw 4, a database 2"
    assert await s.advance(DEPLOYMENT_GENERATION, 3) == 6, "the run profile's 5 counts"
    assert (await s.current(DEPLOYMENT_GENERATION))[0] == 6
    down = store(w, "infra/authz", url=closed_port())
    stores.append(down)
    with pytest.raises(Exception):
        await down.advance(DEPLOYMENT_GENERATION, 6)  # no run profile: no advance


async def test_an_alert_is_written_and_a_double_failure_is_logged(env):  # P12.7, P12.8
    w, stores = env
    got = []

    async def hook(request):
        got.append(await request.text())
        return web.Response()

    app = web.Application()
    app.router.add_post("/", hook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    try:
        s = store(w, "infra/iam-casdoor", webhook=f"http://127.0.0.1:{port}/")
        stores.append(s)
        assert await s.alert({"intent": "disable", "subject": "p1"})
        recs, _ = await s.list(ALERT)
        assert [x["body"] for x in recs] == [{"intent": "disable", "subject": "p1"}]
        for _ in range(60):
            if got:
                break
            await asyncio.sleep(0.05)
        assert len(got) == 1 and '"subject": "p1"' in got[0]

        out = io.StringIO()
        down = store(w, "infra/iam-casdoor", url=closed_port(),
                     log=obs.new_logger(obs.Member("infra/iam-casdoor", "3.0.0"), "info", out))
        stores.append(down)
        start = time.monotonic()
        assert not await down.alert({"intent": "disable"})
        assert time.monotonic() - start < ALERT_TIMEOUT + 1, "the caller goes on within the time limit"
        assert "evidence_double_failure" in out.getvalue()
    finally:
        await runner.cleanup()


async def test_the_trust_history_in_the_store(env, tmp_path):  # P12.24-P12.26, RC71, RC80, RC81
    w, stores = env
    old = store(w, "infra/authz")  # under A
    stores.append(old)
    gen = await old.append(AUTHZ_GENERATION, {"value": 4})
    before = await old.append(ALERT, {"what": "before the revocation"})

    (tmp_path / "b").mkdir()
    b = World(tmp_path / "b", deployment=w.deployment)  # the new trust root B

    def reader():
        r = store(b, "ops/recover")
        stores.append(r)
        return r

    recs, problems = await reader().list(AUTHZ_GENERATION)
    assert recs == [] and len(problems) == 1, "no rotation recorded: A's record is foreign to B"

    ca_a, key_a = w.ca()
    ca_b, key_b = b.ca()
    der = lambda c: c.public_bytes(serialization.Encoding.DER)  # noqa: E731
    rot = trust.accept({"event": trust.ROTATION, "previous": trust.fingerprint(der(ca_a)),
                        "next": trust.fingerprint(der(ca_b)),
                        "ca": ca_b.public_bytes(serialization.Encoding.PEM).decode()}, key_b)
    issuer = reader()
    await issuer.append(trust.KIND_CA, rot, key=key_a, chain=[der(ca_a)])
    recs, _ = await reader().list(AUTHZ_GENERATION)
    assert [x["id"] for x in recs] == [gen["id"]], "rotated: A's record holds under B"

    # B revokes the credential A issued to infra/authz; what the store receives from it after that does not hold
    _, old_chain = old._signing()
    await asyncio.sleep(3)
    await issuer.append(trust.KIND_CREDENTIAL, {"event": trust.REVOKE, "fingerprint": trust.fingerprint(old_chain[0])},
                        key=key_b, chain=[der(ca_b)])
    await asyncio.sleep(3)
    after = await old.append(ALERT, {"what": "after the revocation"})
    listed = await reader().list_all(ALERT)
    assert {x.record["id"]: not x.err for x in listed} == {before["id"]: True, after["id"]: False}
