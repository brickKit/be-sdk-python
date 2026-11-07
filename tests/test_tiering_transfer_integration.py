"""The transfer (P15.14-P15.18, P15.20, P15.32), the same cases as be-sdk-go's tiering/transfer_integration_test.go: a
transfer publishes a verified copy on both routes; only the current claim publishes (a taken-over claim, another
database epoch), the old worker's objects are orphans the cleanup removes; a tampered copy does not verify; a locked
bucket refuses a class with erasable columns."""
import hashlib
import os
import uuid
from dataclasses import replace

import aiohttp
import pytest

from besdk.config import MapSource, parse
from besdk.events.sigv4 import Signer
from besdk.tiering import member, store, transfer
from besdk.tiering.classes import OBJECT, POSTGRES
from besdk.tiering.manifest import manifest_key
from tests.test_tiering_seal_integration import Env, orders_class

pytestmark = pytest.mark.integration
BUCKET = "be-tiering-test"  # shared with be-sdk-go's tests


def s3():
    url, access, secret = (os.environ.get(k, "") for k in ("BE_SDK_TEST_S3_URL", "BE_SDK_TEST_S3_ACCESS_KEY",
                                                           "BE_SDK_TEST_S3_SECRET_KEY"))
    assert url and access and secret, "BE_SDK_TEST_S3_* must name object storage"
    return url, access, secret


def env_of(bucket: str) -> dict[str, str]:
    url, access, secret = s3()
    out = {store.STORE_URL: url, store.BUCKET: bucket}
    for r in store.ROLES:
        a, s = store.credential_keys(r)
        out[a], out[s] = access, secret
    return out


def store_for(bucket: str = BUCKET, prefix: str | None = None) -> store.Store:
    return store.Store.from_values(parse(store.keys(), MapSource(env_of(bucket))), prefix or f"sdk-test-{uuid.uuid7()}")


async def admin(method: str, bucket: str, key: str = "", extra: dict | None = None, body: bytes = b"") -> int:
    url, access, secret = s3()
    u, headers = Signer(url, "us-east-1", access).request(secret, method, bucket, key, "", extra or {},
                                                         hashlib.sha256(body).hexdigest())
    async with aiohttp.ClientSession() as c, c.request(method, u, data=body or None, headers=headers) as r:
        await r.read()
        return r.status


async def ensure_bucket(bucket: str, locked: bool) -> None:
    status = await admin("PUT", bucket, extra={"x-amz-bucket-object-lock-enabled": "true"} if locked else {})
    assert status in (200, 409), status  # 409: it exists already


def with_route(e: Env, route: str) -> None:
    e.cat.classes["sales.orders"] = replace(e.cat.classes["sales.orders"], routes=(route,))


async def sealed_unit(e: Env) -> str:
    await e.order(400)
    await e.order(400)
    groups = await e.cat.groupings("sales.orders")
    res = await e.cat.seal("sales.orders", groups[0])
    assert res.members == 2
    return res.unit


async def unit_row(e: Env, unit: str):
    return await e.a.fetchrow("SELECT state, route, manifest_version, (SELECT version FROM be_tier_catalog) AS catalog "
                              "FROM be_tier_unit WHERE id = $1", uuid.UUID(unit))


@pytest.mark.parametrize("route", [OBJECT, POSTGRES])
async def test_a_transfer_publishes_a_verified_copy(route):  # P15.14-P15.18
    async with Env("py_tier_xfer_" + route.replace("-", "_")) as e:
        await ensure_bucket(BUCKET, False)
        e.cat.store = st = store_for()
        with_route(e, route)
        try:
            unit = await sealed_unit(e)
            before = (await unit_row(e, unit))["catalog"]
            m = await transfer.transfer(e.cat, unit)
            r = await unit_row(e, unit)
            assert (r["state"], r["route"], r["manifest_version"]) == ("published", route, 1)
            assert r["catalog"] == before + 1, "the catalog version moves with the publish"
            assert next(t.rows for t in m.tables if t.name == "orders") == 2
            await transfer.verify(e.cat, m)  # the published copy verifies again
            if route == OBJECT:
                for t in m.tables:
                    for o in t.objects:
                        await st.get(o.key)
                raw = await st.get(manifest_key(st.prefix, "sales.orders", unit, m.generation, 1))
                assert raw == m.encode(), "manifest.json beside the parts"
            with pytest.raises(transfer.StaleClaim):
                await transfer.claim(e.cat, unit)  # a published unit is not claimed for its first transfer again
        finally:
            await st.close()


async def test_only_the_current_claim_publishes():  # TR04, M1, P15.20
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_claims") as e:
            e.cat.store = st
            with_route(e, OBJECT)
            unit = await sealed_unit(e)
            old = await transfer.claim(e.cat, unit)
            old_copy = await transfer.copy(e.cat, old, OBJECT)
            current = await transfer.claim(e.cat, unit)  # the lease ran out; another worker took over
            await transfer.verify(e.cat, old_copy)  # the old copy is complete: only the claim tells
            with pytest.raises(transfer.StaleClaim):
                await transfer.publish(e.cat, old, old_copy)
            assert (await unit_row(e, unit))["state"] == "sealed", "nothing changed"
            m = await transfer.copy(e.cat, current, OBJECT)
            await transfer.verify(e.cat, m)
            await transfer.publish(e.cat, current, m)
            with pytest.raises(transfer.StaleClaim):
                await transfer.publish(e.cat, current, m)  # a published unit is not published again
            assert await transfer.clean_orphans(e.cat, "sales.orders", unit, __import__("datetime").timedelta(0)) == 3, \
                "the old worker's two parts and its manifest.json"
            for o in old_copy.tables[0].objects:
                with pytest.raises(store.Missing):
                    await st.get(o.key)
            await transfer.verify(e.cat, m)  # the published copy stays
        async with Env("py_tier_epoch") as e2:  # a recovery's advance between the claim and the publish (M1)
            with_route(e2, POSTGRES)
            unit2 = await sealed_unit(e2)
            c2 = await transfer.claim(e2.cat, unit2)
            m2 = await transfer.copy(e2.cat, c2, POSTGRES)
            await e2.a.execute("UPDATE be_recovery_generation SET generation = generation + 1")
            with pytest.raises(transfer.StaleClaim):
                await transfer.publish(e2.cat, c2, m2)
    finally:
        await st.close()


async def test_a_tampered_copy_does_not_verify():  # TR05
    await ensure_bucket(BUCKET, False)
    st = store_for()
    try:
        async with Env("py_tier_tamper") as e:
            with_route(e, POSTGRES)
            unit = await sealed_unit(e)
            c = await transfer.claim(e.cat, unit)
            m = await transfer.copy(e.cat, c, POSTGRES)
            await e.a.execute("""UPDATE be_tier_row SET row = convert_to(replace(convert_from(row, 'UTF8'), '"3"',
                '"30"'), 'UTF8') WHERE tbl = 'order_lines' AND convert_from(row, 'UTF8') LIKE '%"qty":"3"%'""")
            with pytest.raises(transfer.Verification):
                await transfer.verify(e.cat, m)
            await transfer.transfer(e.cat, unit)  # a new claim copies again and verifies
        async with Env("py_tier_tamper_obj") as e2:  # the object route: an object replaced behind the manifest's back
            e2.cat.store = st
            with_route(e2, OBJECT)
            unit2 = await sealed_unit(e2)
            c2 = await transfer.claim(e2.cat, unit2)
            m2 = await transfer.copy(e2.cat, c2, OBJECT)
            lines = next(t for t in m2.tables if t.name == "order_lines")
            assert await admin("PUT", BUCKET, f"{lines.objects[0].key}", body=b"not the part") == 200
            with pytest.raises(transfer.Verification):
                await transfer.verify(e2.cat, m2)
            # one byte changed, the size the same: only the object's hash tells
            orders = next(t for t in m2.tables if t.name == "orders")
            data = bytearray(await st.get(orders.objects[0].key))
            data[len(data) // 2] ^= 0xFF
            assert await admin("PUT", BUCKET, orders.objects[0].key, body=bytes(data)) == 200
            m3 = replace(m2, tables=[orders])
            with pytest.raises(transfer.Verification, match="is not what the manifest says"):
                await transfer.verify(e2.cat, m3)
    finally:
        await st.close()


async def test_a_locked_bucket_refuses_erasable_data():  # P15.32
    locked = "be-tiering-test-locked"
    await ensure_bucket(locked, True)
    async with Env("py_tier_locked") as e:
        c = replace(orders_class(), erasable=("note",))
        values = parse(member.keys([c]), MapSource(env_of(locked)))
        with pytest.raises(ValueError, match="P15.32"):
            await member.new_catalog(e.d, [c], values, "sdk-test")
        cat = await member.new_catalog(e.d, [replace(c, erasable=())], values, "sdk-test")
        await cat.store.close()  # a class without erasable columns may go there
