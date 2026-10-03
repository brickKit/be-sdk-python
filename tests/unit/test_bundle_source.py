"""Bundle polling (P6.1): ETag, 15 s poll, first-load backoff, 3 s timeout, poke, fail-static, refusing
a bundle of another contract major."""
import asyncio
import io
import json

import httpx

from besdk import logs
from besdk.auth.source import BundleSource

GOOD = {"contract": "authz/2.0", "revision": "1", "capabilities": {"core": True}, "roles": {"rep": ["a.b.view"]}}


def make(handler, **kw):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    buf = io.StringIO()
    src = BundleSource("http://authz:8223", client, logs.member_logger("c/x", "1", stream=buf),
                       first_backoff=0.01, first_backoff_max=0.02, **kw)
    return src, buf


async def test_loads_then_uses_etag():
    seen = []

    def handler(req):
        seen.append((req.url.path, req.headers.get("if-none-match")))
        if req.headers.get("if-none-match") == '"r1"':
            return httpx.Response(304)
        return httpx.Response(200, json=GOOD, headers={"ETag": '"r1"'})

    src, _ = make(handler, interval=0.01)
    task = asyncio.create_task(src.run())
    await asyncio.wait_for(src.loaded.wait(), 1)
    await asyncio.sleep(0.05)
    task.cancel()
    assert src.bundle.revision == "1"
    assert seen[0] == ("/authz/v2/bundle", None)
    assert seen[1][1] == '"r1"'


async def test_first_load_retries_with_backoff_then_fail_static():
    state = {"n": 0, "up": False}

    def handler(req):
        state["n"] += 1
        if not state["up"]:
            raise httpx.ConnectError("down")
        return httpx.Response(200, json=GOOD)

    src, _ = make(handler, interval=0.02)
    task = asyncio.create_task(src.run())
    await asyncio.sleep(0.08)
    assert not src.loaded.is_set() and state["n"] >= 3
    state["up"] = True
    await asyncio.wait_for(src.loaded.wait(), 1)
    state["up"] = False
    await asyncio.sleep(0.06)
    task.cancel()
    assert src.bundle is not None  # kept while the provider is down


async def test_refuses_other_contract_and_keeps_previous():
    docs = [GOOD, {**GOOD, "contract": "authz/1.3", "revision": "2"}]

    calls = {"n": 0}

    def handler2(req):
        calls["n"] += 1
        return httpx.Response(200, json=docs[0] if calls["n"] == 1 else docs[1])

    src, buf = make(handler2, interval=0.01)
    task = asyncio.create_task(src.run())
    await asyncio.wait_for(src.loaded.wait(), 1)
    await asyncio.sleep(0.05)
    task.cancel()
    assert src.bundle.revision == "1"
    errs = [json.loads(x) for x in buf.getvalue().splitlines() if json.loads(x)["level"] == "error"]
    assert errs and errs[0]["msg"] == "authz_bundle_refused"


async def test_poke_fetches_at_once():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(200, json={**GOOD, "revision": str(calls["n"])})

    src, _ = make(handler, interval=3600)
    task = asyncio.create_task(src.run())
    await asyncio.wait_for(src.loaded.wait(), 1)
    src.poke()
    await asyncio.sleep(0.05)
    task.cancel()
    assert src.bundle.revision == "2"
