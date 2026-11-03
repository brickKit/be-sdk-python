"""P8.25 and P8.26 (foundations item 3, T0): the token-stale vector with the view's own subject entry, and a downstream
401 to a call for the user answered as AUTHORIZATION_UNPROVABLE."""
import socket
import time

import pytest
from aiohttp import web

from besdk import errs, protocol, reqctx
from besdk.authz.view import Shard
from besdk.calls.client import Member
from besdk.guard import stale


def test_token_stale_vectors():
    v = protocol.vector("token-stale")
    assert v["cases"], "precondition: the vector has cases"
    for c in v["cases"]:
        entry = c["entry"]
        shard = Shard.from_dict({"shard": "s", "applicability": [], "subjects": {} if entry is None else {"p": entry}})
        view_sv = shard.subjects.get("p", ([], False, 0))[2]
        assert stale(c["token_sv"], view_sv) == c["expect"]["stale"], c["id"]


@pytest.mark.parametrize("reason", ["TOKEN_STALE", "TOKEN_INVALID"])
async def test_a_downstream_401_for_the_user_is_unprovable(reason):
    seen = []

    async def handler(request):
        seen.append(request.headers.get("Authorization", ""))
        return web.json_response({"status": 401, "reason": request.query["reason"], "domain": "be"}, status=401,
                                 content_type="application/problem+json")
    app = web.Application()
    app.router.add_get("/api/x", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    await web.TCPSite(runner, "127.0.0.1", port).start()
    m = Member()
    try:
        h = m.http("erp/sales", f"http://127.0.0.1:{port}")
        with reqctx.request_scope("rid-1", "Bearer the-users-token"), reqctx.deadline_scope(time.monotonic() + 5):
            with pytest.raises(errs.Error) as e:
                await h.do("GET", "/api/x?reason=" + reason, forward_user=True)
            assert (e.value.reason, e.value.http, e.value.metadata.get("dependency")) == \
                ("AUTHORIZATION_UNPROVABLE", 503, "erp/sales")
            with pytest.raises(errs.Error) as e:
                await h.do("GET", "/api/x?reason=" + reason)
            assert e.value.reason == reason, "not for the user: unchanged"
        assert seen[0] == "Bearer the-users-token", "precondition: the user's credential was sent"
    finally:
        await m.close()
        await runner.cleanup()
