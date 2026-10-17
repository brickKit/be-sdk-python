"""Service identity and the guard (P8.1-P8.7, P8.13, P8.15, P8.16): mTLS only on the system surface, permitted
callers, refused identities, certificate rotation; protected routes verify the token and decide records by the view.
Ported from the Go SDK's grpcx/security_test.go and launcher tests."""
import asyncio
import io
import json
import pathlib
import shutil
import socket
import time

import grpc
import httpx
import pytest
from fastapi.responses import JSONResponse
from google.protobuf import wrappers_pb2
from opentelemetry.sdk.trace import TracerProvider

from besdk import authz, errs, launcher, reqctx, system
from besdk.calls.client import Member, options
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.http import Route
from besdk.identity import ServiceTLS
from besdk.obs import Member as ObsMember, new_logger, new_registry
from besdk.system import Deps, Method, Services, build_server, serve_port
from tests.sectest import ALICE, BOB, defs, pem

SAY = "/demo.Echo/Say"
READ = lambda p: pathlib.Path(p).read_bytes()  # noqa: E731


def echo_services(callers, seen):
    async def say(request, context):
        seen.append(system.caller())
        return wrappers_pb2.StringValue(value="ok")

    s = Services()
    s.add("demo.Echo", {"Say": grpc.unary_unary_rpc_method_handler(
        say, request_deserializer=wrappers_pb2.StringValue.FromString,
        response_serializer=wrappers_pb2.StringValue.SerializeToString)},
        {"Say": Method("erp.inventory.say", list(callers))})
    return s


async def serve_only_for(world, component, *callers, seen=None, tls=None):
    tls = tls or world.tls(component)
    server = build_server(Deps(new_logger(ObsMember(component, "3.0.0"), "debug", io.StringIO()), new_registry(),
                               TracerProvider().get_tracer("t"), tls), echo_services(callers, seen if seen is not None else []))
    port = serve_port(server, "127.0.0.1:0", tls)
    await server.start()
    return server, f"127.0.0.1:{port}"


def member(tls):
    m = Member()
    m.set_tls(tls)
    return m


async def say(channel):
    stub = channel.unary_unary(SAY, request_serializer=wrappers_pb2.StringValue.SerializeToString,
                               response_deserializer=wrappers_pb2.StringValue.FromString)
    with reqctx.deadline_scope(time.monotonic() + 3):
        return (await stub(wrappers_pb2.StringValue(value="x"), metadata=options(idempotent=True))).value


async def test_only_permitted_components_may_call(world):  # P8.6, CK-ID-04
    seen = []
    server, addr = await serve_only_for(world, "erp/inventory", "erp/sales", seen=seen)
    ok, other = member(world.tls("erp/sales")), member(world.tls("erp/billing"))
    try:
        assert await say(ok.grpc("erp/inventory", addr)) == "ok"
        assert seen == ["erp/sales"], "the handler knows its caller by the certificate"
        with pytest.raises(errs.Error) as e:
            await say(other.grpc("erp/inventory", addr))
        assert errs.is_(e.value, "be", "PERMISSION_DENIED")
    finally:
        await asyncio.gather(ok.close(), other.close(), server.stop(None))


async def test_connections_without_a_valid_identity_are_refused(world):  # P8.5, CK-ID-04
    server, addr = await serve_only_for(world, "erp/inventory", "erp/sales")
    plain = grpc.aio.insecure_channel(addr)
    foreign_dep = "0190f3a1-0000-7000-8000-00000000dfff"
    foreign = member(ServiceTLS(foreign_dep, "erp/sales", *world.certs("erp/sales", deployment=foreign_dep), READ))
    root = world.other_root()
    cert, key, _ = world.certs("erp/sales", ca=root)
    other_ca = pathlib.Path(cert).with_name("other-ca.pem")
    other_ca.write_bytes(pem(root[0]))
    rogue = member(ServiceTLS(world.deployment, "erp/sales", cert, key, str(other_ca), READ))
    sales = member(world.tls("erp/sales"))
    try:
        with pytest.raises(grpc.aio.AioRpcError):
            await say(plain)  # no TLS at all
        for m, why in ((foreign, "a certificate of another deployment"), (rogue, "a certificate of another root")):
            with pytest.raises(errs.Error):
                await say(m.grpc("erp/inventory", addr))
                raise AssertionError(why)
        with pytest.raises(errs.Error):
            await say(sales.grpc("erp/finance", addr))  # the client expects finance; the server is inventory
    finally:
        await asyncio.gather(plain.close(), foreign.close(), rogue.close(), sales.close(), server.stop(None))
    with pytest.raises(ValueError, match="no service identity"):
        build_server(Deps(new_logger(ObsMember("x/y", "3.0.0"), "debug", io.StringIO()), new_registry(),
                          TracerProvider().get_tracer("t")), echo_services(["a/b"], []))
    with pytest.raises(ValueError, match=r"no DNS name .erp\.sales\.component\."):
        ServiceTLS(world.deployment, "erp/sales", *world.certs("erp/sales", dns=False), READ)
    with pytest.raises(ValueError, match="this member is"):
        ServiceTLS(world.deployment, "erp/billing", *world.certs("erp/sales"), READ)
    with pytest.raises(ValueError, match="no service identity"):
        member(None).grpc("erp/inventory", addr)  # never plaintext


async def test_rotated_certificates_apply_to_new_connections(world):  # P8.5
    server_files = world.certs("erp/inventory")
    server_tls = ServiceTLS(world.deployment, "erp/inventory", *server_files, READ)
    client_files = world.certs("erp/sales")
    client_tls = ServiceTLS(world.deployment, "erp/sales", *client_files, READ)
    server, addr = await serve_only_for(world, "erp/inventory", "erp/sales", tls=server_tls)
    sales = member(client_tls)
    try:
        channel = sales.grpc("erp/inventory", addr)
        assert await say(channel) == "ok"
        for files, tls, component in ((client_files, client_tls, "erp/sales"), (server_files, server_tls, "erp/inventory")):
            fresh = world.certs(component)
            for src, dst in zip(fresh[:2], files[:2]):
                shutil.copyfile(src, dst)
            assert tls.reload(), "the files changed"
            assert not tls.reload(), "and are now the same"
        before = channel.current()
        assert await say(channel) == "ok", "the next call uses the new certificate on a new connection"
        assert channel.current() is before and channel.generation == client_tls.generation
    finally:
        await asyncio.gather(sales.close(), server.stop(None))


# ---------------------------------------------------------------------------------------------- the user surface

ORDERS = defs("demo/orders", "demo.order", "demo.order.read", "demo.order.approve")


class Orders:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def read(c):
            s = authz.subject()
            d = s.check("demo.order.read", {})
            return JSONResponse({"fields": d.fields, "principal": s.principal, "cutoff_set": reqctx.cutoff() is not None})

        async def approve(c):
            authz.subject().check("demo.order.approve", {})
            return JSONResponse({"approved": c.param("id")})

        r.add(Route("GET", "/api/orders/{id}", read, "demo.order.read"))
        r.add(Route("POST", "/api/orders/{id}/approve", approve, "demo.order.approve"))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def start(d, env):
    port, stop, out = free_port(), asyncio.Event(), io.StringIO()
    full = {"COMPONENT_ID": d.id, "COMPONENT_VERSION": "3.0.0", "PORT": str(port), "STOP_DRAIN_DELAY": "50ms", **env}
    task = asyncio.create_task(launcher.run(d, launcher.Options(env=EnvSource(lookup_env=full.get), stdout=out,
                                                                stderr=out, host="127.0.0.1"), stop))
    async with httpx.AsyncClient() as c:
        for _ in range(500):
            if task.done():
                return None, stop, task, out
            try:
                if (await c.get(f"http://127.0.0.1:{port}/readyz")).status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio.sleep(0.01)
    return f"http://127.0.0.1:{port}", stop, task, out


async def test_protected_routes_verify_the_token_and_decide_by_the_view(world):  # P8.1-P8.3, P8.15, P8.16
    world.grant_all(ALICE, ORDERS, only=["demo.order.read"])
    base, stop, task, out = await start(Definition("demo/orders", "3.0.0", Orders, authz=ORDERS), world.env("demo/orders"))
    assert base, out.getvalue()
    try:
        async with httpx.AsyncClient(base_url=base) as c:
            r = await c.get("/api/orders/1")
            assert r.status_code == 401 and r.json()["reason"] == "TOKEN_INVALID"
            bad = world.token(ALICE)[:-4] + "AAAA"
            r = await c.get("/api/orders/1", headers={"Authorization": "Bearer " + bad})
            assert r.status_code == 401 and r.json()["detail"] == "the credential is missing or not valid"
            assert bad not in out.getvalue() and '"problem": "signature"' in out.getvalue().replace('":"', '": "')
            r = await c.get("/api/orders/1", headers={"Authorization": world.bearer(ALICE)})
            assert r.status_code == 200, r.text
            assert r.json() == {"fields": ["id"], "principal": ALICE, "cutoff_set": True}
            r = await c.post("/api/orders/1/approve", headers={"Authorization": world.bearer(ALICE)})
            assert r.status_code == 403 and r.json()["reason"] == "PERMISSION_DENIED", "visible, not allowed"
            r = await c.get("/api/orders/1", headers={"Authorization": world.bearer(BOB)})
            assert r.status_code == 404 and r.json()["reason"] == "NOT_FOUND", "invisible answers 404, not 403"
            world.revoke(ALICE)
            for _ in range(60):  # the next verification (every 2.5 s) brings the revocation
                r = await c.get("/api/orders/1", headers={"Authorization": world.bearer(ALICE)})
                if r.status_code == 404:
                    break
                await asyncio.sleep(0.1)
            assert r.status_code == 404, "a revoked grant stops at the next verification"
    finally:
        stop.set()
        assert await task == launcher.EXIT_OK, out.getvalue()


async def test_without_a_view_protected_routes_answer_a_retryable_503(world):  # P8.13, CK-AZ-03
    world.authz_up = False
    base, stop, task, out = await start(Definition("demo/orders", "3.0.0", Orders, authz=ORDERS), world.env("demo/orders"))
    try:
        async with httpx.AsyncClient(base_url=base) as c:
            r = await c.get("/api/orders/1", headers={"Authorization": world.bearer(ALICE)})
        assert r.status_code == 503 and r.json()["reason"] == "AUTHORIZATION_VIEW_EXPIRED"
        assert "no authorization view yet" in out.getvalue()
    finally:
        stop.set()
        assert await task == launcher.EXIT_OK, out.getvalue()


async def test_protected_routes_need_the_security_configuration_and_declarations(world):  # P8.7, CK-AZ-01
    env = {k: v for k, v in world.env("demo/orders").items() if k != "IAM_ISSUER"}
    _, _, task, out = await start(Definition("demo/orders", "3.0.0", Orders, authz=ORDERS), env)
    assert await task == launcher.EXIT_CONFIGURATION
    assert any(json.loads(line).get("key") == "IAM_ISSUER" for line in out.getvalue().splitlines() if line.startswith("{"))
    _, _, task, out = await start(Definition("demo/orders", "3.0.0", Orders), world.env("demo/orders"))
    assert await task == launcher.EXIT_FAILURE and "declares no resources and operations" in out.getvalue()
    undeclared = defs("demo/orders", "demo.order", "demo.order.read")
    _, _, task, out = await start(Definition("demo/orders", "3.0.0", Orders, authz=undeclared), world.env("demo/orders"))
    assert await task == launcher.EXIT_FAILURE and "not declared: demo.order.approve" in out.getvalue()
