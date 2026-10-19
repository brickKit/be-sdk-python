"""Audit and calls across languages over the deployment's mTLS (P8.5, P8.6, P8.19-P8.23, P4.19): audit records written
with the business change, sensitive reads and denials within the budget reach the (fake) audit member with receipts;
a call chain Go -> Python -> Go retries at one layer only, and every hop knows its caller by the certificate."""
import asyncio
import io
import os
import pathlib
import signal
import subprocess

import grpc
import httpx
import pytest
from fastapi.responses import JSONResponse
from google.protobuf import wrappers_pb2

from besdk import audit, launcher, system
from besdk.calls.client import options
from besdk.component import Definition
from besdk.config import EnvSource
from besdk.http import Route
from besdk.system import Method
from tests import pgtest
from besdk.testing import ALICE, defs
from tests.test_events_interop import free_port, ready, start_python

pytestmark = pytest.mark.integration
CHAIN = pathlib.Path(__file__).parent / "interop" / "gochain"


class Audited:
    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        async def change(c):
            async def fn(tx):
                await tx.execute("INSERT INTO orders VALUES ($1)", c.param("id"))
                await self.rt.audit.write(tx, audit.Record(audit.BUSINESS_CHANGE, "demo.order.create", audit.SUCCEEDED,
                                                           actor=ALICE, target={"type": "demo.order", "id": c.param("id")},
                                                           changes={"state": "created"}))
            await self.rt.db.tx(fn)
            return JSONResponse({}, status_code=201)

        async def secret(c):
            await self.rt.audit.read(audit.Record("", "demo.order.export", audit.SUCCEEDED, actor=ALICE))
            return JSONResponse({"secret": "released after the record"})

        async def deny(c):
            await self.rt.audit.denial(audit.Record("", "demo.order.approve", "", "PERMISSION_DENIED", actor=ALICE))
            return JSONResponse({})

        r.add(Route("POST", "/api/orders/{id}", change, public=True))
        r.add(Route("GET", "/api/secret", secret, public=True))
        r.add(Route("POST", "/api/deny", deny, public=True))


async def test_audit_records_reach_the_audit_member_with_receipts(world):  # P8.19-P8.23
    name = "py_sec_audit"
    await pgtest.member(name)
    env = {**pgtest.member_env(name), **world.env("demo/audited"), "COMPONENT_VERSION": "3.0.0", "STOP_DRAIN_DELAY": "50ms"}
    base, stop, task = await start_python(Definition("demo/audited", "3.0.0", Audited, migrations={
        "0001_orders.up.sql": "CREATE TABLE orders (id text PRIMARY KEY);"}), env)
    try:
        async with httpx.AsyncClient(base_url=base) as c:
            assert (await c.post("/api/orders/o1")).status_code == 201
            assert (await c.post("/api/orders/o1")).status_code != 201, "a failed change leaves no audit record"
            assert (await c.get("/api/secret")).status_code == 200
            for _ in range(audit.DENIAL_BUDGET + 5):
                assert (await c.post("/api/deny")).status_code == 200
        a = await pgtest.admin()
        try:
            # the denial window is over (as a minute later): the next run appends one summary with the count
            await a.execute(f"UPDATE {name}.be_audit_denial_budget SET minute = minute - interval '1 minute'")
            for _ in range(250):
                kinds = [(r["category"], r["operation"]) for r in world.audited.values()]
                if ("security_denial", "be.audit.denials_over_budget") in kinds:
                    break
                await asyncio.sleep(0.1)
            rows = await a.fetch(f"SELECT seq, transfer_state FROM {name}.be_audit_log ORDER BY seq")
        finally:
            await a.close()
        kinds = [(r["category"], r["operation"]) for r in world.audited.values()]
        assert kinds.count(("business_change", "demo.order.create")) == 1, task.out.getvalue()[-3000:]
        assert kinds.count(("sensitive_read", "demo.order.export")) == 1
        assert kinds.count(("security_denial", "demo.order.approve")) == audit.DENIAL_BUDGET
        summary = next(r for r in world.audited.values() if r["operation"] == "be.audit.denials_over_budget")
        assert summary["changes"]["count"] == "5" and summary["actor"] == ALICE
        change = next(r for r in world.audited.values() if r["operation"] == "demo.order.create")
        assert change["target"] == {"type": "demo.order", "id": "o1"} and change["actor"] == ALICE
        assert [r["seq"] for r in rows] == list(range(1, len(rows) + 1)), "numbered without gaps"
        assert {r["transfer_state"] for r in rows} == {"received"} and len(rows) == len(world.audited)
    finally:
        stop.set()
        assert await task == launcher.EXIT_OK, task.out.getvalue()


@pytest.fixture(scope="module")
def chain_binary(tmp_path_factory):
    out = tmp_path_factory.mktemp("gochain") / "gochain"
    subprocess.run(["go", "build", "-o", str(out), "."], cwd=CHAIN, check=True, timeout=600)
    return out


def start_go(binary, component, env):
    port, gport = free_port(), free_port()
    full = {**os.environ, **env, "MEMBER_ID": component, "COMPONENT_ID": component, "COMPONENT_VERSION": "3.0.0",
            "PORT": str(port), "GRPC_PORT": str(gport), "STOP_DRAIN_DELAY": "50ms"}
    proc = subprocess.Popen([str(binary)], env=full, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return f"http://127.0.0.1:{port}", gport, proc


async def stop_go(proc):
    proc.send_signal(signal.SIGTERM)
    await asyncio.to_thread(proc.wait, 15)
    assert proc.returncode == 0, proc.stdout.read()


class Middle:
    """The Python hop: its Say calls the Go back end and relays what it gets."""

    calls = 0

    def __init__(self, rt):
        self.rt = rt

    def routes(self, r):
        pass

    def services(self, s):
        say = self.rt.grpc("demo/goc").unary_unary(
            "/demo.Echo/Say", request_serializer=wrappers_pb2.StringValue.SerializeToString,
            response_deserializer=wrappers_pb2.StringValue.FromString)

        async def handle(request, context):
            Middle.calls += 1
            assert system.caller() == "demo/goa"
            return await say(request, metadata=options(idempotent=True))

        s.add("demo.Echo", {"Say": grpc.unary_unary_rpc_method_handler(
            handle, request_deserializer=wrappers_pb2.StringValue.FromString,
            response_serializer=wrappers_pb2.StringValue.SerializeToString)},
            {"Say": Method("demo.chain.say", ["demo/goa"])})


async def start_python_without_db(d, env):
    port, stop, out = free_port(), asyncio.Event(), io.StringIO()
    full = {**env, "COMPONENT_ID": d.id, "PORT": str(port)}
    task = asyncio.create_task(launcher.run(d, launcher.Options(env=EnvSource(lookup_env=full.get), stdout=out,
                                                                stderr=out, host="127.0.0.1"), stop))
    await ready(f"http://127.0.0.1:{port}", out)
    task.out = out  # type: ignore[attr-defined]
    return f"http://127.0.0.1:{port}", stop, task


async def test_go_python_go_chain_over_mtls_retries_at_one_layer(world, chain_binary):  # P4.19, P8.5, P8.6
    c_url, c_gport, c_proc = start_go(chain_binary, "demo/goc", {**world.env("demo/goc"), "CALLER_ID": "demo/pyb"})
    b_gport = free_port()
    b_env = {**world.env("demo/pyb"), "COMPONENT_VERSION": "3.0.0", "STOP_DRAIN_DELAY": "50ms",
             "DEMO_GOC_GRPC_ENDPOINT": f"http://127.0.0.1:{c_gport}"}
    a_url, _, a_proc = start_go(chain_binary, "demo/goa", {**world.env("demo/goa"), "CALLER_ID": "demo/none",
                                                           "NEXT_ID": "demo/pyb",
                                                           "DEMO_PYB_GRPC_ENDPOINT": f"http://127.0.0.1:{b_gport}"})
    b_url, b_stop, b_task = await start_python_without_db(Definition(
        "demo/pyb", "3.0.0", Middle, grpc_port=b_gport,
        authz=defs("demo/pyb", "demo.chain", "demo.chain.read", "demo.chain.say")), b_env)
    try:
        await ready(c_url, proc=c_proc)
        await ready(a_url, proc=a_proc)
        async with httpx.AsyncClient() as c:
            got = (await c.post(a_url + "/api/call", headers={"X-Request-Id": "chain-go-py-go"})).json()
            count = (await c.get(c_url + "/api/count")).json()
        assert got == {"reason": "DEPENDENCY_UNAVAILABLE", "dependency": "demo/goc"}, \
            "A gets B's relayed answer naming C, unchanged across both languages"
        assert Middle.calls == 1 and count["say"] == "3", "C is retried by B only; A does not retry the relayed answer"
        assert count["caller"] == "demo/pyb" and count["request_id"] == "chain-go-py-go"
    finally:
        b_stop.set()
        assert await b_task == launcher.EXIT_OK, b_task.out.getvalue()
        await asyncio.gather(stop_go(a_proc), stop_go(c_proc))
