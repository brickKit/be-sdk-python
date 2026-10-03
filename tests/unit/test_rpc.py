"""The system plane (P7): outbound metadata, identity, deadline floor, batch limits, error details both
ways, retries for idempotent methods only, outbound deadline, bulkhead, the transaction guard."""
import asyncio

import grpc
import pytest
from prometheus_client import generate_latest

from besdk import Code, Error, Module, context
from besdk.rpc.server import GrpcServer
from conformance.peer.v1 import peer_pb2, peer_pb2_grpc
from tests.unit._rt import build, manifest


class Peer(peer_pb2_grpc.PeerServiceServicer):
    def __init__(self):
        self.reserve_fail = 0
        self.notify_fail = 0
        self.calls = {"Reserve": 0, "Notify": 0}
        self.block = None
        self.remaining = None

    async def BatchGet(self, request, ctx):
        self.remaining = context.remaining()
        if request.ids and request.ids[0] == "fail":
            raise Error(Code.FAILED_PRECONDITION, "QUOTA_EXCEEDED", {"available": "2"})
        if request.ids and request.ids[0] == "crash":
            raise RuntimeError("SELECT secret")
        if self.block is not None:
            await self.block.wait()
        md = {k: v for k, v in ctx.invocation_metadata()}
        return peer_pb2.BatchGetResponse(items=[peer_pb2.Item(id=i) for i in request.ids], seen_metadata=md)

    async def Reserve(self, request, ctx):
        self.calls["Reserve"] += 1
        if self.calls["Reserve"] <= self.reserve_fail:
            await ctx.abort(grpc.StatusCode.UNAVAILABLE, "try again")
        return peer_pb2.ReserveResponse(attempts=self.calls["Reserve"])

    async def Notify(self, request, ctx):
        self.calls["Notify"] += 1
        if self.calls["Notify"] <= self.notify_fail:
            await ctx.abort(grpc.StatusCode.UNAVAILABLE, "try again")
        return peer_pb2.ReserveResponse(attempts=self.calls["Notify"])


async def _empty(rt):
    return Module()


@pytest.fixture
async def pair(tmp_path):
    peer = Peer()
    srv_rt, _, _ = build(tmp_path / "peer", _empty, doc=manifest("conformance/peer"))
    server = GrpcServer(srv_rt, lambda s: peer_pb2_grpc.add_PeerServiceServicer_to_server(peer, s), port=0)
    await server.start()
    doc = manifest(dependencies={"components": ["conformance/peer@1.0.0"]})
    from tests.unit._rt import env
    e = env(CONFORMANCE_PEER_ENDPOINT="http://127.0.0.1:1", CONFORMANCE_PEER_GRPC_ENDPOINT=f"http://127.0.0.1:{server.port}")
    cli_rt, _, _ = build(tmp_path / "cli", _empty, doc=doc, environ=e)
    stub = peer_pb2_grpc.PeerServiceStub(cli_rt.conn("conformance/peer", "grpc"))
    yield peer, srv_rt, cli_rt, stub, server
    await cli_rt.outbound().close()
    await server.stop(0)


async def test_metadata_and_cached_channel(pair):
    peer, _, cli_rt, stub, _ = pair
    assert cli_rt.conn("conformance/peer", "grpc") is cli_rt.conn("conformance/peer", "grpc")
    with context.scope(request_id="r-7", sub="u_me", act={"sub": "u_admin", "kind": "user"}):
        with cli_rt.tracer.start_as_current_span("caller"):
            r = await stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"]))
    md = dict(r.seen_metadata)
    assert md["be-caller"] == "conformance/widget-py" and md["x-request-id"] == "r-7"
    assert md["be-actor-sub"] == "u_me" and '"kind":"user"' in md["be-actor-act"]
    assert md["traceparent"].startswith("00-")
    r = await stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"]))
    assert "be-actor-sub" not in dict(r.seen_metadata)  # background work: no actor


async def test_missing_caller_is_unauthenticated(pair):
    _, _, _, _, server = pair
    async with grpc.aio.insecure_channel(f"127.0.0.1:{server.port}") as ch:
        with pytest.raises(grpc.aio.AioRpcError) as ei:
            await peer_pb2_grpc.PeerServiceStub(ch).BatchGet(peer_pb2.BatchGetRequest(ids=["a"]), timeout=2)
    assert ei.value.code() == grpc.StatusCode.UNAUTHENTICATED


async def _err(coro) -> Error:
    with pytest.raises(Error) as ei:
        await coro
    return ei.value


@pytest.mark.parametrize("req,field,mx,got", [
    (peer_pb2.BatchGetRequest(ids=["x"] * 4), "ids", "3", "4"),
    (peer_pb2.BatchGetRequest(tags=["t"] * 501), "tags", "500", "501"),
    (peer_pb2.BatchGetRequest(filter=peer_pb2.Filter(sku_ids=["s"] * 3)), "filter.sku_ids", "2", "3"),
])
async def test_batch_limits(pair, req, field, mx, got):
    _, _, _, stub, _ = pair
    e = await _err(stub.BatchGet(req))
    assert (e.code, e.reason, e.domain) == (Code.INVALID_ARGUMENT, "BATCH_TOO_LARGE", "be")
    assert e.metadata == {"field": field, "max": mx, "got": got}


async def test_errors_cross_the_wire(pair):
    _, _, _, stub, _ = pair
    e = await _err(stub.BatchGet(peer_pb2.BatchGetRequest(ids=["fail"])))
    assert (e.code, e.reason, e.domain, e.metadata) == (Code.FAILED_PRECONDITION, "QUOTA_EXCEEDED",
                                                        "conformance/peer", {"available": "2"})
    e = await _err(stub.BatchGet(peer_pb2.BatchGetRequest(ids=["crash"])))
    assert (e.code, e.reason, e.domain) == (Code.INTERNAL, "INTERNAL", "be") and "secret" not in str(e)


async def test_retry_only_idempotent_methods(pair):
    peer, _, _, stub, _ = pair
    peer.reserve_fail = 2
    assert (await stub.Reserve(peer_pb2.ReserveRequest(idempotency_key="k"))).attempts == 3
    peer.notify_fail = 1
    e = await _err(stub.Notify(peer_pb2.ReserveRequest()))
    assert e.code == Code.UNAVAILABLE and peer.calls["Notify"] == 1


async def test_outbound_deadline(pair):
    peer, _, _, stub, _ = pair
    await stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"]))
    assert 2.5 < peer.remaining <= 3.05  # min(3 s, remaining − 50 ms); grpc-timeout has coarse units
    with context.scope(deadline=context.deadline_in(0.03)):
        e = await _err(stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"])))
    assert (e.code, e.reason) == (Code.DEADLINE_EXCEEDED, "DEADLINE_BUDGET_EXHAUSTED")


async def test_server_deadline_floor(pair):
    peer, _, _, _, server = pair
    async with grpc.aio.insecure_channel(f"127.0.0.1:{server.port}") as ch:
        await peer_pb2_grpc.PeerServiceStub(ch).BatchGet(peer_pb2.BatchGetRequest(ids=["a"]),
                                                         metadata=(("be-caller", "x/y"),))
    assert 9 < peer.remaining <= 10


async def test_bulkhead_64(pair):
    peer, _, _, stub, _ = pair
    peer.block = asyncio.Event()
    calls = [asyncio.ensure_future(stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"]))) for _ in range(64)]
    await asyncio.sleep(0.3)
    e = await _err(stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"])))
    assert (e.code, e.reason) == (Code.RESOURCE_EXHAUSTED, "OUTBOUND_LIMIT")
    peer.block.set()
    await asyncio.gather(*calls)


async def test_no_network_inside_a_transaction(pair):
    _, _, _, stub, _ = pair
    with context.scope(tx=object()):
        e = await _err(stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"])))
    assert (e.code, e.reason) == (Code.INTERNAL, "NETWORK_IN_TX")


async def test_red_metrics(pair):
    _, srv_rt, cli_rt, stub, _ = pair
    await stub.BatchGet(peer_pb2.BatchGetRequest(ids=["a"]))
    assert ('be_grpc_server_handled_total{code="OK",component="conformance/peer",method="BatchGet",'
            'service="conformance.peer.v1.PeerService"} 1.0') in generate_latest(srv_rt.registry).decode()
    assert ('be_grpc_client_handled_total{code="OK",component="conformance/widget-py",method="BatchGet",'
            'target="conformance/peer"} 1.0') in generate_latest(cli_rt.registry).decode()
