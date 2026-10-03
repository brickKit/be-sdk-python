"""P7.7 / P3.4: once the outbound budget has ended the call failed because of the budget, whatever
status gRPC reports at that moment. A stream reset at the budget's end comes back as CANCELLED,
INTERNAL or UNAVAILABLE instead of DEADLINE_EXCEEDED; the caller must still answer 504."""
import asyncio

import grpc
import pytest

from besdk import Code, Error, Module
from besdk.rpc.client import BeChannel, _UnaryCall
from tests.unit._rt import build

METHOD = "/conformance.peer.v1.PeerService/BatchGet"


class _Bulkhead:
    def try_acquire(self) -> bool:
        return True

    def release(self) -> None:
        pass


def _rpc_error(code: grpc.StatusCode) -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(code, grpc.aio.Metadata(), grpc.aio.Metadata(), details="stream terminated")


async def _empty(rt):
    return Module()


def _call(tmp_path, inner) -> _UnaryCall:
    rt, _, _ = build(tmp_path, _empty)
    return _UnaryCall(inner, BeChannel(rt, "peer", None, _Bulkhead()), METHOD)


@pytest.mark.parametrize("code", [grpc.StatusCode.CANCELLED, grpc.StatusCode.INTERNAL,
                                  grpc.StatusCode.UNKNOWN, grpc.StatusCode.UNAVAILABLE])
async def test_budget_end_is_the_deadline_whatever_grpc_reports(tmp_path, code):
    async def at_budget_end(request, *, timeout, metadata, **kw):
        await asyncio.sleep(timeout)  # the outbound budget has ended
        raise _rpc_error(code)

    with pytest.raises(Error) as e:
        await _call(tmp_path, at_budget_end)(object(), timeout=0.1)
    assert (e.value.domain, e.value.reason, e.value.code) == ("be", "DEADLINE_BUDGET_EXHAUSTED", Code.DEADLINE_EXCEEDED)


async def test_an_error_before_the_budget_ends_is_kept(tmp_path):
    async def early(request, *, timeout, metadata, **kw):
        raise _rpc_error(grpc.StatusCode.INTERNAL)

    with pytest.raises(Error) as e:
        await _call(tmp_path, early)(object(), timeout=1.0)
    assert e.value.code == Code.INTERNAL
    assert e.value.reason != "DEADLINE_BUDGET_EXHAUSTED"
