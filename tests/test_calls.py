import asyncio
import contextlib
import io
import time

import pytest

from besdk import errs, reqctx
from besdk.calls import policy
from besdk.calls.client import Member
from besdk.calls.outcome import Result
from besdk.obs import Member as ObsMember, new_logger, new_registry
from besdk.runtime import Runtime
from besdk.testing import World


def new_dep():
    d = Member().dependency("demo/c", "grpc")

    async def no_sleep(_):
        return None

    d.sleep = no_sleep  # backoff is covered by the vectors
    return d


@contextlib.contextmanager
def deadline(seconds):
    with reqctx.deadline_scope(time.monotonic() + seconds):
        yield


BUSY = errs.new("SERVICE_BUSY", "busy")


def failing(counter, e):
    async def attempt(timeout):
        counter[0] += 1
        return Result(err=e, code=e.code.value, remote=e)

    return attempt


async def test_retries_only_idempotent_calls_at_most_three_times():
    d, n = new_dep(), [0]
    with deadline(5), pytest.raises(errs.Error) as e:
        await d.do(True, failing(n, BUSY))
    assert n[0] == 3 and e.value.reason == "DEPENDENCY_UNAVAILABLE" and e.value.metadata["dependency"] == "demo/c"
    n[0] = 0
    with deadline(5), pytest.raises(errs.Error) as e:
        await d.do(False, failing(n, BUSY))
    assert n[0] == 1, "a command is never retried"


async def test_relayed_failures_are_neither_retried_nor_counted():
    d, n = new_dep(), [0]
    relayed = errs.new("DEPENDENCY_UNAVAILABLE", "further down").with_("dependency", "demo/d")
    for _ in range(30):
        with deadline(5), pytest.raises(errs.Error) as e:
            await d.do(True, failing(n, relayed))
        assert e.value is relayed, "passed upward unchanged"
    assert n[0] == 30 and d.breaker.state == policy.CLOSED and d.budget.tokens == 10000


async def test_the_budget_stops_retries_when_the_dependency_keeps_failing():
    d, n = new_dep(), [0]
    for _ in range(4):
        with deadline(5), contextlib.suppress(errs.Error):
            await d.do(True, failing(n, BUSY))
    before = n[0]
    with deadline(5), contextlib.suppress(errs.Error):
        await d.do(True, failing(n, BUSY))
    assert n[0] - before == 1


async def test_the_breaker_opens_and_fails_fast_without_sending():
    d, n, now = new_dep(), [0], [1000.0]
    d.now = lambda: now[0]

    async def timeout(_):
        n[0] += 1
        return Result(err=TimeoutError(), code="DEADLINE_EXCEEDED")

    for _ in range(policy.BREAKER_WINDOW):
        with deadline(5), pytest.raises(errs.Error) as e:
            await d.do(False, timeout)
        assert e.value.reason == "RESULT_UNKNOWN", "a command that timed out: query first"
    assert d.breaker.state == policy.OPEN
    sent = n[0]
    with deadline(5), pytest.raises(errs.Error) as e:
        await d.do(True, timeout)
    assert e.value.reason == "CIRCUIT_OPEN" and n[0] == sent, "nothing sent while open"
    now[0] += policy.BREAKER_OPEN_FOR

    async def ok(_):
        return Result()

    for _ in range(policy.BREAKER_TRIALS_TO_CLOSE):
        with deadline(5):
            await d.do(True, ok)
    assert d.breaker.state == policy.CLOSED


async def test_outbound_limit():
    d, release = new_dep(), asyncio.Event()

    async def held(_):
        await release.wait()
        return Result()

    async def one():
        with deadline(5):
            await d.do(True, held)

    tasks = [asyncio.create_task(one()) for _ in range(policy.OUTBOUND_LIMIT)]
    await asyncio.sleep(0.05)
    n = [0]
    with deadline(5), pytest.raises(errs.Error) as e:
        await d.do(True, failing(n, BUSY))
    assert e.value.reason == "OUTBOUND_LIMIT" and e.value.metadata["dependency"] == "demo/c" and n[0] == 0
    release.set()
    await asyncio.gather(*tasks)


async def test_deadlines():
    d, n = new_dep(), [0]
    with pytest.raises(errs.Error) as e:
        await d.do(True, failing(n, BUSY))
    assert e.value.reason == "DEADLINE_BUDGET_EXHAUSTED", "no deadline: not sent"
    with deadline(0.04), pytest.raises(errs.Error) as e:
        await d.do(True, failing(n, BUSY))
    assert e.value.reason == "DEADLINE_BUDGET_EXHAUSTED" and n[0] == 0, "under 50 ms left: not sent"
    seen = []

    async def record(timeout):
        seen.append((timeout, reqctx.remaining()))
        return Result()

    with deadline(10):
        await d.do(True, record)
    with deadline(1):
        await d.do(True, record)
    assert abs(seen[0][0] - 3.0) < 0.05 and abs(seen[0][1] - 3.0) < 0.05, "one attempt gets at most 3 s"
    assert abs(seen[1][0] - 0.95) < 0.05, "remaining minus 50 ms"


async def test_an_attempt_past_its_timeout_counts_as_a_deadline():
    d = new_dep()

    async def hangs(_):
        await asyncio.sleep(10)
        return Result()

    with deadline(0.3), pytest.raises(errs.Error) as e:
        await d.do(False, hangs)
    assert e.value.reason == "RESULT_UNKNOWN"



def test_configured_addresses(tmp_path):  # P4.26
    out = Member()
    rt = Runtime(ObsMember("test/probe-py", "3.1.0"), None, new_logger(ObsMember("test/probe-py", "3.1.0"), "info",  # type: ignore[arg-type]
                 io.StringIO()), new_registry(), None, outbound=out)  # type: ignore[arg-type]
    assert rt.http_at("test/probe-py2", "http://test-probe-py2-3-1-0:8605") is not None
    assert rt.http_at("test/probe-py2", "") is None and rt.grpc_at("test/probe-py2", "") is None
    with pytest.raises(ValueError, match="no service identity"):
        rt.grpc_at("test/probe-py2", "http://test-probe-py2-3-1-0:9605")  # never plaintext
    out.set_tls(World(tmp_path).tls("test/probe-py"))
    ch = rt.grpc_at("test/probe-py2", "http://test-probe-py2-3-1-0:9605")
    assert ch is rt.grpc_at("test/probe-py2", "test-probe-py2-3-1-0:9605"), "one channel per component and address"


class _Call:
    def __init__(self, err):
        self.err = err

    def __await__(self):
        async def answer():
            if self.err is not None:
                raise self.err
            return "ok"
        return answer().__await__()


async def test_background_calls_are_neither_counted_nor_held_by_the_breaker():  # P4.27
    import grpc

    from besdk.calls.client import _Interceptor, options

    d = Member().dependency("infra/authz", "grpc")

    async def no_sleep(_):
        return None

    d.sleep = no_sleep
    sent = [0]
    down = grpc.aio.AioRpcError(grpc.StatusCode.UNAVAILABLE, grpc.aio.Metadata(), grpc.aio.Metadata(), "connection refused")

    def continuation(err):
        async def cont(details, request):
            sent[0] += 1
            return _Call(err)
        return cont

    def details(md):
        return grpc.aio.ClientCallDetails("/m", None, md, None, None)

    i = _Interceptor(d)
    for _ in range(2 * policy.BREAKER_WINDOW):
        with deadline(5), contextlib.suppress(errs.Error):
            await i.intercept_unary_unary(continuation(down), details(options(idempotent=True, background=True)), None)
    assert d.breaker.state == policy.CLOSED, "background failures are not counted"
    for _ in range(policy.BREAKER_WINDOW):
        with deadline(5), contextlib.suppress(errs.Error):
            await i.intercept_unary_unary(continuation(down), details([]), None)
    assert d.breaker.state == policy.OPEN, "ordinary failures open it"
    before = sent[0]
    with deadline(5):
        assert await i.intercept_unary_unary(continuation(None), details(options(idempotent=True, background=True)), None) == "ok"
    assert sent[0] == before + 1, "a background call is sent while the breaker is open"
    assert d.breaker.state == policy.OPEN, "and its success does not close it"
    with deadline(5), pytest.raises(errs.Error) as e:
        await i.intercept_unary_unary(continuation(None), details(options(idempotent=True)), None)
    assert e.value.reason == "CIRCUIT_OPEN", "ordinary calls are still held"


def test_system_channels_resolve_the_host_again_after_a_failure():  # P4.28
    """gRPC's resolver waits 30 s between resolutions by default; a dependency that restarted on another address (a
    container's new IP) must be reached within seconds, so the channel lowers that wait and the reconnection backoff."""
    m = Member()
    m.set_tls(object())
    ch = m.grpc("infra/authz", "infra-authz:9223")
    opts = dict(ch._options)
    assert opts["grpc.dns_min_time_between_resolutions_ms"] <= 1000
    assert opts["grpc.max_reconnect_backoff_ms"] <= 5000
