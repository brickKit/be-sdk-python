import io
import os
import uuid
from urllib.parse import urlsplit

import pytest

from besdk import obs
from besdk.events import jetstream, pgqueue
from besdk.events.core import Outcome
from tests import pgtest, transport_suite

pytestmark = pytest.mark.integration
LOG_BUS = obs.new_logger(obs.Member("bus", "0"), "error", io.StringIO())


def bus_url() -> str:
    u = urlsplit(pgtest.admin_dsn(pgtest.DATABASE))
    return u._replace(scheme="pgqueue", query="schema=py_bus_test&sslmode=disable").geturl()


async def pg_transport():
    await pgtest.ensure_database(pgtest.DATABASE)
    a = await pgtest.admin()
    try:
        await a.execute("CREATE SCHEMA IF NOT EXISTS py_bus_test")
    finally:
        await a.close()
    await pgqueue.migrate_bus(bus_url(), LOG_BUS)
    return await pgqueue.Transport.connect(bus_url(), "be-sdk-python-test")


async def js_transport():
    url = os.environ.get("BE_SDK_TEST_NATS_URL", "")
    assert url, "BE_SDK_TEST_NATS_URL is not set: the JetStream tests need a NATS server"
    return await jetstream.Transport.connect(url, "be-sdk-python-test")


class LosesAcks:
    """A broken adapter: it reports every handler outcome as acknowledged but never settles a nak (P6.24)."""

    def __init__(self, inner):
        self.inner = inner

    def __getattr__(self, name):
        return getattr(self.inner, name)

    async def subscribe(self, spec, h):
        async def swallow(d):
            await h(d)
            return Outcome.ACK  # the outcome is lost: a nak is never redelivered

        return await self.inner.subscribe(spec, swallow)


@pytest.mark.parametrize("case", transport_suite.CASES, ids=lambda c: c.__name__)
@pytest.mark.parametrize("adapter", ["jetstream", "pgqueue"])
async def test_transport_suite(adapter, case):  # P6.16-P6.24, CK-BUS-01..08
    tr = await (js_transport() if adapter == "jetstream" else pg_transport())
    stream = "pysuite.s" + uuid.uuid4().hex[:10]
    try:
        if adapter == "jetstream" and case is not transport_suite.a_subscriber_may_start_before_any_producer:
            from besdk.events.core import Stream

            await tr.ensure_stream(Stream(stream, 4))
        await transport_suite.run(case, tr, stream)
    finally:
        if adapter == "jetstream":
            try:
                await tr.delete_stream(stream)
            except Exception:  # noqa: BLE001
                pass
        await tr.close()


async def test_the_suite_fails_an_adapter_that_loses_acks():  # P6.24
    tr = await pg_transport()
    try:
        with pytest.raises(AssertionError):
            await transport_suite.run(transport_suite.a_nak_is_redelivered_after_the_backoff, LosesAcks(tr),
                                      "pysuite.s" + uuid.uuid4().hex[:10])
    finally:
        await tr.close()
