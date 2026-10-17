"""The transport conformance suite of P6.16-P6.24, the same cases as the Go SDK's transporttest: every adapter runs
them, and a broken adapter must fail them. A case takes a factory returning (transport, unique stream name) and
raises AssertionError when the transport misbehaves."""
from __future__ import annotations

import asyncio
import time

from besdk.events.core import Delivery, Message, Outcome, SubscriptionSpec

ACK_WAIT, BACKOFF = 1.5, 0.2


def msg(stream: str, partition: int, typ: str, data: str) -> Message:
    eid = __import__("uuid").uuid7()
    return Message(f"be.{stream}.{partition}.{typ}", {"ce-id": str(eid), "ce-type": typ}, data.encode())


def spec(stream: str, durable: str) -> SubscriptionSpec:
    # durables are unique per case: some transports keep them in one shared namespace
    return SubscriptionSpec(f"{stream.replace('.', '_')}_{durable}", [stream], ack_wait=ACK_WAIT, backoff=[BACKOFF])


class Recorder:
    def __init__(self) -> None:
        self.got: list[Delivery] = []

    def add(self, d: Delivery) -> None:
        self.got.append(d)

    def distinct(self) -> int:
        return len({d.id for d in self.got})


async def eventually(within: float, cond, what: str) -> None:
    end = time.monotonic() + within
    while time.monotonic() < end:
        if cond():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("never: " + what)


def ack_into(rec: Recorder, outcome: Outcome = Outcome.ACK):
    async def h(d: Delivery) -> Outcome:
        rec.add(d)
        return outcome

    return h


async def offline_consumer_receives_what_was_published_meanwhile(tr, stream, subs):
    rec = Recorder()
    sub = await tr.subscribe(spec(stream, "offline"), ack_into(rec))
    await sub.stop()
    for i in range(3):
        await tr.publish(msg(stream, i, "demo.thing_happened.v1", str(i)))
    subs.append(await tr.subscribe(spec(stream, "offline"), ack_into(rec)))
    await eventually(10, lambda: rec.distinct() == 3, "three messages after coming back")


async def instances_of_one_durable_each_message_once(tr, stream, subs):
    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "compete"), ack_into(rec)))
    subs.append(await tr.subscribe(spec(stream, "compete"), ack_into(rec)))
    for i in range(40):
        await tr.publish(msg(stream, i % 4, "demo.thing_happened.v1", str(i)))
    await eventually(15, lambda: rec.distinct() == 40, "all 40 handled")
    await asyncio.sleep(ACK_WAIT + 0.5)
    assert len(rec.got) == 40, f"{len(rec.got)} deliveries for 40 messages: each is handled by one instance once"


async def an_acknowledged_message_is_never_delivered_again(tr, stream, subs):
    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "acked"), ack_into(rec)))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "x"))
    await eventually(10, lambda: len(rec.got) == 1, "delivered")
    await asyncio.sleep(2 * ACK_WAIT + 0.5)
    assert len(rec.got) == 1, f"{len(rec.got)} deliveries of an acknowledged message"


async def a_nak_is_redelivered_after_the_backoff(tr, stream, subs):
    rec = Recorder()

    async def h(d):
        rec.add(d)
        return Outcome.NAK if d.attempt == 1 else Outcome.ACK

    subs.append(await tr.subscribe(spec(stream, "nak"), h))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "x"))
    await eventually(10, lambda: len(rec.got) == 2, "redelivered once")
    assert rec.got[1].attempt == 2, f"the redelivery says attempt {rec.got[1].attempt}, want 2"
    await asyncio.sleep(ACK_WAIT)
    assert len(rec.got) == 2, f"{len(rec.got)} deliveries; the second was acknowledged"


async def an_unacknowledged_message_is_redelivered_after_the_ack_wait(tr, stream, subs):
    rec = Recorder()
    s = spec(stream, "slow")
    s.concurrency = 2  # the redelivery reaches another handler while the first one hangs

    async def h(d):
        rec.add(d)
        if d.attempt == 1:
            await asyncio.sleep(ACK_WAIT + 0.7)  # the handler hangs past the ack wait
        return Outcome.ACK

    subs.append(await tr.subscribe(s, h))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "x"))
    await eventually(3 * ACK_WAIT + 5, lambda: len(rec.got) >= 2, "redelivered after the ack wait")


async def in_progress_keeps_a_slow_message_from_being_redelivered(tr, stream, subs):
    rec = Recorder()

    async def h(d):
        rec.add(d)
        for _ in range(6):
            await asyncio.sleep(ACK_WAIT / 3)
            await d.in_progress()
        return Outcome.ACK

    subs.append(await tr.subscribe(spec(stream, "progress"), h))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "x"))
    await eventually(10, lambda: len(rec.got) >= 1, "delivered")
    await asyncio.sleep(2 * ACK_WAIT + 1)
    assert len(rec.got) == 1, f"{len(rec.got)} deliveries of a message kept in progress"


async def max_deliver_stops_the_deliveries(tr, stream, subs):
    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "poison"), ack_into(rec, Outcome.NAK)))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "poison"))
    await eventually(10, lambda: len(rec.got) == 3, "three deliveries")
    await asyncio.sleep(ACK_WAIT + 1)
    assert len(rec.got) == 3, f"{len(rec.got)} deliveries, the limit is 3"
    assert [d.attempt for d in rec.got] == [1, 2, 3]


async def term_ends_the_deliveries_at_once(tr, stream, subs):
    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "term"), ack_into(rec, Outcome.TERM)))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "x"))
    await eventually(10, lambda: len(rec.got) == 1, "delivered")
    await asyncio.sleep(ACK_WAIT + 1)
    assert len(rec.got) == 1, f"{len(rec.got)} deliveries after term"


async def creating_a_durable_again_keeps_its_settings(tr, stream, subs):
    rec = Recorder()
    s = spec(stream, "settings")
    sub = await tr.subscribe(s, ack_into(rec, Outcome.NAK))
    await sub.stop()
    s.max_deliver = 6
    subs.append(await tr.subscribe(s, ack_into(rec, Outcome.NAK)))
    await tr.publish(msg(stream, 0, "demo.thing_happened.v1", "x"))
    await eventually(10, lambda: len(rec.got) >= 3, "deliveries")
    await asyncio.sleep(2 * ACK_WAIT + 1)
    assert len(rec.got) == 3, f"{len(rec.got)} deliveries: the existing durable's limit of 3 was changed"


async def headers_and_payload_round_trip_unchanged(tr, stream, subs):
    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "roundtrip"), ack_into(rec)))
    m = msg(stream, 2, "demo.thing_happened.v1", '{"name":"仓库 1","bytes":"\\u0000ÿ"}')
    m.headers.update({"ce-besubjects": '["acc:01","org:7"]', "ce-belegalentity": "法人 A",
                      "ce-traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
    await tr.publish(m)
    await eventually(10, lambda: len(rec.got) == 1, "delivered")
    d = rec.got[0]
    assert d.subject == m.subject and d.data == m.data, (d.subject, d.data)
    for k, v in m.headers.items():
        assert d.headers.get(k) == v, (k, d.headers.get(k), v)


async def a_message_over_the_limit_fails_to_publish(tr, stream, subs):
    from besdk.events.core import TooLarge

    big = msg(stream, 0, "demo.thing_happened.v1", "")
    big.data = bytes(tr.max_message + 1)
    try:
        await tr.publish(big)
    except TooLarge:
        return
    raise AssertionError(f"a message over {tr.max_message} bytes was accepted")


async def the_same_message_id_is_delivered_once(tr, stream, subs):
    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "dedup"), ack_into(rec)))
    m = msg(stream, 0, "demo.thing_happened.v1", "x")
    await tr.publish(m)
    await tr.publish(m)  # the forwarder's ack was lost and it published again
    await eventually(10, lambda: len(rec.got) >= 1, "delivered")
    await asyncio.sleep(ACK_WAIT)
    assert len(rec.got) == 1, f"{len(rec.got)} deliveries of one message ID"


async def two_durables_each_receive_a_copy(tr, stream, subs):
    a, b = Recorder(), Recorder()
    subs.append(await tr.subscribe(spec(stream, "member_a"), ack_into(a)))
    subs.append(await tr.subscribe(spec(stream, "member_b"), ack_into(b)))
    for i in range(5):
        await tr.publish(msg(stream, i % 4, "demo.thing_happened.v1", str(i)))
    await eventually(10, lambda: a.distinct() == 5 and b.distinct() == 5, "five each")


async def filters_select_subjects(tr, stream, subs):
    rec = Recorder()
    s = spec(stream, "filtered")
    s.filters = [f"be.{stream}.*.demo.wanted.v1"]
    subs.append(await tr.subscribe(s, ack_into(rec)))
    await tr.publish(msg(stream, 1, "demo.unwanted.v1", "no"))
    await tr.publish(msg(stream, 2, "demo.wanted.v1", "yes"))
    await eventually(10, lambda: len(rec.got) == 1, "the wanted one")
    await asyncio.sleep(0.5)
    assert len(rec.got) == 1 and rec.got[0].data == b"yes", [d.data for d in rec.got]


async def a_subscriber_may_start_before_any_producer(tr, stream, subs):  # P6.17
    from besdk.events.core import Stream

    rec = Recorder()
    subs.append(await tr.subscribe(spec(stream, "early"), ack_into(rec)))  # the stream does not exist yet
    await tr.ensure_stream(Stream(stream, 4))  # the producer starts later
    await tr.publish(msg(stream, 1, "demo.thing_happened.v1", "late"))
    await eventually(10, lambda: len(rec.got) == 1, "delivered to the early subscriber")


async def notifications_are_neither_stored_nor_redelivered(tr, stream, subs):
    subj = "be.notify." + stream
    await tr.notify(subj, b"before")
    got: list[bytes] = []
    subs.append(await tr.listen(subj, got.append))
    await asyncio.sleep(0.3)
    await tr.notify(subj, b"while")
    await eventually(5, lambda: len(got) >= 1, "received")
    await asyncio.sleep(ACK_WAIT)
    assert got == [b"while"], got


CASES = [offline_consumer_receives_what_was_published_meanwhile, instances_of_one_durable_each_message_once,
         an_acknowledged_message_is_never_delivered_again, a_nak_is_redelivered_after_the_backoff,
         an_unacknowledged_message_is_redelivered_after_the_ack_wait,
         in_progress_keeps_a_slow_message_from_being_redelivered, max_deliver_stops_the_deliveries,
         term_ends_the_deliveries_at_once, creating_a_durable_again_keeps_its_settings,
         headers_and_payload_round_trip_unchanged, a_message_over_the_limit_fails_to_publish,
         the_same_message_id_is_delivered_once, two_durables_each_receive_a_copy, filters_select_subjects,
         a_subscriber_may_start_before_any_producer, notifications_are_neither_stored_nor_redelivered]


async def run(case, tr, stream) -> None:
    subs: list = []
    try:
        await case(tr, stream, subs)
    finally:
        await asyncio.gather(*(s.stop() for s in subs), return_exceptions=True)
