"""P6.31: a subscription's effective concurrency."""
import pytest

from besdk.events.consumer import Consumer, Sub, concurrency, consumer_keys


async def _noop(tx, e):
    return None


@pytest.mark.parametrize("sub,configured,want", [
    (Sub("s", ["x"], _noop), 0, 1),  # nothing set: one at a time
    (Sub("s", ["x"], _noop, concurrency=2), 0, 2),  # the declaration
    (Sub("s", ["x"], _noop, concurrency=2), 8, 8),  # the deployment over the declaration
    (Sub("s", ["x"], _noop), 4, 4),  # the deployment without a declaration
    (Sub("s", ["x"], _noop, concurrency=4, ordered=True), 8, 1),  # ordered stays one
])
def test_the_effective_concurrency_of_a_subscription(sub, configured, want):  # P6.31
    assert concurrency(sub, configured) == want
    assert Consumer("demo/a", sub, None, None, None, configured).spec().concurrency == want  # type: ignore[arg-type]


def test_the_key_is_bounded():
    (k,) = consumer_keys()
    assert (k.name, k.min, k.max) == ("BUS_SUBSCRIPTION_CONCURRENCY", 1, 64)
