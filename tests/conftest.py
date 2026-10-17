import pytest

from tests.sectest import World


@pytest.fixture
async def world(tmp_path):
    """A test deployment's issuer, trust root and fake authorization and audit members (tests/sectest.py)."""
    w = await World(tmp_path).start()
    try:
        yield w
    finally:
        await w.stop()
