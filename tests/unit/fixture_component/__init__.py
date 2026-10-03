"""The tiny component: ``python -m tests.unit.fixture_component`` serves it."""
import asyncio
from pathlib import Path

import besdk

HERE = Path(__file__).parent


def routes(r: besdk.Router):
    @r.get("/slow", guard=besdk.PUBLIC)
    async def slow(ms: int = 500):
        await asyncio.sleep(ms / 1000)
        return {"slept": ms}


async def create(rt: besdk.Runtime) -> besdk.Module:
    rt.config.int("TINY_LIMIT")
    return besdk.Module(http=routes)


spec = besdk.Spec(id="conformance/tiny", migrations=HERE / "migrations", contracts=HERE / "contracts", create=create)
