"""Opens the transport BUS_URL names (P6.16): nats:// for JetStream, pgqueue:// for the PostgreSQL queue. The
launcher and the shell open one per process; members share it."""
from __future__ import annotations

from besdk.config import Key, Type
from besdk.obs import Logger


def keys(produces: bool) -> list[Key]:
    """BUS_URL for every component that produces or consumes events; IMPLEMENTATION_INSTANCE for producers."""
    out = [Key("BUS_URL", Type.STRING, required=True, secret=True, process=True)]
    return out + ([Key("IMPLEMENTATION_INSTANCE", Type.STRING, required=True)] if produces else [])


async def open_bus(url: str, name: str, log: Logger):
    if url.startswith(("nats://", "tls://")):
        from besdk.events.jetstream import Transport as JetStream

        return await JetStream.connect(url, name)
    if url.startswith("pgqueue://"):
        from besdk.events.pgqueue import Transport as Queue

        t = await Queue.connect(url, name)
        t.on_error = lambda e: log.warn("bus", error=repr(e))
        return t
    raise ValueError("BUS_URL: the scheme must be nats:// or pgqueue://")
