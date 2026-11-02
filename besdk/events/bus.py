"""Opens the transport BUS_URL names (P6.16): nats:// for JetStream, pgqueue:// for the PostgreSQL queue. The
launcher and the shell open one per process; members share it."""
from __future__ import annotations

import asyncio

from besdk.config import Key, Type
from besdk.db.pool import StoppedWaiting
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
    raise BusRefused("BUS_URL: the scheme must be nats:// or pgqueue://")


class BusRefused(ValueError):
    """The bus refused the credentials, or BUS_URL names no transport; retrying can't help: exit 78."""


class BusStartCheckError(Exception):
    """The PostgreSQL queue's database does not exist: exit 69."""


async def ready_bus(url: str, name: str, log: Logger, stop: asyncio.Event):
    """Open the bus and wait until it answers (P6.34): the bus may start after the component, as the database may
    (P5.10). Connection failures are retried every second until stop is set (then StoppedWaiting), the component
    staying "starting"; refused credentials, a wrong scheme and a missing queue database end the wait."""
    attempt = 0
    while True:
        attempt += 1
        try:
            t = await _open_once(url, name, log)
            if attempt > 1:
                log.info("the bus answers", attempts=attempt)
            return t
        except (BusRefused, BusStartCheckError):
            raise
        except (OSError, ConnectionError, TimeoutError) as e:
            if attempt == 1 or attempt % 10 == 0:
                log.warn("waiting for the bus", error=str(e))
        try:
            await asyncio.wait_for(stop.wait(), 1)
            raise StoppedWaiting()
        except TimeoutError:
            continue


async def _open_once(url: str, name: str, log: Logger):
    """One attempt: open_bus, and for the lazy PostgreSQL queue a query, with the failures that retrying can't fix
    raised as such."""
    import asyncpg
    import nats.errors

    try:
        t = await open_bus(url, name, log)
    except ConnectionError as e:
        if isinstance(e.__cause__, (nats.errors.AuthorizationError,)) or "authorization violation" in str(e).lower():
            raise BusRefused(str(e)) from e
        raise
    if not url.startswith("pgqueue://"):
        return t
    try:
        await t.pool.fetchval("SELECT 1")
        return t
    except (asyncpg.InvalidPasswordError, asyncpg.InvalidAuthorizationSpecificationError) as e:
        await t.close()
        raise BusRefused(f"bus: {e}") from e
    except asyncpg.InvalidCatalogNameError as e:
        await t.close()
        raise BusStartCheckError(f"bus: {e} (not provisioned)") from e
    except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError) as e:
        await t.close()
        raise ConnectionError(f"bus: {e}") from e
