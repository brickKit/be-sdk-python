"""Wiring a member's ``Events`` declaration: the publisher behind ``tx.publish``, the bus connection
(process-wide, connected in the background with 0.5 s → 15 s backoff, P1.2), the outbox pump and one
consumer per subscription, all supervised (P1.7)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from besdk.config_values import parse_duration_ns
from besdk.events.bus import Bus
from besdk.events.consumer import Consumer
from besdk.events.contract import Contract
from besdk.events.outbox import IDLE, OutboxPump, Publisher

if TYPE_CHECKING:
    from besdk.runtime import Module, Runtime

DEFAULT_MAX_DELIVER = 8
DEFAULT_BACKOFF = "1s,10s,1m,5m,15m,30m,1h"


class EventsRuntime:
    def __init__(self, rt: "Runtime", module: "Module"):
        self.rt, self.module = rt, module
        self.contract = Contract.load(Path(rt.spec.contracts))
        self.publisher = Publisher(rt.id, rt.version, self.contract, list(module.events.publishes),
                                   rt.telemetry.propagator, rt.logger)
        self.pump: OutboxPump | None = None
        self.consumers: list[Consumer] = []
        self.started = asyncio.Event()

    @property
    def wanted(self) -> bool:
        ev = self.module.events
        return bool(ev.publishes or ev.subscribe)

    def bus_url(self) -> str:
        c = self.rt.config
        for k in ("EVENT_BUS_URL", "NATS_URL"):
            if k in c.declared() and c.string(k):
                return c.string(k)
        raise ValueError("events declared but neither EVENT_BUS_URL nor NATS_URL is configured")

    def _settings(self, sub) -> tuple[int, list[int]]:
        """``EVENTS_MAX_DELIVER`` / ``EVENTS_BACKOFF`` when set, else the subscription's own, else 8 and the
        default schedule (P12.5)."""
        c, declared = self.rt.config, self.rt.config.declared()
        if "EVENTS_MAX_DELIVER" in declared and c.present("EVENTS_MAX_DELIVER"):
            md = c.int("EVENTS_MAX_DELIVER")
        else:
            md = sub.max_deliver or DEFAULT_MAX_DELIVER
        if "EVENTS_BACKOFF" in declared and c.present("EVENTS_BACKOFF"):
            bo = [int(x * 1e9) for x in c.durations("EVENTS_BACKOFF")]
        elif sub.backoff:
            bo = [int(x * 1e9) for x in sub.backoff]
        else:
            bo = [parse_duration_ns(x) for x in DEFAULT_BACKOFF.split(",")]
        return md, bo

    async def start(self) -> None:
        if not self.wanted:
            return
        rt = self.rt
        store = rt.store()
        store.publisher = self.publisher
        if rt.shared.bus is None:
            rt.shared.bus = Bus(self.bus_url(), rt.id, rt.logger)
        rt.supervisor.start("be.events", self._bring_up)

    async def _bring_up(self) -> None:
        rt, bus = self.rt, self.rt.shared.bus
        delay = 0.5
        while True:
            try:
                await bus.connect()
                break
            except Exception as e:  # noqa: BLE001 - not up yet: retry (P1.2, P12.13)
                rt.logger.warning("bus_unreachable", extra={"error": f"{type(e).__name__}: {e}", "retry_in_s": delay})
                await asyncio.sleep(delay)
                delay = min(delay * 2, 15.0)
        for subject in self.module.events.publishes:
            await bus.ensure_stream_for(subject)
        src = rt.shared.bundle_source
        if src is not None:  # best-effort poke: fetch the bundle at once (P6.1, P12.10)
            async def poke(_msg) -> None:
                src.poke()

            await bus.nc.subscribe("infra.authz.changed.v1", cb=poke)
        store = rt.store()
        jobs = rt.jobs
        if self.module.events.publishes and (jobs is None or jobs.enabled("be.outbox")):
            idle = jobs.interval("be.outbox", IDLE) if jobs is not None else IDLE
            self.pump = OutboxPump(store, bus, self.publisher, rt.metrics, rt.logger, idle=idle)
            rt.supervisor.start("be.outbox", self.pump.run)
        for sub in self.module.events.subscribe:
            md, bo = self._settings(sub)
            c = Consumer(member=rt.id, sub=sub, store=store, bus=bus, contract=self.contract, max_deliver=md,
                         backoff_ns=bo, metrics=rt.metrics, logger=rt.logger, tracer=rt.tracer,
                         propagator=rt.telemetry.propagator)
            await c.ensure()
            self.consumers.append(c)
            rt.supervisor.start(f"be.consumer.{c.durable}", c.run)
        self.started.set()

    async def stop(self) -> None:
        """Consumers stop fetching and finish in-flight deliveries; the pump finishes its batch (P1.6)."""
        for c in self.consumers:
            await c.drain()
