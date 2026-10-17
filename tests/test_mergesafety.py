import pathlib

from besdk import mergesafety

BAD = """
import os
import os as o
import sys
import logging
from os import environ as e
from prometheus_client import Counter, Histogram, REGISTRY
import prometheus_client as pc
import opentelemetry.trace as ot
import asyncpg
import httpx
import uvicorn

def wire(rt):
    a = os.getenv("PG_PASSWORD")
    b = o.environ["X"]
    c = e.get("Y")
    logging.basicConfig()
    log = logging.getLogger("x")
    Counter("hits", "Hits")
    Histogram("ok", "Fine", registry=rt.metrics)
    pc.Gauge("g", "G")
    ot.set_tracer_provider(None)
    asyncpg.create_pool("postgres://")
    httpx.AsyncClient()
    uvicorn.run(None)
    sys.exit(1)
    exit(2)
"""


def test_finds_every_kind():
    names = [f.name for f in mergesafety.scan_source(BAD, "bad.py")]
    # an aliased import is reported where it is imported: `from os import environ as e` once, besides o.environ
    assert names.count("os.environ") == 2, names
    for want in ["os.getenv", "logging.basicConfig", "logging.getLogger",
                 "prometheus_client.Counter", "prometheus_client.REGISTRY", "prometheus_client.Gauge",
                 "opentelemetry.trace.set_tracer_provider", "asyncpg.create_pool", "httpx.AsyncClient", "uvicorn.run",
                 "sys.exit", "builtins.exit"]:
        assert any(n == want or n.startswith(want) for n in names), (want, names)
    assert "prometheus_client.Histogram" not in names, "a metric with an explicit registry is fine"


def test_the_sdk_is_merge_safe():
    root = pathlib.Path(__file__).resolve().parents[1] / "besdk"
    found = mergesafety.scan([str(root)], {"besdk/" + k.split("/", 1)[1]: v for k, v in mergesafety.SDK_ALLOW.items()})
    assert found == [], "\n".join(map(str, found))
