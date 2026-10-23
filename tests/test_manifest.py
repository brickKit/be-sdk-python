"""The documents a component's tests compare with its code: configSchema, events.publishes and the OpenAPI routes."""
import pathlib

from besdk.authz import Definitions, Operation, Resource
from besdk.component import Definition
from besdk.config import Key, Type
from besdk.http import Route
from besdk.manifest import config_schema, config_schema_yaml, event_problems, manifest_problems, openapi_problems


async def handler(c):
    return None


def definition(**kw):
    return Definition("demo/items", "3.0.0", lambda rt: None, keys=[Key("DEMO_LIMIT", Type.INTEGER, default=5)],
                      authz=Definitions("demo/items", [Resource("demo.item", "demo.item.read", {}, {})],
                                        [Operation("demo.item.read", "demo.item", "read", "low")]), **kw)


def test_the_config_schema_round_trips_and_names_every_difference():
    d = definition()
    manifest = "apiVersion: brickkit/v1\n" + config_schema_yaml(d, {"DEMO_LIMIT": "how many"})
    assert manifest_problems(manifest, d, {"DEMO_LIMIT": "how many"}) == []
    assert config_schema(d)["properties"]["DEMO_LIMIT"] == {"type": "integer", "description": "SDK default 5"}
    tampered = manifest.replace("DEMO_LIMIT:", "DEMO_OTHER:")
    problems = manifest_problems(tampered, d, {"DEMO_LIMIT": "how many"})
    assert "DEMO_LIMIT: not declared" in problems and "DEMO_OTHER: declared, but nothing reads it" in problems
    assert any(p.startswith("DEMO_LIMIT: declared as") for p in manifest_problems(manifest, d, {"DEMO_LIMIT": "other"}))


def test_published_events_match_their_contracts(tmp_path: pathlib.Path):
    (tmp_path / "demo.item.changed.v1.schema.json").write_text("{}")
    (tmp_path / "demo.item.lost.v1.schema.json").write_text("{}")
    manifest = "events:\n  publishes:\n    - demo.item.changed.v1\n    - demo.item.gone.v1\n"
    assert event_problems(manifest, tmp_path) == [
        "demo.item.gone.v1: in events.publishes, but there is no demo.item.gone.v1.schema.json",
        "demo.item.lost.v1: has a contract, but is not in events.publishes"]


def test_routes_match_the_openapi_document():
    doc = """paths:
  /api/items:
    get: {x-operation: demo.item.read}
    parameters: []
  /api/items/{id}:
    get: {x-operation: demo.item.read}
    delete: {x-operation: demo.item.delete}
"""
    routes = [Route("GET", "/api/items", handler, "demo.item.read"), Route("GET", "/api/items/{id}", handler, "demo.item.write"),
              Route("POST", "/api/items", handler, "demo.item.read")]
    assert openapi_problems(doc, routes) == [
        "DELETE /api/items/{id}: in the OpenAPI document but not registered",
        "GET /api/items/{id}: x-operation 'demo.item.read' in the document, 'demo.item.write' registered",
        "POST /api/items: registered but not in the OpenAPI document"]
