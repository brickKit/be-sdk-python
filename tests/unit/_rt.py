"""Build a Runtime for unit tests from an in-memory component.yaml, with the identity and authorization
providers faked through one httpx MockTransport."""
import io
import json
from pathlib import Path

import httpx
import yaml

from besdk.runtime import Module, Runtime, Shared, Spec
from tests.unit._tokens import ISSUER, TENANT, FakeIAM

BUNDLE = {"contract": "authz/2.0", "revision": "7", "capabilities": {"core": True},
          "roles": {"rep": ["conformance.widget.view"], "boss": ["conformance.widget.view", "conformance.widget.approve"]},
          "grants": {}, "stale_since": {}}


def manifest(component_id="conformance/widget-py", props=None, required=None, **extra):
    props = {"AUTHZ_URL": {"type": "string"}, "IAM_URL": {"type": "string"}, "IAM_ISSUER": {"type": "string"},
             "TENANT_ID": {"type": "string"}, "HTTP_DEFAULT_TIMEOUT": {"type": "string", "default": "10s"},
             "LOG_LEVEL": {"type": "string", "default": "info"}, "DEFAULT_LOCALE": {"type": "string", "default": "en"},
             **(props or {})}
    doc = {"metadata": {"id": component_id, "version": "1.0.0"},
           "configSchema": {"properties": props, "required": required or ["AUTHZ_URL", "IAM_URL", "IAM_ISSUER", "TENANT_ID"]},
           "deployment": {"port": 0, "extraPorts": [{"name": "grpc", "port": 0}], "stopGracePeriodSeconds": 30}}
    doc.update(extra)
    return doc


def env(**over):
    e = {"AUTHZ_URL": "http://authz:8223", "IAM_URL": "http://iam:8200", "IAM_ISSUER": ISSUER, "TENANT_ID": TENANT}
    e.update(over)
    return {k: v for k, v in e.items() if v is not None}


class Fakes:
    def __init__(self):
        self.iam = FakeIAM()
        self.bundle = dict(BUNDLE)

    def transport(self):
        iam_t = self.iam.transport()

        def handle(req: httpx.Request) -> httpx.Response:
            if req.url.host == "authz" and req.url.path == "/authz/v2/bundle":
                return httpx.Response(200, json=self.bundle)
            if req.url.host == "iam":
                return iam_t.handle_request(req)
            return httpx.Response(404)

        return httpx.MockTransport(handle)


def build(tmp_path: Path, create, *, doc=None, environ=None, fakes=None, log=None) -> tuple[Runtime, Fakes, io.StringIO]:
    fakes = fakes or Fakes()
    (tmp_path / "contracts").mkdir(exist_ok=True)
    (tmp_path / "migrations").mkdir(exist_ok=True)
    (tmp_path / "component.yaml").write_text(yaml.safe_dump(doc or manifest()))
    spec = Spec(id=(doc or manifest())["metadata"]["id"], migrations=tmp_path / "migrations",
                contracts=tmp_path / "contracts", create=create)
    log = log or io.StringIO()
    shared = Shared.standalone(spec_id=spec.id, otel_base_url="", http_transport=fakes.transport())
    rt = Runtime(spec, environ if environ is not None else env(), shared, log_stream=log)
    return rt, fakes, log


def lines(buf: io.StringIO) -> list[dict]:
    return [json.loads(x) for x in buf.getvalue().splitlines()]
