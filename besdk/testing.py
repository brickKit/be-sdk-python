"""A test deployment's security for components' own tests and the acceptance suite: a token issuer with its published
keys, a trust root issuing component certificates (with the SPIFFE ID and its derived DNS name, P8.5), and fake
authorization and audit members answering over mTLS. For tests only: never import it from component code."""
from __future__ import annotations

import datetime as dt
import http.server
import json
import pathlib
import threading
import time
import uuid
from urllib.parse import urlsplit

import asyncpg
import grpc
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from google.protobuf import wrappers_pb2

from besdk.authz import SECRET, Definitions, Field, Operation, Resource
from besdk.identity import ServiceTLS, dns_name, spiffe

ALICE = "0190f3a1-0000-7000-8000-0000000000a1"
BOB = "0190f3a1-0000-7000-8000-0000000000b2"
DEPLOYMENT = "0190f3a1-0000-7000-8000-00000000d001"


def pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


class World:
    """One test deployment. start() runs the issuer's key endpoint and the fake members; stop() ends them."""

    def __init__(self, directory: pathlib.Path, deployment: str = DEPLOYMENT):
        self.deployment, self.dir, self.issuer = deployment, directory, "https://iam.test/"
        self.signer = ed25519.Ed25519PrivateKey.generate()
        self.subjects: dict[str, dict] = {}
        self.types: set[str] = set()
        self.gen, self.recovery = 1, "1"
        self.audited: dict[str, dict] = {}
        self.verifications = 0
        self.authz_up = True
        self._ca_key = ec.generate_private_key(ec.SECP256R1())
        now = dt.datetime.now(dt.timezone.utc)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test deployment root")])
        self._ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(self._ca_key.public_key())
                    .serial_number(1).not_valid_before(now - dt.timedelta(hours=1))
                    .not_valid_after(now + dt.timedelta(days=1))
                    .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
                    .add_extension(x509.KeyUsage(False, False, False, False, False, True, False, False, False), critical=True)
                    .sign(self._ca_key, hashes.SHA256()))
        self.ca_file = self.dir / "ca.pem"
        self.ca_file.write_bytes(pem(self._ca))
        self._serial = 100

    # ------------------------------------------------------------------ certificates

    def certs(self, component: str, deployment: str | None = None, dns: bool = True, ca: tuple | None = None
              ) -> tuple[str, str, str]:
        """Issues a component's certificate and returns (cert, key, ca) file paths. deployment names another one for
        refusals; dns=False leaves out the derived DNS name; ca=(cert, key) signs with another root."""
        dep = deployment or self.deployment
        key = ec.generate_private_key(ec.SECP256R1())
        self._serial += 1
        now = dt.datetime.now(dt.timezone.utc)
        names: list[x509.GeneralName] = [x509.UniformResourceIdentifier(spiffe(dep, component))]
        if dns:
            names.append(x509.DNSName(dns_name(dep, component)))
        ca_cert, ca_key = ca or (self._ca, self._ca_key)
        cert = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, component)]))
                .issuer_name(ca_cert.subject).public_key(key.public_key()).serial_number(self._serial)
                .not_valid_before(now - dt.timedelta(hours=1)).not_valid_after(now + dt.timedelta(days=1))
                .add_extension(x509.SubjectAlternativeName(names), critical=False)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]),
                               critical=False)
                .sign(ca_key, hashes.SHA256()))
        base = self.dir / str(self._serial)
        cert_file, key_file = pathlib.Path(f"{base}-cert.pem"), pathlib.Path(f"{base}-key.pem")
        cert_file.write_bytes(pem(cert))
        key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                               serialization.NoEncryption()))
        return str(cert_file), str(key_file), str(self.ca_file)

    def other_root(self) -> tuple:
        k = ec.generate_private_key(ec.SECP256R1())
        now = dt.datetime.now(dt.timezone.utc)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "another root")])
        c = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(k.public_key()).serial_number(2)
             .not_valid_before(now - dt.timedelta(hours=1)).not_valid_after(now + dt.timedelta(days=1))
             .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(k, hashes.SHA256()))
        return c, k

    def tls(self, component: str) -> ServiceTLS:
        return ServiceTLS(self.deployment, component, *self.certs(component), lambda p: pathlib.Path(p).read_bytes())

    def env(self, component: str) -> dict[str, str]:
        """A component's security configuration in this deployment."""
        cert, key, ca = self.certs(component)
        return {"DEPLOYMENT_ID": self.deployment, "IAM_ISSUER": self.issuer, "IAM_JWKS_URL": self.jwks_url,
                "AUTHZ_GRPC_URL": "http://" + self.authz_addr, "AUDIT_GRPC_URL": "http://" + self.audit_addr,
                "SERVICE_TLS_CERT_FILE": cert, "SERVICE_TLS_KEY_FILE": key, "SERVICE_TLS_CA_FILE": ca}

    # ------------------------------------------------------------------ tokens

    def token(self, subject: str, ttl: int = 600) -> str:
        now = int(time.time())
        return jwt.encode({"iss": self.issuer, "aud": "be:deployment:" + self.deployment, "sub": subject, "iat": now,
                           "exp": now + ttl}, self.signer, algorithm="EdDSA", headers={"typ": "at+jwt", "kid": "test-1"})

    def bearer(self, subject: str) -> str:
        return "Bearer " + self.token(subject)

    def _jwks(self) -> bytes:
        x = self.signer.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        import base64

        return json.dumps({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "test-1", "alg": "EdDSA", "use": "sig",
                                     "x": base64.urlsafe_b64encode(x).rstrip(b"=").decode()}]}).encode()

    # ------------------------------------------------------------------ grants

    def grant(self, principal: str, *segments: dict) -> None:
        """Gives a principal segments (their resource types become applicable)."""
        e = self.subjects.setdefault(principal, {"segments": [], "accounts_disabled": False})
        e["segments"] += segments
        self.types.update(s["resource_type"] for s in segments)
        self.gen += 1

    def revoke(self, principal: str) -> None:
        self.subjects.setdefault(principal, {"segments": [], "accounts_disabled": False})["segments"] = []
        self.gen += 1

    def grant_all(self, principal: str, defs: Definitions, only: list[str] | None = None) -> None:
        """Every declared operation (or only those named) on every record, reading all non-secret fields."""
        segs = []
        for op in defs.operations:
            if only is not None and op.key not in only:
                continue
            res = next(r for r in defs.resources if r.type == op.resource_type)
            segs.append({"resource_type": res.type, "operation": op.key,
                         "atoms": {d: {"kind": "all"} for d in res.dimensions or {}},
                         "read_fields": sorted(f for f, fd in res.fields.items() if fd.cls != SECRET),
                         "valid": {"from": "2026-01-01T00:00:00Z"}})
        self.grant(principal, *segs)

    def answer(self, req: dict) -> dict:
        self.verifications += 1
        shard = {"shard": "all", "recovery_generation": {"domain": "authz.recovery", "value": self.recovery},
                 "applicability": sorted(self.types), "subjects": json.loads(json.dumps(self.subjects)),
                 "digest": f"sha256:{self.gen}", "security_revisions": [], "capabilities": []}
        return {"nonce": req["nonce"], "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "recovery_generation": shard["recovery_generation"],
                "shards": [{"shard": "all", "status": "changed", "view": shard}]}

    # ------------------------------------------------------------------ fake members

    async def start(self) -> World:
        jwks = self._jwks()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(jwks)

            def log_message(self, *a) -> None:
                pass

        self._http = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._http.serve_forever, daemon=True).start()
        self.jwks_url = f"http://127.0.0.1:{self._http.server_address[1]}/jwks"
        self._authz, self.authz_addr = await self._serve("infra/authz", "be.authz.v3.ViewService",
                                                         {"Verify": self._verify})
        self._audit, self.audit_addr = await self._serve("infra/audit", "be.audit.v3.Intake",
                                                         {"Submit": self._submit, "Watermark": self._watermark})
        return self

    async def stop(self) -> None:
        self._http.shutdown()
        await self._authz.stop(0)
        await self._audit.stop(0)

    async def _serve(self, component: str, service: str, methods: dict):
        tls = self.tls(component)

        def unary(fn):
            async def h(request, context):
                return wrappers_pb2.BytesValue(value=json.dumps(fn(json.loads(request.value), context)).encode())

            return grpc.unary_unary_rpc_method_handler(h, request_deserializer=wrappers_pb2.BytesValue.FromString,
                                                       response_serializer=wrappers_pb2.BytesValue.SerializeToString)

        server = grpc.aio.server()
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
            service, {n: unary(f) for n, f in methods.items()}),))
        port = server.add_secure_port("127.0.0.1:0", tls.server_credentials())
        await server.start()
        return server, f"127.0.0.1:{port}"

    def _verify(self, req: dict, context) -> dict:
        if not self.authz_up:
            raise RuntimeError("the authorization member is down")
        return self.answer(req)

    def _submit(self, records: list, context) -> list:
        out = []
        for r in records:
            self.audited[r["id"]] = r["record"]
            out.append({"id": r["id"], "status": "received", "epoch": "1.1", "seq": len(self.audited),
                        "chain": "sha256:test"})
        return out

    def _watermark(self, body: dict, context) -> dict:
        return {"Epoch": "1.1", "Seq": 0}


def defs(component: str, resource: str, *ops: str, risk: str = "low") -> Definitions:
    """One resource without dimensions; the first operation is its view operation."""
    d = Definitions(component, [Resource(resource, ops[0], {}, {"id": Field("ordinary")})],
                    [Operation(op, resource, "read", risk) for op in ops])
    return d


def new_id() -> str:
    return str(uuid.uuid7())


async def provision(admin_dsn: str, database: str, schema: str, owner: str, runtime: str, shell_role: str = "") -> dict:
    """A member's database as the deployment's provisioning does it (protocol 14): the database, an owner role that
    owns the schema (migrations) and a runtime role with the business privileges, functions not executable by PUBLIC;
    with shell_role, the shell's login role holds the runtime role WITH INHERIT FALSE. The schema starts empty.
    Returns the member's PG_* configuration (passwords are derived from the role names: tests only)."""
    c = await asyncpg.connect(admin_dsn)
    try:
        if not await c.fetchval("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", database):
            try:
                await c.execute(f'CREATE DATABASE "{database}"')
            except asyncpg.DuplicateDatabaseError:
                pass
    finally:
        await c.close()
    c = await asyncpg.connect(urlsplit(admin_dsn)._replace(path="/" + database).geturl())
    try:
        for r in (owner, runtime):
            if not await c.fetchval("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", r):
                try:
                    await c.execute(f"CREATE ROLE {r} LOGIN")
                except asyncpg.DuplicateObjectError:
                    pass
            await c.execute(f"ALTER ROLE {r} LOGIN PASSWORD '{r}-pw'")
        for sql in [f"DROP SCHEMA IF EXISTS {schema} CASCADE", f'GRANT CONNECT ON DATABASE "{database}" TO {owner}, {runtime}',
                    f"CREATE SCHEMA {schema} AUTHORIZATION {owner}", f"GRANT USAGE ON SCHEMA {schema} TO {runtime}",
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE "
                    f"ON TABLES TO {runtime}",
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC",
                    f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {runtime}"]:
            await c.execute(sql)
        if shell_role:
            await c.execute(f"GRANT {runtime} TO {shell_role} WITH INHERIT FALSE, SET TRUE")
    finally:
        await c.close()
    u = urlsplit(admin_dsn)
    return {"PG_HOST": u.hostname or "localhost", "PG_PORT": str(u.port or 5432), "PG_DATABASE": database,
            "PG_USER": runtime, "PG_PASSWORD": runtime + "-pw", "PG_SCHEMA": schema, "PG_OWNER_USER": owner,
            "PG_OWNER_PASSWORD": owner + "-pw"}
