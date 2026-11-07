"""The run profile and the alert channel (be-protocol chapter 12, P12.1-P12.8): an object-locked bucket outside the
deployment, one signed record per object, written with a conditional create under compliance-mode object lock and the
kind's retention, read back with every signature checked against the trust material's history (trust.py). The
recovery generations' current value and advance (P12.4, P12.5), and the alert channel (P12.7, P12.8): a record's
machine-readable copy here and a notification to the webhook, each within its time limit, neither waiting for the
other."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import aiohttp
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from besdk import canon
from besdk.config import Key, Type
from besdk.events.sigv4 import UNSIGNED, Signer, escape
from besdk.identity.service import spiffe
from besdk.runprofile import trust

RECORD_TYPE = "be.ops.run_profile_record"
DEPLOYMENT_GENERATION, AUTHZ_GENERATION = "generation.deployment", "generation.authz"
ALERT, RECOVERY_STEP, BACKUP = "alert", "recovery.step", "backup"
ALERT_TIMEOUT = 2.0  # each alert copy's time limit, seconds (assembly protocol 15's starting value, P12.8)
_S3 = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def retention(kind: str) -> timedelta:
    """A kind's starting retention (P12.3): a deployment may lengthen it, never shorten it."""
    if kind.startswith("generation.") or kind.startswith("trust."):
        return timedelta(days=3650)
    return timedelta(days=30)  # alert, recovery.step, backup: no shorter than the full physical backup's retention


def object_key(deployment: str, kind: str, written_at: str, rid: str) -> str:
    """A record's object key (P12.1)."""
    return f"{deployment}/{kind}/{written_at[0:4]}/{written_at[5:7]}/{written_at[8:10]}/{written_at}-{rid}.json"


def next_generation(run_profile: int, *seen: int) -> int:
    """The largest value seen anywhere, plus one (P12.5)."""
    return max([run_profile, *seen]) + 1


def current_of(records: list[dict]) -> int:
    """A recovery generation's current value: the largest body.value among the valid records, 0 for none (P12.4)."""
    out = 0
    for r in records:
        v = (r.get("body") or {}).get("value") if isinstance(r.get("body"), dict) else None
        if isinstance(v, int) and not isinstance(v, bool) and v > out:
            out = v
    return out


def keys() -> list[Key]:
    """The run profile's and the alert channel's configuration keys, for the components that write evidence intents
    (Definition.evidence). All optional: without RUN_PROFILE_URL there is no alert channel."""
    return [Key("RUN_PROFILE_URL", Type.STRING), Key("RUN_PROFILE_BUCKET", Type.STRING),
            Key("RUN_PROFILE_REGION", Type.STRING, default="us-east-1"), Key("RUN_PROFILE_ACCESS_KEY", Type.STRING),
            Key("RUN_PROFILE_SECRET_KEY", Type.STRING, secret=True), Key("ALERT_WEBHOOK_URL", Type.STRING)]


def writer_is_signer(record: dict, leaf: x509.Certificate) -> bool:
    """The record's writer is its signing certificate's service identity (P12.4): one component's credential does not
    write a record naming another as its writer."""
    want = spiffe(record.get("deployment", ""), (record.get("writer") or {}).get("component", ""))
    try:
        uris = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(
            x509.UniformResourceIdentifier)
    except x509.ExtensionNotFound:
        return False
    return want in uris


class StoreError(Exception):
    """The object store answered something but success (412: the key exists)."""

    def __init__(self, status: int, body: str):
        super().__init__(f"run profile: the object store answered {status}: {body[:200]}")
        self.status = status


@dataclass
class Listed:
    """One record of a kind: err is why it is not to be trusted ("" when it is), with the record as read when there
    was one (the recovery tool reviews a record whose signature does not hold, assembly protocol 15's 6.2)."""

    key: str
    received: datetime
    record: dict = field(default_factory=dict)
    read: bool = False
    err: str = ""


class RunProfile:
    """One member's run profile. identity is the member's ServiceTLS (its key signs, its trust root starts the trust
    history); secret is read at every request, so a rotated secret applies from the next one."""

    def __init__(self, url: str, region: str, bucket: str, access: str, secret: Callable[[], str], deployment: str,
                 component: str, instance: str, identity, webhook: str = "", log=None,
                 now: Callable[[], datetime] | None = None, keep: Callable[[str], timedelta] | None = None):
        if not bucket or not deployment or identity is None:
            raise ValueError("run profile: the bucket, deployment and service identity are required")
        self._signer = Signer(url, region or "us-east-1", access)
        self.bucket, self.secret, self.deployment = bucket, secret, deployment
        self.writer = {"component": component, "instance": instance}
        self.identity, self.webhook, self.log = identity, webhook, log
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.keep = keep or retention  # the package's own tests: objects that expire in minutes
        self._client: aiohttp.ClientSession | None = None
        self._notifying: set[asyncio.Task] = set()

    @staticmethod
    def from_values(v, component: str, instance: str, identity, log=None) -> RunProfile | None:
        """None without RUN_PROFILE_URL or a service identity."""
        if not v.string("RUN_PROFILE_URL") or identity is None:
            return None
        secret = v.secret("RUN_PROFILE_SECRET_KEY")
        return RunProfile(v.string("RUN_PROFILE_URL"), v.string("RUN_PROFILE_REGION"), v.string("RUN_PROFILE_BUCKET"),
                          v.string("RUN_PROFILE_ACCESS_KEY"), lambda: secret.value() if secret else "",
                          v.string("DEPLOYMENT_ID"), component, instance, identity, v.string("ALERT_WEBHOOK_URL"), log)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()

    def _http(self) -> aiohttp.ClientSession:
        if self._client is None:
            self._client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10), trust_env=False)
        return self._client

    async def _do(self, method: str, key: str, query: str = "", extra: dict | None = None, body: bytes = b"",
                  payload: str = UNSIGNED) -> tuple[bytes, dict]:
        url, headers = self._signer.request(self.secret(), method, self.bucket, key, query, extra or {}, payload,
                                            self.now())
        async with self._http().request(method, url, data=body or None, headers=headers) as resp:
            out = await resp.read()
            if resp.status // 100 != 2:
                raise StoreError(resp.status, out.decode(errors="replace"))
            return out, dict(resp.headers)

    def _signing(self):
        key = serialization.load_pem_private_key(self.identity.key, None)
        chain = [c.public_bytes(serialization.Encoding.DER) for c in x509.load_pem_x509_certificates(self.identity.cert)]
        return key, chain

    async def append(self, kind: str, body, key=None, chain: list[bytes] | None = None) -> dict:
        """Signs and writes one record of a kind: a conditional create with the kind's retention under compliance-mode
        object lock; done when the store answers success (P12.1-P12.3). key and chain sign instead of the service
        credential: the trust material's issuer signs with a CA's own key (P12.24)."""
        r = {"record_type": RECORD_TYPE, "schema": "1.0", "deployment": self.deployment, "kind": kind,
             "id": str(uuid.uuid7()), "written_at": canon.time(self.now()), "writer": dict(self.writer),
             "body": json.loads(json.dumps(body))}
        if key is None:
            key, chain = self._signing()
        r["signature"] = {**trust.sign_bytes(trust.signed_bytes(r), key), "cert_chain": trust.pem_chain(chain or [])}
        data = json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode()
        until = (self.now() + self.keep(kind)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        extra = {"content-type": "application/json", "content-md5": base64.b64encode(hashlib.md5(data).digest()).decode(),
                 "if-none-match": "*", "x-amz-object-lock-mode": "COMPLIANCE",
                 "x-amz-object-lock-retain-until-date": until}
        await self._do("PUT", object_key(self.deployment, kind, r["written_at"], r["id"]), "", extra, data,
                       hashlib.sha256(data).hexdigest())
        return r

    def roots(self) -> list[x509.Certificate]:
        """The configured trust root's CA certificates: where the trust history starts (P12.25)."""
        return x509.load_pem_x509_certificates(self.identity.ca)

    async def _keys(self, kind: str) -> list[tuple[str, datetime]]:
        out, token = [], ""
        while True:
            q = {"list-type": "2", "prefix": f"{self.deployment}/{kind}/", **({"continuation-token": token} if token else {})}
            raw, _ = await self._do("GET", "", "&".join(f"{escape(k)}={escape(v)}" for k, v in sorted(q.items())))
            root = ET.fromstring(raw)
            ns = _S3 if root.tag.startswith(_S3) else ""
            for c in root.findall(f"{ns}Contents"):
                k, lm = c.findtext(f"{ns}Key", ""), c.findtext(f"{ns}LastModified", "")
                out.append((k, datetime.fromisoformat(lm.replace("Z", "+00:00"))))
            if root.findtext(f"{ns}IsTruncated", "false") != "true":
                return out
            token = root.findtext(f"{ns}NextContinuationToken", "")

    async def list_all(self, kind: str) -> list[Listed]:
        """Every record of a kind, each with its verification (P12.4): against the trust history at the time the store
        received it (P12.26); the trust kinds themselves against the current roots."""
        history = None if kind in (trust.KIND_CA, trust.KIND_CREDENTIAL) else await self.trust()
        out = []
        for k, received in await self._keys(kind):
            item = Listed(k, received)
            try:
                raw, _ = await self._do("GET", k)
                item.record = json.loads(raw)
                if not isinstance(item.record, dict):
                    raise ValueError
            except StoreError as e:
                item.err = str(e)
            except ValueError:
                item.err = f"run profile: {k}: not a record"
            else:
                item.read = True
                if why := self.verify(item.record, history, received):
                    item.err = f"run profile: {k}: {why}"
            out.append(item)
        return out

    def verify(self, record: dict, history: trust.TrustHistory | None, received: datetime) -> str:
        """Why a record's signature or signer does not hold, "" when it does (P12.2, P12.4, P12.26): without a history
        (the trust kinds), against the current trust root."""
        try:
            chain = trust.chain_of(record)
        except ValueError as e:
            return str(e)
        h = history if history is not None else trust.build(self.roots(), [])
        if why := h.holds(chain, received):
            return f"the signer's chain does not hold at the receipt ({why})"
        leaf = x509.load_der_x509_certificate(chain[0])
        if history is not None and not writer_is_signer(record, leaf):
            return f"the writer {(record.get('writer') or {}).get('component')} is not the signing certificate's identity"
        if not trust.verify_signature(record, leaf):
            return "the signature does not hold"
        return ""

    async def list(self, kind: str) -> tuple[list[dict], list[str]]:
        """The records of a kind whose signature holds, and the problems with the others for the caller to alert on."""
        listed = await self.list_all(kind)
        return [x.record for x in listed if not x.err], [x.err for x in listed if x.err]

    async def trust(self) -> trust.TrustHistory:
        """The trust material's history (P12.24-P12.26): both trust kinds, read against the configured roots."""
        recs = []
        for kind in (trust.KIND_CA, trust.KIND_CREDENTIAL):
            for x in await self.list_all(kind):
                if x.read:
                    recs.append(trust.TrustRecord(x.record, x.received))
        return trust.build(self.roots(), recs)

    async def current(self, kind: str) -> tuple[int, list[str]]:
        """A recovery generation's current value (P12.4) and the problems of the records not used."""
        recs, problems = await self.list(kind)
        return current_of(recs), problems

    async def advance(self, kind: str, *seen: int) -> int:
        """Moves a recovery generation on: written here before it is used; no write, no advance (P12.5)."""
        cur, _ = await self.current(kind)
        nxt = next_generation(cur, *seen)
        await self.append(kind, {"value": nxt})
        return nxt

    async def alert(self, body: dict) -> bool:
        """A record to the alert channel (P12.7, P12.8): its machine-readable copy here and a notification to the
        webhook, each within ALERT_TIMEOUT, neither waiting for the other; whether the copy here was written. When it
        was not, the double failure is logged and the caller goes on."""
        if self.webhook:  # a notification only; kept referenced until done (asyncio holds tasks weakly)
            task = asyncio.create_task(self._notify(body))
            self._notifying.add(task)
            task.add_done_callback(self._notifying.discard)
        try:
            await asyncio.wait_for(self.append(ALERT, body), ALERT_TIMEOUT)
            ok = True
        except Exception as e:  # noqa: BLE001 - logged as the double failure; the caller goes on
            ok = False
            if self.log is not None:
                self.log.error("the alert channel could not be written either", event="evidence_double_failure",
                               error=repr(e))
        return ok

    async def _notify(self, body: dict) -> None:
        payload = json.dumps({"deployment": self.deployment, "writer": self.writer, "alert": body}).encode()
        try:
            async with self._http().post(self.webhook, data=payload, headers={"content-type": "application/json"},
                                         timeout=aiohttp.ClientTimeout(total=ALERT_TIMEOUT)):
                pass
        except Exception:  # noqa: BLE001 - only a notification: the copy above is the record
            pass
