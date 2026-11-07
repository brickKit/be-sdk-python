"""A unit's manifest and its objects' keys (be-protocol P15.15, P15.16; vector tier-manifest): the manifest
be.tier.manifest is canonical JSON naming the unit, the class, its version and route, the generation and epoch, the
format, the class's structure version, the digest's version and value, each table's name, primary key, columns, row
count and objects (key, rows, bytes, SHA-256), and when it was written. Counts are decimal strings. On the object
route the manifest is an object of its own next to the files, so a third-party reader reads them by it."""
from __future__ import annotations

from dataclasses import dataclass, field

from besdk import canon
from besdk.tiering.rows import DIGEST_VERSION

RECORD_TYPE = "be.tier.manifest"
PARQUET, PG_ROWS = "parquet-1", "pg-rows-1"  # the formats of the routes object-parquet and postgres


def object_key(prefix: str, cls: str, unit: str, generation: int, version: int, table: str, part: int) -> str:
    """<component prefix>/<class>/<unit>/<generation>.<version>/<table>-<part>.parquet (P15.15)."""
    return f"{prefix}/{cls}/{unit}/{generation}.{version}/{table}-{part}.parquet"


def manifest_key(prefix: str, cls: str, unit: str, generation: int, version: int) -> str:
    return f"{prefix}/{cls}/{unit}/{generation}.{version}/manifest.json"


@dataclass
class Object:
    key: str
    rows: int
    bytes: int
    sha256: str


@dataclass
class Table:
    name: str
    pk: list[str]
    columns: dict[str, tuple[str, bool]]  # name -> (type, nullable)
    rows: int = 0
    objects: list[Object] = field(default_factory=list)


@dataclass
class Manifest:
    unit: str
    cls: str
    version: int
    route: str
    generation: int
    epoch: str
    format: str
    schema_version: str
    digest: str
    tables: list[Table]
    created_at: str
    digest_version: int = DIGEST_VERSION

    def value(self) -> dict:
        return {"record_type": RECORD_TYPE, "schema": "1.0", "unit": self.unit, "class": self.cls,
                "version": self.version, "route": self.route, "generation": self.generation, "epoch": self.epoch,
                "format": self.format, "schema_version": self.schema_version, "digest_version": self.digest_version,
                "digest": self.digest, "created_at": self.created_at,
                "tables": [{"name": t.name, "pk": list(t.pk), "rows": str(t.rows),
                            "columns": [{"name": c, "type": typ, "nullable": n}
                                        for c, (typ, n) in sorted(t.columns.items())],
                            "objects": [{"key": o.key, "rows": str(o.rows), "bytes": str(o.bytes), "sha256": o.sha256}
                                        for o in t.objects]} for t in sorted(self.tables, key=lambda t: t.name)]}

    def encode(self) -> bytes:
        return canon.encode(self.value())


def parse(raw: bytes) -> Manifest:
    """A manifest read back; raises ValueError for one that is not."""
    import json

    v = json.loads(raw)
    if not isinstance(v, dict) or v.get("record_type") != RECORD_TYPE:
        raise ValueError("not a be.tier.manifest")
    tables = [Table(t["name"], list(t["pk"]), {c["name"]: (c["type"], bool(c["nullable"])) for c in t["columns"]},
                    int(t["rows"]), [Object(o["key"], int(o["rows"]), int(o["bytes"]), o["sha256"])
                                     for o in t.get("objects") or []]) for t in v["tables"]]
    return Manifest(v["unit"], v["class"], int(v["version"]), v["route"], int(v["generation"]), v["epoch"],
                    v["format"], v["schema_version"], v["digest"], tables, v["created_at"], int(v["digest_version"]))
