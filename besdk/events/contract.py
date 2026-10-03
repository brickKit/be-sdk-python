"""The component's event contract, ``contracts/events/*.events.json`` (be-protocol P12.2): per subject the
aggregate type, the consumption mode, whether it is a transaction document, its file and a payload
validator. Loaded once per member."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


@dataclass(frozen=True)
class EventDecl:
    subject: str
    aggregate_type: str
    consumption: str
    transaction_document: bool
    file: str
    validator: Any


class Contract:
    def __init__(self, decls: dict[str, EventDecl]):
        self.decls = decls

    @classmethod
    def load(cls, contracts: Path) -> "Contract":
        decls: dict[str, EventDecl] = {}
        d = Path(contracts) / "events"
        for f in sorted(d.glob("*.events.json")) if d.exists() else ():
            doc = json.loads(f.read_text())
            for e in doc.get("events", ()):
                schema = e.get("payload") or {"type": "object"}
                decls[e["subject"]] = EventDecl(
                    subject=e["subject"], aggregate_type=e["x-aggregate-type"],
                    consumption=e.get("x-consumption", "state"),
                    transaction_document=bool(e.get("x-transaction-document")), file=f.name,
                    validator=Draft202012Validator(schema, format_checker=FormatChecker()))
        return cls(decls)

    def get(self, subject: str) -> EventDecl | None:
        return self.decls.get(subject)

    def problems(self, subject: str, payload: Any) -> list[str]:
        decl = self.decls[subject]
        return [f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
                for e in decl.validator.iter_errors(payload)]
