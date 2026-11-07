"""A full recovery's external reconciliation, step 7 (P13.12): the matching of the local commands not final with the
external records and the issue records, a pure function checked against the vector external-reconcile."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from besdk.extcmd.state import ACCEPTED, MANUAL, UNKNOWN, Capabilities, Facts, deadline, resendable, terminal

# the conclusions (P13.13)
MATCHED = "matched"  # the external record: final, or accepted
DEADLINE = "deadline"  # a resend deadline written, the state unchanged
CONCLUSION_UNKNOWN = UNKNOWN  # the earlier request can no longer land: queried, then sent again
CONCLUSION_MANUAL = MANUAL  # it may still land: a person decides
EXTERNAL_ONLY = "external_only"  # the external system has it, this member does not
POSSIBLY_EXECUTED = "possibly_executed"  # an issue record, no command, no external record


@dataclass(frozen=True)
class Record:
    """One external record an adapter lists."""

    reference: str
    state: str
    external_id: str = ""
    outcome: dict | None = None
    response: str = ""


@dataclass
class Open:
    """A local command not final, as the reconciliation reads it."""

    reference: str
    command: str
    operation: str
    facts: Facts
    sending_at: datetime | None = None  # the start of a send attempt that has not ended


@dataclass
class Conclusion:
    reference: str
    conclusion: str
    command: str = ""  # empty: external_only, possibly_executed
    state: str = ""  # matched: the state taken from the external record
    resend_deadline: datetime | None = None  # deadline
    first_reached: datetime | None = None  # not matched: written with unrecorded_until
    detail: str = ""


def earliest(o: Open, t: datetime) -> datetime:
    """The earliest send of an open command that may have reached the external system (P13.12): the first that may
    have, else the start of a send attempt that has not ended, else the recovery point."""
    return o.facts.first_reached or o.sending_at or t


def match(open_: list[Open], external: dict[str, str], local: set[str], issued: list[str],
          caps: Callable[[str], Capabilities], t: datetime, unrecorded_until: datetime, now: datetime) -> list[Conclusion]:
    """The step 7 matching (P13.12), first matching row wins: open_ are the local commands not final, external the
    external records' states by reference, local every reference this member has a command for (final ones too), issued
    the issue records' references; unrecorded_until is until when sends may have gone on without a record (the
    failure). One conclusion per reference, ordered by reference."""
    out: dict[str, Conclusion] = {}
    for o in open_:
        c = Conclusion(o.reference, "", o.command)
        if (s := external.get(o.reference)) is not None:
            c.conclusion, c.state = MATCHED, s if terminal(s) else ACCEPTED
        elif o.facts.state == MANUAL:
            c.conclusion, c.detail = CONCLUSION_MANUAL, "manual: only a person registers it"
        else:
            f = replace(o.facts, first_reached=earliest(o, t), unrecorded_until=unrecorded_until)
            c.first_reached = f.first_reached
            cp = caps(o.operation)
            if (d := deadline(f, cp)) is not None and not now > d:
                c.conclusion, c.resend_deadline = DEADLINE, d
            else:
                ok, why = resendable(replace(f, resend_deadline=None), replace(cp, dedupe_window=timedelta(0)), now)
                c.conclusion, c.detail = (CONCLUSION_UNKNOWN if ok else CONCLUSION_MANUAL), why
        out[o.reference] = c
    for ref in external:
        if ref not in out and ref not in local:
            out[ref] = Conclusion(ref, EXTERNAL_ONLY)
    for ref in issued:
        if ref not in out and ref not in local:
            out[ref] = Conclusion(ref, POSSIBLY_EXECUTED)
    return [out[k] for k in sorted(out)]
