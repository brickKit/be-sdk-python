"""The SDK's own background jobs and what each does in a full recovery's phases (P1.23). In none of the phases a job
runs as a job (the runner starts only in normal running); what must run during a recovery runs as the host's own
loop instead, because what it writes to never goes back (the audit transfer does, in host.py). A job the SDK adds
says here how it behaves, or the host refuses it: the audit and the evidence transfers were each found not running in
the recovery phases only by a drill (foundations item 4)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RecoveryRule:
    loop: bool  # runs as a recovery-phase loop next to the job
    why: str  # why it runs there, or why not


IN_RECOVERY = {
    "be.outbox.purge": RecoveryRule(False, "not run: a purge during the recovery could drop published rows a "
                                           "redelivery still needs; it runs once open"),
    "be.work.cleanup": RecoveryRule(False, "not run: housekeeping of the member's own database, which goes back; "
                                           "once open"),
    "be.partitions.maintain": RecoveryRule(False, "not run: the partitions ahead were made before T; hourly once "
                                                  "open"),
    "be.evidence.transfer": RecoveryRule(True, "runs as the host's loop in the phases but frozen: the evidence "
                                               "store never goes back, and a conclusion written during the recovery "
                                               "reaches it before the replay (P12.14, P12.15)"),
    "be.extcmd.process": RecoveryRule(True, "runs as the host's loop in verifying and read_only, query only "
                                            "(P13.10): nothing is sent while the external reconciliation decides"),
    "be.idempotency.cleanup": RecoveryRule(False, "not run: housekeeping of the member's own database, which goes "
                                                  "back; once open"),
    "be.tiering.candidates": RecoveryRule(False, "not run: sealing reads and writes the member's own database, which "
                                                 "goes back; the next window once open"),
    "be.tiering.transfer": RecoveryRule(False, "not run: a publish changes the member's catalog, which goes back; the "
                                               "sealed units wait for the opening"),
    "be.tiering.release": RecoveryRule(False, "not run: deleting online rows of the member's own database, which goes "
                                              "back; once open"),
    "be.tiering.export": RecoveryRule(False, "not run: an export reads the member's own database, which goes back, and "
                                             "the authorization being recovered; the queued exports run once open"),
    "be.tiering.destroy": RecoveryRule(False, "not run: a destruction decides on the member's own database, which goes "
                                              "back; the destructions after the recovery's point are replayed from "
                                              "their evidence, the rest run once open"),
    "be.tiering.erase": RecoveryRule(False, "not run: an erasure writes the member's own database and its archive "
                                            "copies; the erasures replayed during the recovery are queued and run "
                                            "once open"),
    "be.tiering.export.cleanup": RecoveryRule(False, "not run: housekeeping of the member's own database, which goes "
                                                     "back; once open"),
    "be.tiering.cleanup": RecoveryRule(False, "not run: what the catalog going back no longer names (units published "
                                              "after the recovery's point) is the recovery check's difference, not an "
                                              "orphan yet; daily once open, past the window"),
}


def add(runner, job) -> None:
    """Registers one of the SDK's own jobs, refusing one that does not say how it behaves in a recovery."""
    if job.name not in IN_RECOVERY:
        raise ValueError(f"the SDK's job {job.name} does not say how it behaves in the recovery phases "
                         "(besdk/sdkjobs.py, P1.23)")
    runner.add(job)
