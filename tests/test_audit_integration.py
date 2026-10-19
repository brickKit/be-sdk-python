"""The audit transfer and deletion against a real database: receipts that are not "received" stop the record, and
the protocol's audit-purge vector (P8.21, P8.22)."""
import pytest

from besdk import audit, protocol, records
from tests import pgtest
from tests.test_db_integration import migrated, open_db

pytestmark = pytest.mark.integration
ALICE = "0190f3a1-0000-7000-8000-0000000000a1"


def change() -> audit.Record:
    return audit.Record(audit.BUSINESS_CHANGE, "demo.order.confirm", audit.SUCCEEDED, actor=ALICE,
                        target={"type": "demo.order", "id": "o1"}, changes={"status": "confirmed"})


class Fake:
    """An audit component that checks the bytes against the digest and answers a status of its own for some IDs."""

    def __init__(self, statuses=None):
        self.statuses, self.seq = statuses or {}, 0

    async def send(self, batch):
        out = []
        for w in batch:
            try:
                records.open_("be.audit.record", w["record"], w["digest"])
            except records.Invalid:
                out.append({"id": w["id"], "status": "invalid"})
                continue
            if w["id"] in self.statuses:
                out.append({"id": w["id"], "status": self.statuses[w["id"]]})
                continue
            self.seq += 1
            out.append({"id": w["id"], "status": "received", "epoch": "1.1", "seq": self.seq, "chain": "sha256:c"})
        return out

    async def watermark(self):
        return {"epoch": "1.1", "seq": 0}


async def auditor(name):
    v, _ = await migrated(name, {})
    d, pool = await open_db(name, v)
    return d, pool, audit.Auditor(d)


async def write(d, a, n=1):
    ids = []
    for _ in range(n):
        async def fn(tx):
            ids.append(await a.write(tx, change()))
        await d.tx(fn)
    return ids


async def test_the_bytes_sent_are_the_record_the_digest_names_and_unknown_receipts_stop_it():
    d, pool, a = await auditor("py_au1")
    try:
        ids = await write(d, a, 3)
        await a.number()
        n = await a.transfer(Fake({ids[0]: "invalid", ids[1]: "archived"}))
        assert n == 3
        adm = await pgtest.admin()
        try:
            states = {r["id"]: r["transfer_state"] for r in await adm.fetch("SELECT id::text, transfer_state FROM py_au1.be_audit_log")}
        finally:
            await adm.close()
        assert states[ids[2]] == "received", "the stored bytes open against their digest"
        assert states[ids[0]] == "conflict" and states[ids[1]] == "conflict", "invalid and an unknown status stop the record"
    finally:
        await pool.close()


async def test_purge_vectors():
    d, pool, a = await auditor("py_au2")
    adm = await pgtest.admin()
    try:
        for c in protocol.vector("audit-purge")["cases"]:
            await adm.execute("DELETE FROM py_au2.be_audit_log")
            [rid] = await write(d, a)
            r, w = c["receipt"], c["watermark"]
            age = "8 days" if r["retained"] else "1 hour"
            await adm.execute(f"UPDATE py_au2.be_audit_log SET transfer_state = 'received', receipt_epoch = $1, "
                              f"receipt_seq = $2, received_at = now() - interval '{age}'", r["epoch"], r["seq"])
            await a.purge(w)
            row = await adm.fetchrow("SELECT transfer_state FROM py_au2.be_audit_log WHERE id = $1::uuid", rid)
            got = "delete" if row is None else "resend" if row["transfer_state"] == "pending" else "keep"
            assert got == c["expect"], c["id"]
    finally:
        await adm.close()
        await pool.close()
