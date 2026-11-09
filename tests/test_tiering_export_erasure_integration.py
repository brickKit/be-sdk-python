"""An erasure reaches the exports (P15.31, assembly project A159), the same case as be-sdk-go's
tiering/export_erasure_integration_test.go: the class's finished export expires (its status answers NOT_FOUND, the
cleanup deletes its object); the export being read when the erasure commits does not finish with what it read: its
job reads again, and only then is it done."""
from dataclasses import replace

import pytest

from besdk import errs
from besdk.evidence import Writer
from besdk.tiering import erase, history
from besdk.work.runner import Runner
from tests.test_tiering_destroy_integration import Keeper
from tests.test_tiering_erase_integration import wait_for
from tests.test_tiering_history_integration import ALICE, QUIET, world

pytestmark = pytest.mark.integration


async def test_an_erasure_voids_the_exports():
    async with await world("py_tier_export_erased") as w:
        w.cat.classes["sales.orders"] = replace(w.cat.classes["sales.orders"], erasable=("note",))
        w.cat.evidence = Writer(w.d, "test/sales", "test-1", QUIET, store=Keeper())
        runner = Runner(w.d, "test/sales", QUIET, poll=0.05)
        runner.add(history.export_job(w.cat, QUIET))
        w.cat.enqueue = runner.enqueue
        w.cat.cold_sync_limit, w.slow = 0.2, 1.0
        await runner.start()
        try:
            _, first = await history.history(w.cat, w.sub, "sales.orders", {"history": True})
            assert first is not None
            st = await w.finished(first.export, 60.0)
            assert st.state == "done", st.reason

            w.slow = 2.0  # the second export's job is still reading when the erasure commits
            _, second = await history.history(w.cat, w.sub, "sales.orders", {"history": True})
            assert second is not None

            async def reading():
                return await w.a.fetchval("SELECT count(*) FROM be_work_item WHERE job = $1 AND state = 'running'",
                                          history.EXPORT_JOB) == 1

            await wait_for(reading, 10.0)  # the second export is being read
            root = await w.a.fetchval("SELECT root_id FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit "
                                      "WHERE u.state = 'released' LIMIT 1")
            assert await erase.erase(w.cat, "sales.orders", root, "a data subject's request") == erase.ERASED

            with pytest.raises(errs.Error) as x:
                await history.export_status(w.cat, first.export, ALICE)
            assert x.value.reason == "NOT_FOUND", "the finished export expired with the erasure"
            w.slow = 0
            st = await w.finished(second.export, 60.0)
            assert st.state == "done", st.reason
        finally:
            await runner.stop()
        assert await w.a.fetchval("SELECT attempts FROM be_work_item WHERE job = $1 AND args->>'export' = $2",
                                  history.EXPORT_JOB, second.export) >= 2, "the read before the erasure did not finish it"
        assert await w.a.fetchval("SELECT count(*) FROM be_tier_export WHERE erased_at IS NOT NULL") == 0, \
            "the mark cleared"
        assert await history.clean_exports(w.cat) == 1, "the voided export's object and row go"
