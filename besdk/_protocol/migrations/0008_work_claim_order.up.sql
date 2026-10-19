-- P7.22: a claim takes an expired lease first, then the earliest due item, each through an index in its own order, so
-- a backlog does not slow a claim. The due index orders by run_at alone (a component's table holds only its own
-- jobs); it replaces the (job, run_at) index, with which a claim over several jobs fetched and sorted the whole
-- backlog (20 000 items: 3-8 ms a claim, against 0.1-0.2 ms).
CREATE INDEX be_work_item_due_at ON be_work_item (run_at) WHERE state IN ('pending', 'retry_wait');
DROP INDEX be_work_item_due;
