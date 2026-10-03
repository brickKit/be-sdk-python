-- be-protocol 1.0 reference DDL: background jobs and reconcilers (P14)
--
-- holder = '<component ID>/<instance id>' for the in-process scheduler, '<component ID>/job-run:<instance id>'
-- for a one-shot `job run <name>` (P14.8): both claim through the same rows, so a slot or a lease is
-- taken once whoever triggers it. Not partitioned; done queue rows and old slots are deleted after
-- retention by the runtime's cleanup singleton.

CREATE TABLE IF NOT EXISTS besdk_job_lease (
    name       TEXT        PRIMARY KEY,
    holder     TEXT        NOT NULL,
    epoch      BIGINT      NOT NULL DEFAULT 0,           -- fencing token, +1 on every takeover
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS besdk_job_slot (
    name       TEXT        NOT NULL,
    slot_at    TIMESTAMPTZ NOT NULL,                     -- the scheduled instant of the slot
    holder     TEXT        NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at    TIMESTAMPTZ,
    result     TEXT,
    PRIMARY KEY (name, slot_at)
);

CREATE TABLE IF NOT EXISTS besdk_job_queue (
    id           UUID        PRIMARY KEY,                -- UUIDv7
    kind         TEXT        NOT NULL,
    args         JSONB       NOT NULL,
    unique_key   TEXT,                                   -- optional de-duplication key
    run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL,
    state        TEXT        NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'running', 'done', 'dead')),
    lease_until  TIMESTAMPTZ,
    last_error   TEXT        NOT NULL DEFAULT '',
    traceparent  TEXT        NOT NULL DEFAULT '',
    causation_id TEXT        NOT NULL DEFAULT '',
    hop_count    INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);

-- one live job per key; a done job frees the key
CREATE UNIQUE INDEX IF NOT EXISTS besdk_job_queue_unique ON besdk_job_queue (kind, unique_key)
    WHERE unique_key IS NOT NULL AND state <> 'done';
CREATE INDEX IF NOT EXISTS besdk_job_queue_due ON besdk_job_queue (kind, run_at)
    WHERE state IN ('ready', 'running');

CREATE TABLE IF NOT EXISTS besdk_reconcile (
    name        TEXT        NOT NULL,                    -- the reconciler's name
    item_id     TEXT        NOT NULL,
    attempts    INT         NOT NULL DEFAULT 0,
    next_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    last_error  TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (name, item_id)
);

CREATE INDEX IF NOT EXISTS besdk_reconcile_due ON besdk_reconcile (name, next_at);
