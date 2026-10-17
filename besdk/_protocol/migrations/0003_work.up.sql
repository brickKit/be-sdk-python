-- Background work items (chapter 07, protocol 01 of the assembly project).
CREATE TABLE be_work_item (
    id                  text PRIMARY KEY,
    job                 text NOT NULL,
    kind                text NOT NULL CHECK (kind IN ('queue', 'cron', 'singleton')),
    state               text NOT NULL CHECK (state IN ('pending', 'running', 'retry_wait', 'done', 'dead', 'paused', 'resolved')),
    epoch               bigint NOT NULL DEFAULT 0,
    claim_db_generation bigint,
    claim_db_timeline   bigint,
    holder              text,
    lease_until         timestamptz,
    attempts            integer NOT NULL DEFAULT 0,
    max_attempts        integer NOT NULL CHECK (max_attempts >= 1),
    run_at              timestamptz NOT NULL DEFAULT now(),
    effect_class        text NOT NULL CHECK (effect_class IN ('local', 'idempotent_external', 'non_idempotent_external', 'emit_only')),
    operation_id        text NOT NULL,
    call_results        jsonb NOT NULL DEFAULT '{}',
    waiting_since       timestamptz,
    outcome_unknown     boolean NOT NULL DEFAULT false,
    pause_reason        text CHECK (pause_reason IN ('outcome_unknown', 'backlog_exceeded', 'business_hold')),
    args                jsonb,
    progress            jsonb,
    result              jsonb,
    last_error          text,
    initiator           jsonb,
    trace               jsonb,
    slot                text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    CHECK ((state = 'paused') = (pause_reason IS NOT NULL))
);
CREATE INDEX be_work_item_due ON be_work_item (job, run_at) WHERE state IN ('pending', 'retry_wait');
CREATE INDEX be_work_item_lease ON be_work_item (job, lease_until) WHERE state = 'running';
CREATE INDEX be_work_item_finished ON be_work_item (finished_at) WHERE state IN ('done', 'resolved');

CREATE TABLE be_cron_watermark (
    job          text PRIMARY KEY,
    watermark_at timestamptz NOT NULL,
    last_slot    text
);
