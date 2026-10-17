-- The local audit table (chapter 08, P8.19-P8.23; protocol 03 of the assembly project): append-only, numbered after
-- commit under its own counter's lock, transferred with receipts.
CREATE TABLE be_audit_log (
    ord              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id               uuid NOT NULL UNIQUE,
    seq              bigint,
    epoch_generation bigint,
    epoch_timeline   bigint,
    occurred_at      timestamptz NOT NULL,
    category         text NOT NULL CHECK (category IN ('business_change', 'security_change', 'sensitive_read',
                                                       'delegated', 'security_denial', 'work_disposition')),
    operation        text NOT NULL,
    outcome          text NOT NULL CHECK (outcome IN ('succeeded', 'denied', 'failed')),
    reason           text,
    actor            jsonb,
    represented      jsonb,
    target           jsonb,
    changes          jsonb,
    request          jsonb,
    summary          jsonb,
    digest           text NOT NULL,
    transfer_state   text NOT NULL DEFAULT 'pending' CHECK (transfer_state IN ('pending', 'received', 'conflict')),
    receipt_epoch    text,
    receipt_seq      bigint,
    receipt_chain    text,
    received_at      timestamptz,
    CHECK ((seq IS NULL) = (epoch_generation IS NULL) AND (seq IS NULL) = (epoch_timeline IS NULL))
);
CREATE INDEX be_audit_log_unnumbered ON be_audit_log (ord) WHERE seq IS NULL;
CREATE UNIQUE INDEX be_audit_log_position ON be_audit_log (epoch_generation, epoch_timeline, seq) WHERE seq IS NOT NULL;
CREATE INDEX be_audit_log_pending ON be_audit_log (ord) WHERE transfer_state = 'pending' AND seq IS NOT NULL;

CREATE TABLE be_audit_counter (
    single           boolean PRIMARY KEY DEFAULT true CHECK (single),
    epoch_generation bigint,
    epoch_timeline   bigint,
    last_seq         bigint NOT NULL DEFAULT 0
);
INSERT INTO be_audit_counter (single) VALUES (true);

-- denial details per principal, reason and minute (P8.23): within the budget each denial is its own row; past it
-- the window only counts here, and once the minute is over one summary row with the count is appended
CREATE TABLE be_audit_denial_budget (
    principal  text NOT NULL,
    reason     text NOT NULL,
    minute     timestamptz NOT NULL,
    detailed   integer NOT NULL DEFAULT 0,
    overflow   integer NOT NULL DEFAULT 0,
    first_at   timestamptz,
    last_at    timestamptz,
    summarized boolean NOT NULL DEFAULT false,
    PRIMARY KEY (principal, reason, minute)
);
CREATE INDEX be_audit_denial_unsummarized ON be_audit_denial_budget (minute) WHERE overflow > 0 AND NOT summarized;
