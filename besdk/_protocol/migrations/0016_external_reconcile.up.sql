-- The external reconciliation of a full recovery (chapter 13, P13.11-P13.13; the assembly project's protocol 15, step 7).
-- unrecorded_until: sends after the recovery point may have gone on until the failure without a record; the longest
-- processing time counts from it (P13.6).
ALTER TABLE be_external_command ADD COLUMN unrecorded_until timestamptz;

CREATE TABLE be_external_reconciled (
    recovery        text NOT NULL,                 -- the recovery's identity, as the recovery tool names it
    reference       text NOT NULL,                 -- the business reference
    command         uuid,                          -- the local command; NULL: external_only, possibly_executed
    conclusion      text NOT NULL CHECK (conclusion IN ('matched', 'deadline', 'unknown', 'manual', 'external_only',
                        'possibly_executed')),
    state           text,                          -- the command's state after the matching
    resend_deadline timestamptz,
    detail          text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (recovery, reference)
);
