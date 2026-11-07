-- A reservation participant's branches (chapter 16): the state machine the runtime keeps for every branch a
-- coordinator holds here, and the consumptions' idempotency. What a branch takes (an amount, stock lines) is the
-- component's, in its own tables keyed by the branch's ID; nobody else reads these tables.
CREATE TABLE be_reserve_branch (
    id              uuid PRIMARY KEY,                -- the coordinator's branch ID (P16.1)
    kind            text NOT NULL,                   -- the participation: one component may hold branches of several kinds
    state           text NOT NULL CHECK (state IN ('held', 'committed', 'consumed', 'released', 'expired', 'rejected',
                        'cancelled')),
    reason          text,                            -- rejected: why (the component's words)
    content_digest  text,                            -- the hold's content (P16.3); none on a tombstone
    hold_expires_at timestamptz,                     -- held: the end of the hold period (P16.10)
    confirmed_at    timestamptz,
    version         bigint NOT NULL DEFAULT 1,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
-- the expiry job's scan (P16.8)
CREATE INDEX be_reserve_branch_due ON be_reserve_branch (kind, hold_expires_at) WHERE state = 'held';

-- One row a consumption ID (P16.7): a repeat with the same content answers the branch; other content is a conflict.
CREATE TABLE be_reserve_consumption (
    id             text PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 128),
    branch_id      uuid NOT NULL REFERENCES be_reserve_branch (id),
    content_digest text NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
