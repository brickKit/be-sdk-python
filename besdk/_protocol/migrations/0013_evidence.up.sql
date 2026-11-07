-- The recorder's side of the evidence (P12.15-P12.18): the local evidence outbox the transfer job empties into the
-- evidence store, the change history every evidence-bearing change writes in its own transaction (a full restore's
-- replay skips a record whose id is here), and the counter whose row lock orders the commit sequence.
CREATE TABLE be_evidence_outbox (
    ord        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- an identity column, as every SDK table: no sequence to grant
    id         uuid NOT NULL,
    phase      text NOT NULL CHECK (phase IN ('intent', 'effective', 'cancelled', 'general')),
    record     bytea NOT NULL,                       -- be.evidence.record, canonical
    digest     text NOT NULL,
    stopped    text,                                 -- the store answered conflict or invalid: not sent again
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (id, phase)
);

CREATE TABLE be_change_history (
    evidence_id    uuid PRIMARY KEY,
    object_type    text NOT NULL,
    object_id      text NOT NULL,
    version_before text,
    epoch          text NOT NULL,                    -- <recovery generation>.<timeline> at the commit
    commit_seq     bigint NOT NULL,
    recorded_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (epoch, commit_seq)
);

CREATE TABLE be_evidence_counter (
    single   boolean PRIMARY KEY DEFAULT true CHECK (single),
    epoch    text,
    last_seq bigint NOT NULL DEFAULT 0
);
INSERT INTO be_evidence_counter (single) VALUES (true);
