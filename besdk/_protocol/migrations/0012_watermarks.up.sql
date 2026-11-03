-- Watermark sources, the consumer's side (chapter 09, P9.19-P9.29; protocol 02 of the assembly project, steps 4-11
-- and its alignment state table). The adopted implementation instance, epoch, alignment generation and state stay in
-- be_projection_source; these tables hold what a watermark source adds to it.

-- A tombstone written for a purged record accepts only inputs read past this position of its partition (P9.11).
ALTER TABLE be_projection ADD COLUMN min_read_seq bigint;

CREATE TABLE be_watermark_source (
    projection    text PRIMARY KEY REFERENCES be_projection_source (projection),
    source        text NOT NULL,     -- the source component
    stream        text NOT NULL,
    partitions    integer NOT NULL CHECK (partitions BETWEEN 1 AND 256),
    deployment    text NOT NULL,     -- this deployment's identity: a response from another one blocks (P9.26)
    system_id     text,              -- the adopted system identifier; NULL until the first primary response
    confirmed     boolean NOT NULL DEFAULT false, -- blocked: an operator confirmed the correction
    rollback      boolean NOT NULL DEFAULT false, -- a cause of the alignment is a rollback
    refetch       boolean NOT NULL DEFAULT false, -- a full re-fetch is due (the state table's refetch actions)
    stalled_gen   bigint,            -- the recovery generation when it entered stalled
    blocked_gen   bigint,            -- ... when it entered blocked
    prior_state   text,              -- the state before blocked
    align_db_epoch text,             -- the consumer database's epoch of the alignment generation (P9.28)
    max_seen      timestamptz,       -- the largest primary time observed (P9.20)
    db_epoch      text               -- the consumer database's epoch it was observed in
);

-- Each partition's contiguous applied position and the identity of the row there (P9.19), its P0 while aligning
-- (P9.28) and the m_p of the last recorded echo within the contiguous range (P9.23).
CREATE TABLE be_watermark_partition (
    projection   text NOT NULL REFERENCES be_watermark_source (projection),
    partition    integer NOT NULL,
    seq          bigint,
    row_identity jsonb,
    p0           bigint,
    echo_mp      timestamptz,
    PRIMARY KEY (projection, partition)
);

-- The pulls this consumer made and still keeps: their nonce and m_p (P9.21); dropped when the clock goes back or the
-- consumer's epoch changes (P9.20).
CREATE TABLE be_watermark_pull (
    projection text NOT NULL,
    nonce      text NOT NULL,
    m_p        timestamptz NOT NULL,
    PRIMARY KEY (projection, nonce)
);

-- Failing events by position until they are quarantined (P9.25).
CREATE TABLE be_watermark_failure (
    projection text NOT NULL,
    partition  integer NOT NULL,
    seq        bigint NOT NULL,
    attempts   integer NOT NULL,
    first_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, partition, seq)
);

-- Quarantined subjects with their cause and the event's full content (P9.25).
CREATE TABLE be_watermark_quarantine (
    projection  text NOT NULL,
    subject     text NOT NULL,
    impl        text NOT NULL,
    epoch       text NOT NULL,
    align_gen   bigint NOT NULL,
    partition   integer NOT NULL,
    seq         bigint NOT NULL,
    event       jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, subject, partition, seq)
);
CREATE INDEX be_watermark_quarantine_subject ON be_watermark_quarantine (projection, subject);
