-- Projections (chapter 09, P9.10-P9.12; protocol 02 of the assembly project): local copies of other components'
-- records with their version and state, and per projection the adopted source and alignment generation.
CREATE TABLE be_projection (
    projection   text NOT NULL,
    record_id    text NOT NULL,
    version      bigint NOT NULL CHECK (version >= 0),
    state        text NOT NULL CHECK (state IN ('complete', 'incomplete', 'tombstone')),
    need_version bigint,
    impl         text,
    epoch        text,
    data         jsonb,
    aligned_gen  bigint,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, record_id),
    CHECK ((state = 'incomplete') = (need_version IS NOT NULL))
);
CREATE INDEX be_projection_incomplete ON be_projection (projection) WHERE state = 'incomplete';

CREATE TABLE be_projection_source (
    projection text PRIMARY KEY,
    impl       text,
    epoch      text,
    align_gen  bigint NOT NULL DEFAULT 0,
    status     text NOT NULL DEFAULT 'aligning' CHECK (status IN ('aligning', 'aligned', 'stalled', 'blocked')),
    aligned_at timestamptz
);
