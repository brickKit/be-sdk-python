-- Events (chapter 06): the deployment recovery generation and the database epoch, the outbox with its per-partition
-- counters and epoch history, and the consumer's inbox and dead letters. Every SDK applies this same file.
CREATE TABLE be_recovery_generation (
    single     boolean PRIMARY KEY DEFAULT true CHECK (single),
    generation bigint NOT NULL CHECK (generation >= 1)
);
INSERT INTO be_recovery_generation (generation) VALUES (1);

-- (deployment recovery generation, current write timeline); fails on a standby, which refuses numbering (P6.8)
CREATE FUNCTION be_current_db_epoch(OUT generation bigint, OUT timeline bigint) LANGUAGE sql VOLATILE AS $$
    SELECT g.generation, ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::bigint
    FROM be_recovery_generation g
$$;

CREATE TABLE be_outbox (
    ord              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL UNIQUE,
    stream           text NOT NULL,
    partition        integer NOT NULL CHECK (partition >= 0),
    type             text NOT NULL,
    occurred_at      timestamptz NOT NULL,
    record_id        text NOT NULL,
    version          bigint NOT NULL,
    projection       text NOT NULL CHECK (projection IN ('full', 'partial', 'delete')),
    legal_entity     text,
    subjects         jsonb,
    causation        uuid,
    hops             integer NOT NULL CHECK (hops >= 0),
    trace            jsonb,
    payload          bytea NOT NULL,
    claim            boolean NOT NULL DEFAULT false,
    digest           text NOT NULL,
    seq              bigint,
    epoch_generation bigint,
    epoch_timeline   bigint,
    published_at     timestamptz,
    CHECK ((seq IS NULL) = (epoch_generation IS NULL) AND (seq IS NULL) = (epoch_timeline IS NULL))
);
CREATE INDEX be_outbox_unnumbered ON be_outbox (stream, partition, ord) WHERE seq IS NULL;
CREATE UNIQUE INDEX be_outbox_position ON be_outbox (stream, partition, epoch_generation, epoch_timeline, seq)
    WHERE seq IS NOT NULL;
CREATE INDEX be_outbox_unpublished ON be_outbox (stream, partition, seq) WHERE seq IS NOT NULL AND published_at IS NULL;
CREATE INDEX be_outbox_published ON be_outbox (published_at) WHERE published_at IS NOT NULL;

CREATE TABLE be_outbox_partition (
    stream           text NOT NULL,
    partition        integer NOT NULL,
    epoch_generation bigint,
    epoch_timeline   bigint,
    last_seq         bigint NOT NULL DEFAULT 0,
    lease_owner      text,
    lease_until      timestamptz,
    PRIMARY KEY (stream, partition)
);

CREATE TABLE be_epoch_history (
    stream          text NOT NULL,
    partition       integer NOT NULL,
    old_generation  bigint NOT NULL,
    old_timeline    bigint NOT NULL,
    fork_seq        bigint NOT NULL,
    new_generation  bigint NOT NULL,
    new_timeline    bigint NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, partition, new_generation, new_timeline)
);

-- deduplication by identity is never partitioned (protocol 14)
CREATE TABLE be_inbox (
    subscription text NOT NULL,
    event_id     uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscription, event_id)
);

CREATE TABLE be_dead_letter (
    subscription text NOT NULL,
    event_id     text NOT NULL,
    type         text NOT NULL,
    headers      jsonb NOT NULL,
    payload      bytea NOT NULL,
    error        text NOT NULL,
    attempts     integer NOT NULL,
    recorded_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscription, event_id)
);
