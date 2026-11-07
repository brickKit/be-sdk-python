-- Consumer progress and redelivery (P6.36-P6.40; the assembly project's protocol 15, step 5 and "a consumer's own
-- database went back"): the inbox keeps each event's position at its source; a subscription's starts mark its runs
-- (a delivery lifetime counts running time only); the database epoch a consumer last ran on tells it its database
-- went back; a producer remembers how far it purged; a consumer records what could not be redelivered.

-- The source position of each event an inbox row stands for (from the envelope: source, beimpl, bestream, bepartition,
-- beepoch, beseq). Rows written before this migration have none and take no part in the progress. No index: the
-- progress is read at a recovery, a scan then is cheap next to an index every handled event would pay for.
ALTER TABLE be_inbox
    ADD COLUMN source_component text,
    ADD COLUMN source_impl      text,
    ADD COLUMN source_stream    text,
    ADD COLUMN source_partition integer,
    ADD COLUMN epoch_generation bigint,
    ADD COLUMN epoch_timeline   bigint,
    ADD COLUMN seq              bigint;

-- One row each time an instance of a subscription starts receiving (the database's clock, as processed_at).
CREATE TABLE be_subscription_run (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription text NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX be_subscription_run_by_subscription ON be_subscription_run (subscription, started_at);

-- The database epoch (be_current_db_epoch) the consumer side last ran on: a different one at a start means the
-- database went back (P6.39).
CREATE TABLE be_consumer_epoch (
    single           boolean PRIMARY KEY DEFAULT true CHECK (single),
    epoch_generation bigint NOT NULL,
    epoch_timeline   bigint NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- What a redelivery could not cover (P6.40): the rows after the consumer's progress were purged at the producer, or
-- the consumer had seen rows the producer no longer has. The recovery report lists them.
CREATE TABLE be_resend_gap (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscription     text NOT NULL,
    source_component text NOT NULL,
    source_stream    text NOT NULL,
    source_partition integer NOT NULL,
    kind             text NOT NULL CHECK (kind IN ('beyond_retention', 'ahead')),
    progress         text,                          -- <impl>/<generation>.<timeline>/<seq>, or null: none
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

-- The largest position be.outbox.purge deleted on each partition (P6.12): a redelivery from before it is beyond the
-- retention.
ALTER TABLE be_outbox_partition
    ADD COLUMN purged_generation bigint,
    ADD COLUMN purged_timeline   bigint,
    ADD COLUMN purged_seq        bigint;
