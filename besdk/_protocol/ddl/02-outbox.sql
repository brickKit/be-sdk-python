-- be-protocol 1.0 reference DDL: event outbox (P12.1, P12.15)
--
-- Partitioned by created_at, one partition per week, created and dropped by the lifecycle engine
-- (class platform: a partition is dropped 14 days after all its rows are PUBLISHED). The platform
-- migration creates the current window (P16.6); this file creates the parent only.

CREATE TABLE IF NOT EXISTS besdk_outbox (
    id                UUID        NOT NULL,              -- UUIDv7 = ce-id = Nats-Msg-Id
    created_at        TIMESTAMPTZ NOT NULL,              -- = the timestamp embedded in id
    subject           TEXT        NOT NULL,              -- ce-type
    aggregate_type    TEXT        NOT NULL,              -- ce-aggregatetype, from the contract
    aggregate_id      TEXT        NOT NULL,              -- ce-subject
    aggregate_version BIGINT      NOT NULL,              -- ce-aggregateversion
    occurred_at       TIMESTAMPTZ NOT NULL,              -- ce-time
    traceparent       TEXT        NOT NULL DEFAULT '',
    causation_id      TEXT        NOT NULL DEFAULT '',   -- ce-causationid
    hop_count         INT         NOT NULL DEFAULT 0,    -- ce-hopcount
    headers           JSONB       NOT NULL DEFAULT '{}', -- other ce-* attributes, e.g. {"ce-legalentity": "LE01"}
    payload           JSONB       NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING', 'SENDING', 'PUBLISHED')),
    attempts          INT         NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_until     TIMESTAMPTZ,
    published_at      TIMESTAMPTZ,
    last_error        TEXT        NOT NULL DEFAULT '',
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS besdk_outbox_due ON besdk_outbox (next_attempt_at) WHERE status <> 'PUBLISHED';

-- Claim, each replica, each round (P10.9):
--   UPDATE besdk_outbox SET status = 'SENDING', claimed_until = now() + interval '30 seconds',
--                           attempts = attempts + 1
--    WHERE (id, created_at) IN (
--          SELECT id, created_at FROM besdk_outbox
--           WHERE (status = 'PENDING' AND next_attempt_at <= now())
--              OR (status = 'SENDING' AND claimed_until < now())
--           ORDER BY created_at, id
--           LIMIT 256
--           FOR UPDATE SKIP LOCKED)
--   RETURNING *;
-- After the broker acknowledged:  UPDATE … SET status = 'PUBLISHED', published_at = now(), claimed_until = NULL
-- After a failed publish:         UPDATE … SET status = 'PENDING', next_attempt_at = now() + <backoff 1s..1min>,
--                                              claimed_until = NULL, last_error = <redacted text>
