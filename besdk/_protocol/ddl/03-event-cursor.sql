-- be-protocol 1.0 reference DDL: consumer cursor (P12.6)
--
-- Not partitioned, on purpose: a partition key in the primary key would let two concurrent
-- deliveries both insert. Bounded by retention: rows unseen for 30 days are deleted (P12.15).

CREATE TABLE IF NOT EXISTS besdk_event_cursor (
    consumer       TEXT        NOT NULL DEFAULT '',    -- projection name; '' is the component's default
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    version        BIGINT      NOT NULL,
    event_id       TEXT        NOT NULL,
    seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, aggregate_type, aggregate_id)
);

CREATE INDEX IF NOT EXISTS besdk_event_cursor_seen ON besdk_event_cursor (seen_at);

-- Decide "new or stale" and advance, in the handler's transaction; no row returned = skip:
--   INSERT INTO besdk_event_cursor (consumer, aggregate_type, aggregate_id, version, event_id)
--   VALUES ($1, $2, $3, $4, $5)
--   ON CONFLICT (consumer, aggregate_type, aggregate_id) DO UPDATE
--      SET version = EXCLUDED.version, event_id = EXCLUDED.event_id, seen_at = now()
--    WHERE besdk_event_cursor.version < EXCLUDED.version
--   RETURNING 1;
