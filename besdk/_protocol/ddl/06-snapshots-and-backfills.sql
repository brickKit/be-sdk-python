-- be-protocol 1.0 reference DDL: snapshot backfill progress (P15.3) and resumable backfills (P11.4)

CREATE TABLE IF NOT EXISTS besdk_snapshot_sync (
    name         TEXT        PRIMARY KEY,                -- the snapshot's name
    status       TEXT        NOT NULL DEFAULT 'PENDING', -- PENDING | RUNNING | DONE
    cursor       TEXT        NOT NULL DEFAULT '',        -- the upstream List cursor to resume from
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS besdk_backfill (
    name       TEXT        PRIMARY KEY,                  -- the backfill step's name
    cursor     TEXT        NOT NULL DEFAULT '',
    done       BOOLEAN     NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
