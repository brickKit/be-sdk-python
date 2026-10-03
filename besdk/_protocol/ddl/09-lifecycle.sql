-- be-protocol 1.0 reference DDL: data lifecycle engine (P16)

CREATE TABLE IF NOT EXISTS besdk_lifecycle_units (
    table_name      TEXT        NOT NULL,
    unit_key        TEXT        NOT NULL,                -- partition name, or the anchor month of a row unit
    range_from      TIMESTAMPTZ,
    range_to        TIMESTAMPTZ,
    list_value      TEXT,                                -- LIST partitions (e.g. an accounting period)
    state           TEXT        NOT NULL
                    CHECK (state IN ('ACTIVE', 'BLOCKED', 'SEALED', 'EXPORTING', 'EXPORTED', 'VERIFIED',
                                     'COLD_PENDING_DROP', 'COLD', 'THAWED', 'DESTROYED')),
    rows            BIGINT,
    min_id          TEXT,
    max_id          TEXT,
    unit_digest     BYTEA,                               -- SHA-256 of the canonical encoding (P16)
    chain_digest    BYTEA,                               -- SHA-256(previous chain || unit_digest)
    manifest_url    TEXT,
    manifest_sha256 BYTEA,
    sealed_at       TIMESTAMPTZ,
    cold_at         TIMESTAMPTZ,
    thawed_until    TIMESTAMPTZ,
    destroyed_at    TIMESTAMPTZ,
    blocked_reason  TEXT,
    version         BIGINT      NOT NULL DEFAULT 1,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, unit_key)
);

-- append-only, kept forever; protected by besdk_sealed_guard from creation
CREATE TABLE IF NOT EXISTS besdk_lifecycle_log (
    id         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    table_name TEXT,
    unit_key   TEXT,
    action     TEXT        NOT NULL,
    actor      TEXT,
    detail     JSONB       NOT NULL
);

CREATE TABLE IF NOT EXISTS besdk_holds (
    hold_id     TEXT        PRIMARY KEY,
    scope       JSONB       NOT NULL,                    -- which tables / units / subjects the hold covers
    reason      TEXT        NOT NULL,
    placed_by   TEXT        NOT NULL,
    placed_at   TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS besdk_erasures (
    request_id  TEXT        PRIMARY KEY,
    subject     TEXT        NOT NULL,                    -- subject type, e.g. customer
    subject_id  TEXT        NOT NULL,                    -- opaque id only, never the erased content
    received_at TIMESTAMPTZ NOT NULL,
    applied_at  TIMESTAMPTZ,
    result      JSONB
);

CREATE TABLE IF NOT EXISTS besdk_exports (
    job_id       TEXT        PRIMARY KEY,
    requested_by TEXT        NOT NULL,
    spec         JSONB       NOT NULL,                   -- {table, from, to, filter, format}
    state        TEXT        NOT NULL,
    result_url   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ
);

-- The guard installed on sealed partitions (BEFORE UPDATE OR DELETE … FOR EACH ROW and
-- BEFORE TRUNCATE … FOR EACH STATEMENT) and on besdk_lifecycle_log. SQLSTATE BE001 is mapped by the
-- runtime to FAILED_PRECONDITION / UNIT_SEALED (P16.5).
CREATE OR REPLACE FUNCTION besdk_sealed_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION USING ERRCODE = 'BE001',
        MESSAGE = format('UNIT_SEALED: %s is sealed', TG_TABLE_NAME);
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                    WHERE c.oid = to_regclass('besdk_lifecycle_log') AND t.tgname = 'besdk_lifecycle_log_append_only') THEN
        CREATE TRIGGER besdk_lifecycle_log_append_only
            BEFORE UPDATE OR DELETE ON besdk_lifecycle_log
            FOR EACH ROW EXECUTE FUNCTION besdk_sealed_guard();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
                    WHERE c.oid = to_regclass('besdk_lifecycle_log') AND t.tgname = 'besdk_lifecycle_log_no_truncate') THEN
        CREATE TRIGGER besdk_lifecycle_log_no_truncate
            BEFORE TRUNCATE ON besdk_lifecycle_log
            FOR EACH STATEMENT EXECUTE FUNCTION besdk_sealed_guard();
    END IF;
END;
$$;
