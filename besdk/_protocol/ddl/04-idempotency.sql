-- be-protocol 1.0 reference DDL: command idempotency (P13)
--
-- Not partitioned; rows are deleted after expires_at (30 days).

CREATE TABLE IF NOT EXISTS besdk_idempotency (
    caller          TEXT        NOT NULL,                -- user:<sub> | svc:<component ID> | system
    idempotency_key TEXT        NOT NULL,
    command         TEXT        NOT NULL,                -- permission key or full rpc name
    target          TEXT        NOT NULL DEFAULT '',     -- aggregate ID; '' for a create
    request_hash    BYTEA       NOT NULL,                -- SHA-256 of the JCS-canonical fingerprint fields
    status          TEXT        NOT NULL DEFAULT 'CLAIMED' CHECK (status IN ('CLAIMED', 'DONE')),
    result          JSONB,                               -- {"status": <http>, "body": …} once DONE
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,                -- created_at + 30 days
    PRIMARY KEY (caller, idempotency_key)
);

CREATE INDEX IF NOT EXISTS besdk_idempotency_expires ON besdk_idempotency (expires_at);
