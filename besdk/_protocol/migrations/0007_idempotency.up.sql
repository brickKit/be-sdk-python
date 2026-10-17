-- Command idempotency keys (chapter 04, P4.21-P4.25): one row per (principal, key), claimed by an atomic insert in
-- the command's transaction together with its writes and its outcome. Not partitioned: it deduplicates by identity.
CREATE TABLE be_idempotency (
    principal   text NOT NULL,
    key         text NOT NULL,
    fingerprint text NOT NULL,
    operation   text NOT NULL,
    status      integer NOT NULL,
    result      jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL,
    PRIMARY KEY (principal, key)
);
CREATE INDEX be_idempotency_expiry ON be_idempotency (expires_at);
