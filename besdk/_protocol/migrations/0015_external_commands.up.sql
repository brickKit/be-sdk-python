-- External commands (chapter 13; the assembly project's protocol 10, "external commands"): an adapter writes a
-- command in the business transaction, a work item sends it, and every attempt is kept. Idempotent by the requesting
-- component and its business reference; the parameters' digest tells a reused reference from a retry.
CREATE TABLE be_external_command (
    id                uuid PRIMARY KEY,                -- UUIDv7, the SDK's
    requester         text NOT NULL,                   -- the business component
    reference         text NOT NULL,                   -- its business reference: also the external idempotency key
    operation         text NOT NULL,
    params            bytea NOT NULL,                  -- canonical, be.external.command_params
    params_digest     text NOT NULL,
    irrevocable       boolean NOT NULL,
    state             text NOT NULL CHECK (state IN ('prepared', 'retry_wait', 'sending', 'unknown', 'accepted',
                          'manual', 'succeeded', 'failed', 'rejected')),
    reason            text,                            -- why the last transition happened
    external_id       text,                            -- the external system's number
    expires_at        timestamptz,                     -- the request's expiry, when the external system takes one
    resend_deadline   timestamptz,                     -- P13.7
    first_reached_at  timestamptz,                     -- the earliest send that may have reached the external system
    last_reached_at   timestamptz,                     -- the latest one
    unknown_since     timestamptz,                     -- since when unknown (the alert and the manual limit)
    accepted_since    timestamptz,                     -- since when accepted (the finality and the manual limit)
    outcome           jsonb,                           -- the terminal result (external status, amounts) as reported
    settled_at        timestamptz,
    next_at           timestamptz NOT NULL DEFAULT now(),
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    UNIQUE (requester, reference)
);
CREATE INDEX be_external_command_due ON be_external_command (next_at)
    WHERE state IN ('prepared', 'retry_wait', 'unknown', 'accepted');

CREATE TABLE be_external_attempt (
    command    uuid NOT NULL REFERENCES be_external_command,
    n          int NOT NULL,
    kind       text NOT NULL CHECK (kind IN ('send', 'query')),
    started_at timestamptz NOT NULL,
    ended_at   timestamptz,
    result     text,                                   -- the classification (P13.5) or the query's answer
    response   text,                                   -- a digest of the external response
    reached    boolean NOT NULL DEFAULT false,         -- a send that may have reached the external system
    PRIMARY KEY (command, n)
);
