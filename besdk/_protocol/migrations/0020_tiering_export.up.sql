-- Asynchronous exports of cold reads (P15.26a): what was asked, by whom, and where the result is.
CREATE TABLE be_tier_export (
    id          uuid PRIMARY KEY,
    class       text NOT NULL,
    principal   text NOT NULL,                       -- the requester: the only one the status answers
    params      jsonb NOT NULL DEFAULT '{}',         -- the read's parameters, as the class's reader takes them
    state       text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'done', 'failed')),
    object_key  text,
    rows        bigint,
    reason      text,                                -- why it failed (an error reason)
    created_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    expires_at  timestamptz
);
CREATE INDEX be_tier_export_expiry ON be_tier_export (expires_at) WHERE expires_at IS NOT NULL;
