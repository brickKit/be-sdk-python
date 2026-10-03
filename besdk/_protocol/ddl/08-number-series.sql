-- be-protocol 1.0 reference DDL: document numbers (P11.10)

CREATE TABLE IF NOT EXISTS besdk_number_series (
    series     TEXT        NOT NULL,                     -- e.g. sales_order, voucher
    scope      TEXT        NOT NULL DEFAULT '',          -- the legal entity id, or another declared scope
    period     TEXT        NOT NULL DEFAULT '',          -- period key from the business date (vouchers: the accounting period)
    next_value BIGINT      NOT NULL DEFAULT 1,
    gapless    BOOLEAN     NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series, scope, period)
);

-- Gap-free, in the document's own transaction (after the idempotency claim succeeded):
--   INSERT INTO besdk_number_series (series, scope, period, gapless) VALUES ($1, $2, $3, true)
--   ON CONFLICT DO NOTHING;
--   UPDATE besdk_number_series SET next_value = next_value + 1, updated_at = now()
--    WHERE series = $1 AND scope = $2 AND period = $3
--   RETURNING next_value - 1;
-- Gapped, in a short transaction of its own, reserving a block of $4 numbers:
--   … SET next_value = next_value + $4 … RETURNING next_value - $4;

-- Uniqueness of the numbers themselves (P11.10). Unpartitioned on purpose: a unique index on a
-- partitioned document table must contain its partition key, so (legal_entity_id, number) could not
-- be unique across partitions there. The runtime inserts every formatted number in the document's
-- own transaction; a repeat fails that transaction with 23505 on this table (a series
-- misconfiguration, surfaced as INTERNAL), and a rollback frees the number. legal_entity_id is the
-- series' scope ('' for a series not scoped by legal entity). Rows are kept as long as the
-- component's documents may exist (class platform; never dropped by the lifecycle engine in 1.0).
CREATE TABLE IF NOT EXISTS besdk_number_allocations (
    legal_entity_id TEXT        NOT NULL,
    series          TEXT        NOT NULL,
    number          TEXT        NOT NULL,                -- the formatted document number
    document_id     TEXT        NOT NULL,                -- the document's id, for look-ups and audits
    allocated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (legal_entity_id, series, number)
);

-- In the document's transaction, right after the number was taken from besdk_number_series:
--   INSERT INTO besdk_number_allocations (legal_entity_id, series, number, document_id)
--   VALUES ($1, $2, $3, $4);
