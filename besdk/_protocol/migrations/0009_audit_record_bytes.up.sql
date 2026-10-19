-- P8.20, P8.21: a row keeps its record's canonical bytes, written with the row and sent unchanged, so the audit
-- component checks the digest against exactly the bytes it was computed over (D76). Rebuilding the record from the
-- columns at transfer gave other bytes (nulls, the database's timestamp format, key order). Rows written before this
-- migration have none and are not transferred (pre-release data only).
ALTER TABLE be_audit_log ADD COLUMN record bytea;
DROP INDEX be_audit_log_pending;
CREATE INDEX be_audit_log_pending ON be_audit_log (ord) WHERE transfer_state = 'pending' AND seq IS NOT NULL AND record IS NOT NULL;
