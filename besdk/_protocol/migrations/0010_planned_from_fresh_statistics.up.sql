-- P6.35: the forwarder reads a partition's unpublished rows in position order (epoch, then sequence); the index on
-- (stream, partition, seq) did not give that order, so every batch either walked past the published rows kept for
-- the redelivery retention (P6.12) along the position index or fetched and sorted every unpublished row (20 000
-- published rows ahead: 3.4 ms a batch of 500, against 0.6 ms along this index; 2026-11-02).
CREATE INDEX be_outbox_unpublished_in_order ON be_outbox (stream, partition, epoch_generation, epoch_timeline, seq)
    WHERE seq IS NOT NULL AND published_at IS NULL;
DROP INDEX be_outbox_unpublished;
ALTER INDEX be_outbox_unpublished_in_order RENAME TO be_outbox_unpublished;

-- P5.33: the SDK's statements keep cached generic plans; one settled while a queue table was small stays until the
-- table is next analyzed, and autovacuum analyzes up to a naptime (60 s by default) after a backlog arrived. Every
-- process that opens the database calls this every 2 seconds: it analyzes the SDK's queue tables that changed by at
-- least 1000 rows and at least their estimated size since their last analyze, if that was over 30 seconds ago;
-- the analyze invalidates the cached plans. It runs as the owner (ANALYZE needs it); one caller at a time.
CREATE FUNCTION be_analyze_stale() RETURNS SETOF text LANGUAGE plpgsql SECURITY DEFINER AS $body$
DECLARE
    t        text;
    changed  bigint;
    estimate real;
    last     timestamptz;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('be_analyze:' || current_schema(), 0)) THEN
        RETURN;
    END IF;
    FOREACH t IN ARRAY ARRAY['be_outbox', 'be_work_item', 'be_audit_log'] LOOP
        SELECT s.n_mod_since_analyze, c.reltuples, greatest(s.last_analyze, s.last_autoanalyze)
            INTO changed, estimate, last
            FROM pg_stat_all_tables s JOIN pg_class c ON c.oid = s.relid
            WHERE s.relid = format('%I.%I', current_schema(), t)::regclass;
        IF changed >= greatest(1000, estimate) AND (last IS NULL OR last < now() - interval '30 seconds') THEN
            -- skipped while a vacuum or another analyze holds the table; the next call sees it again
            EXECUTE format('ANALYZE (SKIP_LOCKED) %I.%I', current_schema(), t);
            RETURN NEXT t;
        END IF;
    END LOOP;
END
$body$;

DO $do$
BEGIN
    EXECUTE format('ALTER FUNCTION be_analyze_stale() SET search_path = %I, pg_temp', current_schema());
END
$do$;
