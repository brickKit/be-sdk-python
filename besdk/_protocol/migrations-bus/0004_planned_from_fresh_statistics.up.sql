-- P5.33 for the PostgreSQL queue: the transport's statements keep cached generic plans; one settled while the
-- delivery table was small stays until the table is next analyzed, and autovacuum analyzes up to a naptime (60 s by
-- default) after a backlog arrived (a claim took 6.8 ms in that window against 0.13 ms after it, 20 000 waiting;
-- planning every claim instead costs 0.43 ms always; 2026-11-02). Every process that opens the transport calls this
-- every 2 seconds: it analyzes the queue's tables that changed by at least 1000 rows and at least their estimated
-- size since their last analyze, if that was over 30 seconds ago; one caller at a time.
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
    FOREACH t IN ARRAY ARRAY['be_bus_delivery', 'be_bus_message'] LOOP
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
