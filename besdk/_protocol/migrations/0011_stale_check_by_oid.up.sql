-- P5.33's check, cheaper (2026-11-03): the same decision read by oid from the statistics functions instead of three
-- reads of the pg_stat_all_tables view. An idle member calls it every 2 seconds and it was then the largest statement
-- its background work ran: 0.18-0.21 ms a call against 0.055-0.062 ms this way (pgbench in one session).
CREATE OR REPLACE FUNCTION be_analyze_stale() RETURNS SETOF text LANGUAGE plpgsql SECURITY DEFINER AS $body$
DECLARE
    t   text;
    rel oid;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtextextended('be_analyze:' || current_schema(), 0)) THEN
        RETURN;
    END IF;
    FOREACH t IN ARRAY ARRAY['be_outbox', 'be_work_item', 'be_audit_log'] LOOP
        rel := format('%I.%I', current_schema(), t)::regclass;
        -- changed by at least 1000 rows and at least the estimated size (-1: never analyzed), last analyzed over
        -- 30 seconds ago; a table without statistics yet has changed nothing
        IF pg_stat_get_mod_since_analyze(rel) >= greatest(1000, (SELECT reltuples FROM pg_class WHERE oid = rel))
            AND coalesce(greatest(pg_stat_get_last_analyze_time(rel), pg_stat_get_last_autoanalyze_time(rel))
                < now() - interval '30 seconds', true) THEN
            -- skipped while a vacuum or another analyze holds the table; the next call sees it again
            EXECUTE format('ANALYZE (SKIP_LOCKED) %I.%I', current_schema(), t);
            RETURN NEXT t;
        END IF;
    END LOOP;
END
$body$;

-- a replaced definition drops the function's settings: pin the search path again
DO $do$
BEGIN
    EXECUTE format('ALTER FUNCTION be_analyze_stale() SET search_path = %I, pg_temp', current_schema());
END
$do$;
