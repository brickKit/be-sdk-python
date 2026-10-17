-- Monthly partitions (chapter 05, P5.21-P5.24; protocol 14, "partition maintenance at run time"). A component's
-- migration declares a range-partitioned table with be_declare_monthly_partitions; be_maintain_partitions, run by
-- the SDK's maintenance job as the runtime role, creates the months ahead as the owner and does nothing else.
CREATE TABLE be_partition_plan (
    table_name   text PRIMARY KEY,
    key_column   text NOT NULL,
    key_type     text NOT NULL CHECK (key_type IN ('date', 'timestamp with time zone')),
    months_ahead integer NOT NULL CHECK (months_ahead BETWEEN 1 AND 24),
    declared_at  timestamptz NOT NULL DEFAULT now()
);

-- Only the owner (the migrate entry) writes the plan; the runtime role's default table grants do not reach it.
CREATE FUNCTION be_partition_plan_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF current_user <> (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = TG_RELID) THEN
        RAISE EXCEPTION 'be_partition_plan is written by migrations only' USING ERRCODE = '42501';
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER be_partition_plan_guard BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON be_partition_plan
    FOR EACH STATEMENT EXECUTE FUNCTION be_partition_plan_guard();

-- The maintenance function runs as its owner, in UTC, with the search path pinned to this schema (pg_temp last).
CREATE FUNCTION be_maintain_partitions() RETURNS SETOF text LANGUAGE plpgsql SECURITY DEFINER SET timezone = 'UTC' AS $body$
DECLARE
    p      record;
    m      integer;
    lo     date;
    hi     date;
    child  text;
    lo_text text;
    hi_text text;
    want   text;
    have   text;
    parent text;
    month0 date := date_trunc('month', now())::date;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('be_partitions:' || current_schema(), 0));
    FOR p IN SELECT table_name, key_type, months_ahead FROM be_partition_plan ORDER BY table_name LOOP
        IF to_regclass(quote_ident(p.table_name)) IS NULL THEN
            RAISE EXCEPTION 'the declared table % no longer exists: undeclare it with be_undeclare_monthly_partitions in the migration that drops or renames it',
                p.table_name USING ERRCODE = '42P01';
        END IF;
        FOR m IN 0..p.months_ahead LOOP
            lo := (month0 + make_interval(months => m))::date;
            hi := (lo + interval '1 month')::date;
            child := p.table_name || '_p' || to_char(lo, 'YYYYMM');
            -- the bounds as PostgreSQL prints them for the key's type, to compare with an existing partition
            EXECUTE format('SELECT %L::%s::text, %L::%s::text', lo, p.key_type, hi, p.key_type) INTO lo_text, hi_text;
            want := format('FOR VALUES FROM (%L) TO (%L)', lo_text, hi_text);
            SELECT pg_get_expr(c.relpartbound, c.oid), i.inhparent::regclass::text INTO have, parent
                FROM pg_class c LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
                WHERE c.relname = child AND c.relnamespace = current_schema()::regnamespace;
            IF FOUND THEN
                IF parent IS DISTINCT FROM p.table_name OR have IS DISTINCT FROM want THEN
                    RAISE EXCEPTION 'partition % exists but is not % of %: %', child, want, p.table_name, coalesce(have, 'detached')
                        USING ERRCODE = '42P17';
                END IF;
                CONTINUE;
            END IF;
            EXECUTE format('CREATE TABLE %I (LIKE %I INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED INCLUDING STORAGE INCLUDING COMMENTS)',
                child, p.table_name);
            EXECUTE format('ALTER TABLE %I ATTACH PARTITION %I %s', p.table_name, child, want);
            RETURN NEXT child;
        END LOOP;
    END LOOP;
END
$body$;

DO $do$
BEGIN
    EXECUTE format('ALTER FUNCTION be_maintain_partitions() SET search_path = %I, pg_temp', current_schema());
END
$do$;

-- Declaring checks the table (range-partitioned on one date or timestamptz column in this schema), records the plan
-- and creates the current month and the months ahead. It runs in the component's migration, as the owner.
CREATE FUNCTION be_declare_monthly_partitions(p_table text, p_months_ahead integer DEFAULT 3) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
    v_column text;
    v_type   text;
BEGIN
    SELECT a.attname, format_type(a.atttypid, a.atttypmod) INTO v_column, v_type
        FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = pt.partattrs[0]
        WHERE c.relname = p_table AND c.relnamespace = current_schema()::regnamespace
          AND pt.partstrat = 'r' AND pt.partnatts = 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION '% is not a table of this schema partitioned by range on one column', p_table USING ERRCODE = '42P17';
    END IF;
    IF octet_length(p_table) > 55 THEN
        RAISE EXCEPTION 'the name % is longer than 55 bytes: its partitions (<name>_pYYYYMM) would be truncated', p_table
            USING ERRCODE = '42622';
    END IF;
    IF v_type NOT IN ('date', 'timestamp with time zone') THEN
        RAISE EXCEPTION 'the partition key %.% is %, not date or timestamptz', p_table, v_column, v_type USING ERRCODE = '42P17';
    END IF;
    INSERT INTO be_partition_plan (table_name, key_column, key_type, months_ahead)
        VALUES (p_table, v_column, v_type, p_months_ahead)
        ON CONFLICT (table_name) DO UPDATE SET months_ahead = excluded.months_ahead;
    RETURN QUERY SELECT * FROM be_maintain_partitions();
END
$$;

-- Undeclaring belongs to the migration that drops or renames a declared table; existing partitions stay as they are.
CREATE FUNCTION be_undeclare_monthly_partitions(p_table text) RETURNS void LANGUAGE sql AS $$
    DELETE FROM be_partition_plan WHERE table_name = p_table;
$$;
