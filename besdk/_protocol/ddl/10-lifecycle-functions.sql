-- be-protocol 1.0 reference DDL: the DDL the runtime may perform (P10.12, P16.2)
--
-- The runtime role (PG_USER) has DML only and never logs in as the owner (PG_OWNER_USER). The few
-- DDL steps the lifecycle engine must take while the service runs go through these functions,
-- which the platform migration creates as the owner. They are SECURITY DEFINER with the
-- search_path pinned to the schema they were created in (SET search_path FROM CURRENT: no schema
-- literal), act only on tables of that schema, and quote every identifier. Only roles with USAGE
-- on the schema can reach them: the owner and the runtime role (in a shell, after SET LOCAL ROLE).
-- DETACH PARTITION … CONCURRENTLY cannot run inside a function or a transaction block; it is used
-- only by the migration step. At run time an expired platform or queue partition is detached with
-- a plain DETACH under the caller's short lock_timeout.

-- create one RANGE partition [p_from, p_to) of p_parent, named p_name; idempotent
CREATE OR REPLACE FUNCTION besdk_ensure_range_partition(p_parent text, p_name text, p_from timestamptz, p_to timestamptz)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    v_schema text := current_schema();
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = v_schema AND c.relname = p_parent AND pt.partstrat = 'r') THEN
        RAISE EXCEPTION USING ERRCODE = '42P01', MESSAGE = format('%s is not a range-partitioned table of this schema', p_parent);
    END IF;
    IF p_name !~ ('^' || p_parent || '_[a-z0-9_]+$') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = format('partition name %s must start with %s_', p_name, p_parent);
    END IF;
    IF to_regclass(format('%I.%I', v_schema, p_name)) IS NOT NULL THEN
        RETURN false;
    END IF;
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_schema, p_name, v_schema, p_parent);
    EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)', v_schema, p_parent, v_schema, p_name, p_from, p_to);
    RETURN true;
END;
$$;

-- create one LIST partition of p_parent for p_value (an accounting period, opened by a command); idempotent
CREATE OR REPLACE FUNCTION besdk_ensure_list_partition(p_parent text, p_name text, p_value text)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    v_schema text := current_schema();
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = v_schema AND c.relname = p_parent AND pt.partstrat = 'l') THEN
        RAISE EXCEPTION USING ERRCODE = '42P01', MESSAGE = format('%s is not a list-partitioned table of this schema', p_parent);
    END IF;
    IF p_name !~ ('^' || p_parent || '_[a-z0-9_]+$') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = format('partition name %s must start with %s_', p_name, p_parent);
    END IF;
    IF to_regclass(format('%I.%I', v_schema, p_name)) IS NOT NULL THEN
        RETURN false;
    END IF;
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_schema, p_name, v_schema, p_parent);
    EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%L)', v_schema, p_parent, v_schema, p_name, p_value);
    RETURN true;
END;
$$;

-- install the sealed-unit guard on one table or partition of this schema; idempotent
CREATE OR REPLACE FUNCTION besdk_seal_table(p_table text)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    v_schema text := current_schema();
    v_oid    regclass := to_regclass(format('%I.%I', current_schema(), p_table));
BEGIN
    IF v_oid IS NULL OR p_table LIKE 'besdk\_%' THEN
        RAISE EXCEPTION USING ERRCODE = '42P01', MESSAGE = format('%s is not a sealable table of this schema', p_table);
    END IF;
    IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_oid AND tgname = 'besdk_sealed_rows') THEN
        RETURN false;
    END IF;
    EXECUTE format('CREATE TRIGGER besdk_sealed_rows BEFORE UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION besdk_sealed_guard()', v_schema, p_table);
    EXECUTE format('CREATE TRIGGER besdk_sealed_truncate BEFORE TRUNCATE ON %I.%I FOR EACH STATEMENT EXECUTE FUNCTION besdk_sealed_guard()', v_schema, p_table);
    RETURN true;
END;
$$;

-- detach (plain, not CONCURRENTLY) and drop one partition of p_parent; idempotent
CREATE OR REPLACE FUNCTION besdk_drop_partition(p_parent text, p_name text)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    v_schema text := current_schema();
    v_part   regclass := to_regclass(format('%I.%I', current_schema(), p_name));
BEGIN
    IF v_part IS NULL THEN
        RETURN false;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = v_part
                      AND i.inhparent = to_regclass(format('%I.%I', v_schema, p_parent))) THEN
        RAISE EXCEPTION USING ERRCODE = '42P01', MESSAGE = format('%s is not a partition of %s', p_name, p_parent);
    END IF;
    EXECUTE format('ALTER TABLE %I.%I DETACH PARTITION %I.%I', v_schema, p_parent, v_schema, p_name);
    EXECUTE format('DROP TABLE %I.%I', v_schema, p_name);
    RETURN true;
END;
$$;

-- thaw, step 1: create the table that will hold one cold unit again, detached and empty, shaped like
-- p_parent; the runtime then loads the unit's rows into it with plain INSERTs (it has DML on the
-- table through the schema's default privileges, because the table is created by the owner); idempotent
CREATE OR REPLACE FUNCTION besdk_thaw_create(p_parent text, p_name text)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    v_schema text := current_schema();
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_partitioned_table pt JOIN pg_class c ON c.oid = pt.partrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = v_schema AND c.relname = p_parent) OR p_parent LIKE 'besdk\_%' THEN
        RAISE EXCEPTION USING ERRCODE = '42P01', MESSAGE = format('%s is not a partitioned business table of this schema', p_parent);
    END IF;
    IF p_name !~ ('^' || p_parent || '_[a-z0-9_]+$') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = format('partition name %s must start with %s_', p_name, p_parent);
    END IF;
    IF to_regclass(format('%I.%I', v_schema, p_name)) IS NOT NULL THEN
        RETURN false;
    END IF;
    EXECUTE format('CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', v_schema, p_name, v_schema, p_parent);
    RETURN true;
END;
$$;

-- thaw, step 2: seal the loaded table (read-only from now on) and attach it as the RANGE partition
-- [p_from, p_to), or, with p_value, as the LIST partition for p_value; idempotent. Re-freezing a
-- thawed unit at the end of its window drops it with besdk_drop_partition (its cold copy stays).
CREATE OR REPLACE FUNCTION besdk_thaw_attach(p_parent text, p_name text, p_from timestamptz, p_to timestamptz, p_value text DEFAULT NULL)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $$
DECLARE
    v_schema text := current_schema();
    v_part   regclass := to_regclass(format('%I.%I', current_schema(), p_name));
BEGIN
    IF v_part IS NULL OR p_name !~ ('^' || p_parent || '_[a-z0-9_]+$') THEN
        RAISE EXCEPTION USING ERRCODE = '42P01', MESSAGE = format('%s is not a thawed table of %s', p_name, p_parent);
    END IF;
    IF EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = v_part) THEN
        RETURN false;
    END IF;
    PERFORM besdk_seal_table(p_name);
    IF p_value IS NULL THEN
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)', v_schema, p_parent, v_schema, p_name, p_from, p_to);
    ELSE
        EXECUTE format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES IN (%L)', v_schema, p_parent, v_schema, p_name, p_value);
    END IF;
    RETURN true;
END;
$$;
