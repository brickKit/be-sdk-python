-- be-protocol 1.0 reference DDL: platform version (P11.3)
--
-- Normative. Every besdk_* table lives in the component's own schema and is created by the
-- runtime's platform migration, which runs in the migration step logged in as the owner role
-- (PG_OWNER_USER), so the owner owns it. The runtime role (PG_USER) gets DML on it through the
-- schema's default privileges, which the project's database initialisation sets (never a GRANT
-- here). Names are unqualified and resolve through search_path = PG_SCHEMA (P11.2). Component SQL
-- never reads or writes a besdk_* table (gate platform-table-scan). Files are applied in name
-- order; every statement is idempotent. PostgreSQL >= 14.

CREATE TABLE IF NOT EXISTS besdk_platform_version (
    component  TEXT        PRIMARY KEY,               -- the component ID that owns this schema
    version    INT         NOT NULL,                  -- the platform migration version applied
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
