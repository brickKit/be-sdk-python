-- Hot and cold data (chapter 15): the owner's unit catalog, its members (the locate index), the manifests of each
-- unit, the holds, the read leases and the catalog version, and the warm route's rows. Every table is the owner's own:
-- no other component reads them.

-- The catalog version: one counter, raised by every change of a unit's route or content (publish, release, route
-- change, erase, destroy); a list or an export reads under the version it started with (P15.24).
CREATE TABLE be_tier_catalog (
    single  boolean PRIMARY KEY DEFAULT true CHECK (single),
    version bigint NOT NULL DEFAULT 0
);
INSERT INTO be_tier_catalog DEFAULT VALUES;

CREATE TABLE be_tier_unit (
    id               uuid PRIMARY KEY,
    class            text NOT NULL,
    grouping         text NOT NULL,                  -- the class's grouping key, canonical (P15.6)
    seq              integer NOT NULL,               -- several units of one grouping: the later closings
    state            text NOT NULL CHECK (state IN ('open', 'sealed', 'published', 'released', 'destroyed')),
    route            text NOT NULL DEFAULT 'online' CHECK (route IN ('online', 'postgres', 'object-parquet')),
    manifest_version integer NOT NULL DEFAULT 0,     -- 0: no manifest (online)
    generation       bigint NOT NULL DEFAULT 0,      -- raised by every claim (P15.14)
    claim_epoch      text,                           -- the database epoch of the last claim
    members          bigint NOT NULL DEFAULT 0,
    digest           text,                           -- the sealed content's digest (P15.9)
    anchor_max       timestamptz,                    -- the latest anchor among the members
    newest_created   timestamptz,                    -- the latest creation among the members (P15.19)
    sealed_at        timestamptz,
    published_at     timestamptz,
    released_at      timestamptz,
    destroyed_at     timestamptz,
    tombstone        jsonb,                          -- destroyed: scope, digest, the basis and the time (P15.29)
    UNIQUE (class, grouping, seq)
);
CREATE INDEX be_tier_unit_state ON be_tier_unit (class, state);

-- A sealed unit's members: the root of each aggregate (its children follow it); the locate index (record -> unit) and
-- the write barrier's lookup (P15.6, P15.13).
CREATE TABLE be_tier_member (
    class   text NOT NULL,
    root_id text NOT NULL,
    unit    uuid NOT NULL REFERENCES be_tier_unit (id),
    PRIMARY KEY (class, root_id)
);
CREATE INDEX be_tier_member_unit ON be_tier_member (unit);

CREATE TABLE be_tier_manifest (
    unit       uuid NOT NULL REFERENCES be_tier_unit (id),
    version    integer NOT NULL,
    route      text NOT NULL CHECK (route IN ('postgres', 'object-parquet')),
    generation bigint NOT NULL,
    manifest   bytea NOT NULL,                       -- be.tier.manifest, canonical (P15.16)
    digest     text NOT NULL,                        -- the content's digest the manifest carries
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (unit, version)
);

-- Holds (P15.28): orthogonal to the state; a unit is held while any hold that covers it has no release.
CREATE TABLE be_tier_hold (
    id            uuid PRIMARY KEY,
    scope_kind    text NOT NULL CHECK (scope_kind IN ('unit', 'class', 'record')),
    scope         text NOT NULL,                     -- a unit id, a class, or "<class>/<root id>"
    source        text NOT NULL CHECK (source IN ('legal', 'audit', 'dispute')),
    authorized_by text NOT NULL,
    reason        text NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT now(),
    released_at   timestamptz,
    released_by   text
);
CREATE INDEX be_tier_hold_open ON be_tier_hold (scope_kind, scope) WHERE released_at IS NULL;

-- Read leases (P15.25): a long read or an export registers the catalog version it reads under; a unit published at a
-- later version is not released while such a lease is unexpired.
CREATE TABLE be_tier_lease (
    id              uuid PRIMARY KEY,
    catalog_version bigint NOT NULL,
    purpose         text NOT NULL,
    expires_at      timestamptz NOT NULL
);
CREATE INDEX be_tier_lease_live ON be_tier_lease (expires_at);

-- The warm route (route "postgres"): a published unit's rows in the owner's schema, each as its canonical row bytes.
CREATE TABLE be_tier_row (
    unit     uuid NOT NULL,
    version  integer NOT NULL,                       -- the manifest version the rows belong to
    tbl      text NOT NULL,
    pk       text NOT NULL,                          -- the primary key, canonical
    row      bytea NOT NULL,                         -- the canonical row (P15.8)
    PRIMARY KEY (unit, version, tbl, pk)
);
