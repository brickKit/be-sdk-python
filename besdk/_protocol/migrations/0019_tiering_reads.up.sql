-- Reads under a catalog snapshot (P15.19, P15.24, P15.27): the catalog version at which a unit's route last changed
-- (a lease taken before it may still read the old route, so the old route is not released), and the thawed copies.
ALTER TABLE be_tier_unit ADD COLUMN route_version bigint;

CREATE TABLE be_tier_thaw (
    unit       uuid PRIMARY KEY REFERENCES be_tier_unit (id),
    version    integer NOT NULL,                     -- the be_tier_row version holding the thawed copy
    expires_at timestamptz NOT NULL
);
