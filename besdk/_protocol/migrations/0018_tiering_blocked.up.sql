-- Why a unit's candidates could not be sealed (P15.11): the reasons of the last try, per aggregate, and since when the
-- unit has been blocked (an alert 30 days past the residence).
ALTER TABLE be_tier_unit ADD COLUMN blocked jsonb;
ALTER TABLE be_tier_unit ADD COLUMN blocked_since timestamptz;
