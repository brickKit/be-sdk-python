-- Old manifest versions released (P15.39): when the data of a version a newer one replaced was deleted.
ALTER TABLE be_tier_manifest ADD COLUMN superseded_at timestamptz;
