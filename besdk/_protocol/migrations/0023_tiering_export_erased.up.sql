-- An erasure voids the exports that may hold what it erased (P15.31, P15.38): a pending export marked here is written
-- again from a new read rather than finished.
ALTER TABLE be_tier_export ADD COLUMN erased_at timestamptz;
