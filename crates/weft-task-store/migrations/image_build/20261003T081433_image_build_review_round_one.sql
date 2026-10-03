ALTER TABLE image_build ADD COLUMN builder_id text;

ALTER TABLE image_build ADD COLUMN failing_since bigint;

ALTER TABLE image_build ALTER COLUMN lane DROP NOT NULL;

ALTER TABLE image_claim ADD COLUMN project_id uuid;

-- Throws away what is in image_build.started_by. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE image_build DROP COLUMN started_by;

ALTER TABLE image_build DROP CONSTRAINT IF EXISTS image_build_lane_not_null;
