ALTER TABLE image_build RENAME COLUMN driver_replica TO started_by;

ALTER TABLE image_build RENAME COLUMN driver_until TO held_until;

ALTER TABLE image_build RENAME CONSTRAINT image_build_driver_replica_not_null TO image_build_started_by_not_null;

ALTER TABLE image_build RENAME CONSTRAINT image_build_driver_until_not_null TO image_build_held_until_not_null;

ALTER TABLE image_build ADD COLUMN log_url text;
