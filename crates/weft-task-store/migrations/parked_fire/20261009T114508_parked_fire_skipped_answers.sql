ALTER TABLE parked_fire ADD COLUMN skipped boolean DEFAULT false NOT NULL;

ALTER TABLE parked_fire ADD CONSTRAINT parked_fire_skipped_not_null NOT NULL skipped;
