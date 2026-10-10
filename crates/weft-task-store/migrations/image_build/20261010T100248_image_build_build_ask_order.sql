-- Fails if the table holds any row: add it nullable, backfill, then SET NOT NULL.
ALTER TABLE version_build ADD COLUMN ask bigint NOT NULL;

ALTER TABLE version_build ADD CONSTRAINT version_build_ask_not_null NOT NULL ask;
