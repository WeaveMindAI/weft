-- Fails if the table holds any row: add it nullable, backfill, then SET NOT NULL.
ALTER TABLE install_domain_door ADD COLUMN served_at_ms bigint NOT NULL;

ALTER TABLE install_domain_door ADD CONSTRAINT install_domain_door_served_at_ms_not_null NOT NULL served_at_ms;
