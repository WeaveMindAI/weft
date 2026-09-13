-- Fails if the table holds any row: add it nullable, backfill, then SET NOT NULL.
ALTER TABLE trigger_bake ADD COLUMN program_hash text NOT NULL;

ALTER TABLE trigger_bake DROP CONSTRAINT trigger_bake_pkey;
ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_pkey PRIMARY KEY (project_id, program_hash);

ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_program_hash_not_null NOT NULL program_hash;

-- Throws away what is in trigger_bake.program_json. Ship this in a later release than the one 
-- that stopped reading the column, so the old pods do not fall over.
ALTER TABLE trigger_bake DROP COLUMN program_json;
