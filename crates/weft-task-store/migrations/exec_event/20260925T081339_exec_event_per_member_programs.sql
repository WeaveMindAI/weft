ALTER TABLE execution_color ADD COLUMN fired_by text;

ALTER TABLE execution_color ADD COLUMN member_id text;

ALTER TABLE signal ADD COLUMN activation_trigger text;

ALTER TABLE signal ADD COLUMN member_id text;

ALTER TABLE signal_token ADD COLUMN expires_at bigint;

ALTER TABLE signal_token ADD COLUMN member_id text;

ALTER TABLE trigger_bake ADD COLUMN member_id text;

ALTER TABLE signal_token ADD CONSTRAINT signal_token_member_expires CHECK (((member_id IS NULL) OR (expires_at IS NOT NULL)));

ALTER TABLE signal_token ADD CONSTRAINT signal_token_member_has_one_project CHECK (((member_id IS NULL) OR (cardinality(allowed_projects) = 1)));

ALTER TABLE trigger_setup DROP CONSTRAINT trigger_setup_pkey;
ALTER TABLE trigger_setup ADD CONSTRAINT trigger_setup_pkey PRIMARY KEY (color);

CREATE INDEX idx_execution_color_member ON execution_color USING btree (project_id, member_id) WHERE (member_id IS NOT NULL);

CREATE INDEX idx_signal_activation ON signal USING btree (project_id, activation_trigger, member_id) WHERE (activation_trigger IS NOT NULL);

DROP INDEX idx_signal_entry_node;
CREATE UNIQUE INDEX idx_signal_entry_node ON signal USING btree (project_id, node_id, member_id) NULLS NOT DISTINCT WHERE (is_resume = false);

CREATE UNIQUE INDEX idx_trigger_bake_key ON trigger_bake USING btree (project_id, member_id, program_hash) NULLS NOT DISTINCT;

CREATE INDEX idx_trigger_setup_project ON trigger_setup USING btree (project_id);

ALTER TABLE trigger_bake DROP CONSTRAINT trigger_bake_pkey;

ALTER TABLE trigger_setup DROP CONSTRAINT trigger_setup_color_key;
