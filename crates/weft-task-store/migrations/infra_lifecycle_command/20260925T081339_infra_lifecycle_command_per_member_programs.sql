ALTER TABLE infra_lifecycle_command ADD COLUMN every_copy boolean DEFAULT false NOT NULL;

ALTER TABLE infra_lifecycle_command ADD COLUMN member_id text;

ALTER TABLE infra_lifecycle_command ADD CONSTRAINT infra_lifecycle_command_every_copy_not_null NOT NULL every_copy;

DROP INDEX uq_lifecycle_cmd_pending_apply;
CREATE UNIQUE INDEX uq_lifecycle_cmd_pending_apply ON infra_lifecycle_command USING btree (project_id, node_id, member_id) NULLS NOT DISTINCT WHERE ((completed_at_unix IS NULL) AND (verb = 'apply'::text));
