ALTER TABLE infra_node RENAME COLUMN instance_id TO copy_id;

ALTER TABLE infra_node RENAME COLUMN member_id TO instance_id;

ALTER TABLE infra_node RENAME CONSTRAINT infra_node_instance_id_not_null TO infra_node_copy_id_not_null;

ALTER TABLE infra_node ADD COLUMN notes_json jsonb DEFAULT '[]'::jsonb NOT NULL;

ALTER TABLE infra_node ADD CONSTRAINT infra_node_notes_json_not_null NOT NULL notes_json;
