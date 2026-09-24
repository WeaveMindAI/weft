ALTER TABLE infra_node ADD COLUMN public_paths_json jsonb DEFAULT '{}'::jsonb NOT NULL;

ALTER TABLE infra_node ADD CONSTRAINT infra_node_public_paths_json_not_null NOT NULL public_paths_json;
