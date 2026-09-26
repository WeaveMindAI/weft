ALTER TABLE infra_node ADD COLUMN member_id text;

CREATE UNIQUE INDEX idx_infra_node_copy ON infra_node USING btree (project_id, node_id, member_id) NULLS NOT DISTINCT;

ALTER TABLE infra_node DROP CONSTRAINT infra_node_pkey;
