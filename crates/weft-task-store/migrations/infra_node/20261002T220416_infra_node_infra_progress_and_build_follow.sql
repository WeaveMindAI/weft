ALTER TABLE infra_node ADD COLUMN provisioning_since_unix bigint;

ALTER TABLE infra_node ADD COLUMN waiting_on text;
