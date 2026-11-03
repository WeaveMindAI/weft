ALTER TABLE infra_node ADD COLUMN id bigserial NOT NULL;

ALTER TABLE infra_node ADD CONSTRAINT infra_node_pkey PRIMARY KEY (id);
