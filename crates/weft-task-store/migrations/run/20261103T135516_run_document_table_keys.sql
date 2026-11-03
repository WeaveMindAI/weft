ALTER TABLE trigger_bake ADD COLUMN id bigserial NOT NULL;

ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_pkey PRIMARY KEY (id);
