ALTER TABLE trigger_activation ADD COLUMN id bigserial NOT NULL;

ALTER TABLE trigger_activation ADD CONSTRAINT trigger_activation_pkey PRIMARY KEY (id);
