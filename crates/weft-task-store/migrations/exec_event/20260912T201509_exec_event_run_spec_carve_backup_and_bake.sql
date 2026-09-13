ALTER TABLE signal ADD COLUMN program_json jsonb;

ALTER TABLE signal ADD COLUMN setup_color uuid;

ALTER TABLE signal ADD COLUMN source_version text;

CREATE TABLE trigger_bake (
    bake_json text NOT NULL,
    program_json text NOT NULL,
    project_id text NOT NULL
);

CREATE TABLE trigger_setup (
    color text NOT NULL,
    project_id text NOT NULL
);

ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_bake_json_not_null NOT NULL bake_json;

ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_pkey PRIMARY KEY (project_id, program_json);

ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_program_json_not_null NOT NULL program_json;

ALTER TABLE trigger_bake ADD CONSTRAINT trigger_bake_project_id_not_null NOT NULL project_id;

ALTER TABLE trigger_setup ADD CONSTRAINT trigger_setup_color_key UNIQUE (color);

ALTER TABLE trigger_setup ADD CONSTRAINT trigger_setup_color_not_null NOT NULL color;

ALTER TABLE trigger_setup ADD CONSTRAINT trigger_setup_pkey PRIMARY KEY (project_id);

ALTER TABLE trigger_setup ADD CONSTRAINT trigger_setup_project_id_not_null NOT NULL project_id;
