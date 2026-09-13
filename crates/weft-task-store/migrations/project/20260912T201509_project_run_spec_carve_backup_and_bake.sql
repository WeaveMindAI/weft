ALTER TABLE project ADD COLUMN running_source jsonb;

CREATE TABLE project_code (
    binary_hash text NOT NULL,
    implementations jsonb NOT NULL,
    project_id uuid NOT NULL
);

ALTER TABLE project_code ADD CONSTRAINT project_code_binary_hash_not_null NOT NULL binary_hash;

ALTER TABLE project_code ADD CONSTRAINT project_code_implementations_not_null NOT NULL implementations;

ALTER TABLE project_code ADD CONSTRAINT project_code_pkey PRIMARY KEY (project_id, binary_hash);

ALTER TABLE project_code ADD CONSTRAINT project_code_project_id_fkey FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE;

ALTER TABLE project_code ADD CONSTRAINT project_code_project_id_not_null NOT NULL project_id;
