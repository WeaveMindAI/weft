CREATE TABLE program_trigger_cut (
    definition_hash text NOT NULL,
    depends_on text[] NOT NULL,
    fingerprint text,
    project_id uuid NOT NULL,
    trigger text NOT NULL,
    unwalkable text
);

ALTER TABLE program_trigger_cut ADD CONSTRAINT program_trigger_cut_check CHECK (((fingerprint IS NULL) = (unwalkable IS NOT NULL)));

ALTER TABLE program_trigger_cut ADD CONSTRAINT program_trigger_cut_pkey PRIMARY KEY (project_id, definition_hash, trigger);

ALTER TABLE program_trigger_cut ADD CONSTRAINT program_trigger_cut_project_id_definition_hash_fkey FOREIGN KEY (project_id, definition_hash) REFERENCES project_definition(project_id, definition_hash) ON DELETE CASCADE;
