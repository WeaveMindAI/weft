CREATE TABLE version_build (
    asked_at bigint NOT NULL,
    ended_at bigint,
    id uuid NOT NULL,
    images text[] NOT NULL,
    manifest jsonb NOT NULL,
    program jsonb NOT NULL,
    project_id uuid NOT NULL,
    project_name text NOT NULL,
    reason text,
    registered jsonb,
    state text NOT NULL,
    tenant_id text NOT NULL,
    waits_on jsonb NOT NULL
);

ALTER TABLE version_build ADD CONSTRAINT version_build_asked_at_not_null NOT NULL asked_at;

ALTER TABLE version_build ADD CONSTRAINT version_build_id_not_null NOT NULL id;

ALTER TABLE version_build ADD CONSTRAINT version_build_images_not_null NOT NULL images;

ALTER TABLE version_build ADD CONSTRAINT version_build_manifest_not_null NOT NULL manifest;

ALTER TABLE version_build ADD CONSTRAINT version_build_pkey PRIMARY KEY (id);

ALTER TABLE version_build ADD CONSTRAINT version_build_program_not_null NOT NULL program;

ALTER TABLE version_build ADD CONSTRAINT version_build_project_id_not_null NOT NULL project_id;

ALTER TABLE version_build ADD CONSTRAINT version_build_project_name_not_null NOT NULL project_name;

ALTER TABLE version_build ADD CONSTRAINT version_build_state_check CHECK ((state = ANY (ARRAY['waiting'::text, 'registered'::text, 'failed'::text, 'cancelled'::text, 'superseded'::text])));

ALTER TABLE version_build ADD CONSTRAINT version_build_state_not_null NOT NULL state;

ALTER TABLE version_build ADD CONSTRAINT version_build_tenant_id_not_null NOT NULL tenant_id;

ALTER TABLE version_build ADD CONSTRAINT version_build_waits_on_not_null NOT NULL waits_on;

CREATE UNIQUE INDEX version_build_one_waiting ON version_build USING btree (project_id) WHERE (state = 'waiting'::text);
