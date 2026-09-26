ALTER TABLE access_connect ADD COLUMN member_id text;

ALTER TABLE access_grant ADD COLUMN member_id text;

ALTER TABLE access_grant ADD CONSTRAINT access_grant_member_has_project CHECK (((member_id IS NULL) OR (project_id IS NOT NULL)));

CREATE INDEX access_grant_member ON access_grant USING btree (project_id, member_id) WHERE (member_id IS NOT NULL);

DROP INDEX access_grant_published;
CREATE UNIQUE INDEX access_grant_published ON access_grant USING btree (tenant_id, project_id, published_by_node, service, member_id) NULLS NOT DISTINCT WHERE (published_by_node IS NOT NULL);

ALTER TABLE access_picker ADD COLUMN member_id text;

ALTER TABLE access_picker ADD COLUMN project_id uuid;

CREATE TABLE member_value (
    field text NOT NULL,
    grant_id uuid,
    member_id text NOT NULL,
    project_id uuid NOT NULL,
    set_at timestamp with time zone DEFAULT now() NOT NULL,
    step text NOT NULL,
    tenant_id text NOT NULL,
    value jsonb NOT NULL
);

ALTER TABLE signal_subscription ADD COLUMN member_id text;

ALTER TABLE signal_subscription ADD COLUMN project_id uuid;

ALTER TABLE member_value ADD CONSTRAINT member_value_field_not_null NOT NULL field;

ALTER TABLE member_value ADD CONSTRAINT member_value_grant_id_fkey FOREIGN KEY (grant_id) REFERENCES access_grant(id) ON DELETE CASCADE;

ALTER TABLE member_value ADD CONSTRAINT member_value_member_id_not_null NOT NULL member_id;

ALTER TABLE member_value ADD CONSTRAINT member_value_pkey PRIMARY KEY (project_id, member_id, step, field);

ALTER TABLE member_value ADD CONSTRAINT member_value_project_id_not_null NOT NULL project_id;

ALTER TABLE member_value ADD CONSTRAINT member_value_set_at_not_null NOT NULL set_at;

ALTER TABLE member_value ADD CONSTRAINT member_value_step_not_null NOT NULL step;

ALTER TABLE member_value ADD CONSTRAINT member_value_tenant_id_not_null NOT NULL tenant_id;

ALTER TABLE member_value ADD CONSTRAINT member_value_value_not_null NOT NULL value;

CREATE INDEX member_value_grant ON member_value USING btree (grant_id) WHERE (grant_id IS NOT NULL);
