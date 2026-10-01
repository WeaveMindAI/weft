CREATE TABLE asset_reference (
    key text NOT NULL,
    project_id text NOT NULL,
    tenant_id text NOT NULL
);

ALTER TABLE asset_reference ADD CONSTRAINT asset_reference_key_not_null NOT NULL key;

ALTER TABLE asset_reference ADD CONSTRAINT asset_reference_pkey PRIMARY KEY (tenant_id, project_id, key);

ALTER TABLE asset_reference ADD CONSTRAINT asset_reference_project_id_not_null NOT NULL project_id;

ALTER TABLE asset_reference ADD CONSTRAINT asset_reference_tenant_id_not_null NOT NULL tenant_id;

CREATE INDEX idx_asset_reference_key ON asset_reference USING btree (key);
