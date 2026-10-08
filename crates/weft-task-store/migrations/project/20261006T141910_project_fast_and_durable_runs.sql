ALTER TABLE project ADD COLUMN api_port integer;

CREATE UNIQUE INDEX idx_project_api_port ON project USING btree (api_port) WHERE (api_port IS NOT NULL);
