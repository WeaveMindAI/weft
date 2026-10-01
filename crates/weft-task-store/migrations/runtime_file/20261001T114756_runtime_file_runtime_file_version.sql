ALTER TABLE runtime_file ADD COLUMN version bigint DEFAULT 1 NOT NULL;

ALTER TABLE runtime_file ADD CONSTRAINT runtime_file_version_not_null NOT NULL version;

CREATE UNIQUE INDEX idx_runtime_file_one_replacement ON runtime_file USING btree (replaces) WHERE (replaces IS NOT NULL);
