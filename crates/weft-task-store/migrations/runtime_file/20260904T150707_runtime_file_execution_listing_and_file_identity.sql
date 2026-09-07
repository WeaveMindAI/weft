ALTER TABLE runtime_file ADD COLUMN identity text;

CREATE UNIQUE INDEX idx_runtime_file_identity ON runtime_file USING btree (regexp_replace(key, '/[^/]+$'::text, ''::text), identity) WHERE (identity IS NOT NULL);
