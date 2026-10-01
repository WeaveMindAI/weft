ALTER TABLE runtime_file ADD COLUMN last_replacement text;

CREATE UNIQUE INDEX idx_runtime_file_last_replacement ON runtime_file USING btree (last_replacement) WHERE (last_replacement IS NOT NULL);
