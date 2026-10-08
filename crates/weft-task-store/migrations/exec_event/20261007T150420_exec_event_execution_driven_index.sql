CREATE INDEX idx_execution_driven ON execution USING btree (started_at_unix) WHERE ((ended_at_unix IS NULL) AND (owner_replica IS NOT NULL));
