DROP INDEX idx_lifecycle_cmd_dispatcher_claim;
CREATE INDEX idx_lifecycle_cmd_dispatcher_claim ON infra_lifecycle_command USING btree (id) WHERE ((completed_at_unix IS NULL) AND (claimed_by_pod IS NULL) AND (verb = ANY (ARRAY['deactivate'::text, 'reactivate'::text, 'upgrade'::text])));
