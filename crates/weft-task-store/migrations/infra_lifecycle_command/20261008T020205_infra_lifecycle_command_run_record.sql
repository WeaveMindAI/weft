ALTER TABLE infra_lifecycle_command ADD COLUMN asked_by uuid;

ALTER TABLE infra_lifecycle_command ADD COLUMN drain_by_unix bigint;

CREATE OR REPLACE FUNCTION infra_command_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'INSERT' OR (NEW.completed_at_unix IS NULL AND NEW.drain_by_unix IS NULL) THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

DROP INDEX idx_lifecycle_cmd_dispatcher_claim;
CREATE INDEX idx_lifecycle_cmd_dispatcher_claim ON infra_lifecycle_command USING btree (id) WHERE ((completed_at_unix IS NULL) AND (claimed_by_replica IS NULL) AND ((drain_by_unix IS NOT NULL) OR (verb = ANY (ARRAY['deactivate'::text, 'reactivate'::text, 'upgrade'::text]))));

CREATE TRIGGER infra_command_notify_on_drained AFTER UPDATE OF drain_by_unix ON infra_lifecycle_command FOR EACH ROW WHEN (((new.drain_by_unix IS NULL) AND (old.drain_by_unix IS NOT NULL))) EXECUTE FUNCTION infra_command_notify();

-- Throws away what is in infra_lifecycle_command.drain_timeout_secs. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE infra_lifecycle_command DROP COLUMN drain_timeout_secs;

-- Throws away what is in infra_lifecycle_command.running_policy. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE infra_lifecycle_command DROP COLUMN running_policy;
