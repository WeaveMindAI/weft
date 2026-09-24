-- infra_lifecycle_command.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_lifecycle_command ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

CREATE OR REPLACE FUNCTION infra_command_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    PERFORM pg_notify('weft_infra_command', 'issued:' || NEW.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_command', 'done:' || NEW.id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER infra_command_notify_on_done AFTER UPDATE OF completed_at_unix ON infra_lifecycle_command FOR EACH ROW WHEN (((new.completed_at_unix IS NOT NULL) AND (old.completed_at_unix IS NULL))) EXECUTE FUNCTION infra_command_notify();

CREATE TRIGGER infra_command_notify_on_issue AFTER INSERT ON infra_lifecycle_command FOR EACH ROW EXECUTE FUNCTION infra_command_notify();
