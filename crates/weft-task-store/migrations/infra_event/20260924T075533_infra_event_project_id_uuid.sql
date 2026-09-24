-- infra_event.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE infra_event ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

CREATE OR REPLACE FUNCTION infra_event_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_infra_event', NEW.id::text);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER infra_event_notify_on_insert AFTER INSERT ON infra_event FOR EACH ROW EXECUTE FUNCTION infra_event_notify();
