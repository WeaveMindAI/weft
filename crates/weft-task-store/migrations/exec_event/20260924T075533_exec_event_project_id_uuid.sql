-- execution_color.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE execution_color ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

-- signal.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE signal ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

-- trigger_bake.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE trigger_bake ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

-- trigger_setup.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE trigger_setup ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

CREATE OR REPLACE FUNCTION exec_event_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_exec_event', NEW.color);
                RETURN NULL;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION signal_parked_fire_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_parked_fire', NEW.project_id::text);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER exec_event_notify_on_insert AFTER INSERT ON exec_event FOR EACH ROW EXECUTE FUNCTION exec_event_notify();

CREATE TRIGGER signal_parked_fire_notify_on_grow AFTER UPDATE OF parked_fires ON signal FOR EACH ROW WHEN ((jsonb_array_length(new.parked_fires) > jsonb_array_length(old.parked_fires))) EXECUTE FUNCTION signal_parked_fire_notify();
