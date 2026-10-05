CREATE OR REPLACE FUNCTION project_declared_infra_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_infra_status', NEW.id::text);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER project_declared_infra_on_change AFTER UPDATE OF project_json ON project FOR EACH ROW WHEN ((new.project_json IS DISTINCT FROM old.project_json)) EXECUTE FUNCTION project_declared_infra_notify();
