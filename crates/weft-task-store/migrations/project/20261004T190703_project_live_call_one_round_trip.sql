CREATE OR REPLACE FUNCTION project_worker_settings_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_worker_settings', OLD.id::text);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER project_routes_on_row AFTER INSERT OR DELETE ON project FOR EACH ROW EXECUTE FUNCTION routes_notify_tenant();

CREATE TRIGGER project_worker_settings_on_change AFTER UPDATE OF worker_settings_json ON project FOR EACH ROW WHEN ((new.worker_settings_json IS DISTINCT FROM old.worker_settings_json)) EXECUTE FUNCTION project_worker_settings_notify();

CREATE TRIGGER project_worker_settings_on_delete AFTER DELETE ON project FOR EACH ROW EXECUTE FUNCTION project_worker_settings_notify();
