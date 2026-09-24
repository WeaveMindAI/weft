-- worker_pod.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE worker_pod ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

CREATE OR REPLACE FUNCTION worker_pod_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_worker_pod', NEW.project_id::text);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER worker_pod_notify_on_change AFTER UPDATE ON worker_pod FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.draining IS DISTINCT FROM old.draining) OR ((new.mem_pressure < (0.75)::double precision) IS DISTINCT FROM (old.mem_pressure < (0.75)::double precision)))) EXECUTE FUNCTION worker_pod_notify();

CREATE TRIGGER worker_pod_notify_on_insert AFTER INSERT ON worker_pod FOR EACH ROW EXECUTE FUNCTION worker_pod_notify();
