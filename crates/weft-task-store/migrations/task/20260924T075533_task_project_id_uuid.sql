-- task.project_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE task ALTER COLUMN project_id TYPE uuid USING project_id::uuid;

CREATE OR REPLACE FUNCTION task_ready_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_task_ready',
                    CASE WHEN NEW.target = 'dispatcher' THEN 'dispatcher'
                         ELSE 'worker:' || COALESCE(NEW.project_id::text, '') END);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER task_ready_on_insert AFTER INSERT ON task FOR EACH ROW WHEN ((new.status = 'pending'::text)) EXECUTE FUNCTION task_ready_notify();

CREATE TRIGGER task_ready_on_pending AFTER UPDATE OF status ON task FOR EACH ROW WHEN (((new.status = 'pending'::text) AND (old.status IS DISTINCT FROM 'pending'::text))) EXECUTE FUNCTION task_ready_notify();
