CREATE OR REPLACE FUNCTION task_ready_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF NEW.kind = 'cancel_execution' THEN
                    PERFORM weft_announce('weft_cancel', COALESCE(NEW.project_id::text, '') || ' ' || COALESCE(NEW.execution_id, ''));
                ELSE
                    PERFORM weft_announce('weft_task_ready',
                        CASE WHEN NEW.target = 'dispatcher' THEN 'dispatcher'
                             ELSE 'worker:' || COALESCE(NEW.project_id::text, '') END);
                END IF;
                RETURN NULL;
            END;
            $function$
;
