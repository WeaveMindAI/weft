CREATE OR REPLACE FUNCTION exec_event_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM weft_announce('weft_exec_event', a.execution_id)
                    FROM (SELECT DISTINCT execution_id FROM added) a;
                RETURN NULL;
            END;
            $function$
;

DROP TRIGGER exec_event_notify_on_insert ON exec_event;
CREATE TRIGGER exec_event_notify_on_insert AFTER INSERT ON exec_event REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION exec_event_notify();
