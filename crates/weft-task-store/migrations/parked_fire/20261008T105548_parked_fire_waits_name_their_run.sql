CREATE OR REPLACE FUNCTION parked_fire_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM weft_announce('weft_parked_fire', COALESCE(
                    (SELECT s.project_id FROM signal s WHERE s.token = NEW.token),
                    (SELECT r.project_id FROM run r WHERE r.execution_id = NEW.execution_id))::text);
                RETURN NULL;
            END;
            $function$
;
