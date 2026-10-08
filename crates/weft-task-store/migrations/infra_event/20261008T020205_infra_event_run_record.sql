CREATE OR REPLACE FUNCTION infra_event_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_infra_event', NEW.project_id::text || ' ' || NEW.id::text);
                RETURN NULL;
            END;
            $function$
;

-- Throws away what is in infra_event.writer_xid. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE infra_event DROP COLUMN writer_xid;

DROP INDEX IF EXISTS idx_infra_event_settled;
