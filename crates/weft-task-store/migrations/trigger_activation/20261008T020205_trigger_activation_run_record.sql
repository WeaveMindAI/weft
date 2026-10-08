CREATE OR REPLACE FUNCTION trigger_activation_triggers_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_triggers', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_triggers', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER trigger_activation_triggers_on_row AFTER INSERT OR DELETE ON trigger_activation FOR EACH ROW EXECUTE FUNCTION trigger_activation_triggers_notify();

CREATE TRIGGER trigger_activation_triggers_on_status AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.accepting_fires IS DISTINCT FROM old.accepting_fires) OR (new.fires_deadline_unix IS DISTINCT FROM old.fires_deadline_unix))) EXECUTE FUNCTION trigger_activation_triggers_notify();

DROP TRIGGER trigger_activation_old_health_flag ON trigger_activation;

-- Throws away what is in trigger_activation.deactivated_by_health. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE trigger_activation DROP COLUMN deactivated_by_health;

DROP FUNCTION trigger_activation_old_health_flag() CASCADE;
