ALTER TABLE trigger_activation ADD COLUMN went_down_at_unix bigint;

ALTER TABLE trigger_activation ADD COLUMN went_down_with text;

CREATE OR REPLACE FUNCTION trigger_activation_old_health_flag()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF NEW.status = 'activating' THEN
                    NEW.went_down_with := NULL;
                    NEW.went_down_at_unix := NULL;
                ELSIF NEW.deactivated_by_health AND NEW.went_down_with IS NULL THEN
                    NEW.went_down_with := 'health';
                    NEW.went_down_at_unix := EXTRACT(EPOCH FROM NOW())::BIGINT;
                ELSIF NOT NEW.deactivated_by_health AND NEW.went_down_with = 'health' THEN
                    NEW.went_down_with := NULL;
                    NEW.went_down_at_unix := NULL;
                END IF;
                RETURN NEW;
            END;
            $function$
;

DROP TRIGGER trigger_activation_held_on_status ON trigger_activation;
CREATE TRIGGER trigger_activation_held_on_status AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.accepting_fires IS DISTINCT FROM old.accepting_fires) OR (new.fires_deadline_unix IS DISTINCT FROM old.fires_deadline_unix))) EXECUTE FUNCTION signal_held_notify();

CREATE TRIGGER trigger_activation_old_health_flag BEFORE INSERT OR UPDATE ON trigger_activation FOR EACH ROW EXECUTE FUNCTION trigger_activation_old_health_flag();

DROP TRIGGER trigger_activation_routes_on_status ON trigger_activation;
CREATE TRIGGER trigger_activation_routes_on_status AFTER UPDATE OF status, accepting_fires, fires_deadline_unix ON trigger_activation FOR EACH ROW WHEN (((new.status IS DISTINCT FROM old.status) OR (new.accepting_fires IS DISTINCT FROM old.accepting_fires) OR (new.fires_deadline_unix IS DISTINCT FROM old.fires_deadline_unix))) EXECUTE FUNCTION trigger_activation_routes_notify();
