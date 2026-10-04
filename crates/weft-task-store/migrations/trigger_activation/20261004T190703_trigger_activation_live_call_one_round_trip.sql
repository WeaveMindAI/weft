CREATE OR REPLACE FUNCTION trigger_activation_routes_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            DECLARE
                tenant TEXT;
            BEGIN
                SELECT p.tenant_id INTO tenant FROM project p
                    WHERE p.id = CASE WHEN TG_OP = 'DELETE' THEN OLD.project_id ELSE NEW.project_id END;
                -- A project already gone told its tenant itself.
                IF tenant IS NOT NULL THEN
                    PERFORM pg_notify('weft_routes', tenant);
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER trigger_activation_routes_on_row AFTER INSERT OR DELETE ON trigger_activation FOR EACH ROW EXECUTE FUNCTION trigger_activation_routes_notify();

CREATE TRIGGER trigger_activation_routes_on_status AFTER UPDATE OF status ON trigger_activation FOR EACH ROW WHEN ((new.status IS DISTINCT FROM old.status)) EXECUTE FUNCTION trigger_activation_routes_notify();
