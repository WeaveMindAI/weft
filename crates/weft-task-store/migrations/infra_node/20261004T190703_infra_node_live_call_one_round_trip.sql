CREATE OR REPLACE FUNCTION infra_node_status_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    PERFORM pg_notify('weft_infra_status', OLD.project_id::text);
                ELSE
                    PERFORM pg_notify('weft_infra_status', NEW.project_id::text);
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER infra_node_status_on_change AFTER UPDATE OF status ON infra_node FOR EACH ROW WHEN ((new.status IS DISTINCT FROM old.status)) EXECUTE FUNCTION infra_node_status_notify();

CREATE TRIGGER infra_node_status_on_row AFTER INSERT OR DELETE ON infra_node FOR EACH ROW EXECUTE FUNCTION infra_node_status_notify();
