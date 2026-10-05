CREATE OR REPLACE FUNCTION access_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF TG_OP <> 'INSERT' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF OLD.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || OLD.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', OLD.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', OLD.project_id::text);
                    END IF;
                END IF;
                IF TG_OP <> 'DELETE' THEN
                    -- Nested: a field only access_grant has is read only there.
                    IF TG_TABLE_NAME = 'access_grant' THEN
                        IF NEW.instance_id IS NULL THEN
                            PERFORM pg_notify('weft_access', 'tenant:' || NEW.tenant_id);
                        ELSE
                            PERFORM pg_notify('weft_access', NEW.project_id::text);
                        END IF;
                    ELSE
                        PERFORM pg_notify('weft_access', NEW.project_id::text);
                    END IF;
                END IF;
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER access_grant_notify AFTER INSERT OR DELETE OR UPDATE ON access_grant FOR EACH ROW EXECUTE FUNCTION access_notify();

CREATE TRIGGER install_pick_notify AFTER INSERT OR DELETE OR UPDATE ON install_pick FOR EACH ROW EXECUTE FUNCTION access_notify();

CREATE TRIGGER instance_value_notify AFTER INSERT OR DELETE OR UPDATE ON instance_value FOR EACH ROW EXECUTE FUNCTION access_notify();
