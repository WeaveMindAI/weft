CREATE OR REPLACE FUNCTION storage_sweep_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_notify('weft_storage_sweep', NEW.color);
                RETURN NULL;
            END;
            $function$
;

CREATE TRIGGER storage_sweep_notify_on_insert AFTER INSERT ON storage_sweep FOR EACH ROW EXECUTE FUNCTION storage_sweep_notify();
