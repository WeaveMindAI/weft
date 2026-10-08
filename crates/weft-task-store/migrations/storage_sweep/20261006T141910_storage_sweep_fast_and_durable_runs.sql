CREATE OR REPLACE FUNCTION storage_sweep_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM weft_announce('weft_storage_sweep', '');
                RETURN NULL;
            END;
            $function$
;
