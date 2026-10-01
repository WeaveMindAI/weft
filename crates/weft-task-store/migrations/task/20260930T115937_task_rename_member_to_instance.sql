ALTER TABLE task RENAME COLUMN target_instance TO target_replica;

CREATE OR REPLACE FUNCTION weft_bind_execution_id_owner()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                IF NEW.execution_id IS NOT NULL AND NEW.claimed_by IS NOT NULL THEN
                    UPDATE execution
                    SET owner_replica = NEW.claimed_by
                    WHERE execution_id = NEW.execution_id;
                END IF;
                RETURN NEW;
            END;
            $function$
;
