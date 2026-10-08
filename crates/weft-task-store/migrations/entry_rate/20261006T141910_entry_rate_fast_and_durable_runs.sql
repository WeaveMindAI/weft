CREATE OR REPLACE FUNCTION weft_slot_stopped_counting(p_execution_id text, p_unborn_until bigint, p_now bigint)
 RETURNS boolean
 LANGUAGE sql
 STABLE
AS $function$
            SELECT (p_unborn_until < p_now
                    AND NOT EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = p_execution_id))
                OR EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = p_execution_id
                           AND ec.ended_at_unix IS NOT NULL)
            $function$
;
