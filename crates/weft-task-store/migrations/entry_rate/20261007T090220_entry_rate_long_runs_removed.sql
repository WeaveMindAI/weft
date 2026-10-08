CREATE OR REPLACE FUNCTION weft_admit(p jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_window BIGINT := (p->>'window_start')::bigint;
                v_later INTEGER := (p->>'retry_after_secs')::integer;
                v_count JSONB;
                v_refused JSONB;
            BEGIN
                FOR v_count IN SELECT c FROM jsonb_array_elements(p->'counts') WITH ORDINALITY AS e(c, n) ORDER BY n LOOP
                    IF weft_rate_hit(v_count->>'key', v_window) > (v_count->>'limit')::integer THEN
                        v_refused := jsonb_build_object('reason', v_count->>'reason', 'retry_after_secs', v_later);
                        EXIT;
                    END IF;
                END LOOP;
                IF v_refused IS NOT NULL THEN
                    PERFORM weft_rate_hit((p->>'refusals_key') || (v_refused->>'reason'), v_window);
                END IF;
                RETURN v_refused;
            END;
            $function$
;

-- Throws away every row in entry_slot.
DROP TABLE entry_slot;

DROP FUNCTION weft_slot_stopped_counting(p_execution_id text, p_unborn_until bigint, p_now bigint) CASCADE;
