CREATE OR REPLACE FUNCTION weft_admit(p jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_window BIGINT := (p->>'window_start')::bigint;
                v_later INTEGER := (p->>'retry_after_secs')::integer;
                v_now BIGINT := (p->>'now')::bigint;
                v_slot JSONB := p->'slot';
                v_count JSONB;
                v_refused JSONB;
                v_room BOOLEAN;
            BEGIN
                FOR v_count IN SELECT c FROM jsonb_array_elements(p->'counts') WITH ORDINALITY AS e(c, n) ORDER BY n LOOP
                    IF weft_rate_hit(v_count->>'key', v_window) > (v_count->>'limit')::integer THEN
                        v_refused := jsonb_build_object('reason', v_count->>'reason', 'retry_after_secs', v_later);
                        EXIT;
                    END IF;
                END LOOP;
                IF v_refused IS NULL AND jsonb_typeof(v_slot) = 'object' THEN
                    IF v_slot->>'execution_id' IS NULL THEN
                        SELECT COUNT(*) < (v_slot->>'max')::bigint INTO v_room FROM entry_slot s
                            WHERE s.signal_token = v_slot->>'token'
                              AND NOT weft_slot_stopped_counting(s.execution_id, s.unborn_until, v_now);
                    ELSE
                        PERFORM pg_advisory_xact_lock(hashtextextended('entry_slot:' || (v_slot->>'token'), 0));
                        SELECT EXISTS (SELECT 1 FROM entry_slot s WHERE s.execution_id = v_slot->>'execution_id'
                                       AND NOT weft_slot_stopped_counting(s.execution_id, s.unborn_until, v_now))
                            OR (SELECT COUNT(*) FROM entry_slot s
                                WHERE s.signal_token = v_slot->>'token' AND s.execution_id <> v_slot->>'execution_id'
                                  AND NOT weft_slot_stopped_counting(s.execution_id, s.unborn_until, v_now))
                               < (v_slot->>'max')::bigint
                            INTO v_room;
                        IF v_room THEN
                            INSERT INTO entry_slot (execution_id, signal_token, unborn_until)
                                VALUES (v_slot->>'execution_id', v_slot->>'token', (v_slot->>'unborn_until')::bigint)
                                ON CONFLICT (execution_id) DO UPDATE
                                    SET unborn_until = GREATEST(entry_slot.unborn_until, EXCLUDED.unborn_until);
                        END IF;
                    END IF;
                    IF NOT v_room THEN
                        v_refused := jsonb_build_object('reason', 'at_once', 'retry_after_secs', 5);
                    END IF;
                END IF;
                IF v_refused IS NOT NULL THEN
                    PERFORM weft_rate_hit((p->>'refusals_key') || (v_refused->>'reason'), v_window);
                END IF;
                RETURN v_refused;
            END;
            $function$
;
