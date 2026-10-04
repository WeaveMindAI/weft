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
                v_hits INTEGER;
                v_refused JSONB;
                v_room BOOLEAN;
            BEGIN
                IF jsonb_typeof(p->'blocked') = 'object' THEN
                    SELECT r.hits INTO v_hits FROM entry_rate r
                        WHERE r.key = p->'blocked'->>'key' AND r.window_start = v_window;
                    IF v_hits >= (p->'blocked'->>'limit')::integer THEN
                        RETURN jsonb_build_object('reason', 'invalid_tokens', 'retry_after_secs', v_later);
                    END IF;
                END IF;
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

CREATE OR REPLACE FUNCTION weft_rate_hit(p_key text, p_window_start bigint)
 RETURNS integer
 LANGUAGE sql
AS $function$
            INSERT INTO entry_rate (key, window_start, hits) VALUES (p_key, p_window_start, 1)
            ON CONFLICT (key, window_start) DO UPDATE SET hits = entry_rate.hits + 1
            RETURNING hits
            $function$
;

CREATE OR REPLACE FUNCTION weft_slot_stopped_counting(p_execution_id text, p_unborn_until bigint, p_now bigint)
 RETURNS boolean
 LANGUAGE sql
 STABLE
AS $function$
            SELECT (p_unborn_until < p_now
                    AND NOT EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = p_execution_id))
                OR EXISTS (SELECT 1 FROM execution ec WHERE ec.execution_id = p_execution_id
                           AND ec.ended_at_unix IS NOT NULL)
                OR EXISTS (SELECT 1 FROM exec_event e WHERE e.execution_id = p_execution_id
                           AND e.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled'))
            $function$
;
