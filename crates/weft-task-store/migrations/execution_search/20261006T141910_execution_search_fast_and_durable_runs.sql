CREATE OR REPLACE FUNCTION weft_run_search_text(p_execution_id text, p_max integer, p_value_max integer)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
            DECLARE
                -- Collected, then joined once: growing one string would
                -- copy all of it at every value.
                v_parts TEXT[] := '{}';
                v_length INTEGER := 0;
                v_payload TEXT;
                v_json JSONB;
                v_value TEXT;
            BEGIN
                FOR v_payload IN SELECT events_json FROM exec_event WHERE execution_id = p_execution_id ORDER BY id LOOP
                    IF strpos(v_payload, '\u0000') > 0 THEN
                        BEGIN
                            v_json := replace(v_payload, '\u0000', '')::jsonb;
                        EXCEPTION WHEN untranslatable_character OR invalid_text_representation THEN
                            v_json := NULL;
                        END;
                        CONTINUE WHEN v_json IS NULL;
                    ELSE
                        v_json := v_payload::jsonb;
                    END IF;
                    -- One row's values at once; the cap is looked at row by
                    -- row, and the last row is cut to it.
                    SELECT string_agg(left(j #>> '{}', p_value_max), ' ') INTO v_value
                    FROM jsonb_path_query(v_json, 'strict $.** ? (@.type() == "string" || @.type() == "number")') j;
                    CONTINUE WHEN v_value IS NULL;
                    v_parts := array_append(v_parts, v_value);
                    v_length := v_length + length(v_value) + CASE WHEN v_length = 0 THEN 0 ELSE 1 END;
                    IF v_length >= p_max THEN
                        RETURN left(array_to_string(v_parts, ' '), p_max);
                    END IF;
                END LOOP;
                RETURN array_to_string(v_parts, ' ');
            END;
            $function$
;
