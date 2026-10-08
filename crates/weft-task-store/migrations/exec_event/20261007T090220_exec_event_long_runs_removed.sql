CREATE OR REPLACE FUNCTION weft_start_execution(p jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
            DECLARE
                v_started JSONB := p->'started';
                v_task JSONB := p->'task';
                v_execution_id TEXT := v_started->'seed'->>'execution_id';
                v_project UUID := (v_started->'seed'->>'project_id')::uuid;
                v_journaled BOOLEAN := (v_started->>'journaled')::boolean;
                v_inserted BOOLEAN;
            BEGIN
                PERFORM weft_lock_execution(v_execution_id);
                IF (v_journaled AND EXISTS (SELECT 1 FROM exec_event
                        WHERE execution_id = v_execution_id AND 'execution_started' = ANY(kinds)))
                   OR (NOT v_journaled AND EXISTS (SELECT 1 FROM execution WHERE execution_id = v_execution_id)) THEN
                    RETURN jsonb_build_object('outcome', 'already_started');
                END IF;
                SELECT d.inserted INTO v_inserted FROM weft_enqueue_dedup(
                    (v_task->>'id')::uuid, v_task->>'kind', v_task->>'target', (v_task->>'project_id')::uuid,
                    v_task->>'dedup_key', v_task->>'execution_id', v_task->>'tenant_id', v_task->>'target_replica',
                    v_task->>'binary_hash', v_task->'payload', (v_task->>'created_at')::bigint) d;
                IF NOT v_inserted THEN
                    RETURN jsonb_build_object('outcome', 'already_started');
                END IF;
                PERFORM weft_execution_started(v_started, p->'kicks');
                -- A trigger setup is born only while the activation that asked
                -- for it still owns its rows (a cancel between the claim and
                -- here wins), and is recorded as in flight.
                IF jsonb_typeof(p->'trigger_setup') = 'object' THEN
                    IF (p->'trigger_setup'->>'for_activation')::boolean THEN
                        PERFORM 1 FROM trigger_activation
                            WHERE project_id = v_project
                              AND activating_execution_id = v_execution_id::uuid
                              AND status = 'activating'
                            FOR UPDATE;
                        IF NOT FOUND THEN
                            RAISE EXCEPTION 'activation % ended before trigger setup could start', v_execution_id;
                        END IF;
                    END IF;
                    INSERT INTO trigger_setup (project_id, execution_id) VALUES (v_project, v_execution_id);
                END IF;
                RETURN jsonb_build_object('outcome', 'started');
            END;
            $function$
;
