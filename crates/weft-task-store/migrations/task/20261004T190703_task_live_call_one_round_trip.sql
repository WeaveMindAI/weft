CREATE OR REPLACE FUNCTION weft_enqueue_dedup(p_id uuid, p_kind text, p_target text, p_project uuid, p_dedup text, p_execution text, p_tenant text, p_target_replica text, p_binary_hash text, p_payload jsonb, p_now bigint, OUT task_id uuid, OUT inserted boolean)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM weft_lock_dedup(p_tenant, p_kind, p_dedup);
                SELECT t.id INTO task_id FROM task t
                    WHERE t.tenant_id = p_tenant AND t.kind = p_kind AND t.dedup_key = p_dedup
                      AND t.status IN ('pending', 'claimed')
                    LIMIT 1;
                IF FOUND THEN
                    inserted := FALSE;
                    RETURN;
                END IF;
                INSERT INTO task (
                    id, kind, status, target, project_id, dedup_key, execution_id, tenant_id,
                    target_replica, binary_hash, payload, attempts, created_at_unix
                ) VALUES (p_id, p_kind, 'pending', p_target, p_project, p_dedup, p_execution, p_tenant,
                          p_target_replica, p_binary_hash, p_payload, 0, p_now);
                task_id := p_id;
                inserted := TRUE;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_lock_dedup(p_tenant text, p_kind text, p_dedup text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM pg_advisory_xact_lock(hashtextextended(p_tenant || '|' || p_kind || '|' || p_dedup, 0));
            END;
            $function$
;
