-- task.payload was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE task ALTER COLUMN payload TYPE text USING payload::text;

-- task.progress was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE task ALTER COLUMN progress TYPE text USING progress::text;

-- task.result was jsonb. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE task ALTER COLUMN result TYPE text USING result::text;

-- weft_enqueue_dedup takes other arguments now: a call with the old ones fails loudly (no such function).
DROP FUNCTION weft_enqueue_dedup(uuid, text, uuid, text, uuid, text, jsonb, bigint);
CREATE OR REPLACE FUNCTION weft_enqueue_dedup(p_id uuid, p_kind text, p_project uuid, p_dedup text, p_run uuid, p_tenant text, p_payload text, p_now bigint, OUT task_id uuid, OUT inserted boolean)
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
                INSERT INTO task (id, kind, status, project_id, dedup_key, run_id, tenant_id, payload, attempts, created_at_unix)
                    VALUES (p_id, p_kind, 'pending', p_project, p_dedup, p_run, p_tenant, p_payload, 0, p_now);
                task_id := p_id;
                inserted := TRUE;
            END;
            $function$
;
