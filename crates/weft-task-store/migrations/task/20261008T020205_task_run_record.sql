-- task.execution_id was text. The cast below is a plain one; if the rows already there
-- need converting differently, change the USING.
ALTER TABLE task ALTER COLUMN execution_id TYPE uuid USING execution_id::uuid;

CREATE OR REPLACE FUNCTION task_ready_notify()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
            BEGIN
                PERFORM weft_announce('weft_task_ready', '');
                RETURN NULL;
            END;
            $function$
;

CREATE OR REPLACE FUNCTION weft_enqueue_dedup(p_id uuid, p_kind text, p_project uuid, p_dedup text, p_execution uuid, p_tenant text, p_payload jsonb, p_now bigint, OUT task_id uuid, OUT inserted boolean)
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
                INSERT INTO task (id, kind, status, project_id, dedup_key, execution_id, tenant_id, payload, attempts, created_at_unix)
                    VALUES (p_id, p_kind, 'pending', p_project, p_dedup, p_execution, p_tenant, p_payload, 0, p_now);
                task_id := p_id;
                inserted := TRUE;
            END;
            $function$
;

CREATE INDEX idx_task_pending ON task USING btree (created_at_unix) WHERE (status = 'pending'::text);

DROP TRIGGER task_claim_binds_execution_id_owner ON task;

-- Throws away what is in task.binary_hash. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE task DROP COLUMN binary_hash;

-- Throws away what is in task.delivered_until_unix. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE task DROP COLUMN delivered_until_unix;

-- Throws away what is in task.rerun_requested. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE task DROP COLUMN rerun_requested;

-- Throws away what is in task.target. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE task DROP COLUMN target;

-- Throws away what is in task.target_replica. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE task DROP COLUMN target_replica;

DROP INDEX IF EXISTS idx_task_deliverable;

DROP INDEX IF EXISTS idx_task_execution_id;

DROP INDEX IF EXISTS idx_task_pending_dispatcher;

DROP INDEX IF EXISTS idx_task_pending_worker;

DROP FUNCTION weft_bind_execution_id_owner() CASCADE;

DROP FUNCTION weft_enqueue_dedup(p_id uuid, p_kind text, p_target text, p_project uuid, p_dedup text, p_execution text, p_tenant text, p_target_replica text, p_binary_hash text, p_payload jsonb, p_now bigint, OUT task_id uuid, OUT inserted boolean) CASCADE;
