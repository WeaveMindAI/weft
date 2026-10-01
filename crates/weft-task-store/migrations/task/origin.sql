CREATE TABLE IF NOT EXISTS task (
            id UUID PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            project_id UUID,
            dedup_key TEXT,
            execution_id TEXT,
            tenant_id TEXT NOT NULL,
            target_instance TEXT,
            binary_hash TEXT,
            payload JSONB NOT NULL,
            claimed_by TEXT,
            claimed_until_unix BIGINT,
            attempts INTEGER NOT NULL DEFAULT 0,
            result JSONB,
            error TEXT,
            created_at_unix BIGINT NOT NULL,
            completed_at_unix BIGINT,
            -- Asked for again while claimed (`enqueue_or_rearm`): the
            -- claimant may already be past the point where it would
            -- have seen why, so finishing puts the row back to pending
            -- instead of ending it.
            rerun_requested BOOLEAN NOT NULL DEFAULT FALSE,
            -- Until when a worker task counts as handed to a worker that
            -- has not claimed it yet (`take_deliveries`): no second
            -- delivery is made before then, so a worker still starting
            -- up is not handed the same execution again.
            delivered_until_unix BIGINT
        );
CREATE INDEX IF NOT EXISTS idx_task_pending_dispatcher
            ON task(created_at_unix)
            WHERE status = 'pending' AND target = 'dispatcher';
CREATE INDEX IF NOT EXISTS idx_task_pending_worker
            ON task(project_id, created_at_unix)
            WHERE status = 'pending' AND target = 'worker' AND project_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_claimed_expired
            ON task(claimed_until_unix)
            WHERE status = 'claimed';
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_dedup_live
            ON task(tenant_id, kind, dedup_key)
            WHERE dedup_key IS NOT NULL AND status IN ('pending', 'claimed');
CREATE INDEX IF NOT EXISTS idx_task_execution_id
            ON task(execution_id)
            WHERE execution_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_tenant ON task(tenant_id);
CREATE INDEX IF NOT EXISTS idx_task_project
            ON task(project_id)
            WHERE project_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_terminal_completed
            ON task(completed_at_unix)
            WHERE status IN ('complete', 'failed');
CREATE OR REPLACE FUNCTION task_ready_notify() RETURNS trigger AS $$
            BEGIN
                PERFORM pg_notify('weft_task_ready',
                    CASE WHEN NEW.target = 'dispatcher' THEN 'dispatcher'
                         ELSE 'worker:' || COALESCE(NEW.project_id::text, '') END);
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS task_ready_on_insert ON task;
CREATE TRIGGER task_ready_on_insert
            AFTER INSERT ON task
            FOR EACH ROW
            WHEN (NEW.status = 'pending')
            EXECUTE FUNCTION task_ready_notify();
DROP TRIGGER IF EXISTS task_ready_on_pending ON task;
CREATE TRIGGER task_ready_on_pending
            AFTER UPDATE OF status ON task
            FOR EACH ROW
            WHEN (NEW.status = 'pending' AND OLD.status IS DISTINCT FROM 'pending')
            EXECUTE FUNCTION task_ready_notify();
CREATE OR REPLACE FUNCTION weft_bind_execution_id_owner() RETURNS trigger AS $$
            BEGIN
                IF NEW.execution_id IS NOT NULL AND NEW.claimed_by IS NOT NULL THEN
                    UPDATE execution
                    SET owner_instance = NEW.claimed_by
                    WHERE execution_id = NEW.execution_id;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS task_claim_binds_execution_id_owner ON task;
CREATE TRIGGER task_claim_binds_execution_id_owner
            AFTER UPDATE OF claimed_by ON task
            FOR EACH ROW
            WHEN (NEW.status = 'claimed' AND NEW.claimed_by IS NOT NULL
                  AND NEW.claimed_by IS DISTINCT FROM OLD.claimed_by
                  AND NEW.kind IN ('execute', 'resume'))
            EXECUTE FUNCTION weft_bind_execution_id_owner();
