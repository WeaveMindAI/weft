CREATE TABLE IF NOT EXISTS task (
            id UUID PRIMARY KEY,
            kind TEXT NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            project_id TEXT,
            dedup_key TEXT,
            color TEXT,
            tenant_id TEXT,
            target_pod_name TEXT,
            binary_hash TEXT,
            payload JSONB NOT NULL,
            claimed_by TEXT,
            claimed_until_unix BIGINT,
            attempts INTEGER NOT NULL DEFAULT 0,
            result JSONB,
            error TEXT,
            created_at_unix BIGINT NOT NULL,
            completed_at_unix BIGINT
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
CREATE INDEX IF NOT EXISTS idx_task_color
            ON task(color)
            WHERE color IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_tenant
            ON task(tenant_id)
            WHERE tenant_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_project
            ON task(project_id)
            WHERE project_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_terminal_completed
            ON task(completed_at_unix)
            WHERE status IN ('complete', 'failed');
