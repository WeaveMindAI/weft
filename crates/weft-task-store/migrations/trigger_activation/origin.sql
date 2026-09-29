CREATE TABLE IF NOT EXISTS trigger_activation (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            -- The trigger, spelled the way the program reads it (`door`,
            -- `one.door` inside an included file): the same spelling the
            -- signal rows carry.
            trigger TEXT NOT NULL,
            -- Whose activation: NULL for the program's shared trigger,
            -- else the member whose copy of a per-member trigger it is.
            member_id TEXT,
            -- registered | activating | active | deactivating | inactive
            status TEXT NOT NULL,
            accepting_fires BOOLEAN NOT NULL,
            fires_visible_to_consumers BOOLEAN NOT NULL,
            fires_deadline_unix BIGINT,
            drain_deadline_unix BIGINT,
            deactivated_by_health BOOLEAN NOT NULL DEFAULT FALSE,
            -- The trigger-setup run of the activation in flight; one run
            -- sets up every activation a verb names, so rows share it.
            activating_execution_id UUID,
            heartbeat_unix BIGINT NOT NULL DEFAULT 0,
            -- What the listeners fire, recorded when setup finished.
            activation_program JSONB,
            activation_version TEXT,
            updated_at BIGINT NOT NULL
        );
CREATE UNIQUE INDEX IF NOT EXISTS trigger_activation_key
             ON trigger_activation (project_id, trigger, member_id) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS trigger_activation_execution_id
             ON trigger_activation (activating_execution_id) WHERE activating_execution_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS trigger_activation_transitional
             ON trigger_activation (status) WHERE status IN ('activating', 'deactivating');
