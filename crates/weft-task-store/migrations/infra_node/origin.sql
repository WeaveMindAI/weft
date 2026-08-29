CREATE TABLE IF NOT EXISTS infra_node (
            project_id          TEXT NOT NULL,
            node_id             TEXT NOT NULL,
            instance_id         TEXT NOT NULL DEFAULT '',
            namespace           TEXT NOT NULL,
            status              TEXT NOT NULL,
            failure_stage       TEXT,
            failure_message     TEXT,
            applied_spec_hash   TEXT,
            applied_at_unix     BIGINT,
            endpoints_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- PVC names to preserve on terminate. JSON array;
            -- empty means "delete all matching PVCs."
            preserve_pvcs_json  JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Per-unit runtime (status + resolved health windows +
            -- stop_behavior) keyed by unit name. The `status` column
            -- is a rollup over these. Stamped at apply.
            units_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (project_id, node_id)
        );
CREATE INDEX IF NOT EXISTS idx_infra_node_project   ON infra_node(project_id);
CREATE INDEX IF NOT EXISTS idx_infra_node_namespace ON infra_node(namespace);
