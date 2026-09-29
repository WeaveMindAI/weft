CREATE TABLE IF NOT EXISTS infra_node (
            project_id          UUID NOT NULL,
            node_id             TEXT NOT NULL,
            instance_id         TEXT NOT NULL DEFAULT '',
            status              TEXT NOT NULL,
            failure_stage       TEXT,
            failure_message     TEXT,
            applied_spec_hash   TEXT,
            applied_at_unix     BIGINT,
            endpoints_json      JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Endpoint name to its front-door path, for Public
            -- endpoints only. Stamped at apply.
            public_paths_json   JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Disk names to keep on terminate. JSON array; empty means
            -- "delete every disk the copy owns".
            preserve_pvcs_json  JSONB NOT NULL DEFAULT '[]'::jsonb,
            -- Per-unit runtime (status + resolved health windows +
            -- stop_behavior) keyed by unit name. The `status` column
            -- is a rollup over these. Stamped at apply.
            units_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Whose copy: NULL for the program's shared one, else the
            -- member whose copy of a `@per_member` node this is.
            member_id           TEXT,
            -- Endpoint name to the `host:port` a SameNetwork endpoint
            -- answers at on the install's network. Stamped at apply.
            doors_json          JSONB NOT NULL DEFAULT '{}'::jsonb,
            -- Endpoint name to where weft's own roles reach it (the
            -- workers' address unless they sit on a network weft's
            -- roles are not on). Stamped at apply.
            install_endpoints_json JSONB NOT NULL DEFAULT '{}'::jsonb
        );
CREATE UNIQUE INDEX IF NOT EXISTS idx_infra_node_copy
             ON infra_node(project_id, node_id, member_id) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS idx_infra_node_project   ON infra_node(project_id);
