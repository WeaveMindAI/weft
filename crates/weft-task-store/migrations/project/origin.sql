CREATE TABLE IF NOT EXISTS project (
                id UUID PRIMARY KEY,
                name TEXT NOT NULL,
                -- Free-text project description (metadata only; never affects
                -- the graph, build, or runtime). Set at create time on the
                -- website; empty string when unset (NOT NULL keeps reads simple).
                description TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                project_json TEXT NOT NULL,
                updated_at BIGINT NOT NULL,
                running_binary_hash TEXT,
                running_definition_hash TEXT,
                running_infra_hash TEXT,
                running_source JSONB,
                accepting_fires BOOLEAN NOT NULL DEFAULT TRUE,
                fires_visible_to_consumers BOOLEAN NOT NULL DEFAULT TRUE,
                fires_deadline_unix BIGINT,
                -- True iff the CURRENT deactivation was performed by the
                -- health loop (autonomous park), not the user. Gates the
                -- health auto-recover reactivate so it never overrides a
                -- user-initiated stop/deactivate. Cleared by every
                -- non-health lifecycle write.
                deactivated_by_health BOOLEAN NOT NULL DEFAULT FALSE,
                -- The TriggerSetup execution the CURRENT activation started,
                -- recorded before the run starts; NULL outside Activating.
                -- Cancel-activate and the reaper cancel exactly this run
                -- with the true cause, and treat any other non-terminal
                -- setup run as a leftover of an older, dead activation.
                activating_ts_execution_id UUID,
                tenant_id TEXT NOT NULL,
                -- Whether this project DECLARES infrastructure (any node
                -- with requires_infra). Derived from the definition and
                -- refreshed on every register/sync, so it tracks edits
                -- that add or remove infra.
                has_infra BOOLEAN NOT NULL DEFAULT FALSE,
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }
                infra_image_tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- The project's own worker levers, each one it sets
                -- replacing the install's (`WorkerOverrides`); empty
                -- runs on the install's.
                worker_settings_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                -- Per-project health protocols overriding the weft
                -- default. NULL = use default. Schema per
                -- weft_infra_supervisor::protocol::HealthProtocols.
                health_protocols_json JSONB,
                -- Verb-transition marker, orthogonal to `status` (the
                -- BUILD axis): 'none' | 'building' | 'cancelling_build'.
                -- Written only by its own single-flight CAS methods
                -- (try_begin_building / request_cancel_build /
                -- finish_building), never by lifecycle writes, so a
                -- deactivate can't stomp an in-flight build marker.
                transition TEXT NOT NULL DEFAULT 'none',
                -- While status='deactivating' with runningPolicy=wait:
                -- the unix second past which the drain gives up (the
                -- reaper cancels the remaining executions and the
                -- drain-watcher lands the row). NULL elsewhere.
                drain_deadline_unix BIGINT,
                -- Heartbeat for driver-backed transitional states
                -- (status='activating', transition='building'/
                -- 'cancelling_build'): the instance driving the transition
                -- bumps this on an interval; the stuck-transition
                -- reaper repairs rows whose heartbeat went stale
                -- (the driver died mid-transition). Per-project and
                -- status-guarded: this replaces the old boot-time
                -- blind bulk downgrade, which wiped live status for
                -- every tenant's projects on any Instance restart.
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0,
                -- The version tree's HEAD (`crate::versions`): the version
                -- the next checkpoint or run parents on, the run the next
                -- `--seed` inherits from (NULL when head is a bare
                -- version), and the version the triggers were activated
                -- on (NULL while inactive). Moved by checkpoint, run,
                -- branch and activate; nothing lives on disk.
                head_version TEXT,
                head_run UUID,
                activation_version TEXT,
                activation_program JSONB
            );
CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id);
CREATE TABLE IF NOT EXISTS project_code (
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            binary_hash TEXT NOT NULL,
            implementations JSONB NOT NULL,
            PRIMARY KEY (project_id, binary_hash)
        );
CREATE TABLE IF NOT EXISTS project_definition (
            project_id UUID NOT NULL,
            definition_hash TEXT NOT NULL,
            project_json TEXT NOT NULL,
            recorded_at_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, definition_hash)
            -- Deliberately NO foreign key to `project`. An execution's
            -- journal outlives the project it ran (that is the point of
            -- a journal), and a run without the program it ran against
            -- cannot be read back: the rows are there and every input
            -- and output is underivable, so the graph paints an empty
            -- run and the reader goes hunting for a bug in the viewer.
            -- The history is therefore kept as long as anything points
            -- at it, and `retire_unused_definitions` drops the versions
            -- no surviving run needs once the project itself is gone.
        );
