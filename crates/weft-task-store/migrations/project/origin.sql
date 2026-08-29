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
                accepting_fires BOOLEAN NOT NULL DEFAULT TRUE,
                fires_visible_to_consumers BOOLEAN NOT NULL DEFAULT TRUE,
                fires_deadline_unix BIGINT,
                -- True iff the CURRENT deactivation was performed by the
                -- health loop (autonomous park), not the user. Gates the
                -- health auto-recover reactivate so it never overrides a
                -- user-initiated stop/deactivate. Cleared by every
                -- non-health lifecycle write.
                deactivated_by_health BOOLEAN NOT NULL DEFAULT FALSE,
                tenant_id TEXT NOT NULL DEFAULT 'local',
                -- Whether this project DECLARES infrastructure (any node
                -- with requires_infra). Derived from the definition and
                -- refreshed on every register/sync, so it tracks edits
                -- that add or remove infra. Decides WORKER placement: an
                -- infra project's worker runs in the project's own k8s
                -- namespace (next to its infra pods), a no-infra
                -- project's worker runs in the shared worker namespace.
                -- The worker namespace is computed from this on demand
                -- (project_namespace::worker_namespace), never stored, so
                -- there is no stale worker-namespace value to reconcile.
                -- Set true the instant infra is declared, which is BEFORE
                -- the per-project namespace below is provisioned, so it
                -- cannot be replaced by `project_namespace <> ''`.
                has_infra BOOLEAN NOT NULL DEFAULT FALSE,
                -- The project's OWN k8s namespace
                -- (wft-project-<tenant>--<project>), where its INFRA pods
                -- and its worker live. Distinct concept from has_infra:
                -- this is the namespace string the supervisor runs
                -- kubectl against, EMPTY until the namespace is actually
                -- provisioned (first infra apply) and re-emptied when
                -- infra is torn down. The broker's supervisor-claim
                -- filters `project_namespace <> ''` to manage only
                -- projects whose namespace exists. A no-infra project
                -- keeps this empty forever (its worker lives in the
                -- shared namespace, which is not project-owned).
                project_namespace TEXT NOT NULL DEFAULT '',
                -- Per-(project, node) image hash maps for Image::Local
                -- references in InfraSpecs. CLI ships these in /sync;
                -- supervisor reads them.
                -- Shape: { "<node_id>": { "<image_name>": "<tag>" } }
                infra_image_tags_json JSONB NOT NULL DEFAULT '{}'::jsonb,
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
                -- 'cancelling_build'): the pod driving the transition
                -- bumps this on an interval; the stuck-transition
                -- reaper repairs rows whose heartbeat went stale
                -- (the driver died mid-transition). Per-project and
                -- status-guarded: this replaces the old boot-time
                -- blind bulk downgrade, which wiped live status for
                -- every tenant's projects on any Pod restart.
                transition_heartbeat_unix BIGINT NOT NULL DEFAULT 0
            );
CREATE INDEX IF NOT EXISTS idx_project_tenant ON project(tenant_id);
CREATE TABLE IF NOT EXISTS project_definition (
            project_id UUID NOT NULL,
            definition_hash TEXT NOT NULL,
            project_json TEXT NOT NULL,
            recorded_at_unix BIGINT NOT NULL,
            PRIMARY KEY (project_id, definition_hash),
            FOREIGN KEY (project_id) REFERENCES project(id) ON DELETE CASCADE
        );
