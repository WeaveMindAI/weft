CREATE TABLE IF NOT EXISTS supervisor_pod (
            pod_name          TEXT PRIMARY KEY,
            admin_url         TEXT NOT NULL,
            namespace         TEXT NOT NULL,
            owner_pod_id      TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL,
            -- The supervisor's last self-reported memory pressure
            -- (usage/limit, [0,1]), written on each ownership tick. The
            -- dispatcher's placement + scale-down read this (the same
            -- load metric the listener uses), so a supervisor sheds /
            -- attracts work by real pressure, not a project count. 0
            -- until the pod reports (fresh-spawned row).
            mem_pressure      DOUBLE PRECISION NOT NULL DEFAULT 0,
            -- Spawn grace: until it passes, the idle reaper leaves the
            -- pod alone even owning zero projects, so a freshly-spawned
            -- supervisor is not torn down in the window before its claim
            -- loop adopts its first project. Exact analog of
            -- `listener_pod.grace_until_unix`.
            grace_until_unix  BIGINT NOT NULL,
            -- True while this pod is being scaled DOWN: its leases have
            -- been released for re-adoption and it must claim nothing
            -- more (else its own ownership loop re-grabs what the drain
            -- just released, defeating consolidation). The broker's
            -- claim CTE excludes a draining pod; the reaper clears it
            -- with the row.
            draining          BOOLEAN NOT NULL DEFAULT FALSE
        );
CREATE TABLE IF NOT EXISTS infra_owner (
            project_id        TEXT PRIMARY KEY,
            supervisor_pod    TEXT NOT NULL,
            namespace         TEXT NOT NULL,
            tenant_id         TEXT NOT NULL,
            leased_until_unix BIGINT NOT NULL
        );
CREATE INDEX IF NOT EXISTS idx_infra_owner_pod
             ON infra_owner(supervisor_pod);
