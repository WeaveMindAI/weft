CREATE TABLE IF NOT EXISTS worker_pod (
            pod_name TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            namespace TEXT NOT NULL,
            status TEXT NOT NULL,
            owner_dispatcher TEXT NOT NULL,
            last_heartbeat_unix BIGINT NOT NULL,
            created_at_unix BIGINT NOT NULL,
            -- Set when the row transitions to a terminal status
            -- (done | dead). NULL while spawning/alive. The pod-GC
            -- sweep ages terminal rows against this to delete the
            -- finished k8s Pod object after a grace window.
            terminal_at_unix BIGINT,
            -- Binary hash the pod's image was built from. Recorded at
            -- spawn time. The spawn_pod executor compares this against
            -- the project's current running_binary_hash on every spawn
            -- attempt: a mismatch means the alive pod has a stale
            -- binary (e.g. user changed a node implementation since
            -- it spawned). The dispatcher kills the stale pod and
            -- proceeds with a fresh spawn. NULL for a role='node-test'
            -- row: a test pod has no worker binary (its image travels
            -- on the task payload), and NULL never compares equal to
            -- a real hash, so no worker query can ever match it.
            -- NOTE: capacity is governed by MEMORY pressure, not a task
            -- count. A pod's `mem_pressure` (below) is what placement and
            -- scale-down read; idle-exit (`mark_done_if_idle`) is gated by
            -- its pending/claimed-task `NOT EXISTS` check (any in-flight
            -- execution, live or not, is a worker task and so blocks
            -- idle-exit).
            binary_hash TEXT,
            -- The worker's last self-reported memory pressure
            -- (usage/limit, [0,1]), written on each heartbeat tick. The
            -- dispatcher's worker placement + scale-down read this (the
            -- SAME metric the listener and supervisor pools use), so a
            -- worker attracts / sheds executions by real memory pressure,
            -- not by a connection or execution count. 0 until the pod
            -- reports (fresh-spawned row); 0 also locally (no cgroup
            -- limit), so one worker until the machine is squeezed.
            mem_pressure DOUBLE PRECISION NOT NULL DEFAULT 0,
            -- True while this pod is being scaled DOWN: placement skips it
            -- so NEW executions stop landing on it, while its in-flight
            -- executions finish and it idle-exits itself via the normal
            -- `mark_done_if_idle` CAS. Unlike the supervisor (which can
            -- release work to a sibling) a worker cannot hand off a
            -- running execution (the journal is one stream per color), so
            -- draining a worker is "stop admitting, let it finish," never
            -- "evacuate." Cleared with the row when the drained pod exits.
            draining BOOLEAN NOT NULL DEFAULT FALSE,
            -- Unix time the pod was marked draining (NULL while not
            -- draining). A drain has no deadline (a live execution may
            -- legitimately run for hours/days, and we never time out a
            -- user's own program), so this is purely for legibility: the
            -- scaledown sweep logs elapsed-since-drain + the pod's
            -- remaining in-flight work as a periodic breadcrumb, so a pod
            -- stuck draining is visible rather than silent.
            drained_at_unix BIGINT,
            -- What the pod IS: 'worker' (runs the project's compiled
            -- graph and claims work) or 'node-test' (a short-lived
            -- test pod holding a broker identity, driven by its
            -- enqueuing task). Worker capacity, placement,
            -- reconciliation, and scale-down read ONLY role='worker';
            -- identity resolution (the broker's pod->tenant lookup)
            -- reads both.
            role TEXT NOT NULL DEFAULT 'worker',
            -- The task that owns this pod's lifecycle, for pods driven
            -- by a task executor rather than by their own claim loop
            -- (role='node-test'). The orphan sweep reaps a non-terminal
            -- node-test row once its owning task is gone or has been
            -- terminal past a grace window (see list_orphaned_node_test).
            -- NULL for role='worker' rows (a worker outlives any one
            -- task).
            owner_task_id UUID
        );
CREATE INDEX IF NOT EXISTS idx_worker_pod_project_alive
            ON worker_pod(project_id)
            WHERE status IN ('spawning', 'alive');
CREATE INDEX IF NOT EXISTS idx_worker_pod_heartbeat
            ON worker_pod(last_heartbeat_unix)
            WHERE status = 'alive';
CREATE OR REPLACE FUNCTION weft_check_pod_alive() RETURNS trigger AS $$
            DECLARE
                pod_status TEXT;
            BEGIN
                IF NEW.pod_name IS NULL THEN
                    RETURN NEW;
                END IF;
                SELECT status INTO pod_status
                FROM worker_pod
                WHERE pod_name = NEW.pod_name;
                IF pod_status IS NULL OR pod_status NOT IN ('spawning', 'alive') THEN
                    RAISE EXCEPTION
                        'pod % is not alive (status=%)',
                        NEW.pod_name, COALESCE(pod_status, 'missing')
                        USING ERRCODE = 'P0001';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS exec_event_pod_alive_check ON exec_event;
CREATE TRIGGER exec_event_pod_alive_check
            BEFORE INSERT ON exec_event
            FOR EACH ROW
            EXECUTE FUNCTION weft_check_pod_alive();
CREATE OR REPLACE FUNCTION weft_bind_color_owner() RETURNS trigger AS $$
            BEGIN
                IF NEW.color IS NOT NULL AND NEW.claimed_by IS NOT NULL THEN
                    UPDATE execution_color
                    SET owner_pod_name = NEW.claimed_by
                    WHERE color = NEW.color;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS task_claim_binds_color_owner ON task;
CREATE TRIGGER task_claim_binds_color_owner
            AFTER UPDATE OF claimed_by ON task
            FOR EACH ROW
            WHEN (NEW.status = 'claimed' AND NEW.claimed_by IS NOT NULL
                  AND NEW.claimed_by IS DISTINCT FROM OLD.claimed_by
                  AND NEW.kind IN ('execute', 'resume'))
            EXECUTE FUNCTION weft_bind_color_owner();
