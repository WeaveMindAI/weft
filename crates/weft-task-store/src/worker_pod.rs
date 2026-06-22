//! `worker_pod` table + journal-fencing trigger. One row per Pod.
//! Worker boots → `register_alive`; engine heartbeats; reaper marks
//! stale rows dead. The trigger on `exec_event` rejects writes whose
//! pod_name is not in `{spawning, alive}`, fencing stale Pods out.

use anyhow::Result;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::tasks::unix_now;

pub const HEARTBEAT_INTERVAL_SECS: u64 = 10;
pub const HEARTBEAT_STALE_SECS: i64 = 30;

/// How long a row may sit in `spawning` (reserved, kubectl-applied, but
/// the worker never registered itself `alive`) before the reaper marks
/// it dead. Without this, a failed boot (OOM during start, a panic
/// before the first heartbeat, a pod deleted out-of-band, a node that
/// never came up) leaves a ghost `spawning` row forever, and the
/// scale-up check counts `spawning` as available capacity, so the
/// project's pending work would hang with no live worker and no error.
///
/// Deliberately GENEROUS, far above any realistic boot (image pull +
/// binary init), so a healthy worker always reaches `register_alive`
/// (which leaves the spawning state) long before this trips: it never
/// false-positives a slow-but-healthy boot, a Pending pod waiting for a
/// node, or a slow multi-GB pull. A genuinely-broken image does not need
/// a faster reap here: it is already surfaced loudly at spawn time
/// (`wait_for_pull_ok` fails the spawn task in seconds). One honest
/// generous deadline, not a per-k8s-state classifier (which can't tell
/// an unscheduled pod that must wait from a wedged one that should be
/// reaped, and would false-positive the former).
pub const SPAWN_BOOT_DEADLINE_SECS: i64 = 1800;

/// Lifecycle states for a `worker_pod` row.
///
/// `spawning` → `alive` → `done | dead`.
///   - `spawning`: dispatcher reserved the row before `kubectl apply`.
///   - `alive`: worker registered itself, heartbeat is fresh.
///   - `done`: worker exited cleanly (heartbeat loop saw the row go
///     away, mark_done called).
///   - `dead`: reaper concluded the heartbeat is stale and the worker
///     died without writing `done`.
///
/// The fencing trigger lets `spawning` and `alive` rows write to
/// `exec_event`. Anything else is rejected.
const STATUS_SPAWNING: &str = "spawning";
const STATUS_ALIVE: &str = "alive";
const STATUS_DONE: &str = "done";
const STATUS_DEAD: &str = "dead";

/// Minimal projection of the `worker_pod` table: only the columns the stale /
/// terminal reapers act on. The e2e rig reads a DIFFERENT projection of the same
/// table (it needs `status` + `terminal_at_unix`, which production reads via
/// WHERE clauses, not struct fields), so the two are honestly-distinct read
/// shapes, not a fragmented concept; keep them separate and grep-linked.
/// SYNC: worker_pod columns <-> crates/weft-e2e/src/platform.rs (WorkerPodRow)
#[derive(Debug, Clone)]
pub struct WorkerPodRow {
    pub pod_name: String,
    pub project_id: String,
    pub namespace: String,
    pub last_heartbeat_unix: i64,
    pub created_at_unix: i64,
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    let stmts = [
        r#"CREATE TABLE IF NOT EXISTS worker_pod (
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
            -- proceeds with a fresh spawn.
            -- NOTE: capacity is governed by MEMORY pressure, not a task
            -- count. A pod's `mem_pressure` (below) is what placement and
            -- scale-down read; idle-exit (`mark_done_if_idle`) is gated by
            -- its pending/claimed-task `NOT EXISTS` check (any in-flight
            -- execution, live or not, is a worker task and so blocks
            -- idle-exit).
            binary_hash TEXT NOT NULL DEFAULT '',
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
            drained_at_unix BIGINT
        )"#,
        r#"CREATE INDEX IF NOT EXISTS idx_worker_pod_project_alive
            ON worker_pod(project_id)
            WHERE status IN ('spawning', 'alive')"#,
        r#"CREATE INDEX IF NOT EXISTS idx_worker_pod_heartbeat
            ON worker_pod(last_heartbeat_unix)
            WHERE status = 'alive'"#,
        // Generation fencing: a Pod whose row is not {spawning, alive}
        // cannot write to exec_event. NULL pod_name (listener /
        // dispatcher writes) bypasses the check.
        r#"CREATE OR REPLACE FUNCTION weft_check_pod_alive() RETURNS trigger AS $$
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
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS exec_event_pod_alive_check ON exec_event"#,
        r#"CREATE TRIGGER exec_event_pod_alive_check
            BEFORE INSERT ON exec_event
            FOR EACH ROW
            EXECUTE FUNCTION weft_check_pod_alive()"#,
        // Ownership-follows-claim: whenever a worker claims a task that
        // carries a color, that pod becomes the color's owner. Done as
        // an AFTER-UPDATE trigger so it commits in the SAME transaction
        // as the claim itself (`tasks::claim_one`'s UPDATE), making
        // "claimed by pod X" and "owned by pod X" atomically
        // inseparable. Without this (a separate UPDATE after the claim
        // commits) a crash between the two leaves a task claimed by a
        // pod that does not own its color, so every journal write from
        // that pod is fenced until the lease expires. "Latest claim
        // wins" is exactly what a resume handoff needs: the fresh pod
        // that reclaims a dead owner's resume takes ownership here. The
        // task table's claim semantics already enforce one active pod
        // per task, so there is never an overlap where two pods own one
        // color. `execution_color` is seeded at ExecutionStarted (well
        // before any task is claimed), so the row always exists; if it
        // somehow does not the UPDATE matches zero rows and the
        // broker's journal-write owner check then refuses the pod
        // loudly (no silent mis-bind).
        // SYNC: this is the ONLY writer of execution_color.owner_pod_name;
        // the reader is crates/weft-broker/src/handlers.rs journal_record.
        r#"CREATE OR REPLACE FUNCTION weft_bind_color_owner() RETURNS trigger AS $$
            BEGIN
                IF NEW.color IS NOT NULL AND NEW.claimed_by IS NOT NULL THEN
                    UPDATE execution_color
                    SET owner_pod_name = NEW.claimed_by
                    WHERE color = NEW.color;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql"#,
        r#"DROP TRIGGER IF EXISTS task_claim_binds_color_owner ON task"#,
        // Fire only on:
        //   - the pending/claimed-lapsed -> claimed transition
        //     (claimed_by goes from NULL/other to a pod), not on every
        //     task UPDATE (complete, heartbeat-renew, requeue), so the
        //     hot path stamps ownership exactly once per claim; AND
        //   - tasks that actually DRIVE the color: 'execute' and
        //     'resume'. A side-channel task that merely carries a color
        //     (a 'cancel_execution' addressed to the owner) must NOT
        //     restamp ownership: ownership follows the driver. (cancel is
        //     pinned to the owner anyway, so even if it did fire the
        //     stamp would be owner->owner, but scoping by kind makes a
        //     future color-bearing task kind unable to steal ownership by
        //     accident, which is the property we want to hold by
        //     construction, not by every caller remembering to pin.)
        r#"CREATE TRIGGER task_claim_binds_color_owner
            AFTER UPDATE OF claimed_by ON task
            FOR EACH ROW
            WHEN (NEW.status = 'claimed' AND NEW.claimed_by IS NOT NULL
                  AND NEW.claimed_by IS DISTINCT FROM OLD.claimed_by
                  AND NEW.kind IN ('execute', 'resume'))
            EXECUTE FUNCTION weft_bind_color_owner()"#,
    ];
    for sql in stmts {
        sqlx::query(sql).execute(pool).await?;
    }
    Ok(())
}

/// Worker flips its pre-inserted row to `alive` and stamps the
/// first heartbeat. The dispatcher's `insert_spawning` always runs
/// before the kubectl apply that creates the pod, so the row is
/// guaranteed to exist by the time the worker calls this. If it
/// doesn't (e.g. a forged pod_name from a compromised caller), the
/// UPDATE affects zero rows and we surface that as an error: there
/// is no INSERT fallback so tenant-supplied namespace/owner data
/// never reaches the row.
pub async fn register_alive(
    pool: &PgPool,
    pod_name: &str,
    project_id: &str,
) -> Result<()> {
    let now = unix_now();
    // `AND status = 'spawning'` is load-bearing: without it a dead-
    // marked row (the reaper called `mark_dead` between this worker's
    // boot and its first heartbeat) could be flipped back to `alive`
    // by the worker, defeating the generation fencing. With the
    // guard, a dead row stays dead; rows_affected = 0 surfaces the
    // condition and the worker exits via `bail!` instead of
    // resurrecting itself.
    let res = sqlx::query(
        r#"UPDATE worker_pod
           SET status = $3, last_heartbeat_unix = $4
           WHERE pod_name = $1 AND project_id = $2 AND status = $5"#,
    )
    .bind(pod_name)
    .bind(project_id)
    .bind(STATUS_ALIVE)
    .bind(now)
    .bind(STATUS_SPAWNING)
    .execute(pool)
    .await?;
    if res.rows_affected() == 0 {
        anyhow::bail!(
            "no spawning worker_pod row for pod_name='{pod_name}' \
             project_id='{project_id}'; the row was either never \
             inserted by the dispatcher or was marked dead before \
             this worker registered"
        );
    }
    Ok(())
}

/// Reserve a row before `kubectl apply`. Idempotent on retry via
/// `ON CONFLICT DO NOTHING`. The dispatcher's spawn_pod task calls
/// this so a partial-success retry collides on the same pod_name.
pub async fn insert_spawning(
    pool: &PgPool,
    pod_name: &str,
    project_id: &str,
    namespace: &str,
    owner_dispatcher: &str,
    binary_hash: &str,
) -> Result<()> {
    let now = unix_now();
    sqlx::query(
        r#"INSERT INTO worker_pod (
            pod_name, project_id, namespace, status, owner_dispatcher,
            last_heartbeat_unix, created_at_unix, binary_hash
        ) VALUES ($1, $2, $3, $4, $5, $6, $6, $7)
        ON CONFLICT (pod_name) DO NOTHING"#,
    )
    .bind(pod_name)
    .bind(project_id)
    .bind(namespace)
    .bind(STATUS_SPAWNING)
    .bind(owner_dispatcher)
    .bind(now)
    .bind(binary_hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Engine heartbeat. Stamps the freshness time AND the worker's current
/// memory pressure in one write (the worker reads its own cgroup
/// pressure each tick and reports it here, the same self-report the
/// supervisor does on its ownership tick). Returns false if the row no
/// longer matches (status changed, row deleted) so the worker can exit.
pub async fn heartbeat(pool: &PgPool, pod_name: &str, mem_pressure: f64) -> Result<bool> {
    // Clamp at the write boundary to a sane [0,1]. The producer
    // (CgroupMemPressure) already clamps, so this only bites a buggy or
    // hostile caller, but placement/scaledown SQL compares `mem_pressure
    // < $sat` and orders by it, and a NaN (which compares false to
    // everything) or an out-of-range value would make those decisions
    // behave unpredictably. NaN maps to 0 (treat "unknown" as "empty",
    // matching the fresh-spawn default) rather than poisoning the order.
    // This guard is defense-in-depth, so if it ever actually FIRES it
    // means a producer is sending garbage: warn loudly rather than
    // silently swallow it (the rules forbid silent recovery).
    let clamped = if mem_pressure.is_nan() { 0.0 } else { mem_pressure.clamp(0.0, 1.0) };
    if clamped != mem_pressure {
        tracing::warn!(
            target: "weft_task_store::worker_pod",
            pod = %pod_name,
            reported = mem_pressure,
            stored = clamped,
            "worker reported an out-of-range memory pressure; clamped it. \
             This should never happen with a correct producer; investigate the reader."
        );
    }
    let mem_pressure = clamped;
    let res = sqlx::query(
        r#"UPDATE worker_pod
           SET last_heartbeat_unix = $1, mem_pressure = $3
           WHERE pod_name = $2 AND status = 'alive'"#,
    )
    .bind(unix_now())
    .bind(pod_name)
    .bind(mem_pressure)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn mark_done(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE worker_pod
           SET status = $1, terminal_at_unix = $3
           WHERE pod_name = $2 AND status IN ('spawning', 'alive')"#,
    )
    .bind(STATUS_DONE)
    .bind(pod_name)
    .bind(crate::tasks::unix_now())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_dead(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query(
        r#"UPDATE worker_pod
           SET status = $1, terminal_at_unix = $3
           WHERE pod_name = $2 AND status IN ('spawning', 'alive')"#,
    )
    .bind(STATUS_DEAD)
    .bind(pod_name)
    .bind(crate::tasks::unix_now())
    .execute(pool)
    .await?;
    Ok(())
}

/// Idle self-exit (worker-driven). Flip this pod's row `alive -> done`
/// when it has no more work to do. The no-work check and the status flip
/// are ONE atomic UPDATE, so a task arriving concurrently is caught:
///   - lands BEFORE the CAS commits: the `NOT EXISTS` fails, the row
///     stays `alive`, the worker keeps running and claims it;
///   - lands AFTER: the row is already `done`, cold_start sees no
///     admittable pod and spawns a fresh one for the new work.
/// Either way no exec is ever routed to a dying worker. Returns true if
/// this call won the flip (caller should then exit).
///
/// "No more work" depends on whether the pod is DRAINING:
///   - NOT draining: no pending/claimed worker task for the whole
///     PROJECT. While the project has any work, a live worker stays warm
///     (it could claim that work). This is the steady-state idle exit:
///     the project went fully quiet.
///   - draining: no task this pod OWNS, i.e. nothing CLAIMED BY it and
///     nothing PINNED to it. Sibling work is irrelevant, a draining pod
///     will never claim unpinned work again (claim_one excludes it), so
///     gating its exit on project-wide work would pin it alive forever
///     while siblings stay busy and the drain would never complete.
/// Both checks read the pod's own row (`pod_name = $1`), never a
/// caller-supplied project id, so a worker can only ever flip itself.
pub async fn mark_done_if_idle(pool: &PgPool, pod_name: &str) -> Result<bool> {
    let res = sqlx::query(
        r#"UPDATE worker_pod wp
           SET status = $2, terminal_at_unix = $3
           WHERE wp.pod_name = $1
             AND wp.status = 'alive'
             AND CASE WHEN wp.draining THEN
                 NOT EXISTS (
                     SELECT 1 FROM task t
                     WHERE t.target = 'worker'
                       AND t.status IN ('pending', 'claimed')
                       AND (t.claimed_by = wp.pod_name OR t.target_pod_name = wp.pod_name)
                 )
             ELSE
                 NOT EXISTS (
                     SELECT 1 FROM task t
                     WHERE t.target = 'worker'
                       AND t.project_id = wp.project_id
                       AND t.status IN ('pending', 'claimed')
                 )
             END"#,
    )
    .bind(pod_name)
    .bind(STATUS_DONE)
    .bind(crate::tasks::unix_now())
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn has_live_for_project(pool: &PgPool, project_id: &str) -> Result<bool> {
    // Cast literal to bigint so sqlx's i64 type expectation matches.
    // PostgreSQL types untyped integer literals as INT4; the column
    // shape doesn't matter here because we throw the value away.
    let row: Option<(i64,)> = sqlx::query_as(
        r#"SELECT 1::bigint FROM worker_pod
           WHERE project_id = $1 AND status IN ('spawning', 'alive')
           LIMIT 1"#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// Look up the currently-alive worker pod for `project_id`. Returns
/// the `(pod_name, namespace, binary_hash)` of the first match, or
/// None when no pod is alive. Used by `spawn_pod` to decide whether
/// to reuse the existing pod or replace it (when its binary_hash no
/// longer matches the project's current `running_binary_hash`).
pub async fn alive_pod_for_project_full(
    pool: &PgPool,
    project_id: &str,
) -> Result<Option<(String, String, String)>> {
    let row: Option<(String, String, String)> = sqlx::query_as(
        r#"SELECT pod_name, namespace, binary_hash FROM worker_pod
           WHERE project_id = $1 AND status IN ('spawning', 'alive')
           ORDER BY created_at_unix ASC
           LIMIT 1"#,
    )
    .bind(project_id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Pick the least-pressured ADMITTABLE worker pod for a project, or
/// None when there is no admittable pod (none alive, or every alive pod
/// is saturated / draining). "Admittable" = alive (spawning|alive), not
/// draining, and below `saturation`. This is the per-project analog of
/// the supervisor pool's `pick_live`: a project runs N workers, and a
/// NEW execution goes to the least-loaded one with memory headroom.
/// Returns `(pod_name, namespace)` so the caller can pin and route.
///
/// A `spawning` pod reports 0 pressure until its first heartbeat, so it
/// is correctly preferred (it is empty). The caller spawns a fresh pod
/// and retries when this returns None (every pod saturated).
pub async fn pick_admittable_for_project(
    pool: &PgPool,
    project_id: &str,
    saturation: f64,
) -> Result<Option<(String, String)>> {
    let row: Option<(String, String)> = sqlx::query_as(
        r#"SELECT pod_name, namespace FROM worker_pod
           WHERE project_id = $1
             AND status IN ('spawning', 'alive')
             AND NOT draining
             AND mem_pressure < $2
           ORDER BY mem_pressure ASC, created_at_unix ASC
           LIMIT 1"#,
    )
    .bind(project_id)
    .bind(saturation)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Per-pod `(pod_name, mem_pressure)` for every ESTABLISHED worker of a
/// project, the raw input to the dispatcher's scale-down planner.
/// "Established" = alive (not still spawning) and not already draining: a
/// pod mid-spawn or mid-drain is not a stable scale-down candidate.
/// Returns raw tuples (NOT `PoolPodLoad`) so this crate stays free of a
/// `weft-platform-traits` dependency; the dispatcher maps the tuples
/// into the planner's shape at the call site.
pub async fn pod_loads_for_project(
    pool: &PgPool,
    project_id: &str,
) -> Result<Vec<(String, f64)>> {
    let rows: Vec<(String, f64)> = sqlx::query_as(
        r#"SELECT pod_name, mem_pressure FROM worker_pod
           WHERE project_id = $1 AND status = 'alive' AND NOT draining"#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Projects with more than one alive, NON-draining worker, the only
/// scale-DOWN candidates (a single stable worker has nothing to
/// consolidate; an already-draining worker is on its way out and is not
/// a candidate). Cheap pre-filter so the scaledown sweep runs the
/// planner only where it could act. The `NOT draining` predicate MUST
/// match `pod_loads_for_project` exactly: if it didn't (e.g. counting a
/// draining pod here), a project with one live + one draining worker
/// would pass this gate but feed the planner a single pod, wasting a
/// tick (the planner's own len() < 2 floor then no-ops). Same candidate
/// set on both sides keeps this honest.
pub async fn projects_with_multiple_workers(pool: &PgPool) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"SELECT project_id FROM worker_pod
           WHERE status = 'alive' AND NOT draining
           GROUP BY project_id
           HAVING COUNT(*) > 1"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(p,)| p).collect())
}

/// Mark a worker pod draining: placement stops sending it NEW executions
/// (`pick_admittable_for_project` skips draining pods) while its
/// in-flight executions finish and it idle-exits via `mark_done_if_idle`.
/// Idempotent (only stamps `drained_at_unix` on the first mark, so the
/// elapsed breadcrumb measures from when draining actually began). Unlike
/// the supervisor drain there is no lease to release: a running execution
/// is bound to its driving worker and cannot be handed off, so draining a
/// worker only stops new admissions.
pub async fn set_draining(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query(
        "UPDATE worker_pod SET draining = TRUE, drained_at_unix = $2 \
         WHERE pod_name = $1 AND NOT draining",
    )
    .bind(pod_name)
    .bind(unix_now())
    .execute(pool)
    .await?;
    Ok(())
}

/// A draining pod's breadcrumb input: `(pod_name, project_id,
/// drained_at_unix, in_flight_tasks)` for every pod currently draining.
/// `in_flight_tasks` is the count of pending/claimed worker tasks the pod
/// still owns (claimed by it or pinned to it), the same set
/// `mark_done_if_idle`'s draining branch waits on, so the breadcrumb says
/// exactly what the drain is blocked on. Used only for periodic logging;
/// a drain has no deadline.
pub async fn draining_breadcrumbs(pool: &PgPool) -> Result<Vec<(String, String, i64, i64)>> {
    let rows: Vec<(String, String, Option<i64>, i64)> = sqlx::query_as(
        r#"SELECT wp.pod_name, wp.project_id, wp.drained_at_unix,
                  (SELECT COUNT(*) FROM task t
                   WHERE t.target = 'worker'
                     AND t.status IN ('pending', 'claimed')
                     AND (t.claimed_by = wp.pod_name OR t.target_pod_name = wp.pod_name)
                  )::bigint
           FROM worker_pod wp
           WHERE wp.status = 'alive' AND wp.draining"#,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(pod, project, drained, n)| (pod, project, drained.unwrap_or(0), n))
        .collect())
}

pub async fn list_stale(pool: &PgPool, threshold_unix: i64) -> Result<Vec<WorkerPodRow>> {
    let rows = sqlx::query(
        r#"SELECT pod_name, project_id, namespace, last_heartbeat_unix, created_at_unix
           FROM worker_pod
           WHERE status = 'alive' AND last_heartbeat_unix < $1"#,
    )
    .bind(threshold_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Rows stuck in `spawning` past `threshold_unix` (pass `now -
/// SPAWN_BOOT_DEADLINE_SECS`): reserved + kubectl-applied, but the worker
/// never registered itself `alive` within the generous boot deadline.
/// The reaper marks these dead so a failed boot stops being counted as
/// capacity by the scale-up check (which treats `spawning` as
/// admittable). Keyed on `created_at_unix` (a spawning row has no
/// heartbeat yet). Returns the rows oldest-first.
pub async fn list_stale_spawning(pool: &PgPool, threshold_unix: i64) -> Result<Vec<WorkerPodRow>> {
    let rows = sqlx::query(
        r#"SELECT pod_name, project_id, namespace, last_heartbeat_unix, created_at_unix
           FROM worker_pod
           WHERE status = 'spawning' AND created_at_unix < $1
           ORDER BY created_at_unix ASC"#,
    )
    .bind(threshold_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Terminal rows (`done` | `dead`) whose `terminal_at_unix` is
/// older than the threshold. The pod-GC sweep `kubectl delete`s
/// the finished k8s Pod object (using the row's own namespace, so
/// no namespace-mapper guessing) and removes the row. Driven off
/// the table, not `kubectl get`, so it's the single source of
/// truth and is fakeable in tests.
pub async fn list_terminal(pool: &PgPool, threshold_unix: i64) -> Result<Vec<WorkerPodRow>> {
    let rows = sqlx::query(
        r#"SELECT pod_name, project_id, namespace, last_heartbeat_unix, created_at_unix
           FROM worker_pod
           WHERE status IN ('done', 'dead')
             AND terminal_at_unix IS NOT NULL
             AND terminal_at_unix < $1"#,
    )
    .bind(threshold_unix)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(parse_row).collect()
}

/// Remove a worker_pod row. Called by the pod-GC sweep after the
/// k8s Pod object is deleted, so the row and the object retire
/// together.
pub async fn delete_row(pool: &PgPool, pod_name: &str) -> Result<()> {
    sqlx::query("DELETE FROM worker_pod WHERE pod_name = $1")
        .bind(pod_name)
        .execute(pool)
        .await?;
    Ok(())
}

/// Decode a worker_pod row, propagating decode errors. A `.ok()`
/// drop here would silently skip a stale/terminal pod from the
/// reaper's / GC's list -> the pod leaks with no error. Fail loud
/// on schema drift instead.
fn parse_row(row: sqlx::postgres::PgRow) -> Result<WorkerPodRow> {
    Ok(WorkerPodRow {
        pod_name: row.try_get("pod_name")?,
        project_id: row.try_get("project_id")?,
        namespace: row.try_get("namespace")?,
        last_heartbeat_unix: row.try_get("last_heartbeat_unix")?,
        created_at_unix: row.try_get("created_at_unix")?,
    })
}

// NOTE: live-connection routing (which pod a new caller pins to) lives in
// `tasks::admit_live_execution`: in one transaction, under a per-project
// advisory lock, it picks the least-PRESSURED admittable worker (alive, not
// draining, memory below the saturation threshold, the same predicate as
// `pick_admittable_for_project`) and INSERTs the pinned execute task on it.
// Capacity is MEMORY-bounded, not a connection count; admission IS the task
// insert, so there is no separate counter to drift and no admit/insert window.
// The advisory lock keeps a burst of concurrent handshakes from stampeding one
// pod before its next heartbeat updates the pressure reading. The end-to-end
// behaviour (placement, spawn-on-saturation, drain) is a DB + k8s concern,
// covered by the Layer-4 cluster e2e, not a pure unit test.
