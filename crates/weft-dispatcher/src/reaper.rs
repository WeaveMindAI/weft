//! Background reapers that sweep stale rows and respawn missing
//! workers. Every dispatcher Pod runs all of these. They don't step
//! on each other because their writes are idempotent (delete-by-key
//! is a no-op on the second pod, `mark_dead` is status-guarded, task
//! reclaim is conditional); the listener and supervisor sweeps add a
//! per-tenant advisory lock on top. The worker-pod sweeps rely on
//! idempotency alone (plain SELECT then idempotent delete), not row
//! locking.

use std::time::Duration;

use crate::state::DispatcherState;

/// Spawn every reaper. Returns immediately; the reapers run for the
/// lifetime of the process.
pub fn spawn_all(state: DispatcherState) {
    spawn_loop(state.clone(), Duration::from_secs(30), "worker_pod", sweep_worker_pods);
    spawn_loop(state.clone(), Duration::from_secs(30), "worker_pod_gc", sweep_terminal_worker_pods);
    spawn_loop(state.clone(), Duration::from_secs(15), "orphaned_tasks", sweep_orphaned_tasks);
    spawn_loop(state.clone(), Duration::from_secs(3600), "tasks", sweep_tasks);
    spawn_loop(state.clone(), Duration::from_secs(10), "listener", sweep_listeners);
    spawn_loop(state.clone(), Duration::from_secs(60), "listener_scaledown", sweep_listener_scaledown);
    spawn_loop(state.clone(), Duration::from_secs(30), "supervisor", sweep_supervisors);
    spawn_loop(state.clone(), Duration::from_secs(60), "supervisor_scaledown", sweep_supervisor_scaledown);
    spawn_loop(state.clone(), Duration::from_secs(60), "worker_scaledown", sweep_worker_scaledown);
    // Storage plane: the durable terminate sweep (un-kept exec files)
    // and the scale-to-zero box reaper. Idempotent across pods like
    // the rest: the sweep queue deletes per-color rows only after the
    // box confirms; teardown re-checks live usage first.
    spawn_loop(
        state.clone(),
        Duration::from_secs(15),
        "storage_sweep",
        crate::storage_box::process_sweep_queue,
    );
    spawn_loop(state, Duration::from_secs(60), "storage_box", crate::storage_box::sweep_boxes);
}

/// Grace before a terminal (`done`/`dead`) worker_pod's k8s Pod
/// object is deleted: keeps a just-finished pod inspectable
/// (`kubectl logs`) for a window before GC.
const TERMINAL_POD_GRACE_SECS: i64 = 120;

/// Spawn a periodic sweep task. The body is the only thing that
/// differs across reapers; the loop shape (sleep / call / log on
/// error) is identical. The sweep takes the state by clone (cheap,
/// `DispatcherState` is Arc-fielded), which keeps the trait bound
/// simple compared to a borrowing closure.
fn spawn_loop<F, Fut>(
    state: DispatcherState,
    interval: Duration,
    name: &'static str,
    sweep: F,
)
where
    F: Fn(DispatcherState) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send,
{
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = sweep(state.clone()).await {
                tracing::warn!(
                    target: "weft_dispatcher::reaper",
                    reaper = name,
                    error = %e,
                    "reaper sweep failed"
                );
            }
        }
    });
}

/// Worker-pod reaper. Once every 30s, mark failed pods `dead` (which
/// makes the fencing trigger reject any further journal writes) +
/// `kubectl delete` the Pod:
///   - `alive` rows whose heartbeat went stale (the worker died), and
///   - `spawning` rows that never registered `alive` within the generous
///     boot deadline. Without sweeping the latter, a ghost `spawning` row
///     is counted as available capacity by the scale-up check forever, so
///     the project's pending work hangs with no live worker and no error.
///
/// The spawning deadline (`SPAWN_BOOT_DEADLINE_SECS`) is deliberately
/// GENEROUS, far above any realistic boot (image pull + binary init), so
/// a healthy worker always reaches `register_alive` (which leaves the
/// spawning state) long before it trips: the deadline never false-
/// positives a slow-but-healthy boot, including a Pending pod waiting for
/// a node or a slow multi-GB pull. We do NOT try to reap a doomed boot
/// faster off its k8s state: a genuinely-broken image is already surfaced
/// loudly at spawn time (`wait_for_pull_ok` fails the spawn task in ~5s),
/// and trying to classify "stuck" from the container-waiting reason can't
/// tell an unscheduled pod (must wait) from a wedged one (reap), so it
/// would false-positive the former. One honest generous deadline instead.
/// Pending tasks remain claimable: cold-start respawns once the ghost is
/// gone.
async fn sweep_worker_pods(state: DispatcherState) -> anyhow::Result<()> {
    let now = crate::lease::now_unix();
    let stale = weft_task_store::worker_pod::list_stale(
        &state.pg_pool,
        now - weft_task_store::worker_pod::HEARTBEAT_STALE_SECS,
    )
    .await?;
    let stuck_spawning = weft_task_store::worker_pod::list_stale_spawning(
        &state.pg_pool,
        now - weft_task_store::worker_pod::SPAWN_BOOT_DEADLINE_SECS,
    )
    .await?;
    // Stale-heartbeat alive pods (worker died) and over-deadline spawning
    // pods (never registered) both reap through the same path; a dead row
    // is not re-listed by either query, so each is reaped once.
    for (reason, row) in stale
        .into_iter()
        .map(|r| ("stale heartbeat", r))
        .chain(
            stuck_spawning
                .into_iter()
                .map(|r| ("spawning past boot deadline, never registered alive", r)),
        )
    {
        reap_worker_pod(&state, &row, reason).await?;
    }
    Ok(())
}

/// Mark a worker pod dead + kubectl-delete it. Shared by the stale-alive
/// and failed-spawning paths. A dead row is no longer re-listed by either
/// query, so each pod is reaped exactly once. Does NOT recover the pod's
/// stranded tasks (that is `sweep_orphaned_tasks`' job, a self-healing
/// task-driven sweep; doing it here would run at most once and strand
/// anything that failed).
async fn reap_worker_pod(
    state: &DispatcherState,
    row: &weft_task_store::worker_pod::WorkerPodRow,
    reason: &str,
) -> anyhow::Result<()> {
    tracing::warn!(
        target: "weft_dispatcher::reaper",
        project = %row.project_id,
        pod = %row.pod_name,
        last_heartbeat = row.last_heartbeat_unix,
        reason,
        "marking failed worker pod dead"
    );
    weft_task_store::worker_pod::mark_dead(&state.pg_pool, &row.pod_name).await?;
    // kubectl delete: log loudly on error. A failed kill leaves the pod
    // alive in k8s while our DB says dead, which means a stale pod can
    // keep running. Not fatal to the sweep (the next tick retries), but
    // never silent.
    if let Err(e) = state
        .workers
        .kill_pod(row.pod_name.clone(), row.namespace.clone())
        .await
    {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            pod = %row.pod_name,
            error = %e,
            "kill_pod failed for failed worker; pod may survive in k8s until next sweep"
        );
    }
    Ok(())
}

/// Self-healing recovery of tasks stranded on a non-routable worker pod. Runs
/// on a timer, INDEPENDENT of how a pod became dead (stale heartbeat, stale-
/// image replacement, crash). A live-execute task pinned to a dead pod cannot
/// re-run (its caller was gateway-routed to that exact pod), so its execution
/// is terminally cancelled; every other stranded task is requeued. The task
/// row is the durable retry handle: anything not fully recovered this tick is
/// re-found next tick. See `tasks::reclaim_orphaned_tasks`.
async fn sweep_orphaned_tasks(state: DispatcherState) -> anyhow::Result<()> {
    let orphans = weft_task_store::tasks::reclaim_orphaned_tasks(&state.pg_pool).await?;
    for orphan in orphans {
        let Ok(color) = orphan.color.parse::<weft_core::Color>() else {
            // Corrupt color: leave the task as evidence, surface loud.
            tracing::error!(
                target: "weft_dispatcher::reaper",
                color = %orphan.color, task = %orphan.task_id,
                "orphaned live execution has an unparseable color; leaving its task for inspection"
            );
            continue;
        };
        // Record the cancel through the canonical GUARDED writer, THEN delete
        // the task. `journal_cancel_terminals` (a) SKIPS if a terminal already
        // exists for the color, which closes the race where the worker wrote
        // `ExecutionCompleted`/`Failed` and then the pod died before its task
        // flipped to `complete` (a bare `ExecutionCancelled` would stack a
        // second, contradictory terminal); and (b) writes `NodeCancelled` per
        // still-running node so node UI state is not left stuck on "running".
        // The task row is the durable retry handle: on failure we `continue`
        // WITHOUT deleting, so the next tick re-finds this orphan and retries
        // (the writer is idempotent). A per-orphan failure never strands the
        // others. The write uses NULL pod_name, bypassing the fencing trigger.
        if let Err(e) = crate::api::execution::journal_cancel_terminals(
            &state,
            color,
            "worker pod died before the live execution completed; the caller connection was \
             routed to that pod and is gone, so the run cannot resume elsewhere",
        )
        .await
        {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                color = %color, error = %e,
                "failed to record cancel terminal for orphan; task kept, will retry next tick"
            );
            continue;
        }
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            color = %color,
            "live execution orphaned by a dead pod; recorded ExecutionCancelled (caller is gone)"
        );
        if let Err(e) = weft_task_store::tasks::delete_task(&state.pg_pool, orphan.task_id).await {
            // The cancel is durably recorded, so a leftover task only means a
            // harmless retry next tick (re-record is a no-op).
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                color = %color, error = %e,
                "failed to delete cancelled orphan task; harmless, next tick retries"
            );
        }
    }
    Ok(())
}

/// Worker-pod GC. Every 30s, delete the k8s Pod object for
/// worker_pod rows in a terminal status (`done` from an idle
/// self-exit, `dead` from the stale-heartbeat reaper above) older
/// than the grace window, then drop the row. Driven off the
/// `worker_pod` table (the single source of truth), NOT a
/// `kubectl get`: the namespace comes from the row itself
/// (`row.namespace`), so there is no namespace-mapper guessing,
/// and the whole thing fakes through `state.kube` for tests.
///
/// `dead` rows were already `kill_pod`'d by `sweep_worker_pods`
/// (which deletes the Pod), but a kill that failed there leaves the
/// row `dead` with the Pod still around; this GC retries the delete
/// idempotently and finally drops the row.
async fn sweep_terminal_worker_pods(state: DispatcherState) -> anyhow::Result<()> {
    let threshold = crate::lease::now_unix() - TERMINAL_POD_GRACE_SECS;
    let terminal = weft_task_store::list_terminal(&state.pg_pool, threshold).await?;
    for row in terminal {
        // kubectl delete via the shared trait. `--wait=false`
        // (no_wait): the GC loop shouldn't block on a slow delete.
        // Idempotent (--ignore-not-found under the hood), so a Pod
        // already gone (e.g. clean-exit pod k8s never recreated) is
        // fine; we still drop the row.
        if let Err(e) = state
            .kube
            .delete_named(
                &row.namespace,
                "pod",
                &row.pod_name,
                weft_platform_traits::DeleteOpts::no_wait(),
            )
            .await
        {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                pod = %row.pod_name,
                namespace = %row.namespace,
                error = %e,
                "terminal worker pod delete failed; will retry next tick (row kept)"
            );
            continue;
        }
        weft_task_store::delete_row(&state.pg_pool, &row.pod_name).await?;
    }
    Ok(())
}

/// Tasks-table retention sweep. Once an hour, delete terminal
/// rows older than the retention window so the table stays small.
async fn sweep_tasks(state: DispatcherState) -> anyhow::Result<()> {
    let n = weft_task_store::tasks::sweep_terminal(&state.pg_pool).await?;
    if n > 0 {
        tracing::info!(
            target: "weft_dispatcher::reaper",
            swept = n,
            "tasks sweeper retired terminal rows"
        );
    }
    Ok(())
}

/// Listener reaper. Every 10s, reap every pooled listener pod holding
/// ZERO signals (per-pod idle reap). `ListenerPool::reap_idle` scans
/// the `listener_pod` registry, claims each idle pod (ownership + lease
/// so two dispatchers do not both reap one), tears it down, and deletes
/// its registry row. A pod holding even one signal is kept.
async fn sweep_listeners(state: DispatcherState) -> anyhow::Result<()> {
    state
        .listeners
        .reap_idle(
            state.listener_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
        )
        .await
}

/// Listener scale-DOWN. Every 60s (slower than the idle reap so the two
/// do not fight), drain AT MOST ONE pod whose signals fit on the other
/// non-saturated pods' headroom: re-place its signals elsewhere, then
/// reap the emptied pod. The twin of spawn-on-saturation; the idle reap
/// only catches already-empty pods, this actively consolidates a
/// partially-loaded pool when load dropped.
async fn sweep_listener_scaledown(state: DispatcherState) -> anyhow::Result<()> {
    state
        .listeners
        .drain_one(
            state.listener_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
        )
        .await
}

/// Supervisor idle reaper. Every 30s, reap every pooled supervisor pod
/// that owns ZERO projects (the supervisor twin of the listener idle
/// reaper). A pod owning even one project is reconciling that infra and
/// is kept; when no infra exists globally, every supervisor owns nothing
/// and the pool drains to zero (cold-start is covered by
/// `ensure_at_least_one` on the next sync). Ownership, not a separate
/// node-count check, is what keeps a busy supervisor alive: a project's
/// `infra_owner` lease IS the "this pod has work" signal.
async fn sweep_supervisors(state: DispatcherState) -> anyhow::Result<()> {
    state
        .supervisors
        .reap_idle(
            state.supervisor_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
        )
        .await
}

/// Supervisor scale-DOWN. Every 60s (slower than the idle reap so the
/// two do not fight), drain AT MOST ONE supervisor whose owned projects
/// fit on the other pods' headroom: release its project leases for the
/// survivors' claim loops to adopt, then reap the emptied pod. The twin
/// of spawn-on-saturation; the idle reap only catches pods that already
/// own nothing, this actively consolidates a partially-loaded pool when
/// load dropped.
async fn sweep_supervisor_scaledown(state: DispatcherState) -> anyhow::Result<()> {
    state
        .supervisors
        .drain_one(
            state.supervisor_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
        )
        .await
}

/// Worker scale-DOWN. Per project running more than one worker, ask the
/// shared `plan_memory_scaledown` whether the pool has excess memory
/// headroom, and if so mark ONE worker draining. A draining worker stops
/// being chosen for NEW executions (`pick_admittable_for_project` and
/// cold_start both skip draining pods) while its in-flight executions
/// finish; it then idle-exits itself via the normal `mark_done_if_idle`
/// CAS and the worker-pod reaper GCs it.
///
/// Unlike the supervisor drain there is NO lease release and NO work
/// hand-off: a running execution is bound to the worker driving it (one
/// journal stream per color), so consolidation here is purely "stop
/// admitting new work to the most-drainable pod and let it empty." That
/// is also why we mark at most one per project per tick: draining frees
/// memory only as the pod's executions complete, so the planner should
/// re-measure real pressure before shedding the next.
///
/// Serialized cluster-wide by the shared scale-down advisory lock so two
/// dispatchers don't both drain workers of the same project; a sibling
/// holding the lock simply skips this tick.
///
/// Also emits a breadcrumb for every pod currently draining (elapsed +
/// remaining in-flight work). A drain has no deadline (a live execution
/// may legitimately run for hours/days, and we never time out a user's
/// program), so a pod can sit draining a long time; the breadcrumb keeps
/// that legible instead of silent, per the long-running-operation rule.
async fn sweep_worker_scaledown(state: DispatcherState) -> anyhow::Result<()> {
    crate::lease::with_scaledown_lock(&state.pg_pool, "worker", || async {
        // Breadcrumb pass: surface every still-draining pod. Runs under
        // the same lock (one dispatcher logs per tick, no duplicate
        // spam) and before planning so a pod that has been draining
        // since a prior tick is reported even if no new drain happens.
        let now = crate::lease::now_unix();
        for (pod, project, drained_at, in_flight) in
            weft_task_store::worker_pod::draining_breadcrumbs(&state.pg_pool).await?
        {
            tracing::info!(
                target: "weft_dispatcher::reaper",
                project = %project,
                pod = %pod,
                draining_for_secs = now.saturating_sub(drained_at),
                in_flight_tasks = in_flight,
                "worker still draining (no deadline; will idle-exit when its in-flight work finishes)"
            );
        }
        let projects =
            weft_task_store::worker_pod::projects_with_multiple_workers(&state.pg_pool).await?;
        for project_id in projects {
            let loads: Vec<weft_platform_traits::PoolPodLoad> =
                weft_task_store::worker_pod::pod_loads_for_project(&state.pg_pool, &project_id)
                    .await?
                    .into_iter()
                    .map(|(pod_name, mem_pressure)| weft_platform_traits::PoolPodLoad {
                        pod_name,
                        mem_pressure,
                    })
                    .collect();
            let Some(target) = weft_platform_traits::plan_memory_scaledown(
                &loads,
                weft_platform_traits::SATURATION_MEM_FRACTION,
            ) else {
                continue;
            };
            weft_task_store::worker_pod::set_draining(&state.pg_pool, &target).await?;
            tracing::info!(
                target: "weft_dispatcher::reaper",
                project = %project_id,
                drain_target = %target,
                "worker scale-down: marked worker draining; it will finish in-flight work and idle-exit"
            );
        }
        Ok(())
    })
    .await
    .map(|_| ())
}
