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

/// The per-tenant infra-supervisor Deployment's name and its
/// `weft.dev/role` label value, as minted in `tenant_namespace.rs`.
/// The supervisor reaper references both so its label selector and
/// its name filter cannot drift from each other. NOT the bare `infra`
/// role, which tags the user's infra NODES in project namespaces.
/// SYNC: SUPERVISOR_NAME/SUPERVISOR_ROLE <-> crate::tenant_namespace (YAML literals: name `weft-infra-supervisor`, `weft.dev/role: infra-supervisor`)
pub(crate) const SUPERVISOR_NAME: &str = "weft-infra-supervisor";
pub(crate) const SUPERVISOR_ROLE: &str = "infra-supervisor";

/// Spawn every reaper. Returns immediately; the reapers run for the
/// lifetime of the process.
pub fn spawn_all(state: DispatcherState) {
    spawn_loop(state.clone(), Duration::from_secs(30), "worker_pod", sweep_worker_pods);
    spawn_loop(state.clone(), Duration::from_secs(30), "worker_pod_gc", sweep_terminal_worker_pods);
    spawn_loop(state.clone(), Duration::from_secs(15), "orphaned_tasks", sweep_orphaned_tasks);
    spawn_loop(state.clone(), Duration::from_secs(3600), "tasks", sweep_tasks);
    spawn_loop(state.clone(), Duration::from_secs(10), "listener", sweep_listeners);
    spawn_loop(state.clone(), Duration::from_secs(60), "supervisor", sweep_supervisors);
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

/// Worker-pod reaper. Once every 30s, scan for `alive` rows with
/// stale heartbeats. Mark them `dead` (which makes the fencing
/// trigger reject any further journal writes from them) and
/// `kubectl delete` the Pod. Pending tasks for the project pool
/// remain claimable: the cold-start trigger spawns a fresh Pod
/// when there's pending work and no live Pod.
async fn sweep_worker_pods(state: DispatcherState) -> anyhow::Result<()> {
    let threshold = crate::lease::now_unix() - weft_task_store::worker_pod::HEARTBEAT_STALE_SECS;
    let stale = weft_task_store::worker_pod::list_stale(&state.pg_pool, threshold).await?;
    for row in stale {
        tracing::warn!(
            target: "weft_dispatcher::reaper",
            project = %row.project_id,
            pod = %row.pod_name,
            last_heartbeat = row.last_heartbeat_unix,
            "marking stale worker pod dead"
        );
        weft_task_store::worker_pod::mark_dead(&state.pg_pool, &row.pod_name).await?;
        // kubectl delete: log loudly on error. A failed kill leaves
        // the pod alive in k8s while our DB says dead, which means
        // a stale pod can keep running. Not fatal to the sweep
        // (the next tick retries), but never silent.
        if let Err(e) = state
            .workers
            .kill_pod(row.pod_name.clone(), row.namespace.clone())
            .await
        {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                pod = %row.pod_name,
                error = %e,
                "kill_pod failed for stale worker; pod may survive in k8s until next sweep"
            );
        }
        // NOTE: this loop only marks the pod dead + deletes the k8s Pod.
        // Recovering the pod's stranded tasks is NOT done here (a pod is
        // marked dead from several places, and a marked-dead pod is never
        // re-listed by `list_stale`, so doing recovery here would run at most
        // once and strand anything that failed). Task recovery is its own
        // self-healing, task-driven sweep: `sweep_orphaned_tasks`.
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

/// Listener reaper. Every 10s, scan `tenant_listener` rows and
/// reap any whose listener is idle. "Idle" is decided by
/// `ListenerPool::try_reap_if_idle` under an EXCLUSIVE per-tenant
/// OP-lock: the lock fences out concurrent `with_listener` calls,
/// and the signal table doubles as the "is the listener semantically
/// needed" check (zero rows for the tenant => safe to kill).
async fn sweep_listeners(state: DispatcherState) -> anyhow::Result<()> {
    let rows = crate::lease::list_tenant_listener_rows(&state.pg_pool).await?;
    for row in rows {
        // Skip rows already mid-teardown on another Pod; their owner
        // will finish the transition or its lease will lapse and we
        // pick them up next sweep.
        if row.state == "stopping" {
            continue;
        }
        let tenant = crate::tenant::TenantId(row.tenant_id.clone());
        match state
            .listeners
            .try_reap_if_idle(
                &tenant,
                &row.namespace,
                state.listener_backend.as_ref(),
                &state.pg_pool,
                state.pod_id.as_str(),
            )
            .await
        {
            Ok(true) => tracing::info!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant,
                namespace = %row.namespace,
                "reaped idle listener"
            ),
            Ok(false) => {} // operation in flight or signals present
            Err(e) => tracing::warn!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant,
                error = %e,
                "listener kill failed"
            ),
        }
    }
    Ok(())
}

/// Supervisor reaper. Once every 60s, find tenants whose supervisor
/// Deployment exists but has zero work to do: no `infra_node` rows
/// AND no pending/claimed `infra_lifecycle_command` rows. Scale the
/// Deployment to 0 so an idle tenant doesn't hold a Pod open. Next
/// sync re-applies the Deployment (idempotent) and the supervisor
/// is back. Mirrors the listener reaper pattern but for supervisor.
async fn sweep_supervisors(state: DispatcherState) -> anyhow::Result<()> {
    // Find every tenant that has at least one project registered.
    // The supervisor Deployment lives in `wm-<tenant>`; we only
    // consider tenants the dispatcher knows about. `project_namespace`
    // is NOT NULL in the schema, so every registered row is in scope.
    let tenants: Vec<String> =
        // Tenant identity is owned by `weft_namespace_tenant`
        // (the registry SoT). Walking project rows would miss
        // tenants whose supervisor is alive but whose projects
        // have all been `weft rm`'d, leaving an idle supervisor
        // pinned to 1 replica forever.
        sqlx::query_scalar("SELECT DISTINCT tenant_id FROM weft_namespace_tenant")
            .fetch_all(&state.pg_pool)
            .await?;

    for tenant_id in tenants {
        if let Err(e) = sweep_one_tenant(&state, &tenant_id).await {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant_id,
                error = %e,
                "sweep_one_tenant errored; continuing with next tenant"
            );
        }
    }
    Ok(())
}

/// Decide whether to scale tenant's supervisor to 0, under a
/// per-tenant xact-scoped advisory lock. The xact wraps:
///   1. take `pg_try_advisory_xact_lock` keyed on the per-tenant
///      supervisor-coord scope :
///      non-blocking; if sync is currently in its critical section
///      we skip this tenant for the cycle;
///   2. read three idle signals: any project's
///      `sync_in_flight_until_unix` in the future, any infra_node
///      rows for this tenant's projects, any pending
///      infra_lifecycle_command rows;
///   3. if all idle, kubectl scale the deployment to 0;
///   4. COMMIT (releases the lock).
///
/// The lock stays held across the kubectl call so a sync that
/// arrives concurrently waits behind us. xact-scoped means the
/// lock auto-releases on commit, no session-leak back to the pool.
async fn sweep_one_tenant(state: &DispatcherState, tenant_id: &str) -> anyhow::Result<()> {
    let mut tx = state.pg_pool.begin().await?;
    let lock_key = crate::lease::advisory_key(
        crate::lease::SUPERVISOR_COORD_DOMAIN,
        tenant_id,
    );
    let got_lock: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(lock_key)
        .fetch_one(&mut *tx)
        .await?;
    if !got_lock {
        // Sync is touching this tenant right now. Drop the tx;
        // next sweep cycle retries.
        return Ok(());
    }

    // All three idle checks (sentinel, node_count, pending_count)
    // run inside the same xact under the advisory lock so a sync
    // concurrent with this sweep can't slip a sentinel + first
    // command in between two reads on different connections. The
    // sentinel inline-query lives here (not behind ProjectStoreOps)
    // because the trait method takes its own pool connection,
    // breaking snapshot consistency.
    let now = crate::lease::now_unix();
    let sync_in_flight: bool = sqlx::query_scalar(
        "SELECT EXISTS ( \
            SELECT 1 FROM project \
            WHERE tenant_id = $1 \
              AND sync_in_flight_until_unix > $2 \
         )",
    )
    .bind(tenant_id)
    .bind(now)
    .fetch_one(&mut *tx)
    .await?;
    if sync_in_flight {
        return Ok(());
    }
    let node_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM infra_node WHERE project_id IN \
         (SELECT id::TEXT FROM project WHERE tenant_id = $1)",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await?;
    if node_count > 0 {
        return Ok(());
    }
    let pending_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::bigint FROM infra_lifecycle_command \
         WHERE tenant_id = $1 AND completed_at_unix IS NULL",
    )
    .bind(tenant_id)
    .fetch_one(&mut *tx)
    .await?;
    if pending_count > 0 {
        return Ok(());
    }

    // Idle. Scale the supervisor Deployment down. The next sync
    // re-applies it and scales back up. We use `scale` rather
    // than `delete` so the Deployment + its NetworkPolicies stay
    // in the cluster; just the Pod is freed.
    //
    // The tx stays open across kubectl so a sync that arrives
    // concurrently waits behind us. The tx commit below releases
    // the lock.
    let namespace = state
        .namespace_mapper
        .namespace_for(&crate::tenant::TenantId(tenant_id.to_string()));
    // Route through `state.kube` (the shared `KubeClient` trait
    // also used by the supervisor crate). The dispatcher no longer
    // forks `tokio::process::Command::new("kubectl")` directly;
    // tests fake this through `FakeKube`.
    // Disposition rule for this fn: COMMIT only after a successful
    // scale (that's what holds the advisory lock across the kubectl
    // so a concurrent sync waits behind us). Every skip path drops
    // `tx` (rollback) instead, since nothing was written and the
    // lock should release immediately. NotFound/Errored are skips.
    use weft_platform_traits::WorkloadKind;
    // Read the supervisor's current replica state. Only scale (and log a reap)
    // when it is actually UP (desired > 0). Re-scaling an already-zero
    // deployment every 60s is a pointless kubectl call AND a lie in the log
    // ("reaped" when nothing changed); gating on the live replica count makes
    // the sweep a true no-op once the supervisor is down. The selector and the
    // name filter both reference SUPERVISOR_* so they cannot drift apart, and
    // the selector value MUST match the label the supervisor Deployment is
    // minted with in tenant_namespace.rs (`weft.dev/role: infra-supervisor`);
    // the bare `infra` role belongs to the user's infra NODES in project
    // namespaces, a different concept that would match nothing here.
    let states = match state
        .kube
        .list_replica_state(&namespace, &format!("weft.dev/role={SUPERVISOR_ROLE}"))
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant_id,
                error = %e,
                "list supervisor replica state errored; skipping this tenant"
            );
            return Ok(());
        }
    };
    if !supervisor_needs_reap(&states) {
        // Deployment absent (never provisioned / already removed) or already
        // scaled to zero: true no-op, no scale call, no log. Drop `tx`.
        return Ok(());
    }
    match state
        .kube
        .scale_workload(&namespace, WorkloadKind::Deployment, SUPERVISOR_NAME, 0)
        .await
    {
        Err(e) => {
            // Errored is a skip, like every other early return: drop `tx`
            // (rollback) so the advisory lock releases and the next sweep
            // retries. Committing here would persist any future write added
            // to this tx despite the scale having failed.
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant_id,
                error = %e,
                "supervisor scale-to-0 failed"
            );
            Ok(())
        }
        Ok(()) => {
            // Commit only after a successful scale (holds the lock across the
            // kubectl call so a concurrent sync waits behind us).
            tx.commit().await?;
            tracing::info!(
                target: "weft_dispatcher::reaper",
                tenant = %tenant_id,
                "reaped idle infra-supervisor (scaled to 0)"
            );
            Ok(())
        }
    }
}

/// Pure decision: given the replica states returned for the
/// supervisor's role selector, should the reaper scale the supervisor
/// Deployment down? True only when the supervisor Deployment is
/// present AND currently up (`desired > 0`). Absent (never
/// provisioned / already removed) or already at zero → false (no-op).
/// Extracted from `sweep_one_tenant` so the workload-selection +
/// desired-gate logic (where a wrong selector silently matched
/// nothing) is unit-testable without a Postgres rig.
fn supervisor_needs_reap(states: &[weft_platform_traits::WorkloadReplicaState]) -> bool {
    use weft_platform_traits::WorkloadKind;
    states
        .iter()
        .find(|s| s.name == SUPERVISOR_NAME && s.kind == WorkloadKind::Deployment)
        .is_some_and(|s| s.desired > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use weft_platform_traits::{WorkloadKind, WorkloadReplicaState};

    fn supervisor(desired: i64) -> WorkloadReplicaState {
        let mut labels = HashMap::new();
        labels.insert("weft.dev/role".into(), SUPERVISOR_ROLE.into());
        WorkloadReplicaState {
            kind: WorkloadKind::Deployment,
            name: SUPERVISOR_NAME.into(),
            namespace: "wm-alice".into(),
            desired,
            ready: desired,
            labels,
        }
    }

    #[test]
    fn reaps_when_up() {
        assert!(supervisor_needs_reap(&[supervisor(1)]));
    }

    #[test]
    fn no_op_when_already_zero() {
        assert!(!supervisor_needs_reap(&[supervisor(0)]));
    }

    #[test]
    fn no_op_when_absent() {
        assert!(!supervisor_needs_reap(&[]));
    }

    /// The selector that feeds this decision must return the supervisor by its
    /// real role label. A decoy workload under a DIFFERENT name (e.g. a user
    /// infra node that leaked into the same list) must never be mistaken for
    /// the supervisor: only the exact `SUPERVISOR_NAME` Deployment counts.
    #[test]
    fn ignores_non_supervisor_workloads() {
        let mut decoy = supervisor(1);
        decoy.name = "some-user-infra-node".into();
        assert!(!supervisor_needs_reap(&[decoy]));
    }
}
