//! Infra lifecycle endpoints.
//!
//! Three project-scoped verbs (Start / Restart / Upgrade all map to
//! the same `/sync` endpoint), two project-scoped destroy verbs
//! (`/stop` and `/terminate`), and two per-node destroy verbs for
//! partial-state recovery.
//!
//! `/sync` runs an `InfraSetup` subworkflow exec: the worker walks
//! every `requires_infra` node + its upstream closure; each infra
//! node calls `Node::provision_infra`. The engine then makes a local
//! skip / fresh / replace decision (comparing the compiled spec
//! hash against the broker's stored `infra_node.applied_spec_hash`)
//! and, when not Skip, enqueues an `Apply` lifecycle command. The
//! tenant's supervisor picks the command up, runs kubectl, writes
//! the updated `infra_node` row. The hash-match Skip path makes
//! Restart cheap and Upgrade selective.
//!
//! `/stop` and `/terminate` enqueue an `infra_lifecycle_command` row
//! for the tenant's supervisor pod to claim and execute. Per-node
//! variants scope to a single (project, node).

use std::collections::BTreeMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::infra_lifecycle_command::{self, InfraLifecycleVerb, RunningPolicy};
use crate::infra_node::{self, InfraNodeRow};
use crate::project_namespace;
use crate::state::DispatcherState;

// =================================================================
// Sync request (Start / Restart / Upgrade)
// =================================================================

// SYNC: SyncRequest body keys <-> crates/weft-cli/src/commands/infra.rs (the
// hand-built sync body map). Every field here is `#[serde(default)]`, so a
// renamed key would silently deserialize to its default instead of failing:
// change both sides together.
#[derive(Debug, Default, Deserialize)]
pub struct SyncRequest {
    #[serde(default, rename = "binaryHash")]
    pub binary_hash: Option<String>,
    #[serde(default, rename = "definitionHash")]
    pub definition_hash: Option<String>,
    #[serde(default, rename = "infraHash")]
    pub infra_hash: Option<String>,
    /// Per-(node, image_name) hash map. Shape:
    /// `{ "<node_id>": { "<image_name>": "<hash_tag>" } }`.
    /// The supervisor reads it (executing a claimed infra lifecycle command)
    /// to resolve `Image::Local { name }` references to concrete docker tags.
    #[serde(default, rename = "imageHashes")]
    pub image_hashes: BTreeMap<String, BTreeMap<String, String>>,
    /// UPGRADE mode: cycle the running infra onto the current specs
    /// (deactivate per `triggerDeactivation` when Active, run the
    /// STOP leg, then the normal apply). One backend-side definition
    /// of "upgrade = stop then start" that every client gets from a
    /// single POST. `false` (a plain START) only brings down units
    /// up and never deactivates.
    #[serde(default)]
    pub upgrade: bool,
    /// How to deactivate triggers before an UPGRADE of an Active
    /// project (required then, 412 otherwise; rejected on a plain
    /// start, which never deactivates). Same `DeactivateSpec` shape
    /// as the standalone `/deactivate` endpoint, so clients reuse
    /// one picker UI.
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<weft_broker_client::protocol::DeactivateSpec>,
    /// How the worker reconciliation inside sync treats RUNNING
    /// executions when a worker must be replaced (stale image, or its
    /// namespace no longer matches placement after infra appeared /
    /// went away). `wait` (the default) drains the doomed workers (no
    /// new admissions; in-flight work finishes) up to
    /// `drainTimeoutSecs`, then replaces; `cancel` cancels the running
    /// executions first. Never a silent kill.
    #[serde(default, rename = "runningPolicy")]
    pub running_policy: Option<RunningPolicy>,
    /// Cap on the `wait` drain, in seconds; defaults to
    /// `DEFAULT_DRAIN_TIMEOUT_SECS`. The user's "wait this long as a
    /// courtesy, then proceed" choice, picked alongside the policy.
    #[serde(default, rename = "drainTimeoutSecs")]
    pub drain_timeout_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct SyncResponse {
    pub nodes: Vec<InfraStatusEntry>,
}

/// Return shape for verbs that asynchronously enqueue a lifecycle
/// command. The body intentionally does NOT contain `nodes`: the
/// command hasn't been claimed yet, so any snapshot would be the
/// pre-action state, misleading the caller. Clients poll `/status`
/// (or subscribe to the event SSE) for the post-action shape.
#[derive(Debug, Serialize)]
pub struct LifecycleCommandIssued {
    pub command_id: i64,
}

#[derive(Debug, Serialize)]
pub struct InfraStatusEntry {
    pub node_id: String,
    pub status: String,
    pub endpoint_url: Option<String>,
    pub failure_stage: Option<String>,
    pub failure_message: Option<String>,
}

/// Body for `/infra/stop` and `/infra/terminate`. Carries the
/// trigger-deactivation choice when the project is Active. The verb
/// itself encodes the infra-side intent; the trigger side is the
/// user's choice (same picker as the standalone Deactivate verb).
#[derive(Debug, Default, Deserialize)]
pub struct StopRequest {
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<weft_broker_client::protocol::DeactivateSpec>,
    /// Cap on the supervisor's `wait` drain (Stop waits for running
    /// executions before scaling down), in seconds; defaults to
    /// `DEFAULT_DRAIN_TIMEOUT_SECS`.
    #[serde(default, rename = "drainTimeoutSecs")]
    pub drain_timeout_secs: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PerNodeRequest {
    #[serde(default, rename = "runningPolicy")]
    pub running_policy: Option<RunningPolicy>,
    /// Stop only: force scale-to-zero every unit, ignoring `on_stop`.
    /// Lets the user take down a unit that would normally stay up
    /// (NoOp) so they can update it on the next start. Ignored by
    /// terminate (terminate already removes everything).
    #[serde(default)]
    pub force: bool,
    /// Cap on the `wait` drain, in seconds; defaults to
    /// `DEFAULT_DRAIN_TIMEOUT_SECS`.
    #[serde(default, rename = "drainTimeoutSecs")]
    pub drain_timeout_secs: Option<u64>,
}

// =================================================================
// Handlers
// =================================================================


pub async fn sync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    body: Option<Json<SyncRequest>>,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();

    // No sync-in-flight sentinel: the supervisor pool reaps a supervisor
    // by OWNERSHIP (a pod owning zero projects), not by a global idle
    // scan that a sync would need to block. A project being synced is
    // owned by its supervisor (non-zero, so never reaped), and
    // `ensure_supervisor` below guarantees a live pod exists; there is no
    // reaper race for a sentinel to prevent.
    sync_inner(state, id, body).await
}

async fn sync_inner(
    state: DispatcherState,
    id: uuid::Uuid,
    body: SyncRequest,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    let project_id = id.to_string();

    // The running-hash trio + infra image-tag map is written LATER, only after
    // every reject gate below has passed AND the upgrade stop leg (if any) has
    // succeeded, i.e. once this sync has actually committed to applying. Writing
    // them up here (before the gates) stamped a project as "running the new build"
    // even when a gate rejected the sync or the stop leg failed, so drift detection
    // then read desired==running and reported "up to date" while the old worker
    // still ran. See the write just before `ensure_project_namespace_if_infra`.

    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if matches!(
        lifecycle.status,
        crate::project_store::ProjectStatus::Activating
            | crate::project_store::ProjectStatus::Deactivating
    ) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!(
                "project is {}; wait or cancel before syncing infra",
                lifecycle.status.as_str()
            ),
        ));
    }
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .unwrap_or(crate::project_store::ProjectTransition::None);
    if transition.is_building() {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "project is {}; wait for the build to finish or cancel it before syncing infra",
                transition.as_str()
            ),
        ));
    }
    // Fast reject before any side effect; re-checked under the lock
    // below (the locked re-check is the race-safe one).
    if crate::api::project::infra_setup_in_flight(&state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}")))?
    {
        return Err((
            StatusCode::CONFLICT,
            "an infra sync is already in flight for this project; wait for it to \
             finish or cancel it (`/infra/cancel`)"
                .into(),
        ));
    }

    // Verb auto-build, BEFORE the transition lock: a build
    // takes minutes and must never run while the per-project advisory
    // lock pins a pool connection. The `coherent_definition` call
    // inside the locked `start_infra_setup` below then cache-hits
    // (nothing left to build) and stays lock-cheap.
    crate::transition::ensure_built_gated(&state, id).await?;

    // Enforce against the same reconciliation the action bar renders:
    // the two faces of sync are distinct table verbs. A plain START is
    // `infra_start` (offered when infra is down/stopped/partial, never
    // when everything already runs); an UPGRADE is `infra_upgrade`
    // (re-cycle running infra onto current specs).
    crate::api::project::require_action(
        &state,
        id,
        if body.upgrade { &["infra_upgrade"] } else { &["infra_start"] },
    )
    .await?;

    // A plain START never deactivates: an active project's triggers
    // stay live while infra comes up (fires whose subgraph touches the
    // not-yet-running infra fail loudly at the node; fires that don't
    // touch it keep working, which is the continuity an active project
    // is owed). An UPGRADE disturbs live infra the triggers may depend
    // on, so on an Active project it REQUIRES the user's deactivation
    // choice and applies it before the stop leg.
    let was_active = matches!(lifecycle.status, crate::project_store::ProjectStatus::Active);
    if body.trigger_deactivation.is_some() && !body.upgrade {
        return Err((
            StatusCode::BAD_REQUEST,
            "triggerDeactivation only applies to an upgrade (a plain infra start never \
             deactivates); pass upgrade=true or drop the field"
                .into(),
        ));
    }
    if body.upgrade && was_active {
        let Some(deactivation) = body.trigger_deactivation.as_ref() else {
            return Err((
                StatusCode::PRECONDITION_REQUIRED,
                "project is active; an upgrade requires triggerDeactivation \
                 { mode, runningPolicy, graceMinutes? } (same picker as the standalone \
                 Deactivate verb)"
                    .into(),
            ));
        };
        crate::api::project::execute_trigger_deactivation(&state, id, deactivation).await?;
    }

    // Lazy supervisor spawn. The supervisor is what owns kubectl
    // for user infra; sync is the first verb that needs it (orphan
    // reap and Apply commands both depend on a live supervisor).
    // Idempotent: applies the same Deployment manifest every time;
    // k8s no-ops if already present. MUST land before any code path
    // that enqueues a lifecycle command (orphan reap, start_infra_setup).
    ensure_supervisor(&state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    // Orphan reap. An `infra_node` row whose `node_id` isn't in the
    // current project source (or no longer carries `requires_infra`)
    // is stale: the user removed it from .weft but the supervisor
    // still has Pods/Services/PVCs deployed for it. Reap before
    // running the subworkflow so the new shape is the only thing
    // alive afterwards. Hard error: leaking stale infra is silently
    // worse than asking the user to retry.
    reap_orphans(&state, id).await?;

    // UPGRADE stop leg: the apply path leaves up units frozen, so to
    // cycle a running unit onto a new spec the sync stops first
    // (respecting each unit's on_stop) and BLOCKS until the stop
    // settles, exactly like it blocks on the InfraSetup below; the
    // start half then recreates the down units. Stop drains in-flight
    // infra executions (RunningPolicy::Wait) up to the caller's cap;
    // the wait here adds a generous margin over that cap so a
    // legitimately slow drain is never misread as a wedged supervisor.
    if body.upgrade {
        let drain_timeout_secs = body
            .drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
        let command_id = issue_lifecycle_ensuring_supervisor(
            &state,
            &project_id,
            None,
            InfraLifecycleVerb::Stop,
            RunningPolicy::Wait,
            false,
            drain_timeout_secs,
        )
        .await?;
        let wait = std::time::Duration::from_secs(drain_timeout_secs + 300);
        match crate::infra_lifecycle_command::wait_for_command(&state.pg_pool, command_id, wait)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("wait for stop leg: {e}")))?
        {
            crate::infra_lifecycle_command::WaitOutcome::Succeeded => {}
            crate::infra_lifecycle_command::WaitOutcome::Failed { error } => {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!("upgrade stop leg failed: {error}; infra left as-is, retry or act \
                             per node (`weft infra status`)"),
                ));
            }
            crate::infra_lifecycle_command::WaitOutcome::Cancelled { reason } => {
                return Err((
                    StatusCode::CONFLICT,
                    format!("upgrade stop leg cancelled ({reason}); infra left as-is"),
                ));
            }
            crate::infra_lifecycle_command::WaitOutcome::Timeout => {
                return Err((
                    StatusCode::GATEWAY_TIMEOUT,
                    format!(
                        "upgrade stop leg did not complete within {}s (drain cap {}s + margin); \
                         the stop command is still in flight: wait and retry the upgrade, or \
                         cancel it (`/infra/cancel`)",
                        wait.as_secs(),
                        drain_timeout_secs
                    ),
                ));
            }
        }
    }

    // There is NO worker "move": a worker's namespace is fixed at
    // spawn; a placement change is drain-or-cancel-gated
    // kill-then-respawn (`reconcile_worker`). Infra Pods are
    // reachable ONLY from inside the project namespace (the
    // namespace's ingress policy), so EVERY worker that may talk to
    // infra, including the InfraSetup provisioning execution, must
    // run there; placement anchors on the namespace existing, which
    // is why the namespace is created FIRST. Sequencing:
    //
    //   1. ensure_project_namespace_if_infra: create the project
    //      namespace + RBAC before any infra Pod is applied AND
    //      before the reconcile, so the placement resolver already
    //      answers "project namespace" for everything that follows.
    //      Idempotent, so safe outside the lock.
    //   2. reconcile_worker (pre-apply, OUTSIDE the lock: a Wait-
    //      policy drain can sit for minutes and must never pin the
    //      lock's connection): replace a stale-image or misplaced
    //      worker so the InfraSetup exec never runs on an old binary
    //      or on a shared-pool pod the infra network wall would
    //      block. Both halves of the replacement converge under
    //      concurrency (mark_dead is idempotent, the spawn task is
    //      dedup-keyed).
    //   3. UNDER the per-project transition lock (short; two
    //      concurrent syncs serialize here and the second is rejected
    //      by the in-flight re-check):
    //      a. re-check no InfraSetup execution is in flight;
    //      b. start_infra_setup: journal the InfraSetup color (the
    //         durable "sync in flight" state) + enqueue.
    //   4. OUTSIDE the lock: await the InfraSetup execution (user
    //      code upstream of infra nodes may legitimately be slow;
    //      never hold a lock across it).
    //   5. reconcile_worker (post-apply, outside the lock again):
    //      placement may have changed (a no-longer-infra source's
    //      namespace is about to go); kill-then-respawn the worker
    //      into the right namespace, drained/cancelled per
    //      `runningPolicy`.
    //   6. UNDER the lock: teardown_project_namespace_if_no_infra,
    //      deleting the (now worker-less) namespace + its registry row
    //      when the project no longer has ANY infra state.
    let running_policy = body.running_policy.unwrap_or(RunningPolicy::Wait);
    let drain_timeout_secs = body
        .drain_timeout_secs
        .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);

    // Advance the running-hash trio + infra image-tag map NOW: every reject gate
    // has passed and the upgrade stop leg (if any) succeeded, so from here the sync
    // is committed to applying the new spec. The running pointers must reflect
    // committed reality, never intent, or drift detection lies. ONE ATOMIC write
    // for the trio AND the complete tag map: separate statements opened a window
    // where a crash (or a sibling Pod's /run between them) saw a new binary hash
    // paired with an old definition hash, OR a project stamped runnable with its
    // infra tags absent/half-written (a supervisor apply then resolves
    // `Image::Local { name }` to nothing and dangles). The whole tag map is
    // REPLACED (this sync recomputed every node's tags). The supervisor's
    // apply-hash compute reads the trio on its next tick to decide
    // skip/fresh/replace, and `reconcile_worker` below reads the binary hash to
    // decide whether to kill the running pod.
    let infra_image_tags: crate::project_store::InfraImageTags = body
        .image_hashes
        .iter()
        .map(|(node_id, tags)| {
            (
                node_id.clone(),
                tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            )
        })
        .collect();
    state
        .projects
        .set_running_hashes(
            id,
            body.binary_hash.as_deref(),
            body.definition_hash.as_deref(),
            body.infra_hash.as_deref(),
            Some(&infra_image_tags),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_running_hashes: {e}")))?;

    ensure_project_namespace_if_infra(&state, id).await?;
    crate::api::project::reconcile_worker(&state, &project_id, running_policy, drain_timeout_secs)
        .await?;
    let started: Result<Option<crate::api::project::InfraSetupRun>, (StatusCode, String)> =
        crate::lease::with_project_transition_lock(&state.pg_pool, &project_id, || async {
            if crate::api::project::infra_setup_in_flight(&state, &project_id).await? {
                return Ok(Err((
                    StatusCode::CONFLICT,
                    "an infra sync is already in flight for this project; wait for it \
                     to finish or cancel it (`/infra/cancel`)"
                        .into(),
                )));
            }
            Ok(crate::api::project::start_infra_setup(&state, id).await)
        })
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("project transition lock: {e}"))
        })?;
    if let Some(run) = started? {
        crate::api::project::await_infra_setup(&state, run).await?;
    }

    // The landing flip: relocate the worker to match post-apply
    // placement (drain outside the lock), then tear down an
    // infra-less namespace under it.
    crate::api::project::reconcile_worker(&state, &project_id, running_policy, drain_timeout_secs)
        .await?;
    let landing: Result<(), (StatusCode, String)> =
        crate::lease::with_project_transition_lock(&state.pg_pool, &project_id, || async {
            Ok(teardown_project_namespace_if_no_infra(&state, id).await)
        })
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("project transition lock: {e}"))
        })?;
    landing?;

    // No auto-reactivate. Upgrade is CLI-orchestrated stop-then-start:
    // the stop already deactivated the project, and a user-invoked
    // upgrade intentionally leaves it deactivated (the user clicks
    // Activate when ready). Automatic reactivation lives only in the
    // autonomous health-recovery path (the supervisor's AutoRecover
    // protocol -> dispatcher lifecycle_claimer -> activate_inner), where
    // there is no human to click.

    Ok(Json(SyncResponse {
        nodes: read_infra_entries(&state, &project_id).await?,
    }))
}

pub async fn stop(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    // Reject-don't-crash against the same reconciliation the action
    // bar renders (a stale tab firing stop into a transitional /
    // already-stopped project).
    crate::api::project::require_action(&state, id, &["infra_stop"]).await?;
    issue_destroy(state, id_str, InfraLifecycleVerb::Stop, body).await
}

pub async fn terminate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    crate::api::project::require_action(&state, id, &["infra_terminate"]).await?;
    issue_destroy(state, id_str, InfraLifecycleVerb::Terminate, body).await
}

/// `POST /projects/{id}/infra/cancel`. Cancel the project's in-flight
/// infra work: flag claimed lifecycle commands (the executing
/// supervisor halts between kubectl steps), cancel still-unclaimed
/// commands outright, and cancel any non-terminal InfraSetup
/// provisioning execution. Cancel = HALT, never rollback: kubectl is
/// not transactional, so per-node partial state stays visible and the
/// user terminates/retries per-node from where it stopped.
///
/// 412 when nothing infra-transitional is in flight (stale tab; the
/// client refetches `/status` and reconciles).
pub async fn cancel(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();

    let touched = infra_lifecycle_command::request_cancel_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request cancel: {e}")))?;

    // Cancel the provisioning sub-execution too (the InfraSetup worker
    // run that computes specs and enqueues applies).
    let colors = crate::api::project::non_terminal_infra_setup_colors(&state, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup colors: {e}")))?;
    let had_setup = !colors.is_empty();
    for color in colors {
        crate::api::execution::cancel_color(&state, color)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_color: {e}")))?;
    }

    if touched == 0 && !had_setup {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "no infra operation in flight to cancel".into(),
        ));
    }
    Ok(StatusCode::ACCEPTED)
}

/// Stop / Terminate share this body. The trigger deactivation choice
/// (when needed) comes from the client; the infra-side running
/// policy follows from the verb (Stop = wait; Terminate = cancel).
///
/// Returns `202 Accepted` with `{ command_id }`. The supervisor
/// hasn't run yet at this point; clients poll `/status` or watch
/// the event SSE for the post-action shape.
async fn issue_destroy(
    state: DispatcherState,
    id_str: String,
    verb: InfraLifecycleVerb,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if matches!(
        lifecycle.status,
        crate::project_store::ProjectStatus::Activating
    ) {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!("project is activating; cannot {}", verb.as_str()),
        ));
    }
    // Verb-specific infra-side running policy. Stop lets in-flight
    // executions finish (then scales to 0); Terminate cancels them
    // before deleting resources. Distinct from the trigger-side
    // running policy in `triggerDeactivation`, which the user picks.
    let running_policy = match verb {
        InfraLifecycleVerb::Stop => RunningPolicy::Wait,
        InfraLifecycleVerb::Terminate => RunningPolicy::Cancel,
        // Apply is never routed through issue_destroy (it is issued as its
        // own infra lifecycle command). Deactivate / Reactivate are
        // dispatcher-owned and don't take this path either. If we get here,
        // a caller wired a new verb without updating this match.
        InfraLifecycleVerb::Apply
        | InfraLifecycleVerb::Deactivate
        | InfraLifecycleVerb::Reactivate => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "issue_destroy called with verb '{}'; only Stop/Terminate are valid here",
                    verb.as_str()
                ),
            ));
        }
    };
    let was_active = matches!(lifecycle.status, crate::project_store::ProjectStatus::Active);
    if was_active {
        let Some(deactivation) = body.trigger_deactivation.as_ref() else {
            return Err((
                StatusCode::PRECONDITION_REQUIRED,
                format!(
                    "project is active; triggerDeactivation {{ mode, runningPolicy, graceMinutes? }} \
                     required so the user can choose how to deactivate triggers before {}",
                    verb.as_str()
                ),
            ));
        };
        crate::api::project::execute_trigger_deactivation(&state, id, deactivation).await?;
    }
    let drain_timeout_secs = body
        .drain_timeout_secs
        .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
    let command_id = issue_lifecycle_ensuring_supervisor(
        &state,
        &project_id,
        None,
        verb,
        running_policy,
        false,
        drain_timeout_secs,
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(LifecycleCommandIssued { command_id }),
    ))
}

pub async fn stop_node(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id_str, node_id)): Path<(String, String)>,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    authorize_project(&state, &caller.0, parse_id(&id_str)?).await?;
    issue_per_node(
        state,
        id_str,
        node_id,
        InfraLifecycleVerb::Stop,
        body,
        RunningPolicy::Wait,
    )
    .await
}

pub async fn terminate_node(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id_str, node_id)): Path<(String, String)>,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    authorize_project(&state, &caller.0, parse_id(&id_str)?).await?;
    issue_per_node(
        state,
        id_str,
        node_id,
        InfraLifecycleVerb::Terminate,
        body,
        RunningPolicy::Cancel,
    )
    .await
}

async fn issue_per_node(
    state: DispatcherState,
    id_str: String,
    node_id: String,
    verb: InfraLifecycleVerb,
    body: Option<Json<PerNodeRequest>>,
    default_running: RunningPolicy,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let project_id = id.to_string();
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // Validation is in the type: serde rejected unknown variants
    // at deserialize. None falls back to the verb's default.
    let running_policy = body.running_policy.unwrap_or(default_running);

    // Per-node verbs are surgical, not consent-bypassing. Refuse
    // when the project is currently Active AND any trigger has the
    // targeted infra node in its upstream closure: stopping or
    // terminating that node would silently break a live trigger.
    // Caller must first deactivate the project (or run a
    // project-level stop with preservation), then retry the per-node
    // verb. Plan section 6.6.
    let lifecycle = state
        .projects
        .lifecycle(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("lifecycle: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if matches!(lifecycle.status, crate::project_store::ProjectStatus::Active) {
        let project = state
            .projects
            .project(id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
            .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
        let deps = crate::api::project::compute_trigger_deps(&project);
        let dependent_triggers: Vec<String> = deps
            .into_iter()
            .filter(|(infra, _)| infra == &node_id)
            .map(|(_, trigger)| trigger)
            .collect();
        if !dependent_triggers.is_empty() {
            return Err((
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "project is active and triggers depend on infra node '{}': [{}]. \
                     Deactivate the project (or run a project-level stop with \
                     trigger preservation) before per-node {}.",
                    node_id,
                    dependent_triggers.join(", "),
                    verb.as_str()
                ),
            ));
        }
    }

    // force only applies to Stop (terminate removes everything anyway).
    let force = matches!(verb, InfraLifecycleVerb::Stop) && body.force;
    let command_id = issue_lifecycle_ensuring_supervisor(
        &state,
        &project_id,
        Some(&node_id),
        verb,
        running_policy,
        force,
        body.drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS),
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(LifecycleCommandIssued { command_id }),
    ))
}

pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id_str): Path<String>,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();
    Ok(Json(SyncResponse {
        nodes: read_infra_entries(&state, &project_id).await?,
    }))
}

#[derive(serde::Serialize)]
pub struct CommandStatusResponse {
    /// True once the supervisor marked the command complete.
    pub done: bool,
    /// `succeeded` / `failed` / `cancelled`, only when `done`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<&'static str>,
    /// Error (on failed) or reason (on cancelled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Poll target for a stop / terminate command's completion. The
/// command outcome is the honest "is it done" signal: a stop where a
/// NoOp unit stays up leaves the project rollup at `running`, so the
/// CLI can't infer completion from the rollup.
pub async fn command_status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id_str, cmd_id)): Path<(String, i64)>,
) -> Result<Json<CommandStatusResponse>, (StatusCode, String)> {
    // Scope the command read to this project: the command is looked up
    // by `(id, project_id)`, so a caller can't read another project's
    // command outcome by enumerating the sequential id.
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();
    use infra_lifecycle_command::WaitOutcome;
    let outcome =
        infra_lifecycle_command::read_command_outcome(&state.pg_pool, &project_id, cmd_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read command: {e}")))?;
    Ok(Json(match outcome {
        None => CommandStatusResponse { done: false, outcome: None, message: None },
        Some(WaitOutcome::Succeeded) => {
            CommandStatusResponse { done: true, outcome: Some("succeeded"), message: None }
        }
        Some(WaitOutcome::Failed { error }) => CommandStatusResponse {
            done: true,
            outcome: Some("failed"),
            message: Some(error),
        },
        Some(WaitOutcome::Cancelled { reason }) => CommandStatusResponse {
            done: true,
            outcome: Some("cancelled"),
            message: Some(reason),
        },
        // read_command_outcome never returns Timeout (non-blocking).
        Some(WaitOutcome::Timeout) => CommandStatusResponse { done: false, outcome: None, message: None },
    }))
}

pub async fn live(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id_str, node_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    authorize_project(&state, &caller.0, id).await?;
    let project_id = id.to_string();

    // Gate on the catalog metadata's `features.live_endpoint`.
    // Nodes that don't expose a /live HTTP route (Postgres, Redis,
    // anything TCP-only) leave it unset and hit this 404 instead of a
    // 502 from the downstream connection refusal.
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let node_def = project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .ok_or((StatusCode::NOT_FOUND, "no such node in project".into()))?;
    // A node opts into /live by naming the endpoint that serves it
    // (features.live_endpoint). `None` = no /live: 404. Resolving by
    // name (not an arbitrary map entry) means a multi-endpoint node's
    // /live hits the right one.
    let live_endpoint = node_def.features.live_endpoint.as_deref().ok_or((
        StatusCode::NOT_FOUND,
        "node does not expose a /live endpoint".to_string(),
    ))?;

    let row = infra_node::get(&state.pg_pool, &project_id, &node_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node lookup: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "no such infra node".into()))?;
    let endpoint_url = row.endpoints.get(live_endpoint).cloned().ok_or((
        StatusCode::NOT_FOUND,
        format!("infra node has no endpoint named '{live_endpoint}' (live_endpoint)"),
    ))?;
    let live_url = format!("{}/live", endpoint_url.trim_end_matches('/'));
    // Reuse the dispatcher's shared HTTP client (one connection pool for the
    // process, not a fresh pool per request). Bound the WHOLE exchange, connect +
    // headers + body, with a single 3s deadline so a downstream node that accepts
    // the connection then trickles the body can't pin the request open.
    let value = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let resp = state
            .http
            .get(&live_url)
            .send()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live fetch: {e}")))?;
        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live parse: {e}")))
    })
    .await
    .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "live endpoint timed out".to_string()))??;
    Ok(Json(value))
}

// =================================================================
// Helpers
// =================================================================

async fn read_infra_entries(
    state: &DispatcherState,
    project_id: &str,
) -> Result<Vec<InfraStatusEntry>, (StatusCode, String)> {
    let rows = infra_node::list_for_project(&state.pg_pool, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    Ok(rows.into_iter().map(row_to_entry).collect())
}

fn row_to_entry(row: InfraNodeRow) -> InfraStatusEntry {
    InfraStatusEntry {
        node_id: row.node_id,
        status: row.status.as_str().to_string(),
        // Coarse UI hint: the first endpoint by name (BTreeMap, so
        // deterministic). Node code resolves a specific endpoint by
        // name via ctx.endpoint(...); this is just a status summary.
        endpoint_url: row.endpoints.values().next().cloned(),
        failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
        failure_message: row.failure_message,
    }
}

fn parse_id(raw: &str) -> Result<uuid::Uuid, (StatusCode, String)> {
    raw.parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad id".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sync_request_defaults() {
        let r: SyncRequest = serde_json::from_value(json!({})).unwrap();
        assert!(r.binary_hash.is_none());
        assert!(r.definition_hash.is_none());
        assert!(r.infra_hash.is_none());
        assert!(r.image_hashes.is_empty());
        assert!(r.trigger_deactivation.is_none());
        assert!(r.running_policy.is_none());
    }

    #[test]
    fn sync_request_running_policy_round_trips() {
        let r: SyncRequest =
            serde_json::from_value(json!({ "runningPolicy": "cancel" })).unwrap();
        assert_eq!(r.running_policy, Some(RunningPolicy::Cancel));
        // ONE wire spelling: snake_case is an unknown field.
        let r: SyncRequest =
            serde_json::from_value(json!({ "running_policy": "cancel" })).unwrap();
        assert_eq!(r.running_policy, None);
    }

    #[test]
    fn sync_request_parses_camelcase_only() {
        let camel: SyncRequest = serde_json::from_value(json!({
            "binaryHash": "abc",
            "definitionHash": "def0",
            "infraHash": "def",
            "imageHashes": { "node1": { "bridge": "x:1" } },
            "triggerDeactivation": {
                "mode": "park",
                "graceMinutes": 30,
                "runningPolicy": "wait",
            },
        }))
        .unwrap();
        assert_eq!(camel.binary_hash.as_deref(), Some("abc"));
        assert_eq!(camel.definition_hash.as_deref(), Some("def0"));
        let td = camel.trigger_deactivation.expect("trigger_deactivation present");
        assert_eq!(td.mode, crate::api::project::DeactivationMode::Park);
        assert_eq!(td.grace_minutes, 30);
        assert_eq!(td.running_policy, RunningPolicy::Wait);

        // ONE wire spelling: snake_case keys are unknown fields, not
        // a tolerated second dialect. (`SyncRequest`'s fields are all
        // defaulted, so unknown top-level keys are silently ignored
        // by serde; the load-bearing check is that the snake key does
        // NOT populate the field.)
        let snake: SyncRequest = serde_json::from_value(json!({
            "binary_hash": "abc",
        }))
        .unwrap();
        assert_eq!(snake.binary_hash, None, "snake_case must not populate the field");
        // A required inner field spelled snake_case fails the parse
        // outright (`runningPolicy` has no default).
        let bad_inner: Result<SyncRequest, _> = serde_json::from_value(json!({
            "triggerDeactivation": {
                "mode": "wipe",
                "running_policy": "cancel",
            },
        }));
        assert!(bad_inner.is_err(), "snake_case runningPolicy must not parse");
    }

    #[test]
    fn stop_request_defaults() {
        let r: StopRequest = serde_json::from_value(json!({})).unwrap();
        assert!(r.trigger_deactivation.is_none());
    }

    #[test]
    fn stop_request_carries_trigger_deactivation() {
        let r: StopRequest = serde_json::from_value(json!({
            "triggerDeactivation": {
                "mode": "park",
                "runningPolicy": "wait",
            }
        }))
        .unwrap();
        let td = r.trigger_deactivation.expect("present");
        assert_eq!(td.mode, crate::api::project::DeactivationMode::Park);
        assert_eq!(td.running_policy, RunningPolicy::Wait);
    }

    #[test]
    fn per_node_request_defaults() {
        let r: PerNodeRequest = serde_json::from_value(json!({})).unwrap();
        assert!(r.running_policy.is_none());
    }

    #[test]
    fn per_node_request_running_policy_round_trips() {
        let r: PerNodeRequest =
            serde_json::from_value(json!({"runningPolicy": "cancel"})).unwrap();
        assert_eq!(r.running_policy, Some(RunningPolicy::Cancel));
        // ONE wire spelling: a snake_case key is an unknown field and
        // must not populate the (defaulted) field.
        let r: PerNodeRequest =
            serde_json::from_value(json!({"running_policy": "wait"})).unwrap();
        assert_eq!(r.running_policy, None, "snake_case must not populate the field");
    }

    #[test]
    fn image_hashes_nested_shape() {
        // Per-(node_id, image_name) map. Verify the wire shape
        // deserializes via the documented `imageHashes` key.
        let r: SyncRequest = serde_json::from_value(json!({
            "imageHashes": {
                "tgi": { "bridge": "weft-infra-bridge:abc123", "engine": "weft-infra-engine:def456" },
                "whatsapp": { "bridge": "weft-infra-bridge:111" }
            }
        }))
        .unwrap();
        assert_eq!(r.image_hashes.len(), 2);
        assert_eq!(
            r.image_hashes.get("tgi").unwrap().get("bridge").unwrap(),
            "weft-infra-bridge:abc123"
        );
    }
}

/// Project deletion entry point. Called by `weft rm`.
///
/// Issues a project-wide `Terminate` lifecycle command and waits for
/// the supervisor to mark it complete (default 120s). Then drops
/// every `infra_*` row for the project and deletes the project
/// namespace (which takes any leftover resources with it).
///
/// Lazy supervisor spawn. Ensures AT LEAST ONE pooled infra-supervisor
/// pod is live in the control-plane namespace (the pool scales up from
/// there by load). Idempotent: a no-op when any live pod already exists,
/// spawns one when the pool is empty. Called at the top of sync before
/// any orphan reap or Apply command enqueue, so a project that just
/// declared infra has a supervisor able to claim it.
async fn ensure_supervisor(state: &DispatcherState) -> anyhow::Result<()> {
    state
        .supervisors
        .ensure_at_least_one(
            state.supervisor_backend.as_ref(),
            &state.pg_pool,
            state.pod_id.as_str(),
        )
        .await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("ensure supervisor pool: {e}"))
}

/// CREATE half of per-project-namespace reconciliation: if the project
/// declares infra, ensure its own namespace + RBAC bundle exists and
/// stamp the row. No-op for a no-infra project. Split from the teardown
/// half because they sit at opposite ends of the sync sequence: create
/// runs BEFORE any infra Pod is applied; teardown runs at the landing
/// flip, after the worker reconciliation has already respawned the
/// worker in the shared pool (there is no worker "move"; a placement
/// change is a kill-then-respawn, see `reconcile_worker`).
///
/// The namespace + RBAC bundle (worker/infra SAs, NetworkPolicies,
/// RoleBindings to the pooled supervisor/listener ClusterRoles) is what
/// every infra Pod the supervisor applies needs around it, so this
/// create must run before the supervisor touches the project. The row's
/// `project_namespace <> ''` is the broker's "this project has a
/// namespace to manage" signal, stamped only after the namespace lands.
async fn ensure_project_namespace_if_infra(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("load project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if !weft_core::has_infra(&project) {
        return Ok(());
    }
    let project_id_str = id.to_string();
    let tenant = state
        .tenant_router
        .tenant_for_project(&project_id_str)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let project_namespace = crate::project_namespace::name_for(tenant.as_str(), &project_id_str);
    let args = crate::project_namespace::ProjectNamespaceArgs {
        project_id: &project_id_str,
        tenant_id: tenant.as_str(),
        namespace: &project_namespace,
        pod_cidr: &state.cluster_pod_cidr,
        service_cidr: &state.cluster_service_cidr,
        ingress_namespace: &state.cluster_ingress_namespace,
        control_plane_namespace: &state.control_plane_namespace,
    };
    crate::project_namespace::ensure(&state.pg_pool, &*state.kube, &args)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("ensure project namespace {project_namespace}: {e}"),
            )
        })?;
    // Stamp only after the namespace actually landed, so
    // `project_namespace <> ''` is never true for a namespace that
    // doesn't exist (which would make the supervisor try to apply into a
    // missing namespace).
    state
        .projects
        .set_project_namespace(id, &project_namespace)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("set_project_namespace: {e}"),
            )
        })?;
    Ok(())
}

/// TEARDOWN half: if the project no longer declares infra AND no live
/// infra state remains, delete the per-project namespace + clear the
/// row + delete its `weft_namespace_tenant` registry row. Runs at the
/// sync landing flip, AFTER `reconcile_worker` has kill-then-respawned
/// the worker into the shared pool, so we never delete a namespace
/// that still hosts the project's worker.
///
/// The live-rows guard is Model 1's never-silently-kill guarantee: an
/// orphaned infra node whose terminate timed out still has an
/// `infra_node` row, and deleting the namespace under it would kill
/// live (billed) infra the user can still see and act on. Skip with a
/// breadcrumb; the user terminates the orphan via the always-visible
/// infra controls and the next sync (or `weft rm`) tears down.
///
/// Clears the row BEFORE deleting the namespace: a cleared row pointing
/// at a not-yet-deleted namespace is benign (the supervisor simply stops
/// managing it), whereas a set row pointing at a DELETED namespace would
/// make the supervisor flap kubectl against a gone namespace. So clear
/// first, delete second; a crash between leaves only an empty orphan
/// namespace (reclaimed on project rm or by a manual delete), never a
/// live-advertised dead namespace. The registry row is deleted LAST
/// (after the namespace object): while the namespace exists in k8s, its
/// row must exist too (the broker's TokenReview resolves pod tenancy
/// from it).
async fn teardown_project_namespace_if_no_infra(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("load project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    if weft_core::has_infra(&project) {
        return Ok(());
    }
    let project_id_str = id.to_string();
    let existing = state
        .projects
        .project_namespace(&project_id_str)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project_namespace: {e}")))?
        .unwrap_or_default();
    if existing.is_empty() {
        return Ok(());
    }
    if crate::infra_node::any_for_project(&state.pg_pool, &project_id_str)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?
    {
        tracing::warn!(
            target: "weft_dispatcher::api::infra",
            project_id = %project_id_str,
            namespace = %existing,
            "namespace teardown skipped: live infra rows remain (orphaned infra whose \
             terminate has not completed); terminate it via the infra controls, then \
             re-sync"
        );
        return Ok(());
    }
    // Clear the row first (stop advertising the namespace to supervisors)
    // ...
    state
        .projects
        .clear_project_namespace(id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("clear_project_namespace: {e}"),
            )
        })?;
    // ... then delete the now-unadvertised, now-worker-less namespace.
    if let Err(e) = crate::project_namespace::delete(&*state.kube, &existing).await {
        tracing::warn!(
            target: "weft_dispatcher::api::infra",
            error = %e,
            project_id = %project_id_str,
            "delete now-infra-less project namespace failed (continuing); \
             row already cleared so no supervisor manages it"
        );
        // Keep the registry row while the k8s namespace object may
        // still exist (broker TokenReview needs it); the next sync
        // retries the delete and then drops the row.
        return Ok(());
    }
    // ... and finally drop the namespace's tenant-registry row, so a
    // torn-down namespace leaves no dangling auth mapping behind.
    if let Err(e) = crate::namespace_registry::delete(&state.pg_pool, &existing).await {
        tracing::warn!(
            target: "weft_dispatcher::api::infra",
            error = %e,
            namespace = %existing,
            "delete weft_namespace_tenant row failed (continuing); harmless until \
             the next sync retries (an unmapped namespace only over-restricts)"
        );
    }
    Ok(())
}

/// Enqueue a lifecycle command AFTER making sure at least one pooled
/// supervisor is alive. A supervisor only claims a command for a project
/// it owns, and only a live supervisor claims+owns projects, so an
/// enqueue with an empty supervisor pool would sit unclaimed forever.
///
/// Every dispatcher-side enqueue path goes through this helper.
/// `issue_lifecycle` itself stays a plain DB-write helper (no
/// kubectl coupling) so the supervisor-side code that ALREADY runs
/// inside a live supervisor can call it directly without recursing
/// into `ensure_supervisor`.
async fn issue_lifecycle_ensuring_supervisor(
    state: &DispatcherState,
    project_id: &str,
    node_id: Option<&str>,
    verb: InfraLifecycleVerb,
    running_policy: RunningPolicy,
    force: bool,
    drain_timeout_secs: u64,
) -> Result<i64, (StatusCode, String)> {
    ensure_supervisor(state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("ensure_supervisor: {e}")))?;
    let tenant = state
        .tenant_router
        .tenant_for_project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    infra_lifecycle_command::issue_lifecycle(
        &state.pg_pool,
        tenant.as_str(),
        project_id,
        node_id,
        verb,
        running_policy,
        force,
        drain_timeout_secs,
        state.pod_id.as_str(),
    )
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("issue {}: {e}", verb.as_str()),
        )
    })
}

/// Reap infra_node rows whose `node_id` no longer appears in the
/// project source as a `requires_infra` node. Step 1 of the sync
/// pipeline so the rest of the subworkflow operates on the new
/// shape only.
///
/// Issues a per-node Terminate lifecycle command for each orphan
/// and waits up to 60s for the supervisor to complete. Top-level
/// "look up the project / list its infra_nodes" failures propagate
/// (the rest of sync can't reason about state without them).
/// Per-orphan supervisor outcomes (Failed / Timeout / Cancelled)
/// are logged; one wedged orphan does not block the rest.
async fn reap_orphans(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let project_id = id.to_string();
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let declared: std::collections::HashSet<String> = project
        .nodes
        .iter()
        .filter(|n| n.requires_infra)
        .map(|n| n.id.clone())
        .collect();
    let rows = crate::infra_node::list_for_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    let orphans: Vec<String> = rows
        .into_iter()
        .filter(|r| !declared.contains(&r.node_id))
        .map(|r| r.node_id)
        .collect();
    if orphans.is_empty() {
        return Ok(());
    }
    let tenant = state
        .tenant_router
        .tenant_for_project(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Step 1: issue every terminate in parallel. issue_lifecycle is
    // a single INSERT; bundling them keeps DB roundtrip cost flat
    // regardless of orphan count.
    let issue_futures = orphans.iter().map(|node_id| {
        let tenant_str = tenant.as_str().to_string();
        let project_id = project_id.clone();
        let node_id = node_id.clone();
        let pool = state.pg_pool.clone();
        let pod = state.pod_id.as_str().to_string();
        async move {
            let res = infra_lifecycle_command::issue_lifecycle(
                &pool,
                &tenant_str,
                &project_id,
                Some(&node_id),
                InfraLifecycleVerb::Terminate,
                RunningPolicy::Cancel,
                false,
                // Cancel never drains; the cap is inert. Default keeps
                // the row honest.
                weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
                &pod,
            )
            .await;
            (node_id, res)
        }
    });
    let issued: Vec<(String, anyhow::Result<i64>)> = futures::future::join_all(issue_futures).await;

    // Step 2: wait on every issued command in ONE batched poll.
    // The previous shape spawned N concurrent `wait_for_command`
    // tasks (each polling the DB at 2 qps), saturating the pool on
    // a wedged supervisor with many orphans. The batched wait
    // collapses to one `SELECT ... WHERE id = ANY($1)` per cycle.
    let deadline = std::time::Duration::from_secs(60);
    let mut cmd_ids: Vec<i64> = Vec::new();
    let mut node_by_id: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
    for (node_id, res) in issued {
        match res {
            Ok(cmd_id) => {
                cmd_ids.push(cmd_id);
                node_by_id.insert(cmd_id, node_id);
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %project_id,
                    node_id = %node_id,
                    error = %e,
                    "orphan reap: failed to issue terminate; skipping"
                );
            }
        }
    }
    let outcomes =
        infra_lifecycle_command::wait_for_commands(&state.pg_pool, &cmd_ids, deadline)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("orphan reap: batched wait: {e}"),
                )
            })?;
    for (cmd_id, outcome) in outcomes {
        // `wait_for_commands` returns in input order; every cmd_id
        // it returns came from `node_by_id`. A missing entry would
        // be a programming error.
        let node_id = node_by_id
            .remove(&cmd_id)
            .expect("wait_for_commands returned cmd_id that wasn't in our issue set");
        match outcome {
            infra_lifecycle_command::WaitOutcome::Succeeded => tracing::info!(
                target: "weft_dispatcher::api::infra",
                project_id = %project_id,
                node_id = %node_id,
                "orphan terminated"
            ),
            infra_lifecycle_command::WaitOutcome::Failed { error } => tracing::warn!(
                target: "weft_dispatcher::api::infra",
                project_id = %project_id,
                node_id = %node_id,
                error = %error,
                "orphan reap: supervisor reported error"
            ),
            infra_lifecycle_command::WaitOutcome::Cancelled { reason } => tracing::info!(
                target: "weft_dispatcher::api::infra",
                project_id = %project_id,
                node_id = %node_id,
                reason = %reason,
                "orphan reap: command cancelled (likely raced a node removal)"
            ),
            infra_lifecycle_command::WaitOutcome::Timeout => tracing::warn!(
                target: "weft_dispatcher::api::infra",
                project_id = %project_id,
                node_id = %node_id,
                "orphan reap: supervisor did not complete within 60s"
            ),
        }
    }
    Ok(())
}

/// `force = true` (i.e. `weft rm --force`) skips the wait: the
/// dispatcher proceeds immediately. Any in-flight supervisor work
/// for the project errors on its kubectl calls because the namespace
/// is gone; the supervisor logs but doesn't retry.
pub async fn delete_project(
    state: &DispatcherState,
    id: uuid::Uuid,
    force: bool,
) -> Result<(), (StatusCode, String)> {
    let project_id = id.to_string();
    // Step 0: does this project have infra at all? A no-infra project has NOTHING
    // for the supervisor to terminate, so enqueuing a Terminate + waiting on it is
    // pure waste: no supervisor owns the project, the command is never marked
    // complete, and the wait below burns the full 120s timeout on EVERY no-infra
    // `weft rm` (the common case, and most e2e teardowns). Skip the supervisor
    // round-trip entirely; go straight to the broker-row cleanup. `None` (project
    // already unregistered) is also "nothing to terminate".
    let has_infra = state
        .projects
        .project_has_infra(&project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project_has_infra: {e}")))?
        .unwrap_or(false);
    if has_infra {
        // Step 1: enqueue a project-wide terminate so the supervisor
        // tears down the workloads. `issue_lifecycle_ensuring_supervisor`
        // guarantees the supervisor is alive first; if the reaper scaled
        // it to 0 during an idle period, `weft rm` would otherwise leave
        // k8s resources behind. A silent failure here is not acceptable;
        // refuse the rm and let the user retry.
        let cmd_id = issue_lifecycle_ensuring_supervisor(
            state,
            &project_id,
            None,
            InfraLifecycleVerb::Terminate,
            RunningPolicy::Cancel,
            false,
            weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS,
        )
        .await?;
        // Step 2: wait for the supervisor unless --force. A wedged
        // supervisor blocks rm indefinitely otherwise; force lets the
        // user proceed knowing orphans may persist. Wait failures are
        // logged but don't block rm: the terminate row is in the queue
        // and the next sweep tick will catch it.
        if !force {
        match infra_lifecycle_command::wait_for_command(
            &state.pg_pool,
            cmd_id,
            std::time::Duration::from_secs(120),
        )
        .await
        {
            Ok(infra_lifecycle_command::WaitOutcome::Failed { error }) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %project_id,
                    error = %error,
                    "supervisor reported terminate failure; continuing with rm cleanup"
                );
            }
            Ok(infra_lifecycle_command::WaitOutcome::Cancelled { reason }) => {
                tracing::info!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %project_id,
                    reason = %reason,
                    "terminate cancelled (race with node removal); continuing with rm cleanup"
                );
            }
            Ok(infra_lifecycle_command::WaitOutcome::Succeeded) => {}
            Ok(infra_lifecycle_command::WaitOutcome::Timeout) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %project_id,
                    "supervisor did not complete terminate within 120s; \
                     continuing with rm cleanup (orphans will be swept by the \
                     next supervisor sweep cycle)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %project_id,
                    error = %e,
                    "wait_for_command errored; continuing with rm cleanup"
                );
            }
        }
        }
    }
    // Step 3: drop the broker-side rows. All three MUST succeed
    // (per the cascade contract on `remove_node`). If the DB writes
    // fail, the next `weft rm` retry is a clean replay.
    infra_node::remove_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("infra_node::remove_project: {e}"),
            )
        })?;
    crate::infra_event::remove_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("infra_event::remove_project: {e}"),
            )
        })?;
    crate::infra_lifecycle_command::remove_project(&state.pg_pool, &project_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("infra_lifecycle_command::remove_project: {e}"),
            )
        })?;
    // Step 4: delete the project's own k8s namespace. Clear the row
    // FIRST then delete (same ordering as the sync-time teardown): a
    // cleared row pointing at a not-yet-deleted namespace is benign,
    // whereas a set row pointing at a DELETED namespace makes the broker
    // advertise a gone namespace to supervisors. Only logged on error: a
    // missing namespace is a no-op, and a transient kubectl failure
    // leaves a tenant-empty namespace that the next sync will repurpose
    // (or the user can manually `kubectl delete ns`). An empty string
    // means the project never had a per-project namespace (a no-infra
    // project, whose worker lives in the shared namespace); nothing to
    // delete and nothing to clear.
    let namespace = state.projects.project_namespace(&project_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("project_namespace: {e}"),
        )
    })?;
    if let Some(ns) = namespace.filter(|n| !n.is_empty()) {
        // Clear the row first so a re-registered project under the same
        // id (or the broker's supervisor-claim) never sees a stale
        // namespace, even if the delete below fails or we crash after it.
        if let Err(e) = state.projects.clear_project_namespace(id).await {
            tracing::warn!(
                target: "weft_dispatcher::api::infra",
                error = %e,
                project_id = %project_id,
                "clear project_namespace row failed (continuing)"
            );
        }
        match project_namespace::delete(&*state.kube, &ns).await {
            Ok(()) => {
                // Namespace gone from k8s: drop its tenant-registry
                // row too (same ordering rule as the sync-time
                // teardown: row outlives the namespace object, never
                // the reverse).
                if let Err(e) = crate::namespace_registry::delete(&state.pg_pool, &ns).await {
                    tracing::warn!(
                        target: "weft_dispatcher::api::infra",
                        error = %e,
                        namespace = %ns,
                        "delete weft_namespace_tenant row failed (continuing)"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    error = %e,
                    project_id = %project_id,
                    "delete project namespace failed (continuing); row already cleared"
                );
            }
        }
    }
    Ok(())
}
