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
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::api::project::StatusError;
use crate::authenticator::{authorize_project, CallerTenant};
use crate::infra_lifecycle_command::{self, InfraLifecycleVerb};
use weft_core::{DeactivateSpec, RunningChoice, RunningPolicy};
use crate::infra_node::{self, InfraNodeRow, InfraNodeStatus};
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
    /// Per-(place, image_name) hash map. Shape:
    /// `{ "<node>": { "<image_name>": "<hash_tag>" } }`, `<node>` being
    /// the infra node's place spelled the way a person writes it
    /// (`one.db`), the key its `infra_node` row is stored under. The
    /// supervisor reads it (executing a claimed infra lifecycle command)
    /// to resolve `Image::Local { name }` references to concrete docker tags.
    /// Absent leaves the stored map as it is: a program starting a
    /// member's copy builds nothing, so it has no new tags to write.
    #[serde(default, rename = "imageHashes")]
    pub image_hashes: Option<BTreeMap<String, BTreeMap<String, String>>>,
    /// How the worker reconciliation inside sync (and an upgrade's
    /// stop leg) treats RUNNING executions when a worker must be
    /// replaced (stale image, or its namespace no longer matches
    /// placement after infra appeared / went away). `cancel` (the
    /// default) cancels the running executions first; `wait` drains
    /// the doomed workers (no new admissions; in-flight work finishes)
    /// up to `drainTimeoutSecs`, then replaces. Never a silent kill.
    /// On an upgrade, outranked by `triggerDeactivation`'s answer when
    /// that picker was shown.
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Whose copies: a member's copies of the nodes marked
    /// `@per_member`, or (absent) the shared nodes. `weft infra start
    /// --member`, and a program's `ctx.infra(..).member(..).start()`.
    #[serde(default)]
    pub member: Option<weft_core::member::MemberId>,
    /// Only these infra nodes (by place), every one of the owner's kind
    /// when empty.
    #[serde(default)]
    pub nodes: Vec<String>,
}

/// Body for `/infra/sync`: the sync request, plus the two keys an
/// upgrade used to ride on this route with. A client that still sends
/// them wants an upgrade, and a plain start in its place would drop the
/// answer it gave, so the route refuses them naming `/infra/upgrade`.
#[derive(Debug, Default, Deserialize)]
pub struct SyncBody {
    #[serde(flatten)]
    pub sync: SyncRequest,
    #[serde(default)]
    upgrade: Option<serde_json::Value>,
    #[serde(default, rename = "triggerDeactivation")]
    trigger_deactivation: Option<serde_json::Value>,
}

impl SyncBody {
    fn into_sync(self) -> Result<SyncRequest, (StatusCode, String)> {
        if self.upgrade.is_some() || self.trigger_deactivation.is_some() {
            return Err((
                StatusCode::BAD_REQUEST,
                "an upgrade is its own route now: POST /projects/{id}/infra/upgrade \
                 (`upgrade` and `triggerDeactivation` are not keys of /infra/sync); \
                 update the client"
                    .into(),
            ));
        }
        Ok(self.sync)
    }
}

/// Body for `/infra/upgrade`: cycle the running infra onto the current
/// specs (the triggers reading it taken down per `triggerDeactivation`,
/// then a stop leg, then the start). What to run is the sync body's.
#[derive(Debug, Default, Deserialize)]
pub struct UpgradeRequest {
    #[serde(flatten)]
    pub sync: SyncRequest,
    /// How to deactivate the triggers reading this infra when one is on
    /// (required then: 428 with the trigger-choice header otherwise).
    /// Same `DeactivateSpec` shape as the standalone `/deactivate`
    /// endpoint, so clients reuse one picker.
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<DeactivateSpec>,
}

#[derive(Debug, Serialize)]
pub struct SyncResponse {
    pub nodes: Vec<InfraStatusEntry>,
}

/// Make `cancel` mean cancel before an infra command that will take
/// the containers away. The supervisor treats a `cancel` command as
/// "the dispatcher already ended the running executions" and tears
/// down at once, and that is true only when a trigger deactivation
/// ran (an active project's picker). On an inactive project nothing
/// ran, so a `weft run` using that infra would have its container
/// pulled out from under it and fail at the node with no cancel on
/// record; this is the cancel it gets instead. `wait` needs nothing
/// here: the supervisor drains before acting.
pub(crate) async fn settle_running_before_infra_op(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    copies: &weft_core::member::Copies,
    running_policy: RunningPolicy,
    trigger_deactivation_ran: bool,
    // The run that asked (a program's `ctx.infra(..).stop(..)`): never
    // among the runs this cancels; its own `StopSelf` decides its fate.
    asked_by: Option<weft_core::Color>,
) -> Result<(), (StatusCode, String)> {
    if running_policy == RunningPolicy::Cancel && !trigger_deactivation_ran {
        let runs = crate::take_down::live_runs(state, project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("live runs: {e}")))?;
        let user = weft_core::exec::CancelCause::User;
        let targets: Vec<(weft_core::Color, &weft_core::exec::CancelCause)> =
            crate::take_down::runs_using_copies(copies, &runs, asked_by)
                .into_iter()
                .filter(|r| !r.suspended)
                .map(|r| (r.color, &user))
                .collect();
        crate::api::execution::cancel_colors(state, &targets)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))?;
    }
    Ok(())
}

/// The activations whose triggers read any of `nodes` (places) in one of
/// `copies`: a shared copy feeds every owner's triggers that read it; a
/// member's copy feeds only that member's.
pub(crate) async fn activations_reading(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
    nodes: &std::collections::BTreeSet<String>,
    copies: &weft_core::member::Copies,
) -> Result<Vec<crate::activation_store::Activation>, (StatusCode, String)> {
    let deps = crate::api::project::compute_trigger_deps(project);
    Ok(state
        .activations
        .list(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?
        .into_iter()
        .filter(|a| reads(&deps, &a.key, nodes, copies))
        .collect())
}

/// Whether the activation `key`'s trigger reads any of `nodes` (places)
/// in one of `copies`, given the program's `(infra, trigger)` reads
/// (`compute_trigger_deps`).
pub(crate) fn reads(
    deps: &[(String, String)],
    key: &weft_core::activation::ActivationKey,
    nodes: &std::collections::BTreeSet<String>,
    copies: &weft_core::member::Copies,
) -> bool {
    let reads_a_node = deps.iter().any(|(infra, trigger)| *trigger == key.trigger && nodes.contains(infra));
    reads_a_node
        && match copies {
            weft_core::member::Copies::Member(m) => key.member() == Some(m),
            weft_core::member::Copies::Shared | weft_core::member::Copies::Every => true,
        }
}

/// Whether the infra place `spelled` (`one.db`) is a node that exists
/// once per member.
pub(crate) fn is_per_member_place(project: &weft_core::ProjectDefinition, spelled: &str) -> bool {
    let (id, _) = weft_core::project::resolve_address(project, spelled);
    project.nodes.iter().any(|n| n.id == id && n.per_member.is_some())
}

/// The infra places a verb aimed at `nodes` (every infra node when
/// empty) for `member` acts on: the nodes marked per member for a
/// member, the shared ones otherwise. A mismatch is refused naming the
/// fix, like the trigger verbs do.
pub(crate) fn resolve_infra_nodes(
    project: &weft_core::ProjectDefinition,
    nodes: &[String],
    member: Option<&weft_core::member::MemberId>,
) -> Result<std::collections::BTreeSet<String>, (StatusCode, String)> {
    let per_member_of = |spelled: &str| is_per_member_place(project, spelled);
    let declared = weft_core::project::infra_place_spellings(project);
    if nodes.is_empty() {
        return Ok(declared.into_iter().filter(|n| per_member_of(n) == member.is_some()).collect());
    }
    let mut out = std::collections::BTreeSet::new();
    for node in nodes {
        if !declared.contains(node) {
            return Err((StatusCode::NOT_FOUND, format!("'{node}' is no infra node of this program")));
        }
        match (per_member_of(node), member) {
            (true, None) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("infra node '{node}' exists once per member; name whose copy with --member <id>"),
                ));
            }
            (false, Some(m)) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("infra node '{node}' is shared by every member, so there is no copy of it for '{m}'; leave --member out"),
                ));
            }
            _ => {
                out.insert(node.clone());
            }
        }
    }
    Ok(out)
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
    /// The instance's place, spelled the way a person writes the node
    /// (`db`, `one.db`): what a person is shown, what the editor matches
    /// against its canvas, and what every per-node verb takes.
    pub node: String,
    /// Whose copy: absent for the shared one, else the member's.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<weft_core::member::MemberId>,
    pub status: String,
    pub endpoint_url: Option<String>,
    /// Endpoint name to the address a caller outside the cluster uses,
    /// for each `TenantPublic` endpoint (the same address
    /// `ctx.endpoint(name)?.public_url()` gives the node).
    pub public_urls: std::collections::BTreeMap<String, String>,
    pub failure_stage: Option<String>,
    pub failure_message: Option<String>,
}

/// Body for `/infra/stop` and `/infra/terminate`. Carries the
/// trigger-deactivation choice when the project is Active (the same
/// picker as the standalone Deactivate verb, and its answer governs
/// the running executions), and the running-work choice on its own
/// for when it is not: an inactive project can still have executions
/// running on this infra, and `wait` lets them land before the
/// supervisor scales it down.
#[derive(Debug, Default, Deserialize)]
pub struct StopRequest {
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<DeactivateSpec>,
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Whose copies: one member's (`--member`), or (absent) the shared
    /// ones.
    #[serde(default)]
    pub member: Option<weft_core::member::MemberId>,
}

#[derive(Debug, Default, Deserialize)]
pub struct PerNodeRequest {
    /// What happens to the running executions this copy can reach.
    /// Which ones use this one instance is not recorded, so for the
    /// shared copy `cancel` (the default) ends every running execution
    /// of the project, and for a member's copy every run of that
    /// member; `wait` lets the same set land first.
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Stop only: force scale-to-zero every unit, ignoring `on_stop`.
    /// Lets the user take down a unit that would normally stay up
    /// (NoOp) so they can update it on the next start. Ignored by
    /// terminate (terminate already removes everything).
    #[serde(default)]
    pub force: bool,
    /// Whose copy of the node: a member's copy of a per-member node,
    /// or (absent) the shared one.
    #[serde(default)]
    pub member: Option<weft_core::member::MemberId>,
}

// =================================================================
// Handlers
// =================================================================


pub async fn sync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<SyncBody>>,
) -> Result<Json<SyncResponse>, StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default().into_sync()?;

    // No sync-in-flight sentinel: the supervisor pool reaps a supervisor
    // by OWNERSHIP (a pod owning zero projects), not by a global idle
    // scan that a sync would need to block. A project being synced is
    // owned by its supervisor (non-zero, so never reaped), and
    // `ensure_supervisor` below guarantees a live pod exists; there is no
    // reaper race for a sentinel to prevent.
    sync_inner(state, id, body).await
}

pub(super) async fn sync_inner(
    state: DispatcherState,
    id: uuid::Uuid,
    body: SyncRequest,
) -> Result<Json<SyncResponse>, StatusError> {
    let begun = begin_sync(&state, id, &body).await?;
    finish_sync(&state, id, begun).await?;
    Ok(Json(SyncResponse {
        nodes: read_infra_entries(&state, id).await?,
    }))
}

/// `POST /projects/{id}/infra/upgrade`. Checks the upgrade can go ahead
/// (the person's answer in `triggerDeactivation` is required when a
/// trigger reading the infra is on), then issues an `upgrade` command
/// and answers 202 with its id at once; 409 while another upgrade of
/// the same copies is in flight. A dispatcher pod runs it
/// ([`run_upgrade`]): the triggers down, the stop leg, then the start. The caller follows
/// the command (`/infra/commands/{id}`) to its outcome; nothing about it
/// depends on this request staying open, and a pod dying mid-way hands
/// it to another.
pub async fn upgrade(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<UpgradeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    let UpgradeRequest { sync, trigger_deactivation } = body.map(|Json(b)| b).unwrap_or_default();
    let gated = gate_sync(&state, id, &sync, SyncKind::Upgrade { trigger_deactivation: trigger_deactivation.as_ref() }).await?;
    let (running_policy, drain_timeout_secs) = sync.running.resolve(trigger_deactivation.as_ref());
    let work = infra_lifecycle_command::UpgradeWork {
        nodes: sync.nodes,
        running_policy,
        drain_timeout_secs,
        trigger_deactivation: if gated.triggers_on { trigger_deactivation } else { None },
        binary_hash: sync.binary_hash,
        definition_hash: sync.definition_hash,
        infra_hash: sync.infra_hash,
        image_hashes: sync.image_hashes,
        stopped: false,
    };
    let tenant = state
        .tenant_router
        .tenant_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let issued = infra_lifecycle_command::issue_upgrade(
        &state.pg_pool,
        tenant.as_str(),
        id,
        sync.member.as_ref(),
        &work,
        state.pod_id.as_str(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("issue upgrade: {e:#}")))?;
    match issued {
        infra_lifecycle_command::UpgradeIssued::Issued(command_id) => {
            Ok((StatusCode::ACCEPTED, Json(LifecycleCommandIssued { command_id })))
        }
        infra_lifecycle_command::UpgradeIssued::AlreadyInFlight(command_id) => Err(StatusError::Other(
            StatusCode::CONFLICT,
            format!(
                "an upgrade of these copies is already in flight (command {command_id}); wait for it \
                 to finish or cancel it (`/infra/cancel`)"
            ),
        )),
    }
}

/// How an upgrade command ended, short of failing.
pub(crate) enum UpgradeEnd {
    Done,
    /// Cancelled (`/infra/cancel`, or the project went); the reason.
    Cancelled(String),
}

/// Carry out upgrade command `command_id` of `project_id`: `member`'s
/// copies (the shared ones for `None`). The triggers reading them down
/// and the stop leg (unless a claimer before this one already landed
/// it, `work.stopped`), then the start, exactly a sync's. Safe to run
/// again from the top on a takeover: triggers already down are not on
/// any more, the stop is skipped once recorded, and a setup a dead
/// claimer left in flight is waited out before the start goes again.
pub(crate) async fn run_upgrade(
    state: &DispatcherState,
    id: uuid::Uuid,
    command_id: i64,
    member: Option<&weft_core::member::MemberId>,
    work: &infra_lifecycle_command::UpgradeWork,
) -> Result<UpgradeEnd, (StatusCode, String)> {
    match upgrade_legs(state, id, command_id, member, work).await {
        Ok(end) => Ok(end),
        Err(crate::api::project::SyncNotLanded::Cancelled(reason)) => Ok(UpgradeEnd::Cancelled(reason)),
        // An infra cancel ends the setup or the stop it lands in, and
        // that surfaces here as whatever error the leg met: the cancel
        // is what happened.
        Err(crate::api::project::SyncNotLanded::Failed(code, message)) => {
            if infra_lifecycle_command::cancel_requested(&state.pg_pool, command_id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the cancel flag: {e:#}")))?
            {
                return Ok(UpgradeEnd::Cancelled(format!("cancelled ({message})")));
            }
            Err((code, message))
        }
    }
}

/// [`run_upgrade`]'s legs, in order.
async fn upgrade_legs(
    state: &DispatcherState,
    id: uuid::Uuid,
    command_id: i64,
    member: Option<&weft_core::member::MemberId>,
    work: &infra_lifecycle_command::UpgradeWork,
) -> Result<UpgradeEnd, crate::api::project::SyncNotLanded> {
    let Some(project) = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
    else {
        return Ok(UpgradeEnd::Cancelled(format!("project {id} no longer exists")));
    };
    let copies = weft_core::member::Copies::of(member.cloned());
    if !work.stopped {
        let targeted = resolve_infra_nodes(&project, &work.nodes, member)?;
        let triggers_taken_down = take_down_upgrade_readers(state, id, &project, &targeted, &copies, work).await?;
        // The apply path leaves up units frozen, so to cycle a running
        // unit onto a new spec the stop comes first (respecting each
        // unit's on_stop). What happens to the executions running
        // meanwhile is the person's answer: `wait` lets them finish up
        // to their cap, `cancel` ends them first.
        settle_running_before_infra_op(state, id, &copies, work.running_policy, triggers_taken_down, None).await?;
        let mut pending = issue_per_nodes_ensuring_supervisor(
            state,
            id,
            &targeted,
            &copies,
            InfraLifecycleVerb::Stop,
            work.running_policy,
            false,
            work.drain_timeout_secs,
        )
        .await?;
        // No deadline: a drain the person asked to wait for can run as
        // long as their executions do. A breadcrumb each minute keeps
        // the wait legible, and `/infra/cancel` is the way out (it
        // completes the stop as cancelled, which ends this wait).
        let mut waited_minutes = 0u64;
        while !pending.is_empty() {
            let outcomes = crate::infra_lifecycle_command::wait_for_commands(
                &state.pg_pool,
                &state.signals,
                &pending,
                std::time::Duration::from_secs(60),
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("wait for stop leg: {e}")))?;
            pending.clear();
            for (stop_id, outcome) in outcomes {
                match outcome {
                    crate::infra_lifecycle_command::WaitOutcome::Succeeded => {}
                    crate::infra_lifecycle_command::WaitOutcome::Failed { error } => {
                        return Err(crate::api::project::SyncNotLanded::Failed(
                            StatusCode::BAD_GATEWAY,
                            format!("upgrade stop leg failed: {error}; infra left as-is, retry or act \
                                     per node (`weft infra status`)"),
                        ));
                    }
                    crate::infra_lifecycle_command::WaitOutcome::Cancelled { reason } => {
                        return Ok(UpgradeEnd::Cancelled(format!("upgrade stop leg cancelled ({reason}); infra left as-is")));
                    }
                    crate::infra_lifecycle_command::WaitOutcome::Timeout => pending.push(stop_id),
                }
            }
            if !pending.is_empty() {
                waited_minutes += 1;
                tracing::info!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    command_id,
                    waited_minutes,
                    drain_timeout_secs = work.drain_timeout_secs,
                    still_stopping = pending.len(),
                    "upgrade stop leg still in flight; cancel it with `/infra/cancel`"
                );
            }
        }
        let still_ours = infra_lifecycle_command::mark_upgrade_stopped(&state.pg_pool, command_id, state.pod_id.as_str())
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("record the stop leg: {e:#}")))?;
        if !still_ours {
            return Ok(UpgradeEnd::Cancelled("another dispatcher took this upgrade over".into()));
        }
    }
    // A cancel that came while the stop was landing ends the upgrade
    // here, with the infra stopped (a cancel halts, never rolls back).
    if infra_lifecycle_command::cancel_requested(&state.pg_pool, command_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the cancel flag: {e:#}")))?
    {
        return Ok(UpgradeEnd::Cancelled("cancelled between the stop and the start; the infra is stopped".into()));
    }
    // A setup a claimer before this one started is waited out first: a
    // second start while it runs would be refused as in flight.
    // One nothing will advance any more is ended instead of followed.
    // Its outcome is not this upgrade's: whatever it ended in, the start
    // below goes again (a cancel meant for this upgrade is read off the
    // command's own flag, right after).
    let left = crate::api::project::live_infra_setup_colors(state, id, Some(member))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup colors: {e:#}")))?;
    for color in left {
        if let Err(ended) =
            crate::api::project::await_infra_setup(state, crate::api::project::InfraSetupRun::follow(state, id, color).await)
                .await
        {
            tracing::info!(%id, %color, ?ended, "an earlier infra setup ended before this upgrade's start");
        }
    }
    if infra_lifecycle_command::cancel_requested(&state.pg_pool, command_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the cancel flag: {e:#}")))?
    {
        return Ok(UpgradeEnd::Cancelled("cancelled before the start; the infra is stopped".into()));
    }
    let body = SyncRequest {
        binary_hash: work.binary_hash.clone(),
        definition_hash: work.definition_hash.clone(),
        infra_hash: work.infra_hash.clone(),
        image_hashes: work.image_hashes.clone(),
        running: RunningChoice {
            running_policy: Some(work.running_policy),
            drain_timeout_secs: Some(work.drain_timeout_secs),
        },
        member: member.cloned(),
        nodes: work.nodes.clone(),
    };
    let begun = apply_sync(state, id, &body).await?;
    finish_sync(state, id, begun).await?;
    Ok(UpgradeEnd::Done)
}

/// Take down, by the person's answer, the triggers reading the copies an
/// upgrade cycles. Whether any came down here (their take-down then
/// settled the runs by the same answer). One that came on after the
/// upgrade was asked for, with no answer on the command, stops it.
async fn take_down_upgrade_readers(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &weft_core::ProjectDefinition,
    targeted: &std::collections::BTreeSet<String>,
    copies: &weft_core::member::Copies,
    work: &infra_lifecycle_command::UpgradeWork,
) -> Result<bool, (StatusCode, String)> {
    let live = live_reader_keys(&activations_reading(state, id, project, targeted, copies).await?);
    if live.is_empty() {
        return Ok(false);
    }
    let Some(deactivation) = &work.trigger_deactivation else {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!(
                "{} reading this infra came on after the upgrade was asked for; ask for the \
                 upgrade again and answer for them",
                triggers_counted(live.len())
            ),
        ));
    };
    crate::api::project::execute_trigger_deactivation(state, id, live, deactivation).await?;
    Ok(true)
}

/// A sync whose InfraSetup is durably started: what [`finish_sync`]
/// needs to see it land.
pub(crate) struct BegunSync {
    run: Option<crate::api::project::InfraSetupRun>,
    running_policy: RunningPolicy,
    drain_timeout_secs: u64,
}

/// Which face of sync a request is: a plain start (brings down units
/// up, never deactivates), or an upgrade's (cycles running infra, with
/// the person's answer for the triggers reading it).
#[derive(Clone, Copy)]
enum SyncKind<'a> {
    Start,
    Upgrade { trigger_deactivation: Option<&'a DeactivateSpec> },
}

/// What the gates leave for the rest of a sync or an upgrade.
struct Gated {
    /// A trigger reading this infra is on (an upgrade only: its answer
    /// for them is then required, and the run takes them down).
    triggers_on: bool,
}

/// Everything of a plain sync up to its InfraSetup being journaled and
/// queued: the gates, then [`apply_sync`]. What a program's
/// `ctx.infra(..).start()` waits for before it returns (the copy then
/// comes up on its own).
pub(crate) async fn begin_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    body: &SyncRequest,
) -> Result<BegunSync, StatusError> {
    gate_sync(state, id, body, SyncKind::Start).await?;
    Ok(apply_sync(state, id, body).await?)
}

/// The checks a sync or an upgrade passes before anything changes. An
/// upgrade of infra a live trigger reads needs the person's answer for
/// those triggers; the upgrade's run applies it.
async fn gate_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    body: &SyncRequest,
    kind: SyncKind<'_>,
) -> Result<Gated, StatusError> {
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let copies = weft_core::member::Copies::of(body.member.clone());
    let targeted = resolve_infra_nodes(&registered, &body.nodes, body.member.as_ref())?;
    let readers = activations_reading(state, id, &registered, &targeted, &copies).await?;
    if let Some(busy) = readers.iter().find(|a| {
        matches!(
            a.lifecycle.status,
            crate::activation_store::ProjectStatus::Activating | crate::activation_store::ProjectStatus::Deactivating
        )
    }) {
        return Err(StatusError::Other(
            StatusCode::PRECONDITION_FAILED,
            format!(
                "trigger {} reading this infra is {}; wait or cancel before syncing infra",
                busy.key,
                busy.lifecycle.status.as_str()
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
        return Err(StatusError::Other(
            StatusCode::CONFLICT,
            format!(
                "project is {}; wait for the build to finish or cancel it before syncing infra",
                transition.as_str()
            ),
        ));
    }
    // Fast reject before any side effect; re-checked under the lock
    // in `apply_sync` (the locked re-check is the race-safe one).
    if crate::api::project::infra_setup_in_flight(state, id, Some(body.member.as_ref()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}")))?
    {
        return Err(StatusError::Other(
            StatusCode::CONFLICT,
            "an infra sync is already in flight for these copies; wait for it to \
             finish or cancel it (`/infra/cancel`)"
                .into(),
        ));
    }

    // Verb auto-build, BEFORE the transition lock: a build
    // takes minutes and must never run while the per-project advisory
    // lock pins a pool connection. The `coherent_definition` call
    // inside the locked `start_infra_setup` then cache-hits
    // (nothing left to build) and stays lock-cheap.
    crate::transition::ensure_built_gated(state, id).await?;

    // Enforce against the same reconciliation the action bar renders:
    // the two faces of sync are distinct table verbs. A plain START is
    // `infra_start` (offered when infra is down/stopped/partial, never
    // when everything already runs); an UPGRADE is `infra_upgrade`
    // (re-cycle running infra onto current specs).
    // The action table is the project's SHARED infra (what the editor's
    // bar starts and stops); a member's copies are started by the
    // program or `--member`, and gated by their own checks above.
    if body.member.is_none() {
        let action = match kind {
            SyncKind::Start => "infra_start",
            SyncKind::Upgrade { .. } => "infra_upgrade",
        };
        crate::api::project::require_action(state, id, None, &[action]).await?;
    }

    // A plain START never deactivates: an active project's triggers
    // stay live while infra comes up (fires whose subgraph touches the
    // not-yet-running infra fail loudly at the node; fires that don't
    // touch it keep working, which is the continuity an active project
    // is owed). An UPGRADE disturbs live infra the triggers may depend
    // on, so when one is on it REQUIRES the person's deactivation
    // choice; the upgrade's run applies it before its stop leg, so a
    // refused or failed issue leaves the triggers as they were.
    let SyncKind::Upgrade { trigger_deactivation } = kind else {
        return Ok(Gated { triggers_on: false });
    };
    let live_readers = live_reader_keys(&readers);
    if live_readers.is_empty() {
        return Ok(Gated { triggers_on: false });
    }
    let Some(deactivation) = trigger_deactivation else {
        return Err(StatusError::NeedsTriggerChoice(weft_core::trigger_choice_required(&format!(
            "{} reading this infra {} on and an upgrade takes it down",
            triggers_counted(live_readers.len()),
            if live_readers.len() == 1 { "is" } else { "are" },
        ))));
    };
    deactivation
        .validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, format!("triggerDeactivation: {m}")))?;
    Ok(Gated { triggers_on: true })
}

/// The keys of the `readers` whose triggers are on.
fn live_reader_keys(readers: &[crate::activation_store::Activation]) -> Vec<weft_core::activation::ActivationKey> {
    readers
        .iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .map(|a| a.key.clone())
        .collect()
}

/// A gated sync (or an upgrade past its stop leg) from the orphans'
/// reap to its InfraSetup being journaled and queued: the running
/// hashes, the namespace and the worker.
async fn apply_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    body: &SyncRequest,
) -> Result<BegunSync, (StatusCode, String)> {
    // Lazy supervisor spawn. The supervisor is what owns kubectl
    // for user infra; sync is the first verb that needs it (orphan
    // reap and Apply commands both depend on a live supervisor).
    // Idempotent: applies the same Deployment manifest every time;
    // k8s no-ops if already present. MUST land before any code path
    // that enqueues a lifecycle command (orphan reap, start_infra_setup).
    ensure_supervisor(state)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")))?;

    // Orphan reap. An `infra_node` row whose `node_id` isn't in the
    // current project source (or no longer carries `requires_infra`)
    // is stale: the user removed it from .weft but the supervisor
    // still has Pods/Services/PVCs deployed for it. Reap before
    // running the subworkflow so the new shape is the only thing
    // alive afterwards. Hard error: leaking stale infra is silently
    // worse than asking the user to retry.
    reap_orphans(state, id).await?;

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
    //   2. retire_stale_workers (pre-apply): mark a stale-image or
    //      misplaced worker draining, so the InfraSetup exec never
    //      lands on an old binary or on a shared-pool pod the infra
    //      network wall would block; the cold-start sweep brings up a
    //      fresh one for it. The old worker's work is waited for (or
    //      cancelled) and the pod killed in step 5, after the setup:
    //      a run on it may be the very one waiting for this setup.
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
    let (running_policy, drain_timeout_secs) = body.running.resolve(None);

    // Advance the running-hash trio + infra image-tag map NOW: every reject gate
    // has passed and an upgrade's stop leg (if any) succeeded, so from here the sync
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
    let infra_image_tags: Option<crate::project_store::InfraImageTags> = body.image_hashes.as_ref().map(|hashes| {
        hashes
            .iter()
            .map(|(node_id, tags)| {
                (
                    node_id.clone(),
                    tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                )
            })
            .collect()
    });
    state
        .projects
        .set_running_hashes(
            id,
            body.binary_hash.as_deref(),
            body.definition_hash.as_deref(),
            body.infra_hash.as_deref(),
            infra_image_tags.as_ref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_running_hashes: {e}")))?;

    ensure_project_namespace_if_infra(state, id).await?;
    // New work stops landing on stale workers now, so the setup below
    // runs on a fresh one; waiting for the old ones' work and killing
    // them is `finish_sync`'s, after the setup. Waiting here would hold
    // the setup behind a run that is itself waiting for it (a program
    // that starts infra and waits for it to run).
    crate::api::project::retire_stale_workers(state, id).await?;
    let started: Result<Option<crate::api::project::InfraSetupRun>, (StatusCode, String)> =
        crate::lease::with_project_transition_lock(&state.lock_pool, id, || async {
            if crate::api::project::infra_setup_in_flight(state, id, Some(body.member.as_ref())).await? {
                return Ok(Err((
                    StatusCode::CONFLICT,
                    "an infra sync is already in flight for these copies; wait for it \
                     to finish or cancel it (`/infra/cancel`)"
                        .into(),
                )));
            }
            Ok(crate::api::project::start_infra_setup(state, id, body.member.as_ref(), &body.nodes).await)
        })
        .await
        .map_err(|e| crate::lease::lock_answer("project transition lock", e))?;
    Ok(BegunSync { run: started?, running_policy, drain_timeout_secs })
}

/// The rest of a sync: wait for its InfraSetup to land, then move the
/// worker to where placement now puts it and drop a namespace that holds
/// no infra any more.
pub(crate) async fn finish_sync(
    state: &DispatcherState,
    id: uuid::Uuid,
    begun: BegunSync,
) -> Result<(), crate::api::project::SyncNotLanded> {
    let BegunSync { run, running_policy, drain_timeout_secs } = begun;
    if let Some(run) = run {
        crate::api::project::await_infra_setup(state, run).await?;
    }

    // The landing flip: relocate the worker to match post-apply
    // placement (drain outside the lock), then tear down an
    // infra-less namespace under it.
    crate::api::project::reconcile_worker(state, id, running_policy, drain_timeout_secs)
        .await?;
    let landing: Result<(), (StatusCode, String)> =
        crate::lease::with_project_transition_lock(&state.lock_pool, id, || async {
            Ok(teardown_project_namespace_if_no_infra(state, id).await)
        })
        .await
        .map_err(|e| crate::lease::lock_answer("project transition lock", e))?;
    landing?;

    // No auto-reactivate. An upgrade takes its triggers down before its
    // stop leg, and a user-invoked
    // upgrade intentionally leaves it deactivated (the user clicks
    // Activate when ready). Automatic reactivation lives only in the
    // autonomous health-recovery path (the supervisor's AutoRecover
    // protocol -> dispatcher lifecycle_claimer -> activate_inner), where
    // there is no human to click.
    Ok(())
}

/// "one trigger" / "3 triggers", for a refusal that counts them.
fn triggers_counted(n: usize) -> String {
    if n == 1 { "one trigger".to_string() } else { format!("{n} triggers") }
}

pub async fn stop(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    // Reject-don't-crash against the same reconciliation the action
    // bar renders (a stale tab firing stop into a transitional /
    // already-stopped project).
    issue_destroy(state, id, InfraLifecycleVerb::Stop, body).await
}

pub async fn terminate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    authorize_project(&state, &caller.0, id).await?;
    issue_destroy(state, id, InfraLifecycleVerb::Terminate, body).await
}

/// `POST /projects/{id}/infra/cancel[?member=<id>]`. Cancel one
/// owner's in-flight infra work: the shared copies' (no `member`) or
/// one member's. Flags that owner's claimed supervisor commands (the
/// executing supervisor halts between kubectl steps), cancels its
/// still-unclaimed ones outright, and cancels its non-terminal
/// InfraSetup provisioning execution. Dispatcher-owned verbs
/// (deactivate / reactivate) are never touched: they are health's own
/// work, not something a person started. Cancel = HALT, never
/// rollback: kubectl is not transactional, so per-node partial state
/// stays visible and the user terminates/retries per-node from where
/// it stopped.
///
/// 412 when nothing infra-transitional is in flight for that owner
/// (stale tab; the client refetches `/status` and reconciles).
pub async fn cancel(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Query(copy): Query<CopyQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;

    let touched = infra_lifecycle_command::request_cancel_owner(&state.pg_pool, id, copy.member.as_ref())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request cancel: {e}")))?;

    // Cancel the provisioning sub-execution too (the InfraSetup worker
    // run that computes specs and enqueues applies).
    let colors = crate::api::project::non_terminal_infra_setup_colors(&state, id, Some(copy.member.as_ref()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup colors: {e}")))?;
    let had_setup = !colors.is_empty();
    for color in colors {
        crate::api::execution::cancel_color(&state, color, &weft_core::exec::CancelCause::User)
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
/// (when needed) comes from the client, and its running policy is the
/// one answer for the infra side too; with no picker (an inactive
/// project) the shared default stands, cancel.
///
/// Returns `202 Accepted` with `{ command_id }`. The supervisor
/// hasn't run yet at this point; clients poll `/status` or watch
/// the event SSE for the post-action shape.
async fn issue_destroy(
    state: DispatcherState,
    id: uuid::Uuid,
    verb: InfraLifecycleVerb,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), StatusError> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // Apply is never routed through issue_destroy (it is issued as its
    // own infra lifecycle command). Deactivate / Reactivate are
    // dispatcher-owned and don't take this path either. If we get here,
    // a caller wired a new verb without updating this match.
    let action = match verb {
        InfraLifecycleVerb::Stop => "infra_stop",
        InfraLifecycleVerb::Terminate => "infra_terminate",
        other => {
            return Err(StatusError::Other(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("issue_destroy called with verb '{}'; only Stop/Terminate are valid here", other.as_str()),
            ));
        }
    };
    // Reject-don't-crash against the same reconciliation the action
    // bar renders (a stale tab firing stop into a transitional /
    // already-stopped project). The bar is the SHARED infra's; a
    // member's copies are the program's and `--member`'s to manage.
    if body.member.is_none() {
        crate::api::project::require_action(&state, id, None, &[action]).await?;
    }
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let copies = weft_core::member::Copies::of(body.member.clone());
    let targeted = resolve_infra_nodes(&project, &[], body.member.as_ref())?;
    let readers = activations_reading(&state, id, &project, &targeted, &copies).await?;
    if let Some(busy) = readers.iter().find(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Activating) {
        return Err(StatusError::Other(
            StatusCode::PRECONDITION_FAILED,
            format!("trigger {} reading this infra is activating; cannot {}", busy.key, verb.as_str()),
        ));
    }
    // What happens to the executions running right now is the person's
    // answer, not the verb's. When triggers read this infra they gave it
    // in the same picker a plain deactivate uses; otherwise (no door to
    // close, nothing to park, hibernate or wipe, so no picker) the
    // body's own `runningPolicy` is the same flag. Either way `wait`
    // lets them finish (up to their cap) before the infra goes and
    // `cancel` ends them first; with nothing said, nothing waits.
    let (running_policy, drain_timeout_secs) = body.running.resolve(body.trigger_deactivation.as_ref());
    let live_readers: Vec<weft_core::activation::ActivationKey> = readers
        .iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .map(|a| a.key.clone())
        .collect();
    let was_active = !live_readers.is_empty();
    if was_active {
        let Some(deactivation) = body.trigger_deactivation.as_ref() else {
            return Err(StatusError::NeedsTriggerChoice(weft_core::trigger_choice_required(&format!(
                "{} reading this infra {} on and must come down before the {}",
                triggers_counted(live_readers.len()),
                if live_readers.len() == 1 { "is" } else { "are" },
                verb.as_str()
            ))));
        };
        crate::api::project::execute_trigger_deactivation(&state, id, live_readers, deactivation).await?;
    }
    settle_running_before_infra_op(&state, id, &copies, running_policy, was_active, None).await?;
    let command_id = issue_lifecycle_ensuring_supervisor(
        &state,
        id,
        None,
        &copies,
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

/// `POST /projects/{id}/infra/nodes/{node}/stop`, `{node}` being the
/// instance's place as a person spells it (`one.db`).
pub async fn stop_node(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    issue_per_node(state, id, node, InfraLifecycleVerb::Stop, body).await
}

/// `POST /projects/{id}/infra/nodes/{node}/terminate`; see `stop_node`.
pub async fn terminate_node(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    issue_per_node(state, id, node, InfraLifecycleVerb::Terminate, body).await
}

async fn issue_per_node(
    state: DispatcherState,
    id: uuid::Uuid,
    node: String,
    verb: InfraLifecycleVerb,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // Validation is in the type: serde rejected unknown variants at
    // deserialize. No picker on a per-node verb, so the body's answer
    // or the shared default (cancel): a wait is asked for, never
    // assumed, for one node exactly as for the project.
    let (running_policy, drain_timeout_secs) = body.running.resolve(None);
    // The place has to be a copy the program declares (a declared node
    // on the side `member` names: a member's copy of a per-member node,
    // the shared copy of a shared one), or one a live row still holds:
    // an orphan the user is taking down by hand, left behind when the
    // node was removed or changed side. A spelling that is neither would
    // enqueue a command for a row that cannot exist, and a 202 for it
    // would be a lie. The program is read once here and reused by the
    // dependent-trigger guard below.
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    // An orphan is no copy of the program's: no trigger reads it and no
    // run of the program reaches it, so neither the reader guard nor the
    // runs' settling below concern it.
    let orphan = match resolve_infra_nodes(&project, std::slice::from_ref(&node), body.member.as_ref()) {
        Ok(_) => false,
        Err(refusal) => {
            let held = infra_node::get(&state.pg_pool, id, &node, body.member.as_ref())
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node lookup: {e}")))?
                .is_some();
            if !held {
                return Err(refusal);
            }
            true
        }
    };
    // Surgical or not, the verb is the same destructive command the
    // project-level one is: it is held to the same reconciliation the
    // action bar renders, so a transitional project refuses it here
    // instead of tearing one node out from under a build.
    let action = match verb {
        InfraLifecycleVerb::Stop => "infra_stop",
        InfraLifecycleVerb::Terminate => "infra_terminate",
        other => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("{} is not a per-node verb", other.as_str()),
            ))
        }
    };
    if body.member.is_none() {
        crate::api::project::require_action(&state, id, None, &[action]).await?;
    }

    // Per-node verbs are surgical, not consent-bypassing. Refuse when a
    // trigger reading this copy is live: stopping or terminating it
    // would silently break that trigger. The caller takes those
    // triggers down first (or runs the project-level stop, which asks
    // how), then retries the per-node verb. Both sides are spelled per
    // place: the trigger under `one` depends on the instance under
    // `one`, and stopping `two.db` leaves it alone.
    let copies = weft_core::member::Copies::of(body.member.clone());
    let live_readers: Vec<String> = if orphan {
        Vec::new()
    } else {
        live_reader_keys(
            &activations_reading(&state, id, &project, &std::collections::BTreeSet::from([node.clone()]), &copies).await?,
        )
        .iter()
        .map(ToString::to_string)
        .collect()
    };
    if !live_readers.is_empty() {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            format!(
                "live triggers depend on infra node '{}': [{}]. Deactivate them (or run a \
                 project-level stop with trigger preservation) before per-node {}.",
                node,
                live_readers.join(", "),
                verb.as_str()
            ),
        ));
    }

    // force only applies to Stop (terminate removes everything anyway).
    let force = matches!(verb, InfraLifecycleVerb::Stop) && body.force;
    // No picker on a per-node verb (an active project's dependent
    // triggers refused it above; the rest run on), so nothing else
    // cancelled the running executions. Which executions use this one
    // instance is not something the journal records, so under cancel
    // every run the copy can reach is cancelled, the ones that never
    // touched it included: every run of the project for the shared
    // copy, every run of that member for a member's copy
    // (`take_down::runs_using_copies`). The verb's help says so, and
    // `wait` is the way to let them land first.
    if !orphan {
        settle_running_before_infra_op(&state, id, &copies, running_policy, false, None).await?;
    }
    let command_id = issue_lifecycle_ensuring_supervisor(
        &state,
        id,
        Some(&node),
        &copies,
        verb,
        running_policy,
        force,
        drain_timeout_secs,
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
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(SyncResponse {
        nodes: read_infra_entries(&state, id).await?,
    }))
}

/// One door serving right now, as `weft door --list` prints it.
#[derive(serde::Serialize)]
pub struct DoorEntry {
    /// The node as the program spells it.
    pub node: String,
    /// Whose copy: absent for the shared one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member: Option<weft_core::member::MemberId>,
    pub endpoint: String,
    /// The loopback port on the operator's machine. Not a URL: a door
    /// carries whatever protocol the endpoint speaks, and most of them
    /// are not HTTP.
    pub port: u16,
}

#[derive(serde::Serialize)]
pub struct DoorsResponse {
    pub doors: Vec<DoorEntry>,
}

/// The doors this project has SERVING, read from the cluster rather
/// than from a row of ours.
///
/// The apiserver owns these numbers: it is what allocated them and
/// what refuses a duplicate, so it is the honest place to ask. A row
/// would be a second copy, free to go stale the moment an apply
/// changed one.
pub async fn doors(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<DoorsResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let rows = infra_node::list_for_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    let held = state
        .kube
        .node_ports()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("read the cluster's node ports: {e}")))?;
    let mut doors = Vec::new();
    for row in rows {
        for endpoint in row.endpoints.keys() {
            // The Service the compiler emits for a door on this
            // endpoint. Present means serving; absent means the node
            // has no door there.
            let name = weft_core::infra::door_service_name(&row.instance_id, endpoint);
            // Namespace AND name: `node_ports` is cluster wide (a node
            // port is), so a name on its own would answer with another
            // tenant's Service. We are holding this project's namespace
            // already, and the holder carries one, so there is no
            // reason to key on the name alone.
            let Some(holder) = held
                .iter()
                .find(|h| h.namespace == row.namespace && h.service == name)
            else {
                continue;
            };
            doors.push(DoorEntry {
                node: row.node_id.clone(),
                member: row.member.clone(),
                endpoint: endpoint.clone(),
                port: holder.port,
            });
        }
    }
    Ok(Json(DoorsResponse { doors }))
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
    Path((id, cmd_id)): Path<(uuid::Uuid, i64)>,
    Query(hold): Query<super::HoldQuery>,
) -> Result<Json<CommandStatusResponse>, (StatusCode, String)> {
    // Scope the command read to this project: the command is looked up
    // by `(id, project_id)`, so a caller can't read another project's
    // command outcome by enumerating the sequential id.
    authorize_project(&state, &caller.0, id).await?;
    use infra_lifecycle_command::WaitOutcome;
    let outcome = infra_lifecycle_command::held_command_outcome(
        &state.pg_pool,
        &state.signals,
        id,
        cmd_id,
        hold.hold(),
    )
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

/// Whose copy a per-node read or press is about, or whose in-flight
/// work a cancel halts: absent for the shared one.
#[derive(Debug, Default, Deserialize)]
pub struct CopyQuery {
    #[serde(default)]
    pub member: Option<weft_core::member::MemberId>,
}

/// `GET /projects/{id}/infra/nodes/{node}/live`, `{node}` being the
/// instance's place as a person spells it (`one.db`).
pub async fn live(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    Query(copy): Query<CopyQuery>,
) -> Result<Json<weft_core::live::LiveFeed>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(read_live(&state, id, &node, copy.member.as_ref()).await?))
}

/// What an infra node's container is showing right now.
///
/// The container's `/live` answer, read into the one display shape on
/// the way through: an answer that is not `{ "items": [...] }` is a
/// 502 naming what it sent, and an item that does not fit becomes a
/// line saying so. The caller has already been authorized for the
/// project (the editor by its project token, an outside consumer by
/// its signal token), so this is the one implementation both doors
/// share.
pub(crate) async fn read_live(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    member: Option<&weft_core::member::MemberId>,
) -> Result<weft_core::live::LiveFeed, (StatusCode, String)> {
    let endpoint_url = live_endpoint_url(state, id, node, member).await?;
    let live_url = format!("{}/live", endpoint_url.trim_end_matches('/'));
    // Reuse the dispatcher's shared HTTP client (one connection pool for the
    // process, not a fresh pool per request). Bound the WHOLE exchange, connect +
    // headers + body, with a single 3s deadline so a downstream node that accepts
    // the connection then trickles the body can't pin the request open.
    let answer = tokio::time::timeout(std::time::Duration::from_secs(3), async {
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
    // Read it into the one display shape here, at the door, so a
    // container serving something else is a loud 502 naming what it
    // sent rather than an empty panel every reader has to interpret.
    weft_core::live::LiveFeed::from_answer(&answer)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("the container's /live: {e}")))
}

/// Body for `/infra/nodes/{node}/action`: the button a `/live` item
/// carries, pressed.
///
/// The one press body on this side of weft, shared by the editor's
/// door here and the token door at `/signal-token/displays/.../action`,
/// so a node's author writes one `/action` handler and both reach it.
/// Only an INFRA node's display has buttons; a trigger's is read-only.
// SYNC: InfraActionBody <-> crates/weft-core/src/live.rs LiveAction, packages/weft-graph/src/protocol.ts LiveDataItem.action
#[derive(Debug, Deserialize)]
pub struct InfraActionBody {
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// POST /projects/{id}/infra/nodes/{node}/action, `{node}` being the
/// instance's place as a person spells it (`one.db`): press a button a
/// `/live` item offered. The container serving `/live` also serves
/// `/action` with the bridge envelope (`{ "action", "payload" }` in,
/// `{ "result" }` out; a `result.error` is the container refusing),
/// and this route carries the press there, so a container's own
/// action (log a phone out, rotate a key) needs nothing in weft beyond
/// the button on its `/live` item. The container's refusal comes back
/// as 400 with its text, whether it refused in the envelope or with a
/// 4xx of its own; an unreachable container or a 5xx is a 502.
///
/// SYNC: the /action envelope <-> catalog/bailey/bridge/images/bridge/src/actions.js, catalog/postgres/database/images/credential/bootstrap.py The press waits as
/// long as the container takes: the work is the container's and the
/// wait the user's, so no deadline is put on it here (the editor shows
/// the button pressed until the answer lands).
pub async fn action(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, node)): Path<(uuid::Uuid, String)>,
    Query(copy): Query<CopyQuery>,
    Json(body): Json<InfraActionBody>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(press_live(&state, id, &node, copy.member.as_ref(), &body.kind, &body.payload).await?))
}

/// Press a button one of an infra node's `/live` items carries. The
/// caller has already been authorized for the project; both doors onto
/// a node's display land here.
pub(crate) async fn press_live(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    member: Option<&weft_core::member::MemberId>,
    kind: &str,
    payload: &serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let endpoint_url = live_endpoint_url(state, id, node, member).await?;
    let action_url = format!("{}/action", endpoint_url.trim_end_matches('/'));
    let resp = state
        .http
        .post(&action_url)
        .json(&serde_json::json!({ "action": kind, "payload": payload }))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("action send: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("action read: {e}")))?;
    if !status.is_success() {
        // A 4xx is the container saying the press was wrong (an action
        // it does not have, a missing field), which is the caller's
        // problem and comes back as one. Only a 5xx or an unreachable
        // container is a gateway fault.
        let code = if status.is_client_error() {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::BAD_GATEWAY
        };
        return Err((code, format!("the container answered {status}: {text}")));
    }
    // The press changed what the node shows (a fresh QR code, a new
    // key): an editor watching it through any pod sees it now rather
    // than at the next look.
    crate::display_feeds::DisplayFeeds::announce_look_now(
        &state.pg_pool,
        &crate::display_feeds::DisplayKey {
            project: id,
            source: crate::display_feeds::DisplaySource::Infra,
            node: node.to_string(),
            member: member.cloned(),
        },
    )
    .await;
    let answer = serde_json::from_str::<serde_json::Value>(&text)
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("action parse: {e}")))?;
    infra_action_result(kind, answer)
}

/// The `result` of a container's `/action` answer, or the refusal it
/// carried (`result.error`) as a 400 naming the action. An answer with
/// no `result` at all is not the envelope: a container that answered
/// 200 with something else is reported as such (502), never read as an
/// action that succeeded with nothing to say.
fn infra_action_result(
    kind: &str,
    answer: serde_json::Value,
) -> Result<serde_json::Value, (StatusCode, String)> {
    let Some(result) = answer.get("result").cloned() else {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("{kind}: the container answered without a `result` envelope: {answer}"),
        ));
    };
    if let Some(err) = result.get("error").and_then(|e| e.as_str()) {
        return Err((StatusCode::BAD_REQUEST, format!("{kind}: {err}")));
    }
    Ok(result)
}

/// The URL of the endpoint an infra node names as serving `/live` (and
/// `/action`): the node must opt in through `features.live_endpoint`,
/// and the endpoint must be provisioned. Every miss is a 404 naming
/// which of those it is, so a TCP-only node (Postgres) answers "no
/// live endpoint" instead of a 502 from a refused connection.
pub(crate) async fn live_endpoint_url(
    state: &DispatcherState,
    id: uuid::Uuid,
    node: &str,
    member: Option<&weft_core::member::MemberId>,
) -> Result<String, (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    // `node` is a place spelling; the node behind it says whether it
    // serves a display, and the row under that spelling says where.
    let (node_id, _) = weft_core::project::resolve_address(&project, node);
    let node_def = project
        .nodes
        .iter()
        .find(|n| n.id == node_id)
        .ok_or((StatusCode::NOT_FOUND, "no such node in project".into()))?;
    let live_endpoint = node_def.features.live_endpoint.as_deref().ok_or((
        StatusCode::NOT_FOUND,
        "node does not expose a /live endpoint".to_string(),
    ))?;
    let row = infra_node::get(&state.pg_pool, id, node, member)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node lookup: {e}")))?
        .ok_or_else(|| match member {
            Some(member) => (StatusCode::NOT_FOUND, format!("member '{member}' has no copy of infra node '{node}'")),
            None => (StatusCode::NOT_FOUND, "no such infra node".to_string()),
        })?;
    // A copy that is not up has nothing behind its address (a stopped
    // one is scaled to zero, a starting one has no ready Pod yet), so
    // asking it would be a connection error. Nothing to show is a 404,
    // naming the state so a reader can say "starting" rather than
    // "not started".
    if !matches!(row.status, InfraNodeStatus::Running | InfraNodeStatus::Flaky) {
        return Err((
            StatusCode::NOT_FOUND,
            format!("infra node '{node}' is {}, so it has nothing to show yet", row.status.as_str()),
        ));
    }
    row.endpoints.get(live_endpoint).cloned().ok_or((
        StatusCode::NOT_FOUND,
        format!("infra node has no endpoint named '{live_endpoint}' (live_endpoint)"),
    ))
}

// =================================================================
// Helpers
// =================================================================

/// Every copy as `weft status` and a program's `ctx.infra(..).status()`
/// show it (`infra_node::observe`): a start or a stop under way reads as
/// such before the supervisor reaches the copy.
async fn read_infra_entries(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<Vec<InfraStatusEntry>, (StatusCode, String)> {
    let project = state
        .projects
        .project(project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let copies = infra_node::observe(&state.pg_pool, project_id, &project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let mut entries: Vec<InfraStatusEntry> =
        copies.rows.into_iter().map(|row| row_to_entry(row, &state.public_base_url)).collect();
    entries.extend(copies.starting.into_iter().map(|(node, member)| InfraStatusEntry {
        node,
        member,
        status: infra_node::InfraNodeStatus::Provisioning.as_str().to_string(),
        endpoint_url: None,
        public_urls: Default::default(),
        failure_stage: None,
        failure_message: None,
    }));
    Ok(entries)
}

fn row_to_entry(row: InfraNodeRow, front_door: &str) -> InfraStatusEntry {
    InfraStatusEntry {
        public_urls: row
            .public_paths
            .iter()
            .map(|(name, path)| (name.clone(), weft_core::infra::tenant_public_url(front_door, path)))
            .collect(),
        node: row.node_id,
        member: row.member,
        status: row.status.as_str().to_string(),
        // Coarse UI hint: the first endpoint by name (BTreeMap, so
        // deterministic). Node code resolves a specific endpoint by
        // name via ctx.endpoint(...); this is just a status summary.
        endpoint_url: row.endpoints.values().next().cloned(),
        failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
        failure_message: row.failure_message,
    }
}

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
    let tenant = state
        .tenant_router
        .tenant_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let project_namespace =
        crate::project_namespace::name_for(&state.instance, tenant.as_str(), id);
    let args = crate::project_namespace::ProjectNamespaceArgs {
        project_id: id,
        tenant_id: tenant.as_str(),
        namespace: &project_namespace,
        pod_cidr: &state.cluster_pod_cidr,
        service_cidr: &state.cluster_service_cidr,
        instance: &state.instance,
    };
    crate::project_namespace::ensure(&*state.kube, &args)
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
/// infra state remains, delete the per-project namespace and clear the
/// row pointing at it. Runs at the sync landing flip, AFTER
/// `reconcile_worker` has kill-then-respawned the worker into the
/// shared pool, so we never delete a namespace that still hosts the
/// project's worker.
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
/// live-advertised dead namespace. Nothing else has to be retired in
/// step with the namespace object: a worker inside it is identified by
/// its pod, not by where it sits, so a namespace that lingers while it
/// terminates takes nobody's identity with it.
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
    let existing = state
        .projects
        .project_namespace(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project_namespace: {e}")))?
        .unwrap_or_default();
    if existing.is_empty() {
        return Ok(());
    }
    if crate::infra_node::any_for_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?
    {
        tracing::warn!(
            target: "weft_dispatcher::api::infra",
            project_id = %id,
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
    // Nothing about a worker's identity hangs off this namespace, so
    // there is no record to retire in step with it: a worker is known
    // by its pod, whose row outlives the pod itself.
    if let Err(e) = crate::project_namespace::delete(&*state.kube, &existing).await {
        tracing::warn!(
            target: "weft_dispatcher::api::infra",
            error = %e,
            project_id = %id,
            "delete now-infra-less project namespace failed (continuing); \
             row already cleared so no supervisor manages it"
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
pub(crate) async fn issue_lifecycle_ensuring_supervisor(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    node_id: Option<&str>,
    copies: &weft_core::member::Copies,
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
        copies,
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

/// One command per node for `nodes` of `copies` (an upgrade's stop leg
/// over exactly the nodes it re-applies), the supervisor ensured first.
async fn issue_per_nodes_ensuring_supervisor(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    nodes: &std::collections::BTreeSet<String>,
    copies: &weft_core::member::Copies,
    verb: InfraLifecycleVerb,
    running_policy: RunningPolicy,
    force: bool,
    drain_timeout_secs: u64,
) -> Result<Vec<i64>, (StatusCode, String)> {
    let mut ids = Vec::with_capacity(nodes.len());
    for node in nodes {
        ids.push(
            issue_lifecycle_ensuring_supervisor(state, project_id, Some(node), copies, verb, running_policy, force, drain_timeout_secs)
                .await?,
        );
    }
    Ok(ids)
}

/// Reap infra_node rows whose place is no longer one the project
/// source puts a `requires_infra` node at: the node was deleted, or the
/// include that reached it was (a file included twice and then once
/// leaves the second call's instance behind). Step 1 of the sync
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
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let declared = weft_core::project::infra_place_spellings(&project);
    let per_member_of = |spelled: &str| {
        let (node_id, _) = weft_core::project::resolve_address(&project, spelled);
        project.nodes.iter().any(|n| n.id == node_id && n.per_member.is_some())
    };
    let rows = crate::infra_node::list_for_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node list: {e}")))?;
    // A copy the source no longer declares: the node is gone, or it
    // changed sides (a node no longer per member leaves its members'
    // copies behind, a node now per member leaves its shared one).
    let orphans: Vec<(String, Option<weft_core::member::MemberId>)> = rows
        .into_iter()
        .filter(|r| !declared.contains(&r.node_id) || per_member_of(&r.node_id) != r.member.is_some())
        .map(|r| (r.node_id, r.member))
        .collect();
    if orphans.is_empty() {
        return Ok(());
    }
    let tenant = state
        .tenant_router
        .tenant_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Step 1: issue every terminate in parallel. issue_lifecycle is
    // a single INSERT; bundling them keeps DB roundtrip cost flat
    // regardless of orphan count.
    let issue_futures = orphans.iter().map(|(node_id, member)| {
        let tenant_str = tenant.as_str().to_string();
        let node_id = node_id.clone();
        let copies = weft_core::member::Copies::of(member.clone());
        let pool = state.pg_pool.clone();
        let pod = state.pod_id.as_str().to_string();
        async move {
            let res = infra_lifecycle_command::issue_lifecycle(
                &pool,
                &tenant_str,
                id,
                Some(&node_id),
                &copies,
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
                    project_id = %id,
                    node_id = %node_id,
                    error = %e,
                    "orphan reap: failed to issue terminate; skipping"
                );
            }
        }
    }
    let outcomes =
        infra_lifecycle_command::wait_for_commands(&state.pg_pool, &state.signals, &cmd_ids, deadline)
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
                project_id = %id,
                node_id = %node_id,
                "orphan terminated"
            ),
            infra_lifecycle_command::WaitOutcome::Failed { error } => tracing::warn!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                error = %error,
                "orphan reap: supervisor reported error"
            ),
            infra_lifecycle_command::WaitOutcome::Cancelled { reason } => tracing::info!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                reason = %reason,
                "orphan reap: command cancelled (likely raced a node removal)"
            ),
            infra_lifecycle_command::WaitOutcome::Timeout => tracing::warn!(
                target: "weft_dispatcher::api::infra",
                project_id = %id,
                node_id = %node_id,
                "orphan reap: supervisor did not complete within 60s"
            ),
        }
    }
    Ok(())
}

/// Project deletion entry point. Called by `weft rm`.
///
/// For a project with infra, issues a `Terminate` of every copy (the
/// shared ones and each member's) and waits up to 120s for the
/// supervisor to complete it. Then removes the connections the
/// project's nodes published, deletes the project namespace (which
/// takes any leftover resources with it), and releases the project's
/// supervisor lease. The `infra_*` rows go with the project row itself
/// (`ProjectStore::remove`), not here.
///
/// `force = true` (i.e. `weft rm --force`) skips the wait: the
/// dispatcher proceeds immediately. Any in-flight supervisor work
/// for the project errors on its kubectl calls because the namespace
/// is gone; the supervisor logs but doesn't retry.
pub async fn delete_project(
    state: &DispatcherState,
    id: uuid::Uuid,
    tenant: &str,
    force: bool,
) -> Result<(), (StatusCode, String)> {
    // Step 0: does this project have infra at all? A no-infra project has NOTHING
    // for the supervisor to terminate, so enqueuing a Terminate + waiting on it is
    // pure waste: no supervisor owns the project, the command is never marked
    // complete, and the wait below burns the full 120s timeout on EVERY no-infra
    // `weft rm` (the common case, and most e2e teardowns). Skip the supervisor
    // round-trip entirely; go straight to the broker-row cleanup. `None` (project
    // already unregistered) is also "nothing to terminate".
    let has_infra = state
        .projects
        .project_has_infra(id)
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
            id,
            None,
            &weft_core::member::Copies::Every,
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
            &state.signals,
            cmd_id,
            std::time::Duration::from_secs(120),
        )
        .await
        {
            Ok(infra_lifecycle_command::WaitOutcome::Failed { error }) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    error = %error,
                    "supervisor reported terminate failure; continuing with rm cleanup"
                );
            }
            Ok(infra_lifecycle_command::WaitOutcome::Cancelled { reason }) => {
                tracing::info!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    reason = %reason,
                    "terminate cancelled (race with node removal); continuing with rm cleanup"
                );
            }
            Ok(infra_lifecycle_command::WaitOutcome::Succeeded) => {}
            Ok(infra_lifecycle_command::WaitOutcome::Timeout) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    "supervisor did not complete terminate within 120s; \
                     continuing with rm cleanup (orphans will be swept by the \
                     next supervisor sweep cycle)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::infra",
                    project_id = %id,
                    error = %e,
                    "wait_for_command errored; continuing with rm cleanup"
                );
            }
        }
        }
    }
    // Step 3: the project's infra rows (`infra_node`, `infra_event`,
    // `infra_lifecycle_command`) are NOT dropped here: they go with the
    // project row itself, in `ProjectStore::remove`'s one transaction,
    // because the broker inserts commands and events for as long as the
    // project row is visible. Dropped earlier, a health tick or a
    // worker's apply landing in between left rows nobody would ever run
    // or read.
    // The connections this project's nodes published opened services
    // this project ran; with the project gone they name nothing. The
    // per-node cleanup on terminate does not cover this path (a forced
    // delete does not WAIT for the terminate to land, and a project
    // with no infra rows never issues one at all), and a credential
    // nobody can place is exactly the junk the cleanup rule forbids. Connections a PERSON
    // connected are untouched: those are theirs.
    weft_access_store::delete_published_grants(&state.pg_pool, tenant, id, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("delete the connections this project's nodes published: {e}"),
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
    let namespace = state.projects.project_namespace(id).await.map_err(|e| {
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
                project_id = %id,
                "clear project_namespace row failed (continuing)"
            );
        }
        if let Err(e) = project_namespace::delete(&*state.kube, &ns).await {
            tracing::warn!(
                target: "weft_dispatcher::api::infra",
                error = %e,
                project_id = %id,
                "delete project namespace failed (continuing); row already cleared"
            );
        }
    }
    // Step 5: release the project's exclusive supervisor lease
    // (`infra_owner`). AFTER the namespace-row clear on purpose: the
    // broker's claim path only offers projects with a non-empty
    // `project_namespace`, so with the row cleared a released project
    // cannot be re-claimed mid-teardown (release-first left a window
    // where a supervisor re-adopted the dying project and started
    // reconciling its infra). Left behind, the owning supervisor would
    // renew a lease on a ghost forever: it never becomes idle, the pool
    // never drains to zero, and rows accumulate one per removed project.
    crate::supervisor_pool::release_project(&state.pg_pool, id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("supervisor_pool::release_project: {e}"),
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// An older client asking for an upgrade through `/infra/sync` is
    /// refused naming the upgrade route, never handed a plain start.
    #[test]
    fn a_sync_body_carrying_upgrade_keys_is_refused() {
        for body in [json!({ "upgrade": true }), json!({ "triggerDeactivation": { "all": true } })] {
            let parsed: SyncBody = serde_json::from_value(body).unwrap();
            let (code, why) = parsed.into_sync().unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST);
            assert!(why.contains("/infra/upgrade"), "{why}");
        }
        let plain: SyncBody = serde_json::from_value(json!({ "binaryHash": "h" })).unwrap();
        assert_eq!(plain.into_sync().unwrap().binary_hash.as_deref(), Some("h"));
    }

    #[test]
    fn sync_request_defaults() {
        let r: SyncRequest = serde_json::from_value(json!({})).unwrap();
        assert!(r.binary_hash.is_none());
        assert!(r.definition_hash.is_none());
        assert!(r.infra_hash.is_none());
        assert!(r.image_hashes.is_none(), "absent means keep the stored map");
        assert_eq!(r.running, RunningChoice::default());
    }

    #[test]
    fn sync_request_running_policy_round_trips() {
        let r: SyncRequest =
            serde_json::from_value(json!({ "runningPolicy": "cancel", "drainTimeoutSecs": 30 })).unwrap();
        assert_eq!(r.running.running_policy, Some(RunningPolicy::Cancel));
        assert_eq!(r.running.drain_timeout_secs, Some(30));
        // ONE wire spelling: snake_case is an unknown field.
        let r: SyncRequest =
            serde_json::from_value(json!({ "running_policy": "cancel" })).unwrap();
        assert_eq!(r.running.running_policy, None);
    }

    #[test]
    fn sync_request_parses_camelcase_only() {
        let camel: UpgradeRequest = serde_json::from_value(json!({
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
        assert_eq!(camel.sync.binary_hash.as_deref(), Some("abc"));
        assert_eq!(camel.sync.definition_hash.as_deref(), Some("def0"));
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
        let bad_inner: Result<UpgradeRequest, _> = serde_json::from_value(json!({
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
        assert_eq!(r.running, RunningChoice::default());
    }

    /// A stop of an INACTIVE project shows no picker, so the body's own
    /// answer is the only way to ask it to wait, and it has to reach
    /// the handler.
    #[test]
    fn stop_request_carries_its_own_running_choice() {
        let r: StopRequest =
            serde_json::from_value(json!({ "runningPolicy": "wait", "drainTimeoutSecs": 1800 })).unwrap();
        assert_eq!(r.running.resolve(None), (RunningPolicy::Wait, 1800));
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
        assert_eq!(r.running, RunningChoice::default());
        assert!(!r.force);
    }

    #[test]
    fn per_node_request_running_policy_round_trips() {
        let r: PerNodeRequest =
            serde_json::from_value(json!({"runningPolicy": "cancel", "force": true})).unwrap();
        assert_eq!(r.running.running_policy, Some(RunningPolicy::Cancel));
        assert!(r.force);
        // ONE wire spelling: a snake_case key is an unknown field and
        // must not populate the (defaulted) field.
        let r: PerNodeRequest =
            serde_json::from_value(json!({"running_policy": "wait"})).unwrap();
        assert_eq!(r.running.running_policy, None, "snake_case must not populate the field");
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
        let hashes = r.image_hashes.as_ref().expect("sent");
        assert_eq!(hashes.len(), 2);
        assert_eq!(
            hashes.get("tgi").unwrap().get("bridge").unwrap(),
            "weft-infra-bridge:abc123"
        );
    }

    #[test]
    fn an_action_answer_is_its_result_or_the_refusal_it_carried() {
        use super::infra_action_result;
        let ok = infra_action_result("logout", serde_json::json!({ "result": { "success": true } })).unwrap();
        assert_eq!(ok, serde_json::json!({ "success": true }));
        let (status, msg) = infra_action_result(
            "logout",
            serde_json::json!({ "result": { "error": "WhatsApp not connected" } }),
        )
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(msg, "logout: WhatsApp not connected");
        // No `result` at all is not the envelope: reported, never read as success.
        let (status, msg) = infra_action_result("x", serde_json::json!({ "ok": true })).unwrap_err();
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(msg.contains("without a `result` envelope"), "{msg}");
    }
}
