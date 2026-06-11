//! Infra lifecycle endpoints.
//!
//! Three project-scoped verbs (Start / Restart / Upgrade all map to
//! the same `/sync` endpoint), two project-scoped destroy verbs
//! (`/stop` and `/terminate`), and two per-node destroy verbs for
//! partial-state recovery.
//!
//! `/sync` runs an `InfraSetup` subworkflow exec: the worker walks
//! every `requires_infra` node + its upstream closure; each infra
//! node calls `Node::provision`. The engine then makes a local
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

use crate::infra_lifecycle_command::{self, InfraLifecycleVerb, RunningPolicy};
use crate::infra_node::{self, InfraNodeRow};
use crate::project_namespace;
use crate::state::DispatcherState;

// =================================================================
// Sync request (Start / Restart / Upgrade)
// =================================================================

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
    /// Used by `ApplyInfraExecutor` to resolve `Image::Local { name }`
    /// references to concrete docker tags at compile time.
    #[serde(default, rename = "imageHashes")]
    pub image_hashes: BTreeMap<String, BTreeMap<String, String>>,
    /// How to deactivate triggers when the project is currently
    /// Active. Required (412 otherwise) when project is Active;
    /// ignored when Inactive. Carries the same `DeactivateSpec`
    /// shape as the standalone `/deactivate` endpoint, so clients
    /// reuse one picker UI.
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<weft_broker_client::protocol::DeactivateSpec>,
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
}

// =================================================================
// Handlers
// =================================================================

/// Arm the sync-in-flight sentinel under a per-tenant xact-scoped
/// advisory lock. Delegates to `ProjectStoreOps::arm_sync_with_advisory_lock`
/// so the trait surface stays the single I/O boundary (layer-3
/// rigs that fake `ProjectStoreOps` see the call).
///
/// xact-scoped means the lock auto-releases on COMMIT; no
/// session-leak back into the connection pool.
async fn arm_sync_sentinel(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    tenant_id: &str,
) -> anyhow::Result<()> {
    let key = crate::lease::advisory_key(
        crate::lease::SUPERVISOR_COORD_DOMAIN,
        tenant_id,
    );
    let until = crate::lease::now_unix() + crate::lease::SENTINEL_TTL_SECS;
    state
        .projects
        .arm_sync_with_advisory_lock(project_id, key, until)
        .await
}

pub async fn sync(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<SyncRequest>>,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
    let body = body.map(|Json(b)| b).unwrap_or_default();

    // Sentinel + xact lock: mark "sync in flight" so the reaper
    // doesn't scale the tenant's supervisor down while this
    // handler runs. The TTL is short (SENTINEL_TTL_SECS); a
    // background TtlHeartbeat re-arms it for the duration of
    // sync, so user-code runtime (which can be unbounded inside
    // run_infra_setup) doesn't bump against the TTL. Dispatcher
    // liveness alone bounds the wait: if THIS pod dies, the
    // heartbeat stops, the sentinel expires in <= TTL, the
    // reaper proceeds.
    //
    // Why xact-scoped + sentinel instead of a session-held lock:
    // sqlx pool connections don't release session-scoped advisory
    // locks on PoolConnection drop; they survive on the backend
    // until the session closes. That leaks locks across unrelated
    // pool users. Xact-scoped locks auto-release on COMMIT.
    let tenant_id = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_for: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    arm_sync_sentinel(&state, id, &tenant_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("arm_sync_sentinel: {e}")))?;
    let _heartbeat = {
        let state_clone = state.clone();
        let tenant_clone = tenant_id.clone();
        crate::lease::TtlHeartbeat::spawn(
            "SyncSentinel",
            crate::lease::heartbeat_interval(),
            move || {
                let state = state_clone.clone();
                let tenant = tenant_clone.clone();
                async move {
                    arm_sync_sentinel(&state, id, &tenant).await
                }
            },
        )
    };
    sync_inner(state, id, body).await
}

async fn sync_inner(
    state: DispatcherState,
    id: uuid::Uuid,
    body: SyncRequest,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    let project_id = id.to_string();

    // Binary / definition / infra-hash writes are load-bearing:
    // the supervisor's apply-hash compute reads them on the next
    // tick to decide skip/fresh/replace, and
    // `replace_stale_worker_if_needed` (below) uses the binary_hash
    // to decide whether to kill the running pod. One ATOMIC write for
    // the trio: separate statements opened a window where a crash (or
    // a sibling Pod's /run between them) saw a new binary hash paired
    // with an old definition hash.
    state
        .projects
        .set_running_hashes(
            id,
            body.binary_hash.as_deref(),
            body.definition_hash.as_deref(),
            body.infra_hash.as_deref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("set_running_hashes: {e}")))?;
    // A binary-hash change means the worker pod's baked-in
    // engine/node code is stale. Kill it now so the next
    // worker-target task (the InfraSetup `execute` enqueued below by
    // `run_infra_setup`) triggers cold_start to spawn a fresh pod
    // with the new image; definition changes don't need this (the
    // worker re-fetches the definition per execution by hash).
    // Without the kill, the stale pod happily claims the task before
    // cold_start ever notices.
    crate::api::project::replace_stale_worker_if_needed(&state, &project_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("replace_stale_worker_if_needed: {e}"),
            )
        })?;
    for (node_id, tags) in &body.image_hashes {
        let tags_map: std::collections::HashMap<String, String> =
            tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        state
            .projects
            .set_infra_image_tags(id, node_id, tags_map)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("set_infra_image_tags(node={node_id}): {e}"),
                )
            })?;
    }

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

    // When the project is currently Active, the user has to tell us
    // how to deactivate triggers (wipe / hibernate / park + running
    // policy). That choice lives in `triggerDeactivation`. A missing
    // choice on an Active project is a client bug: respond with 412
    // so the CLI / extension knows to prompt and retry.
    let was_active = matches!(lifecycle.status, crate::project_store::ProjectStatus::Active);
    if was_active {
        let Some(deactivation) = body.trigger_deactivation.as_ref() else {
            return Err((
                StatusCode::PRECONDITION_REQUIRED,
                "project is active; triggerDeactivation { mode, runningPolicy, graceMinutes? } \
                 required so the user can choose how to deactivate triggers (same picker as \
                 the standalone Deactivate verb)"
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
    // that enqueues a lifecycle command (orphan reap, run_infra_setup).
    ensure_supervisor_for_project(&state, &project_id)
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

    // run_infra_setup short-circuits internally when the project has
    // no requires_infra nodes. It also computes the upstream-closure
    // root seeds itself so callers don't have to know the seeding
    // contract.
    if let Err(err) = crate::api::project::run_infra_setup(&state, id).await {
        return Err(err);
    }

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
    Path(id_str): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    issue_destroy(state, id_str, InfraLifecycleVerb::Stop, body).await
}

pub async fn terminate(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
    body: Option<Json<StopRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
    issue_destroy(state, id_str, InfraLifecycleVerb::Terminate, body).await
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
        // Apply is never routed through issue_destroy (the engine
        // enqueues ApplyInfra tasks directly). Deactivate /
        // Reactivate are dispatcher-owned and don't take this path
        // either. If we get here, a caller wired a new verb without
        // updating this match.
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
    let command_id =
        issue_lifecycle_ensuring_supervisor(&state, &project_id, None, verb, running_policy, false)
            .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(LifecycleCommandIssued { command_id }),
    ))
}

pub async fn stop_node(
    State(state): State<DispatcherState>,
    Path((id_str, node_id)): Path<(String, String)>,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
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
    Path((id_str, node_id)): Path<(String, String)>,
    body: Option<Json<PerNodeRequest>>,
) -> Result<(StatusCode, Json<LifecycleCommandIssued>), (StatusCode, String)> {
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
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(LifecycleCommandIssued { command_id }),
    ))
}

pub async fn status(
    State(state): State<DispatcherState>,
    Path(id_str): Path<String>,
) -> Result<Json<SyncResponse>, (StatusCode, String)> {
    let project_id = parse_id(&id_str)?.to_string();
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
    Path((id_str, cmd_id)): Path<(String, i64)>,
) -> Result<Json<CommandStatusResponse>, (StatusCode, String)> {
    // Scope the command read to this project: the command is looked up
    // by `(id, project_id)`, so a caller can't read another project's
    // command outcome by enumerating the sequential id.
    let project_id = parse_id(&id_str)?.to_string();
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
    Path((id_str, node_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = parse_id(&id_str)?;
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
    let client = reqwest::Client::new();
    let resp = client
        .get(&live_url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live fetch: {e}")))?;
    let value: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, format!("live parse: {e}")))?;
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
/// Lazy supervisor spawn. Renders + applies the per-tenant
/// `weft-infra-supervisor` Deployment in the tenant namespace.
/// Idempotent: kubectl apply on the same manifest is a no-op when
/// the cluster already matches. Called at the top of sync before
/// any orphan reap or Apply command enqueue.
async fn ensure_supervisor_for_project(
    state: &DispatcherState,
    project_id: &str,
) -> anyhow::Result<()> {
    let tenant = state.tenant_router.tenant_for_project(project_id);
    let namespace = state.namespace_mapper.namespace_for(&tenant);
    crate::tenant_namespace::ensure_supervisor_deployment(
        &*state.kube,
        &namespace,
        tenant.as_str(),
        &state.supervisor_image,
    )
    .await
    .map_err(|e| {
        anyhow::anyhow!(
            "ensure_supervisor_deployment(tenant={}): {e}",
            tenant.as_str()
        )
    })
}

/// Enqueue a lifecycle command AFTER making sure the per-tenant
/// supervisor is alive. The supervisor's claim loop only runs when
/// its Deployment has replicas=1, so an enqueue with the reaper
/// having scaled the supervisor down would sit unclaimed forever.
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
) -> Result<i64, (StatusCode, String)> {
    ensure_supervisor_for_project(state, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("ensure_supervisor: {e}")))?;
    let tenant = state.tenant_router.tenant_for_project(project_id);
    infra_lifecycle_command::issue_lifecycle(
        &state.pg_pool,
        tenant.as_str(),
        project_id,
        node_id,
        verb,
        running_policy,
        force,
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
    let tenant = state.tenant_router.tenant_for_project(&project_id);

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
    // Step 4: delete the k8s namespace. Like step 2, only logged on
    // error: a missing namespace is a no-op, and a transient kubectl
    // failure leaves a tenant-empty namespace that the next sync
    // will repurpose (or the user can manually `kubectl delete ns`).
    let namespace = state.projects.project_namespace(&project_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("project_namespace: {e}"),
        )
    })?;
    if let Some(ns) = namespace {
        if let Err(e) = project_namespace::delete(&*state.kube, &ns).await {
            tracing::warn!(
                target: "weft_dispatcher::api::infra",
                error = %e,
                project_id = %project_id,
                "delete project namespace failed (continuing)"
            );
        }
    }
    Ok(())
}
