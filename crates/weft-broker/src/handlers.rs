//! Per-endpoint handlers. Each one:
//!   1. Lifts the authenticated `CallerIdentity` out of the request.
//!   2. Runs a scope check (the security-critical bit).
//!   3. Delegates to the underlying Postgres-direct client.
//!
//! Steps 2 and 3 are intentionally separate calls per handler so the
//! audit log records the exact `(caller, scope-kind, requested,
//! resource-tenant)` tuple.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    Json,
};
use weft_broker_client::protocol::*;
use weft_task_store::tasks::{ClaimFilter, DedupOutcome, TaskTarget};
use weft_task_store::TaskKind;

use crate::auth::{AuthedCaller, CallerIdentity, Role};
use crate::scope;
use crate::state::BrokerState;

type Resp<T> = Result<Json<T>, (StatusCode, String)>;

pub async fn health() -> &'static str {
    "ok"
}

// ---------- Journal ----------

pub async fn journal_record(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalRecordRequest>,
) -> Resp<JournalRecordResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers journal events".into()));
    }
    let color = req.event.color();
    scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, &color.to_string())
        .await?;
    // Pod-name binding: the caller can only journal under its own
    // bound pod. Without this check, a worker could stamp a sibling's
    // pod_name and either bypass fencing (if the sibling is alive) or
    // poison attribution. The kubelet stamps `caller.pod_name` into
    // the projected SA token; it's unforgeable from inside the pod.
    let claimed_pod = req.pod_name.as_str();
    require_pod_name_matches(&caller, claimed_pod)?;
    // Cross-color sabotage gate: the color's owning pod (stamped at
    // first task_claim_one) must match the caller's bound pod. A
    // compromised tenant pod can journal-write only under colors it
    // legitimately owns, not arbitrary sibling colors in the same
    // tenant. `owner_pod_name IS NULL` means the color has not been
    // claimed yet (e.g. dispatcher-orchestrated phase still in
    // flight); workers shouldn't be writing in that state anyway,
    // so we refuse.
    let owner: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT owner_pod_name FROM execution_color WHERE color = $1",
    )
    .bind(color.to_string())
    .fetch_optional(&state.pool)
    .await
    .map_err(internal)?;
    let owner_pod = owner.and_then(|(p,)| p).ok_or((
        StatusCode::FORBIDDEN,
        "color has no owning pod yet; worker may not journal under it".into(),
    ))?;
    if owner_pod != claimed_pod {
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_pod = %claimed_pod,
            color = %color,
            owner_pod = %owner_pod,
            "broker rejected cross-color journal write"
        );
        return Err((
            StatusCode::FORBIDDEN,
            "color owned by a different worker pod".into(),
        ));
    }
    state
        .journal
        .record_event(&req.event, Some(claimed_pod))
        .await
        .map_err(internal)?;
    Ok(Json(JournalRecordResponse {}))
}

pub async fn journal_fetch(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalFetchRequest>,
) -> Resp<JournalFetchResponse> {
    scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, &req.color).await?;
    let color: weft_core::Color = req
        .color
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad color: {e}")))?;
    let events = state.journal.events_for_color(color).await.map_err(internal)?;
    Ok(Json(JournalFetchResponse { events }))
}

pub async fn journal_has_terminal(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalHasTerminalRequest>,
) -> Resp<JournalHasTerminalResponse> {
    scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, &req.color).await?;
    let color: weft_core::Color = req
        .color
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad color: {e}")))?;
    let terminal = state.journal.has_terminal_event(color).await.map_err(internal)?;
    Ok(Json(JournalHasTerminalResponse { terminal }))
}

// ---------- Tasks ----------

/// Should a held-event fire be FENCED (dropped) by the placement
/// generation? A fire is fenced iff the signal row exists AND the fire's
/// generation is strictly below the row's current one, meaning it came
/// from a pod that has since been drained (a scale-down move registered
/// the signal on a newer pod under a higher generation). A fire equal to
/// or above the current generation is the live holder's; a signal with no
/// row (`None`) is never fenced (no move could have happened, and the
/// downstream scope check handles a genuinely-missing signal). Pure so
/// the fence rule is layer-1 testable without a Postgres row.
fn fire_is_fenced(fire_gen: i64, current_gen: Option<i64>) -> bool {
    matches!(current_gen, Some(cur) if fire_gen < cur)
}

/// Fold a newly-resolved resource tenant into the task's anchor tenant,
/// enforcing that every named resource agrees. A task naming resources
/// in two different tenants (project in A, color in B) is ambiguous and
/// a sign of a confused or malicious caller; we refuse it loudly rather
/// than letting the last-resolved resource silently win. Pure so the
/// agreement rule is layer-1 testable without a Postgres lookup.
fn merge_anchor_tenant(
    anchor: &mut Option<String>,
    resource_tenant: String,
) -> Result<(), (StatusCode, String)> {
    match anchor {
        Some(prev) if *prev != resource_tenant => Err((
            StatusCode::CONFLICT,
            format!(
                "task names resources in two different tenants ('{prev}' and \
                 '{resource_tenant}'); refusing to guess which tenant owns the task"
            ),
        )),
        _ => {
            *anchor = Some(resource_tenant);
            Ok(())
        }
    }
}

pub async fn task_enqueue_dedup(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskEnqueueDedupRequest>,
) -> Resp<TaskEnqueueDedupResponse> {
    let kind = req.spec.kind.clone();
    let target = req.spec.target;

    // Per-role allow list of kinds. Anything else is a 403.
    match caller.role {
        Role::Worker => {
            // Workers enqueue control-plane work for the dispatcher
            // to handle: register a wake signal, provision infra,
            // and durable side-effect records (cost + log) that must
            // survive the worker pod dying.
            if ![
                TaskKind::RegisterSignal.as_str(),
                TaskKind::RecordCost.as_str(),
                TaskKind::RecordLog.as_str(),
            ]
            .contains(&kind.as_str())
            {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("worker may not enqueue task kind {kind}"),
                ));
            }
            // RecordCost payload validation at enqueue time, so a
            // malicious worker can't submit a bad row and die before the
            // dispatcher's executor would catch it:
            //   - amount_usd is null (a meter's honest unknown) or a
            //     finite non-negative number; never negative or NaN.
            //   - billed must be false: a worker-side record is a
            //     MEASUREMENT. Only the deployment's own billing path may
            //     mark a record billed, and it does not come through here.
            if kind == TaskKind::RecordCost.as_str() {
                match req.spec.payload.get("amount_usd") {
                    None | Some(serde_json::Value::Null) => {}
                    Some(v) => {
                        let amount = v.as_f64().ok_or((
                            StatusCode::BAD_REQUEST,
                            "record_cost amount_usd must be null or a number".to_string(),
                        ))?;
                        if !(amount.is_finite() && amount >= 0.0) {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                format!(
                                    "record_cost amount_usd must be a finite non-negative \
                                     number; got {amount}"
                                ),
                            ));
                        }
                    }
                }
                if req.spec.payload.get("billed").and_then(|v| v.as_bool()) != Some(false) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "record_cost from a worker must carry billed: false (a worker \
                         records measurements, never charges)"
                            .to_string(),
                    ));
                }
            }
        }
        Role::Listener => {
            // Listeners enqueue exactly one kind: a held-event fire.
            if kind != TaskKind::FireSignal.as_str() {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("listener may not enqueue task kind {kind}"),
                ));
            }
        }
        Role::InfraSupervisor => {
            return Err((
                StatusCode::FORBIDDEN,
                "infra-supervisor may not enqueue tasks (uses /supervisor/* endpoints)"
                    .into(),
            ));
        }
    }

    if target != TaskTarget::Dispatcher {
        return Err((
            StatusCode::FORBIDDEN,
            "tenant-pod-enqueued tasks must target dispatcher".into(),
        ));
    }
    // `target_pod_name` is meaningful only for cancel-style tasks
    // claimed by a specific worker pod. Tenant pods never enqueue
    // those (the dispatcher emits cancels server-side), so any
    // wire-set value is either confused or hostile. Refuse to
    // persist a value the caller has no legitimate use for.
    if req.spec.target_pod_name.is_some() {
        return Err((
            StatusCode::FORBIDDEN,
            "tenant pods may not set target_pod_name".into(),
        ));
    }

    // Resolve the task's authoritative tenant from the resource it
    // names, enforcing the caller's scope on every named resource. A
    // worker (tenant-scoped) may only act for its own tenant; a pooled
    // listener (control-plane, trusted) may fire held events for any
    // tenant. The `require_*` helpers each return the resource's true
    // tenant, so we never trust `req.spec.tenant_id` from the wire and a
    // control-plane caller (which has no single tenant) still yields a
    // correctly-tenanted task. When several resources are named they
    // MUST agree: `merge_anchor_tenant` rejects a task that names
    // resources in two different tenants (e.g. project P in tenant A and
    // color C in tenant B) rather than silently picking one, so the
    // stamped tenant is never ambiguous.
    let mut anchor_tenant: Option<String> = None;
    if let Some(project_id) = req.spec.project_id.as_deref() {
        let t =
            scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, project_id)
                .await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
    }
    if let Some(color) = req.spec.color.as_deref() {
        let t = scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, color).await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
    }
    if kind == TaskKind::FireSignal.as_str() {
        // Listener held-event fire: the signal token is the tenant
        // anchor. Pull it from the payload and resolve.
        let token = req
            .spec
            .payload
            .get("token")
            .and_then(|v| v.as_str())
            .ok_or((StatusCode::BAD_REQUEST, "fire_signal payload missing token".into()))?;
        let t = scope::require_signal_owned_by(&state.scope_cache, &state.pool, &caller, token).await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;

        // Placement-generation FENCE. A held-event fire carries the
        // generation the firing pod holds the signal under. During a
        // scale-down move the signal is briefly armed on two pods (the
        // new pod registered under gen+1 BEFORE the old pod is
        // unregistered); a self-firing kind (Timer/SSE) could fire on
        // both. The new pod's fire carries the current generation; the
        // stale old pod's carries a LOWER one. Drop the stale fire so the
        // event is delivered exactly once. A fire missing the field (or
        // for a signal with no row) is treated as current (gen 0), never
        // fenced, so non-move paths are unaffected.
        let fire_gen = req
            .spec
            .payload
            .get("placement_generation")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let current_gen: Option<(i64,)> =
            sqlx::query_as("SELECT placement_generation FROM signal WHERE token = $1")
                .bind(token)
                .fetch_optional(&state.pool)
                .await
                .map_err(|e| internal(anyhow::anyhow!("read placement_generation: {e}")))?;
        if fire_is_fenced(fire_gen, current_gen.map(|(g,)| g)) {
            tracing::info!(
                target: "weft_broker::handlers",
                %token,
                fire_gen,
                current_gen = ?current_gen.map(|(g,)| g),
                "fenced stale held-event fire (old pod fired during a scale-down move overlap)"
            );
            return Ok(Json(TaskEnqueueDedupResponse {
                id: None,
                inserted: false,
                fenced: true,
            }));
        }
    }

    // A task with no tenant-bearing resource can only come from a
    // tenant-scoped caller (its own tenant is the anchor). A
    // control-plane caller MUST name a resource so the tenant is
    // derivable; reject the ambiguous case rather than guess.
    let resolved_tenant = match anchor_tenant {
        Some(t) => t,
        None => match caller.scope.pinned_tenant() {
            Some(t) => t.to_string(),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "control-plane caller must name a project / color / signal so the task's \
                     tenant can be resolved".into(),
                ));
            }
        },
    };

    // `req.spec` IS a `NewTask` directly (the wire shape matches the
    // type). Stamp the RESOLVED tenant (the resource's true tenant),
    // never the wire value and never the caller identity.
    let mut new_task = req.spec;
    new_task.tenant_id = Some(resolved_tenant);
    let outcome = state.tasks.enqueue_dedup(new_task).await.map_err(internal)?;
    let (id, inserted) = match outcome {
        DedupOutcome::Inserted(id) => (id, true),
        DedupOutcome::AlreadyLive(id) => (id, false),
        // The local Postgres `enqueue_dedup` has no placement-generation
        // context and never fences; the only fence is the explicit
        // early-return above. Reaching here with Fenced is impossible.
        DedupOutcome::Fenced => unreachable!(
            "local enqueue_dedup cannot fence; the generation fence early-returns above"
        ),
    };
    Ok(Json(TaskEnqueueDedupResponse {
        id: Some(id),
        inserted,
        fenced: false,
    }))
}

pub async fn task_wait_terminal(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskWaitTerminalRequest>,
) -> Resp<TaskWaitTerminalResponse> {
    require_task_owned_by(&state, &caller, req.task_id).await?;
    let outcome = state
        .tasks
        .wait_for_terminal(
            req.task_id,
            Duration::from_millis(req.timeout_ms),
            Duration::from_millis(req.poll_interval_ms),
        )
        .await
        .map_err(internal)?;
    Ok(Json(TaskWaitTerminalResponse::from_outcome(outcome)))
}

pub async fn task_claim_one(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskClaimOneRequest>,
) -> Resp<TaskClaimOneResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_id)?;
    let filter = req.filter;
    if let ClaimFilter::Worker { project_id } = &filter {
        scope::require_project_owned_by(
            &state.scope_cache,
            &state.pool,
            &caller,
            project_id,
        )
        .await?;
    } else {
        return Err((
            StatusCode::FORBIDDEN,
            "workers may only use Worker claim filter".into(),
        ));
    }
    let task = state
        .tasks
        .claim_one(&req.pod_id, filter)
        .await
        .map_err(internal)?;
    // Latest-claim-wins color ownership is bound IN the claim's own
    // transaction by the `task_claim_binds_color_owner` DB trigger
    // (weft-task-store worker_pod migration): claiming a color-bearing
    // task atomically stamps execution_color.owner_pod_name to the
    // claimer. The broker does NOT stamp it here, so "claimed by pod X"
    // and "owned by pod X" can never disagree (a separate post-claim
    // UPDATE could be lost to a crash, leaving the claimer's journal
    // writes fenced). The journal_record owner check above reads what
    // the trigger wrote.
    Ok(Json(TaskClaimOneResponse { task }))
}

pub async fn task_heartbeat(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskHeartbeatRequest>,
) -> Resp<TaskHeartbeatResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_id)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    let renewed = state
        .tasks
        .heartbeat(req.task_id, &req.pod_id)
        .await
        .map_err(internal)?;
    Ok(Json(TaskHeartbeatResponse { renewed }))
}

pub async fn task_complete(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskCompleteRequest>,
) -> Resp<TaskCompleteResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_id)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    state
        .tasks
        .complete(req.task_id, &req.pod_id, req.result)
        .await
        .map_err(internal)?;
    Ok(Json(TaskCompleteResponse {}))
}

pub async fn task_fail(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskFailRequest>,
) -> Resp<TaskFailResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_id)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    state
        .tasks
        .fail(req.task_id, &req.pod_id, req.error)
        .await
        .map_err(internal)?;
    Ok(Json(TaskFailResponse {}))
}

// ---------- worker_pod ----------

pub async fn worker_pod_register_alive(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<WorkerPodRegisterAliveRequest>,
) -> Resp<WorkerPodRegisterAliveResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_name)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    state
        .worker_pods
        .register_alive(&req.pod_name, &req.project_id)
        .await
        .map_err(internal)?;
    Ok(Json(WorkerPodRegisterAliveResponse {}))
}

pub async fn worker_pod_heartbeat(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<WorkerPodHeartbeatRequest>,
) -> Resp<WorkerPodHeartbeatResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_name)?;
    require_worker_pod_owned_by(&state, &caller, &req.pod_name).await?;
    let renewed = state
        .worker_pods
        .heartbeat(&req.pod_name, req.mem_pressure)
        .await
        .map_err(internal)?;
    Ok(Json(WorkerPodHeartbeatResponse { renewed }))
}

pub async fn worker_pod_mark_done(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<WorkerPodMarkDoneRequest>,
) -> Resp<WorkerPodMarkDoneResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_name)?;
    require_worker_pod_owned_by(&state, &caller, &req.pod_name).await?;
    state
        .worker_pods
        .mark_done(&req.pod_name)
        .await
        .map_err(internal)?;
    Ok(Json(WorkerPodMarkDoneResponse {}))
}

pub async fn worker_pod_mark_done_if_idle(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<WorkerPodMarkDoneIfIdleRequest>,
) -> Resp<WorkerPodMarkDoneIfIdleResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_name)?;
    require_worker_pod_owned_by(&state, &caller, &req.pod_name).await?;
    // No project_id from the request: the guarded CAS reads the
    // pod's own project from its row, so a worker can't scope the
    // no-work check to a different project.
    let exited = state
        .worker_pods
        .mark_done_if_idle(&req.pod_name)
        .await
        .map_err(internal)?;
    Ok(Json(WorkerPodMarkDoneIfIdleResponse { exited }))
}

// ---------- Infra ----------

pub async fn infra_endpoint_url(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<InfraEndpointUrlRequest>,
) -> Resp<InfraEndpointUrlResponse> {
    // Workers and listeners both need infra endpoint URLs.
    // - Workers: at fire-time, hit the infra pod's `/action` etc.
    // - Listeners: at signal registration time, subscribe to SSE
    //   served from infra pods (e.g. WhatsApp messages).
    if !matches!(caller.role, Role::Worker | Role::Listener) {
        return Err((StatusCode::FORBIDDEN, "worker or listener only".into()));
    }
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let endpoint_url = state
        .infra
        .endpoint_url(&req.project_id, &req.node_id, &req.endpoint_name)
        .await
        .map_err(internal)?;
    Ok(Json(InfraEndpointUrlResponse { endpoint_url }))
}

/// Worker fetches a project definition at execution claim time,
/// keyed by `(project_id, definition_hash)`. The hash makes the
/// lookup content-addressed: callers always get back the EXACT
/// shape they asked for, regardless of what the project row's
/// current `running_definition_hash` says. That's load-bearing for
/// resumes after re-register: a suspended execution must resume on
/// the shape it was started on, even if the user has edited and
/// re-registered the project in the meantime.
///
/// The history table `project_definition` (append-only, keyed on
/// `(project_id, definition_hash)`) is written by the dispatcher's
/// register handler. A missing row means no register has ever
/// happened under this hash for this project; that's a 404 (the
/// caller's expected_hash is genuinely invalid, not just stale).
pub async fn project_fetch_definition(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ProjectFetchDefinitionRequest>,
) -> Resp<ProjectFetchDefinitionResponse> {
    if !matches!(caller.role, Role::Worker) {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let project_uuid: uuid::Uuid = req
        .project_id
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad project_id: {e}")))?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT project_json FROM project_definition \
         WHERE project_id = $1 AND definition_hash = $2",
    )
    .bind(project_uuid)
    .bind(&req.expected_hash)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let Some((project_json,)) = row else {
        return Err((
            StatusCode::NOT_FOUND,
            format!(
                "no project definition for project_id={} hash={}",
                req.project_id, req.expected_hash
            ),
        ));
    };
    Ok(Json(ProjectFetchDefinitionResponse {
        project_json,
        definition_hash: req.expected_hash,
    }))
}

// ---------- Provider access ----------

/// Worker opens access to a provider on the deployment's configured key. The
/// caller's execution scope anchors the tenant; the deployment's
/// `CredentialSource` decides whether THIS node may use its key and answers
/// with the credential to authenticate with, plus (when it relays) where
/// calls on it go.
///
/// The node declares how long its provider work may take
/// (`expected_duration_secs`) and the runtime does not second-guess it: a
/// legitimately long action (a multi-day agent, a slow batch job) says so and
/// gets a window that long. There is no ceiling here.
pub async fn open_provider_access(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ProviderAccessRequest>,
) -> Resp<ProviderAccessResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    // The provider name arrives on the wire from the worker. It is the string
    // the deployment's key is derived from (`<NAME>_API_KEY`), so it is held to
    // the same charset it is validated against where it is DECLARED
    // (`weft_core::node::is_valid_provider_name`). Without this, the derivation
    // would run on an arbitrary caller-supplied string.
    if !weft_core::node::is_valid_provider_name(&req.provider) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "provider name '{}' is invalid: use only lowercase letters, digits, and '_'",
                req.provider
            ),
        ));
    }
    let tenant =
        scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, &req.color)
            .await?;
    let key_req = crate::credential::KeyRequest {
        tenant,
        color: req.color.clone(),
        project_id: req.project_id,
        node_id: req.node_id,
        frames: req.frames,
        node_type: req.node_type,
        provider: req.provider.clone(),
        pod_name: caller.pod_name.clone(),
        window: std::time::Duration::from_secs(req.expected_duration_secs),
    };
    match state.credentials.resolve(&state.pool, &key_req).await.map_err(internal)? {
        crate::credential::KeyResolution::Access { credential, relay_url } => {
            Ok(Json(ProviderAccessResponse { credential, relay_url }))
        }
        crate::credential::KeyResolution::NotConfigured => Err((
            StatusCode::PRECONDITION_FAILED,
            format!(
                "this deployment has no key configured for '{}'; set your own key on the \
                 node's key input",
                req.provider
            ),
        )),
        crate::credential::KeyResolution::Denied { reason } => {
            Err((StatusCode::FORBIDDEN, reason))
        }
    }
}

/// Runtime gives a deployment-granted access back (the node that opened it
/// finished): a source that hands out time-bounded credentials retires this
/// one now, rather than leaving it usable to its window (the crash
/// backstop).
///
/// The color scope check keeps a worker from retiring another tenant's
/// accesses; the source's close is scoped to the caller's tenant, so a
/// credential it does not own is simply not found (and closing is
/// idempotent).
pub async fn close_provider_access(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ProviderAccessCloseRequest>,
) -> Resp<ProviderAccessCloseResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    let tenant =
        scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, &req.color)
            .await?;
    state
        .credentials
        .close(&state.pool, &req.credential, &tenant)
        .await
        .map_err(internal)?;
    Ok(Json(ProviderAccessCloseResponse {}))
}

// ---------- Supervisor surface ----------

pub async fn supervisor_sync_ownership(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorSyncOwnershipRequest>,
) -> Resp<SupervisorSyncOwnershipResponse> {
    require_supervisor(&caller)?;
    // The pooled supervisor's ownership tick, done atomically so two
    // supervisors never end up owning one project. Steps in one
    // transaction:
    //   0. Record this pod's reported memory pressure on its registry row
    //      (the dispatcher's placement + scale-down read it).
    //   1. Renew this pod's existing leases (it is alive and working).
    //   2. Claim a BATCH of MORE projects, but only while the pod is
    //      below the shared memory saturation threshold (a saturated pod
    //      keeps what it owns and takes on no more; the dispatcher then
    //      spawns another supervisor). Claiming is memory-gated, not
    //      count-gated, so load is the SAME metric as the listener.
    //      `FOR UPDATE SKIP LOCKED` + `ON CONFLICT` make concurrent
    //      supervisors partition the free projects without double-claiming.
    //   3. Return the full owned set (the work loops act only on these).
    // A project is eligible only if it has a namespace (only namespaced/
    // paid-tier projects have infra). All time comes from the DB clock
    // (`EXTRACT(EPOCH FROM NOW())`), never the app clock, so a skewed
    // dispatcher/broker host can't mis-judge lease expiry.
    let lease_secs = weft_broker_client::lifecycle_command::INFRA_OWNER_LEASE_SECS;
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|e| internal(anyhow::anyhow!("begin sync_ownership tx: {e}")))?;

    // 0. Record reported memory pressure and read back whether the
    //    dispatcher has marked this pod draining (scaled down). Best-
    //    effort: a fresh pod's row may not exist yet if the dispatcher
    //    spawn hasn't committed; the UPDATE then returns no row and the
    //    next tick records it. A draining pod must renew + claim nothing
    //    (its leases were released for re-adoption; re-grabbing them would
    //    defeat consolidation).
    let draining: bool = sqlx::query_scalar(
        "UPDATE supervisor_pod SET mem_pressure = $1 WHERE pod_name = $2 RETURNING draining",
    )
    .bind(req.mem_pressure)
    .bind(&req.pod_name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("record supervisor mem_pressure: {e}")))?
    .unwrap_or(false);

    // 1. Renew owned leases, UNLESS draining (then we want the leases to
    //    lapse / stay released so survivors adopt them; the drain already
    //    deleted them, this guards against a renew racing that delete).
    if !draining {
        sqlx::query(
            "UPDATE infra_owner \
             SET leased_until_unix = EXTRACT(EPOCH FROM NOW())::BIGINT + $1 \
             WHERE supervisor_pod = $2",
        )
        .bind(lease_secs)
        .bind(&req.pod_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(anyhow::anyhow!("renew infra_owner leases: {e}")))?;
    }

    // 2. Claim a batch, but only while under the memory saturation
    //    threshold AND not draining. At/above saturation, or while
    //    draining, claim nothing (a saturated pod keeps what it owns; a
    //    draining pod is being removed and must take on nothing).
    let saturated = weft_platform_traits::is_saturated(
        req.mem_pressure,
        weft_platform_traits::SATURATION_MEM_FRACTION,
    );
    let headroom = if saturated || draining {
        0
    } else {
        weft_broker_client::lifecycle_command::SUPERVISOR_CLAIM_BATCH
    };
    if headroom > 0 {
        // Atomic claim via a CTE: `free` selects projects with no live
        // owner and LOCKS them `FOR UPDATE OF p SKIP LOCKED`, so a
        // sibling supervisor's concurrent claim takes a DISJOINT set
        // (never the same row). The INSERT then takes the EXCLUSIVE
        // `infra_owner` lease for each. ON CONFLICT covers a stale
        // (expired-lease) row still physically present: we overwrite it
        // ONLY if its lease is actually expired, so a live owner is
        // never stolen. Rows we lock are guaranteed free at insert time
        // because the lock is held to the end of the tx.
        sqlx::query(
            "WITH free AS ( \
                 SELECT p.id::TEXT AS project_id, p.project_namespace, p.tenant_id \
                 FROM project p \
                 WHERE p.project_namespace <> '' \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM infra_owner io \
                       WHERE io.project_id = p.id::TEXT \
                         AND io.leased_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT \
                   ) \
                 ORDER BY p.id \
                 LIMIT $2 \
                 FOR UPDATE OF p SKIP LOCKED \
             ) \
             INSERT INTO infra_owner \
                 (project_id, supervisor_pod, namespace, tenant_id, leased_until_unix) \
             SELECT project_id, $1, project_namespace, tenant_id, \
                    EXTRACT(EPOCH FROM NOW())::BIGINT + $3 \
             FROM free \
             ON CONFLICT (project_id) DO UPDATE \
               SET supervisor_pod = EXCLUDED.supervisor_pod, \
                   namespace = EXCLUDED.namespace, \
                   tenant_id = EXCLUDED.tenant_id, \
                   leased_until_unix = EXCLUDED.leased_until_unix \
               WHERE infra_owner.leased_until_unix < EXTRACT(EPOCH FROM NOW())::BIGINT",
        )
        .bind(&req.pod_name)
        .bind(headroom)
        .bind(lease_secs)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(anyhow::anyhow!("claim infra_owner rows: {e}")))?;
    }

    // 3. Return the full owned set (joined to current project state).
    let projects = owned_projects_in(&mut *tx, &req.pod_name).await?;
    tx.commit()
        .await
        .map_err(|e| internal(anyhow::anyhow!("commit sync_ownership tx: {e}")))?;
    Ok(Json(SupervisorSyncOwnershipResponse { owned: projects }))
}

/// Pure read: the projects a supervisor pod currently owns, joined to
/// live project state. No claim, no renew (ownership breadth changes
/// only via `sync_ownership`). Used by the work loops + per-command
/// namespace lookups.
pub async fn supervisor_owned_projects(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorOwnedProjectsRequest>,
) -> Resp<SupervisorOwnedProjectsResponse> {
    require_supervisor(&caller)?;
    let owned = owned_projects_in(&state.pool, &req.pod_name).await?;
    Ok(Json(SupervisorOwnedProjectsResponse { owned }))
}

/// Decode the owned `SupervisorProject` set for a pod against any
/// executor (a pool ref or a transaction). The single source of the
/// owned-set query + row decode, shared by `sync_ownership` (inside its
/// tx) and `owned_projects` (against the pool).
async fn owned_projects_in<'e, E>(
    executor: E,
    pod_name: &str,
) -> Result<Vec<SupervisorProject>, (StatusCode, String)>
where
    E: sqlx::PgExecutor<'e>,
{
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT p.id::TEXT AS project_id, p.tenant_id, p.project_namespace, p.status, \
                p.deactivated_by_health \
         FROM infra_owner io \
         JOIN project p ON p.id::TEXT = io.project_id \
         WHERE io.supervisor_pod = $1 AND p.project_namespace <> ''",
    )
    .bind(pod_name)
    .fetch_all(executor)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let mut projects = Vec::with_capacity(rows.len());
    for r in rows {
        let project_id: String = r
            .try_get("project_id")
            .map_err(|e| internal(anyhow::anyhow!("decode project_id: {e}")))?;
        let tenant_id: String = r
            .try_get("tenant_id")
            .map_err(|e| internal(anyhow::anyhow!("decode tenant_id: {e}")))?;
        let project_namespace: String = r
            .try_get("project_namespace")
            .map_err(|e| internal(anyhow::anyhow!("decode project_namespace: {e}")))?;
        let status_str: String = r
            .try_get("status")
            .map_err(|e| internal(anyhow::anyhow!("decode status: {e}")))?;
        let status = weft_broker_client::protocol::ProjectStatus::parse(&status_str)
            .ok_or_else(|| {
                internal(anyhow::anyhow!(
                    "project.status='{status_str}' is not a known ProjectStatus"
                ))
            })?;
        let deactivated_by_health: bool = r
            .try_get("deactivated_by_health")
            .map_err(|e| internal(anyhow::anyhow!("decode deactivated_by_health: {e}")))?;
        projects.push(SupervisorProject {
            project_id,
            tenant_id,
            project_namespace,
            status,
            deactivated_by_health,
        });
    }
    Ok(projects)
}

pub async fn supervisor_infra_nodes(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorInfraNodesRequest>,
) -> Resp<SupervisorInfraNodesResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT node_id, instance_id, status, applied_spec_hash, \
                endpoints_json, preserve_pvcs_json, units_json \
         FROM infra_node WHERE project_id = $1",
    )
    .bind(&req.project_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // Decode every column; row-decode errors mean schema drift and
    // must surface as 500. Empty-string node_ids slipping through
    // would silently corrupt the supervisor's view of the world.
    let mut nodes = Vec::with_capacity(rows.len());
    for r in rows {
        let node_id: String = r
            .try_get("node_id")
            .map_err(|e| internal(anyhow::anyhow!("decode node_id: {e}")))?;
        let instance_id: String = r
            .try_get("instance_id")
            .map_err(|e| internal(anyhow::anyhow!("decode instance_id: {e}")))?;
        let status_str: String = r
            .try_get("status")
            .map_err(|e| internal(anyhow::anyhow!("decode status: {e}")))?;
        let status = weft_broker_client::protocol::InfraNodeStatus::parse(&status_str)
            .ok_or_else(|| {
                internal(anyhow::anyhow!(
                    "infra_node.status='{status_str}' is not a known InfraNodeStatus"
                ))
            })?;
        let applied_spec_hash: Option<String> = r
            .try_get::<Option<String>, _>("applied_spec_hash")
            .map_err(|e| internal(anyhow::anyhow!("decode applied_spec_hash: {e}")))?;
        let endpoints_json: serde_json::Value = r
            .try_get("endpoints_json")
            .map_err(|e| internal(anyhow::anyhow!("decode endpoints_json: {e}")))?;
        let endpoints: std::collections::BTreeMap<String, String> = serde_json::from_value(
            endpoints_json,
        )
        .map_err(|e| {
            internal(anyhow::anyhow!(
                "infra_node.endpoints_json for node='{node_id}' is not a string-to-string map: {e}"
            ))
        })?;
        let preserve_pvcs_json: serde_json::Value = r
            .try_get("preserve_pvcs_json")
            .map_err(|e| internal(anyhow::anyhow!("decode preserve_pvcs_json: {e}")))?;
        let preserve_pvcs: Vec<String> = serde_json::from_value(preserve_pvcs_json).map_err(|e| {
            internal(anyhow::anyhow!(
                "infra_node.preserve_pvcs_json for node='{node_id}' is not Vec<String>: {e}"
            ))
        })?;
        let units_json: serde_json::Value = r
            .try_get("units_json")
            .map_err(|e| internal(anyhow::anyhow!("decode units_json: {e}")))?;
        let units: std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime> =
            serde_json::from_value(units_json).map_err(|e| {
                internal(anyhow::anyhow!(
                    "infra_node.units_json for node='{node_id}' is not a unit map: {e}"
                ))
            })?;
        nodes.push(SupervisorInfraNode {
            node_id,
            instance_id,
            status,
            applied_spec_hash,
            endpoints,
            preserve_pvcs,
            units,
        });
    }
    Ok(Json(SupervisorInfraNodesResponse { nodes }))
}

pub async fn supervisor_health_protocols(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorHealthProtocolsRequest>,
) -> Resp<SupervisorHealthProtocolsResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let id = req
        .project_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad project_id".to_string()))?;
    use sqlx::Row;
    let row = sqlx::query("SELECT health_protocols_json FROM project WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let protocols = match row {
        None => None,
        Some(r) => r
            .try_get::<Option<serde_json::Value>, _>("health_protocols_json")
            .map_err(|e| internal(anyhow::anyhow!("decode health_protocols_json: {e}")))?,
    };
    Ok(Json(SupervisorHealthProtocolsResponse { protocols }))
}

pub async fn supervisor_claim_command(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorClaimCommandRequest>,
) -> Resp<SupervisorClaimCommandResponse> {
    require_supervisor(&caller)?;
    // A pooled supervisor claims a lifecycle command ONLY for a project
    // whose infra it currently OWNS (the `infra_owner` exclusive lease).
    // Ownership is the supervisor's ONE single-actor authority: it is
    // exclusive (one pod per project), continuously renewed on each
    // ownership tick, and the owner's work loop is sequential, so two
    // supervisors can never run kubectl for one project. There is NO
    // per-command claim lease here (it would be redundant with exclusive
    // ownership, and its fixed expiry would wrongly let a sibling re-run
    // a long command mid-flight). The command simply remains uncompleted
    // until its owner finishes it; if ownership moves mid-command, every
    // write from the old owner is rejected (see `owns_project_predicate`
    // on the set_* / complete handlers) and the new owner re-runs it.
    //
    // Verb filter: the supervisor claims only the verbs it owns.
    // Dispatcher verbs (`deactivate`, `reactivate`) are claimed by
    // dispatcher pods directly (via their own `claimed_by_pod` lease);
    // the supervisor MUST skip them.
    //
    // No row UPDATE here: claiming is a pure SELECT of the oldest
    // uncompleted owned command. No row lock is needed: the owning pod
    // runs a sequential work loop (one command at a time), and ownership
    // is exclusive, so no two claim queries ever target this project's
    // commands concurrently. Re-running an already-claimed-but-unfinished
    // command after a crash is correct and idempotent (declarative
    // kubectl), so there is nothing to serialize against.
    let sql = format!(
        "SELECT c.id, c.project_id, c.node_id, c.verb, c.running_policy, c.spec_json, c.force, \
                c.drain_timeout_secs \
         FROM infra_lifecycle_command c \
         WHERE c.verb IN ('apply', 'stop', 'terminate') \
           AND c.completed_at_unix IS NULL \
           AND {owns} \
         ORDER BY c.id ASC \
         LIMIT 1",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$1", "c.project_id"),
    );
    let row = sqlx::query(&sql)
        .bind(&req.claimer_pod)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let command = match row {
        None => None,
        Some(r) => Some(decode_claimed_command(&r).map_err(internal)?),
    };
    Ok(Json(SupervisorClaimCommandResponse { command }))
}

/// Decode one `infra_lifecycle_command` row into the typed
/// `SupervisorCommandRow` wire shape. EVERY column read propagates
/// errors; unknown enum values (verb / running_policy) become 500s
/// rather than silent fallbacks. Shared by every handler that hands
/// claimed rows to the supervisor.
fn decode_claimed_command(
    r: &sqlx::postgres::PgRow,
) -> anyhow::Result<SupervisorCommandRow> {
    use sqlx::Row;
    let id: i64 = r.try_get("id")?;
    let project_id: String = r.try_get("project_id")?;
    let node_id: Option<String> = r.try_get::<Option<String>, _>("node_id")?;
    let verb_str: String = r.try_get("verb")?;
    let verb = weft_broker_client::protocol::InfraLifecycleVerb::parse(&verb_str)
        .ok_or_else(|| anyhow::anyhow!("infra_lifecycle_command.id={id}: unknown verb '{verb_str}'"))?;
    // Nullable: dispatcher verbs (deactivate / reactivate) carry
    // policy inside spec_json; Apply ignores it. Stop / Terminate
    // populate it.
    let running_policy_str: Option<String> = r.try_get("running_policy")?;
    let running_policy = match running_policy_str.as_deref() {
        None => None,
        Some(s) => Some(
            weft_broker_client::protocol::RunningPolicy::parse(s).ok_or_else(|| {
                anyhow::anyhow!(
                    "infra_lifecycle_command.id={id}: unknown running_policy '{s}'"
                )
            })?,
        ),
    };
    let spec_json: Option<serde_json::Value> =
        r.try_get::<Option<serde_json::Value>, _>("spec_json")?;
    let force: bool = r.try_get("force")?;
    let drain_timeout_secs: i64 = r.try_get("drain_timeout_secs")?;
    Ok(SupervisorCommandRow {
        id,
        project_id,
        node_id,
        verb,
        running_policy,
        spec_json,
        force,
        drain_timeout_secs: drain_timeout_secs.max(0) as u64,
    })
}

pub async fn supervisor_event_record(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorEventRecordRequest>,
) -> Resp<SupervisorEventRecordResponse> {
    require_supervisor(&caller)?;
    // The project's tenant (returned by the ownership check) is the
    // event's tenant. A pooled supervisor is control-plane and has no
    // tenant of its own, so the row's tenant always comes from the
    // resource, never the caller identity.
    let project_tenant =
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
            .await?;
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_event \
         (tenant_id, project_id, node_id, kind, payload, at_unix) \
         VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(&project_tenant)
    .bind(&req.project_id)
    .bind(req.node_id.as_deref())
    .bind(req.kind.as_str())
    .bind(&req.payload)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // Wake the dispatcher's bridge. Same shape as lifecycle_cmd:
    // best-effort, dropped NOTIFY is caught by the bridge's safety
    // poll.
    if let Err(e) = sqlx::query("SELECT pg_notify('weft_infra_event', $1)")
        .bind(row.0.to_string())
        .execute(&state.pool)
        .await
    {
        tracing::warn!(
            target: "weft_broker::handlers",
            error = %e,
            event_id = row.0,
            "pg_notify(weft_infra_event) failed; bridge safety poll will catch it"
        );
    }
    Ok(Json(SupervisorEventRecordResponse { id: row.0 }))
}

/// SQL fragment computing the node-level rollup status from a units
/// Roll a units jsonb expression up to one node status (worst-of-units
/// by `InfraNodeStatus::rollup_rank`). Must match
/// `InfraNodeStatus::rollup_rank` in the protocol crate. Empty map ->
/// 'stopped' (the rank-0 default). `units_expr` is the SQL expression
/// holding the units map: this is parameterized (not hardcoded to the
/// `units_json` column) because in a single UPDATE every SET RHS is
/// evaluated against the OLD row, so rolling up the bare column would
/// use the PRE-update unit statuses. We must roll up the SAME new-units
/// expression we're writing.
fn rollup_sql(units_expr: &str) -> String {
    format!(
        "(SELECT COALESCE( \
            (SELECT v->>'status' FROM jsonb_each({units_expr}) AS e(k, v) \
             ORDER BY CASE v->>'status' \
               WHEN 'terminating' THEN 7 WHEN 'stopping' THEN 6 \
               WHEN 'provisioning' THEN 5 WHEN 'failed' THEN 4 \
               WHEN 'flaky' THEN 3 WHEN 'running' THEN 2 \
               WHEN 'stopped' THEN 1 ELSE 0 END DESC \
             LIMIT 1), \
            'stopped'))"
    )
}

pub async fn supervisor_set_status(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorSetStatusRequest>,
) -> Resp<SupervisorSetStatusResponse> {
    require_supervisor(&caller)?;
    require_node_id(&req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    // Per-unit (`unit = Some`): set that unit's status inside
    // `units_json`, then recompute the node-level `status` as the
    // rollup over the UPDATED units. Node-wide (`unit = None`): set
    // every unit's status AND the node status to the same value (a
    // lifecycle-driven uniform transition like Stopping/Terminating).
    // Both run in ONE UPDATE so the per-unit write and the rollup are
    // atomic, and so the lease/fence WHERE clause guards them together.
    // CRITICAL: the rollup must read the NEW units expression, not the
    // `units_json` column. In one UPDATE, Postgres evaluates every SET
    // RHS against the pre-update row, so `status = rollup_sql("units_json")`
    // would roll up the OLD statuses (leaving e.g. `stopping` after the
    // unit already went `stopped`).
    let set_clause = if req.unit.is_some() {
        let new_units = "jsonb_set(units_json, ARRAY[$1], \
            COALESCE(units_json->$1, '{}'::jsonb) || jsonb_build_object('status', $2::text))";
        format!(
            "units_json = {new_units}, status = {rollup}, failure_stage = $3, failure_message = $4",
            rollup = rollup_sql(new_units),
        )
    } else {
        // No unit: rewrite every unit's status to $2, then the rollup
        // collapses to $2 too (all units equal).
        let new_units = "(SELECT COALESCE(jsonb_object_agg(k, v || jsonb_build_object('status', $2::text)), '{}'::jsonb) \
            FROM jsonb_each(units_json) AS e(k, v))";
        format!(
            "units_json = {new_units}, status = {rollup}, failure_stage = $3, failure_message = $4",
            rollup = rollup_sql(new_units),
        )
    };
    // The lease-ownership check lives inside the UPDATE's WHERE
    // clause so the check and the write are evaluated atomically
    // by Postgres on the same row snapshot. No TOCTOU window.
    // `$1` is the unit name (or a placeholder, unused when unit=None,
    // but the jsonb_set path needs it bound regardless).
    let unit_key = req.unit.clone().unwrap_or_default();
    let res = if let Some(cid) = req.command_id {
        // Lifecycle-driven write: require we still OWN the project
        // (the supervisor's single-actor authority) and that the
        // command still applies (targets this row, not yet completed).
        // The pod identity is the supervisor's claim id (`req.pod_name`
        // = WEFT_POD_NAME, what keys `infra_owner`), NOT the auth token's
        // Pod name (which carries a ReplicaSet suffix and would never
        // match the lease). The instant ownership moves to another pod,
        // this write is rejected and the command flows to the new owner.
        sqlx::query(&format!(
            "UPDATE infra_node SET {set_clause} \
             WHERE project_id = $5 AND node_id = $6 AND EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command \
               WHERE id = $7 \
                 AND project_id = $5 \
                 AND (node_id = $6 OR node_id IS NULL) \
                 AND completed_at_unix IS NULL \
             ) AND {owns}",
            owns = weft_broker_client::lifecycle_command::owns_project_predicate("$8", "$5"),
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(&req.project_id)
        .bind(&req.node_id)
        .bind(cid)
        .bind(&req.pod_name)
        .execute(&state.pool)
        .await
    } else {
        // Autonomous health write: tenant scope already enforced
        // above; no command to anchor to. Fence it against an
        // in-flight user infra action: if any uncompleted
        // infra_lifecycle_command targets this project/node, the
        // lifecycle handler owns the status and the health reconcile
        // must NOT write (it would clobber an apply/stop/terminate
        // mid-flight). The supervisor's tick-level gate already skips
        // these, but this closes the window between the dispatcher
        // accepting the action and the command row existing: the
        // EXISTS is evaluated atomically with the write, so a command
        // that appeared after the supervisor's gate-read still blocks
        // here. rows_affected=0 -> the supervisor logs + skips.
        sqlx::query(&format!(
            "UPDATE infra_node SET {set_clause} \
             WHERE project_id = $5 AND node_id = $6 AND NOT EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command \
               WHERE project_id = $5 \
                 AND (node_id = $6 OR node_id IS NULL) \
                 AND completed_at_unix IS NULL \
             )"
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(&req.project_id)
        .bind(&req.node_id)
        .execute(&state.pool)
        .await
    }
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // rows_affected = 0 means one of:
    //   - the infra_node row was removed (concurrent remove_node);
    //   - (command branch) the lifecycle command was completed, named a
    //     (project, node) that doesn't match, OR this pod no longer owns
    //     the project (ownership moved to another supervisor mid-command);
    //   - (autonomous branch) a user infra action's command is in
    //     flight for this node, so the health reconcile must stand
    //     down (the fence blocked it).
    // All are "this caller's view is stale or it must not write right
    // now"; surface as 410 so the supervisor logs + skips (and, for a
    // lost-ownership command, leaves it uncompleted for the new owner).
    if res.rows_affected() == 0 {
        return Err((
            StatusCode::GONE,
            format!(
                "set_status raced: infra_node row gone, command completed, \
                 or project ownership moved (project={}, node={}, cmd={:?})",
                req.project_id, req.node_id, req.command_id
            ),
        ));
    }
    Ok(Json(SupervisorSetStatusResponse {}))
}

/// Atomic post-apply write of every infra_node field the supervisor
/// produced: status=Running, instance_id, applied_spec_hash,
/// endpoints, namespace. UPSERT keyed on (project_id, node_id).
/// Write the `infra_node` row for an apply command, gated on the
/// caller still owning the command's claim. Shared by
/// `set_applied` (Running + hash/endpoints) and
/// `set_provisioning` (Provisioning + NULLs); the ONLY difference
/// is the four "applied-state" column values, passed in as
/// `ApplyRowState`. The TOCTOU-critical parts (the ownership
/// SELECT predicate and the raced-410 mapping) live here once so
/// they can't diverge between the two writes.
///
/// The INSERT pulls its values FROM the caller's still-uncompleted
/// apply command AND requires the caller to still OWN the project
/// (`owns_project_predicate`), so the existence check and the write
/// share one row snapshot (no window between "I checked" and "I
/// wrote"). Zero rows affected → ownership moved to another supervisor
/// (drain / lease takeover) or the command completed/cancelled
/// (remove_node cascade) → 410, and the command (if still uncompleted)
/// flows to the new owner.
struct ApplyRowState {
    status: &'static str,
    /// `Some` for set_applied; `None` for set_provisioning (the
    /// row hasn't successfully applied yet).
    applied_spec_hash: Option<String>,
    /// True for set_applied (stamps `applied_at_unix = NOW()`),
    /// false for provisioning (leaves it NULL).
    stamp_applied_at: bool,
    endpoints_json: serde_json::Value,
}

async fn write_apply_row(
    state: &BrokerState,
    op: &str,
    project_id: &str,
    node_id: &str,
    instance_id: &str,
    namespace: &str,
    preserve_pvcs: &[String],
    units_json: serde_json::Value,
    command_id: i64,
    owner_pod: &str,
    row: ApplyRowState,
) -> Result<(), (StatusCode, String)> {
    let preserve_pvcs_json = serde_json::to_value(preserve_pvcs)
        .map_err(|e| internal(anyhow::anyhow!("preserve_pvcs serialize: {e}")))?;
    // The INSERT pulls its values FROM the caller's still-claimed
    // apply command so the ownership check and the write share one
    // row snapshot. Every variable is a bind ($1..$13): no SQL
    // built by string interpolation. `applied_at_unix` uses the DB
    // clock (consistent with every other timestamp write in this
    // file), gated on the bound `$7` flag via CASE.
    let res = sqlx::query(
        &format!("INSERT INTO infra_node \
         (project_id, node_id, instance_id, namespace, status, \
          failure_stage, failure_message, applied_spec_hash, \
          applied_at_unix, endpoints_json, preserve_pvcs_json, units_json) \
         SELECT $1, $2, $3, $4, $5, NULL, NULL, $6, \
                CASE WHEN $7 THEN EXTRACT(EPOCH FROM NOW())::BIGINT ELSE NULL END, \
                $8, $9, $10 \
         FROM infra_lifecycle_command \
         WHERE id = $11 \
           AND project_id = $1 \
           AND node_id = $2 \
           AND verb = 'apply' \
           AND completed_at_unix IS NULL \
           AND {owns} \
         ON CONFLICT (project_id, node_id) DO UPDATE SET \
            instance_id        = EXCLUDED.instance_id, \
            namespace          = EXCLUDED.namespace, \
            status             = EXCLUDED.status, \
            failure_stage      = NULL, \
            failure_message    = NULL, \
            applied_spec_hash  = EXCLUDED.applied_spec_hash, \
            applied_at_unix    = EXCLUDED.applied_at_unix, \
            endpoints_json     = EXCLUDED.endpoints_json, \
            preserve_pvcs_json = EXCLUDED.preserve_pvcs_json, \
            units_json         = EXCLUDED.units_json",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$12", "$1"),
    ),
    )
    .bind(project_id)
    .bind(node_id)
    .bind(instance_id)
    .bind(namespace)
    .bind(row.status)
    .bind(&row.applied_spec_hash)
    .bind(row.stamp_applied_at)
    .bind(row.endpoints_json)
    .bind(preserve_pvcs_json)
    .bind(units_json)
    .bind(command_id)
    .bind(owner_pod)
    .execute(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    if res.rows_affected() == 0 {
        return Err((
            StatusCode::GONE,
            format!(
                "{op} raced: command id={command_id} already completed, or project \
                 ownership moved away from {owner_pod}"
            ),
        ));
    }
    Ok(())
}

pub async fn supervisor_set_applied(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorSetAppliedRequest>,
) -> Resp<SupervisorSetAppliedResponse> {
    require_supervisor(&caller)?;
    require_node_id(&req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let endpoints_json = serde_json::to_value(&req.endpoints)
        .map_err(|e| internal(anyhow::anyhow!("endpoints serialize: {e}")))?;
    let units_json = serde_json::to_value(&req.units)
        .map_err(|e| internal(anyhow::anyhow!("units serialize: {e}")))?;
    write_apply_row(
        &state,
        "set_applied",
        &req.project_id,
        &req.node_id,
        &req.instance_id,
        &req.namespace,
        &req.preserve_pvcs,
        units_json,
        req.command_id,
        &req.pod_name,
        ApplyRowState {
            status: weft_broker_client::protocol::InfraNodeStatus::Running.as_str(),
            applied_spec_hash: Some(req.applied_spec_hash.clone()),
            stamp_applied_at: true,
            endpoints_json,
        },
    )
    .await?;
    Ok(Json(SupervisorSetAppliedResponse {}))
}

/// Supervisor-callable: write the `infra_node` row at `Provisioning`
/// before kubectl apply begins. Locks in the (instance_id, namespace,
/// preserve_pvcs) tuple so that a partial-apply leaves a visible row
/// the user can Terminate. On apply success, `set_applied` flips to
/// `Running` and fills endpoints + applied_spec_hash. Same ownership
/// guard as `set_applied`: the caller must still own the command's
/// claim.
pub async fn supervisor_set_provisioning(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorSetProvisioningRequest>,
) -> Resp<SupervisorSetProvisioningResponse> {
    require_supervisor(&caller)?;
    require_node_id(&req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let units_json = serde_json::to_value(&req.units)
        .map_err(|e| internal(anyhow::anyhow!("units serialize: {e}")))?;
    write_apply_row(
        &state,
        "set_provisioning",
        &req.project_id,
        &req.node_id,
        &req.instance_id,
        &req.namespace,
        &req.preserve_pvcs,
        units_json,
        req.command_id,
        &req.pod_name,
        ApplyRowState {
            // Not-yet-applied: NULL hash + applied_at, empty
            // endpoints. set_applied flips these on success.
            status: weft_broker_client::protocol::InfraNodeStatus::Provisioning.as_str(),
            applied_spec_hash: None,
            stamp_applied_at: false,
            endpoints_json: serde_json::json!({}),
        },
    )
    .await?;
    Ok(Json(SupervisorSetProvisioningResponse {}))
}

/// Supervisor-callable: enqueue a dispatcher-targeted lifecycle
/// command (`deactivate` | `reactivate`). Used by HealthProtocol
/// actions when the supervisor decides the project should be
/// parked / hibernated / wiped or re-activated. Side-channel
/// `event_record(notify, payload.action=...)` is GONE; lifecycle
/// commands are the single channel, with retries via the claim
/// loop.
pub async fn supervisor_enqueue_lifecycle(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorEnqueueLifecycleRequest>,
) -> Resp<SupervisorEnqueueLifecycleResponse> {
    require_supervisor(&caller)?;
    // The command's tenant is the project's tenant (control-plane
    // supervisor has none of its own).
    let project_tenant =
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
            .await?;
    // Verify the typed spec's (mode, policy) combo is coherent
    // before persisting. The rule lives next to `DeactivateSpec`
    // in `weft-broker-client::protocol` so every caller (this
    // handler, dispatcher /deactivate, supervisor's enqueue
    // construction) shares one validator. Today the supervisor
    // is the only caller and its three construction sites build
    // sane combos, but a future caller (or a regression on the
    // supervisor side) hits this boundary check.
    if let weft_broker_client::protocol::LifecycleSpec::Deactivate(spec) = &req.spec {
        if let Err(msg) = spec.validate() {
            return Err((StatusCode::BAD_REQUEST, msg.to_string()));
        }
    }
    // The typed `LifecycleSpec` only constructs `Deactivate(...)` /
    // `Reactivate`, so a caller can't enqueue a supervisor-owned
    // verb here. `into_row_columns()` returns running_policy =
    // None for both variants (Deactivate carries it inside
    // spec_json; Reactivate has no policy). Bind NULL.
    let (verb, running_policy, spec_json) = req.spec.into_row_columns();
    let running_policy_str = running_policy.map(|p| p.as_str());
    let issued_by_pod = caller.pod_name.as_deref().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            "supervisor token missing pod claim".to_string(),
        )
    })?;
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, \
          spec_json, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, NULL, $3, $4, $5, $6, \
                 EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(&project_tenant)
    .bind(&req.project_id)
    .bind(verb.as_str())
    .bind(running_policy_str)
    .bind(&spec_json)
    .bind(issued_by_pod)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // Wake any dispatcher pod's `lifecycle_claimer` listener.
    // Best-effort: a missed NOTIFY (dropped connection) falls back
    // to the 30s safety poll in the claimer. We log on failure but
    // don't bail; the row is already persisted.
    if let Err(e) = sqlx::query("SELECT pg_notify('weft_lifecycle_cmd', $1)")
        .bind(row.0.to_string())
        .execute(&state.pool)
        .await
    {
        tracing::warn!(
            target: "weft_broker::handlers",
            error = %e,
            command_id = row.0,
            "pg_notify(weft_lifecycle_cmd) failed; claimer's safety poll will catch it"
        );
    }
    Ok(Json(SupervisorEnqueueLifecycleResponse { command_id: row.0 }))
}

/// Supervisor reads the project's per-(node, image_name) hash map so
/// it can resolve `Image::Local` references at apply time. The map
/// was stored on the project row by the CLI in the most recent
/// `/infra/sync` body.
pub async fn supervisor_project_image_tags(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorProjectImageTagsRequest>,
) -> Resp<SupervisorProjectImageTagsResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let id = req
        .project_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad project_id".to_string()))?;
    use sqlx::Row;
    let row = sqlx::query("SELECT infra_image_tags_json FROM project WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // No row = project doesn't exist; broker returns empty tags
    // (the supervisor's caller will surface "MissingLocalImage"
    // downstream against a clear context). Row exists but decode
    // fails = schema drift; fail loud rather than coerce to empty
    // (which would mask the real cause).
    let tags: std::collections::HashMap<String, String> = match row {
        None => std::collections::HashMap::new(),
        Some(r) => {
            let value: serde_json::Value = r
                .try_get("infra_image_tags_json")
                .map_err(|e| internal(anyhow::anyhow!("decode infra_image_tags_json: {e}")))?;
            // The column is structured as `{ node_id: { image_name: tag } }`.
            // Treat NULL or empty-object as "no tags set yet" (legal
            // for projects with no Local images). Anything else
            // that isn't an object is corruption.
            let outer = match value {
                serde_json::Value::Null => return Ok(Json(SupervisorProjectImageTagsResponse {
                    tags: Default::default(),
                })),
                serde_json::Value::Object(m) => m,
                other => {
                    return Err(internal(anyhow::anyhow!(
                        "infra_image_tags_json for project={} is not an object: {other:?}",
                        req.project_id
                    )));
                }
            };
            let Some(inner) = outer.get(&req.node_id) else {
                return Ok(Json(SupervisorProjectImageTagsResponse {
                    tags: Default::default(),
                }));
            };
            let inner_obj = inner.as_object().ok_or_else(|| {
                internal(anyhow::anyhow!(
                    "infra_image_tags_json[{}] is not a string-to-string map",
                    req.node_id
                ))
            })?;
            let mut out = std::collections::HashMap::with_capacity(inner_obj.len());
            for (k, v) in inner_obj {
                let s = v.as_str().ok_or_else(|| {
                    internal(anyhow::anyhow!(
                        "infra_image_tags_json[{}][{k}] is not a string",
                        req.node_id
                    ))
                })?;
                out.insert(k.clone(), s.to_string());
            }
            out
        }
    };
    Ok(Json(SupervisorProjectImageTagsResponse { tags }))
}

/// Worker-callable: enqueue an Apply lifecycle command after the
/// engine's local skip/fresh/replace decision. The supervisor picks
/// up the command on its next poll.
pub async fn infra_enqueue_apply(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<InfraEnqueueApplyRequest>,
) -> Resp<InfraEnqueueApplyResponse> {
    require_node_id(&req.node_id)?;
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    // Bind the project's tenant on the row (equals the worker's tenant,
    // since the ownership check passed). Uniform with the supervisor
    // enqueue paths: the row tenant always comes from the resource.
    let project_tenant =
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
            .await?;
    // Dedup against an in-flight apply for the same (project, node):
    // a worker restart that retries this call must NOT double-enqueue.
    // The partial unique index `uq_lifecycle_cmd_pending_apply`
    // enforces "at most one pending apply per (project_id, node_id)".
    //
    // Single-statement ON CONFLICT DO UPDATE: the no-op SET on
    // `issued_at_unix = excluded value` causes the UPDATE branch to
    // be taken AND `RETURNING id` to fire. (DO NOTHING would skip
    // the RETURNING and force a separate SELECT, leaving a race
    // window where the existing row could be completed between
    // INSERT and SELECT.) The "update" is a no-op on real values
    // because we set the same `issued_at_unix` we'd have written.
    let issued_by_pod = caller.pod_name.as_deref().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            "worker token missing pod claim".to_string(),
        )
    })?;
    // Apply doesn't carry a running_policy (no in-flight executions
    // to drain; the supervisor just applies). NULL the column.
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_lifecycle_command \
         (tenant_id, project_id, node_id, verb, running_policy, \
          spec_json, issued_by_pod, issued_at_unix) \
         VALUES ($1, $2, $3, $4, NULL, $5, $6, \
                 EXTRACT(EPOCH FROM NOW())::BIGINT) \
         ON CONFLICT (project_id, node_id) \
           WHERE completed_at_unix IS NULL AND verb = 'apply' \
           DO UPDATE SET issued_at_unix = \
             infra_lifecycle_command.issued_at_unix \
         RETURNING id",
    )
    .bind(&project_tenant)
    .bind(&req.project_id)
    .bind(&req.node_id)
    .bind(weft_broker_client::protocol::InfraLifecycleVerb::Apply.as_str())
    .bind(&req.spec_json)
    .bind(issued_by_pod)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    Ok(Json(InfraEnqueueApplyResponse { command_id: row.0 }))
}

/// Worker-callable: poll a previously-issued apply command for its
/// terminal state. Worker calls this in a loop. One round-trip per
/// poll tick keeps the broker stateless w.r.t. wait semantics.
pub async fn infra_wait_apply(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<InfraWaitApplyRequest>,
) -> Resp<InfraWaitApplyResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT completed_at_unix, outcome, outcome_message, project_id \
         FROM infra_lifecycle_command WHERE id = $1",
    )
    .bind(req.command_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let Some(r) = row else {
        return Err((StatusCode::NOT_FOUND, "no such command".into()));
    };
    // Defense-in-depth: the command must belong to the caller's project.
    let cmd_project: String = r
        .try_get("project_id")
        .map_err(|e| internal(anyhow::anyhow!("decode project_id: {e}")))?;
    if cmd_project != req.project_id {
        return Err((StatusCode::FORBIDDEN, "command belongs to a different project".into()));
    }
    let done: Option<i64> = r
        .try_get::<Option<i64>, _>("completed_at_unix")
        .map_err(|e| internal(anyhow::anyhow!("decode completed_at_unix: {e}")))?;
    let outcome_str: Option<String> = r
        .try_get::<Option<String>, _>("outcome")
        .map_err(|e| internal(anyhow::anyhow!("decode outcome: {e}")))?;
    let message: Option<String> = r
        .try_get::<Option<String>, _>("outcome_message")
        .map_err(|e| internal(anyhow::anyhow!("decode outcome_message: {e}")))?;
    // Parse the outcome string into the typed enum. NULL while
    // pending; an unknown string means schema drift and we fail
    // loud so the worker doesn't silently coerce it.
    use weft_broker_client::protocol::LifecycleOutcome;
    let outcome = match (done.is_some(), outcome_str.as_deref()) {
        (false, _) => None,
        (true, Some(s)) => Some(LifecycleOutcome::parse(s).ok_or_else(|| {
            internal(anyhow::anyhow!(
                "infra_lifecycle_command.id={} has unknown outcome '{s}'",
                req.command_id
            ))
        })?),
        (true, None) => {
            return Err(internal(anyhow::anyhow!(
                "infra_lifecycle_command.id={} completed but outcome is NULL",
                req.command_id
            )));
        }
    };
    Ok(Json(InfraWaitApplyResponse {
        completed: done.is_some(),
        outcome,
        outcome_message: message,
    }))
}

pub async fn supervisor_remove_node(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorRemoveNodeRequest>,
) -> Resp<SupervisorRemoveNodeResponse> {
    require_supervisor(&caller)?;
    require_node_id(&req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    // Ownership gate: only the pod that currently OWNS the project may
    // cascade-delete its infra_node + cancel its pending commands. A
    // supervisor that lost ownership mid-Terminate must NOT wipe rows
    // out from under the new owner. 410 → the supervisor aborts the
    // command (leaving it uncompleted for the new owner to re-run). The
    // check runs INSIDE the cascade transaction so the ownership read
    // and the deletes share one snapshot (no TOCTOU window). The
    // identity is the supervisor's claim id (`req.pod_name` =
    // WEFT_POD_NAME, what keys `infra_owner`), not the auth token's
    // suffixed Pod name.
    // Cascade in one transaction so a remove-then-readd of the same
    // node_id starts clean: no stale events claiming "flaky" from
    // the prior generation, no pending lifecycle commands from the
    // generation we just terminated.
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|e| internal(anyhow::anyhow!("begin tx: {e}")))?;
    let owns: bool = sqlx::query_scalar(&format!(
        "SELECT {owns}",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$1", "$2"),
    ))
    .bind(&req.pod_name)
    .bind(&req.project_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("remove_node ownership check: {e}")))?;
    if !owns {
        return Err((
            StatusCode::GONE,
            format!(
                "remove_node rejected: {} no longer owns project {}",
                req.pod_name, req.project_id
            ),
        ));
    }
    let res = sqlx::query("DELETE FROM infra_node WHERE project_id = $1 AND node_id = $2")
        .bind(&req.project_id)
        .bind(&req.node_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(anyhow::anyhow!("delete infra_node: {e}")))?;
    sqlx::query(
        "DELETE FROM infra_event WHERE project_id = $1 AND node_id = $2",
    )
    .bind(&req.project_id)
    .bind(&req.node_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("delete infra_event: {e}")))?;
    // Cancel any not-yet-completed lifecycle commands targeting this
    // node_id by stamping a completion. Pending commands targeting
    // `node_id IS NULL` (project-wide) are intentionally left in
    // place; those are for the WHOLE project.
    use weft_broker_client::protocol::LifecycleOutcome;
    sqlx::query(
        "UPDATE infra_lifecycle_command \
            SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
                outcome = $3, \
                outcome_message = 'node removed by remove_node' \
          WHERE project_id = $1 AND node_id = $2 \
            AND completed_at_unix IS NULL",
    )
    .bind(&req.project_id)
    .bind(&req.node_id)
    .bind(LifecycleOutcome::Cancelled.as_str())
    .execute(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("cancel pending commands: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| internal(anyhow::anyhow!("commit: {e}")))?;
    Ok(Json(SupervisorRemoveNodeResponse {
        removed: res.rows_affected() > 0,
    }))
}

pub async fn supervisor_trigger_deps(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorTriggerDepsRequest>,
) -> Resp<SupervisorTriggerDepsResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let id = req
        .project_id
        .parse::<uuid::Uuid>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "bad project_id".to_string()))?;
    use sqlx::Row;
    let row = sqlx::query("SELECT project_json FROM project WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let Some(r) = row else {
        return Ok(Json(SupervisorTriggerDepsResponse { deps: Vec::new() }));
    };
    let project_json: String = r.try_get("project_json").map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let project: weft_core::project::ProjectDefinition =
        serde_json::from_str(&project_json).map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let deps = weft_core::project::compute_trigger_deps(&project)
        .into_iter()
        .map(|(infra_node_id, trigger_node_id)| SupervisorTriggerDep {
            infra_node_id,
            trigger_node_id,
        })
        .collect();
    Ok(Json(SupervisorTriggerDepsResponse { deps }))
}

pub async fn supervisor_running_count(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorRunningCountRequest>,
) -> Resp<SupervisorRunningCountResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    // "Running" = has a live worker pod. Parked / suspended
    // executions (form trigger waiting for input, timer waiting to
    // fire) hold ZERO workers per the project's runtime-tier rule
    // ("workers die on stall"). Counting them as running would
    // deadlock `running_policy=wait` against any project with a
    // long-lived parked trigger fire.
    //
    // Live worker = `worker_pod` row in (spawning, alive). The
    // partial index `idx_worker_pod_project_alive` covers it.
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT COUNT(*)::bigint AS n \
         FROM worker_pod \
         WHERE project_id = $1 \
           AND status IN ('spawning', 'alive')",
    )
    .bind(&req.project_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let running_count: i64 = row.try_get("n").map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    Ok(Json(SupervisorRunningCountResponse { running_count }))
}

/// Does the project have an uncompleted infra lifecycle command (a
/// user infra action: apply / stop / terminate is running)?
/// The supervisor's health loop checks this at the top of each project
/// tick and stands down while it's true, so an autonomous health
/// reconcile never races a user action over `infra_node.status`.
pub async fn supervisor_infra_command_in_flight(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorInfraCommandInFlightRequest>,
) -> Resp<SupervisorInfraCommandInFlightResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT EXISTS ( \
           SELECT 1 FROM infra_lifecycle_command \
           WHERE project_id = $1 AND completed_at_unix IS NULL \
         ) AS in_flight",
    )
    .bind(&req.project_id)
    .fetch_one(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let in_flight: bool = row.try_get("in_flight").map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    Ok(Json(SupervisorInfraCommandInFlightResponse { in_flight }))
}

pub async fn supervisor_command_complete(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorCommandCompleteRequest>,
) -> Resp<SupervisorCommandCompleteResponse> {
    require_supervisor(&caller)?;
    // The supervisor's own row claim already enforced tenant scope.
    // Re-check: the row we're completing must belong to this caller's tenant.
    use sqlx::Row;
    let row = sqlx::query("SELECT tenant_id FROM infra_lifecycle_command WHERE id = $1")
        .bind(req.command_id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let tenant_id: String = match row {
        None => return Err((StatusCode::NOT_FOUND, "no such command".into())),
        Some(r) => r
            .try_get("tenant_id")
            .map_err(|e| internal(anyhow::anyhow!("decode tenant_id: {e}")))?,
    };
    scope::require_tenant_in_scope(&caller, &tenant_id)?;
    // Supervisor completions: success (error=None), failure
    // (error=Some), or a user-requested cancellation the supervisor
    // honored mid-command (`cancelled=true`; `error` then carries the
    // halt point as the outcome message, never counted as a failure).
    use weft_broker_client::protocol::LifecycleOutcome;
    let outcome = if req.cancelled {
        LifecycleOutcome::Cancelled
    } else {
        match req.error {
            Some(_) => LifecycleOutcome::Failed,
            None => LifecycleOutcome::Succeeded,
        }
    };
    // Ownership check: only the pod that currently OWNS the project may
    // stamp the command terminal. A supervisor that lost ownership mid-
    // command (drain / lease takeover) must NOT complete it: leaving it
    // uncompleted is exactly what lets the new owner re-run and finish
    // it (no user re-action). Combined with `completed_at_unix IS NULL`,
    // this gives "exactly the current owner, exactly once."
    // Ownership identity is the supervisor's claim id (`req.pod_name` =
    // WEFT_POD_NAME, what keys `infra_owner`), not the auth token's Pod
    // name (suffixed, never matches the lease). Tenant scope was already
    // re-checked above via the token.
    let res = sqlx::query(&format!(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = $1, \
             outcome_message = $2 \
         WHERE id = $3 \
           AND completed_at_unix IS NULL \
           AND {owns}",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$4", "project_id"),
    ))
    .bind(outcome.as_str())
    .bind(req.error.as_deref())
    .bind(req.command_id)
    .bind(&req.pod_name)
    .execute(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // rows_affected=0 means the row was completed already, OR this pod
    // no longer owns the project (ownership moved to a sibling). Either
    // way this caller's view is stale; surface 410 so it aborts the
    // command without completing it.
    if res.rows_affected() == 0 {
        return Err((
            StatusCode::GONE,
            format!(
                "infra_lifecycle_command id={} not completable by {}: \
                 either already completed or project ownership moved to another pod",
                req.command_id, req.pod_name
            ),
        ));
    }
    Ok(Json(SupervisorCommandCompleteResponse {}))
}

/// Poll target for an executing supervisor: has the user requested
/// cancellation of the claimed command? Tenant-scoped like
/// `supervisor_command_complete`. A missing row (project removed
/// mid-command) reads as cancelled: the executor should stop working
/// on it either way.
pub async fn supervisor_command_cancel_requested(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<weft_broker_client::protocol::SupervisorCommandCancelRequestedRequest>,
) -> Resp<weft_broker_client::protocol::SupervisorCommandCancelRequestedResponse> {
    require_supervisor(&caller)?;
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT tenant_id, cancel_requested FROM infra_lifecycle_command WHERE id = $1",
    )
    .bind(req.command_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let Some(row) = row else {
        return Ok(Json(
            weft_broker_client::protocol::SupervisorCommandCancelRequestedResponse {
                cancel_requested: true,
            },
        ));
    };
    let tenant_id: String = row
        .try_get("tenant_id")
        .map_err(|e| internal(anyhow::anyhow!("decode tenant_id: {e}")))?;
    scope::require_tenant_in_scope(&caller, &tenant_id)?;
    let cancel_requested: bool = row
        .try_get("cancel_requested")
        .map_err(|e| internal(anyhow::anyhow!("decode cancel_requested: {e}")))?;
    Ok(Json(
        weft_broker_client::protocol::SupervisorCommandCancelRequestedResponse {
            cancel_requested,
        },
    ))
}

/// Reject empty-string `node_id` at the handler boundary.
/// Persisting an empty key (or matching against one) corrupts the
/// per-node indexes; better to bail at 400 than silently shape
/// the DB around it.
fn require_node_id(node_id: &str) -> Result<(), (StatusCode, String)> {
    if node_id.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "node_id required".into()));
    }
    Ok(())
}

fn require_supervisor(caller: &CallerIdentity) -> Result<(), (StatusCode, String)> {
    if caller.role != Role::InfraSupervisor {
        return Err((StatusCode::FORBIDDEN, "infra-supervisor only".into()));
    }
    Ok(())
}

// ---------- Signals ----------

pub async fn signal_list_for_pod(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalListForPodRequest>,
) -> Resp<SignalListForPodResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    // The listener is a trusted control-plane caller; it rehydrates the
    // signals placed on its own pod (mixed tenants). No per-tenant scope
    // check: placement (`listener_pod`) is the authority for what this
    // pod holds, and each returned row carries its own tenant.
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT token, tenant_id, node_id, spec_json, is_resume, color, \
                surface_kind, mount_path, auth_kind, auth_config, \
                kind_state, placement_generation \
         FROM signal WHERE listener_pod = $1",
    )
    .bind(&req.pod_name)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // try_get on a NOT NULL column should never fail; if it does the
    // row shape has drifted from the schema. Surface that loudly
    // rather than silently returning empty strings to the listener.
    let out = rows
        .into_iter()
        .map(|r| -> Result<SignalRowWire, anyhow::Error> {
            let surface_str: String = r.try_get("surface_kind")?;
            let surface_kind = weft_broker_client::protocol::SignalSurfaceKind::parse(
                &surface_str,
            )
            .ok_or_else(|| anyhow::anyhow!("unknown surface_kind '{surface_str}'"))?;
            let auth_str: String = r.try_get("auth_kind")?;
            let auth_kind = weft_broker_client::protocol::SignalAuthKind::parse(&auth_str)
                .ok_or_else(|| anyhow::anyhow!("unknown auth_kind '{auth_str}'"))?;
            Ok(SignalRowWire {
                token: r.try_get("token")?,
                // Each row carries its own tenant (the pod holds mixed
                // tenants); the listener stamps it on the rehydrated
                // registry entry so held-event fires are correctly
                // tenanted.
                tenant_id: r.try_get("tenant_id")?,
                node_id: r.try_get("node_id")?,
                spec_json: r.try_get("spec_json")?,
                is_resume: r.try_get("is_resume")?,
                color: r.try_get("color")?,
                surface_kind,
                mount_path: r.try_get("mount_path")?,
                auth_kind,
                auth_config: r.try_get("auth_config")?,
                kind_state: r.try_get("kind_state")?,
                placement_generation: r.try_get("placement_generation")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal(anyhow::anyhow!("signal row decode: {e}")))?;
    Ok(Json(SignalListForPodResponse { rows: out }))
}

// ---------- helpers ----------

fn require_worker(caller: &CallerIdentity) -> Result<(), (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    Ok(())
}

/// Reject if the request claims a `pod_name` other than the one the
/// kubelet bound into the caller's SA token.
fn require_pod_name_matches(
    caller: &CallerIdentity,
    claimed: &str,
) -> Result<(), (StatusCode, String)> {
    let bound = caller.pod_name.as_deref().ok_or((
        StatusCode::FORBIDDEN,
        "caller token has no bound pod name; refusing pod-bound op".into(),
    ))?;
    if bound != claimed {
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_role = ?caller.role,
            bound_pod = %bound,
            claimed_pod = %claimed,
            "broker rejected pod_name mismatch"
        );
        return Err((
            StatusCode::FORBIDDEN,
            "claimed pod_name does not match SA token's bound pod".into(),
        ));
    }
    Ok(())
}

async fn require_task_owned_by(
    state: &Arc<BrokerState>,
    caller: &CallerIdentity,
    task_id: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    let row: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT tenant_id FROM task WHERE id = $1",
    )
    .bind(task_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let owner = row.and_then(|(t,)| t).ok_or((
        StatusCode::NOT_FOUND,
        format!("unknown task {task_id}"),
    ))?;
    scope::require_tenant_in_scope(caller, &owner)
}

async fn require_worker_pod_owned_by(
    state: &Arc<BrokerState>,
    caller: &CallerIdentity,
    pod_name: &str,
) -> Result<(), (StatusCode, String)> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT project_id FROM worker_pod WHERE pod_name = $1",
    )
    .bind(pod_name)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let Some((project_id,)) = row else {
        // No row means register_alive hasn't run yet for this pod.
        // Heartbeat / mark_done MUST come after register_alive in the
        // worker boot sequence, so this is either a misconfigured
        // caller or a token forging an arbitrary `pod_name` it doesn't
        // own. Either way, refuse loudly: an open-door fallback here
        // would let any worker token poison rows for pod names that
        // haven't yet been claimed by their legitimate owner.
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_role = ?caller.role,
            pod_name,
            "broker rejected worker_pod op for unregistered pod"
        );
        return Err((
            StatusCode::FORBIDDEN,
            format!("worker_pod '{pod_name}' has no register_alive row"),
        ));
    };
    // Enforce ownership; the returned tenant is not needed here.
    scope::require_project_owned_by(&state.scope_cache, &state.pool, caller, &project_id)
        .await
        .map(|_| ())
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_fire_below_current_generation_is_fenced() {
        // Old pod fired under gen 1 after a move bumped the row to gen 2.
        assert!(fire_is_fenced(1, Some(2)));
    }

    #[test]
    fn current_holders_fire_is_not_fenced() {
        // Equal generation = the live holder; never fenced.
        assert!(!fire_is_fenced(2, Some(2)));
        // A higher fire generation than the row (shouldn't happen, but be
        // safe) is also not fenced: only STRICTLY-stale fires are dropped.
        assert!(!fire_is_fenced(3, Some(2)));
    }

    #[test]
    fn fire_for_a_signal_with_no_row_is_not_fenced() {
        // No row means no move could have re-placed it; the downstream
        // scope check handles a genuinely-missing signal. Never fence on
        // absence (which would silently drop a legitimate fire).
        assert!(!fire_is_fenced(0, None));
        assert!(!fire_is_fenced(5, None));
    }

    #[test]
    fn missing_generation_field_defaults_to_zero_and_is_fenced_if_row_advanced() {
        // A fire with no placement_generation field is read as 0 by the
        // handler; if the row has advanced past 0 (any real placement),
        // that ancient fire is correctly fenced.
        assert!(fire_is_fenced(0, Some(1)));
        // ...but against a never-advanced row (gen 0) it is NOT fenced.
        assert!(!fire_is_fenced(0, Some(0)));
    }

    #[test]
    fn merge_anchor_first_resource_sets_tenant() {
        let mut anchor = None;
        merge_anchor_tenant(&mut anchor, "acme".into()).unwrap();
        assert_eq!(anchor.as_deref(), Some("acme"));
    }

    #[test]
    fn merge_anchor_same_tenant_agrees() {
        let mut anchor = Some("acme".to_string());
        merge_anchor_tenant(&mut anchor, "acme".into()).unwrap();
        assert_eq!(anchor.as_deref(), Some("acme"));
    }

    #[test]
    fn merge_anchor_different_tenants_rejected() {
        // A task naming a project in one tenant and a color in another is
        // ambiguous; refuse it rather than silently stamping either.
        let mut anchor = Some("acme".to_string());
        let err = merge_anchor_tenant(&mut anchor, "globex".into()).unwrap_err();
        assert_eq!(err.0, StatusCode::CONFLICT);
    }
}
