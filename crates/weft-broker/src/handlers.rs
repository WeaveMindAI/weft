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
use weft_task_store::tasks::{ClaimFilter, DedupOutcome, NewTask, TaskTarget};
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
    let claimed_pod = req
        .pod_name
        .as_deref()
        .ok_or((StatusCode::BAD_REQUEST, "pod_name required for worker journal write".into()))?;
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
            caller_tenant = %caller.tenant_id,
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

pub async fn task_enqueue_dedup(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskEnqueueDedupRequest>,
) -> Resp<TaskEnqueueDedupResponse> {
    let kind = req.spec.kind;
    let target = req.spec.target;

    // Per-role allow list of kinds. Anything else is a 403.
    match caller.role {
        Role::Worker => {
            // Workers enqueue control-plane work for the dispatcher
            // to handle: register a wake signal, provision a sidecar,
            // and durable side-effect records (cost + log) that must
            // survive the worker pod dying.
            if !matches!(
                kind,
                TaskKind::RegisterSignal
                    | TaskKind::ProvisionSidecar
                    | TaskKind::RecordCost
                    | TaskKind::RecordLog
            ) {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("worker may not enqueue task kind {}", kind.as_str()),
                ));
            }
            // RecordCost payload validation: reject negative amounts
            // at enqueue time so a malicious worker can't submit a
            // negative-cost row and die before the dispatcher's
            // executor would catch it.
            if kind == TaskKind::RecordCost {
                let amount = req
                    .spec
                    .payload
                    .get("amount_usd")
                    .and_then(|v| v.as_f64())
                    .ok_or((
                        StatusCode::BAD_REQUEST,
                        "record_cost payload missing numeric amount_usd".into(),
                    ))?;
                if !(amount.is_finite() && amount >= 0.0) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        format!("record_cost amount_usd must be a finite non-negative number; got {amount}"),
                    ));
                }
            }
        }
        Role::Listener => {
            // Listeners enqueue exactly one kind: a held-event fire.
            if kind != TaskKind::FireSignal {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("listener may not enqueue task kind {}", kind.as_str()),
                ));
            }
        }
        Role::Sidecar => {
            return Err((
                StatusCode::FORBIDDEN,
                "sidecar may not enqueue tasks".into(),
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

    // Tenant scope check: the payload's tenant_id, if present, must
    // match the caller's tenant. If absent, fill it from caller; if
    // present but mismatched, 403.
    let claimed_tenant = req.spec.tenant_id.as_deref().unwrap_or(&caller.tenant_id);
    scope::require_tenant_eq(&caller, claimed_tenant)?;

    if let Some(project_id) = req.spec.project_id.as_deref() {
        scope::require_project_owned_by(
            &state.scope_cache,
            &state.pool,
            &caller,
            project_id,
        )
        .await?;
    }
    if let Some(color) = req.spec.color.as_deref() {
        scope::require_color_owned_by(&state.scope_cache, &state.pool, &caller, color).await?;
    }
    if kind == TaskKind::FireSignal {
        // Listener firing a held event: the token must belong to the
        // listener's tenant. Pull token out of payload.
        let token = req
            .spec
            .payload
            .get("token")
            .and_then(|v| v.as_str())
            .ok_or((StatusCode::BAD_REQUEST, "fire_signal payload missing token".into()))?;
        scope::require_signal_owned_by(&state.scope_cache, &state.pool, &caller, token).await?;
    }

    // Reuse the wire's own conversion to keep the field list in one
    // place; force tenant_id to the caller's identity (never trust
    // what the wire claimed).
    let mut new_task: NewTask = req.spec.into_new_task();
    new_task.tenant_id = Some(caller.tenant_id.clone());
    let outcome = state.tasks.enqueue_dedup(new_task).await.map_err(internal)?;
    let (id, inserted) = match outcome {
        DedupOutcome::Inserted(id) => (id, true),
        DedupOutcome::AlreadyLive(id) => (id, false),
    };
    Ok(Json(TaskEnqueueDedupResponse { id, inserted }))
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
    let filter: ClaimFilter = req.filter.into_filter();
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
    // Latest-claim-wins binding: whenever a worker pod successfully
    // claims a task carrying a `color`, stamp its pod_name onto
    // execution_color.owner_pod_name. Subsequent journal_record
    // calls verify against this binding so a compromised tenant pod
    // can't journal events under sibling colors it never claimed.
    // Always stamping (not "iff NULL") handles resume: the second
    // worker pod that picks up a Resume task for a long-suspended
    // color takes over ownership from the now-dead original. The
    // task table's claim semantics already enforce one-active-pod-
    // per-task, so there is no overlap window where two pods could
    // both think they own writes.
    if let Some(t) = task.as_ref() {
        if let Some(color) = t.color.as_deref() {
            sqlx::query(
                "UPDATE execution_color SET owner_pod_name = $1 \
                 WHERE color = $2",
            )
            .bind(&req.pod_id)
            .bind(color)
            .execute(&state.pool)
            .await
            .map_err(internal)?;
        }
    }
    Ok(Json(TaskClaimOneResponse {
        task: task.map(TaskWire::from_task),
    }))
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
        .heartbeat(&req.pod_name)
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

// ---------- Infra ----------

pub async fn infra_sidecar_endpoint(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<InfraSidecarEndpointRequest>,
) -> Resp<InfraSidecarEndpointResponse> {
    // Only workers need sidecar endpoint URLs; listeners and sidecars
    // have no business resolving them. Least-privilege beats relying
    // on NetworkPolicy alone.
    require_worker(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let endpoint_url = state
        .infra
        .sidecar_endpoint(&req.project_id, &req.node_id)
        .await
        .map_err(internal)?;
    Ok(Json(InfraSidecarEndpointResponse { endpoint_url }))
}

// ---------- Signals ----------

pub async fn signal_list_for_tenant(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalListForTenantRequest>,
) -> Resp<SignalListForTenantResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    scope::require_tenant_eq(&caller, &req.tenant_id)?;

    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT token, node_id, spec_json, is_resume, color, \
                surface_kind, mount_path, auth_kind, auth_config, \
                kind_state \
         FROM signal WHERE tenant_id = $1",
    )
    .bind(&req.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // try_get on a NOT NULL column should never fail; if it does the
    // row shape has drifted from the schema. Surface that loudly
    // rather than silently returning empty strings to the listener.
    let out = rows
        .into_iter()
        .map(|r| -> Result<SignalRowWire, sqlx::Error> {
            Ok(SignalRowWire {
                token: r.try_get("token")?,
                node_id: r.try_get("node_id")?,
                spec_json: r.try_get("spec_json")?,
                is_resume: r.try_get("is_resume")?,
                color: r.try_get("color")?,
                surface_kind: r.try_get("surface_kind")?,
                mount_path: r.try_get("mount_path")?,
                auth_kind: r.try_get("auth_kind")?,
                auth_config: r.try_get("auth_config")?,
                kind_state: r.try_get("kind_state")?,
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal(anyhow::anyhow!("signal row decode: {e}")))?;
    Ok(Json(SignalListForTenantResponse { rows: out }))
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
            caller_tenant = %caller.tenant_id,
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
    scope::require_tenant_eq(caller, &owner)
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
            caller_tenant = %caller.tenant_id,
            caller_role = ?caller.role,
            pod_name,
            "broker rejected worker_pod op for unregistered pod"
        );
        return Err((
            StatusCode::FORBIDDEN,
            format!("worker_pod '{pod_name}' has no register_alive row"),
        ));
    };
    scope::require_project_owned_by(&state.scope_cache, &state.pool, caller, &project_id).await
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}"))
}
