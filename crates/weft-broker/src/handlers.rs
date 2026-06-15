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
            // to handle: register a wake signal, provision infra,
            // and durable side-effect records (cost + log) that must
            // survive the worker pod dying.
            if !matches!(
                kind,
                TaskKind::RegisterSignal
                    | TaskKind::RecordCost
                    | TaskKind::RecordLog
                    | TaskKind::EnsureStorageBox
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

    // `req.spec` IS a `NewTask` directly (the wire shape matches
    // the type). Force tenant_id to the caller's identity: never
    // trust what the wire claimed.
    let mut new_task = req.spec;
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

// ---------- Supervisor surface ----------

pub async fn supervisor_projects_for_tenant(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorProjectsForTenantRequest>,
) -> Resp<SupervisorProjectsForTenantResponse> {
    require_supervisor(&caller)?;
    scope::require_tenant_eq(&caller, &req.tenant_id)?;
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT id::TEXT AS project_id, project_namespace, status, deactivated_by_health \
         FROM project WHERE tenant_id = $1",
    )
    .bind(&req.tenant_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let mut projects = Vec::with_capacity(rows.len());
    for r in rows {
        let project_id: String = r
            .try_get("project_id")
            .map_err(|e| internal(anyhow::anyhow!("decode project_id: {e}")))?;
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
        if project_namespace.is_empty() {
            // Skip projects without a namespace (mid-register state).
            continue;
        }
        projects.push(SupervisorProject {
            project_id,
            project_namespace,
            status,
            deactivated_by_health,
        });
    }
    Ok(Json(SupervisorProjectsForTenantResponse { projects }))
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
    scope::require_tenant_eq(&caller, &req.tenant_id)?;
    // Verb filter: the supervisor claims only the verbs it owns.
    // Dispatcher verbs (`deactivate`, `reactivate`) are claimed by
    // dispatcher pods directly; the supervisor MUST skip them or it
    // would block dispatcher-owned actions behind its own loop.
    //
    // The claim predicate (shared with the dispatcher's claimer) is
    // a lease: a row claimed by a now-dead pod becomes reclaimable
    // after `CLAIM_LEASE_TTL`. Without the lease, a `kubectl delete`
    // of a supervisor pod mid-execution pins the row forever
    // (claimed_by_pod = dead-pod-name, completed_at_unix IS NULL).
    let sql = format!(
        "UPDATE infra_lifecycle_command \
         SET claimed_by_pod = $1, claimed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT \
         WHERE id = ( \
            SELECT id FROM infra_lifecycle_command \
            WHERE tenant_id = $2 \
              AND verb IN ('apply', 'stop', 'terminate') \
              AND {predicate} \
            ORDER BY id ASC \
            FOR UPDATE SKIP LOCKED \
            LIMIT 1 \
         ) \
         RETURNING id, project_id, node_id, verb, running_policy, spec_json, force",
        predicate = weft_broker_client::lifecycle_command::claimable_predicate(),
    );
    let row = sqlx::query(&sql)
        .bind(&req.claimer_pod)
        .bind(&req.tenant_id)
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
    Ok(SupervisorCommandRow {
        id,
        project_id,
        node_id,
        verb,
        running_policy,
        spec_json,
        force,
    })
}

pub async fn supervisor_event_record(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorEventRecordRequest>,
) -> Resp<SupervisorEventRecordResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, &req.project_id)
        .await?;
    let row: (i64,) = sqlx::query_as(
        "INSERT INTO infra_event \
         (tenant_id, project_id, node_id, kind, payload, at_unix) \
         VALUES ($1, $2, $3, $4, $5, EXTRACT(EPOCH FROM NOW())::BIGINT) \
         RETURNING id",
    )
    .bind(&caller.tenant_id)
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
    let caller_pod_opt = caller.pod_name.as_deref();
    let res = if let Some(cid) = req.command_id {
        // Lifecycle-driven write: require we still own the
        // command's claim (and the command targets this row).
        let caller_pod = caller_pod_opt.ok_or((
            StatusCode::FORBIDDEN,
            "supervisor caller missing pod_name in identity".to_string(),
        ))?;
        sqlx::query(&format!(
            "UPDATE infra_node SET {set_clause} \
             WHERE project_id = $5 AND node_id = $6 AND EXISTS ( \
               SELECT 1 FROM infra_lifecycle_command \
               WHERE id = $7 \
                 AND project_id = $5 \
                 AND (node_id = $6 OR node_id IS NULL) \
                 AND claimed_by_pod = $8 \
                 AND completed_at_unix IS NULL \
             )"
        ))
        .bind(&unit_key)
        .bind(req.status.as_str())
        .bind(req.failure_stage.map(|s| s.as_str()))
        .bind(req.failure_message.as_deref())
        .bind(&req.project_id)
        .bind(&req.node_id)
        .bind(cid)
        .bind(caller_pod)
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
    //   - (command branch) the lifecycle command was completed or
    //     reclaimed by a sibling pod, or named a (project, node) that
    //     doesn't match;
    //   - (autonomous branch) a user infra action's command is in
    //     flight for this node, so the health reconcile must stand
    //     down (the fence blocked it).
    // All are "this caller's view is stale or it must not write right
    // now"; surface as 410 so the supervisor logs + skips.
    if res.rows_affected() == 0 {
        return Err((
            StatusCode::GONE,
            format!(
                "set_status raced: infra_node row gone, command completed, \
                 or claim reassigned (project={}, node={}, cmd={:?})",
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
/// The INSERT pulls its values FROM `infra_lifecycle_command`
/// filtered to the caller's still-claimed apply command, so the
/// existence check and the write share one row snapshot (no
/// window between "I checked" and "I wrote"). Zero rows affected
/// → the claim was reassigned (lease takeover) or the command
/// completed/cancelled (remove_node cascade) → 410.
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
    caller_pod: &str,
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
        "INSERT INTO infra_node \
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
           AND claimed_by_pod = $12 \
           AND completed_at_unix IS NULL \
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
    .bind(caller_pod)
    .execute(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    if res.rows_affected() == 0 {
        return Err((
            StatusCode::GONE,
            format!(
                "{op} raced: command id={command_id} no longer claimed by {caller_pod} \
                 or already completed"
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
    let caller_pod = caller.pod_name.as_deref().ok_or((
        StatusCode::FORBIDDEN,
        "supervisor caller missing pod_name in identity".to_string(),
    ))?;
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
        caller_pod,
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
    let caller_pod = caller.pod_name.as_deref().ok_or((
        StatusCode::FORBIDDEN,
        "supervisor caller missing pod_name in identity".to_string(),
    ))?;
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
        caller_pod,
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
    .bind(&caller.tenant_id)
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
    .bind(&caller.tenant_id)
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
    // Cascade in one transaction so a remove-then-readd of the same
    // node_id starts clean: no stale events claiming "flaky" from
    // the prior generation, no pending lifecycle commands from the
    // generation we just terminated.
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(|e| internal(anyhow::anyhow!("begin tx: {e}")))?;
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
    scope::require_tenant_eq(&caller, &tenant_id)?;
    // Supervisor completions are either success (error=None) or
    // failure (error=Some). Cancellation is broker-side only
    // (remove_node); the supervisor never produces a cancelled
    // outcome.
    use weft_broker_client::protocol::LifecycleOutcome;
    let outcome = match req.error {
        Some(_) => LifecycleOutcome::Failed,
        None => LifecycleOutcome::Succeeded,
    };
    // Lease ownership check: a partitioned supervisor pod whose
    // lease expired may have been reclaimed by a sibling pod. The
    // stale pod must not be able to stamp a terminal state over
    // the new owner's still-running work. `AND claimed_by_pod =
    // $caller_pod` rejects writes from anyone other than the
    // current claimer. Combined with `completed_at_unix IS NULL`,
    // this gives us "exactly the claimer, exactly once."
    let caller_pod = caller.pod_name.as_deref().ok_or((
        StatusCode::FORBIDDEN,
        "supervisor caller missing pod_name in identity".to_string(),
    ))?;
    let res = sqlx::query(
        "UPDATE infra_lifecycle_command \
         SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
             outcome = $1, \
             outcome_message = $2 \
         WHERE id = $3 \
           AND completed_at_unix IS NULL \
           AND claimed_by_pod = $4",
    )
    .bind(outcome.as_str())
    .bind(req.error.as_deref())
    .bind(req.command_id)
    .bind(caller_pod)
    .execute(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // rows_affected=0 means the row was completed by someone else
    // first, OR was reclaimed by a sibling pod via lease expiry.
    // Either way, this caller's view of the world is stale.
    if res.rows_affected() == 0 {
        return Err((
            StatusCode::GONE,
            format!(
                "infra_lifecycle_command id={} not completable by {caller_pod}: \
                 either already completed or claim reassigned to another pod",
                req.command_id
            ),
        ));
    }
    Ok(Json(SupervisorCommandCompleteResponse {}))
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
                node_id: r.try_get("node_id")?,
                spec_json: r.try_get("spec_json")?,
                is_resume: r.try_get("is_resume")?,
                color: r.try_get("color")?,
                surface_kind,
                mount_path: r.try_get("mount_path")?,
                auth_kind,
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

/// `POST /storage/authorize`: the identity authority behind the
/// storage box's prefix wall. The RELAYED bearer (the box forwards
/// its caller's token; the dispatcher forwards a box's token) is
/// reviewed and interpreted into the verified facts the wall needs:
///   - worker  -> tenant + project from the token's NAMESPACE
///     (DB-backed; nothing is claimed), color verified exactly like
///     journal writes (`execution_color.owner_pod_name` must be the
///     caller's pod, and the color must belong to the caller's
///     project).
///   - the dispatcher (`weft-dispatcher` in `weft-system`) -> the
///     control plane.
///   - a storage box (`weft-storage-sa` in a tenant namespace) ->
///     StorageBox (used by the dispatcher to authenticate
///     grow/shrink requests).
/// This endpoint deliberately bypasses the tenant role table: its
/// caller universe is wider than the broker's data endpoints.
pub async fn storage_authorize(
    State(state): State<Arc<BrokerState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<StorageAuthorizeRequest>,
) -> Resp<StorageAuthorizeResponse> {
    use crate::auth::{DISPATCHER_NS, DISPATCHER_SA, STORAGE_SA, WORKER_SA};
    let reviewed = crate::auth::reviewed_token(&state, &headers).await?;
    match reviewed.sa_name.as_str() {
        DISPATCHER_SA if reviewed.namespace == DISPATCHER_NS => {
            Ok(Json(StorageAuthorizeResponse::ControlPlane))
        }
        STORAGE_SA => {
            let tenant_id = scope::lookup_namespace_tenant(
                &state.scope_cache,
                &state.pool,
                &reviewed.namespace,
            )
            .await?;
            Ok(Json(StorageAuthorizeResponse::StorageBox { tenant_id }))
        }
        WORKER_SA => {
            // Resolve project AND tenant from the ONE `project` row for
            // this namespace, not from two independent table lookups.
            // A worker's namespace IS its project, and the project row
            // carries the tenant, so a single source of truth removes
            // any chance of the namespace->tenant and project->tenant
            // mappings disagreeing.
            //
            // SAFETY (registration gate preserved by write ordering):
            // the dispatcher's project register path writes the
            // `weft_namespace_tenant` registry row (via
            // project_namespace::ensure -> namespace_registry::register)
            // BEFORE it writes the `project` row's `project_namespace`
            // column (register_with_hashes), both with the SAME tenant.
            // So a `project` row whose `project_namespace` matches here
            // could only exist if the namespace was already registered
            // to that same tenant: trusting `project.tenant_id` is
            // equivalent to going through the registry gate, not weaker.
            // (If that ordering is ever reversed, restore the explicit
            // `lookup_namespace_tenant` gate + a tenant-agreement check.)
            let row: Option<(String, String)> = sqlx::query_as(
                "SELECT id::text, tenant_id FROM project WHERE project_namespace = $1",
            )
            .bind(&reviewed.namespace)
            .fetch_optional(&state.pool)
            .await
            .map_err(internal)?;
            let (project_id, tenant_id) = row.ok_or((
                StatusCode::FORBIDDEN,
                format!("namespace '{}' is not a registered project namespace", reviewed.namespace),
            ))?;
            let color = match req.color {
                None => None,
                Some(color) => {
                    let row: Option<(String, String, Option<String>)> = sqlx::query_as(
                        "SELECT tenant_id, project_id, owner_pod_name \
                         FROM execution_color WHERE color = $1",
                    )
                    .bind(&color)
                    .fetch_optional(&state.pool)
                    .await
                    .map_err(internal)?;
                    let Some((color_tenant, color_project, owner_pod)) = row else {
                        return Err((StatusCode::FORBIDDEN, "unknown execution color".into()));
                    };
                    if color_tenant != tenant_id || color_project != project_id {
                        tracing::warn!(
                            target: "weft_broker::scope",
                            caller_ns = %reviewed.namespace,
                            color = %color,
                            "storage authorize rejected cross-project color claim"
                        );
                        return Err((
                            StatusCode::FORBIDDEN,
                            "color belongs to a different project".into(),
                        ));
                    }
                    // Same gate as journal writes: only the pod that
                    // claimed the execution drives its color.
                    if reviewed.pod_name.is_none()
                        || owner_pod.as_deref() != reviewed.pod_name.as_deref()
                    {
                        return Err((
                            StatusCode::FORBIDDEN,
                            "color is not owned by the calling pod".into(),
                        ));
                    }
                    Some(color)
                }
            };
            Ok(Json(StorageAuthorizeResponse::Worker { tenant_id, project_id, color }))
        }
        other => Err((
            StatusCode::FORBIDDEN,
            format!("service account '{other}' has no storage identity"),
        )),
    }
}
