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
use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL};
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
    require_worker_owns_color(&state, &caller, color, &req.pod_name).await?;
    state
        .journal
        .record_event(&req.event, Some(req.pod_name.as_str()))
        .await
        .map_err(internal)?;
    Ok(Json(JournalRecordResponse {}))
}

/// The gate every write a worker makes ABOUT a color passes: the color
/// is in the caller's scope, the caller is the pod it claims to be, and
/// that pod is the color's current owner. Returns the color's scope
/// (tenant + project) so the handler can act inside it.
///
/// Pod-name binding: the caller can only act under its own bound pod.
/// Without this check, a worker could stamp a sibling's pod_name and
/// either bypass fencing (if the sibling is alive) or poison
/// attribution. The kubelet stamps `caller.pod_name` into the projected
/// SA token; it's unforgeable from inside the pod.
///
/// Cross-color sabotage gate: the color's owning pod (stamped at first
/// task_claim_one) must match the caller's bound pod. A compromised
/// tenant pod can act only on colors it legitimately owns, not
/// arbitrary sibling colors in the same tenant. `owner_pod_name IS
/// NULL` means the color has not been claimed yet (e.g. a
/// dispatcher-orchestrated phase still in flight); workers shouldn't be
/// writing in that state anyway, so we refuse.
async fn require_worker_owns_color(
    state: &BrokerState,
    caller: &CallerIdentity,
    color: weft_core::Color,
    claimed_pod: &str,
) -> Result<scope::ProjectScope, (StatusCode, String)> {
    let color_scope =
        scope::require_color_scope(&state.scope_cache, &state.pool, caller, &color.to_string())
            .await?;
    require_pod_name_matches(caller, claimed_pod)?;
    let owner: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT owner_pod_name FROM execution_color WHERE color = $1",
    )
    .bind(color.to_string())
    .fetch_optional(&state.pool)
    .await
    .map_err(internal)?;
    let owner_pod = owner.and_then(|(p,)| p).ok_or((
        StatusCode::FORBIDDEN,
        "color has no owning pod yet; worker may not act on it".into(),
    ))?;
    if owner_pod != claimed_pod {
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_pod = %claimed_pod,
            color = %color,
            owner_pod = %owner_pod,
            "broker rejected cross-color worker write"
        );
        return Err((
            StatusCode::FORBIDDEN,
            "color owned by a different worker pod".into(),
        ));
    }
    Ok(color_scope)
}

// ---------- Execution steering ----------

/// `ctx.tag_execution`: journal `ExecutionTagged` and write the
/// `execution_tag` rows in ONE transaction, synchronously, so the tag
/// rows exist by the time the node's call returns (a following
/// `stop_tagged` anchors on them). Same gate as `journal_record`: the
/// worker may only tag the color it owns.
pub async fn execution_tag(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ExecutionTagRequest>,
) -> Resp<ExecutionTagResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers tag executions".into()));
    }
    let color: weft_core::Color = req
        .color
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad color: {e}")))?;
    if req.tags.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "tag_execution needs at least one tag".into()));
    }
    // The ctx validated already; the broker trusts no pod, so again.
    weft_core::tag::validate_tags(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    require_worker_owns_color(&state, &caller, color, &req.pod_name).await?;
    let at_unix = unix_now_secs();
    let mut tx = state.pool.begin().await.map_err(internal)?;
    weft_journal::tags::tag_execution_in(&mut tx, color, &req.tags, at_unix, Some(&req.pod_name))
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(Json(ExecutionTagResponse {}))
}

/// `ctx.stop_tagged`: queue a `stop_tagged` task for the dispatcher,
/// with the ordering anchor resolved NOW. The project the stop runs in
/// is the asking color's own (from its `execution_color` row); the
/// request never names a project, so a stop cannot cross one.
///
/// The anchor rule, THE place it is decided:
///   - `Keep`: the asker's own `seq` for the tag, or, if it never
///     carried the tag, one past the newest row anywhere. Only rows
///     below it are stopped, so a sibling that tags itself after this
///     instant is out of reach however late the dispatcher runs the
///     task, and two concurrent "stop the others" calls leave the
///     later one alive.
///   - `Include`: no anchor; every live run carrying the tag, the
///     asker too.
pub async fn execution_stop_tagged(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ExecutionStopTaggedRequest>,
) -> Resp<ExecutionStopTaggedResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers stop executions by tag".into()));
    }
    let color: weft_core::Color = req
        .color
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad color: {e}")))?;
    weft_core::tag::validate_tag(&req.tag)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let color_scope = require_worker_owns_color(&state, &caller, color, &req.pod_name).await?;
    let own_seq = weft_journal::tags::tag_seq(&state.pool, color, &req.tag)
        .await
        .map_err(internal)?;
    let before_seq = match req.stop_self {
        weft_core::StopSelf::Include => None,
        weft_core::StopSelf::Keep => Some(match own_seq {
            Some(own) => own,
            None => weft_journal::tags::max_tag_seq(&state.pool).await.map_err(internal)? + 1,
        }),
    };
    // Whether the asker is among the runs this stop reaches, answered by
    // the same read and the same rule the dispatcher applies to carry
    // it out: a run the dispatcher would never cancel (a node test, which
    // `live_tagged_executions` leaves out) must not be told to wait for
    // its own cancel.
    let stops_asker = match req.stop_self {
        weft_core::StopSelf::Keep => false,
        weft_core::StopSelf::Include => {
            let live =
                weft_journal::tags::live_tagged_executions(&state.pool, color_scope.project, &req.tag)
                    .await
                    .map_err(internal)?;
            weft_journal::tags::select_stop_targets(&live, color, before_seq, req.stop_self)
                .contains(&color)
        }
    };
    let payload = weft_task_store::StopTaggedPayload {
        project_id: color_scope.project,
        tag: req.tag,
        by: color.to_string(),
        before_seq,
        stop_self: req.stop_self,
    };
    // Every ask is its own task: a second stop for the same tag from
    // the same run is a new decision with a new anchor, never a
    // duplicate to collapse, so the dedup key is fresh per call.
    let task = weft_task_store::tasks::NewTask {
        kind: TaskKind::StopTagged.into(),
        target: TaskTarget::Dispatcher,
        project_id: Some(color_scope.project),
        dedup_key: Some(format!("stop_tagged:{}", uuid::Uuid::new_v4())),
        color: Some(color.to_string()),
        tenant_id: Some(color_scope.tenant),
        target_pod_name: None,
        binary_hash: None,
        payload: serde_json::to_value(&payload).map_err(internal)?,
    };
    state.tasks.enqueue_dedup(task).await.map_err(internal)?;
    Ok(Json(ExecutionStopTaggedResponse { stops_asker }))
}

/// Seconds since the unix epoch, the stamp every broker-side write
/// puts on a journal row.
fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

/// How long a held request may actually be held: what the pod asked
/// for, never more than `MAX_HOLD` (a pod that wants longer asks again).
fn held(wait_ms: u64) -> Duration {
    Duration::from_millis(wait_ms).min(weft_task_store::pg_signal::MAX_HOLD)
}

/// The rows of one execution after the last one the worker applied,
/// held open until one lands (woken by the row's own notification) or
/// the hold ends.
pub async fn journal_wait(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalWaitRequest>,
) -> Resp<JournalWaitResponse> {
    scope::require_color_scope(&state.scope_cache, &state.pool, &caller, &req.color).await?;
    let color: weft_core::Color = req
        .color
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad color: {e}")))?;
    // RAW rows, never decode-and-re-encode: the broker only ferries
    // these, and a typed hop would silently strip any event field this
    // build predates. The worker decodes them, loudly.
    let rows = state
        .journal
        .raw_rows_after(color, req.after_id, held(req.wait_ms))
        .await
        .map_err(unavailable_or_internal)?;
    Ok(Json(JournalWaitResponse { rows }))
}

pub async fn journal_has_terminal(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalHasTerminalRequest>,
) -> Resp<JournalHasTerminalResponse> {
    scope::require_color_scope(&state.scope_cache, &state.pool, &caller, &req.color).await?;
    let color: weft_core::Color = req
        .color
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad color: {e}")))?;
    let terminal = state.journal.has_terminal_event(color).await.map_err(unavailable_or_internal)?;
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
            // to handle: register a wake signal, give birth to the
            // execution a live caller arrived for, provision infra,
            // and durable side-effect records (cost + log) that must
            // survive the worker pod dying.
            if ![
                TaskKind::RegisterSignal.as_str(),
                TaskKind::LiveArrival.as_str(),
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
            //     MEASUREMENT. Only the runtime's own billing path may
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
    // those (the dispatcher emits cancels itself), so any
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
    if let Some(project_id) = req.spec.project_id {
        let t =
            scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, project_id)
                .await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
    }
    if let Some(color) = req.spec.color.as_deref() {
        let scope =
            scope::require_color_scope(&state.scope_cache, &state.pool, &caller, color).await?;
        merge_anchor_tenant(&mut anchor_tenant, scope.tenant)?;
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
        .wait_for_terminal(req.task_id, held(req.wait_ms))
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
            *project_id,
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
        .claim_one(&req.pod_id, filter, held(req.wait_ms))
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

pub async fn task_requeue(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskRequeueRequest>,
) -> Resp<TaskRequeueResponse> {
    require_worker(&caller)?;
    require_pod_name_matches(&caller, &req.pod_id)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    let requeued = state
        .tasks
        .requeue(req.task_id, &req.pod_id)
        .await
        .map_err(internal)?;
    Ok(Json(TaskRequeueResponse { requeued }))
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    state
        .worker_pods
        .register_alive(&req.pod_name, req.project_id)
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
    let standing = state
        .worker_pods
        .heartbeat(&req.pod_name, req.mem_pressure)
        .await
        .map_err(internal)?;
    Ok(Json(WorkerPodHeartbeatResponse {
        renewed: standing.is_some(),
        draining: standing.is_some_and(|s| s.draining),
    }))
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let address = state
        .infra
        .endpoint_address(req.project_id, &req.node_id, &req.endpoint_name)
        .await
        .map_err(internal)?;
    Ok(Json(InfraEndpointUrlResponse { address }))
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT project_json FROM project_definition \
         WHERE project_id = $1 AND definition_hash = $2",
    )
    .bind(req.project_id)
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

// ---------- Connections ----------

/// The worker-caller prologue every connection verb shares: WHOSE
/// execution this caller is acting for. The caller must be a worker,
/// and the colour it names must be one it may act for.
///
/// The ownership rule itself lives in `require_color_scope`, which
/// every colour-named verb goes through, so this adds only the
/// role gate: connections are a worker's business and nobody else's.
async fn worker_execution_scope(
    state: &BrokerState,
    caller: &crate::auth::CallerIdentity,
    color: &str,
) -> Result<scope::ProjectScope, (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    scope::require_color_scope(&state.scope_cache, &state.pool, caller, color).await
}

/// Worker resolves a connection for one firing. The store fetches the
/// row (tenant wall, lazy single-flight refresh, required-permission
/// drift backstop) and answers EXACTLY the stored values the service's
/// auth steps interpolate; refresh tokens and app secrets never leave
/// the store side. For a row whose credential the RUNTIME supplies,
/// the credential source answers instead: it decides whether THIS node
/// may use the runtime's credential and hands back what to
/// authenticate with, plus (when it relays) where calls on it go.
///
/// The node declares how long its provider work may take
/// (`expected_duration_secs`) and the runtime does not second-guess it: a
/// legitimately long action (a multi-day agent, a slow batch job) says so and
/// gets a window that long. There is no ceiling here.
pub async fn resolve_connection(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ResolveConnectionRequest>,
) -> Resp<ResolveConnectionResponse> {
    let owner = worker_execution_scope(&state, &caller, &req.color).await?;
    let tenant = owner.tenant.clone();
    let connection_id: uuid::Uuid = req.connection_id.parse().map_err(|_| {
        (StatusCode::BAD_REQUEST, format!("malformed connection id '{}'", req.connection_id))
    })?;
    let resolved = weft_access_store::resolve_for_worker(
        &state.pool,
        &tenant,
        connection_id,
        &req.service,
        &req.required_permissions,
        &req.required_values,
    )
    .await
    .map_err(|e| match e.downcast_ref::<weft_access_store::AccessError>() {
        Some(weft_access_store::AccessError::NotFound) => (
            StatusCode::NOT_FOUND,
            "this connection does not exist here; pick one on the access node".into(),
        ),
        Some(weft_access_store::AccessError::Invalid(_))
        | Some(weft_access_store::AccessError::NeedsReconnect { .. }) => {
            (StatusCode::CONFLICT, format!("{e}"))
        }
        None => internal(e),
    })?;

    let response = match resolved.owner {
        weft_core::CredentialOwner::TheirOwn => ResolveConnectionResponse {
            values: resolved.values,
            auth: resolved.auth,
            identity: resolved.identity,
            relay_url: None,
            owner: resolved.owner,
        },
        weft_core::CredentialOwner::Ours => {
            // The row's service (already matched against the request)
            // keys the credential source; nothing here trusts a
            // caller-supplied name for anything but that equality.
            let name = crate::credential::single_value_name(&resolved.auth, &resolved.service)
                .map_err(internal)?;
            let key_req = crate::credential::KeyRequest {
                tenant,
                color: req.color.clone(),
                project_id: owner.project,
                node_id: req.node_id,
                frames: req.frames,
                node_type: req.node_type,
                service: resolved.service.clone(),
                auth: resolved.auth.clone(),
                pod_name: caller.pod_name.clone(),
                window: std::time::Duration::from_secs(req.expected_duration_secs),
            };
            match state.credentials.resolve(&state.pool, &key_req).await.map_err(internal)? {
                crate::credential::KeyResolution::Access { credential, relay_url } => {
                    let mut values = std::collections::BTreeMap::new();
                    values.insert(name.clone(), credential);
                    ResolveConnectionResponse {
                        values,
                        auth: resolved.auth,
                        identity: resolved.identity,
                        relay_url,
                        owner: resolved.owner,
                    }
                }
                crate::credential::KeyResolution::NotConfigured => {
                    return Err((
                        StatusCode::PRECONDITION_FAILED,
                        format!(
                            "no credential is configured for '{}'; connect your own on the \
                             access node",
                            resolved.service
                        ),
                    ))
                }
                crate::credential::KeyResolution::Denied { reason } => {
                    return Err((StatusCode::FORBIDDEN, reason))
                }
            }
        }
    };
    Ok(Json(response))
}

/// The firing that resolved a connection finished: give the lease back
/// NOW, rather than leaving a runtime-supplied credential usable to
/// its window (the crash backstop). Serves RUNTIME-OWNED releases:
/// the engine calls it only for an `Ours`-owned connection (a
/// runtime-supplied credential is the only thing there is to retire;
/// a user's own stored values never travel back). Closing is
/// idempotent, and a value the source never supplied is simply not
/// found. The color scope check keeps a worker from retiring another
/// tenant's credentials.
pub async fn release_connection(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ReleaseConnectionRequest>,
) -> Resp<ReleaseConnectionResponse> {
    let tenant = worker_execution_scope(&state, &caller, &req.color).await?.tenant;
    for value in req.values.values() {
        state.credentials.close(&state.pool, value, &tenant).await.map_err(internal)?;
    }
    Ok(Json(ReleaseConnectionResponse {}))
}

/// A node publishes a connection to something it runs itself. The
/// store keys the row on (tenant, project, node, service), so a
/// second publish updates the first row rather than piling up.
///
/// Nothing the caller says about WHERE the row lands is trusted: the
/// project comes from the execution's own row, so a worker cannot
/// publish into a sibling project (or into a project that does not
/// exist, which would leave a row the cleanup path can never reach).
/// The row is always the user's own credential, and the store refuses
/// any recipe shape that could later make weft call out on the
/// tenant's behalf.
pub async fn publish_access(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<PublishAccessRequest>,
) -> Resp<PublishAccessResponse> {
    let owner = publisher_scope(&state, &caller, &req.color, &mut req.node_id).await?;
    if req.spec.service != req.service {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "publishing '{}' with the recipe for '{}'",
                req.service, req.spec.service
            ),
        ));
    }
    let done = weft_access_store::publish_grant(
        &state.pool,
        &owner.tenant,
        weft_access_store::PublishAccess {
            spec: req.spec,
            project_id: owner.project,
            node_id: req.node_id,
            values: req.values,
            label: req.label,
        },
    )
    .await
    .map_err(store_err)?;
    Ok(Json(PublishAccessResponse {
        connection: weft_core::access::wire::PublishedConnection {
            connection_id: done.grant.id.to_string(),
            identity: done.grant.identity,
        },
    }))
}

/// The connection this node published for this service, if any. How a
/// node finds what it opened last time instead of asking the thing it
/// runs for its credentials a second time.
pub async fn published_access(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<PublishedAccessRequest>,
) -> Resp<PublishedAccessResponse> {
    let owner = publisher_scope(&state, &caller, &req.color, &mut req.node_id).await?;
    let found = weft_access_store::published_connection(
        &state.pool,
        &owner.tenant,
        owner.project,
        &req.node_id,
        &req.service,
    )
    .await
    .map_err(store_err)?;
    Ok(Json(match found {
        Some(connection) => PublishedAccessResponse::Published { connection },
        None => PublishedAccessResponse::NothingPublished,
    }))
}

/// Where a publish is allowed to land: the execution's own tenant and
/// project. `node_id` is trimmed in place, so the caller's own field
/// is the key the row is written under.
///
/// Authorization first, then the shape of the request: a caller with
/// no business here should hear that, not a complaint about its
/// arguments.
async fn publisher_scope(
    state: &BrokerState,
    caller: &crate::auth::CallerIdentity,
    color: &str,
    node_id: &mut String,
) -> Result<scope::ProjectScope, (StatusCode, String)> {
    let owner = worker_execution_scope(state, caller, color).await?;
    require_node_id(node_id)?;
    Ok(owner)
}

// ---------- Supervisor surface ----------

pub async fn supervisor_sync_ownership(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorSyncOwnershipRequest>,
) -> Resp<SupervisorSyncOwnershipResponse> {
    require_supervisor(&caller)?;
    let synced = crate::lifecycle_writes::sync_ownership(&state.pool, &req.pod_name, req.mem_pressure)
        .await
        .map_err(internal)?;
    Ok(Json(synced))
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
    let owned = crate::lifecycle_writes::owned_projects(&state.pool, &req.pod_name)
        .await
        .map_err(internal)?;
    Ok(Json(SupervisorOwnedProjectsResponse { owned }))
}

pub async fn supervisor_infra_nodes(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorInfraNodesRequest>,
) -> Resp<SupervisorInfraNodesResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT node_id, instance_id, status, applied_spec_hash, \
                endpoints_json, public_paths_json, preserve_pvcs_json, units_json \
         FROM infra_node WHERE project_id = $1",
    )
    .bind(req.project_id)
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
        let public_paths_json: serde_json::Value = r
            .try_get("public_paths_json")
            .map_err(|e| internal(anyhow::anyhow!("decode public_paths_json: {e}")))?;
        let public_paths: std::collections::BTreeMap<String, String> =
            serde_json::from_value(public_paths_json).map_err(|e| {
                internal(anyhow::anyhow!(
                    "infra_node.public_paths_json for node='{node_id}' is not a string-to-string map: {e}"
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
        let units = weft_broker_client::protocol::decode_units_json(
            units_json,
            req.project_id,
            &node_id,
        )
        .map_err(internal)?;
        nodes.push(SupervisorInfraNode {
            node_id,
            instance_id,
            status,
            applied_spec_hash,
            addresses: weft_broker_client::protocol::AppliedEndpoints { urls: endpoints, public_paths },
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let id = req.project_id;
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
) -> Resp<SupervisorClaim> {
    require_supervisor(&caller)?;
    // Which command is next, and why that answer is safe without a row
    // lock: `lifecycle_writes::next_command`.
    //
    // Held: with nothing waiting, the request sleeps until a command is
    // issued anywhere (the row's own trigger announces it) and looks
    // again. Any issue wakes it, not only one for a project this pod
    // owns: ownership can move during the hold, and the look is one
    // indexed read.
    //
    // A wake that finds nothing for this pod may be a command for a
    // project nobody owns yet (ownership is taken on the supervisors'
    // own ticks): the answer then says so, and the pod takes ownership
    // at once instead of on its next tick. Only after a wake, and only to
    // a pod that takes on projects (registered, not draining, below
    // saturation: `lifecycle_writes::takes_on_projects`, the rule its
    // ownership tick claims by); any other pod would tick, claim
    // nothing, and come straight back, so it holds as usual.
    let deadline = tokio::time::Instant::now() + held(req.wait_ms);
    let mut heard = state.signals.subscribe();
    let mut woken_once = false;
    loop {
        let next = crate::lifecycle_writes::next_command(
            &state.pool,
            &req.claimer_pod,
            &req.busy_projects,
        )
        .await
        .map_err(internal)?;
        if let Some(command) = next {
            return Ok(Json(SupervisorClaim::Command(command)));
        }
        if woken_once
            && crate::lifecycle_writes::unowned_work_waiting(&state.pool)
                .await
                .map_err(internal)?
            && crate::lifecycle_writes::pod_takes_on_projects(&state.pool, &req.claimer_pod)
                .await
                .map_err(internal)?
        {
            return Ok(Json(SupervisorClaim::UnownedWork));
        }
        let woken = heard
            .woken_before(deadline, |channel, payload| {
                channel == INFRA_COMMAND_CHANNEL
                    && matches!(InfraCommandSignal::parse(payload), Some(InfraCommandSignal::Issued { .. }))
            })
            .await
            .map_err(internal)?;
        if !woken {
            return Ok(Json(SupervisorClaim::Nothing));
        }
        woken_once = true;
    }
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
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
            .await?;
    let id = crate::lifecycle_writes::record_event(
        &state.pool,
        &project_tenant,
        req.project_id,
        req.node_id.as_deref(),
        req.kind.as_str(),
        &req.payload,
    )
    .await
    .map_err(internal)?
    .ok_or_else(|| project_gone(req.project_id))?;
    // The dispatcher's bridge is woken by the row's own trigger
    // (`infra_event::GROUP`), when this insert commits.
    Ok(Json(SupervisorEventRecordResponse { id }))
}

/// Map a fenced lifecycle write's outcome to the wire: Applied is the
/// 2xx body, Displaced is 410, Gone is 409. The supervisor client
/// reads exactly these two codes back into `WriteOutcome`.
// SYNC: the two status codes <-> crates/weft-broker-client/src/client.rs
//       (post_fenced, the one reader of them)
fn fenced_to_http(
    outcome: crate::lifecycle_writes::FencedWrite,
    what: impl FnOnce() -> String,
) -> Result<(), (StatusCode, String)> {
    use crate::lifecycle_writes::FencedWrite;
    match outcome {
        FencedWrite::Applied => Ok(()),
        FencedWrite::Displaced => Err((StatusCode::GONE, format!("{}: project ownership moved", what()))),
        FencedWrite::Gone => Err((StatusCode::CONFLICT, format!("{}: target gone", what()))),
    }
}

pub async fn supervisor_set_status(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<SupervisorSetStatusRequest>,
) -> Resp<SupervisorSetStatusResponse> {
    require_supervisor(&caller)?;
    require_node_id(&mut req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    // The write, its fence and its stale answers live in
    // `lifecycle_writes::set_status` (pool-level, db-tested); this is
    // the scope-checked HTTP wrapper. The pod identity is the
    // supervisor's claim id (`req.pod_name` = WEFT_POD_NAME, what keys
    // `infra_owner`), NOT the auth token's Pod name (which carries a
    // ReplicaSet suffix and would never match the lease).
    let outcome = crate::lifecycle_writes::set_status(&state.pool, &req)
        .await
        .map_err(internal)?;
    fenced_to_http(outcome, || {
        format!(
            "set_status(project={}, node={}, unit={:?}, cmd={:?})",
            req.project_id, req.node_id, req.unit, req.command_id
        )
    })?;
    Ok(Json(SupervisorSetStatusResponse {}))
}

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
    public_paths_json: serde_json::Value,
}

async fn write_apply_row(
    state: &BrokerState,
    op: &str,
    project_id: uuid::Uuid,
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
          applied_at_unix, endpoints_json, public_paths_json, preserve_pvcs_json, units_json) \
         SELECT $1, $2, $3, $4, $5, NULL, NULL, $6, \
                CASE WHEN $7 THEN EXTRACT(EPOCH FROM NOW())::BIGINT ELSE NULL END, \
                $8, $13, $9, $10 \
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
            public_paths_json  = EXCLUDED.public_paths_json, \
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
    .bind(row.public_paths_json)
    .execute(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    if res.rows_affected() == 0 {
        let outcome = crate::lifecycle_writes::stale_answer(&state.pool, owner_pod, project_id)
            .await
            .map_err(internal)?;
        fenced_to_http(outcome, || format!("{op}(command id={command_id})"))?;
    }
    Ok(())
}

pub async fn supervisor_set_applied(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<SupervisorSetAppliedRequest>,
) -> Resp<SupervisorSetAppliedResponse> {
    require_supervisor(&caller)?;
    require_node_id(&mut req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let endpoints_json = serde_json::to_value(&req.addresses.urls)
        .map_err(|e| internal(anyhow::anyhow!("endpoints serialize: {e}")))?;
    let public_paths_json = serde_json::to_value(&req.addresses.public_paths)
        .map_err(|e| internal(anyhow::anyhow!("public_paths serialize: {e}")))?;
    let units_json = serde_json::to_value(&req.units)
        .map_err(|e| internal(anyhow::anyhow!("units serialize: {e}")))?;
    write_apply_row(
        &state,
        "set_applied",
        req.project_id,
        &req.node_id,
        &req.instance_id,
        &req.namespace,
        &req.preserve_pvcs,
        units_json,
        req.command_id,
        &req.pod_name,
        ApplyRowState {
            // Flaky if a frozen unit still is, Running otherwise: the
            // node status must agree with the roster it is written with
            // (the health reconcile only rewrites on per-unit drift, so
            // a flat Running here would stand until the next edge).
            status: weft_broker_client::protocol::InfraNodeStatus::applied_rollup(
                req.units.values().map(|u| &u.status),
            )
            .as_str(),
            applied_spec_hash: Some(req.applied_spec_hash.clone()),
            stamp_applied_at: true,
            endpoints_json,
            public_paths_json,
        },
    )
    .await?;
    Ok(Json(SupervisorSetAppliedResponse {}))
}

/// Supervisor-callable: write the `infra_node` row at `Provisioning`
/// before the apply begins. Locks in the (instance_id, namespace,
/// preserve_pvcs) tuple so that a partial-apply leaves a visible row
/// the user can Terminate. On apply success, `set_applied` flips to
/// `Running` and fills endpoints + applied_spec_hash. Same ownership
/// guard as `set_applied`: the caller must still own the command's
/// claim.
pub async fn supervisor_set_provisioning(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<SupervisorSetProvisioningRequest>,
) -> Resp<SupervisorSetProvisioningResponse> {
    require_supervisor(&caller)?;
    require_node_id(&mut req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let units_json = serde_json::to_value(&req.units)
        .map_err(|e| internal(anyhow::anyhow!("units serialize: {e}")))?;
    write_apply_row(
        &state,
        "set_provisioning",
        req.project_id,
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
            public_paths_json: serde_json::json!({}),
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
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
            .await?;
    // Verify the typed spec's (mode, policy) combo is coherent
    // before persisting. The rule lives next to `DeactivateSpec`
    // in `weft_core::running_policy` so every caller (this
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
    let issued_by_pod = caller.pod_name.as_deref().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            "supervisor token missing pod claim".to_string(),
        )
    })?;
    let command_id = crate::lifecycle_writes::issue_command(
        &state.pool,
        &crate::lifecycle_writes::IssuedCommand {
            tenant_id: &project_tenant,
            project_id: req.project_id,
            node_id: None,
            verb,
            running_policy,
            spec_json: spec_json.as_ref(),
            issued_by_pod,
        },
    )
    .await
    .map_err(internal)?
    .ok_or_else(|| project_gone(req.project_id))?;
    // The dispatcher's `lifecycle_claimer` is woken by the row's own
    // trigger (`infra_lifecycle_command::GROUP`), when this insert
    // commits.
    Ok(Json(SupervisorEnqueueLifecycleResponse { command_id }))
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let id = req.project_id;
    use sqlx::Row;
    let row = sqlx::query("SELECT infra_image_tags_json FROM project WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await
        .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    // No row = project doesn't exist; broker returns empty tags
    // (the supervisor's caller will surface "MissingLocalImage"
    // downstream against a clear context). Row exists but decode
    // fails = schema drift; fail loud through the canonical decode
    // rather than coerce to empty (which would mask the real cause).
    // A node missing from the decoded map is a legal empty (the
    // project's map exists but this node hasn't been written).
    let tags: std::collections::HashMap<String, String> = match row {
        None => std::collections::HashMap::new(),
        Some(r) => {
            let value: serde_json::Value = r
                .try_get("infra_image_tags_json")
                .map_err(|e| internal(anyhow::anyhow!("decode infra_image_tags_json: {e}")))?;
            weft_broker_client::protocol::decode_infra_image_tags(
                value,
                &format!("project={}", req.project_id),
            )
            .map_err(internal)?
            .get(&req.node_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect()
        }
    };
    Ok(Json(SupervisorProjectImageTagsResponse { tags }))
}

/// Worker-callable: enqueue an Apply lifecycle command after the
/// engine's local skip/fresh/replace decision. The owning supervisor's
/// held claim wakes on the row's own notification and picks it up.
pub async fn infra_enqueue_apply(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<InfraEnqueueApplyRequest>,
) -> Resp<InfraEnqueueApplyResponse> {
    require_node_id(&mut req.node_id)?;
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    // Bind the project's tenant on the row (equals the worker's tenant,
    // since the ownership check passed). Uniform with the supervisor
    // enqueue paths: the row tenant always comes from the resource.
    let project_tenant =
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
            .await?;
    // Deduplicated against an in-flight apply for the same (project,
    // node): `lifecycle_writes::issue_command`.
    let issued_by_pod = caller.pod_name.as_deref().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            "worker token missing pod claim".to_string(),
        )
    })?;
    // Apply doesn't carry a running_policy (no in-flight executions
    // to drain; the supervisor just applies).
    let command_id = crate::lifecycle_writes::issue_command(
        &state.pool,
        &crate::lifecycle_writes::IssuedCommand {
            tenant_id: &project_tenant,
            project_id: req.project_id,
            node_id: Some(&req.node_id),
            verb: weft_broker_client::protocol::InfraLifecycleVerb::Apply,
            running_policy: None,
            spec_json: Some(&req.spec_json),
            issued_by_pod,
        },
    )
    .await
    .map_err(internal)?
    .ok_or_else(|| project_gone(req.project_id))?;
    Ok(Json(InfraEnqueueApplyResponse { command_id }))
}

/// Worker-callable: a previously-issued apply command's state, held
/// open until it completes (woken by the row's own notification) or the
/// hold ends. The worker asks again until it completes.
pub async fn infra_wait_apply(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<InfraWaitApplyRequest>,
) -> Resp<InfraWaitApplyResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let deadline = tokio::time::Instant::now() + held(req.wait_ms);
    let mut heard = state.signals.subscribe();
    loop {
        let answer = apply_command_state(&state, &req).await?;
        if answer.completed {
            return Ok(Json(answer));
        }
        let woken = heard
            .woken_before(deadline, |channel, payload| {
                channel == INFRA_COMMAND_CHANNEL
                    && InfraCommandSignal::parse(payload) == Some(InfraCommandSignal::Done { id: req.command_id })
            })
            .await
            .map_err(internal)?;
        if !woken {
            return Ok(Json(answer));
        }
    }
}

/// One read of an apply command's row, as the wire answers it.
async fn apply_command_state(
    state: &BrokerState,
    req: &InfraWaitApplyRequest,
) -> Result<InfraWaitApplyResponse, (StatusCode, String)> {
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
    let cmd_project: uuid::Uuid = r
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
    Ok(InfraWaitApplyResponse {
        completed: done.is_some(),
        outcome,
        outcome_message: message,
    })
}

pub async fn supervisor_remove_node(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<SupervisorRemoveNodeRequest>,
) -> Resp<SupervisorRemoveNodeResponse> {
    require_supervisor(&caller)?;
    require_node_id(&mut req.node_id)?;
    let tenant =
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
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
    .bind(req.project_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("remove_node ownership check: {e}")))?;
    if !owns {
        // 410 = displaced (see `fenced_to_http`; here the ownership check
        // is explicit, so no second lookup). A row that is already gone
        // is NOT stale for this write: the DELETE below simply removes
        // nothing and reports `removed: false`.
        return Err((
            StatusCode::GONE,
            format!(
                "remove_node: {} no longer owns project {}",
                req.pod_name, req.project_id
            ),
        ));
    }
    let res = sqlx::query("DELETE FROM infra_node WHERE project_id = $1 AND node_id = $2")
        .bind(req.project_id)
        .bind(&req.node_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(anyhow::anyhow!("delete infra_node: {e}")))?;
    sqlx::query(
        "DELETE FROM infra_event WHERE project_id = $1 AND node_id = $2",
    )
    .bind(req.project_id)
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
    .bind(req.project_id)
    .bind(&req.node_id)
    .bind(LifecycleOutcome::Cancelled.as_str())
    .execute(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("cancel pending commands: {e}")))?;
    // A connection this node published opens the very thing being
    // removed, so it goes with it: its credentials would otherwise
    // name a database that no longer exists, and nothing downstream
    // could use or clean it.
    let published = weft_access_store::delete_published_grants(
        &mut *tx,
        &tenant,
        req.project_id,
        Some(&req.node_id),
    )
    .await
    .map_err(|e| internal(anyhow::anyhow!("delete published connections: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| internal(anyhow::anyhow!("commit: {e}")))?;
    // Logged AFTER the commit. A line saying rows were removed, on a
    // transaction that then failed to commit, is a log that lies.
    if published > 0 {
        tracing::info!(
            target: "weft_broker",
            project_id = %req.project_id,
            node_id = %req.node_id,
            published,
            "removed the connections this node published"
        );
    }
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let id = req.project_id;
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
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
           AND status IN ('spawning', 'alive') \
           AND role = 'worker'",
    )
    .bind(req.project_id)
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    use sqlx::Row;
    let row = sqlx::query(
        "SELECT EXISTS ( \
           SELECT 1 FROM infra_lifecycle_command \
           WHERE project_id = $1 AND completed_at_unix IS NULL \
         ) AS in_flight",
    )
    .bind(req.project_id)
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
    // The terminal write, its ownership fence and its stale answers
    // live in `lifecycle_writes::complete_command` (pool-level,
    // db-tested). Ownership identity is the supervisor's claim id
    // (`req.pod_name` = WEFT_POD_NAME, what keys `infra_owner`), not
    // the auth token's Pod name (suffixed, never matches the lease).
    // Tenant scope was already re-checked above via the token.
    let outcome = crate::lifecycle_writes::complete_command(&state.pool, &req)
        .await
        .map_err(internal)?;
    fenced_to_http(outcome, || format!("command_complete(id={})", req.command_id))?;
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

/// Hold `node_id` to what a row may be keyed on, TRIMMING IT IN PLACE
/// so every later read of it is already the key.
///
/// In place, rather than handing a trimmed copy back, because a
/// caller can ignore a returned value and five of them did: they
/// validated the trimmed form and then persisted the padded one.
/// Persisting an empty key (or matching against one) corrupts the
/// per-node indexes, and persisting a padded one is worse, because
/// `"db"` and `" db"` are two keys for one node, so the credential a
/// node published under one spelling survives the cleanup that names
/// the other.
fn require_node_id(node_id: &mut String) -> Result<(), (StatusCode, String)> {
    let trimmed = node_id.trim();
    if trimmed.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "node_id required".into()));
    }
    if trimmed.len() != node_id.len() {
        *node_id = trimmed.to_string();
    }
    Ok(())
}

/// The answer to an insert that found its project row gone
/// (`lifecycle_writes::issue_command` / `record_event` read it live,
/// since the caller's authorization can outlive the removal).
fn project_gone(project_id: uuid::Uuid) -> (StatusCode, String) {
    (
        StatusCode::NOT_FOUND,
        format!("project {project_id} no longer exists; nothing was recorded for it"),
    )
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
    // pod holds, and each returned row carries its own tenant. Which
    // placed rows belong in a registry is `signals_held_by_pod`'s rule.
    let out = crate::signal_placement::signals_held_by_pod(&state.pool, &req.pod_name)
        .await
        .map_err(internal)?;
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
    let row: Option<(uuid::Uuid,)> = sqlx::query_as(
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
    scope::require_project_owned_by(&state.scope_cache, &state.pool, caller, project_id)
        .await
        .map(|_| ())
}

/// A store error as this surface answers it. The mapping itself lives
/// in the store (`client_error`), so the broker and the dispatcher
/// cannot drift on what a store failure looks like.
pub(crate) fn store_err(e: anyhow::Error) -> (StatusCode, String) {
    let (status, message) = weft_access_store::client_error(e);
    (StatusCode::from_u16(status).expect("store status codes are valid"), message)
}

/// A failure that is OURS, not the caller's: logged here with its
/// cause chain, and answered opaquely.
///
/// Opaque because the callers on this surface include workers, which
/// run tenant-authored code. A sqlx error's Display names tables and
/// columns, and echoing one hands the untrusted side a description of
/// our schema. The operator loses nothing: the log has strictly more
/// than the response ever did.
///
/// ONE definition for the whole broker, so no surface can be the one
/// that answers differently.
pub(crate) fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    tracing::error!(target: "weft_broker", "internal: {e:#}");
    (StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
}

/// A failed database call, as a caller that can wait must tell it
/// apart: 503 when the database could not be reached right now (asking
/// again will work once it can), 500 for anything else (a row that does
/// not decode fails the same way every time). The worker's journal
/// client waits out a 503 and fails the run on a 500.
// SYNC: 503 = ask again <-> crates/weft-broker-client/src/client.rs broker_unavailable
pub(crate) fn unavailable_or_internal(e: anyhow::Error) -> (StatusCode, String) {
    if e.chain().filter_map(|cause| cause.downcast_ref::<sqlx::Error>()).any(database_unreachable) {
        tracing::warn!(target: "weft_broker", "could not reach the database: {e:#}");
        return (StatusCode::SERVICE_UNAVAILABLE, "the database is unavailable; ask again".into());
    }
    internal(e)
}

/// Whether a database error means the server could not be reached or
/// is restarting, rather than that the statement itself failed: no
/// connection to be had, a connection that broke, or the server
/// refusing because it is shutting down or starting up (SQLSTATE class
/// 08, and 57P01 to 57P03).
fn database_unreachable(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) | sqlx::Error::Protocol(_) => {
            true
        }
        sqlx::Error::Database(db) => db
            .code()
            .is_some_and(|code| code.starts_with("08") || matches!(&*code, "57P01" | "57P02" | "57P03")),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_unreachable_database_asks_the_caller_again() {
        let pool_gone = anyhow::Error::from(sqlx::Error::PoolTimedOut).context("color lookup");
        assert_eq!(unavailable_or_internal(pool_gone).0, StatusCode::SERVICE_UNAVAILABLE);
        let bad_row = anyhow::Error::from(sqlx::Error::RowNotFound);
        assert_eq!(unavailable_or_internal(bad_row).0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(unavailable_or_internal(anyhow::anyhow!("undecodable row")).0, StatusCode::INTERNAL_SERVER_ERROR);
    }

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
