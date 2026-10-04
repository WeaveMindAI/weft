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
use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL, ISSUED_WAKE};
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

/// A worker's journal rows, all of one execution, written in one
/// statement that also fences them: they go in only while the asking
/// replica owns the execution's claim (see
/// [`require_worker_owns_execution_id`] for why). The owner is read in the
/// write itself, so the common case costs one round trip; only a refused
/// write reads it again, to say why.
pub async fn journal_record(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalRecordRequest>,
) -> Resp<JournalRecordResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers journal events".into()));
    }
    let Some(first) = req.events.first() else {
        return Ok(Json(JournalRecordResponse {}));
    };
    let execution_id = first.execution_id();
    require_worker_execution_scope(&state, &caller, execution_id, &req.replica).await?;
    let written = weft_journal::record_events(&state.pool, &req.events, Some(&req.replica), Some(&req.replica))
        .await
        .map_err(|e| match e {
            weft_journal::RecordError::MixedExecutions { .. } => (StatusCode::BAD_REQUEST, e.to_string()),
            other => internal(other),
        })?;
    if written == 0 {
        require_owner(&state, &caller, execution_id, &req.replica).await?;
        // The owner read again says the replica owns it: it took the
        // claim between the write and this read. The rows did not go in.
        return Err((
            StatusCode::CONFLICT,
            "the execution's claim changed hands during the write; nothing was journaled".into(),
        ));
    }
    Ok(Json(JournalRecordResponse {}))
}

/// A failed unrecorded run's whole record, written at once: the run
/// becomes a recorded run (`weft_journal::unrecorded`). Same gate as
/// `journal_record`: the worker may only write the execution it owns.
pub async fn journal_record_retroactive(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalRecordRetroactiveRequest>,
) -> Resp<JournalRecordResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers journal events".into()));
    }
    let Some(first) = req.events.first() else {
        return Err((StatusCode::BAD_REQUEST, "an unrecorded run's record has at least its birth".into()));
    };
    require_worker_owns_execution_id(&state, &caller, first.execution_id(), &req.replica).await?;
    state
        .journal
        .record_retroactively(&req.events, Some(req.replica.as_str()))
        .await
        .map_err(internal)?;
    Ok(Json(JournalRecordResponse {}))
}

/// An unrecorded run ended without failing: its execution row goes (unless
/// its costs keep it) and its un-kept run files start their linger, the
/// same sweep a recorded run's ending queues.
pub async fn journal_forget_unrecorded(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalForgetUnrecordedRequest>,
) -> Resp<JournalRecordResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers journal events".into()));
    }
    let execution_id: weft_core::ExecutionId =
        req.execution_id.parse().map_err(|e| (StatusCode::BAD_REQUEST, format!("bad execution: {e}")))?;
    let scope = require_worker_owns_execution_id(&state, &caller, execution_id, &req.replica).await?;
    // Files first: a failed sweep leaves the row, so the worker's error
    // names a run that is still there to look at.
    state.runtime_store.sweep_exec(&scope.tenant, &req.execution_id).await.map_err(internal)?;
    state
        .journal
        .forget_unrecorded(execution_id, Some(req.replica.as_str()))
        .await
        .map_err(internal)?;
    Ok(Json(JournalRecordResponse {}))
}

/// The gate every write a worker makes ABOUT an execution passes: the execution
/// is in the caller's scope, the caller is the replica it says it is,
/// and that replica is the execution's current owner. Returns the execution's scope
/// (tenant + project) so the handler can act inside it.
///
/// Replica binding: the caller can only act under the replica its
/// request names (`caller.replica`, from the replica header). Without
/// it, a worker could write under a sibling's replica id and slip past
/// the owner check or poison attribution. The replica is self-asserted;
/// the platform identity underneath already pins the caller to its
/// project, so this only orders writers inside one project.
///
/// Cross-execution sabotage gate: the execution's owning replica (stamped at
/// first task_claim_one) must match the caller's. A compromised worker
/// can act only on executions it legitimately owns, not
/// arbitrary sibling executions in the same tenant. `owner_replica IS
/// NULL` means the execution has not been claimed yet (e.g. a
/// dispatcher-orchestrated phase still in flight); workers shouldn't be
/// writing in that state anyway, so we refuse.
async fn require_worker_owns_execution_id(
    state: &BrokerState,
    caller: &CallerIdentity,
    execution_id: weft_core::ExecutionId,
    claimed_replica: &str,
) -> Result<scope::ExecutionScope, (StatusCode, String)> {
    let execution_id_scope = require_worker_execution_scope(state, caller, execution_id, claimed_replica).await?;
    require_owner(state, caller, execution_id, claimed_replica).await?;
    Ok(execution_id_scope)
}

/// The execution is in the caller's scope and the caller is the replica
/// it says it is: the half of [`require_worker_owns_execution_id`] that
/// needs no read of the execution's owner.
async fn require_worker_execution_scope(
    state: &BrokerState,
    caller: &CallerIdentity,
    execution_id: weft_core::ExecutionId,
    claimed_replica: &str,
) -> Result<scope::ExecutionScope, (StatusCode, String)> {
    let execution_id_scope =
        scope::require_execution_id_scope(&state.scope_cache, &state.pool, caller, &execution_id.to_string())
            .await?;
    require_replica_matches(caller, claimed_replica)?;
    Ok(execution_id_scope)
}

/// `claimed_replica` owns `execution_id`'s claim, read from the execution
/// row; the refusal names which way it does not.
async fn require_owner(
    state: &BrokerState,
    caller: &CallerIdentity,
    execution_id: weft_core::ExecutionId,
    claimed_replica: &str,
) -> Result<(), (StatusCode, String)> {
    let owner: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT owner_replica FROM execution WHERE execution_id = $1",
    )
    .bind(execution_id.to_string())
    .fetch_optional(&state.pool)
    .await
    .map_err(internal)?;
    let owner_replica = owner.and_then(|(p,)| p).ok_or((
        StatusCode::FORBIDDEN,
        "execution has no owning replica yet; worker may not act on it".into(),
    ))?;
    if owner_replica != claimed_replica {
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_replica = %claimed_replica,
            execution_id = %execution_id,
            owner_replica = %owner_replica,
            "broker rejected cross-execution worker write"
        );
        return Err((
            StatusCode::FORBIDDEN,
            "execution owned by a different worker replica".into(),
        ));
    }
    Ok(())
}

// ---------- Execution steering ----------

/// `ctx.tag_execution`: journal `ExecutionTagged` and write the
/// `execution_tag` rows in ONE transaction, synchronously, so the tag
/// rows exist by the time the node's call returns (a following
/// `stop_tagged` anchors on them). Same gate as `journal_record`: the
/// worker may only tag the execution it owns.
pub async fn execution_tag(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ExecutionTagRequest>,
) -> Resp<ExecutionTagResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers tag executions".into()));
    }
    let execution_id: weft_core::ExecutionId = req
        .execution_id
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad execution: {e}")))?;
    if req.tags.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "tag_execution needs at least one tag".into()));
    }
    // The ctx validated already; the broker trusts no worker, so again.
    weft_core::tag::validate_tags(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    require_worker_owns_execution_id(&state, &caller, execution_id, &req.replica).await?;
    let at_unix = unix_now_secs();
    let mut tx = state.pool.begin().await.map_err(internal)?;
    weft_journal::tags::tag_execution_in(&mut tx, execution_id, &req.tags, at_unix, Some(&req.replica))
        .await
        .map_err(internal)?;
    tx.commit().await.map_err(internal)?;
    Ok(Json(ExecutionTagResponse {}))
}

/// `ctx.stop_tagged`: queue a `stop_tagged` task for the dispatcher,
/// with the ordering anchor resolved NOW. The project the stop runs in
/// is the asking execution's own (from its `execution` row); the
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
    let execution_id: weft_core::ExecutionId = req
        .execution_id
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad execution: {e}")))?;
    weft_core::tag::validate_tag(&req.tag)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let execution_id_scope = require_worker_owns_execution_id(&state, &caller, execution_id, &req.replica).await?;
    let own_seq = weft_journal::tags::tag_seq(&state.pool, execution_id, &req.tag)
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
                weft_journal::tags::live_tagged_executions(&state.pool, execution_id_scope.project, &req.tag)
                    .await
                    .map_err(internal)?;
            weft_journal::tags::select_stop_targets(&live, execution_id, before_seq, req.stop_self)
                .contains(&execution_id)
        }
    };
    let payload = weft_task_store::StopTaggedPayload {
        project_id: execution_id_scope.project,
        tag: req.tag,
        by: execution_id.to_string(),
        before_seq,
        stop_self: req.stop_self,
    };
    // Every ask is its own task: a second stop for the same tag from
    // the same run is a new decision with a new anchor, never a
    // duplicate to collapse, so the dedup key is fresh per call.
    let task = weft_task_store::tasks::NewTask {
        kind: TaskKind::StopTagged.into(),
        target: TaskTarget::Dispatcher,
        project_id: Some(execution_id_scope.project),
        dedup_key: Some(format!("stop_tagged:{}", uuid::Uuid::new_v4())),
        execution_id: Some(execution_id.to_string()),
        tenant_id: execution_id_scope.tenant,
        target_replica: None,
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

/// How long a held request may actually be held: what the process asked
/// for, never more than `MAX_HOLD` (a process that wants longer asks again).
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
    scope::require_execution_id_scope(&state.scope_cache, &state.pool, &caller, &req.execution_id).await?;
    let execution_id: weft_core::ExecutionId = req
        .execution_id
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad execution: {e}")))?;
    // RAW rows, never decode-and-re-encode: the broker only ferries
    // these, and a typed hop would silently strip any event field this
    // build predates. The worker decodes them, loudly.
    let rows = state
        .journal
        .raw_rows_after(execution_id, req.after_id, held(req.wait_ms))
        .await
        .map_err(unavailable_or_internal)?;
    Ok(Json(JournalWaitResponse { rows }))
}

pub async fn journal_has_terminal(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<JournalHasTerminalRequest>,
) -> Resp<JournalHasTerminalResponse> {
    scope::require_execution_id_scope(&state.scope_cache, &state.pool, &caller, &req.execution_id).await?;
    let execution_id: weft_core::ExecutionId = req
        .execution_id
        .parse()
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("bad execution: {e}")))?;
    let terminal = state.journal.has_terminal_event(execution_id).await.map_err(unavailable_or_internal)?;
    Ok(Json(JournalHasTerminalResponse { terminal }))
}

// ---------- Tasks ----------

/// Fold a newly-resolved resource tenant into the task's anchor tenant,
/// enforcing that every named resource agrees. A task naming resources
/// in two different tenants (project in A, execution in B) is ambiguous and
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
            // survive the worker dying.
            if ![
                TaskKind::RegisterSignal.as_str(),
                TaskKind::RecordCost.as_str(),
                TaskKind::RecordLog.as_str(),
                TaskKind::ProgramCall.as_str(),
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
            "worker-enqueued tasks must target dispatcher".into(),
        ));
    }
    // `target_replica` is meaningful only for cancel-style tasks
    // claimed by a specific worker replica. Workers never enqueue
    // those (the dispatcher emits cancels itself), so any
    // wire-set value is either confused or hostile. Refuse to
    // persist a value the caller has no legitimate use for.
    if req.spec.target_replica.is_some() {
        return Err((
            StatusCode::FORBIDDEN,
            "workers may not set target_replica".into(),
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
    // execution C in tenant B) rather than silently picking one, so the
    // stamped tenant is never ambiguous.
    let mut anchor_tenant: Option<String> = None;
    if let Some(project_id) = req.spec.project_id {
        let t =
            scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, project_id)
                .await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
    }
    if let Some(execution_id) = req.spec.execution_id.as_deref() {
        let scope =
            scope::require_execution_id_scope(&state.scope_cache, &state.pool, &caller, execution_id).await?;
        merge_anchor_tenant(&mut anchor_tenant, scope.tenant)?;
    }
    if kind == TaskKind::FireSignal.as_str() {
        // Listener held-event fire: the signal token is the tenant
        // anchor. Pull it from the payload and resolve.
        let fire: weft_task_store::kinds::FireSignalPayload = serde_json::from_value(req.spec.payload.clone())
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("malformed fire_signal payload: {e}")))?;
        let t = scope::require_signal_owned_by(&state.scope_cache, &state.pool, &caller, &fire.token).await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
        // A held connection fires under its holder's claim: only the
        // holder itself may name it, and only while the row is still held
        // under it (another holder took it, or it was registered again to
        // be served another way, and the copy that lost it has not looked
        // yet).
        match crate::held_signals::judge_held_fire(&state.pool, caller.replica.as_deref(), &fire)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?
        {
            crate::held_signals::HeldFire::Taken => {}
            crate::held_signals::HeldFire::NotItsSender => {
                return Err((StatusCode::FORBIDDEN, "a held fire names the holder that sends it".into()));
            }
            crate::held_signals::HeldFire::NoLongerHeld => {
                return Err((StatusCode::CONFLICT, format!("signal {} is no longer held by its sender", fire.token)));
            }
        }
    }

    // These kinds act on the run their PAYLOAD names (the dispatcher's
    // executor reads `payload.execution_id` and takes the tenant and
    // project from that run), so the payload's run must be the one the
    // task names and the scope check above just proved is the worker's
    // own. Otherwise a worker could name its own run on the task and
    // another tenant's run in the payload.
    if caller.role == Role::Worker
        && [TaskKind::RegisterSignal.as_str(), TaskKind::RecordCost.as_str(), TaskKind::RecordLog.as_str()]
            .contains(&kind.as_str())
    {
        let named = req.spec.execution_id.as_deref().ok_or((
            StatusCode::BAD_REQUEST,
            format!("a {kind} task names the run it is for (execution_id)"),
        ))?;
        if req.spec.payload.get("execution_id").and_then(|v| v.as_str()) != Some(named) {
            return Err((
                StatusCode::FORBIDDEN,
                format!("a {kind} task's payload names the same run as the task"),
            ));
        }
    }

    // A program call acts on the asking run's own project, as that run:
    // the task names the run (its execution), the project the run belongs to,
    // and the payload's asker is that same run. Nothing else is taken on
    // the worker's word.
    if kind == TaskKind::ProgramCall.as_str() {
        let execution_id = req.spec.execution_id.as_deref().ok_or((
            StatusCode::BAD_REQUEST,
            "a program call names the asking run (execution)".to_string(),
        ))?;
        let run = scope::require_execution_id_scope(&state.scope_cache, &state.pool, &caller, execution_id).await?;
        if req.spec.project_id != Some(run.project) {
            return Err((
                StatusCode::FORBIDDEN,
                "a program call acts on the asking run's own project".into(),
            ));
        }
        let payload: weft_core::program::ProgramCallPayload = serde_json::from_value(req.spec.payload.clone())
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("program_call payload: {e}")))?;
        if payload.by.to_string() != execution_id {
            return Err((
                StatusCode::FORBIDDEN,
                "a program call's asker is the run that sends it".into(),
            ));
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
                    "control-plane caller must name a project / execution / signal so the task's \
                     tenant can be resolved".into(),
                ));
            }
        },
    };

    // `req.spec` IS a `NewTask` directly (the wire shape matches the
    // type). Stamp the RESOLVED tenant (the resource's true tenant),
    // never the wire value and never the caller identity.
    let mut new_task = req.spec;
    new_task.tenant_id = resolved_tenant;
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
    require_replica_matches(&caller, &req.replica)?;
    let filter = req.filter;
    if let ClaimFilter::ExecutionId { project_id, .. } = &filter {
        scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, *project_id).await?;
    } else {
        return Err((StatusCode::FORBIDDEN, "a worker claims only the execution it was called for".into()));
    }
    let task = state
        .tasks
        .claim_one(&req.replica, filter, held(req.wait_ms))
        .await
        .map_err(internal)?;
    // Latest-claim-wins execution ownership is bound IN the claim's own
    // transaction by the `task_claim_binds_execution_id_owner` DB trigger:
    // claiming an execution-bearing task atomically stamps
    // execution.owner_replica to the claiming replica. The broker
    // does NOT stamp it here, so "claimed by X" and "owned by X" can never
    // disagree. The journal_record owner check reads what the trigger
    // wrote.
    Ok(Json(TaskClaimOneResponse { task }))
}

pub async fn task_heartbeat(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskHeartbeatRequest>,
) -> Resp<TaskHeartbeatResponse> {
    require_worker(&caller)?;
    require_replica_matches(&caller, &req.replica)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    let renewed = state
        .tasks
        .heartbeat(req.task_id, &req.replica)
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
    require_replica_matches(&caller, &req.replica)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    let requeued = state
        .tasks
        .requeue(req.task_id, &req.replica)
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
    require_replica_matches(&caller, &req.replica)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    state
        .tasks
        .complete(req.task_id, &req.replica, req.result)
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
    require_replica_matches(&caller, &req.replica)?;
    require_task_owned_by(&state, &caller, req.task_id).await?;
    state
        .tasks
        .fail(req.task_id, &req.replica, req.error)
        .await
        .map_err(internal)?;
    Ok(Json(TaskFailResponse {}))
}

pub async fn task_wait_cancels(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskWaitCancelsRequest>,
) -> Resp<TaskWaitCancelsResponse> {
    require_worker(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id).await?;
    // Only the executions the calling replica drives: a cancel is taken as it
    // is answered, so a worker waiting on an execution it does not own would
    // swallow a cancel meant for the one that does.
    let replica = caller.replica.as_deref().ok_or((StatusCode::FORBIDDEN, "a worker names its replica".into()))?;
    let owned: Vec<String> = sqlx::query_scalar(
        "SELECT execution_id FROM execution WHERE execution_id = ANY($1) AND project_id = $2 AND owner_replica = $3",
    )
    .bind(&req.execution_ids)
    .bind(req.project_id)
    .bind(replica)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("owned executions")))?;
    if owned.len() != req.execution_ids.len() {
        return Err((StatusCode::FORBIDDEN, "a worker waits only on the executions it drives".into()));
    }
    let cancels = state.tasks.wait_cancels(req.project_id, owned, held(req.wait_ms)).await.map_err(internal)?;
    Ok(Json(TaskWaitCancelsResponse { cancels }))
}

// ---------- Infra ----------

/// Whose copy a node's call reaches: the run's instance when the node
/// exists once per instance, the shared copy otherwise. A per-instance
/// node asking from a run that carries no instance is refused: reading
/// the shared copy in its place would hand it another instance's thing.
fn instance_copy(
    instance: Option<&weft_core::instance::InstanceId>,
    per_instance: bool,
) -> Result<Option<&weft_core::instance::InstanceId>, (StatusCode, String)> {
    match (per_instance, instance) {
        (false, _) => Ok(None),
        (true, Some(instance)) => Ok(Some(instance)),
        (true, None) => Err((
            StatusCode::BAD_REQUEST,
            "this node exists once per instance, but the run asking carries no instance; \
             start it from an instance's door"
                .into(),
        )),
    }
}

/// What infra `project`'s registered program declares: from the cache
/// when its definition's digest is known there (one small read), else
/// read whole, parsed and kept. `None` when the project is gone.
async fn declared_infra(
    state: &BrokerState,
    project: uuid::Uuid,
) -> Result<Option<Arc<weft_core::project::DeclaredInfra>>, (StatusCode, String)> {
    let digest: Option<String> = sqlx::query_scalar("SELECT md5(project_json) FROM project WHERE id = $1")
        .bind(project)
        .fetch_optional(&state.pool)
        .await
        .map_err(definition_unavailable)?;
    match digest {
        Some(digest) => declared_infra_under(state, project, digest).await,
        None => Ok(None),
    }
}

/// What the program registered for `project` under `digest`
/// (`md5(project_json)`) declares: the cached answer, or, the first time,
/// the definition read and parsed. `None` when the project is gone.
async fn declared_infra_under(
    state: &BrokerState,
    project: uuid::Uuid,
    digest: String,
) -> Result<Option<Arc<weft_core::project::DeclaredInfra>>, (StatusCode, String)> {
    if let Some(declared) = state.declared_infra.get(project, &digest) {
        return Ok(Some(declared));
    }
    // Read with its own digest, so what is kept is filed under the
    // definition actually parsed, even if it changed since `digest`.
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT project_json, md5(project_json) FROM project WHERE id = $1")
            .bind(project)
            .fetch_optional(&state.pool)
            .await
            .map_err(definition_unavailable)?;
    let Some((project_json, digest)) = row else { return Ok(None) };
    let definition: weft_core::project::ProjectDefinition = serde_json::from_str(&project_json)
        .map_err(|e| internal(anyhow::anyhow!("project {project}: definition: {e}")))?;
    let declared = Arc::new(weft_core::project::DeclaredInfra::of(&definition));
    state.declared_infra.put(project, digest, declared.clone());
    Ok(Some(declared))
}

fn definition_unavailable(e: sqlx::Error) -> (StatusCode, String) {
    unavailable_or_internal(anyhow::Error::from(e).context("project definition"))
}

/// Refuse a handle naming a copy the program (its stored definition)
/// does not declare: a place that is not infra, or the side it no
/// longer has. The message names the handle, never an address.
fn require_declared_infra(
    declared: &weft_core::project::DeclaredInfra,
    infra: &weft_core::infra::InfraHandle,
) -> Result<(), (StatusCode, String)> {
    let instance_copy = infra.instance().is_some();
    if declared.declares(infra.place(), instance_copy) {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        format!(
            "{infra} is not infra this program declares{}; wire the input to an infra \
             node's handle output in this program",
            if instance_copy { " once per instance" } else { " with one shared copy" },
        ),
    ))
}

pub async fn infra_endpoint_url(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<InfraEndpointUrlRequest>,
) -> Resp<InfraEndpointUrlResponse> {
    // A worker's run asks, at fire time, for an endpoint of an infra
    // node: its node's own, or one another infra node shared with it.
    // The project is the run's, so a handle never reaches outside it. A
    // handle naming an instance names that instance's copy, which only
    // that instance's runs may reach; one naming none is the shared copy,
    // and a per-instance node has no shared row to find.
    require_worker(&caller)?;
    let run = scope::require_execution_id_scope(&state.scope_cache, &state.pool, &caller, &req.execution_id.to_string())
        .await?;
    let instance = req.infra.instance();
    if instance.is_some() && instance != run.instance.as_ref() {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "{} belongs to another instance than the run asking for it ({}); \
                 a run reaches only its own instance's infra",
                req.infra,
                run.instance.as_ref().map_or("no instance", |i| i.as_str()),
            ),
        ));
    }
    // The handle may have been minted against an older program: a node
    // since removed, or one that changed sides (shared vs per instance),
    // can leave its old copy running. Only a copy the project's program
    // declares is reachable.
    let declared = declared_infra(&state, run.project).await?.ok_or_else(|| {
        (StatusCode::NOT_FOUND, format!("{}: the run's project is no longer registered", req.infra))
    })?;
    require_declared_infra(&declared, &req.infra)?;
    let address = state
        .infra
        .endpoint_address(run.project, req.infra.place(), instance, req.infra.endpoint())
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
/// and the execution it names must be one it may act for.
///
/// The ownership rule itself lives in `require_execution_id_scope`, which
/// every execution-named verb goes through, so this adds only the
/// role gate: connections are a worker's business and nobody else's.
async fn worker_execution_scope(
    state: &BrokerState,
    caller: &crate::auth::CallerIdentity,
    execution_id: &str,
) -> Result<scope::ExecutionScope, (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    scope::require_execution_id_scope(&state.scope_cache, &state.pool, caller, execution_id).await
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
    let owner = worker_execution_scope(&state, &caller, &req.execution_id).await?;
    let tenant = owner.tenant.clone();
    let connection_id: uuid::Uuid = req.connection_id.parse().map_err(|_| {
        (StatusCode::BAD_REQUEST, format!("malformed connection id '{}'", req.connection_id))
    })?;
    // An instance's connection serves that instance's runs alone.
    let for_instance = owner
        .instance
        .clone()
        .map(|instance| weft_core::instance::InstanceScope { project_id: owner.project, instance });
    let resolved = weft_access_store::resolve_for_worker(
        &state.pool,
        &tenant,
        weft_access_store::GrantUser::of(for_instance.as_ref()),
        connection_id,
        &req.service,
        &req.required_permissions,
        &req.required_values,
    )
    .await
    .map_err(|e| match e.downcast_ref::<weft_access_store::AccessError>() {
        // The store's own words name no place to fix it from; a node does.
        Some(weft_access_store::AccessError::NotFound) => (
            StatusCode::NOT_FOUND,
            "this connection does not exist here; pick one on the access node".into(),
        ),
        _ => store_err(e),
    })?;

    let response = match resolved.owner {
        weft_core::CredentialOwner::Author | weft_core::CredentialOwner::Instance(_) => ResolveConnectionResponse {
            values: resolved.values,
            auth: resolved.auth,
            identity: resolved.identity,
            relay_url: None,
            owner: resolved.owner,
        },
        weft_core::CredentialOwner::Platform => {
            // The row's service (already matched against the request)
            // keys the credential source; nothing here trusts a
            // caller-supplied name for anything but that equality.
            let name = crate::credential::single_value_name(&resolved.auth, &resolved.service)
                .map_err(internal)?;
            let key_req = crate::credential::KeyRequest {
                tenant,
                execution_id: req.execution_id.clone(),
                project_id: owner.project,
                node_id: req.node_id,
                frames: req.frames,
                node_type: req.node_type,
                service: resolved.service.clone(),
                auth: resolved.auth.clone(),
                replica: caller.replica.clone(),
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
/// found. The execution scope check keeps a worker from retiring another
/// tenant's credentials.
pub async fn release_connection(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ReleaseConnectionRequest>,
) -> Resp<ReleaseConnectionResponse> {
    let tenant = worker_execution_scope(&state, &caller, &req.execution_id).await?.tenant;
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
    let owner = publisher_scope(&state, &caller, &req.execution_id, &mut req.node_id).await?;
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
            // An instance's copy of a node publishes that instance's
            // connection: the run it publishes from says whose.
            instance: instance_copy(owner.instance.as_ref(), req.per_instance)?.cloned(),
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
    let owner = publisher_scope(&state, &caller, &req.execution_id, &mut req.node_id).await?;
    let found = weft_access_store::published_connection(
        &state.pool,
        &owner.tenant,
        owner.project,
        &req.node_id,
        instance_copy(owner.instance.as_ref(), req.per_instance)?,
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
    execution_id: &str,
    node_id: &mut String,
) -> Result<scope::ExecutionScope, (StatusCode, String)> {
    let owner = worker_execution_scope(state, caller, execution_id).await?;
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
    let synced = crate::lifecycle_writes::sync_ownership(&state.pool, &req.replica, &req.held_projects)
        .await
        .map_err(internal)?;
    Ok(Json(synced))
}

/// Pure read: the projects a supervisor process currently owns, joined to
/// live project state. No claim, no renew (ownership breadth changes
/// only via `sync_ownership`). Used by the work loops.
pub async fn supervisor_owned_projects(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorOwnedProjectsRequest>,
) -> Resp<SupervisorOwnedProjectsResponse> {
    require_supervisor(&caller)?;
    let owned = crate::lifecycle_writes::owned_projects(&state.pool, &req.replica)
        .await
        .map_err(internal)?;
    Ok(Json(SupervisorOwnedProjectsResponse { owned }))
}

/// Which of the named copies of one project are gone for good (see
/// [`SupervisorGoneCopiesRequest`]). Fenced on the project's
/// `infra_owner` lease like a lifecycle write, in the same statement that
/// reads the definition: an existing project is judged only for the
/// supervisor that owns it (410 otherwise), because only the owner can
/// be applying the very copy a judgment calls gone. A removed project is
/// judged for anyone: nothing applies it until it is registered again.
pub async fn supervisor_gone_copies(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorGoneCopiesRequest>,
) -> Resp<SupervisorGoneCopiesResponse> {
    require_supervisor(&caller)?;
    if let Some(stray) = req.copies.iter().find(|c| c.project != req.project) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("gone_copies of project {}: copy {} belongs to project {}", req.project, stray.copy_id, stray.project),
        ));
    }
    let err = |e: sqlx::Error| internal(anyhow::anyhow!("{e}"));
    let project: Option<(String, bool)> = sqlx::query_as(&format!(
        "SELECT md5(p.project_json), {owns} FROM project p WHERE p.id = $1",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$2", "p.id"),
    ))
    .bind(req.project)
    .bind(&req.replica)
    .fetch_optional(&state.pool)
    .await
    .map_err(err)?;
    let gone = match project {
        None => req.copies.iter().map(|c| c.copy_id.clone()).collect(),
        Some((_, false)) => {
            return Err((StatusCode::GONE, format!("gone_copies of project {}: project ownership moved", req.project)));
        }
        Some((digest, true)) => {
            let Some(declared) = declared_infra_under(&state, req.project, digest).await? else {
                // Removed between the two reads: every copy is gone.
                return Ok(Json(SupervisorGoneCopiesResponse { gone: req.copies.iter().map(|c| c.copy_id.clone()).collect() }));
            };
            let rows: std::collections::HashSet<String> =
                sqlx::query_scalar("SELECT copy_id FROM infra_node WHERE project_id = $1")
                    .bind(req.project)
                    .fetch_all(&state.pool)
                    .await
                    .map_err(err)?
                    .into_iter()
                    .collect();
            req.copies
                .iter()
                .filter(|c| !rows.contains(&c.copy_id) && !declared.declares_copy(c))
                .map(|c| c.copy_id.clone())
                .collect()
        }
    };
    Ok(Json(SupervisorGoneCopiesResponse { gone }))
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
        "SELECT node_id, instance_id, copy_id, status, applied_spec_hash, applied_at_unix, \
                endpoints_json, install_endpoints_json, public_paths_json, doors_json, keep_disks_json, units_json \
         FROM infra_node WHERE project_id = $1 ORDER BY node_id, instance_id NULLS FIRST",
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
        let instance: Option<String> = r
            .try_get("instance_id")
            .map_err(|e| internal(anyhow::anyhow!("decode instance_id: {e}")))?;
        let instance = instance
            .map(weft_core::instance::InstanceId::new)
            .transpose()
            .map_err(|e| internal(anyhow::anyhow!("infra_node.instance_id for node='{node_id}': {e}")))?;
        let copy_id: String = r
            .try_get("copy_id")
            .map_err(|e| internal(anyhow::anyhow!("decode copy_id: {e}")))?;
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
        let applied_at_unix: Option<i64> = r
            .try_get("applied_at_unix")
            .map_err(|e| internal(anyhow::anyhow!("decode applied_at_unix: {e}")))?;
        // A JSON column that must decode as `T`, naming the column when
        // it does not (schema drift surfaces as a 500, never a default).
        fn column<T: serde::de::DeserializeOwned>(
            r: &sqlx::postgres::PgRow,
            name: &str,
            node_id: &str,
        ) -> Result<T, (StatusCode, String)> {
            let v: serde_json::Value = r.try_get(name).map_err(|e| internal(anyhow::anyhow!("decode {name}: {e}")))?;
            serde_json::from_value(v)
                .map_err(|e| internal(anyhow::anyhow!("infra_node.{name} for node='{node_id}' does not decode: {e}")))
        }
        let addresses = weft_broker_client::protocol::AppliedEndpoints {
            urls: column(&r, "endpoints_json", &node_id)?,
            install_urls: column(&r, "install_endpoints_json", &node_id)?,
            public_paths: column(&r, "public_paths_json", &node_id)?,
            doors: column(&r, "doors_json", &node_id)?,
        };
        let keep_disks: Vec<String> = column(&r, "keep_disks_json", &node_id)?;
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
            instance,
            copy_id,
            status,
            applied_spec_hash,
            applied_at_unix,
            addresses,
            keep_disks,
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
    // again. Any issue wakes it, not only one for a project this process
    // owns: ownership can move during the hold, and the look is one
    // indexed read.
    //
    // A wake that finds nothing for this process may be a command for a
    // project nobody owns yet (ownership is taken on the supervisors'
    // own ticks): the answer then says so, and the process takes ownership
    // at once instead of on its next tick. Only after a wake, so a
    // supervisor woken by a command it cannot take yet does not spin.
    let deadline = tokio::time::Instant::now() + held(req.wait_ms);
    let mut heard = state.signals.subscribe();
    let mut woken_once = false;
    loop {
        let next = crate::lifecycle_writes::next_command(
            &state.pool,
            &req.claimer_replica,
            &req.busy_projects,
        )
        .await
        .map_err(internal)?;
        if let Some(command) = next {
            return Ok(Json(SupervisorClaim::Command(command)));
        }
        if woken_once && crate::lifecycle_writes::unowned_work_waiting(&state.pool).await.map_err(internal)? {
            return Ok(Json(SupervisorClaim::UnownedWork));
        }
        let woken = heard
            .woken_before(deadline, |channel, payload| ISSUED_WAKE.hears(channel, payload))
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
        req.instance.as_ref(),
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
    // the scope-checked HTTP wrapper. The identity is the supervisor's
    // replica id (`req.replica`, what keys `infra_owner`).
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

/// Record what an apply waits on, for `weft status`: the supervisor
/// writes it as its units come up. Fenced like `set_status` under a
/// command.
pub async fn supervisor_set_waiting(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<SupervisorSetWaitingRequest>,
) -> Resp<SupervisorSetWaitingResponse> {
    require_supervisor(&caller)?;
    require_node_id(&mut req.node_id)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let outcome = crate::lifecycle_writes::set_waiting(&state.pool, &req)
        .await
        .map_err(internal)?;
    fenced_to_http(outcome, || {
        format!("set_waiting(project={}, node={}, cmd={})", req.project_id, req.node_id, req.command_id)
    })?;
    Ok(Json(SupervisorSetWaitingResponse {}))
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
    /// True for set_applied (stamps `applied_at_unix = NOW()`, clears
    /// `provisioning_since_unix`), false for provisioning (the other way
    /// round). Either way `waiting_on` starts empty.
    stamp_applied_at: bool,
    /// Where the endpoints answer; empty until the apply succeeds.
    addresses: weft_broker_client::protocol::AppliedEndpoints,
    /// What the host runs differently from what was asked
    /// (`SupervisorSetAppliedRequest::notes`); empty until the apply
    /// succeeds.
    notes: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
async fn write_apply_row(
    state: &BrokerState,
    op: &str,
    project_id: uuid::Uuid,
    node_id: &str,
    instance: Option<&weft_core::instance::InstanceId>,
    copy_id: &str,
    keep_disks: &[String],
    units_json: serde_json::Value,
    command_id: i64,
    owner: &str,
    row: ApplyRowState,
) -> Result<(), (StatusCode, String)> {
    let json = |what: &str, v: Result<serde_json::Value, serde_json::Error>| {
        v.map_err(|e| internal(anyhow::anyhow!("{what} serialize: {e}")))
    };
    let keep_disks_json = json("keep_disks", serde_json::to_value(keep_disks))?;
    let endpoints_json = json("endpoints", serde_json::to_value(&row.addresses.urls))?;
    let public_paths_json = json("public_paths", serde_json::to_value(&row.addresses.public_paths))?;
    let doors_json = json("doors", serde_json::to_value(&row.addresses.doors))?;
    let install_endpoints_json = json("install_endpoints", serde_json::to_value(&row.addresses.install_urls))?;
    let notes_json = json("notes", serde_json::to_value(&row.notes))?;
    // The INSERT pulls its values FROM the caller's still-claimed
    // apply command so the ownership check and the write share one
    // row snapshot. Every variable is a bind: no SQL built by string
    // interpolation. `applied_at_unix` uses the DB clock (consistent
    // with every other timestamp write in this file), gated on the
    // bound `$6` flag via CASE.
    let res = sqlx::query(
        &format!("INSERT INTO infra_node \
         (project_id, node_id, instance_id, copy_id, status, \
          failure_stage, failure_message, applied_spec_hash, \
          applied_at_unix, endpoints_json, public_paths_json, doors_json, keep_disks_json, units_json, \
          install_endpoints_json, notes_json, waiting_on, provisioning_since_unix) \
         SELECT $1, $2, $13, $3, $4, NULL, NULL, $5, \
                CASE WHEN $6 THEN EXTRACT(EPOCH FROM NOW())::BIGINT ELSE NULL END, \
                $7, $12, $14, $8, $9, $15, $16, NULL, \
                CASE WHEN $6 THEN NULL ELSE EXTRACT(EPOCH FROM NOW())::BIGINT END \
         FROM infra_lifecycle_command \
         WHERE id = $10 \
           AND project_id = $1 \
           AND node_id = $2 \
           AND instance_id IS NOT DISTINCT FROM $13 \
           AND verb = 'apply' \
           AND completed_at_unix IS NULL \
           AND {owns} \
         ON CONFLICT (project_id, node_id, instance_id) DO UPDATE SET \
            copy_id            = EXCLUDED.copy_id, \
            status             = EXCLUDED.status, \
            failure_stage      = NULL, \
            failure_message    = NULL, \
            applied_spec_hash  = EXCLUDED.applied_spec_hash, \
            applied_at_unix    = EXCLUDED.applied_at_unix, \
            endpoints_json     = EXCLUDED.endpoints_json, \
            public_paths_json  = EXCLUDED.public_paths_json, \
            doors_json         = EXCLUDED.doors_json, \
            install_endpoints_json = EXCLUDED.install_endpoints_json, \
            notes_json         = EXCLUDED.notes_json, \
            keep_disks_json = EXCLUDED.keep_disks_json, \
            units_json         = EXCLUDED.units_json, \
            waiting_on         = NULL, \
            provisioning_since_unix = EXCLUDED.provisioning_since_unix",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$11", "$1"),
    ),
    )
    .bind(project_id)
    .bind(node_id)
    .bind(copy_id)
    .bind(row.status)
    .bind(&row.applied_spec_hash)
    .bind(row.stamp_applied_at)
    .bind(endpoints_json)
    .bind(keep_disks_json)
    .bind(units_json)
    .bind(command_id)
    .bind(owner)
    .bind(public_paths_json)
    .bind(instance.map(|m| m.as_str()))
    .bind(doors_json)
    .bind(install_endpoints_json)
    .bind(notes_json)
    .execute(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    if res.rows_affected() == 0 {
        let outcome = crate::lifecycle_writes::stale_answer(&state.pool, owner, project_id)
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
    let units_json = serde_json::to_value(&req.units)
        .map_err(|e| internal(anyhow::anyhow!("units serialize: {e}")))?;
    write_apply_row(
        &state,
        "set_applied",
        req.project_id,
        &req.node_id,
        req.instance.as_ref(),
        &req.copy_id,
        &req.keep_disks,
        units_json,
        req.command_id,
        &req.replica,
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
            addresses: req.addresses.clone(),
            notes: req.notes.clone(),
        },
    )
    .await?;
    Ok(Json(SupervisorSetAppliedResponse {}))
}

/// Supervisor-callable: write the `infra_node` row at `Provisioning`
/// before the apply begins. Locks in the (copy_id, keep_disks) pair
/// so that a partial-apply leaves a visible row
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
        req.instance.as_ref(),
        &req.copy_id,
        &req.keep_disks,
        units_json,
        req.command_id,
        &req.replica,
        ApplyRowState {
            // Not-yet-applied: NULL hash + applied_at, empty
            // endpoints. set_applied flips these on success.
            status: weft_broker_client::protocol::InfraNodeStatus::Provisioning.as_str(),
            applied_spec_hash: None,
            stamp_applied_at: false,
            addresses: Default::default(),
            notes: Vec::new(),
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
    // Verify the spec before persisting (`LifecycleSpec::validate`):
    // a take-down's (mode, policy) combo is coherent and it names the
    // broken copies it aims at; a recovery names the recovered ones.
    if let Err(msg) = req.spec.validate() {
        return Err((StatusCode::BAD_REQUEST, msg.to_string()));
    }
    // The typed `LifecycleSpec` only constructs `Deactivate(...)` /
    // `Reactivate(...)`, so a caller can't enqueue a supervisor-owned
    // verb here. `into_row_columns()` returns running_policy =
    // None for both variants (Deactivate carries it inside
    // spec_json; Reactivate has no policy). Bind NULL.
    let (verb, running_policy, spec_json) = req.spec.into_row_columns();
    let issued_by_replica = caller.replica.as_deref().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            "supervisor token missing replica claim".to_string(),
        )
    })?;
    let command_id = crate::lifecycle_writes::issue_command(
        &state.pool,
        &crate::lifecycle_writes::IssuedCommand {
            tenant_id: &project_tenant,
            project_id: req.project_id,
            node_id: None,
            // A dispatcher verb acts on activations, not on infra
            // copies; the column stays at its shared default.
            copies: &weft_core::instance::Copies::Shared,
            verb,
            running_policy,
            spec_json: spec_json.as_ref(),
            issued_by_replica,
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
    let issued_by_replica = caller.replica.as_deref().ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            "worker token missing replica claim".to_string(),
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
            copies: &weft_core::instance::Copies::of(req.instance.clone()),
            verb: weft_broker_client::protocol::InfraLifecycleVerb::Apply,
            running_policy: None,
            spec_json: Some(&req.spec_json),
            issued_by_replica,
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
    // Ownership gate: only the replica that currently OWNS the project may
    // cascade-delete its infra_node + cancel its pending commands. A
    // supervisor that lost ownership mid-Terminate must NOT wipe rows
    // out from under the new owner. 410 → the supervisor aborts the
    // command (leaving it uncompleted for the new owner to re-run). The
    // check runs INSIDE the cascade transaction so the ownership read
    // and the deletes share one snapshot (no TOCTOU window). The
    // identity is the supervisor's replica id (`req.replica`, what
    // keys `infra_owner`).
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
    .bind(&req.replica)
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
                req.replica, req.project_id
            ),
        ));
    }
    let instance = req.instance.as_ref().map(|m| m.as_str());
    let res = sqlx::query(
        "DELETE FROM infra_node \
          WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
    )
        .bind(req.project_id)
        .bind(&req.node_id)
        .bind(instance)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(anyhow::anyhow!("delete infra_node: {e}")))?;
    sqlx::query(
        "DELETE FROM infra_event \
          WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
    )
    .bind(req.project_id)
    .bind(&req.node_id)
    .bind(instance)
    .execute(&mut *tx)
    .await
    .map_err(|e| internal(anyhow::anyhow!("delete infra_event: {e}")))?;
    // Cancel any not-yet-completed lifecycle commands aimed at exactly
    // this copy (this node, this owner) by stamping a completion, except
    // the terminate doing the removing, which completes on its own.
    // Commands for the whole project (`node_id IS NULL`) or for every
    // copy are left in place: they still have other copies to act on.
    use weft_broker_client::protocol::LifecycleOutcome;
    sqlx::query(
        "UPDATE infra_lifecycle_command \
            SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
                outcome = $3, \
                outcome_message = 'node removed by remove_node' \
          WHERE project_id = $1 AND node_id = $2 \
            AND NOT every_copy AND instance_id IS NOT DISTINCT FROM $4 \
            AND completed_at_unix IS NULL AND id <> $5",
    )
    .bind(req.project_id)
    .bind(&req.node_id)
    .bind(LifecycleOutcome::Cancelled.as_str())
    .bind(instance)
    .bind(req.command_id)
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
        Some((&req.node_id, req.instance.as_ref())),
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
    let running_count = crate::lifecycle_writes::live_run_count(&state.pool, req.project_id, &req.copies)
        .await
        .map_err(internal)?;
    Ok(Json(SupervisorRunningCountResponse { running_count }))
}

/// The project's uncompleted supervisor commands (apply / stop /
/// terminate), each as the copies it acts on. The supervisor's health
/// loop stands down for exactly those copies, so an autonomous health
/// reconcile never races a user action over a copy's status, and every
/// other copy keeps its health. Dispatcher-owned verbs (deactivate /
/// reactivate) are health's own requests and touch no copy's status.
pub async fn supervisor_infra_command_in_flight(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorInfraCommandInFlightRequest>,
) -> Resp<SupervisorInfraCommandInFlightResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    let rows: Vec<(Option<String>, Option<String>, bool)> = sqlx::query_as(&format!(
        "SELECT node_id, instance_id, every_copy FROM infra_lifecycle_command \
         WHERE project_id = $1 AND completed_at_unix IS NULL AND verb IN ({verbs})",
        verbs = weft_broker_client::lifecycle_command::SUPERVISOR_VERBS_SQL,
    ))
    .bind(req.project_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| internal(anyhow::anyhow!("{e}")))?;
    let commands = rows
        .into_iter()
        .map(|(node_id, instance, every)| {
            let copies = weft_core::instance::Copies::from_columns(instance, every)
                .map_err(|e| internal(anyhow::anyhow!("infra_lifecycle_command copies: {e}")))?;
            Ok(InFlightCommand { node_id, copies })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(SupervisorInfraCommandInFlightResponse { commands }))
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
    // db-tested). Ownership identity is the supervisor's replica id
    // (`req.replica`, what keys `infra_owner`).
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

pub async fn signal_list_held(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalListHeldRequest>,
) -> Resp<SignalListHeldResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    // The listener is a trusted control-plane caller; it rehydrates every
    // held signal (mixed tenants, each row carrying its own), or one
    // project's when an activation asks.
    let out = crate::held_signals::signals_held(&state.pool, req.project).await.map_err(internal)?;
    Ok(Json(SignalListHeldResponse { rows: out }))
}

/// One held signal by token: what the listener loads a signal it has not
/// seen yet from (after a restart, or on another copy of a serverless
/// listener).
pub async fn signal_get_held(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalGetHeldRequest>,
) -> Resp<SignalGetHeldResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    let row = crate::held_signals::signal_held(&state.pool, &req.token).await.map_err(internal)?;
    Ok(Json(SignalGetHeldResponse { row }))
}

/// A signal kind's durable state (a feed cursor, a timer's next moment),
/// written by the listener directly as a claim that exactly one of two
/// racing listeners wins (see `SignalWriteKindStateRequest`).
pub async fn signal_write_kind_state(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalWriteKindStateRequest>,
) -> Resp<SignalWriteKindStateResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    let written = crate::held_signals::write_kind_state(&state.pool, &req.token, &req.kind_state, req.from_seq)
        .await
        .map_err(internal)?;
    Ok(Json(SignalWriteKindStateResponse { written }))
}

/// One look of a holder: renew, report, take (see
/// `SignalHoldRequest`). Bound to the calling replica, so a holder only
/// ever renews or takes in its own name.
pub async fn signal_hold(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalHoldRequest>,
) -> Resp<SignalHoldResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    require_replica_matches(&caller, &req.replica)?;
    let out = crate::held_signals::hold(
        &state.pool,
        &req.replica,
        &req.holding,
        req.room,
        &req.want,
        weft_broker_client::protocol::hold_lease_secs(),
    )
    .await
    .map_err(internal)?;
    Ok(Json(out))
}

/// What a signal's kind decides about holding it, for a row that said
/// otherwise.
pub async fn signal_set_holds(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalSetHoldsRequest>,
) -> Resp<serde_json::Value> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    crate::held_signals::set_holds(&state.pool, &req.token, req.holds).await.map_err(internal)?;
    Ok(Json(serde_json::json!({})))
}

/// A holder giving up its claims (it is stopping).
pub async fn signal_let_go(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalLetGoRequest>,
) -> Resp<serde_json::Value> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    require_replica_matches(&caller, &req.replica)?;
    crate::held_signals::let_go(&state.pool, &req.replica).await.map_err(internal)?;
    Ok(Json(serde_json::json!({})))
}

// ---------- helpers ----------

fn require_worker(caller: &CallerIdentity) -> Result<(), (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    Ok(())
}

/// Reject if the request claims a `replica` other than the replica the
/// call came from.
fn require_replica_matches(
    caller: &CallerIdentity,
    claimed: &str,
) -> Result<(), (StatusCode, String)> {
    let bound = caller.replica.as_deref().ok_or((
        StatusCode::FORBIDDEN,
        "the call names no replica; refusing a replica-bound op".into(),
    ))?;
    if bound != claimed {
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_role = ?caller.role,
            bound_replica = %bound,
            claimed_replica = %claimed,
            "broker rejected replica mismatch"
        );
        return Err((
            StatusCode::FORBIDDEN,
            "claimed replica is not the calling replica".into(),
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
        let pool_gone = anyhow::Error::from(sqlx::Error::PoolTimedOut).context("execution lookup");
        assert_eq!(unavailable_or_internal(pool_gone).0, StatusCode::SERVICE_UNAVAILABLE);
        let bad_row = anyhow::Error::from(sqlx::Error::RowNotFound);
        assert_eq!(unavailable_or_internal(bad_row).0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(unavailable_or_internal(anyhow::anyhow!("undecodable row")).0, StatusCode::INTERNAL_SERVER_ERROR);
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
        // A task naming a project in one tenant and an execution in another is
        // ambiguous; refuse it rather than silently stamping either.
        let mut anchor = Some("acme".to_string());
        let err = merge_anchor_tenant(&mut anchor, "globex".into()).unwrap_err();
        assert_eq!(err.0, StatusCode::CONFLICT);
    }

    /// A program with `shared` (one copy), `mine` (one per instance)
    /// and `plain` (no infra).
    fn program_json() -> String {
        let node = |id: &str, infra: bool, per_instance: bool| {
            let mut n = serde_json::json!({
                "id": id, "nodeType": "Any", "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [], "outputs": [], "scope": [], "groupBoundary": null,
                "requiresInfra": infra, "images": []
            });
            if per_instance {
                n["perInstance"] = serde_json::json!("marked");
            }
            n
        };
        serde_json::json!({
            "id": uuid::Uuid::nil(),
            "nodes": [node("shared", true, false), node("mine", true, true), node("plain", false, false)],
            "edges": [],
            "groups": []
        })
        .to_string()
    }

    #[test]
    fn a_handle_reaches_only_a_copy_the_program_declares() {
        use weft_core::infra::InfraHandle;
        let definition: weft_core::project::ProjectDefinition = serde_json::from_str(&program_json()).unwrap();
        let json = weft_core::project::DeclaredInfra::of(&definition);
        let alice = || Some(weft_core::instance::InstanceId::new("alice").unwrap());
        require_declared_infra(&json, &InfraHandle::new("shared", "api", None)).expect("the shared copy");
        require_declared_infra(&json, &InfraHandle::new("mine", "api", alice())).expect("an instance's copy");

        for (handle, side) in [
            (InfraHandle::new("mine", "api", None), "with one shared copy"),
            (InfraHandle::new("shared", "api", alice()), "once per instance"),
            (InfraHandle::new("plain", "api", None), "with one shared copy"),
            (InfraHandle::new("gone", "api", None), "with one shared copy"),
        ] {
            let (status, why) = require_declared_infra(&json, &handle).unwrap_err();
            assert_eq!(status, StatusCode::FORBIDDEN, "{why}");
            assert!(why.starts_with(&handle.to_string()), "it names the handle: {why}");
            assert!(why.contains(&format!("is not infra this program declares {side}")), "{why}");
        }
    }
}
