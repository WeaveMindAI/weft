//! Per-endpoint handlers. Each one:
//!   1. Lifts the authenticated `CallerIdentity` out of the request.
//!   2. Runs a scope check (the security-critical bit).
//!   3. Delegates to the underlying Postgres-direct client.
//!
//! Steps 2 and 3 are intentionally separate calls per handler so the
//! audit log records the exact `(caller, scope-kind, requested,
//! resource-tenant)` tuple. A worker's records and the runs it drives are
//! `crate::records`'s.

use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
};
use weft_broker_client::lifecycle_command::{InfraCommandSignal, INFRA_COMMAND_CHANNEL, ISSUED_WAKE};
use weft_broker_client::protocol::*;
use weft_task_store::db::{self, Client as _, Row};
use weft_task_store::tasks::DedupOutcome;
use weft_task_store::TaskKind;

use crate::auth::{AuthedCaller, CallerIdentity, Role};
use crate::scope;
use crate::state::BrokerState;
use weft_core::wire_shape::Json;

type Resp<T> = Result<Json<T>, (StatusCode, String)>;

pub async fn health() -> &'static str {
    "ok"
}

/// The gate every call a worker makes ABOUT a run it drives passes: the
/// run is in the caller's scope and the calling replica drives it now
/// (`run.owner`, while it runs). A worker acts only on the runs it drives,
/// never on a sibling's in the same project. Returns the run's scope
/// (tenant, project, instance) so the handler can act inside it.
pub(crate) async fn require_worker_drives(
    state: &BrokerState,
    caller: &CallerIdentity,
    run_id: weft_core::RunId,
) -> Result<scope::RunScope, (StatusCode, String)> {
    let replica = caller.replica.as_deref().ok_or((StatusCode::FORBIDDEN, "a worker names its replica".to_string()))?;
    let scope = scope::require_run_id_scope(&state.scope_cache, &state.pool, caller, run_id).await?;
    let driven: bool = state
        .pool
        .scalar("SELECT EXISTS (SELECT 1 FROM run WHERE run_id = $1 AND owner = $2 AND state = 'running')", &[&run_id, &replica])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a run's owner")))?;
    if !driven {
        tracing::warn!(
            target: "weft_broker::scope",
            caller_tenant = ?caller.scope.pinned_tenant(),
            caller_replica = %replica,
            %run_id,
            "broker refused a worker acting on a run it does not drive"
        );
        return Err((StatusCode::FORBIDDEN, "the run is not driven by the calling replica".into()));
    }
    Ok(scope)
}

// ---------- Run steering ----------

/// `ctx.tag_run`: write the `run_tag` rows, synchronously, so
/// they exist by the time the node's call returns (a following
/// `stop_tagged` anchors on them). The run's own `RunTagged` is on
/// record already (for an unrecorded run, its birth): its worker writes
/// it before any call that names the run. Only the worker driving the
/// run tags it.
pub async fn run_tag(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<RunTagRequest>,
) -> Resp<RunTagResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers tag runs".into()));
    }
    if req.tags.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "tag_run needs at least one tag".into()));
    }
    // The ctx validated already; the broker trusts no worker, so again.
    weft_core::tag::validate_tags(&req.tags)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    require_worker_drives(&state, &caller, req.run_id).await?;
    weft_journal::tags::tag_run_in(&state.pool, req.run_id, &req.tags, unix_now_secs())
        .await
        .map_err(|e| unavailable_or_internal(e.into()))?;
    Ok(Json(RunTagResponse {}))
}

/// `ctx.stop_tagged`: queue a `stop_tagged` task for the dispatcher,
/// with the ordering anchor resolved NOW. The project the stop runs in
/// is the asking run's own (from its row); the request never names a
/// project, so a stop cannot cross one.
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
pub async fn run_stop_tagged(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<RunStopTaggedRequest>,
) -> Resp<RunStopTaggedResponse> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "only workers stop runs by tag".into()));
    }
    let run_id = req.run_id;
    weft_core::tag::validate_tag(&req.tag)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let run_id_scope = require_worker_drives(&state, &caller, run_id).await?;
    let own_seq = weft_journal::tags::tag_seq(&state.pool, run_id, &req.tag)
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the asker's tag")))?;
    let before_seq = match req.stop_self {
        weft_core::StopSelf::Include => None,
        weft_core::StopSelf::Keep => Some(match own_seq {
            Some(own) => own,
            None => {
                weft_journal::tags::max_tag_seq(&state.pool)
                    .await
                    .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the newest tag")))?
                    + 1
            }
        }),
    };
    // Whether the asker is among the runs this stop reaches, answered by
    // the same read and the same rule the dispatcher applies to carry
    // it out: a run the dispatcher would never cancel (a node test, which
    // `live_tagged_runs` leaves out) must not be told to wait for
    // its own cancel.
    let stops_asker = match req.stop_self {
        weft_core::StopSelf::Keep => false,
        weft_core::StopSelf::Include => {
            let live =
                weft_journal::tags::live_tagged_runs(&state.pool, run_id_scope.project, &req.tag)
                    .await
                    .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the runs carrying the tag")))?;
            weft_journal::tags::select_stop_targets(&live, run_id, before_seq, req.stop_self)
                .contains(&run_id)
        }
    };
    let payload = weft_task_store::StopTaggedPayload {
        project_id: run_id_scope.project,
        tag: req.tag,
        by: run_id,
        before_seq,
        stop_self: req.stop_self,
    };
    // Every ask is its own task: a second stop for the same tag from
    // the same run is a new decision with a new anchor, never a
    // duplicate to collapse, so it has no dedup key.
    let task = weft_task_store::tasks::NewTask {
        kind: TaskKind::StopTagged,
        project_id: Some(run_id_scope.project),
        dedup_key: None,
        run_id: Some(run_id),
        tenant_id: run_id_scope.tenant,
        payload: serde_json::to_value(&payload).map_err(internal)?,
    };
    // The call's one write: everything before it read.
    weft_task_store::tasks::enqueue(&state.pool, task).await.map_err(|e| unavailable_or_internal(e.context("queue the stop")))?;
    Ok(Json(RunStopTaggedResponse { stops_asker }))
}

/// Seconds since the unix epoch, read off the system clock: the one
/// reading every broker handler makes. A clock before the epoch is a
/// broken machine, and fails loudly.
pub(crate) fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

/// When a held call's wait of `wait_ms` ends: as long as the caller asks
/// (it is held on a line, which has no idle timeout), and forever for a
/// wait past what the clock holds.
pub(crate) fn held_until(wait_ms: u64) -> tokio::time::Instant {
    weft_core::deadline::after(tokio::time::Instant::now(), Duration::from_millis(wait_ms))
}

// ---------- Tasks ----------

/// Fold a newly-resolved resource tenant into the task's anchor tenant,
/// enforcing that every named resource agrees. A task naming resources
/// in two different tenants (project in A, run in B) is ambiguous and
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

/// The answer to a task whose payload does not read as its kind's: the
/// payload is part of the call's body, so it is refused as a body of
/// another shape is (`crate::wire`).
fn unreadable_payload(state: &BrokerState, caller: &CallerIdentity, e: &serde_json::Error) -> (StatusCode, String) {
    state.unreadable_shapes.refuse(crate::wire::Sender::of_caller(caller), "/v1/task/enqueue_dedup", e)
}

pub async fn task_enqueue_dedup(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<TaskEnqueueDedupRequest>,
) -> Resp<TaskEnqueueDedupResponse> {
    let kind = req.spec.kind;

    // Per-role allow list of kinds. Anything else is a 403.
    match caller.role {
        Role::Worker => {
            // Workers enqueue control-plane work for the dispatcher to
            // handle: register a wake signal, withdraw one it gave up, or a
            // call a program makes on its own project.
            if !matches!(kind, TaskKind::RegisterSignal | TaskKind::WithdrawSignal | TaskKind::ProgramCall) {
                return Err((
                    StatusCode::FORBIDDEN,
                    format!("worker may not enqueue task kind {kind}"),
                ));
            }
        }
        Role::Listener => {
            // Listeners enqueue exactly one kind: an answer to a waiting
            // run (an entry's event goes to its worker, or waits in its
            // trigger's queue).
            if kind != TaskKind::FireSignal {
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

    // Resolve the task's authoritative tenant from the resource it
    // names, enforcing the caller's scope on every named resource. A
    // worker (tenant-scoped) may only act for its own tenant; a pooled
    // listener (control-plane, trusted) may fire held events for any
    // tenant. The `require_*` helpers each return the resource's true
    // tenant, so we never trust `req.spec.tenant_id` from the wire and a
    // control-plane caller (which has no single tenant) still yields a
    // correctly-tenanted task. When several resources are named they
    // MUST agree: `merge_anchor_tenant` rejects a task that names
    // resources in two different tenants rather than silently picking
    // one, so the stamped tenant is never ambiguous.
    let mut anchor_tenant: Option<String> = None;
    if let Some(project_id) = req.spec.project_id {
        let t =
            scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, project_id)
                .await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
    }
    if let Some(run_id) = req.spec.run_id {
        // A worker acts only for a run it drives; the listener names the
        // run an answer resolves.
        let scope = match caller.role {
            Role::Worker => require_worker_drives(&state, &caller, run_id).await?,
            _ => scope::require_run_id_scope(&state.scope_cache, &state.pool, &caller, run_id).await?,
        };
        merge_anchor_tenant(&mut anchor_tenant, scope.tenant)?;
    }
    if kind == TaskKind::FireSignal {
        // A listener's answer to a waiting run: the signal token is the
        // tenant anchor, and the signal must be the wait of the run the
        // answer names.
        let fire: weft_task_store::kinds::FireSignalPayload = serde_json::from_value(req.spec.payload.clone())
            .map_err(|e| unreadable_payload(&state, &caller, &e))?;
        let t = scope::require_signal_owned_by(&state.scope_cache, &state.pool, &caller, &fire.token).await?;
        merge_anchor_tenant(&mut anchor_tenant, t)?;
        let wait: Option<(bool, Option<weft_core::RunId>)> = state
            .pool
            .opt("SELECT is_resume, run_id FROM signal WHERE token = $1", &[&fire.token])
            .await
            .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a signal")))?;
        match wait {
            Some((true, Some(run))) if run == fire.run_id => {}
            Some((false, _)) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "an entry's event goes to its worker or waits in its trigger's queue (`/v1/door/park_fire`), never as a fire_signal task".into(),
                ));
            }
            _ => return Err((StatusCode::BAD_REQUEST, format!("signal {} is no wait of run {}", fire.token, fire.run_id))),
        }
        // A held connection fires under its holder's claim: only the
        // holder itself may name it, and only while the row is still held
        // under it (another holder took it, or it was registered again to
        // be served another way, and the copy that lost it has not looked
        // yet).
        match crate::held_signals::judge_held_fire(&state.pool, caller.replica.as_deref(), &fire)
            .await
            .map_err(|e| unavailable_or_internal(e.context("read whether a held signal is still held")))?
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

    // A wait a worker registers acts on the run its PAYLOAD names (the
    // dispatcher's executor reads `payload.run_id` and takes the
    // tenant and project from that run), so the payload's run must be the
    // one the task names and the scope check above just proved is the
    // worker's own.
    if caller.role == Role::Worker && kind == TaskKind::RegisterSignal {
        let named = req.spec.run_id.ok_or((
            StatusCode::BAD_REQUEST,
            format!("a {kind} task names the run it is for (run_id)"),
        ))?;
        let register: weft_task_store::RegisterSignalPayload = serde_json::from_value(req.spec.payload.clone())
            .map_err(|e| unreadable_payload(&state, &caller, &e))?;
        if register.run_id != named {
            return Err((
                StatusCode::FORBIDDEN,
                format!("a {kind} task's payload names the same run as the task"),
            ));
        }
    }

    // A wait a worker withdraws is one of the run the task names, which
    // the scope check above proved is the worker's own.
    if kind == TaskKind::WithdrawSignal {
        let named = req.spec.run_id.ok_or((
            StatusCode::BAD_REQUEST,
            format!("a {kind} task names the run it is for (run_id)"),
        ))?;
        let withdraw: weft_task_store::WithdrawSignalPayload = serde_json::from_value(req.spec.payload.clone())
            .map_err(|e| unreadable_payload(&state, &caller, &e))?;
        let signal: Option<(bool, Option<weft_core::RunId>)> = state
            .pool
            .opt("SELECT is_resume, run_id FROM signal WHERE token = $1", &[&withdraw.token])
            .await
            .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a signal")))?;
        // A wait already answered or withdrawn has no signal left, and
        // withdrawing it again removes nothing. Anything else the token
        // names (an entry trigger, another run's wait) is refused.
        let a_wait_of_the_run = match signal {
            None => true,
            Some((is_resume, run)) => is_resume && run == Some(named),
        };
        if withdraw.run_id != named || !a_wait_of_the_run {
            return Err((StatusCode::FORBIDDEN, format!("a {kind} task withdraws a wait of the run it names")));
        }
    }

    // A program call is carried as the asking run: the task names the run,
    // the project the run belongs to, and the payload's asker is that same
    // run. The project it acts on is the run's own or another of the same
    // owner, which is held here as well as by the action. Nothing else is
    // taken on the worker's word.
    if kind == TaskKind::ProgramCall {
        let run_id = req.spec.run_id.ok_or((
            StatusCode::BAD_REQUEST,
            "a program call names the asking run (run)".to_string(),
        ))?;
        let run = require_worker_drives(&state, &caller, run_id).await?;
        if req.spec.project_id != Some(run.project) {
            return Err((
                StatusCode::FORBIDDEN,
                "a program call acts on the asking run's own project".into(),
            ));
        }
        let payload: weft_core::program::ProgramCallPayload = serde_json::from_value(req.spec.payload.clone())
            .map_err(|e| unreadable_payload(&state, &caller, &e))?;
        if payload.by != run_id {
            return Err((
                StatusCode::FORBIDDEN,
                "a program call's asker is the run that sends it".into(),
            ));
        }
        // The key names the call's step in the asking run, and the install
        // names what the call makes after it
        // (`weft_core::program::program_call_id`): a key of another run's
        // would reach what that run's calls made.
        // SYNC: the step's spelling <-> crates/weft-engine/src/context.rs (program_call)
        if !req.spec.dedup_key.as_deref().is_some_and(|key| key.starts_with(&format!("program_call/{run_id}/"))) {
            return Err((
                StatusCode::FORBIDDEN,
                "a program call is keyed to its step in the asking run (program_call/<run>/...)".into(),
            ));
        }
        if let Some(target) = payload.project {
            // The worker is pinned to its own project, so the scope check
            // every other resource goes through would refuse a sibling:
            // only the owner is compared here, and the action decides the rest.
            let owner = scope::lookup_project_tenant(&state.scope_cache, &state.pool, target).await?;
            if owner != run.tenant {
                return Err((StatusCode::FORBIDDEN, "a program call acts on projects of its own owner only".into()));
            }
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
                    "control-plane caller must name a project / run / signal so the task's \
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
    // The call's one write (one statement): everything before it read.
    let outcome = state.tasks.enqueue_dedup(new_task).await.map_err(|e| unavailable_or_internal(e.context("enqueue the task")))?;
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
        .wait_for_terminal(req.task_id, Duration::from_millis(req.wait_ms))
        .await
        .map_err(|e| unavailable_or_internal(e.context("wait for the task to end")))?;
    Ok(Json(TaskWaitTerminalResponse::from_outcome(outcome)))
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
    let digest: Option<String> = state
        .pool
        .scalar_opt("SELECT md5(project_json) FROM project WHERE id = $1", &[&project])
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
    if let Some(declared) = state.declared_infra.get(&(project, digest.clone())) {
        return Ok(Some(declared));
    }
    // Read with its own digest, so what is kept is filed under the
    // definition actually parsed, even if it changed since `digest`.
    let row: Option<(String, weft_core::stored::Version, String)> = state
        .pool
        .opt("SELECT project_json, project_json_version, md5(project_json) FROM project WHERE id = $1", &[&project])
        .await
        .map_err(definition_unavailable)?;
    let Some((project_json, version, digest)) = row else { return Ok(None) };
    let definition = weft_core::project::ProjectDefinition::STORED
        .decode(project_json.as_str(), version)
        .map_err(|e| internal(e.at(format_args!("project {project}'s program"))))?;
    let declared = Arc::new(weft_core::project::DeclaredInfra::of(&definition));
    state.declared_infra.put((project, digest), declared.clone());
    Ok(Some(declared))
}

fn definition_unavailable(e: db::Error) -> (StatusCode, String) {
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

/// A machine running `project_id`'s infra says how its units stand
/// changed: announce it, which wakes a supervisor that scales to zero to
/// look at the project's health (`lifecycle_command::LOOK_WAKE`). The
/// machine's token names the copy it runs, so it asks only about that
/// copy's project; a worker asks about its own, weft's own roles about
/// any. Unlike a person's look (the dispatcher's `infra::look`), it
/// records nothing: a health rule that gave up is armed again by what
/// the look sees changing, never by the asking.
pub async fn infra_look(
    State(state): State<Arc<BrokerState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<InfraLookRequest>,
) -> Resp<InfraLookResponse> {
    match crate::auth::verified_principal(&state, &headers).await? {
        weft_platform_traits::identity::Principal::InfraCopy { project, .. } if project == req.project_id => {}
        weft_platform_traits::identity::Principal::InfraCopy { .. } => {
            return Err((StatusCode::FORBIDDEN, "an infra machine asks for a look at its own project only".into()))
        }
        _ => {
            let caller = crate::auth::extract_identity(&state, &headers).await?;
            scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id).await?;
        }
    }
    weft_task_store::infra_command::ask_for_look(&state.pool, req.project_id)
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("announce the look")))?;
    Ok(Json(InfraLookResponse {}))
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
    let run = scope::require_run_id_scope(&state.scope_cache, &state.pool, &caller, req.run_id)
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
    let stored = state
        .infra
        .endpoint(run.project, req.infra.place(), instance, req.infra.endpoint())
        .await
        .map_err(|e| unavailable_or_internal(e.context("read the infra endpoint")))?;
    // A public endpoint hangs off the address the open internet reaches
    // now (a quick tunnel's moves when it is opened again), read only for
    // an endpoint that has one.
    let address = match stored {
        None => None,
        Some(weft_task_store::StoredEndpoint { url, public_path: None }) => Some(weft_core::infra::EndpointAddress { url, public_url: None }),
        Some(weft_task_store::StoredEndpoint { url, public_path: Some(path) }) => {
            let base = state.front_door().await.map_err(|e| unavailable_or_internal(e.context("read the install's outside address")))?;
            Some(weft_core::infra::EndpointAddress { url, public_url: Some(weft_core::infra::public_url(&base, &path)) })
        }
    };
    Ok(Json(InfraEndpointUrlResponse { address }))
}

/// `POST /v1/infra/baked`: what an infra node's copy saved for its baked
/// outputs, for a run of the node (`InfraReader::baked_outputs`). The run
/// is the caller's own, and the copy is the shared one or the run's own
/// instance's, like an endpoint's.
pub async fn infra_baked(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<weft_broker_client::protocol::InfraBakedRequest>,
) -> Resp<weft_broker_client::protocol::InfraBakedResponse> {
    require_worker(&caller)?;
    let run = scope::require_run_id_scope(&state.scope_cache, &state.pool, &caller, req.run_id)
        .await?;
    if req.instance.is_some() && req.instance != run.instance {
        return Err((
            StatusCode::FORBIDDEN,
            format!("{}'s copy belongs to another instance than the run asking for it; a run reads only its own instance's infra", req.place),
        ));
    }
    let saved = state
        .infra
        .baked_outputs(run.project, &req.place, req.instance.as_ref())
        .await
        .map_err(|e| unavailable_or_internal(e.context("read what an infra node baked")))?;
    Ok(Json(weft_broker_client::protocol::InfraBakedResponse { saved }))
}

/// `POST /v1/infra/bake`: see [`weft_broker_client::protocol::InfraBakeRequest`].
pub async fn infra_bake(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<weft_broker_client::protocol::InfraBakeRequest>,
) -> Resp<serde_json::Value> {
    require_worker(&caller)?;
    let run = scope::require_run_id_scope(&state.scope_cache, &state.pool, &caller, req.run_id)
        .await?;
    if req.instance.is_some() && req.instance != run.instance {
        return Err((StatusCode::FORBIDDEN, format!("{}'s copy belongs to another instance than the run saving it", req.place)));
    }
    let phase: Option<String> = state
        .pool
        .scalar_opt("SELECT phase FROM run WHERE run_id = $1", &[&req.run_id])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the run's phase")))?;
    if phase.as_deref() != Some(weft_core::context::Phase::InfraSetup.as_str()) {
        return Err((StatusCode::FORBIDDEN, "only an infra setup saves what an infra node bakes".into()));
    }
    let instance = req.instance.as_ref().map(|i| i.as_str());
    let saved = crate::lifecycle_writes::save_baked(&state.pool, run.project, &req.place, instance, &req.values)
        .await
        .map_err(|e| unavailable_or_internal(e.context("save what an infra node baked")))?;
    if !saved {
        return Err((StatusCode::CONFLICT, format!("{} has no infra copy here to save what it baked with", req.place)));
    }
    Ok(Json(serde_json::json!({})))
}

/// Worker fetches a project definition at run claim time,
/// keyed by `(project_id, definition_hash)`. The hash makes the
/// lookup content-addressed: callers always get back the EXACT
/// shape they asked for, regardless of what the project row's
/// current `running_definition_hash` says. That's load-bearing for
/// resumes after re-register: a suspended run must resume on
/// the shape it was started on, even if the user has edited and
/// re-registered the project in the meantime.
///
/// The history table `project_definition` (append-only, keyed on
/// `(project_id, definition_hash)`) holds every version a build
/// registered and every cut a run runs (kept with the run that went in
/// naming it, `cut_from` set): a run reads its program back by its hash
/// either way. A missing row means no version or run ever named this
/// hash for this project; that's a 404 (the caller's expected_hash is
/// genuinely invalid, not just stale).
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
    let row: Option<(String, weft_core::stored::Version)> = state
        .pool
        .opt(
            "SELECT project_json, project_json_version FROM project_definition \
             WHERE project_id = $1 AND definition_hash = $2",
            &[&req.project_id, &req.expected_hash],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a project definition")))?;
    let Some((project_json, project_json_version)) = row else {
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
        project_json_version,
        definition_hash: req.expected_hash,
    }))
}

// ---------- Connections ----------

/// The worker-caller prologue every connection verb shares: WHOSE
/// run this caller is acting for. The caller must be a worker,
/// and the run it names must be one it may act for.
///
/// The ownership rule itself lives in `require_run_id_scope`, which
/// every run-named verb goes through, so this adds only the
/// role gate: connections are a worker's business and nobody else's.
async fn worker_run_scope(
    state: &BrokerState,
    caller: &crate::auth::CallerIdentity,
    run_id: weft_core::RunId,
) -> Result<scope::RunScope, (StatusCode, String)> {
    if caller.role != Role::Worker {
        return Err((StatusCode::FORBIDDEN, "worker only".into()));
    }
    scope::require_run_id_scope(&state.scope_cache, &state.pool, caller, run_id).await
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
    let owner = worker_run_scope(&state, &caller, req.run_id).await?;
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
            keep_until_unix: Some(resolved.fresh_until.map_or(i64::MAX, |at| at.timestamp())),
            published_by: resolved.published_by,
        },
        weft_core::CredentialOwner::Platform => {
            // The row's service (already matched against the request)
            // keys the credential source; nothing here trusts a
            // caller-supplied name for anything but that equality.
            let name = crate::credential::single_value_name(&resolved.auth, &resolved.service)
                .map_err(internal)?;
            let key_req = crate::credential::KeyRequest {
                tenant,
                run_id: req.run_id,
                project_id: owner.project,
                node_id: req.node_id,
                frames: req.frames,
                node_type: req.node_type,
                service: resolved.service.clone(),
                auth: resolved.auth.clone(),
                replica: caller.replica.clone(),
                window: std::time::Duration::from_secs(req.expected_duration_secs),
            };
            // Its writes, if any, are one statement or one transaction
            // (`CredentialSource`); the store's resolve above wrote at
            // most a refresh, which a call sent again finds done.
            match state.credentials.resolve(&state.pool, &key_req).await.map_err(unavailable_or_internal)? {
                crate::credential::KeyResolution::Access { credential, relay_url } => {
                    let mut values = std::collections::BTreeMap::new();
                    values.insert(name.clone(), credential);
                    ResolveConnectionResponse {
                        values,
                        auth: resolved.auth,
                        identity: resolved.identity,
                        relay_url,
                        owner: resolved.owner,
                        // Leased per firing: never kept.
                        keep_until_unix: None,
                        published_by: resolved.published_by,
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
/// found. The run scope check keeps a worker from retiring another
/// tenant's credentials.
pub async fn release_connection(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<ReleaseConnectionRequest>,
) -> Resp<ReleaseConnectionResponse> {
    let tenant = worker_run_scope(&state, &caller, req.run_id).await?.tenant;
    for value in req.values.values() {
        // Closing is idempotent, so a call sent again after a value
        // closed repeats nothing.
        state.credentials.close(&state.pool, value, &tenant).await.map_err(unavailable_or_internal)?;
    }
    Ok(Json(ReleaseConnectionResponse {}))
}

/// A node publishes a connection to something it runs itself. The
/// store keys the row on (tenant, project, node, service), so a
/// second publish updates the first row rather than piling up.
///
/// Nothing the caller says about WHERE the row lands is trusted: the
/// project comes from the run's own row, so a worker cannot
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
    let owner = publisher_scope(&state, &caller, req.run_id, &mut req.node_id).await?;
    if req.spec.service != req.service {
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "publishing '{}' with the recipe for '{}'",
                req.service, req.spec.service
            ),
        ));
    }
    let connection = weft_access_store::publish_grant(
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
    Ok(Json(PublishAccessResponse { connection }))
}

/// The connection this node published for this service, if any. How a
/// node finds what it opened last time instead of asking the thing it
/// runs for its credentials a second time.
pub async fn published_access(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(mut req): Json<PublishedAccessRequest>,
) -> Resp<PublishedAccessResponse> {
    let owner = publisher_scope(&state, &caller, req.run_id, &mut req.node_id).await?;
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

/// Where a publish is allowed to land: the run's own tenant and
/// project. `node_id` is trimmed in place, so the caller's own field
/// is the key the row is written under.
///
/// Authorization first, then the shape of the request: a caller with
/// no business here should hear that, not a complaint about its
/// arguments.
async fn publisher_scope(
    state: &BrokerState,
    caller: &crate::auth::CallerIdentity,
    run_id: weft_core::RunId,
    node_id: &mut String,
) -> Result<scope::RunScope, (StatusCode, String)> {
    let owner = worker_run_scope(state, caller, run_id).await?;
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
    // One transaction.
    let synced = crate::lifecycle_writes::sync_ownership(&state.pool, &req, state.supervisor_booking.as_ref())
        .await
        .map_err(unavailable_or_internal)?;
    Ok(Json(synced))
}

/// Replace a project's health rules' state, for the supervisor holding
/// its lease (410 otherwise).
pub async fn supervisor_set_health_state(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorSetHealthStateRequest>,
) -> Resp<SupervisorSetHealthStateResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id).await?;
    // One transaction.
    let outcome = crate::lifecycle_writes::set_health_state(&state.pool, &req).await.map_err(unavailable_or_internal)?;
    fenced_to_http(outcome, || format!("set_health_state(project={})", req.project_id))?;
    Ok(Json(SupervisorSetHealthStateResponse {}))
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
        .map_err(unavailable_or_internal)?;
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
    let err = |e: db::Error| unavailable_or_internal(anyhow::Error::from(e).context("read a project's copies"));
    let project: Option<(String, bool)> = state
        .pool
        .opt(
            &format!(
                "SELECT md5(p.project_json), {owns} FROM project p WHERE p.id = $1",
                owns = weft_broker_client::lifecycle_command::owns_project_predicate("$2", "p.id"),
            ),
            &[&req.project, &req.replica],
        )
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
            let rows: std::collections::HashSet<String> = state
                .pool
                .scalars("SELECT copy_id FROM infra_node WHERE project_id = $1", &[&req.project])
                .await
                .map_err(err)?
                .into_iter()
                .collect();
            req.copies
                .iter()
                .filter(|c| !rows.contains(&c.copy_id) && declared.gone_copy(c))
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
    let rows = state
        .pool
        .query(
            &format!(
                "SELECT node_id, instance_id, copy_id, status, applied_spec_hash, applied_at_unix, \
                        endpoints_json, public_paths_json, doors_json, keep_disks_json, {} AS units, \
                        health_rules_json, health_rules_json_version \
                 FROM infra_node n WHERE project_id = $1 ORDER BY node_id, instance_id NULLS FIRST",
                weft_broker_client::protocol::units_of_copy_sql("n"),
            ),
            &[&req.project_id],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a project's infra nodes")))?;
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
            .try_get::<_, Option<String>>("applied_spec_hash")
            .map_err(|e| internal(anyhow::anyhow!("decode applied_spec_hash: {e}")))?;
        let applied_at_unix: Option<i64> = r
            .try_get("applied_at_unix")
            .map_err(|e| internal(anyhow::anyhow!("decode applied_at_unix: {e}")))?;
        // A JSON column that must decode as `T`, naming the column when
        // it does not (schema drift surfaces as a 500, never a default).
        fn column<T: serde::de::DeserializeOwned>(
            r: &Row,
            name: &str,
            node_id: &str,
        ) -> Result<T, (StatusCode, String)> {
            let db::JsonText(v) = r.try_get::<_, db::JsonText<serde_json::Value>>(name).map_err(|e| internal(anyhow::anyhow!("decode {name}: {e}")))?;
            serde_json::from_value(v)
                .map_err(|e| internal(anyhow::anyhow!("infra_node.{name} for node='{node_id}' does not decode: {e}")))
        }
        let addresses = weft_broker_client::protocol::AppliedEndpoints {
            urls: column(&r, "endpoints_json", &node_id)?,
            public_paths: column(&r, "public_paths_json", &node_id)?,
            doors: column(&r, "doors_json", &node_id)?,
        };
        let keep_disks: Vec<String> = column(&r, "keep_disks_json", &node_id)?;
        // NULL: the node wrote no rules (it runs the built-in ones).
        let health_rules = r
            .try_get::<_, Option<db::JsonText<serde_json::Value>>>("health_rules_json")
            .map_err(|e| internal(anyhow::anyhow!("decode health_rules_json: {e}")))?
            .map(|rules| rules.0);
        let health_rules_version: weft_core::stored::Version = r
            .try_get("health_rules_json_version")
            .map_err(|e| internal(anyhow::anyhow!("decode health_rules_json_version: {e}")))?;
        let health_rules = health_rules
            .map(|rules| weft_core::infra::health::STORED_RULES.decode(rules, health_rules_version))
            .transpose()
            .map_err(|e| internal(e.at(format_args!("infra node '{node_id}'"))))?;
        let db::Json(units) = r.try_get("units").map_err(|e| internal(anyhow::anyhow!("decode units: {e}")))?;
        let units = weft_broker_client::protocol::decode_units(units, req.project_id, &node_id).map_err(internal)?;
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
            health_rules,
        });
    }
    Ok(Json(SupervisorInfraNodesResponse { nodes }))
}

/// Where each health rule of the project's copies stands.
pub async fn supervisor_health_state(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorHealthStateRequest>,
) -> Resp<SupervisorHealthStateResponse> {
    require_supervisor(&caller)?;
    scope::require_project_owned_by(&state.scope_cache, &state.pool, &caller, req.project_id)
        .await?;
    Ok(Json(crate::lifecycle_writes::health_state(&state.pool, req.project_id).await.map_err(unavailable_or_internal)?))
}

pub async fn supervisor_claim_command(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SupervisorClaimCommandRequest>,
) -> Resp<SupervisorClaim> {
    require_supervisor(&caller)?;
    // Which command is next, and what its claim marks:
    // `lifecycle_writes::claim_next`.
    //
    // Held: with nothing waiting, the request sleeps until a command is
    // issued anywhere (the row's own trigger announces it) and looks
    // again. Any issue wakes it, not only one for a project this process
    // owns: ownership can move during the hold, and the look is one
    // indexed read. A person cancelling one of the commands this process
    // runs (`cancel:<id>`, from the trigger on `cancel_requested`) wakes
    // it too, and comes back as the cancel.
    //
    // A wake that finds nothing for this process may be a command for a
    // project nobody owns yet (ownership is taken on the supervisors'
    // own ticks): the answer then says so, and the process takes ownership
    // at once instead of on its next tick. Only after a wake, so a
    // supervisor woken by a command it cannot take yet does not spin.
    let deadline = held_until(req.wait_ms);
    let mut heard = state.signals.subscribe();
    let mut woken_once = false;
    let cancel_heard = |channel: &str, payload: &str| {
        channel == INFRA_COMMAND_CHANNEL
            && matches!(
                InfraCommandSignal::parse(payload),
                Some(InfraCommandSignal::Cancel { id }) if req.busy_commands.contains(&id) && !req.cancelling.contains(&id)
            )
    };
    // Each look is a read or one statement (`claim_next`), and a claim
    // that took a command answers at once, so a call that fails had
    // written at most commands given up, which a call sent again finds
    // done.
    loop {
        if !req.busy_commands.is_empty() {
            let cancels = crate::lifecycle_writes::cancels_for(&state.pool, &req.busy_commands, &req.cancelling)
                .await
                .map_err(unavailable_or_internal)?;
            if !cancels.is_empty() {
                return Ok(Json(SupervisorClaim::Cancel { commands: cancels }));
            }
        }
        let next = crate::lifecycle_writes::claim_next(
            &state.pool,
            &req.claimer_replica,
            &req.busy_commands,
        )
        .await
        .map_err(unavailable_or_internal)?;
        if let Some(command) = next {
            return Ok(Json(SupervisorClaim::Command(command)));
        }
        if woken_once && crate::lifecycle_writes::unowned_work_waiting(&state.pool).await.map_err(unavailable_or_internal)? {
            return Ok(Json(SupervisorClaim::UnownedWork));
        }
        let woken = heard
            .woken_before(deadline, |channel, payload| ISSUED_WAKE.hears(channel, payload) || cancel_heard(channel, payload))
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
    .map_err(unavailable_or_internal)?
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
        .map_err(unavailable_or_internal)?;
    fenced_to_http(outcome, || {
        format!(
            "set_status(project={}, node={}, unit={:?}, cmd={:?})",
            req.project_id, req.node_id, req.unit, req.command_id
        )
    })?;
    Ok(Json(SupervisorSetStatusResponse {}))
}

/// Record what a change of a copy waits on, for `weft status`: the
/// supervisor writes it as an apply's units come up, and as a stop or a
/// terminate hands the copy to its host. Fenced like `set_status` under
/// a command.
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
        .map_err(unavailable_or_internal)?;
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
    health_rules: Option<&[weft_core::infra::health::HealthRule]>,
    units: &std::collections::BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    command_id: i64,
    owner: &str,
    row: ApplyRowState,
) -> Result<(), (StatusCode, String)> {
    // Each as its JSON text: the names in them are the ones the node's
    // spec wrote.
    let keep_disks_json = db::JsonText(keep_disks);
    let health_rules_json = health_rules.map(db::JsonText);
    let endpoints_json = db::JsonText(&row.addresses.urls);
    let public_paths_json = db::JsonText(&row.addresses.public_paths);
    let doors_json = db::JsonText(&row.addresses.doors);
    let notes_json = db::JsonText(&row.notes);
    // The INSERT pulls its values FROM the caller's still-claimed
    // apply command so the ownership check and the write share one
    // row snapshot. Every variable is a bind: no SQL built by string
    // interpolation. `applied_at_unix` uses the DB clock (consistent
    // with every other timestamp write in this file), gated on the
    // bound `$6` flag via CASE. A row still `terminating` is a copy whose
    // terminate never finished, which this apply finished and replaces:
    // what it saved for its baked outputs is the dead copy's, so it goes
    // (a bake only writes the outputs its setup sent on, over what is
    // there). The copy's units are written in the same transaction, once
    // the row is ([`crate::lifecycle_writes::write_units`]).
    // One transaction: the row and its units commit together or not at
    // all, and the read after a write that matched nothing wrote nothing.
    let unreached = |what: &'static str| move |e: db::Error| unavailable_or_internal(anyhow::Error::from(e).context(what));
    let mut conn = state.pool.get().await.map_err(unreached("begin the apply row's transaction"))?;
    let tx = conn.transaction().await.map_err(unreached("begin the apply row's transaction"))?;
    let sql = format!(
        "INSERT INTO infra_node \
         (project_id, node_id, instance_id, copy_id, status, \
          failure_stage, failure_message, applied_spec_hash, \
          applied_at_unix, endpoints_json, public_paths_json, doors_json, keep_disks_json, health_rules_json_version, \
          notes_json, waiting_on, provisioning_since_unix, health_rules_json) \
         SELECT $1, $2, $13, $3, $4, NULL, NULL, $5, \
                CASE WHEN $6 THEN EXTRACT(EPOCH FROM NOW())::BIGINT ELSE NULL END, \
                $7, $12, $14, $8, $9, $15, NULL, \
                CASE WHEN $6 THEN NULL ELSE EXTRACT(EPOCH FROM NOW())::BIGINT END, $16 \
         FROM infra_lifecycle_command \
         WHERE id = $10 \
           AND project_id = $1 \
           AND node_id = $2 \
           AND instance_id IS NOT DISTINCT FROM $13 \
           AND verb = 'apply' \
           AND completed_at_unix IS NULL \
           AND {owns} \
         ON CONFLICT (project_id, node_id, instance_id) DO UPDATE SET \
            baked_json         = CASE WHEN infra_node.status = 'terminating' \
                                      THEN '{{}}' ELSE infra_node.baked_json END, \
            copy_id            = EXCLUDED.copy_id, \
            status             = EXCLUDED.status, \
            failure_stage      = NULL, \
            failure_message    = NULL, \
            applied_spec_hash  = EXCLUDED.applied_spec_hash, \
            applied_at_unix    = EXCLUDED.applied_at_unix, \
            endpoints_json     = EXCLUDED.endpoints_json, \
            public_paths_json  = EXCLUDED.public_paths_json, \
            doors_json         = EXCLUDED.doors_json, \
            notes_json         = EXCLUDED.notes_json, \
            keep_disks_json = EXCLUDED.keep_disks_json, \
            health_rules_json  = EXCLUDED.health_rules_json, \
            health_rules_json_version = EXCLUDED.health_rules_json_version, \
            waiting_on         = NULL, \
            provisioning_since_unix = EXCLUDED.provisioning_since_unix",
        owns = weft_broker_client::lifecycle_command::owns_project_predicate("$11", "$1"),
    );
    let written = tx
        .execute(
            &sql,
            &[
                &project_id,
                &node_id,
                &copy_id,
                &row.status,
                &row.applied_spec_hash,
                &row.stamp_applied_at,
                &endpoints_json,
                &keep_disks_json,
                &weft_core::infra::health::STORED_RULES.version(),
                &command_id,
                &owner,
                &public_paths_json,
                &instance.map(|m| m.as_str()),
                &doors_json,
                &notes_json,
                &health_rules_json,
            ],
        )
        .await
        .map_err(unreached("write the apply row"))?;
    if written == 0 {
        drop(tx);
        drop(conn);
        let outcome = crate::lifecycle_writes::stale_answer(&state.pool, owner, project_id)
            .await
            .map_err(unavailable_or_internal)?;
        fenced_to_http(outcome, || format!("{op}(command id={command_id})"))?;
        return Ok(());
    }
    crate::lifecycle_writes::write_units(&tx, project_id, node_id, instance, units).await.map_err(unavailable_or_internal)?;
    tx.commit().await.map_err(unreached("commit the apply row"))?;
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
    write_apply_row(
        &state,
        "set_applied",
        req.project_id,
        &req.node_id,
        req.instance.as_ref(),
        &req.copy_id,
        &req.keep_disks,
        req.health_rules.as_deref(),
        &req.units,
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
    write_apply_row(
        &state,
        "set_provisioning",
        req.project_id,
        &req.node_id,
        req.instance.as_ref(),
        &req.copy_id,
        &req.keep_disks,
        req.health_rules.as_deref(),
        &req.units,
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
/// command (`deactivate` | `reactivate`). Used by the health rules'
/// actions when the supervisor decides the triggers reading broken infra
/// go down, or come back once it is healthy. Side-channel
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
    // verb here.
    let (verb, spec_json) = req.spec.into_row_columns();
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
            spec_json: spec_json.as_ref(),
            source_hash: None,
            issued_by_replica,
        },
    )
    .await
    .map_err(unavailable_or_internal)?
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
    let row: Option<Row> = state
        .pool
        .opt("SELECT infra_image_tags_json FROM project WHERE id = $1", &[&id])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a project's image tags")))?;
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
            let db::JsonText(value) = r
                .try_get::<_, db::JsonText<serde_json::Value>>("infra_image_tags_json")
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

/// `POST /v1/infra/pushed`: what changed of the values one infra copy's
/// node handed weft (`weft_core::infra::bake::PushedValues`), from the
/// agent beside that copy (`weft_platform_traits::unit_agent::VALUES_PATH`).
/// The agent's identity names its copy, and that copy is the only one it
/// can write.
pub async fn infra_pushed(
    State(state): State<Arc<BrokerState>>,
    headers: axum::http::HeaderMap,
    Json(values): Json<weft_core::infra::bake::PushedValues>,
) -> Resp<serde_json::Value> {
    let weft_platform_traits::identity::Principal::InfraCopy { project, copy_id, .. } = crate::auth::verified_principal(&state, &headers).await? else {
        return Err((StatusCode::FORBIDDEN, "only the agent beside an infra copy pushes that copy's values".into()));
    };
    weft_access_store::write_pushed_values(&state.pool, project, &copy_id, &values).await.map_err(|e| {
        match e.downcast_ref::<weft_access_store::AccessError>() {
            // The values name nothing the node handed weft: the infra's fault.
            Some(weft_access_store::AccessError::Invalid(why)) => (StatusCode::UNPROCESSABLE_ENTITY, why.clone()),
            _ => unavailable_or_internal(e.context("write what an infra said changed")),
        }
    })?;
    Ok(Json(serde_json::json!({})))
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
    let source_hash = crate::lifecycle_writes::place_hash_of_run(&state.pool, req.project_id, req.run_id, &req.node_id)
        .await
        .map_err(unavailable_or_internal)?;
    let command_id = crate::lifecycle_writes::issue_command(
        &state.pool,
        &crate::lifecycle_writes::IssuedCommand {
            tenant_id: &project_tenant,
            project_id: req.project_id,
            node_id: Some(&req.node_id),
            copies: &weft_core::instance::Copies::of(req.instance.clone()),
            verb: weft_broker_client::protocol::InfraLifecycleVerb::Apply,
            spec_json: Some(&req.spec_json),
            source_hash: source_hash.as_deref(),
            issued_by_replica,
        },
    )
    .await
    .map_err(unavailable_or_internal)?
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
    let deadline = held_until(req.wait_ms);
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
    let row: Option<Row> = state
        .pool
        .opt(
            "SELECT completed_at_unix, outcome, outcome_message, project_id \
             FROM infra_lifecycle_command WHERE id = $1",
            &[&req.command_id],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read an apply command")))?;
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
        .try_get::<_, Option<i64>>("completed_at_unix")
        .map_err(|e| internal(anyhow::anyhow!("decode completed_at_unix: {e}")))?;
    let outcome_str: Option<String> = r
        .try_get::<_, Option<String>>("outcome")
        .map_err(|e| internal(anyhow::anyhow!("decode outcome: {e}")))?;
    let message: Option<String> = r
        .try_get::<_, Option<String>>("outcome_message")
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
    let unreached = |what: &'static str| move |e: db::Error| unavailable_or_internal(anyhow::Error::from(e).context(what));
    let mut conn = state.pool.get().await.map_err(unreached("begin remove_node's transaction"))?;
    let tx = conn.transaction().await.map_err(unreached("begin remove_node's transaction"))?;
    let owns: bool = tx
        .scalar(
            &format!("SELECT {owns}", owns = weft_broker_client::lifecycle_command::owns_project_predicate("$1", "$2")),
            &[&req.replica, &req.project_id],
        )
        .await
        .map_err(unreached("remove_node ownership check"))?;
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
    let removed = tx
        .execute(
            "DELETE FROM infra_node \
              WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
            &[&req.project_id, &req.node_id, &instance],
        )
        .await
        .map_err(unreached("delete infra_node"))?;
    tx.execute(
        "DELETE FROM infra_event \
          WHERE project_id = $1 AND node_id = $2 AND instance_id IS NOT DISTINCT FROM $3",
        &[&req.project_id, &req.node_id, &instance],
    )
    .await
    .map_err(unreached("delete infra_event"))?;
    // Cancel any not-yet-completed lifecycle commands aimed at exactly
    // this copy (this node, this owner) by stamping a completion, except
    // the terminate doing the removing, which completes on its own.
    // Commands for the whole project (`node_id IS NULL`) or for every
    // copy are left in place: they still have other copies to act on.
    use weft_broker_client::protocol::LifecycleOutcome;
    tx.execute(
        "UPDATE infra_lifecycle_command \
            SET completed_at_unix = EXTRACT(EPOCH FROM NOW())::BIGINT, \
                outcome = $3, \
                outcome_message = 'node removed by remove_node' \
          WHERE project_id = $1 AND node_id = $2 \
            AND NOT every_copy AND instance_id IS NOT DISTINCT FROM $4 \
            AND completed_at_unix IS NULL AND id <> $5",
        &[&req.project_id, &req.node_id, &LifecycleOutcome::Cancelled.as_str(), &instance, &req.command_id],
    )
    .await
    .map_err(unreached("cancel pending commands"))?;
    // A connection this node published opens the very thing being
    // removed, so it goes with it: its credentials would otherwise
    // name a database that no longer exists, and nothing downstream
    // could use or clean it.
    let published = weft_access_store::delete_published_grants(
        &tx,
        &tenant,
        req.project_id,
        Some((&req.node_id, req.instance.as_ref())),
    )
    .await
    .map_err(|e| unavailable_or_internal(e.context("delete published connections")))?;
    tx.commit().await.map_err(unreached("commit remove_node"))?;
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
        removed: removed > 0,
    }))
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
    let rows: Vec<(Option<String>, Option<String>, bool)> = state
        .pool
        .all(
            &format!(
                "SELECT node_id, instance_id, every_copy FROM infra_lifecycle_command \
                 WHERE project_id = $1 AND completed_at_unix IS NULL AND verb IN ({verbs})",
                verbs = weft_broker_client::lifecycle_command::SUPERVISOR_VERBS_SQL,
            ),
            &[&req.project_id],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read the project's commands in flight")))?;
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
    let row: Option<Row> = state
        .pool
        .opt("SELECT tenant_id FROM infra_lifecycle_command WHERE id = $1", &[&req.command_id])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a command's tenant")))?;
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
        .map_err(unavailable_or_internal)?;
    fenced_to_http(outcome, || format!("command_complete(id={})", req.command_id))?;
    Ok(Json(SupervisorCommandCompleteResponse {}))
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
    let out = crate::held_signals::signals_held(&state.pool, req.project).await.map_err(unavailable_or_internal)?;
    Ok(Json(SignalListHeldResponse { rows: out }))
}

/// Where an event of a signal goes (`SignalFireTarget`): its project, and
/// the address of the project's front, which the listener hands an
/// entry's event to.
pub async fn signal_fire_target(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<weft_broker_client::protocol::SignalFireTargetRequest>,
) -> Resp<Option<weft_broker_client::protocol::SignalFireTarget>> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    let row: Option<(uuid::Uuid, bool, Option<String>)> = state
        .pool
        .opt(
            "SELECT s.project_id, s.is_resume, p.api_address FROM signal s LEFT JOIN project p ON p.id = s.project_id WHERE s.token = $1",
            &[&req.token],
        )
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("read a signal's project")))?;
    let Some((project_id, is_resume, address)) = row else { return Ok(Json(None)) };
    // Only an address that serves: one the project's front could not open
    // says why instead.
    let address = match address.as_deref().map(serde_json::from_str::<weft_core::projects::ProjectAddress>).transpose() {
        Ok(Some(weft_core::projects::ProjectAddress::Serving { url })) => Some(url),
        Ok(Some(weft_core::projects::ProjectAddress::Unavailable { .. }) | None) => None,
        Err(e) => {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("project {project_id}'s stored address does not read back: {e}")));
        }
    };
    Ok(Json(Some(weft_broker_client::protocol::SignalFireTarget { project_id, is_resume, address })))
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
    let row = crate::held_signals::signal_held(&state.pool, &req.token).await.map_err(unavailable_or_internal)?;
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
        .map_err(unavailable_or_internal)?;
    Ok(Json(SignalWriteKindStateResponse { written }))
}

/// One look of a holder: keep, report, take (see `SignalHoldRequest`).
/// Bound to the calling replica, so a holder only ever keeps or takes in
/// its own name.
pub async fn signal_hold(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalHoldRequest>,
) -> Resp<SignalHoldResponse> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    require_replica_matches(&caller, &req.replica)?;
    let out = crate::held_signals::hold(&state.pool, &req.replica, &req.holding, req.room, &req.want)
        .await
        .map_err(unavailable_or_internal)?;
    Ok(Json(out))
}

/// The listener or a holder gave up on a signal for good (see
/// `SignalGiveUpRequest`). A caller with no replica gives up only a
/// signal nobody holds.
pub async fn signal_give_up(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalGiveUpRequest>,
) -> Resp<serde_json::Value> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    let written =
        crate::held_signals::give_up(&state.pool, &req.token, req.generation, &req.reason, caller.replica.as_deref())
            .await
            .map_err(unavailable_or_internal)?;
    if !written {
        tracing::info!(
            target: "weft_broker::held", token = %req.token, generation = req.generation,
            "a give-up wrote nothing: the signal was registered again, already given up, or held by another"
        );
    }
    Ok(Json(serde_json::json!({})))
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
    crate::held_signals::set_holds(&state.pool, &req.token, req.holds).await.map_err(unavailable_or_internal)?;
    Ok(Json(serde_json::json!({})))
}

/// A holder's check going off (`weft_platform_traits::HeldCheck`): the
/// holder stopped pushing it ahead of itself, so it is taken for dead and
/// every claim it holds is freed for the others. Mounted behind the guard
/// that lets only weft itself in (the alarm calls with the core account).
/// Answered 200 once handled, whatever was freed, so the alarm does not
/// call again. A check in a shape this broker does not read (queued by
/// the weft before an update) is answered as every call of another shape is,
/// which the alarm takes as a call that can never be taken
/// (`weft_platform_traits::alarm::WakeRefusal::of_status`). A database
/// that fails answers 500, and the alarm tries again.
pub async fn held_check(
    State(state): State<Arc<BrokerState>>,
    Json(call): Json<weft_platform_traits::WakeCall<weft_platform_traits::HeldCheck>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let check = call.body;
    let freed = crate::held_signals::release(&state.pool, &check.replica).await.map_err(unavailable_or_internal)?;
    if freed > 0 {
        tracing::warn!(
            target: "weft_broker::held", replica = %check.replica, generation = check.generation, freed,
            "a holder stopped pushing its check; its claims are freed"
        );
    }
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
    crate::held_signals::release(&state.pool, &req.replica).await.map_err(unavailable_or_internal)?;
    Ok(Json(serde_json::json!({})))
}

/// A holder saying its load, letting go of what it hands over (see
/// `SignalHolderLoadRequest`). Bound to the calling replica, so a holder
/// only ever speaks for itself.
pub async fn signal_holder_load(
    State(state): State<Arc<BrokerState>>,
    AuthedCaller(caller): AuthedCaller,
    Json(req): Json<SignalHolderLoadRequest>,
) -> Resp<serde_json::Value> {
    if caller.role != Role::Listener {
        return Err((StatusCode::FORBIDDEN, "listener only".into()));
    }
    require_replica_matches(&caller, &req.replica)?;
    use crate::held_signals::{HandTo, Reported};
    let to = if req.spare_only { HandTo::Spare } else { HandTo::SpareOrAny };
    match crate::held_signals::report_load(&state.pool, &req.replica, req.cpu_percent, req.room, &req.let_go, to)
        .await
        .map_err(unavailable_or_internal)?
    {
        Reported::Said { handed, to } => {
            tracing::info!(
                target: "weft_broker::held", replica = %req.replica, cpu_percent = req.cpu_percent, room = req.room, handed,
                spare = ?to, "a holder said its load"
            );
            Ok(Json(serde_json::json!({})))
        }
        Reported::NothingHeld => Err((
            StatusCode::CONFLICT,
            "this holder claims none of the signals it hands over any more; nothing was written, and it looks again before it says its load".into(),
        )),
        Reported::NoSpare => Err((
            StatusCode::CONFLICT,
            "no spare is free to take what this holder hands over (another holder handed it work first); nothing was written, and it looks again to take its signals back".into(),
        )),
    }
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
    let row: Option<Option<String>> = state
        .pool
        .scalar_opt("SELECT tenant_id FROM task WHERE id = $1", &[&task_id])
        .await
        .map_err(|e| unavailable_or_internal(anyhow::Error::from(e).context("task owner lookup")))?;
    let owner = row.flatten().ok_or((
        StatusCode::NOT_FOUND,
        format!("unknown task {task_id}"),
    ))?;
    scope::require_tenant_in_scope(caller, &owner)
}

/// An access store error as this surface answers it: a database that
/// could not be reached as [`unavailable_or_internal`] answers it (so the
/// same promises hold: the store call is a read, one write, or writes
/// that repeat harmlessly), anything else through the store's own mapping
/// (`client_error`), so the broker and the dispatcher cannot drift on
/// what a store failure looks like.
// SYNC: store_err <-> crates/weft-dispatcher/src/api/mod.rs (access_store_error)
pub(crate) fn store_err(e: anyhow::Error) -> (StatusCode, String) {
    store_answer(e, Written::Nothing)
}

/// [`store_err`] for a store call that may commit part of its work before
/// it fails (as [`unavailable_after_writes_or_internal`]).
pub(crate) fn store_err_after_writes(e: anyhow::Error) -> (StatusCode, String) {
    store_answer(e, Written::Maybe)
}

fn store_answer(e: anyhow::Error, written: Written) -> (StatusCode, String) {
    if let Some(answer) = database_unreached(&e, written) {
        return answer;
    }
    let (status, message) = weft_access_store::client_error(e);
    (StatusCode::from_u16(status).expect("store status codes are valid"), message)
}

/// A failure that is OURS, not the caller's: logged here with its
/// cause chain, and answered opaquely.
///
/// Opaque because the callers on this surface include workers, which
/// run tenant-authored code. A database error's Display names tables and
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
/// again will work once it can), with [`NOT_DONE`] when no connection
/// could even be had, so nothing ran and a write may be sent again; 500
/// for anything else (a row that does not decode fails the same way every
/// time). The worker's journal client sends a write again on a 503 that
/// did nothing, waits out any 503 on a read, and fails the run otherwise.
///
/// [`NOT_DONE`] is only true when nothing of the call committed before
/// `e`, and a missing connection says so only about the statement that
/// wanted one. So `e` is one of: an error of a read, raised before the
/// call wrote anything; an error of the call's one write (a single
/// statement, or a single transaction, which rolls back whole when it
/// fails); or an error after writes that sending the whole call again
/// repeats harmlessly. A call that may have committed something else
/// first maps through [`unavailable_after_writes_or_internal`]. (A
/// statement on a connection the call already holds never meets a
/// missing connection: the pool is only asked at the acquire.)
// SYNC: 503 = ask again <-> crates/weft-broker-client/src/client.rs broker_unavailable, turned_away
pub(crate) fn unavailable_or_internal(e: anyhow::Error) -> (StatusCode, String) {
    failure_answer(e, Written::Nothing)
}

/// [`unavailable_or_internal`] for a call that may have committed part
/// of its work before `e`: a missing connection is answered "ask again"
/// like any unreachable database, never [`NOT_DONE`], since sending the
/// call again would repeat what committed.
pub(crate) fn unavailable_after_writes_or_internal(e: anyhow::Error) -> (StatusCode, String) {
    failure_answer(e, Written::Maybe)
}

/// What a failed call may have committed before its error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Written {
    Nothing,
    Maybe,
}

/// The answer to a call that failed on `e`, having committed `written`
/// before it.
fn failure_answer(e: anyhow::Error, written: Written) -> (StatusCode, String) {
    database_unreached(&e, written).unwrap_or_else(|| internal(e))
}

/// The answer to a call that failed on `e` because the database could
/// not be reached, having committed `written` before it; `None` when the
/// database was reached and `e` is some other failure.
fn database_unreached(e: &anyhow::Error, written: Written) -> Option<(StatusCode, String)> {
    if let Some(why) = db::database_no_connection(e) {
        // Said with every other call that got no connection, as what kept
        // it (a busy pool, a database that does not answer), once a minute
        // at most rather than once per call turned away. Either way not
        // one statement ran, so the caller waits and sends it again.
        weft_task_store::throttle::no_connection(why);
        tracing::debug!(target: "weft_broker", ?why, "no connection to the database could be had: {e:#}");
        if written == Written::Nothing {
            return Some((StatusCode::SERVICE_UNAVAILABLE, NOT_DONE.into()));
        }
    } else if db::database_unreachable(e) {
        tracing::warn!(target: "weft_broker", "could not reach the database: {e:#}");
    } else {
        return None;
    }
    Some((StatusCode::SERVICE_UNAVAILABLE, "the database connection dropped, or the database is unavailable; ask again".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a statement that got no connection fails with, for `why`.
    fn no_connection(why: db::NoConnection) -> anyhow::Error {
        anyhow::Error::from(db::Error::NoConnection { why, refused: None })
    }

    #[test]
    fn only_an_unreachable_database_asks_the_caller_again() {
        let pool_gone = no_connection(db::NoConnection::Busy).context("run lookup");
        assert_eq!(unavailable_or_internal(pool_gone).0, StatusCode::SERVICE_UNAVAILABLE);
        let bad_row = anyhow::Error::from(db::Error::NoRow);
        assert_eq!(unavailable_or_internal(bad_row).0, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(unavailable_or_internal(anyhow::anyhow!("undecodable row")).0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// "Nothing was done" is said only by a call that wrote nothing before
    /// the connection it could not get; one that may have committed part
    /// of its work is told to ask again without that promise, and a
    /// failure that is not the database's reach stays a 500 either way.
    #[test]
    fn only_a_call_that_wrote_nothing_says_nothing_was_done() {
        let busy = || no_connection(db::NoConnection::Busy).context("a later step");
        assert_eq!(failure_answer(busy(), Written::Nothing), (StatusCode::SERVICE_UNAVAILABLE, NOT_DONE.to_string()));
        let (status, body) = failure_answer(busy(), Written::Maybe);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(body, NOT_DONE, "a call that may have committed never promises nothing was done");
        let (status, body) = failure_answer(no_connection(db::NoConnection::Closed), Written::Maybe);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(body, NOT_DONE);
        assert_eq!(failure_answer(anyhow::Error::from(db::Error::NoRow), Written::Maybe).0, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// A store call that could not get a connection waits like any other
    /// call on the same promise; a store refusal keeps the store's own
    /// answer, and any other failure stays the store's opaque 500.
    #[test]
    fn a_store_call_without_a_connection_waits_and_its_refusals_keep_their_answer() {
        let busy = || no_connection(db::NoConnection::Busy).context("resolve a connection");
        assert_eq!(store_err(busy()), (StatusCode::SERVICE_UNAVAILABLE, NOT_DONE.to_string()));
        let (status, body) = store_err_after_writes(busy());
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_ne!(body, NOT_DONE, "a store call that may have committed never promises nothing was done");
        assert_eq!(store_err(anyhow::Error::from(weft_access_store::AccessError::NotFound)).0, StatusCode::NOT_FOUND);
        assert_eq!(store_err_after_writes(anyhow::Error::from(weft_access_store::AccessError::NotFound)).0, StatusCode::NOT_FOUND);
        let (status, body) = store_err(anyhow::Error::from(db::Error::NoRow).context("read a grant"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body.contains("grant"), "a store failure answers opaquely: {body}");
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
        // A task naming a project in one tenant and a run in another is
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
