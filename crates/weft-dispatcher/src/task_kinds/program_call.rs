//! `program_call` task: one call a program makes on its own project
//! (`weft_core::program::ProgramCall`). The broker enqueued it for the
//! asking run, pinned to that run's project; a dispatcher pod carries it
//! out here, through the same functions the CLI and the editor reach, and
//! the worker reads the answer off the task.
//!
//! A call that takes something down never reaches the run that asked: the
//! take-down engines take the asker as the one run to leave alone. With
//! `StopSelf::Include`, and only when the asker is among what the call
//! reaches, the asker is cancelled here once the take-down is queued, and
//! the answer says so (`stops_asker`), so the worker waits for its own
//! cancel instead of running on.
//!
//! A replayed run never issues a call twice: the worker journals each
//! answer (`ctx.run`) and a replay reads it back. A call that did run but
//! whose answer was lost is issued again, so every call here is safe to
//! repeat: starting a running copy, taking down what is already down,
//! activating what is active, forgetting what is gone all answer without
//! doing anything.

use anyhow::Result;
use async_trait::async_trait;
use axum::http::StatusCode;
use serde_json::Value;

use weft_core::activation::ActivationScope;
use weft_core::member::{Copies, MemberId};
use weft_core::program::{
    ConnectionsForgotten, CostFilter, CostRecord, InfraCopy, InfraStartAnswer, MemberHoldings, MemberTrigger, ProgramCall,
    ProgramCallOutcome, ProgramCallPayload, TokensRevoked,
};
use weft_core::running_policy::DeactivateSpec;
use weft_core::{Color, StopSelf};
use weft_task_store::executor::TaskExecutor;
use weft_task_store::tasks::Task;

use crate::state::DispatcherState;

pub struct ProgramCallExecutor;

type CallError = (StatusCode, String);

#[async_trait]
impl TaskExecutor<DispatcherState> for ProgramCallExecutor {
    async fn execute(&self, state: &DispatcherState, task: &Task) -> Result<Value> {
        let payload: ProgramCallPayload = serde_json::from_value(task.payload.clone())?;
        let project_id = task
            .project_id
            .ok_or_else(|| anyhow::anyhow!("program_call task names no project"))?;
        let outcome = run_call(state, project_id, &payload)
            .await
            .map_err(|(_, message)| anyhow::anyhow!("{message}"))?;
        Ok(serde_json::to_value(outcome)?)
    }
}

/// Carry out one call for the run `payload.by` of `project_id`.
pub(crate) async fn run_call(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    payload: &ProgramCallPayload,
) -> Result<ProgramCallOutcome, CallError> {
    let asker = payload.by;
    // What members' values, connections and tokens, and the project's
    // runs, are keyed by: the project's owning tenant, read only by the
    // calls that reach them.
    let owner = || async {
        crate::member_values::owning_tenant(state, project_id).await.map(crate::tenant::TenantId)
    };
    let answered = |value: Value| Ok(ProgramCallOutcome { value, stops_asker: false });
    match &payload.call {
        ProgramCall::InfraStart { node, member } => {
            let started = infra_start(state, project_id, node, member.as_ref()).await?;
            answered(serde_json::to_value(started).map_err(internal("answer"))?)
        }
        ProgramCall::InfraStop { node, member, spec } => {
            infra_down(state, project_id, node, member.as_ref(), InfraVerb::Stop, spec, asker, payload.stop_self).await
        }
        ProgramCall::InfraTerminate { node, member, spec } => {
            infra_down(state, project_id, node, member.as_ref(), InfraVerb::Terminate, spec, asker, payload.stop_self).await
        }
        ProgramCall::InfraStatus { node, member } => {
            let copy = observed_copies(state, project_id)
                .await?
                .into_iter()
                .find(|copy| &copy.node == node && copy.member.as_ref() == member.as_ref());
            answered(serde_json::to_value(copy).map_err(internal("answer"))?)
        }
        ProgramCall::InfraCopies { node } => {
            let copies: Vec<InfraCopy> =
                observed_copies(state, project_id).await?.into_iter().filter(|copy| &copy.node == node).collect();
            answered(serde_json::to_value(copies).map_err(internal("answer"))?)
        }
        ProgramCall::MembersList => {
            let tenant = owner().await?;
            answered(serde_json::to_value(members(state, &tenant, project_id).await?).map_err(internal("answer"))?)
        }
        ProgramCall::TriggerActivate { scope } => answered(triggers_activate(state, project_id, scope).await?),
        ProgramCall::TriggerDeactivate { scope, spec } => {
            triggers_deactivate(state, project_id, scope, spec, asker, payload.stop_self).await
        }
        ProgramCall::ConnectionsList { member } => {
            let tenant = owner().await?;
            let grants = weft_access_store::list_grants(
                &state.pg_pool,
                tenant.as_str(),
                None,
                weft_access_store::GrantOwnerScope::Member { project_id, member },
            )
            .await
            .map_err(access_error)?;
            answered(serde_json::to_value(grants).map_err(internal("answer"))?)
        }
        ProgramCall::ConnectionsForget { member } => {
            let tenant = owner().await?;
            let forgotten = weft_access_store::forget_member_grants(&state.pg_pool, tenant.as_str(), project_id, member)
                .await
                .map_err(access_error)?;
            answered(serde_json::to_value(ConnectionsForgotten { forgotten }).map_err(internal("answer"))?)
        }
        ProgramCall::ValuesGet { member } => {
            let tenant = owner().await?;
            let values = weft_access_store::member_values(&state.pg_pool, tenant.as_str(), project_id, member)
                .await
                .map_err(access_error)?;
            answered(serde_json::to_value(values).map_err(internal("answer"))?)
        }
        ProgramCall::ValuesChange { member, set, clear } => {
            let clear: Vec<(String, String)> = clear.iter().map(|f| (f.step.clone(), f.field.clone())).collect();
            let changed = crate::member_values::change(state, owner().await?.as_str(), project_id, member, set, &clear).await?;
            answered(serde_json::to_value(changed).map_err(internal("answer"))?)
        }
        ProgramCall::ValuesForget { member } => {
            let changed = crate::member_values::forget(state, owner().await?.as_str(), project_id, member).await?;
            answered(serde_json::to_value(changed).map_err(internal("answer"))?)
        }
        ProgramCall::CostsList { filter } => {
            answered(serde_json::to_value(costs(state, project_id, filter).await?).map_err(internal("answer"))?)
        }
        ProgramCall::RunsClean { filter, running } => {
            let tenant = owner().await?;
            let asker_matches = asker_matches_filter(state, project_id, filter, asker).await?;
            let outcome =
                crate::api::execution::clean_runs(state, &tenant, Some(project_id), filter, *running, Some(asker)).await?;
            let stops_asker = payload.stop_self == StopSelf::Include && asker_matches;
            stop_asker(state, asker, stops_asker).await?;
            Ok(ProgramCallOutcome { value: serde_json::to_value(outcome).map_err(internal("answer"))?, stops_asker })
        }
        ProgramCall::TokensRevoke { member, id } => {
            let tenant = owner().await?;
            let revoked = crate::journal::postgres::revoke_member_tokens(&state.pg_pool, tenant.as_str(), project_id, member, *id)
                .await
                .map_err(internal("revoke tokens"))?;
            answered(serde_json::to_value(TokensRevoked { revoked }).map_err(internal("answer"))?)
        }
    }
}

#[derive(Clone, Copy)]
enum InfraVerb {
    Stop,
    Terminate,
}

impl InfraVerb {
    fn lifecycle(self) -> weft_broker_client::protocol::InfraLifecycleVerb {
        match self {
            InfraVerb::Stop => weft_broker_client::protocol::InfraLifecycleVerb::Stop,
            InfraVerb::Terminate => weft_broker_client::protocol::InfraLifecycleVerb::Terminate,
        }
    }
}

/// Start one copy of an infra node, returning once the start is queued
/// (its setup run journaled, so every reader shows the copy starting from
/// here on); the copy comes up on its own and `ctx.infra(..).status()`
/// says when. A copy already running, or already starting, answers
/// without starting anything. A refusal for a passing reason (a build in
/// progress, a trigger reading the copy mid-activation or mid-drain)
/// answers `waiting` with that reason, and `ctx.infra(..).start()` asks
/// again at its next look; any other refusal fails the call.
async fn infra_start(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    node: &str,
    member: Option<&MemberId>,
) -> Result<InfraStartAnswer, CallError> {
    // The node must be one this member has a copy of: refused now, to
    // the program, naming the fix.
    let project = load_project(state, project_id).await?;
    crate::api::infra::resolve_infra_nodes(&project, &[node.to_string()], member)?;
    let copies = crate::infra_node::observe(&state.pg_pool, project_id, &project).await.map_err(internal("infra copies"))?;
    match copies.status_of(node, member) {
        Some(crate::infra_node::InfraNodeStatus::Running) => {
            return Ok(InfraStartAnswer::AlreadyRunning);
        }
        Some(crate::infra_node::InfraNodeStatus::Provisioning) => {
            return Ok(InfraStartAnswer::AlreadyStarting);
        }
        _ => {}
    }
    // Up to the setup run being journaled, here; the rest of the sync runs
    // on after the answer. A first copy coming up can move the project's
    // workers next to their infra, and the asking run is on one of them:
    // waiting here for that move would wait on the asker itself. A start
    // never takes a run down, so the old workers drain instead of
    // cancelling.
    let body = crate::api::infra::SyncRequest {
        member: member.cloned(),
        nodes: vec![node.to_string()],
        running: weft_core::running_policy::RunningChoice {
            running_policy: Some(weft_core::running_policy::RunningPolicy::Wait),
            drain_timeout_secs: None,
        },
        ..Default::default()
    };
    let begun = match crate::api::infra::begin_sync(state, project_id, &body).await.map_err(|refused| refused.into_parts()) {
        Ok(begun) => begun,
        Err((StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED, reason)) => {
            return Ok(InfraStartAnswer::Waiting { reason });
        }
        Err(refused) => return Err(refused),
    };
    let state = state.clone();
    let node = node.to_string();
    tokio::spawn(async move {
        if let Err(not_landed) = crate::api::infra::finish_sync(&state, project_id, begun).await {
            let (status, message) = <(StatusCode, String)>::from(not_landed);
            tracing::error!(
                target: "weft_dispatcher::program_call",
                %project_id, node = %node, %status, %message,
                "a program's infra start was queued, then did not land; `weft infra status` shows the copy"
            );
        }
    });
    Ok(InfraStartAnswer::Started)
}

/// Take one copy of an infra node down: the triggers reading it with it
/// (by `spec`), the runs using it by `spec`'s running policy, then the
/// supervisor's command. Never the run that asked, unless it said
/// `StopSelf::Include` and it uses the copy.
#[allow(clippy::too_many_arguments)]
async fn infra_down(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    node: &str,
    member: Option<&MemberId>,
    verb: InfraVerb,
    spec: &DeactivateSpec,
    asker: Color,
    stop_self: StopSelf,
) -> Result<ProgramCallOutcome, CallError> {
    spec.validate().map_err(|m| (StatusCode::BAD_REQUEST, m.to_string()))?;
    let project = load_project(state, project_id).await?;
    let (copies_now, pending) =
        crate::infra_node::observe_with(&state.pg_pool, project_id, &project).await.map_err(internal("infra copies"))?;
    // A start of this copy still in its setup run would issue its apply
    // after the command below and bring the copy back up: the take-down
    // is the later intent, so the start ends here.
    let cancelled_start = !pending.setups_starting(node, member).is_empty();
    for setup in pending.setups_starting(node, member) {
        crate::api::execution::cancel_color(state, setup, &weft_core::exec::CancelCause::User)
            .await
            .map_err(internal("cancel the copy's start"))?;
    }
    use crate::infra_node::InfraNodeStatus;
    let row = copies_now.rows.iter().find(|r| r.node_id == node && r.member.as_ref() == member).map(|r| r.status);
    let already_down = match (row, verb) {
        (None, _) => true,
        (Some(InfraNodeStatus::Stopped | InfraNodeStatus::Stopping), InfraVerb::Stop) => true,
        (Some(InfraNodeStatus::Terminating), InfraVerb::Terminate) => true,
        (Some(_), _) => false,
    };
    if already_down {
        return Ok(ProgramCallOutcome {
            value: serde_json::json!({ "issued": false, "cancelled_start": cancelled_start }),
            stops_asker: false,
        });
    }
    let copies = Copies::of(member.cloned());
    let nodes = std::collections::BTreeSet::from([node.to_string()]);
    let live_readers: Vec<weft_core::activation::ActivationKey> =
        crate::api::infra::activations_reading(state, project_id, &project, &nodes, &copies)
            .await?
            .into_iter()
            .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
            .map(|a| a.key)
            .collect();
    let deactivated = !live_readers.is_empty();
    if deactivated {
        crate::take_down::take_down(
            state,
            project_id,
            &crate::take_down::TakeDownTarget::Activations(live_readers),
            spec,
            false,
            Some(asker),
        )
        .await?;
    }
    let runs = crate::take_down::live_runs(state, project_id).await.map_err(internal("live runs"))?;
    let asker_uses_it = crate::take_down::runs_using_copies(&copies, &runs, None).iter().any(|r| r.color == asker);
    crate::api::infra::settle_running_before_infra_op(
        state,
        project_id,
        &copies,
        spec.running_policy,
        false,
        Some(asker),
    )
    .await?;
    let drain = spec.drain_timeout_secs.unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
    let command_id = crate::api::infra::issue_lifecycle_ensuring_supervisor(
        state,
        project_id,
        Some(node),
        &copies,
        verb.lifecycle(),
        spec.running_policy,
        false,
        drain,
    )
    .await?;
    let stops_asker = stop_self == StopSelf::Include && asker_uses_it;
    stop_asker(state, asker, stops_asker).await?;
    Ok(ProgramCallOutcome { value: serde_json::json!({ "issued": true, "command": command_id }), stops_asker })
}

/// Activate the activations `scope` names. Those already active answer
/// without being set up again. The asking run may sit on a worker built
/// from an older image, so that worker is retired, never waited on.
async fn triggers_activate(state: &DispatcherState, project_id: uuid::Uuid, scope: &ActivationScope) -> Result<Value, CallError> {
    let project = state
        .projects
        .project(project_id)
        .await
        .map_err(internal("project"))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    let keys = scope.resolve(&project).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let existing = state.activations.list(project_id).await.map_err(internal("activations"))?;
    let active = |key: &weft_core::activation::ActivationKey| {
        existing.iter().any(|a| &a.key == key && a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
    };
    if keys.iter().all(active) {
        return Ok(serde_json::json!({ "activated": false, "already": "active" }));
    }
    let request = crate::api::project::ActivateRequest { scope: scope.clone(), ..Default::default() };
    let answer =
        crate::api::project::activate_with(state, project_id, request, crate::api::project::ActivateAsker::Run)
            .await
            .map_err(<(StatusCode, String)>::from)?;
    serde_json::to_value(answer.0).map_err(internal("answer"))
}

/// Take the activations `scope` names down under `spec`, never touching
/// the run that asked unless it said `StopSelf::Include` and one of them
/// fired it.
async fn triggers_deactivate(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    scope: &ActivationScope,
    spec: &DeactivateSpec,
    asker: Color,
    stop_self: StopSelf,
) -> Result<ProgramCallOutcome, CallError> {
    let project = state
        .projects
        .project(project_id)
        .await
        .map_err(internal("project"))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    let keys = scope.resolve(&project).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let target = crate::take_down::TakeDownTarget::Activations(keys);
    let runs = crate::take_down::live_runs(state, project_id).await.map_err(internal("live runs"))?;
    let asker_fired = crate::take_down::affected_runs(&target, &runs, None).iter().any(|r| r.color == asker);
    crate::take_down::take_down(state, project_id, &target, spec, false, Some(asker)).await?;
    let stops_asker = stop_self == StopSelf::Include && asker_fired;
    stop_asker(state, asker, stops_asker).await?;
    Ok(ProgramCallOutcome { value: serde_json::json!({ "deactivated": true }), stops_asker })
}

/// Cancel the asking run when its own call reaches it and it asked to be
/// stopped with the rest. Its worker is waiting for exactly this.
async fn stop_asker(state: &DispatcherState, asker: Color, stops: bool) -> Result<(), CallError> {
    if stops {
        crate::api::execution::cancel_color(state, asker, &weft_core::exec::CancelCause::User)
            .await
            .map_err(internal("cancel the asking run"))?;
    }
    Ok(())
}

/// Whether the asking run is one `filter` reaches (a clean that includes
/// the asker stops it rather than deleting it from under itself).
async fn asker_matches_filter(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    filter: &weft_core::program::RunFilter,
    asker: Color,
) -> Result<bool, CallError> {
    let Some(run) = state.journal.execution_summary(asker).await.map_err(internal("the asking run"))? else {
        return Ok(false);
    };
    let now = crate::lease::now_unix() as u64;
    let tagged = match &filter.tag {
        Some(tag) => run.tags.iter().any(|t| t == tag),
        None => true,
    };
    Ok(run.project_id == project_id
        && filter.member.as_ref().is_none_or(|m| run.member.as_ref() == Some(m))
        && filter.status.as_deref().is_none_or(|s| s == "running")
        && filter.node.as_deref().is_none_or(|n| n == run.entry_node)
        && filter.older_than_secs.is_none_or(|secs| run.started_at <= now.saturating_sub(secs))
        && tagged)
}

/// Cost records of the project's runs, filtered. The run's member is its
/// `execution_color` row's, born with the run and never changed.
async fn costs(state: &DispatcherState, project_id: uuid::Uuid, filter: &CostFilter) -> Result<Vec<CostRecord>, CallError> {
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT e.color, e.payload_json, ec.member_id \
         FROM exec_event e JOIN execution_color ec ON ec.color = e.color \
         WHERE ec.project_id = $1 AND e.kind = 'cost_reported' \
           AND ($2::text IS NULL OR ec.member_id = $2) \
           AND ($3::text IS NULL OR e.color = $3) \
           AND ($4::bigint IS NULL OR e.created_at >= $4) \
         ORDER BY e.id",
    )
    .bind(project_id)
    .bind(filter.member.as_ref().map(|m| m.as_str()))
    .bind(filter.run.map(|r| r.to_string()))
    .bind(filter.since_unix.map(|s| s as i64))
    .fetch_all(&state.pg_pool)
    .await
    .map_err(internal("costs"))?;
    let project = state.projects.project(project_id).await.map_err(internal("project"))?;
    let mut out = Vec::new();
    for (color, payload, member) in rows {
        let color: Color = color.parse().map_err(internal("cost row color"))?;
        let event = weft_journal::decode_event(color, &payload).map_err(internal("cost row"))?;
        let weft_journal::ExecEvent::CostReported { node_id, frames, service, model, amount_usd, origin, at_unix, .. } = event else {
            continue;
        };
        let node = match &project {
            Some(project) => {
                let call_path: Vec<String> = weft_core::frames::call_path(&frames).into_iter().map(str::to_string).collect();
                weft_core::project::address_of(project, &node_id, &call_path)
            }
            None => node_id,
        };
        if filter.node.as_deref().is_some_and(|n| n != node) || filter.service.as_deref().is_some_and(|s| s != service) {
            continue;
        }
        if filter.paid_by.as_ref().is_some_and(|paid_by| !paid_by.covers(&origin)) {
            continue;
        }
        out.push(CostRecord {
            run: color,
            member: member.map(MemberId::new).transpose().map_err(internal("cost row member"))?,
            node,
            service,
            model,
            amount_usd,
            paid_by: origin,
            at_unix,
        });
    }
    Ok(out)
}

/// The project's registered definition, or the refusal a program reads
/// when it is gone.
async fn load_project(state: &DispatcherState, project_id: uuid::Uuid) -> Result<weft_core::ProjectDefinition, CallError> {
    state
        .projects
        .project(project_id)
        .await
        .map_err(internal("project"))?
        .ok_or((StatusCode::NOT_FOUND, "the project is gone".to_string()))
}

/// Every copy of the project's infra as `weft status` and `weft infra
/// status` show it (`infra_node::observe`): a start or a stop under way
/// reads as such before the supervisor reaches the copy.
async fn observed_copies(state: &DispatcherState, project_id: uuid::Uuid) -> Result<Vec<InfraCopy>, CallError> {
    let project = load_project(state, project_id).await?;
    let copies = crate::infra_node::observe(&state.pg_pool, project_id, &project).await.map_err(internal("infra copies"))?;
    Ok(copies
        .rows
        .into_iter()
        .map(|row| InfraCopy {
            node: row.node_id,
            member: row.member,
            status: row.status,
            failure: row.failure_message,
        })
        .chain(copies.starting.into_iter().map(|(node, member)| InfraCopy {
            node,
            member,
            status: crate::infra_node::InfraNodeStatus::Provisioning,
            failure: None,
        }))
        .collect())
}

/// Every member weft holds anything for in the project, with what it
/// holds (`MemberHoldings::merge` over each store's counts).
async fn members(
    state: &DispatcherState,
    tenant: &crate::tenant::TenantId,
    project_id: uuid::Uuid,
) -> Result<Vec<MemberHoldings>, CallError> {
    let values = weft_access_store::member_value_counts(&state.pg_pool, tenant.as_str(), project_id)
        .await
        .map_err(internal("member values"))?;
    let connections = weft_access_store::member_connection_counts(&state.pg_pool, tenant.as_str(), project_id)
        .await
        .map_err(internal("member connections"))?;
    let tokens = crate::journal::postgres::member_token_counts(
        &state.pg_pool,
        tenant.as_str(),
        project_id,
        crate::lease::now_unix(),
    )
    .await
    .map_err(internal("member tokens"))?;
    let copies = observed_copies(state, project_id).await?;
    let waiting = crate::api::signal::member_waits(&state.pg_pool, project_id).await.map_err(internal("waiting fires"))?;
    let triggers: Vec<(MemberId, MemberTrigger)> = state
        .activations
        .list(project_id)
        .await
        .map_err(internal("activations"))?
        .into_iter()
        .filter_map(|a| {
            let member = a.key.member()?.clone();
            Some((
                member,
                MemberTrigger {
                    waiting: waiting.get(&a.key).cloned(),
                    trigger: a.key.trigger,
                    mode: a.lifecycle.mode(),
                },
            ))
        })
        .collect();
    Ok(MemberHoldings::merge(values, connections, tokens, copies, triggers))
}

fn internal<E: std::fmt::Display>(what: &'static str) -> impl Fn(E) -> CallError {
    move |e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"))
}

fn access_error(e: anyhow::Error) -> CallError {
    match e.downcast_ref::<weft_access_store::AccessError>() {
        Some(weft_access_store::AccessError::NotFound) => (StatusCode::NOT_FOUND, format!("{e}")),
        Some(_) => (StatusCode::BAD_REQUEST, format!("{e}")),
        None => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}
