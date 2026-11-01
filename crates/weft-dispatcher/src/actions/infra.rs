//! A project's infra copies: every verb of `weft infra`, defined once.
//! The person's routes (`crate::api::infra`) and a program's calls
//! (`weft_core::program::infra::InfraCall`) reach the same function, so
//! a start, an upgrade, a stop or a terminate meets the same gates from
//! both: whose project it is, a project being removed, maintenance, the
//! action table of the shared infra, a transition or a setup in flight,
//! the build it was asked against. The machinery they share (the sync
//! pipeline, the take-down of what reaches a copy, the upgrade's legs)
//! lives beside the routes, in `crate::api::infra`.
//!
//! A take-down never reaches the run that asked
//! ([`Actor::take_down_asker`]), unless it said `StopSelf::Include` and it
//! is among the runs reaching the copies: it is then cancelled once the
//! take-down is queued ([`super::stop_asker`]).

use axum::http::StatusCode;
use weft_core::infra::wire::{
    CommandOutcome, CommandStatus, CopyRef, Door, DoorsResponse, InfraLogs, InfraStatus, InfraStatusEntry, LifecycleCommandIssued, LogBlock,
    LogsFrom, StopRequest, SyncRequest, UpgradeRequest,
};
use weft_core::instance::{Copies, InstanceId, InstanceTarget};
use weft_core::live::{LiveFeed, LivePress};
use weft_core::program::infra::{InfraCall, InfraCancelAnswer, InfraLogsRequest, RebakeAnswer, SetupEnd};
use weft_core::program::{InfraCopy, InfraDownAnswer, InfraStartAnswer, ProgramCallOutcome};
use weft_core::take_down::TakeDownChoice;
use weft_core::{RunId, StopSelf};

use super::{answered, failed, stop_asker, Actor, Refused};
use crate::api::infra::{self as machinery, SyncKind};
use crate::infra_lifecycle_command::{self, Teardown};
use crate::infra_node::{self, InfraNodeRow, InfraNodeStatus};
use crate::state::DispatcherState;

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.infra.start", super::Reach::Everyone),
    ("weft.infra.stop", super::Reach::Everyone),
    ("weft.infra.terminate", super::Reach::Everyone),
    ("weft.infra.upgrade", super::Reach::Everyone),
    ("weft.infra.status", super::Reach::Everyone),
    ("weft.infra.copies", super::Reach::Everyone),
    ("weft.infra.cancel", super::Reach::Everyone),
    ("weft.infra.look", super::Reach::Everyone),
    ("weft.infra.logs", super::Reach::Everyone),
    ("weft.infra.doors", super::Reach::Everyone),
    ("weft.infra.command", super::Reach::Everyone),
    ("weft.infra.show", super::Reach::Everyone),
    ("weft.infra.press", super::Reach::Everyone),
    ("weft.infra.rebake", super::Reach::Everyone),
    ("weft.infra.setup", super::Reach::Everyone),
];

/// Carry out one infra call of `actor` on project `project_id`: each
/// verb as the person's route does it, with one copy's request.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    call: &InfraCall,
    stop_self: StopSelf,
) -> Result<ProgramCallOutcome, Refused> {
    match call {
        InfraCall::Start { node, instance } => answered(&request_start(state, actor, project_id, node, instance.as_ref()).await?),
        InfraCall::Stop { node, instance, take_down, force } => {
            let request = one_copy(node, instance, take_down);
            let down = take_down_copies(state, actor, project_id, &request, Teardown::Stop { force: *force }, stop_self).await?;
            Ok(ProgramCallOutcome { value: serde_json::to_value(down.copy).map_err(failed("answer"))?, stops_asker: down.stops_asker })
        }
        InfraCall::Terminate { node, instance, take_down, disks } => {
            let request = one_copy(node, instance, take_down);
            let down = take_down_copies(state, actor, project_id, &request, Teardown::Terminate { disks: *disks }, stop_self).await?;
            Ok(ProgramCallOutcome { value: serde_json::to_value(down.copy).map_err(failed("answer"))?, stops_asker: down.stops_asker })
        }
        InfraCall::Upgrade { node, instance, take_down } => {
            let request = UpgradeRequest {
                sync: SyncRequest { build: Default::default(), instances: InstanceTarget::of(instance.clone()), nodes: vec![node.clone()] },
                take_down: *take_down,
            };
            let upgraded = upgrade(state, actor, project_id, &request, stop_self).await?;
            Ok(ProgramCallOutcome { value: serde_json::to_value(upgraded.issued).map_err(failed("answer"))?, stops_asker: upgraded.stops_asker })
        }
        InfraCall::Status { node, instance } => {
            actor.may_act_on(state, project_id).await?;
            let copy = observed_copies(state, project_id)
                .await?
                .into_iter()
                .find(|copy| &copy.node == node && copy.instance.as_ref() == instance.as_ref());
            answered(&copy)
        }
        InfraCall::Copies { node } => {
            actor.may_act_on(state, project_id).await?;
            let copies: Vec<InfraCopy> = observed_copies(state, project_id).await?.into_iter().filter(|copy| &copy.node == node).collect();
            answered(&copies)
        }
        InfraCall::Cancel { instance } => answered(&InfraCancelAnswer { cancelled: cancel(state, actor, project_id, instance.as_ref()).await? }),
        InfraCall::Look => {
            look(state, actor, project_id).await?;
            answered(&serde_json::json!({}))
        }
        InfraCall::Logs { request } => answered(&logs(state, actor, project_id, request).await?),
        InfraCall::Doors => answered(&doors(state, actor, project_id).await?),
        // Read at once: a held read would hold the asking run's worker
        // open; `ctx.all_infra().follow_command(..)` looks again on a
        // parked timer instead.
        InfraCall::Command { id } => answered(&command(state, actor, project_id, *id, weft_task_store::stopping::Deadline::at(tokio::time::Instant::now())).await?),
        InfraCall::Show { node, instance } => answered(&show(state, actor, project_id, node, instance.as_ref()).await?),
        InfraCall::Press { node, instance, press: pressed } => answered(&press(state, actor, project_id, node, instance.as_ref(), pressed).await?),
        InfraCall::Rebake { node, instance } => {
            let answer = match begin_rebake(state, actor, project_id, node, instance.as_ref()).await? {
                Rebaking::Started(run) => RebakeAnswer::Started { run: run.run_id },
                Rebaking::Behind(_) => RebakeAnswer::Waiting { reason: "another infra setup of these copies is under way".into() },
                Rebaking::NothingToSetUp => RebakeAnswer::NothingToSetUp,
            };
            answered(&answer)
        }
        InfraCall::Setup { run } => answered(&setup(state, actor, project_id, *run).await?),
    }
}

/// A person's request aimed at one node's copy, the shape a program's
/// call on that copy is carried as.
fn one_copy(node: &str, instance: &Option<InstanceId>, take_down: &TakeDownChoice) -> StopRequest {
    StopRequest { take_down: *take_down, instances: InstanceTarget::of(instance.clone()), nodes: vec![node.to_string()], force: false }
}

/// The project's registered definition, or the refusal when it is gone.
pub(crate) async fn load_project(state: &DispatcherState, project_id: uuid::Uuid) -> Result<weft_core::ProjectDefinition, Refused> {
    state
        .projects
        .project(project_id)
        .await
        .map_err(failed("project"))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))
}

/// Refuse a project `weft rm` marked: it takes no verb.
async fn unless_removing(state: &DispatcherState, project_id: uuid::Uuid) -> Result<(), Refused> {
    state
        .projects
        .get(project_id)
        .await
        .map_err(failed("project"))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?
        .unless_removing()
        .map(drop)
}

// ----- start ---------------------------------------------------------------

/// `weft infra start`: start the copies of the owners named that are not
/// running, and nothing else, answering once their setups landed. Owner
/// by owner, each on its own: one that cannot start is said with its
/// owner, and the others still start. The gates are each owner's
/// [`machinery::begin_sync`].
pub(crate) async fn start(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, request: &SyncRequest) -> Result<InfraStatus, Refused> {
    actor.may_act_on(state, project_id).await?;
    let owners = machinery::copy_owners(state, project_id, &request.instances, true).await?;
    let asked = owners.len();
    let registered = load_project(state, project_id).await?;
    let mut refused: Vec<(Option<InstanceId>, Refused)> = Vec::new();
    // Whether an owner's start went ahead: then the answer is no longer
    // one nothing of the request ran.
    let mut any_started = false;
    for instance in owners {
        let Some(nodes) = machinery::owner_nodes(&registered, &request.instances, &request.nodes, instance.as_ref()) else { continue };
        let started: Result<(), Refused> = async {
            let begun = machinery::begin_sync(state, project_id, &request.build, instance.as_ref(), &nodes, request.instances.is_shared(), SyncKind::Start)
                .await
                .map_err(|e| e.into_parts())?;
            if let Some(begun) = begun {
                machinery::finish_sync(state, begun).await?;
            }
            Ok(())
        }
        .await;
        match started {
            Ok(()) => any_started = true,
            Err(e) => refused.push((instance, e)),
        }
    }
    if asked == 1 {
        if let Some((_, e)) = refused.pop() {
            return Err(e);
        }
    }
    let answer = |e: Refused| if any_started { crate::api::after_writes(e) } else { e };
    if let Some((_, (status, _))) = refused.first() {
        let status = *status;
        let lines: Vec<String> = refused
            .into_iter()
            .map(|(instance, (_, message))| match instance {
                Some(instance) => format!("instance '{instance}': {message}"),
                None => format!("the program's own infra: {message}"),
            })
            .collect();
        return Err(answer((status, format!("some copies did not start:\n  {}", lines.join("\n  ")))));
    }
    infra_status(state, project_id).await.map_err(answer)
}

/// A program's start of one copy, returning once the start is queued (its
/// setup run journaled, so every reader shows the copy starting from here
/// on); the copy comes up on its own and `ctx.infra(..).status()` says
/// when. A copy already running, or already starting, answers without
/// starting anything. A refusal for a passing reason (a build in
/// progress, a trigger whose cut contains the copy mid-activation)
/// answers `waiting` with that reason, and `ctx.infra(..).start()` asks
/// again at its next look; any other refusal fails the call. The gates
/// are [`start`]'s.
async fn request_start(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    node: &str,
    instance: Option<&InstanceId>,
) -> Result<InfraStartAnswer, Refused> {
    actor.may_act_on(state, project_id).await?;
    // The node must be one this instance has a copy of: refused now, to
    // the program, naming the fix.
    let project = load_project(state, project_id).await?;
    machinery::resolve_infra_nodes(&project, &[node.to_string()], instance)?;
    let copies = infra_node::observe(&state.pg_pool, project_id, &project).await.map_err(failed("infra copies"))?;
    match copies.status_of(node, instance) {
        Some(InfraNodeStatus::Running) => return Ok(InfraStartAnswer::AlreadyRunning),
        Some(InfraNodeStatus::Provisioning) => return Ok(InfraStartAnswer::AlreadyStarting),
        _ => {}
    }
    // Up to the setup run being journaled, here; the rest of the sync runs
    // on after the answer. A start never takes a run down.
    let begun = match machinery::begin_sync(state, project_id, &Default::default(), instance, &[node.to_string()], instance.is_none(), SyncKind::Start)
        .await
        .map_err(|refused| refused.into_parts())
    {
        Ok(Some(begun)) => begun,
        // Running or starting by the time the gates passed.
        Ok(None) => return Ok(InfraStartAnswer::AlreadyStarting),
        Err((StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED, reason)) => return Ok(InfraStartAnswer::Waiting { reason }),
        Err(refused) => return Err(refused),
    };
    let state = state.clone();
    let node = node.to_string();
    // The start lands after this answer: work until it does.
    let busy = state.signals.busy().await;
    tokio::spawn(async move {
        let _busy = busy;
        if let Err(not_landed) = machinery::finish_sync(&state, begun).await {
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

// ----- upgrade -------------------------------------------------------------

/// What an upgrade, a stop or a terminate issued, and whether the run that
/// asked goes with it.
pub(crate) struct Issued {
    pub issued: LifecycleCommandIssued,
    pub stops_asker: bool,
}

/// `weft infra upgrade` (the build already made): replace with the
/// registered build only the copies running on another one, issuing an
/// `upgrade` command per owner that has some, after taking down the
/// triggers whose cut contains the copies it cycles, and answer at once
/// with their ids. Nothing running behind the build is nothing to do,
/// answered with no command; another upgrade of the same copies in flight
/// is refused. A dispatcher runs each command
/// ([`machinery::run_upgrade`]): the stop leg, then the start, so nothing
/// depends on the asker staying around.
pub(crate) async fn upgrade(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    request: &UpgradeRequest,
    stop_self: StopSelf,
) -> Result<Issued, Refused> {
    actor.may_act_on(state, project_id).await?;
    let UpgradeRequest { sync, take_down } = request;
    let take_down = take_down.resolve().map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    // What it replaces: each owner's copies running on another build than
    // the registered one, never one that is stopped or missing, nor one
    // already on it. Nothing of that kind anywhere is nothing to do, with
    // nothing checked or taken down: what a deploy that runs it on every
    // push meets on its first one.
    let registered = load_project(state, project_id).await?;
    let binary_hash = state.projects.running_binary_hash(project_id).await.map_err(failed("running hashes"))?;
    let definition_hash = state.projects.running_definition_hash(project_id).await.map_err(failed("running hashes"))?;
    let desired = crate::api::project::Desired::registered(state, project_id, &registered, binary_hash.as_deref(), definition_hash.as_deref()).await?;
    let observed = infra_node::observe(&state.pg_pool, project_id, &registered).await.map_err(failed("infra copies"))?;
    let mut behind_by_owner = Vec::new();
    for instance in machinery::copy_owners(state, project_id, &sync.instances, false).await? {
        let Some(nodes) = machinery::owner_nodes(&registered, &sync.instances, &sync.nodes, instance.as_ref()) else { continue };
        let targeted = machinery::resolve_infra_nodes(&registered, &nodes, instance.as_ref())?;
        let running: std::collections::BTreeSet<String> =
            targeted.into_keys().filter(|place| machinery::upgrades(&observed, &desired, place, instance.as_ref())).collect();
        if !running.is_empty() {
            behind_by_owner.push((instance, nodes, running));
        }
    }
    // Every owner is checked before the first one's triggers come down, so
    // a refusal leaves everything as it was.
    let mut planned = Vec::new();
    for (instance, nodes, running) in behind_by_owner {
        let registered = machinery::gate_sync(state, project_id, &sync.build, instance.as_ref(), &nodes, SyncKind::Upgrade, sync.instances.is_shared())
            .await
            .map_err(|e| e.into_parts())?;
        if let Some(command_id) = infra_lifecycle_command::upgrade_in_flight(&state.pg_pool, project_id, instance.as_ref().map(|m| m.as_str()))
            .await
            .map_err(failed("upgrades in flight"))?
        {
            return Err(infra_lifecycle_command::upgrade_in_flight_refusal(command_id));
        }
        let reach = crate::infra_reach::InfraReach { places: running.clone(), copies: Copies::of(instance.clone()) };
        let live = machinery::triggers_reached(state, project_id, &registered, &reach).await?;
        planned.push((instance, nodes, running, reach, live));
    }
    let stops_asker = asker_reaches(state, actor, project_id, planned.iter().map(|(_, _, _, reach, _)| reach), stop_self).await?;
    let tenant = machinery::tenant_of(state, project_id).await?;
    let mut taken = crate::take_down::Taken::default();
    for (instance, nodes, running, reach, live) in planned {
        // Down with the command, in one write, as the asker answered: the
        // command is claimable only once the doors are set, so nothing
        // slips in between. Their start brings them back.
        let commands = infra_lifecycle_command::TakeDownCommands {
            tenant_id: tenant.clone(),
            issued_by_replica: state.replica.as_str().to_string(),
            work: infra_lifecycle_command::TakeDownWork::Upgrade {
                instance,
                work: infra_lifecycle_command::UpgradeWork {
                    nodes,
                    running: running.into_iter().collect(),
                    take_down,
                    binary_hash: sync.build.binary_hash.clone(),
                    definition_hash: sync.build.definition_hash.clone(),
                    infra_hash: sync.build.infra_hash.clone(),
                    waits_on_tokens: Vec::new(),
                    stopped: false,
                },
            },
        };
        taken.add(machinery::take_down_reached(state, project_id, &reach, live, &take_down, actor.take_down_asker(project_id), &commands).await?);
    }
    stop_asker(state, actor, stops_asker).await?;
    Ok(Issued { issued: LifecycleCommandIssued { command_ids: taken.commands.clone(), report: taken.report(take_down) }, stops_asker })
}

/// Whether the run that asked is among the runs reaching any of `reaches`
/// and said `StopSelf::Include`: the take-down then stops it too. Only a
/// run of this very project can reach its copies.
async fn asker_reaches<'r>(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    reaches: impl Iterator<Item = &'r crate::infra_reach::InfraReach>,
    stop_self: StopSelf,
) -> Result<bool, Refused> {
    let (StopSelf::Include, Some(asker), true) = (stop_self, actor.run(), actor.is_own_project(project_id)) else { return Ok(false) };
    for reach in reaches {
        let reached = crate::infra_reach::runs_reached(state, project_id, reach, None).await.map_err(failed("the runs reaching the copies"))?;
        if reached.iter().any(|run| run.run_id == asker) {
            return Ok(true);
        }
    }
    Ok(false)
}

// ----- stop and terminate --------------------------------------------------

/// What a stop or a terminate did: what it issued, whether the asker goes
/// with it, and, for one copy, whether it took one down.
pub(crate) struct CopiesTakenDown {
    pub issued: LifecycleCommandIssued,
    pub stops_asker: bool,
    /// [`InfraDownAnswer::TakenDown`] when something was taken down,
    /// `AlreadyDown` when every copy named was down already (or on its
    /// way), `NoCopy` when there was none.
    pub copy: InfraDownAnswer,
}

/// `weft infra stop` and `weft infra terminate`: for each owner the
/// request names, the triggers whose cut contains the copies taken down as
/// its answers say, the runs reaching them as those answers say, then the
/// command (after the wait for those runs, when it waits), answered once
/// the commands are issued. A start of a copy still in its setup run is
/// called off first: it would issue its apply after the command and bring
/// the copy back up, and the take-down is the later intent. An owner whose
/// copies are all down already (or on their way) is left as it is, so
/// asking again is answered without doing anything; a terminate deleting
/// every disk always goes, since only the supervisor's pass over the host
/// can tell whether a copy with no row still holds the disks an earlier
/// terminate kept.
pub(crate) async fn take_down_copies(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    request: &StopRequest,
    teardown: Teardown,
    stop_self: StopSelf,
) -> Result<CopiesTakenDown, Refused> {
    actor.may_act_on(state, project_id).await?;
    let take_down = request.take_down.resolve().map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    if request.force && matches!(teardown, Teardown::Terminate { .. }) {
        return Err((StatusCode::BAD_REQUEST, "`force` brings down the units that stay up on a stop; a terminate removes every unit anyway".into()));
    }
    unless_removing(state, project_id).await?;
    let project = load_project(state, project_id).await?;
    let (observed, pending) = infra_node::observe_with(&state.pg_pool, project_id, &project).await.map_err(failed("infra copies"))?;
    // Every owner is checked before the first one's triggers come down, so
    // a refusal leaves everything as it was.
    let mut planned = Vec::new();
    let mut any_copy = false;
    for instance in machinery::copy_owners(state, project_id, &request.instances, false).await? {
        let Some(nodes) = machinery::owner_nodes(&project, &request.instances, &request.nodes, instance.as_ref()) else { continue };
        let plan = machinery::plan_copies_down(state, project_id, &project, instance.clone(), &nodes).await?;
        let named = |node: &str| nodes.is_empty() || nodes.iter().any(|n| n == node);
        let places: Vec<&String> = if nodes.is_empty() { plan.reach.places.iter().collect() } else { nodes.iter().collect() };
        let starts: Vec<RunId> = places.iter().flat_map(|place| pending.setups_starting(place, instance.as_ref())).collect();
        let statuses: Vec<InfraNodeStatus> = observed
            .rows
            .iter()
            .filter(|row| row.instance == instance && named(&row.node_id))
            .map(|row| row.status)
            .chain(observed.starting.iter().filter(|(node, owner)| *owner == instance && named(node)).map(|_| InfraNodeStatus::Provisioning))
            .collect();
        any_copy |= !statuses.is_empty() || !starts.is_empty();
        let deletes_every_disk = matches!(teardown, Teardown::Terminate { disks: weft_core::infra::TerminateDisks::DeleteAll });
        if deletes_every_disk || !starts.is_empty() || statuses.iter().any(|status| !already_down(teardown, *status)) {
            planned.push((plan, starts));
        }
    }
    let copy = match (planned.is_empty(), any_copy) {
        (false, _) => InfraDownAnswer::TakenDown,
        (true, true) => InfraDownAnswer::AlreadyDown,
        (true, false) => InfraDownAnswer::NoCopy,
    };
    if planned.is_empty() {
        let nothing = crate::take_down::Taken::default();
        return Ok(CopiesTakenDown { issued: LifecycleCommandIssued { command_ids: Vec::new(), report: nothing.report(take_down) }, stops_asker: false, copy });
    }
    // Reject-don't-crash against the same reconciliation the status
    // renders (a stale screen firing stop into a transitional project).
    // The table is the SHARED infra's; an instance's copies are the
    // program's and `--instance`'s to manage.
    if request.instances.is_shared() {
        let action = match teardown {
            Teardown::Stop { .. } => "infra_stop",
            Teardown::Terminate { .. } => "infra_terminate",
        };
        crate::api::project::require_action(state, project_id, &[action]).await?;
    }
    for setup in planned.iter().flat_map(|(_, starts)| starts) {
        crate::api::run::cancel_run_id(state, *setup, &weft_core::run::CancelCause::User).await.map_err(failed("cancel the copy's start"))?;
    }
    let stops_asker = asker_reaches(state, actor, project_id, planned.iter().map(|(plan, _)| &plan.reach), stop_self).await?;
    let mut taken = crate::take_down::Taken::default();
    for (plan, _) in planned {
        taken.add(machinery::take_planned_copies_down(state, project_id, plan, teardown, &take_down, actor.take_down_asker(project_id)).await?);
    }
    stop_asker(state, actor, stops_asker).await?;
    if actor.run().is_some() {
        tracing::info!(target: "weft_dispatcher::program_call", %project_id, commands = ?taken.commands, "a program took infra copies down");
    }
    Ok(CopiesTakenDown { issued: LifecycleCommandIssued { command_ids: taken.commands.clone(), report: taken.report(take_down) }, stops_asker, copy })
}

/// Whether a copy reading `status` is already what `teardown` makes it,
/// or on its way there.
fn already_down(teardown: Teardown, status: InfraNodeStatus) -> bool {
    match teardown {
        Teardown::Stop { .. } => matches!(status, InfraNodeStatus::Stopped | InfraNodeStatus::Stopping | InfraNodeStatus::Terminating),
        Teardown::Terminate { .. } => status == InfraNodeStatus::Terminating,
    }
}

// ----- cancel and look -----------------------------------------------------

/// `weft infra cancel`: halt one owner's infra work in flight, the shared
/// copies' (`None`) or one instance's. Flags that owner's claimed
/// supervisor commands (the executing supervisor halts between platform
/// calls), cancels its still-unclaimed ones outright, and cancels its
/// InfraSetup runs still going. A take-down called off while it waits for
/// the runs reaching its copies leaves them running, so the triggers it
/// took down with them come back (`infra_lifecycle_command::SinceTakeDown`,
/// heard by the `infra_returns` loop). Dispatcher-owned verbs (deactivate,
/// reactivate) are never touched: they are health's own work. Cancel is a
/// HALT, never a rollback: no platform applies a project
/// transactionally, so what each copy reached stays visible. Answers
/// whether there was anything in flight.
pub(crate) async fn cancel(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, instance: Option<&InstanceId>) -> Result<bool, Refused> {
    actor.may_act_on(state, project_id).await?;
    let touched = infra_lifecycle_command::request_cancel_owner(&state.pg_pool, project_id, instance).await.map_err(failed("request cancel"))?;
    let run_ids = crate::api::project::infra_setup_run_ids(state, project_id, Some(instance)).await.map_err(failed("infra_setup runs"))?;
    let had_setup = !run_ids.is_empty();
    for run_id in run_ids {
        crate::api::run::cancel_run_id(state, run_id, &weft_core::run::CancelCause::User).await.map_err(failed("cancel_run_id"))?;
    }
    Ok(touched > 0 || had_setup)
}

/// `weft infra look`: ask the supervisor to look at the project's infra
/// again, so what it gave up on (`weft status`) is tried once more, its
/// health rules that gave up included. The notice wakes a supervisor that
/// scales to zero (`weft_broker_client::lifecycle_command::LOOK_WAKE`).
pub(crate) async fn look(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid) -> Result<(), Refused> {
    actor.may_act_on(state, project_id).await?;
    infra_lifecycle_command::person_asks_for_look(&state.pg_pool, project_id).await.map_err(failed("ask the supervisor to look"))
}

// ----- reading the copies --------------------------------------------------

/// One copy as every reader of the project's infra shows it: its state,
/// its row when it has one, and how far a change of it got.
pub(crate) struct ObservedCopy {
    pub copy: InfraCopy,
    pub row: Option<InfraNodeRow>,
    pub progress: Option<weft_core::infra::wire::ChangeProgress>,
}

/// Every copy of the project's infra (`infra_node::observe`), the one read
/// `weft infra status`, `weft status`, a program's `ctx.infra(..)` and the
/// instances listing all go through: a start or a stop under way reads as
/// such before the supervisor reaches the copy, and a start counts its
/// progress from when it was asked for. The rows first, then the copies a
/// start is bringing up that have none yet.
pub(crate) async fn read_copies(state: &DispatcherState, project_id: uuid::Uuid) -> Result<Vec<ObservedCopy>, Refused> {
    let project = load_project(state, project_id).await?;
    let copies = infra_node::observe(&state.pg_pool, project_id, &project)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("read the infra copies", e))?;
    let starting: Vec<ObservedCopy> = copies
        .starting
        .iter()
        .map(|(node, instance)| ObservedCopy {
            copy: InfraCopy { node: node.clone(), instance: instance.clone(), status: InfraNodeStatus::Provisioning, failure: None },
            row: None,
            progress: copies.progress_of(node, instance.as_ref()),
        })
        .collect();
    let progress: Vec<_> = copies.rows.iter().map(|row| copies.progress_of(&row.node_id, row.instance.as_ref())).collect();
    Ok(copies
        .rows
        .into_iter()
        .zip(progress)
        .map(|(row, progress)| ObservedCopy {
            copy: InfraCopy { node: row.node_id.clone(), instance: row.instance.clone(), status: row.status, failure: row.failure_message.clone() },
            row: Some(row),
            progress,
        })
        .chain(starting)
        .collect())
}

/// [`read_copies`], each copy's state alone.
pub(crate) async fn observed_copies(state: &DispatcherState, project_id: uuid::Uuid) -> Result<Vec<InfraCopy>, Refused> {
    Ok(read_copies(state, project_id).await?.into_iter().map(|observed| observed.copy).collect())
}

/// `weft infra status`: every copy that exists, and weft's own calls this
/// infra's work waits on that keep failing.
pub(crate) async fn status(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid) -> Result<InfraStatus, Refused> {
    actor.may_act_on(state, project_id).await?;
    infra_status(state, project_id).await
}

/// [`status`]'s answer, for an actor already held to the project.
async fn infra_status(state: &DispatcherState, project_id: uuid::Uuid) -> Result<InfraStatus, Refused> {
    let copies = read_copies(state, project_id).await?;
    // Read only once a copy has a public endpoint, so the status of
    // infra with none answers while the tunnel is down.
    let front_door = state.external_base();
    let mut nodes = Vec::with_capacity(copies.len());
    for ObservedCopy { copy, row, progress } in copies {
        let mut entry = InfraStatusEntry {
            node: copy.node,
            instance: copy.instance,
            status: copy.status.as_str().to_string(),
            endpoint_url: None,
            public_urls: Default::default(),
            failure_stage: None,
            failure_message: copy.failure,
            notes: Vec::new(),
            progress,
        };
        if let Some(row) = row {
            for (name, path) in &row.public_paths {
                let url = front_door.url(path).await.map_err(|e| (StatusCode::BAD_GATEWAY, format!("{e:#}")))?;
                entry.public_urls.insert(name.clone(), url);
            }
            // Coarse UI hint: the first endpoint by name (BTreeMap, so
            // deterministic). Node code resolves a specific endpoint by
            // name via ctx.endpoint(...); this is just a status summary.
            entry.endpoint_url = row.endpoints.values().next().cloned();
            entry.failure_stage = row.failure_stage.map(|f| f.as_str().to_string());
            entry.notes = row.notes;
        }
        nodes.push(entry);
    }
    Ok(InfraStatus { nodes, unanswered: crate::api::project::unanswered_for(state, project_id).await? })
}

/// `weft infra list-doors`: every `SameNetwork` endpoint of every copy,
/// at the address its host gave it when it applied the unit
/// (`infra_node.doors`), plus the copies still being applied, whose doors
/// have no address yet.
pub(crate) async fn doors(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid) -> Result<DoorsResponse, Refused> {
    actor.may_act_on(state, project_id).await?;
    let rows = infra_node::list_for_project(&state.pg_pool, project_id).await.map_err(failed("infra_node list"))?;
    Ok(doors_of(&rows))
}

fn copy_ref(row: &InfraNodeRow) -> CopyRef {
    CopyRef { node: row.node_id.clone(), instance: row.instance.clone() }
}

/// One entry per door of each row, at its address as the host wrote it,
/// and every copy mid-apply.
fn doors_of(rows: &[InfraNodeRow]) -> DoorsResponse {
    DoorsResponse {
        doors: rows
            .iter()
            .flat_map(|row| row.doors.iter().map(move |(endpoint, address)| Door { copy: copy_ref(row), endpoint: endpoint.clone(), address: address.clone() }))
            .collect(),
        applying: rows.iter().filter(|r| r.status == InfraNodeStatus::Provisioning).map(copy_ref).collect(),
    }
}

/// `weft infra logs`: the infra units' own output, every container of
/// every unit of every copy of the node (or of every node), each block
/// headed by what wrote it, and the cursor that asks for exactly what
/// comes next.
pub(crate) async fn logs(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, request: &InfraLogsRequest) -> Result<InfraLogs, Refused> {
    actor.may_act_on(state, project_id).await?;
    let rows = infra_node::list_for_project(&state.pg_pool, project_id).await.map_err(failed("infra_node list"))?;
    let rows: Vec<&InfraNodeRow> = rows.iter().filter(|r| request.node.as_deref().is_none_or(|n| r.node_id == n)).collect();
    if rows.is_empty() {
        let why = match &request.node {
            Some(node) => format!("no infra node at `{node}`: it is not provisioned (`weft infra status` says where each one stands), or it is not an infra node"),
            None => "nothing is provisioned for this project (`weft infra start`)".to_string(),
        };
        return Err((StatusCode::NOT_FOUND, why));
    }
    let mut out = InfraLogs { blocks: Vec::new(), cursor: request.after.clone().unwrap_or_default() };
    for row in rows {
        let node = weft_core::infra::NodeRef {
            tenant: actor.tenant().as_str().to_string(),
            project: project_id,
            node: row.node_id.clone(),
            copy_id: row.copy_id.clone(),
        };
        for unit in row.units.keys() {
            let from = match &request.after {
                Some(cursor) => LogsFrom::After(cursor.for_unit(&row.copy_id, unit)),
                None => LogsFrom::Tail(request.tail),
            };
            let streams =
                state.host.logs(&node, unit, &from).await.map_err(|e| (StatusCode::BAD_GATEWAY, format!("the logs of {} {unit}: {e:#}", row.node_id)))?;
            for stream in streams {
                // The host made the mark: it reads the clock that stamped
                // the lines, which this process's clock may not agree with.
                out.cursor.0.insert(weft_core::infra::wire::LogCursor::key(&row.copy_id, unit, &stream.source), stream.mark);
                if !stream.lines.is_empty() {
                    out.blocks.push(LogBlock { copy: copy_ref(row), unit: unit.clone(), stream });
                }
            }
        }
    }
    Ok(out)
}

/// Whether lifecycle command `command_id` of the project finished, and
/// how: the honest "is it done" (a stop where a unit stays up leaves the
/// rollup at `running`, so completion cannot be read off it). Held up to
/// `hold` for the end, never longer than the door allows. The command is
/// looked up by `(id, project)`, so nobody reads another project's
/// command by counting through the ids.
pub(crate) async fn command(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    command_id: i64,
    hold: weft_task_store::stopping::Deadline,
) -> Result<CommandStatus, Refused> {
    actor.may_act_on(state, project_id).await?;
    use infra_lifecycle_command::WaitOutcome;
    let outcome = infra_lifecycle_command::held_command_outcome(&state.pg_pool, &state.signals, project_id, command_id, hold)
        .await
        .map_err(failed("read command"))?;
    Ok(match outcome {
        // A read returns no `Timeout`: it is a command still going.
        None | Some(WaitOutcome::Timeout) => CommandStatus { done: false, outcome: None, message: None },
        Some(WaitOutcome::Succeeded) => CommandStatus { done: true, outcome: Some(CommandOutcome::Succeeded), message: None },
        Some(WaitOutcome::Failed { error }) => CommandStatus { done: true, outcome: Some(CommandOutcome::Failed), message: Some(error) },
        Some(WaitOutcome::Cancelled { reason }) => CommandStatus { done: true, outcome: Some(CommandOutcome::Cancelled), message: Some(reason) },
    })
}

// ----- a node's card -------------------------------------------------------

/// `weft infra show`: what an infra node's card shows right now
/// ([`machinery::read_live`]).
pub(crate) async fn show(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, node: &str, instance: Option<&InstanceId>) -> Result<LiveFeed, Refused> {
    actor.may_act_on(state, project_id).await?;
    machinery::read_live(state, project_id, node, instance).await
}

/// `weft infra press`: press a button the node's card offers, answering
/// what the container answered ([`machinery::press_live`]). The press
/// waits as long as the container takes: the work is the container's.
pub(crate) async fn press(
    state: &DispatcherState,
    actor: &Actor,
    project_id: uuid::Uuid,
    node: &str,
    instance: Option<&InstanceId>,
    pressed: &LivePress,
) -> Result<serde_json::Value, Refused> {
    actor.may_act_on(state, project_id).await?;
    machinery::press_live(state, project_id, node, instance, &pressed.kind, &pressed.payload).await
}

// ----- rebake --------------------------------------------------------------

/// Where a rebake's setup stands once asked for.
enum Rebaking {
    Started(crate::api::project::InfraSetupRun),
    /// Other setups of the same copies under way, to wait out first: they
    /// may not cover the node, so the rebake runs after them rather than
    /// counting on them.
    Behind(Vec<RunId>),
    /// The node's setup has nothing to run.
    NothingToSetUp,
}

/// `weft infra rebake`: make the node's baked outputs again
/// (`weft_core::infra::bake`), by hand, for a copy whose values changed in
/// a way weft did not see (a password changed by hand inside a database):
/// run its infra setup again, which applies it (a copy already as asked is
/// left as it is) and runs its body, which saves the new values. Answers
/// once the setup ended, a setup of the same copies already under way
/// waited out first, whatever it ends in.
pub(crate) async fn rebake(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, node: &str, instance: Option<&InstanceId>) -> Result<(), Refused> {
    loop {
        match begin_rebake(state, actor, project_id, node, instance).await? {
            Rebaking::Started(run) => return crate::api::project::await_infra_setup(state, run).await.map_err(Refused::from),
            Rebaking::NothingToSetUp => return Ok(()),
            Rebaking::Behind(live) => {
                for run_id in live {
                    let other = crate::api::project::InfraSetupRun::follow(state, project_id, run_id).await;
                    // Its outcome is its own: this rebake only waits for
                    // it to end, then asks again.
                    let _ = crate::api::project::await_infra_setup(state, other).await;
                }
            }
        }
    }
}

/// A rebake's gates and its setup run's start: refused for a project being
/// removed and while the install is in maintenance (re-checked under the
/// project's lock, the one `weft maintenance on` takes before it reads
/// what is in flight); the node is held to an infra node of the owner's
/// side by the setup's own start.
async fn begin_rebake(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, node: &str, instance: Option<&InstanceId>) -> Result<Rebaking, Refused> {
    actor.may_act_on(state, project_id).await?;
    unless_removing(state, project_id).await?;
    crate::maintenance::refuse_while_on(&state.pg_pool).await?;
    let nodes = [node.to_string()];
    let next: Result<Rebaking, Refused> = crate::lease::with_project_transition_lock(&state.lock_pool, project_id, || async {
        if crate::maintenance::is_on(&state.pg_pool).await? {
            return Ok(Err((StatusCode::CONFLICT, weft_core::maintenance::IN_MAINTENANCE.to_string())));
        }
        let live = crate::api::project::infra_setup_run_ids(state, project_id, Some(instance)).await?;
        if !live.is_empty() {
            return Ok(Ok(Rebaking::Behind(live)));
        }
        Ok(crate::api::project::start_infra_setup(state, project_id, instance, &nodes)
            .await
            .map(|started| started.map_or(Rebaking::NothingToSetUp, Rebaking::Started)))
    })
    .await
    .map_err(|e| crate::lease::lock_answer("project transition lock", e))?;
    next
}

/// Where infra setup run `run` of the project stands: what a program's
/// rebake follows between its parked looks.
async fn setup(state: &DispatcherState, actor: &Actor, project_id: uuid::Uuid, run: RunId) -> Result<SetupEnd, Refused> {
    actor.may_act_on(state, project_id).await?;
    let owner = state.runs.run_owner(run).await.map_err(failed("read who owns the run"))?;
    if !owner.is_some_and(|owner| owner.project_id == project_id && owner.phase == weft_core::context::Phase::InfraSetup) {
        return Err((StatusCode::NOT_FOUND, format!("this project has no infra setup run {run}")));
    }
    use crate::api::run::TerminalOutcome;
    Ok(match crate::api::run::terminal_outcome(&state.pg_pool, run).await.map_err(failed("read how the setup ended"))? {
        None => SetupEnd::Going,
        Some(TerminalOutcome::Completed) => SetupEnd::Completed,
        Some(TerminalOutcome::Failed(reason)) => SetupEnd::Failed { reason },
        Some(TerminalOutcome::Cancelled(reason)) => SetupEnd::Cancelled { reason },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::take_down::Running;

    fn door_row(node_id: &str, instance: Option<&str>, status: InfraNodeStatus, doors: &[(&str, &str)]) -> InfraNodeRow {
        InfraNodeRow {
            project_id: uuid::Uuid::nil(),
            node_id: node_id.into(),
            instance: instance.map(|m| InstanceId::new(m).unwrap()),
            copy_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: doors.iter().map(|(e, a)| (e.to_string(), a.to_string())).collect(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
            source_hash: None,
        }
    }

    /// Every door keeps its address as the host wrote it (a name as the
    /// host part too), names whose copy it is, and a copy mid-apply is
    /// reported rather than left out.
    #[test]
    fn doors_carry_every_address_and_the_copies_still_applying() {
        let rows = [
            door_row("db", None, InfraNodeStatus::Running, &[("pg", "weft-db-main:5432")]),
            door_row("db", Some("ann"), InfraNodeStatus::Running, &[("pg", "10.0.0.3:5432")]),
            door_row("cache", None, InfraNodeStatus::Provisioning, &[]),
        ];
        let answer = doors_of(&rows);
        assert_eq!(answer.doors.len(), 2);
        assert_eq!(answer.doors[0].address, "weft-db-main:5432");
        assert_eq!(answer.doors[1].copy.instance.as_ref().map(|m| m.as_str()), Some("ann"));
        assert_eq!(answer.applying, vec![CopyRef { node: "cache".into(), instance: None }]);
    }

    /// Asking again for a take-down already done (or under way) does
    /// nothing: a stop is done once the copy is stopped, stopping or
    /// terminating, a terminate only once it is terminating, and a copy
    /// still up or failed is always taken down.
    #[test]
    fn a_take_down_already_done_is_not_done_again() {
        let stop = Teardown::Stop { force: false };
        for status in [InfraNodeStatus::Stopped, InfraNodeStatus::Stopping, InfraNodeStatus::Terminating] {
            assert!(already_down(stop, status), "{status}");
        }
        for status in [InfraNodeStatus::Running, InfraNodeStatus::Provisioning, InfraNodeStatus::Failed, InfraNodeStatus::Flaky] {
            assert!(!already_down(stop, status), "{status}");
        }
        assert!(already_down(Teardown::TERMINATE, InfraNodeStatus::Terminating));
        assert!(!already_down(Teardown::TERMINATE, InfraNodeStatus::Stopped), "a stopped copy still has its disks");
    }

    /// A program's call on one copy is carried as the person's request for
    /// that copy: the node named, its owner alone, nothing forced.
    #[test]
    fn a_program_call_on_one_copy_is_the_persons_request_for_it() {
        let ada = Some(InstanceId::new("ada").unwrap());
        let choice = TakeDownChoice { running: Some(Running::Cancel), ..Default::default() };
        let request = one_copy("one.db", &ada, &choice);
        assert_eq!(request.nodes, vec!["one.db".to_string()]);
        assert_eq!(request.instances, InstanceTarget::of(ada));
        assert_eq!(request.take_down, choice);
        assert!(!request.force);
        assert!(one_copy("db", &None, &TakeDownChoice::default()).instances.is_shared());
    }
}
