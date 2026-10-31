//! A project's trigger activations: turning them on and off, cancelling
//! one being turned on, preparing their settings (bake), and bringing the
//! ones on onto the registered build (resync), from the CLI, the editor
//! and a program alike (`weft_core::program::triggers::TriggerCall`).
//!
//! A program may resync any project of its owner but its own: a resync
//! waits for (or cancels) the runs on the old version, and the run asking
//! is one of them, so it would wait on itself ([`refuse_own_resync`]).

use axum::http::StatusCode;
use weft_core::activation::{ActivateRequest, ActivateResponse, ActivateStarted, ActivationScope, BakeRequest, CancelActivateResponse};
use weft_core::deactivation::{DeactivateRequest, DeactivateResponse, ResyncAnswer, ResyncRequest, ResyncResponse};
use weft_core::program::triggers::{BakeAnswer, TriggerCall};
use weft_core::program::ProgramCallOutcome;
use weft_core::StopSelf;
use weft_task_store::db::Client as _;

use super::{answered, failed, stop_asker, Actor, Refused};
use crate::api::project::{
    activate_with, coherent_definition, deactivate_scope, end_unfinished_activation, instances_with_triggers_on, keys_by_owner,
    keys_to_take_down, on_another_binary, on_another_binary_refusal, prepare_trigger_setup, refuse_unrunnable, require_action,
    require_registered_build, require_trigger_infra, resolve_scope, run_trigger_setup, ActivateAsker, ActivateError, Desired, StatusError,
    Unrunnable,
};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.triggers.activate", super::Reach::Everyone),
    ("weft.triggers.deactivate", super::Reach::Everyone),
    ("weft.triggers.cancel_activate", super::Reach::Everyone),
    ("weft.triggers.bake", super::Reach::Everyone),
    ("weft.triggers.resync", super::Reach::Everyone),
    ("weft.triggers.resume_resync", super::Reach::Everyone),
];

/// Carry out one trigger call of `actor` on project `project`.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    call: &TriggerCall,
    stop_self: StopSelf,
) -> Result<ProgramCallOutcome, Refused> {
    match call {
        TriggerCall::Activate { request } => answered(&activate(state, actor, project, request.clone()).await?),
        TriggerCall::Deactivate { request } => {
            let Deactivated { answer, stops_asker } =
                deactivate(state, actor, project, request, stop_self).await.map_err(StatusError::into_parts)?;
            Ok(ProgramCallOutcome { value: serde_json::to_value(answer).map_err(failed("answer"))?, stops_asker })
        }
        TriggerCall::CancelActivate { scope } => answered(&cancel_activate(state, actor, project, scope).await?),
        TriggerCall::Bake { request } => answered(&bake(state, actor, project, request).await?),
        TriggerCall::Resync { request } => answered(&resync(state, actor, project, request).await?),
        TriggerCall::ResumeResync => answered(&resume_resync(state, actor, project).await?),
    }
}

/// A program resyncing its own project: refused, since the resync waits
/// for the runs on the old version to end (or cancels them), and the run
/// asking is one of them.
fn refuse_own_resync(actor: &Actor, project: uuid::Uuid) -> Result<(), Refused> {
    match actor.is_own_project(project) {
        true => Err((
            StatusCode::FORBIDDEN,
            "a program cannot resync its own project: the resync waits for the runs on the old version to end (or cancels \
             them), and this run is one of them, so it would wait on itself. Resync it from another project, or with `weft \
             resync`"
                .into(),
        )),
        false => Ok(()),
    }
}

/// `ctx.triggers().activate(..)`: turn on the triggers the request names
/// that are off ([`activate_checked`]), under the claim of the program's
/// call's own task (`crate::task_kinds::program_call`).
pub(crate) async fn activate(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    request: ActivateRequest,
) -> Result<ActivateResponse, Refused> {
    actor.may_act_on(state, id).await?;
    Ok(activate_checked(state, id, request).await?)
}

/// `POST /projects/{id}/activations` (`weft activate`): hand the activation to
/// a task of its own (`crate::task_kinds::activate`) and answer that task
/// at once, for the person to follow in short looks
/// (`GET /projects/{id}/follow/{task}`): it waits on its setup run, which
/// may wait on a person, and goes on whatever becomes of this request or
/// of the dispatcher copy that took it.
pub(crate) async fn begin_activate(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    request: ActivateRequest,
) -> Result<ActivateStarted, Refused> {
    actor.may_act_on(state, id).await?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("start the activation: {e:#}"));
    let tenant = state.projects.tenant_for(id).await.map_err(internal)?.ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    let payload = crate::task_kinds::activate::ActivatePayload { request, asker: crate::task_kinds::activate::TaskAsker::Outside };
    let task = weft_task_store::tasks::enqueue(&state.pg_pool, crate::task_kinds::activate::task(&tenant, id, &payload)).await.map_err(internal)?;
    Ok(ActivateStarted { task })
}

/// Turn on the triggers `request` names that are off, as a person or a
/// program asks ([`activate_with`]). A port the request names is checked
/// before anything is activated, so a taken one refuses the activation and
/// changes nothing.
pub(crate) async fn activate_checked(state: &DispatcherState, id: uuid::Uuid, request: ActivateRequest) -> Result<ActivateResponse, ActivateError> {
    if let Some(port) = request.port {
        let Some(ports) = &state.project_ports else {
            return Err(ActivateError::Failed(
                StatusCode::BAD_REQUEST,
                "this install gives projects no port: a project's own address is the platform's".into(),
            ));
        };
        let taken = ports.check_asked(&state.pg_pool, id, port).await.map_err(|e| ActivateError::failed(&format!("check port {port}"), e))?;
        if let Some(why) = taken {
            return Err(ActivateError::Failed(
                StatusCode::CONFLICT,
                format!("{why}, so nothing was activated: free it, or leave out `--port` and the project keeps a port of its own"),
            ));
        }
    }
    Ok(activate_with(state, id, request, ActivateAsker::Outside).await?.0)
}

/// `POST /projects/{id}/deactivate`: take down the triggers the scope
/// names (every shared one by default) as the body's answers say, each left
/// out the default: the door holds what arrives, what is held is kept, and
/// the runs going finish on their own version: the answer names the wait
/// for them (`waiting`), a task the person follows in short looks
/// ([`super::project::follow`]), so no request stays open for as long as they run. A
/// trigger already down gets the new door; a named trigger
/// never activated is refused.
pub(crate) async fn deactivate(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    request: &DeactivateRequest,
    stop_self: StopSelf,
) -> Result<Deactivated, StatusError> {
    // Marked gate: deactivate is part of teardown flows (`weft rm
    // --journal` quiesces through it), so its "no such project" must
    // be tellable apart from a routing 404, same as `remove`/`status`.
    super::project::may_act_marked(state, actor, id).await?;
    let take_down = request
        .choice
        .resolve()
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    // Vanished between the gate and the deactivate (a concurrent rm):
    // same "no project I may see" answer, marker included.
    let Some(taken) = deactivate_scope(state, id, &request.scope, &take_down, actor.take_down_asker(id)).await? else {
        return Err(StatusError::NotMyProject);
    };
    // The asking run is left alone; it ends with the rest only when it
    // said `StopSelf::Include` and one of them fired it.
    let target = crate::take_down::TakeDownTarget::Activations(taken.keys.clone());
    let runs = crate::take_down::live_runs(state, id).await.map_err(failed("live runs"))?;
    let asker_fired = crate::take_down::affected_runs(&target, &runs, None).iter().any(|r| Some(r.run_id) == actor.run());
    let stops_asker = stop_self == StopSelf::Include && asker_fired;
    stop_asker(state, actor, stops_asker).await?;
    // The wait is for a person to follow. A program's call answers once the
    // triggers are down and never follows it: a run waiting inside its own
    // call on the runs of its trigger would wait on its siblings doing the
    // same, for good (or on itself, left going).
    let followed = matches!(actor, Actor::Operator { .. });
    let waiting = match followed && take_down.running == weft_core::take_down::Running::Wait && !taken.keys.is_empty() {
        true => {
            let not_started = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("the triggers are down; the wait for their runs could not start: {e:#}"));
            let tenant = state.projects.tenant_for(id).await.map_err(not_started)?.ok_or(StatusError::NotMyProject)?;
            let payload = weft_task_store::TakeDownWaitPayload { triggers: weft_core::activation::by_owner(&taken.keys), tokens: taken.tokens.clone() };
            Some(weft_task_store::tasks::enqueue(&state.pg_pool, crate::task_kinds::take_down_wait::task(&tenant, id, &payload)).await.map_err(not_started)?)
        }
        false => None,
    };
    let instances_still_on = instances_with_triggers_on(state, id).await?;
    Ok(Deactivated { answer: DeactivateResponse { report: taken.report(take_down), instances_still_on, waiting }, stops_asker })
}

/// What a deactivate did, and whether the run that asked goes with it.
pub(crate) struct Deactivated {
    pub answer: DeactivateResponse,
    pub stops_asker: bool,
}

/// Cancel in-flight activations of the named triggers (every shared
/// one by default): each activation among them ends, its setup run
/// cancelled, the way every unfinished activation ends
/// ([`end_unfinished_activation`]): triggers that held work (arrivals,
/// paused runs) land down with their door taking and every signal kept,
/// others land refusing. With no trigger and no instance named, a resync the project
/// holds is given up too, under way or stopped, with its own activation
/// still going: the triggers it had not brought back stay down, the
/// answer names them, `weft status` shows them until they are on again or
/// the next resync begins, and builds go through again.
///
/// 409 for a cancel naming triggers or an instance while a resync holds
/// the project: the resync covers every owner's triggers, so only the
/// unnamed cancel gives it up. 404 for a named trigger with no activation.
/// An activate whose task no dispatcher claimed yet, reaching a trigger
/// the cancel names, is failed before it starts
/// ([`fail_activates_not_started`]); one under way ends as every other
/// activation here, and its task answers whoever follows it with why.
///
/// 412 if none of them is activating, no activate reaching them waits to
/// start, and the project holds no resync: the user (or a stale UI)
/// clicked cancel against triggers that are already up or already down.
pub(crate) async fn cancel_activate(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    scope: &ActivationScope,
) -> Result<CancelActivateResponse, Refused> {
    actor.may_act_on(state, id).await?;
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // Every shared trigger, nothing named: the only scope a resync can be
    // given up in.
    let whole = *scope == ActivationScope::shared();
    let given_up = if whole {
        // Given up first, in one transaction with the transition landing
        // and what it leaves down written into its plan: from here its
        // task writes, takes down and turns on nothing (its lifecycle
        // writes and its claim are refused), so the activations read below
        // are every one it started.
        let plan = state.projects.give_up_resync(id).await.map_err(|e| internal("give the resync up", e))?;
        if plan.is_some() {
            crate::transition::publish_transition_changed(state, id).await;
        }
        plan
    } else {
        None
    };
    let rows = state.activations.list(id).await.map_err(|e| internal("activations", e))?;
    let mut keys = keys_to_take_down(&rows, scope)?;
    // A resync takes down and brings back the triggers of every owner it
    // names as one, so one trigger or one instance cannot leave it. Read
    // after the rows: a
    // resync only begins while nothing activates, so one found absent here
    // started none of the activations those rows hold.
    if !whole
        && state.projects.resync(id).await.map_err(|e| internal("read the resync", e))?.is_some()
    {
        return Err((
            StatusCode::CONFLICT,
            "cancel-activate: a resync holds this project, and it covers the triggers of every owner it names as one, so it \
             cannot be cancelled for some of them. `weft cancel-activate` with no --trigger or --instance gives the whole resync up \
             (and names the triggers that stay down); `weft resync --resume` takes it up again if it stopped"
                .into(),
        ));
    }
    if let Some(plan) = &given_up {
        keys.extend(plan.owners.iter().flat_map(|owner| owner.keys(&owner.down)));
    }
    let activations: std::collections::BTreeSet<uuid::Uuid> = rows
        .into_iter()
        .filter(|a| keys.contains(&a.key))
        .filter_map(|a| a.lifecycle.activating_run_id)
        .collect();
    // An activate whose task no dispatcher claimed yet holds no trigger,
    // so the rows do not show it: its task fails before it starts.
    let not_started = fail_activates_not_started(state, id, scope).await.map_err(|e| internal("cancel the activates not started", e))?;
    if activations.is_empty() && given_up.is_none() && not_started == 0 {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "cancel-activate: none of these triggers is activating".into(),
        ));
    }
    // The cause is the person's: they pressed Cancel.
    for activation in activations {
        end_unfinished_activation(state, id, activation, &weft_core::run::CancelCause::User).await?;
    }
    state
        .events
        .publish(DispatcherEvent::ProjectDeactivated { project_id: id })
        .await;
    let resync_given_up = given_up.and_then(|plan| plan.given_up).map(|given_up| given_up.left_down);
    Ok(CancelActivateResponse { resync_given_up })
}

/// Fail every `activate` task of project `id` no dispatcher claimed yet
/// whose request reaches a trigger `scope` names ([`reaches`]), so it
/// never starts: what `weft cancel-activate` does to an activate it beat
/// to its first step. Answers how many.
async fn fail_activates_not_started(state: &DispatcherState, id: uuid::Uuid, scope: &ActivationScope) -> anyhow::Result<usize> {
    let waiting: Vec<(uuid::Uuid, weft_task_store::db::JsonText<serde_json::Value>)> = state
        .pg_pool
        .all(
            "SELECT id, payload FROM task WHERE project_id = $1 AND kind = $2 AND status = 'pending'",
            &[&id, &weft_task_store::TaskKind::Activate.as_str()],
        )
        .await?;
    let mut failed = 0;
    for (task, payload) in waiting {
        let payload: crate::task_kinds::activate::ActivatePayload = serde_json::from_value(payload.0)?;
        if reaches(scope, &payload.request.scope)
            && weft_task_store::tasks::fail_pending(&state.pg_pool, task, "cancelled with `weft cancel-activate` before it started").await?
        {
            failed += 1;
        }
    }
    Ok(failed)
}

/// Whether a cancel of `cancel` reaches an activate of `asked`: some owner
/// both name, and some trigger of it both name (no trigger named is every
/// one). Pure.
fn reaches(cancel: &ActivationScope, asked: &ActivationScope) -> bool {
    use weft_core::instance::InstanceTarget::{EveryInstance, Everything, Named, Shared};
    let owners = match (&cancel.instances, &asked.instances) {
        (Everything, _) | (_, Everything) => true,
        (Shared, Shared) => true,
        (Shared, _) | (_, Shared) => false,
        (EveryInstance, _) | (_, EveryInstance) => true,
        (Named(cancel), Named(asked)) => cancel.iter().any(|instance| asked.contains(instance)),
    };
    let triggers = cancel.triggers.is_empty() || asked.triggers.is_empty() || cancel.triggers.iter().any(|trigger| asked.triggers.contains(trigger));
    owners && triggers
}

/// Prepare trigger settings without changing what is listening: which
/// triggers (every shared one by default). It runs their setup on the
/// registered build's workers, and touches no run going. The answer
/// names each owner's setup, and the parts of the baked build that do not
/// compile, which no setup that needs one ran.
pub(crate) async fn bake(state: &DispatcherState, actor: &Actor, id: uuid::Uuid, request: &BakeRequest) -> Result<BakeAnswer, Refused> {
    actor.may_act_on(state, id).await?;
    let (program, project) = coherent_definition(state, id).await?;
    let keys = resolve_scope(state, id, &project, &request.scope).await?;
    let mut baked = Vec::new();
    for (_, keys) in keys_by_owner(&keys) {
        // Once an owner's setup ran, a later one the database did not take
        // is no longer a request nothing of which ran.
        let answer = |e: Refused| if baked.is_empty() { e } else { crate::api::after_writes(e) };
        prepare_trigger_setup(state, id, &project, &keys).await.map_err(answer)?;
        let setup = run_trigger_setup(state, id, &project, &keys, &program, None, None).await.map_err(answer)?;
        baked.push(setup.summary());
    }
    Ok(BakeAnswer { baked, broken: project.broken.clone() })
}

/// `POST /projects/{id}/resync`: bring the triggers the scope names that
/// are on and behind the registered build up to it: each owner's taken
/// down as the body's answers say (each left out the default: the door
/// holds what arrives, what is held is kept and moves onto the new build,
/// the runs going on the old version finish there), then, once every owner
/// is down and the old version has nothing left running, turned back on
/// on the new build. A trigger that is off is never turned on: the
/// answer names the triggers of the build that are off. With nothing
/// behind, nothing is touched, and the answer says so.
///
/// The scope is the shared triggers by default, the named instances'
/// (`--instance`) or every instance's (`--all-instances`) otherwise.
///
/// Every refusal the request can meet up front (a build or infra change in
/// flight, another build registered since the caller's, infra a trigger's
/// setup reads that is not running, a connection not picked) comes BEFORE
/// the first take-down, so triggers it cannot bring back are left exactly
/// as they were; and the project holds its `resyncing` transition from
/// then to the end, so no build starts in between.
///
/// The take-down and the bring-back are done by a task of their own
/// (`crate::task_kinds::resync`), which carries on whatever becomes of
/// this request; the request follows it and answers as it ended. A
/// project holding a resync already (under way, or stopped with triggers
/// down) refuses this: [`resume_resync`] takes that one up again. A
/// program's resync of its own project is refused ([`refuse_own_resync`]).
///
/// What the install's maintenance took down in the project and could not
/// bring back then comes back too, on the registered build
/// (`crate::maintenance::bring_back_project`), once every refusal above
/// has passed: a project whose code did not build on the new weft is fixed
/// in its folder, and this resync is what turns it back on.
pub(crate) async fn resync(state: &DispatcherState, actor: &Actor, id: uuid::Uuid, request: &ResyncRequest) -> Result<ResyncAnswer, Refused> {
    use crate::activation_store::ProjectStatus;
    actor.may_act_on(state, id).await?;
    refuse_own_resync(actor, id)?;
    // Before anything comes down: its bring-back would be refused.
    crate::maintenance::refuse_while_on(&state.pg_pool).await?;
    let take_down = request
        .choice
        .resolve()
        .map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let tenant = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    require_registered_build(state, id, &request.build).await?;
    let bring_back = || async {
        crate::maintenance::bring_back_project(state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("bring back what the maintenance left down: {e:#}")))
    };

    // 1. Which triggers: the scope's, as the registered build has them, of
    //    which the ones on and behind it are refreshed and the ones off
    //    are named in the answer.
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    let activations = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    // Every instance with a trigger on is what `--all-instances` (and
    // `--all`, beside the shared ones) reaches here: an instance with none
    // on has nothing to refresh.
    let on = match request.scope.instances.every_instance() {
        true => instances_with_triggers_on(state, id).await?,
        false => Vec::new(),
    };
    let keys = request.scope.resolve(&registered, &on).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e:#}"));
    let binary_hash = state.projects.running_binary_hash(id).await.map_err(internal)?;
    let definition_hash = state.projects.running_definition_hash(id).await.map_err(internal)?;
    let desired = Desired::registered(state, id, &registered, binary_hash.as_deref(), definition_hash.as_deref()).await?;
    let row = |key: &weft_core::activation::ActivationKey| activations.iter().find(|a| &a.key == key);
    // Off: never turned on, or down. One being switched on is not.
    let off: Vec<weft_core::activation::ActivationKey> = keys
        .iter()
        .filter(|key| !row(key).is_some_and(|a| matches!(a.lifecycle.status, ProjectStatus::Active | ProjectStatus::Activating)))
        .cloned()
        .collect();
    let behind: Vec<weft_core::activation::ActivationKey> =
        keys.iter().filter(|key| row(key).is_some_and(|a| desired.behind(a))).cloned().collect();
    if behind.is_empty() {
        let brought_back = bring_back().await?;
        // What came back is on now, so no longer among the triggers off.
        let back: Vec<&weft_core::activation::OwnerTriggers> = brought_back.iter().flat_map(|b| &b.turned_on).collect();
        let off: Vec<weft_core::activation::ActivationKey> = off
            .into_iter()
            .filter(|key| !back.iter().any(|on| on.owner == key.owner && on.triggers.contains(&key.trigger)))
            .collect();
        return Ok(ResyncAnswer::Done(ResyncResponse { brought_back, ..nothing_to_resync(take_down, weft_core::activation::by_owner(&off)) }));
    }
    // One binary answers the project's routes: a trigger left on outside
    // this resync, on another build, would split them. Refused before
    // anything comes down.
    if let Some(binary) = binary_hash.as_deref() {
        let elsewhere = on_another_binary(&activations, &behind, binary);
        if !elsewhere.is_empty() {
            return Err((StatusCode::CONFLICT, on_another_binary_refusal(&elsewhere)));
        }
    }
    let owners: Vec<crate::task_kinds::resync::ResyncOwner> = keys_by_owner(&behind)
        .into_iter()
        .map(|(owner, keys)| crate::task_kinds::resync::ResyncOwner {
            owner,
            on: keys.into_iter().map(|key| key.trigger).collect(),
            down: Vec::new(),
        })
        .collect();

    // 2. Refuse BEFORE tearing anything down. Resync takes the triggers
    //    off and puts them back; a refusal after the first half leaves
    //    them down over a condition it could have checked while they
    //    were still up: a build or an infra change in flight, triggers
    //    mid-flip, infra a trigger's setup reads not running, a
    //    connection not picked, a setup or a fire that could never run
    //    (its cut needs a part that does not compile, a node whose
    //    metadata did not load among them, judged on the program as its
    //    worker runs it, what it left out included). Every owner is
    //    checked before the first comes down. Each activate below re-reads
    //    and re-checks; this is what keeps the project whole.
    require_action(state, id, &["resync"]).await?;
    let (_, runnable) = coherent_definition(state, id).await?;
    for owner in &owners {
        require_trigger_infra(state, id, &registered, &owner.keys(&owner.on)).await?;
        refuse_unrunnable(state, id, &runnable, &owner.keys(&owner.on)).await.map_err(|unrunnable| match unrunnable {
            Unrunnable::Cannot(why) => (StatusCode::UNPROCESSABLE_ENTITY, format!("{why}\n{RESYNC_TOOK_NOTHING_DOWN}")),
            Unrunnable::Lacks(why) => (StatusCode::PRECONDITION_FAILED, format!("{why}\n{RESYNC_TOOK_NOTHING_DOWN}")),
            Unrunnable::Unchecked(failed) => failed,
        })?;
    }
    crate::install_picks::require_activation_picks(state, id, &registered).await?;
    let brought_back = bring_back().await?;

    // 3. Write the resync down and enter the project's `resyncing`
    //    transition, which it holds until every owner's triggers are back:
    //    no build lands a version, and no other activation starts, between
    //    an owner's take-down and its turn back on. Its task does the work,
    //    owner by owner, the program's first, whatever becomes of this
    //    request.
    let plan = crate::task_kinds::resync::ResyncPlan {
        driver: uuid::Uuid::now_v7(),
        owners,
        take_down,
        build: request.build.clone(),
        resynced: Vec::new(),
        urls: Vec::new(),
        held: Default::default(),
        old_builds: Vec::new(),
        old_tokens: Vec::new(),
        switching: false,
        runs: 0,
        moved_runs: 0,
        off: weft_core::activation::by_owner(&off),
        stopped: None,
        given_up: None,
    };
    let task = crate::task_kinds::resync::task(&tenant, id, plan.driver);
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("resync transition: {e:#}"));
    let Some(task) = state.projects.try_begin_resyncing(id, &plan, task).await.map_err(internal)? else {
        // What the maintenance had left down came back already; the
        // refusal says so, since nothing else would.
        let back = match &brought_back {
            Some(back) if !back.turned_on.is_empty() || !back.started.is_empty() => {
                " (what the install's maintenance had left down came back on this build before the resync was refused)"
            }
            _ => "",
        };
        let held = state.projects.transition(id).await.map_err(internal)?.unwrap_or(crate::project_store::ProjectTransition::None);
        if let Some(why) = held.refusal() {
            return Err((StatusCode::CONFLICT, format!("cannot resync now: {why}{back}")));
        }
        require_registered_build(state, id, &request.build).await.map_err(|(status, why)| (status, format!("{why}{back}")))?;
        return Err((
            StatusCode::CONFLICT,
            format!("cannot resync now: a trigger of the project is being switched on; try again once it has landed{back}"),
        ));
    };
    crate::transition::publish_transition_changed(state, id).await;
    Ok(ResyncAnswer::UnderWay { task, brought_back })
}

/// What a resync refused for a build that cannot run its triggers adds,
/// below the parts that do not compile.
const RESYNC_TOOK_NOTHING_DOWN: &str =
    "The resync took nothing down. Fix that in the source (the message above says how), then run `weft resync` again.";

/// What a resync with nothing behind answers: nothing taken down, and the
/// triggers of the build that are off.
fn nothing_to_resync(take_down: weft_core::take_down::TakeDown, off: Vec<weft_core::activation::OwnerTriggers>) -> ResyncResponse {
    ResyncResponse {
        report: crate::take_down::Taken::default().report(take_down),
        urls: Vec::new(),
        resynced: Vec::new(),
        moved_runs: 0,
        off,
        brought_back: None,
    }
}

/// `POST /projects/{id}/resync/resume`: take the project's resync up
/// again, where it stopped, with the answers and the build it began with,
/// and follow it as [`resync`] does. One still going is followed as it
/// is. Refused when the project holds no resync (it never starts one), and
/// when it stopped because its build cannot run its triggers
/// (`crate::task_kinds::resync::Stopped::build_cannot_run`), naming what
/// does bring them back.
pub(crate) async fn resume_resync(state: &DispatcherState, actor: &Actor, id: uuid::Uuid) -> Result<ResyncAnswer, Refused> {
    actor.may_act_on(state, id).await?;
    refuse_own_resync(actor, id)?;
    crate::maintenance::refuse_while_on(&state.pg_pool).await?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("take the resync up again: {e:#}"));
    let tenant = state.projects.tenant_for(id).await.map_err(internal)?.ok_or((StatusCode::NOT_FOUND, "project not found".to_string()))?;
    // One its build cannot run would fail the same way: a resume keeps
    // that build.
    let held = state.projects.resync(id).await.map_err(internal)?;
    if let Some(stopped) = held.as_ref().and_then(|held| held.plan.stopped.as_ref()).filter(|stopped| stopped.build_cannot_run) {
        return Err((
            StatusCode::CONFLICT,
            format!("this resync stopped because its build cannot run the triggers it brings back:\n{}\n{}", stopped.error, crate::task_kinds::resync::BUILD_CANNOT_RUN_NEXT),
        ));
    }
    let Some(task) = state.projects.resume_resync(id, &tenant).await.map_err(internal)? else {
        return Err((
            StatusCode::CONFLICT,
            "this project holds no resync to take up again: it finished, or was given up with `weft cancel-activate`; \
             `weft resync` starts a new one"
                .into(),
        ));
    };
    crate::transition::publish_transition_changed(state, id).await;
    Ok(ResyncAnswer::UnderWay { task, brought_back: None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::TenantId;

    /// A cancel reaches an activate not started when they share an owner
    /// and a trigger, no trigger named being every one.
    #[test]
    fn a_cancel_reaches_an_activate_sharing_an_owner_and_a_trigger() {
        use weft_core::instance::{InstanceId, InstanceTarget};
        let scope = |triggers: &[&str], instances: InstanceTarget| ActivationScope { triggers: triggers.iter().map(|t| t.to_string()).collect(), instances };
        let ada = || InstanceTarget::Named(vec![InstanceId::new("ada").unwrap()]);
        let bob = || InstanceTarget::Named(vec![InstanceId::new("bob").unwrap()]);
        assert!(reaches(&ActivationScope::shared(), &ActivationScope::shared()));
        assert!(reaches(&ActivationScope::shared(), &scope(&["door"], InstanceTarget::Shared)));
        assert!(reaches(&scope(&["door"], InstanceTarget::Shared), &ActivationScope::shared()));
        assert!(!reaches(&scope(&["door"], InstanceTarget::Shared), &scope(&["feed"], InstanceTarget::Shared)));
        assert!(!reaches(&ActivationScope::shared(), &scope(&[], ada())), "the shared ones are not ada's");
        assert!(reaches(&scope(&[], InstanceTarget::Everything), &scope(&[], ada())));
        assert!(reaches(&scope(&[], InstanceTarget::EveryInstance), &scope(&[], ada())));
        assert!(!reaches(&scope(&[], InstanceTarget::EveryInstance), &ActivationScope::shared()));
        assert!(!reaches(&scope(&[], bob()), &scope(&[], ada())));
        assert!(reaches(&scope(&["inbox"], ada()), &scope(&[], InstanceTarget::Named(vec![InstanceId::new("bob").unwrap(), InstanceId::new("ada").unwrap()]))));
    }

    /// A program may resync another project of its owner, never its own
    /// (it would wait on itself); a person may resync any.
    #[test]
    fn a_program_cannot_resync_its_own_project() {
        let (own, other) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let program = Actor::Program { tenant: TenantId::local(), project: own, run: uuid::Uuid::now_v7() };
        let (status, why) = refuse_own_resync(&program, own).unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(why.contains("would wait on itself"), "{why}");
        assert!(refuse_own_resync(&program, other).is_ok());
        assert!(refuse_own_resync(&Actor::operator(TenantId::local()), own).is_ok());
    }
}
