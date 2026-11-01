//! A project's own actions: the owner's list of projects, making one, its
//! status, cancelling its build, removing it, and one look at a task a
//! command left to follow (`weft_core::program::project::ProjectCall`).
//!
//! A program may remove (and quiesce) any project of its owner but its
//! own: removing its own project cancels the run asking, so it is refused
//! naming why ([`refuse_own_removal`]).

use axum::http::StatusCode;
use weft_core::deactivation::Following;
use weft_core::program::project::ProjectCall;
use weft_core::program::ProgramCallOutcome;
use weft_core::projects::{
    DeclareRequest, GoingRun, ProjectDrift, ProjectRemoved, ProjectRunsSummary, ProjectStatusResponse, ProjectSummary, QuiesceRequest, Quiesced,
    StatusQuery,
};
use weft_core::ProjectDefinition;
use weft_task_store::db::Client as _;

use super::{answered, Actor, Refused};
use crate::api::project::{
    compute_available_actions, gather_action_snapshot, infra_entries, last_calls_sent, limited_entries, project_address, run_infra_ready, trigger_entries,
    trigger_runs, unanswered_for, ActionInputs, Desired, StatusError,
};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

/// This area's actions ([`super::names`]).
pub const NAMES: &[(&str, super::Reach)] = &[
    ("weft.projects.list", super::Reach::Everyone),
    ("weft.projects.get", super::Reach::Everyone),
    ("weft.projects.declare", super::Reach::Everyone),
    ("weft.projects.status", super::Reach::Everyone),
    ("weft.projects.cancel_build", super::Reach::Everyone),
    ("weft.projects.quiesce", super::Reach::Everyone),
    ("weft.projects.remove", super::Reach::Everyone),
    ("weft.projects.follow", super::Reach::Everyone),
];

/// Carry out one project call of `actor` on project `project`.
pub(crate) async fn program_call(
    state: &DispatcherState,
    actor: &Actor,
    project: uuid::Uuid,
    call: &ProjectCall,
) -> Result<ProgramCallOutcome, Refused> {
    match call {
        ProjectCall::List => answered(&list(state, actor).await?),
        ProjectCall::Get => answered(&get(state, actor, project).await.map_err(StatusError::into_parts)?),
        ProjectCall::Declare { request } => answered(&declare(state, actor, request).await?),
        ProjectCall::Status { query } => answered(&status(state, actor, project, query).await.map_err(StatusError::into_parts)?),
        ProjectCall::CancelBuild => answered(&cancel_build(state, actor, project).await?),
        ProjectCall::Quiesce { request } => answered(&quiesce(state, actor, project, request).await.map_err(StatusError::into_parts)?),
        ProjectCall::Remove { force } => answered(&remove(state, actor, project, *force).await.map_err(StatusError::into_parts)?),
        // One look that waits for nothing: the program parks on a timer
        // between looks, so no task of the dispatcher's is held open.
        ProjectCall::Follow { task } => {
            answered(&follow(state, actor, project, *task, weft_task_store::stopping::Deadline::at(tokio::time::Instant::now())).await.map_err(StatusError::into_parts)?)
        }
    }
}

/// Hold `actor` to `project` the way a marked route answers: "no project
/// you may see under this id" carries the `x-weft-not-found: project`
/// marker ([`StatusError::NotMyProject`]), so a client tells it apart from
/// a version-skewed dispatcher missing the route, and `weft rm`, which
/// retries idempotently, reads "already gone" as done.
pub(crate) async fn may_act_marked(state: &DispatcherState, actor: &Actor, project: uuid::Uuid) -> Result<(), StatusError> {
    actor.may_act_on(state, project).await.map_err(|(status, message)| match status {
        StatusCode::NOT_FOUND => StatusError::NotMyProject,
        _ => StatusError::Other(status, message),
    })
}

/// A program removing its own project: refused, since the removal cancels
/// every run of it, the one asking among them.
fn refuse_own_removal(actor: &Actor, project: uuid::Uuid) -> Result<(), StatusError> {
    match actor.is_own_project(project) {
        true => Err(StatusError::Other(
            StatusCode::FORBIDDEN,
            "a program cannot remove its own project: the removal cancels every run of it, this one among them, before it \
             finishes. Remove it from another project, or with `weft rm`"
                .into(),
        )),
        false => Ok(()),
    }
}

/// The summary of one project: its shared activations read as one
/// status (`activation_store::aggregate`), or, for a project `weft rm`
/// stopped partway through removing, that it is being removed.
pub(crate) async fn summary_of(
    state: &DispatcherState,
    summary: crate::project_store::StoredProjectSummary,
) -> Result<ProjectSummary, (StatusCode, String)> {
    let status = if summary.removing {
        crate::project_store::ProjectRemoving.to_string()
    } else {
        let activations = state
            .activations
            .list(summary.id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
        crate::activation_store::aggregate(
            activations.iter().filter(|a| a.key.owner == weft_core::instance::Owner::Shared).map(|a| &a.lifecycle),
        )
        .status
        .as_str()
        .to_string()
    };
    Ok(ProjectSummary { id: summary.id.to_string(), name: summary.name, description: summary.description, status })
}

/// Every project of the actor's owner (`weft ps`).
pub(crate) async fn list(state: &DispatcherState, actor: &Actor) -> Result<Vec<ProjectSummary>, Refused> {
    let items = state
        .projects
        .list(actor.tenant().as_str())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list projects: {e}")))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(summary_of(state, item).await?);
    }
    Ok(out)
}

/// `POST /projects`: the project exists on this install, under the
/// actor's owner, with nothing built yet. What a version needs before
/// its first build (`weft checkpoint` records one without building), and
/// the first thing a build does. Idempotent: an existing project of the
/// owner's is left as it is; one of another tenant answers as if it did
/// not exist.
pub(crate) async fn declare(state: &DispatcherState, actor: &Actor, request: &DeclareRequest) -> Result<ProjectSummary, Refused> {
    let summary = declare_project(state, request.id, &request.name, actor.tenant()).await?;
    summary_of(state, summary).await
}

/// Make sure the project row exists under `tenant`: registered with an
/// empty program and no running hashes when it does not.
pub(crate) async fn declare_project(
    state: &DispatcherState,
    id: uuid::Uuid,
    name: &str,
    tenant: &crate::tenant::TenantId,
) -> Result<crate::project_store::StoredProjectSummary, (StatusCode, String)> {
    match state.projects.tenant_for(id).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_for: {e}")))? {
        Some(owner) if owner == tenant.as_str() => {
            return state
                .projects
                .get(id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
                .ok_or((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()))?
                .unless_removing();
        }
        Some(_) => return Err((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string())),
        None => {}
    }
    let empty: ProjectDefinition = serde_json::from_value(serde_json::json!({ "id": id, "nodes": [], "edges": [] }))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("empty program: {e}")))?;
    let summary = state
        .projects
        .register_with_hashes(empty, name, "", tenant.as_str(), None, None, None, None, None, None, None, None)
        .await
        .map_err(|e| {
            if crate::project_store::ProjectRemoving::is(&e) {
                crate::project_store::ProjectRemoving::refusal()
            } else {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("declare the project: {e}"))
            }
        })?
        .ok_or((StatusCode::INTERNAL_SERVER_ERROR, "declaring the project registered nothing".to_string()))?;
    state
        .events
        .publish(DispatcherEvent::ProjectRegistered {
            project_id: summary.id,
            name: weft_core::truncate_user_string(&summary.name, 4096),
        })
        .await;
    Ok(summary)
}

/// Answers through `StatusError` so "no project you may see under this
/// id" carries the `x-weft-not-found: project` marker. A client asking
/// whether a project is known yet (the CLI's checkpoint, before it
/// registers the source) must be able to tell that apart from a
/// version-skewed dispatcher missing the route.
pub(crate) async fn get(state: &DispatcherState, actor: &Actor, id: uuid::Uuid) -> Result<ProjectSummary, StatusError> {
    may_act_marked(state, actor, id).await?;
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or(StatusError::NotMyProject)?;
    Ok(summary_of(state, summary).await?)
}

/// Aggregate view for `weft status`: registration, listener state,
/// per-node infra state, each trigger's state, a rollup of recent runs,
/// drift (against the hashes the query names, else the registered
/// build's), and the verbs valid right now. One answer, no stitching
/// required by a client.
pub(crate) async fn status(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    query: &StatusQuery,
) -> Result<ProjectStatusResponse, StatusError> {
    // The marked gate: a client asking about a project it has not
    // registered yet (a brand-new `weft run`) reads the marker as "not
    // here yet", where a headerless 404 is a version-skewed route.
    may_act_marked(state, actor, id).await?;
    // A project `weft rm` stopped partway through removing is half taken
    // down: its status says so (`removing`), describes what is left, and
    // offers no verb.
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or(StatusError::NotMyProject)?;
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        // A registered project missing its definition is a real
        // inconsistency, NOT "no such project": Other (no marker), so
        // the CLI bubbles it loudly instead of skipping the gate.
        .ok_or((StatusCode::NOT_FOUND, "project definition missing".to_string()))?;
    let listener_running = crate::listener::project_is_listening(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("listener status: {e}")))?;

    let internal = |what: &'static str| move |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let binary_hash = state.projects.running_binary_hash(id).await.map_err(internal("running_binary_hash"))?;
    let definition_hash = state.projects.running_definition_hash(id).await.map_err(internal("running_definition_hash"))?;
    let desired = Desired::of_query(state, id, &project, query, binary_hash.as_deref(), definition_hash.as_deref()).await?;
    let snapshot = gather_action_snapshot(state, id, &project, &desired).await?;
    let (infra, instance_infra) = infra_entries(&project, &snapshot.infra, &snapshot.activations, &desired);
    let traffic = crate::api::project::TriggerTraffic::of(&state.pg_pool, id, crate::lease::now_unix()).await.map_err(internal("trigger traffic"))?;
    let has_infra = snapshot.has_infra;
    let has_triggers = snapshot.has_triggers;
    let infra_rollup = snapshot.infra_rollup.clone();

    // Project-filtered, newest first: `total` is the true count of the
    // program's own runs (from SQL, not capped at a fetched window), the
    // set `weft tree` lists, and the first row is the latest for the
    // `last_*` fields. The runs that set it up are counted beside them.
    let runs_of = |phase: Option<weft_core::context::Phase>| crate::store::RunListQuery {
        limit: 1,
        offset: 0,
        project_id: Some(id),
        started_after: None,
        started_before: None,
        phase,
        entry_node: None,
        status: None,
        instance: None,
        tag: None,
        node: None,
        search: None,
        below: None,
    };
    let (program_runs, every_run) = (runs_of(Some(weft_core::context::Phase::Fire)), runs_of(None));
    let (runs, every) = tokio::try_join!(
        state.runs.list_runs(actor.tenant().as_str(), &program_runs),
        state.runs.list_runs(actor.tenant().as_str(), &every_run),
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list runs: {e}")))?;
    let last = runs.runs.first();
    let running = state
        .runs
        .going_run_ids_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs going: {e}")))?;
    // Oldest first, so the LAST is the most recently started. The
    // editor's action bar follows "the latest run" and nothing else on
    // the wire says which that is; sorting these by id, as this used
    // to, made "latest" mean whichever uuid happened to sort highest.
    let running: Vec<GoingRun> = running
        .into_iter()
        .map(|(run_id, phase)| GoingRun { run_id: run_id.to_string(), phase })
        .collect();
    let runs = ProjectRunsSummary {
        total: runs.total as usize,
        setup: every.total.saturating_sub(runs.total) as usize,
        last_completed_at: last.and_then(|l| l.completed_at),
        last_run_id: last.map(|l| l.run_id.to_string()),
        last_status: last.map(|l| l.status),
        running,
    };

    let drift = ProjectDrift {
        infra_drift: infra.iter().any(|entry| entry.behind) || instance_infra.iter().any(|entry| entry.behind),
        binary_drift: desired.binary_drifts(binary_hash.as_deref()),
        definition_drift: desired.definition.as_deref().zip(definition_hash.as_deref()).is_some_and(|(want, have)| want != have),
    };
    let available_actions = compute_available_actions(&ActionInputs {
        removing: summary.removing,
        run_infra_ready: run_infra_ready(has_infra, &infra_rollup),
        infra_behind: drift.infra_drift,
        ..snapshot.inputs()
    });
    let tenant = crate::tenant::TenantId(crate::instance_values::owning_tenant(state, id).await?);
    let instances = crate::actions::instances::instances(state, &tenant, id).await?;

    Ok(ProjectStatusResponse {
        id,
        name: summary.name,
        removing: summary.removing,
        status: snapshot.lifecycle.status,
        transition: snapshot.transition,
        listener_running,
        has_triggers,
        infra,
        runs,
        has_infra,
        built: binary_hash.is_some(),
        builds: crate::build::ledger::running(&state.pg_pool, Some(id))
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running builds: {e:#}")))?
            .into_iter()
            // A build not made on the builder yet has no id there to name.
            .filter_map(|b| {
                Some(weft_core::projects::BuildInFlight {
                    image: b.image_ref,
                    build: b.builder_id?,
                    started_at_unix: b.started_at,
                    log_url: b.log_url,
                })
            })
            .collect(),
        address: project_address(state, id).await,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift,
        available_actions,
        instance_infra,
        triggers: trigger_entries(&project, &snapshot.activations, &traffic, &desired, binary_hash.as_deref(), crate::lease::now_unix()),
        instances,
        limited: limited_entries(state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit refusals: {e}")))?,
        trigger_runs: trigger_runs(state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("run counts: {e}")))?,
        unanswered: unanswered_for(state, id).await?,
        resync: snapshot.resync.as_ref().map(crate::task_kinds::resync::ResyncState::standing),
    })
}

/// `POST /projects/{id}/cancel-build`, `ctx.project(..).cancel_build()`. Cancel the in-flight build:
/// `transition` → cancelling_build, then every image build the project
/// started is stopped and its waiting version ends cancelled
/// (`VersionBuilder::cancel_project`), and the transition lands at rest
/// once nothing holds it (`crate::transition::settle`). A build request
/// still starting its builds is refused its next claim, and stops what it
/// started itself. Cancel reconciles, never asserts: the
/// response is 202 and the displayed state is whatever the backend
/// reports next (a build may still complete if it beat the cancel).
///
/// 412 when no build is in flight (stale tab; the client refetches
/// `/status` and reconciles).
pub(crate) async fn cancel_build(state: &DispatcherState, actor: &Actor, id: uuid::Uuid) -> Result<(), Refused> {
    actor.may_act_on(state, id).await?;
    let flipped = state
        .projects
        .request_cancel_build(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("request_cancel_build: {e}")))?;
    if !flipped {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "no build in flight to cancel".into(),
        ));
    }
    crate::transition::publish_transition_changed(state, id).await;
    state
        .builder
        .cancel_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("stop the project's builds: {e:#}")))?;
    crate::transition::settle(state, Some(id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("settle the build transition: {e:#}")))?;
    Ok(())
}

/// `POST /projects/{id}/quiesce`: `weft rm`'s first step. Mark the
/// project being removed, so no run of it is born from here on, take it
/// down whole (every owner's triggers down with nothing kept, every live
/// run cancelled, those started by hand and unrecorded ones included),
/// and answer the task that waits for what that cancelled to be over
/// (`crate::task_kinds::removal_wait`): the cancelled runs' last calls,
/// and, under `everyRun` (`weft rm --journal`, before it erases the
/// project's history, so nothing is still writing a run it erases), every
/// run ending. The person follows it in short looks however long it takes.
pub(crate) async fn quiesce(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    request: &QuiesceRequest,
) -> Result<Quiesced, StatusError> {
    // Marked gate: rm tells "already gone" apart from a routing 404.
    may_act_marked(state, actor, id).await?;
    refuse_own_removal(actor, id)?;
    let marked = state
        .projects
        .mark_removing(id)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("start removing the project", e))?;
    if !marked || !deactivate_project(state, id).await? {
        return Err(StatusError::NotMyProject);
    }
    let not_started = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("the project is down; the wait for what it cancelled could not start: {e:#}; retry `weft rm`"));
    let tenant = state.projects.tenant_for(id).await.map_err(not_started)?.ok_or(StatusError::NotMyProject)?;
    let payload = weft_task_store::RemovalWaitPayload { every_run: request.every_run };
    let waiting = weft_task_store::tasks::enqueue(&state.pg_pool, crate::task_kinds::removal_wait::task(&tenant, id, &payload)).await.map_err(not_started)?;
    Ok(Quiesced { waiting })
}

/// `DELETE /projects/{id}` (`weft rm`, `ctx.project(..).remove()`, once
/// [`quiesce`] took the project down): remove the project and everything
/// kept for it. `force` (`weft rm --force`) skips the supervisor
/// terminate-wait window and drops the project's infra rows straight away,
/// and goes on past cloud leftovers it names: for a supervisor wedged and
/// a person who wants the project gone now. It never goes past workers
/// that cannot be killed: nothing is erased while one runs.
pub(crate) async fn remove(state: &DispatcherState, actor: &Actor, id: uuid::Uuid, force: bool) -> Result<ProjectRemoved, StatusError> {
    // Marked gate: rm is a delete the CLI retries idempotently, so it reads
    // "already gone" as the end state it wanted.
    may_act_marked(state, actor, id).await?;
    refuse_own_removal(actor, id)?;
    // Marked first: no run of the project is born from here on, so the
    // take-down below reaches every run it will ever have. A failed `weft
    // rm` leaves it marked, refusing runs, until `weft rm` is run again.
    let marked = state
        .projects
        .mark_removing(id)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("start removing the project", e))?;
    if !marked {
        // A concurrent rm dropped the row after our gate.
        return Err(StatusError::NotMyProject);
    }
    // Deactivate first: cancels any in-flight runs, unregisters
    // every wake signal (entry + resume) from the listener processes holding them,
    // drops entry tokens. If deactivate fails on a DB / listener
    // error we must abort: removing the project row while signals
    // remain in the listener leaves dangling registrations that
    // outlive their owning project. `weft rm` took the project down and
    // waited for what it cancelled already (`quiesce`), so both are
    // over at once then; they are here for a removal asked on its own.
    deactivate_project(state, id).await?;
    // From here on the project is down: part of the removal ran, so a
    // failure is never the 503 that says nothing did
    // (`weft_core::install::asks_again`); `weft rm` again goes on from
    // where it stands.
    last_calls_sent(state, id)
        .await
        .map_err(|e| StatusError::Other(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}; retry `weft rm`")))?;
    // The project's OWNING tenant is its `project.tenant_id`, the same source that
    // keyed everything of the project's that lives outside the project row: its
    // stored data, and the connections its nodes published. `tenant_router` is a
    // request-routing lookup (the default returns `local` for every project), NOT
    // the resource owner, so using it here would key those by the wrong tenant and
    // find nothing.
    //
    // Resolved ONCE, here, before anything starts deleting: every step below needs
    // it, and a lookup repeated after a step has already run could come back empty
    // and leave that step silently skipped.
    let tenant = state
        .projects
        .tenant_for(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("tenant_for: {e:#}")))?
        // The row vanished mid-remove (a concurrent rm won): that IS
        // rm's desired end state, so it gets the marker too.
        .ok_or(StatusError::NotMyProject)?;
    // Its workers, killed outright before anything of the project is
    // erased: one left alive would keep serving from what it remembers of
    // a project that is gone (the programs it holds as on record among
    // them). Not before the take-down above, which ends every run of the
    // project: a run still waking up could start a fresh worker after the
    // kill. `--force` does not get past this one: what is still running is
    // named, with how to stop it by hand.
    state.runner.kill_workers(tenant.as_str(), id).await.map_err(|e| StatusError::Other(
        StatusCode::BAD_GATEWAY,
        format!("the project's workers could not be stopped, so none of its data was erased: {e:#}; once they are stopped, run `weft rm` again"),
    ))?;
    // Its frontends: one whose service cannot be removed stops the
    // removal with everything else still whole (`--force` forgets it, and
    // the answer names what stays on the cloud).
    let mut left = crate::frontends::remove_project(state, &tenant, id, force).await.map_err(|e| {
        (StatusCode::BAD_GATEWAY, format!("remove the project's frontends: {e:#}; retry `weft rm`, or `weft rm --force`"))
    })?;
    // What the platform keeps for the project once its workers are gone
    // (on a cloud, the account they ran as), the same way: what cannot be
    // removed stops the removal, unless `--force`, whose answer names
    // what stays.
    match state.runner.retire(tenant.as_str(), id).await {
        Ok(()) => {}
        Err(e) if force => left.push(format!("what the platform keeps for the project's workers: {e:#}")),
        Err(e) => {
            return Err(StatusError::Other(
                StatusCode::BAD_GATEWAY,
                format!("remove what the platform keeps for the project's workers: {e:#}; retry `weft rm`, or `weft rm --force`"),
            ))
        }
    }
    // Tear down infra: issues a supervisor terminate command, waits
    // up to 120s for completion (unless --force), then drops all
    // infra_* rows. MUST succeed: if any of the DB cascade writes fail,
    // the project row stays (so a retry replays cleanly).
    crate::api::infra::delete_project(state, id, &tenant, force).await?;
    // Its images stop counting as anybody's builds: the ones no other
    // project names become unused, and the pass asked for here reclaims
    // them (`crate::build::prune::AfterBuildPrunes`).
    crate::build::ledger::forget_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    // Its version builds go too, a waiting one's claim with it.
    crate::build::waiting::forget_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    state.builder.prunes.request(state, None);
    // Reclaim the project's stored data through the ONE hook. The default
    // frees the project's `project/`- and `instance/`-scoped runtime files; a
    // reclaimer that also stores project content elsewhere (a versioned
    // project-editing history) extends it. `shared/`-scoped runtime files are the owner's and are
    // deliberately NOT touched (they outlive the project). MUST run before the
    // project row is dropped: a row-cascade would otherwise erase the bookkeeping
    // this reclaim reads, stranding bytes. A failure aborts the rm (a retry
    // replays cleanly).
    state
        .project_reclaimer
        .reclaim(state, tenant.as_str(), id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                // `{e:#}` prints the full anyhow cause chain, so the real reason
                // (a SQL error, a ledger inconsistency, a missing tree) reaches the
                // caller instead of just the top wrapper.
                format!("could not reclaim the project's stored data: {e:#}; retry `weft rm`"),
            )
        })?;
    // The row and everything kept under it, in one transaction: its runs,
    // its programs and version tree, its signals and queued work, its
    // infra rows and lease. A failure removes nothing, so the person runs
    // `weft rm` again. Its domains go with the row, and the platform's
    // door follows the rows on its own (`crate::domains::drain_loop`).
    let removed = state
        .projects
        .remove(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("could not remove the project: {e:#}; retry `weft rm`")))?;
    match removed {
        Some(signals) => {
            // The listener lets go of what it held for the project.
            state.listener.unregister_many(&signals).await;
            Ok(ProjectRemoved { left })
        }
        // A concurrent rm dropped the row after our gate: still rm's
        // desired end state, so it gets the marker.
        None => Err(StatusError::NotMyProject),
    }
}

/// Take the whole project down for good: every activation of every
/// owner taken down with nothing kept, and every live run cancelled
/// (project removal). Returns true if the project existed.
async fn deactivate_project(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<bool, (StatusCode, String)> {
    let taken = crate::take_down::take_down(
        state,
        id,
        &crate::take_down::TakeDownTarget::WholeProject,
        &weft_core::take_down::TakeDown::REMOVE,
        crate::take_down::DownCause::Person,
        crate::take_down::TakeDownAsker::Outside,
    )
    .await?;
    Ok(taken != crate::take_down::TakenDown::ProjectGone)
}

/// `GET /projects/{id}/follow/{task}?wait_ms=<n>`: one look at a task a
/// person (or a program) follows (an activate, a resync, a deactivate's
/// wait for its runs): it waits
/// at most `look` (a route's `n` milliseconds, never past
/// [`crate::api::HoldQuery`]'s cap; nothing for a program) for the
/// task to end, then answers that it goes on, or what it ended with
/// ([`weft_core::deactivation::Following`]). A resync that stopped is
/// answered as the refusal it stopped with: the triggers it had not
/// brought back down, the project held for it until `weft resync
/// --resume` takes it up again or `weft cancel-activate` gives it up. An
/// activate answers what the activation answered, or its refusal
/// (`crate::task_kinds::activate::followed`), and while it goes, where
/// the activation it drives is. The person looks again for as long as
/// they follow; Ctrl+C stops following and never the task.
pub(crate) async fn follow(
    state: &DispatcherState,
    actor: &Actor,
    id: uuid::Uuid,
    task: uuid::Uuid,
    look: weft_task_store::stopping::Deadline,
) -> Result<Following, StatusError> {
    actor.may_act_on(state, id).await?;
    let internal = |what: &str, e: String| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e}"));
    let outcome = weft_task_store::terminal::wait_for_terminal_in_project_before(&state.pg_pool, &state.signals, task, id, look)
        .await
        .map_err(|e| internal("follow the task", format!("{e:#}")))?
        .ok_or((StatusCode::NOT_FOUND, format!("project {id} has no task {task} to follow")))?;
    let kind = || async {
        let kind: Option<String> = state
            .pg_pool
            .scalar_opt("SELECT kind FROM task WHERE id = $1 AND project_id = $2", &[&task, &id])
            .await
            .map_err(|e| internal("read the task", e.to_string()))?;
        Ok::<_, StatusError>(kind.as_deref().and_then(weft_task_store::TaskKind::parse))
    };
    use weft_task_store::TaskKind;
    match outcome.status {
        weft_core::task::TaskStatus::Pending | weft_core::task::TaskStatus::Claimed => {
            let said = || async {
                weft_task_store::tasks::progress(&state.pg_pool, task).await.map_err(|e| internal("read where the task is", format!("{e:#}")))
            };
            let turning_on = || async {
                crate::task_kinds::activate::progress(state, id, task).await.map_err(|e| internal("read where the activation is", format!("{e:#}")))
            };
            Ok(Following::Going(match kind().await? {
                Some(TaskKind::Activate) => turning_on().await?.map(serde_json::to_value).transpose().map_err(|e| internal("where the activation is", e.to_string()))?,
                // What the resync's task said, with the activation it
                // drives now: the bring-back of an owner can take minutes.
                Some(TaskKind::Resync) => {
                    let progress: weft_core::deactivation::ResyncProgress = match said().await? {
                        Some(said) => serde_json::from_value(said).map_err(|e| internal("read where the resync is", e.to_string()))?,
                        None => Default::default(),
                    };
                    let going = weft_core::deactivation::ResyncGoing { progress, turning_on: turning_on().await? };
                    Some(serde_json::to_value(going).map_err(|e| internal("where the resync is", e.to_string()))?)
                }
                _ => said().await?,
            }))
        }
        weft_core::task::TaskStatus::Complete => {
            let ended = match kind().await? {
                Some(TaskKind::Activate) => crate::task_kinds::activate::followed(outcome.result),
                Some(TaskKind::StartRun) => crate::task_kinds::start_run::followed(outcome.result),
                _ => Ok(outcome.result.unwrap_or(serde_json::Value::Null)),
            };
            Ok(Following::Done(ended.map_err(|(status, why)| StatusError::Other(status, why))?))
        }
        weft_core::task::TaskStatus::Failed => {
            let error = outcome.error.unwrap_or_else(|| "it ended without saying why".into());
            if kind().await? != Some(weft_task_store::TaskKind::Resync) {
                return Err(StatusError::Other(StatusCode::CONFLICT, error));
            }
            let held = state.projects.resync(id).await.map_err(|e| internal("read the resync", format!("{e:#}")))?;
            let (status, message) = resync_stopped(held.and_then(|held| held.plan.stopped), error);
            Err(StatusError::Other(status, message))
        }
    }
}

/// What a resync that ended without finishing answers: why it stopped, and
/// what goes on by itself and what a person does, when it recorded a stop;
/// its task's error otherwise.
fn resync_stopped(stopped: Option<crate::task_kinds::resync::Stopped>, error: String) -> (StatusCode, String) {
    match stopped {
        Some(stopped) => (
            StatusCode::from_u16(stopped.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            format!(
                "{}\nThe resync stopped with these triggers down, and holds the project until it finishes: {}",
                stopped.error,
                crate::task_kinds::resync::what_next(Some(&stopped))
            ),
        ),
        None => (StatusCode::CONFLICT, error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::TenantId;

    /// A program may remove another project of its owner, never its own;
    /// a person may remove any.
    #[test]
    fn a_program_cannot_remove_its_own_project() {
        let (own, other) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let program = Actor::Program { tenant: TenantId::local(), project: own, run: uuid::Uuid::now_v7(), call: uuid::Uuid::now_v7() };
        let (status, why) = refuse_own_removal(&program, own).unwrap_err().into_parts();
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(why.contains("cannot remove its own project"), "{why}");
        assert!(refuse_own_removal(&program, other).is_ok());
        assert!(refuse_own_removal(&Actor::operator(TenantId::local()), own).is_ok());
    }
}
