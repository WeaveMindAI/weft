//! The install's maintenance: everything switched off while weft itself is
//! updated (`weft_core::maintenance`), so an older and a newer weft never
//! run side by side.
//!
//! `weft maintenance on` ([`on`]) records the maintenance (one row in
//! `maintenance`, which every verb that turns something on reads), then
//! project by project takes down every trigger that is on with its door
//! refusing (`MaintenanceOnRequest::take_down`), marking each
//! (`trigger_activation.went_down_with = 'maintenance'`, through
//! [`DownCause::Maintenance`]), and stops every infra copy that runs,
//! recording each (`maintenance_copy`) before its stop is issued. The
//! triggers go down first, so the infra stop finds none of them on and
//! leaves their mark as it is (an infra take-down only takes down what is
//! still on, `crate::activation_store`). A second `on` adds what is on
//! now, and clears nothing.
//!
//! While the row is there nothing is turned on: an activation's claim
//! refuses every claimant but the lift (`crate::activation_store::Claimant`),
//! an infra start every start but the lift's (`crate::api::infra::SyncKind`),
//! and a resync or a run started by hand is refused at its request. A
//! trigger it took down takes no work, as any whose door refuses, so
//! nothing of it starts a worker or keeps a project's front up; what it
//! held already waits for the lift. A person may still take things down: a deactivate clears a trigger's
//! mark, so it stays off after the lift, and a copy terminated meanwhile
//! is skipped by it.
//!
//! `weft maintenance off` ([`off`]) runs the lift as a task of its own
//! (`crate::task_kinds::maintenance_lift`): project by project, the
//! project is built again on this weft from the request its running build
//! came from (`ProjectStoreOps::running_build`) and registered,
//! the recorded copies are started on that build and waited for, then the
//! marked triggers are brought back on it ([`ProjectLift::steps`]), each
//! record and mark going as what it names comes back. A project whose
//! running build an older weft registered kept no request: it comes back
//! on that build, and the report names the `weft resync` that builds it
//! again.
//!
//! One project that cannot come back (its code does not build on this
//! weft, a trigger's setup fails) never holds the others: the lift tries
//! every project, then ends the maintenance ([`end`]). What that project
//! had taken down stays recorded, and is its own: once its code is fixed,
//! its next `weft resync` brings it back ([`bring_back_project`]), and so
//! does another `weft maintenance off`.
//!
//! Nothing here keeps anything up: `on` is one request, and the lift is
//! one task that ends.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use weft_task_store::db::{Client, Pool};

use weft_core::activation::ActivationKey;
use weft_core::infra::wire::CopyRef;
use weft_core::instance::{InstanceId, Owner};
use weft_core::maintenance::{
    Going, LiftAnswer, LiftAt, LiftDoing, LiftProgress, LiftReport, MaintenanceOn, MaintenanceOnRequest, MaintenanceState, ProjectLeft, ProjectLifted, ProjectWentDown,
};
use weft_core::take_down::{Running, TakeDown};

use crate::activation_store::{DownCause, ProjectStatus};
use crate::authenticator::CallerTenant;
use crate::infra_node::InfraNodeStatus;
use crate::state::DispatcherState;

type ApiError = (StatusCode, String);

/// The maintenance tables. The canonical CREATE lives here, edited in
/// place; an existing database is carried to it by a migration written
/// with `./setup.sh --migration <name> --release`.
pub static GROUP: weft_task_store::SchemaGroup = weft_task_store::SchemaGroup {
    name: "maintenance",
    tables: &["maintenance", "maintenance_copy"],
    ddl: &[
        r#"CREATE TABLE IF NOT EXISTS maintenance (
            -- One row while the install is in maintenance: `weft
            -- maintenance on` writes it, the lift deletes it once nothing
            -- is left to bring back. Every verb that turns something on
            -- reads it.
            one BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (one),
            since_unix BIGINT NOT NULL
        )"#,
        r#"CREATE TABLE IF NOT EXISTS maintenance_copy (
            -- An infra copy `weft maintenance on` stopped, which the lift
            -- starts again: its project, its node's place as
            -- `infra_node.node_id` spells it, and the instance whose copy
            -- it is (NULL for the shared one).
            project_id UUID NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            node_id TEXT NOT NULL,
            instance_id TEXT,
            stopped_unix BIGINT NOT NULL
        )"#,
        r#"CREATE UNIQUE INDEX IF NOT EXISTS maintenance_copy_key
             ON maintenance_copy (project_id, node_id, instance_id) NULLS NOT DISTINCT"#,
    ],
    seed: &[],
    data_steps: &[],
};

// =================================================================
// The tables
// =================================================================

/// Whether the install is in maintenance.
pub async fn is_on(db: &impl Client) -> anyhow::Result<bool> {
    Ok(db.scalar("SELECT EXISTS (SELECT 1 FROM maintenance)", &[]).await?)
}

/// Refuse a verb that turns something on while the install is in
/// maintenance, naming the way out.
pub(crate) async fn refuse_while_on(pool: &Pool) -> Result<(), ApiError> {
    match is_on(pool).await {
        Ok(false) => Ok(()),
        Ok(true) => Err((StatusCode::CONFLICT, weft_core::maintenance::IN_MAINTENANCE.to_string())),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("read whether the install is in maintenance: {e:#}"))),
    }
}

/// Record the install as in maintenance from `now_unix`, unless it is
/// already: answers since when it is, the first record's time.
pub async fn begin(pool: &Pool, now_unix: i64) -> anyhow::Result<i64> {
    pool.execute("INSERT INTO maintenance (since_unix) VALUES ($1) ON CONFLICT (one) DO NOTHING", &[&now_unix]).await?;
    since(pool).await?.ok_or_else(|| anyhow::anyhow!("the maintenance row went as it was written"))
}

/// Since when the install is in maintenance, if it is.
pub async fn since(pool: &Pool) -> anyhow::Result<Option<i64>> {
    Ok(pool.scalar_opt("SELECT since_unix FROM maintenance", &[]).await?)
}

/// One infra copy the maintenance stopped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordedCopy {
    pub project_id: uuid::Uuid,
    /// The node's place, as `infra_node.node_id` spells it.
    pub node: String,
    pub instance: Option<InstanceId>,
}

/// Record `nodes`, `instance`'s copies of `project_id`, as stopped by the
/// maintenance. One already recorded stays as it is.
pub async fn record_copies(
    pool: &Pool,
    project_id: uuid::Uuid,
    instance: Option<&InstanceId>,
    nodes: &[String],
    now_unix: i64,
) -> anyhow::Result<()> {
    pool.execute(
        "INSERT INTO maintenance_copy (project_id, node_id, instance_id, stopped_unix) \
         SELECT $1::uuid, n, $3::text, $4::bigint FROM unnest($2::text[]) AS n \
         ON CONFLICT (project_id, node_id, instance_id) DO NOTHING",
        &[&project_id, &nodes, &instance.map(InstanceId::as_str), &now_unix],
    )
    .await?;
    Ok(())
}

/// Forget `nodes`, `instance`'s copies of `project_id`: they came back, or
/// there is nothing to bring back.
pub async fn forget_copies(pool: &Pool, project_id: uuid::Uuid, instance: Option<&InstanceId>, nodes: &[String]) -> anyhow::Result<()> {
    pool.execute(
        "DELETE FROM maintenance_copy \
         WHERE project_id = $1 AND node_id = ANY($2) AND instance_id IS NOT DISTINCT FROM $3",
        &[&project_id, &nodes, &instance.map(InstanceId::as_str)],
    )
    .await?;
    Ok(())
}

/// Forget the maintenance's mark on `triggers`, `instance`'s triggers of
/// project `project_id` that are still down with it: the lift has nothing
/// to bring back for them (the program no longer has them).
pub async fn forget_marks(pool: &Pool, project_id: uuid::Uuid, instance: Option<&InstanceId>, triggers: &[String]) -> anyhow::Result<()> {
    pool.execute(
        "UPDATE trigger_activation SET went_down_with = NULL \
         WHERE project_id = $1 AND trigger = ANY($2) AND instance_id IS NOT DISTINCT FROM $3 \
           AND went_down_with = 'maintenance' AND status = 'inactive'",
        &[&project_id, &triggers, &instance.map(InstanceId::as_str)],
    )
    .await?;
    Ok(())
}

/// Every copy the maintenance stopped and the lift has not started yet.
pub async fn recorded_copies(pool: &Pool) -> anyhow::Result<Vec<RecordedCopy>> {
    let rows: Vec<(uuid::Uuid, String, Option<String>)> = pool
        .all("SELECT project_id, node_id, instance_id FROM maintenance_copy ORDER BY project_id, node_id, instance_id NULLS FIRST", &[])
        .await?;
    rows.into_iter()
        .map(|(project_id, node, instance)| {
            let instance = instance.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("maintenance_copy.instance_id: {e}"))?;
            Ok(RecordedCopy { project_id, node, instance })
        })
        .collect()
}

/// Every trigger the maintenance took down that is still down, with its
/// project. One a person took down since carries no mark any more.
// SYNC: 'maintenance' <-> weft_core::projects::DownWith::Maintenance
pub async fn marked_triggers(pool: &Pool) -> anyhow::Result<Vec<(uuid::Uuid, ActivationKey)>> {
    let rows: Vec<(uuid::Uuid, String, Option<String>)> = pool
        .all(
            "SELECT project_id, trigger, instance_id FROM trigger_activation \
             WHERE went_down_with = 'maintenance' AND status = 'inactive' \
             ORDER BY project_id, trigger, instance_id NULLS FIRST",
            &[],
        )
        .await?;
    rows.into_iter()
        .map(|(project, trigger, instance)| {
            let instance = instance.map(InstanceId::new).transpose().map_err(|e| anyhow::anyhow!("trigger_activation.instance_id: {e}"))?;
            Ok((project, ActivationKey::new(trigger, Owner::from_instance(instance))))
        })
        .collect()
}

/// End the maintenance: every verb that turns something on goes through
/// again. What a project could not bring back stays recorded beside it,
/// for its own next lift.
pub async fn end(pool: &Pool) -> anyhow::Result<()> {
    pool.execute("DELETE FROM maintenance", &[]).await?;
    Ok(())
}

/// What is on across `tenant`'s projects right now. The infra copies are
/// read the way `weft status` reads them (`crate::infra_node::observe`):
/// a stop the supervisor has not reached yet reads `stopping`, a start
/// `provisioning`, though the row still says what the copy was.
// SYNC: the run states <-> crate::store::postgres::GROUP
pub async fn going(pool: &Pool, projects: &dyn crate::project_store::ProjectStoreOps, tenant: &str) -> anyhow::Result<Going> {
    let (triggers_on, runs): (i64, i64) = pool
        .one(
            "SELECT \
               (SELECT count(*) FROM trigger_activation a JOIN project p ON p.id = a.project_id \
                 WHERE p.tenant_id = $1 AND a.status IN ('active', 'activating')), \
               (SELECT count(*) FROM run r JOIN project p ON p.id = r.project_id \
                 WHERE p.tenant_id = $1 AND r.state IN ('queued', 'running'))",
            &[&tenant],
        )
        .await?;
    let count = |n: i64| u64::try_from(n).unwrap_or_default();
    let mut going = Going { triggers_on: count(triggers_on), runs: count(runs), ..Going::default() };
    for project in projects.list(tenant).await? {
        // Gone since it was listed: nothing of it is on.
        let Some(definition) = projects.project(project.id).await? else { continue };
        let observed = crate::infra_node::observe(pool, project.id, &definition).await?;
        let (up, changing) = copies_going(&observed);
        going.copies_up += up;
        going.copies_changing += changing;
    }
    Ok(going)
}

/// How many of `observed`'s copies are up (running, below their
/// readiness, or starting) and how many are going down (being stopped or
/// terminated).
fn copies_going(observed: &crate::infra_node::ObservedCopies) -> (u64, u64) {
    let (mut up, mut changing) = (observed.starting.len() as u64, 0);
    for row in &observed.rows {
        match row.status {
            InfraNodeStatus::Running | InfraNodeStatus::Flaky | InfraNodeStatus::Provisioning => up += 1,
            InfraNodeStatus::Stopping | InfraNodeStatus::Terminating => changing += 1,
            InfraNodeStatus::Stopped | InfraNodeStatus::Failed => {}
        }
    }
    (up, changing)
}

/// Whether a lift is under way: its task is queued or claimed.
async fn lift_under_way(pool: &Pool) -> anyhow::Result<Option<uuid::Uuid>> {
    Ok(pool
        .scalar_opt(
            "SELECT id FROM task WHERE kind = $1 AND status IN ('pending', 'claimed') LIMIT 1",
            &[&weft_task_store::TaskKind::MaintenanceLift.as_str()],
        )
        .await?)
}

// =================================================================
// What a lift does, in order
// =================================================================

/// One project's part of a lift.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectLift {
    pub project_id: uuid::Uuid,
    /// The copies to start, by owner (one start per owner's copies).
    pub copies: BTreeMap<Owner, Vec<String>>,
    /// The triggers to bring back, by owner (one activation per owner).
    pub triggers: BTreeMap<Owner, Vec<String>>,
}

/// What a lift of `copies` and `triggers` does, project by project.
pub fn plan(copies: Vec<RecordedCopy>, triggers: Vec<(uuid::Uuid, ActivationKey)>) -> Vec<ProjectLift> {
    let mut by_project: BTreeMap<uuid::Uuid, ProjectLift> = BTreeMap::new();
    fn of(by_project: &mut BTreeMap<uuid::Uuid, ProjectLift>, project_id: uuid::Uuid) -> &mut ProjectLift {
        by_project.entry(project_id).or_insert_with(|| ProjectLift { project_id, ..Default::default() })
    }
    for copy in copies {
        of(&mut by_project, copy.project_id).copies.entry(Owner::from_instance(copy.instance)).or_default().push(copy.node);
    }
    for (project_id, key) in triggers {
        of(&mut by_project, project_id).triggers.entry(key.owner).or_default().push(key.trigger);
    }
    by_project.into_values().collect()
}

/// What the next lift does, as the tables stand.
pub async fn left(pool: &Pool) -> anyhow::Result<Vec<ProjectLift>> {
    Ok(plan(recorded_copies(pool).await?, marked_triggers(pool).await?))
}

// =================================================================
// The API
// =================================================================

/// `GET /install/maintenance`: whether the install is in maintenance, what
/// a lift brings back, and what is on.
pub async fn state(State(state): State<DispatcherState>, caller: CallerTenant) -> Result<Json<MaintenanceState>, ApiError> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the maintenance: {e:#}"));
    let tenant = caller.0.as_str();
    let names = project_names(&state, tenant).await?;
    let left = left(&state.pg_pool)
        .await
        .map_err(internal)?
        .into_iter()
        .map(|lift| ProjectLeft {
            id: lift.project_id,
            name: names.get(&lift.project_id).cloned().unwrap_or_default(),
            copies: copy_refs(&lift.copies),
            triggers: owner_triggers(&lift.triggers),
        })
        .collect();
    Ok(Json(MaintenanceState {
        since_unix: since(&state.pg_pool).await.map_err(internal)?,
        left,
        going: going(&state.pg_pool, state.projects.as_ref(), tenant).await.map_err(internal)?,
    }))
}

/// `POST /install/maintenance` (`weft maintenance on`): record the
/// maintenance, then take down what is on in every project. Refused while
/// a lift is under way, which brings things back as this would take them
/// down.
pub async fn on(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    body: Option<Json<MaintenanceOnRequest>>,
) -> Result<Json<MaintenanceOn>, ApiError> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let request = body.map(|Json(b)| b).unwrap_or_default();
    if let Some(task) = lift_under_way(&state.pg_pool).await.map_err(|e| internal("look for a lift under way", e))? {
        return Err((
            StatusCode::CONFLICT,
            format!("the maintenance is being lifted (task {task}); follow it with `weft maintenance off`, then run this again"),
        ));
    }
    let since_unix = begin(&state.pg_pool, crate::lease::now_unix()).await.map_err(|e| internal("record the maintenance", e))?;
    let take_down = request.take_down();
    let mut projects = Vec::new();
    for project in state.projects.list(caller.0.as_str()).await.map_err(|e| internal("list the projects", e))? {
        // `weft rm` is taking it down already.
        if project.removing {
            continue;
        }
        let went = take_project_down(&state, project.id, project.name, &take_down).await;
        if !(went.triggers.is_empty() && went.copies.is_empty() && went.runs_cancelled == 0 && went.refused.is_empty()) {
            projects.push(went);
        }
    }
    Ok(Json(MaintenanceOn { since_unix, projects }))
}

/// `POST /install/maintenance/off` (`weft maintenance off`): start the
/// lift as a task of its own, or follow the one under way. Once the
/// maintenance is over, a project that could not come back still has its
/// records, and this lifts those again.
pub async fn off(State(state): State<DispatcherState>, caller: CallerTenant) -> Result<Json<LiftAnswer>, ApiError> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let on = is_on(&state.pg_pool).await.map_err(|e| internal("read whether the install is in maintenance", e))?;
    if !on && left(&state.pg_pool).await.map_err(|e| internal("read what the maintenance left", e))?.is_empty() {
        return Ok(Json(LiftAnswer::NotOn));
    }
    let task = weft_task_store::tasks::enqueue_dedup(&state.pg_pool, crate::task_kinds::maintenance_lift::task(caller.0.as_str()))
        .await
        .map_err(|e| internal("start the lift", e))?;
    Ok(Json(LiftAnswer::UnderWay { task: task.id() }))
}

/// `GET /install/maintenance/follow/{task}?wait_ms=<n>`: one look at a
/// lift, the way `/projects/{id}/follow/{task}` looks at a project's task:
/// it goes on, or what it ended with.
pub async fn follow(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
    Path(task): Path<uuid::Uuid>,
    Query(hold): Query<crate::api::HoldQuery>,
) -> Result<Json<weft_core::deactivation::Following>, ApiError> {
    let kind: Option<String> = state
        .pg_pool
        .scalar_opt("SELECT kind FROM task WHERE id = $1", &[&task])
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the task: {e}")))?;
    if kind.as_deref() != Some(weft_task_store::TaskKind::MaintenanceLift.as_str()) {
        return Err((StatusCode::NOT_FOUND, format!("no lift {task} to follow (a finished one is forgotten after an hour)")));
    }
    let outcome = weft_task_store::terminal::wait_for_terminal_before(&state.pg_pool, &state.signals, task, hold.hold(&state.stopping))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("follow the lift: {e:#}")))?;
    match outcome.status {
        weft_core::task::TaskStatus::Pending | weft_core::task::TaskStatus::Claimed => Ok(Json(weft_core::deactivation::Following::Going(
            weft_task_store::tasks::progress(&state.pg_pool, task)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read where the lift is: {e:#}")))?,
        ))),
        weft_core::task::TaskStatus::Complete => {
            Ok(Json(weft_core::deactivation::Following::Done(outcome.result.unwrap_or(serde_json::Value::Null))))
        }
        weft_core::task::TaskStatus::Failed => Err((
            StatusCode::CONFLICT,
            format!(
                "the lift stopped: {}; what it had not brought back is still recorded, and `weft maintenance off` tries it again",
                outcome.error.unwrap_or_else(|| "it ended without saying why".into())
            ),
        )),
    }
}

// =================================================================
// Taking a project down
// =================================================================

/// Take down what is on in project `id`: its triggers that are on, marked;
/// then its infra copies that run, recorded and stopped; then, under
/// `--running cancel`, every run still going. What cannot be taken down
/// (a trigger being switched on, a copy being started, a build in flight)
/// is said with the way through, and the rest still goes down.
async fn take_project_down(state: &DispatcherState, id: uuid::Uuid, name: String, take_down: &TakeDown) -> ProjectWentDown {
    let mut went = ProjectWentDown { id, name, triggers: Vec::new(), copies: Vec::new(), runs_cancelled: 0, refused: Vec::new() };
    let again = "then run `weft maintenance on` again";
    if let Err(e) = fence(state, id).await {
        went.refused.push(format!("could not wait for what was being switched on: {e:#}; {again}"));
        return went;
    }
    // 1. The triggers that are on.
    match state.activations.list(id).await {
        Ok(activations) => {
            for busy in activations.iter().filter(|a| a.lifecycle.status == ProjectStatus::Activating) {
                went.refused.push(format!("trigger {} is being switched on; wait for it or cancel it (`weft cancel-activate`), {again}", busy.key));
            }
            let on: Vec<ActivationKey> =
                activations.iter().filter(|a| a.lifecycle.status == ProjectStatus::Active).map(|a| a.key.clone()).collect();
            if !on.is_empty() {
                match crate::api::project::take_down_triggers(state, id, on, take_down, DownCause::Maintenance, crate::take_down::TakeDownAsker::Outside).await {
                    Ok(taken) => went.triggers = weft_core::activation::by_owner(&taken.keys),
                    Err((_, why)) => went.refused.push(format!("its triggers: {why}; {again}")),
                }
            }
        }
        Err(e) => went.refused.push(format!("could not read its triggers: {e:#}; {again}")),
    }
    // 2. The infra copies that run.
    if let Err(why) = stop_copies(state, id, take_down, &mut went).await {
        went.refused.push(format!("its infra: {why}; {again}"));
    }
    // 3. What is still going, when the person said to cancel it.
    if take_down.running == Running::Cancel {
        match cancel_going(state, id).await {
            Ok(cancelled) => went.runs_cancelled = cancelled,
            Err(e) => went.refused.push(format!("could not cancel its runs: {e:#}; {again}")),
        }
    }
    went
}

/// Wait for what was switching project `id` on when the maintenance was
/// recorded: an activation's claim holds the project row's lock, the start
/// of an infra sync its transition lock, and each reads the maintenance
/// under it. Once both were free, every later one sees the maintenance and
/// every earlier one is visible (a trigger activating, a setup in flight).
async fn fence(state: &DispatcherState, id: uuid::Uuid) -> anyhow::Result<()> {
    let mut conn = state.pg_pool.get().await?;
    let tx = conn.transaction().await?;
    tx.query("SELECT 1 FROM project WHERE id = $1 FOR UPDATE", &[&id]).await?;
    tx.commit().await?;
    crate::lease::with_project_transition_lock(&state.lock_pool, id, || async { Ok(()) }).await
}

/// Record and stop project `id`'s copies that run, owner by owner, and
/// say the ones being started (whose start went ahead before the
/// maintenance) as refused. Every unit of a copy goes down, the ones its
/// node keeps up on a plain stop included (`force`): nothing of the old
/// install keeps running. Disks are kept, as on any stop.
async fn stop_copies(state: &DispatcherState, id: uuid::Uuid, take_down: &TakeDown, went: &mut ProjectWentDown) -> Result<(), String> {
    let Some(project) = state.projects.project(id).await.map_err(|e| format!("{e:#}"))? else { return Ok(()) };
    if crate::api::project::infra_setup_in_flight(state, id, None).await.map_err(|e| format!("{e:#}"))? {
        went.refused.push("its infra is being started; wait for it or cancel it (`weft infra cancel`), then run `weft maintenance on` again".into());
    }
    // Read as `weft status` reads them: a stop already asked for reads
    // `stopping` and is left to land, a start asked for `provisioning`.
    let observed = crate::infra_node::observe(&state.pg_pool, id, &project).await.map_err(|e| format!("{e:#}"))?;
    let being_started = |node: &str, instance: Option<&InstanceId>| {
        format!(
            "infra copy {} is being started; wait for it or cancel it (`weft infra cancel`), then run `weft maintenance on` again",
            copy_words(node, instance)
        )
    };
    let mut up: BTreeMap<Option<InstanceId>, Vec<String>> = BTreeMap::new();
    for row in observed.rows {
        match row.status {
            InfraNodeStatus::Running | InfraNodeStatus::Flaky => up.entry(row.instance).or_default().push(row.node_id),
            InfraNodeStatus::Provisioning => went.refused.push(being_started(&row.node_id, row.instance.as_ref())),
            InfraNodeStatus::Stopped | InfraNodeStatus::Failed | InfraNodeStatus::Stopping | InfraNodeStatus::Terminating => {}
        }
    }
    for (node, instance) in &observed.starting {
        went.refused.push(being_started(node, instance.as_ref()));
    }
    let now = crate::lease::now_unix();
    for (instance, nodes) in up {
        // Recorded before its stop is issued: a stop that lands with no
        // record would leave a copy nothing brings back.
        record_copies(&state.pg_pool, id, instance.as_ref(), &nodes, now).await.map_err(|e| format!("{e:#}"))?;
        let stop = crate::infra_lifecycle_command::Teardown::Stop { force: true };
        match crate::api::infra::take_copies_down(state, id, &project, instance.as_ref(), &nodes, stop, take_down, crate::take_down::TakeDownAsker::Outside).await {
            Ok(_) => went.copies.extend(nodes.into_iter().map(|node| CopyRef { node, instance: instance.clone() })),
            Err((_, why)) => went.refused.push(format!("stopping {}: {why}", nodes.join(", "))),
        }
    }
    Ok(())
}

/// Cancel every run of project `id` still going (queued or running; a run
/// paused on a wait keeps it, and goes on after the update).
async fn cancel_going(state: &DispatcherState, id: uuid::Uuid) -> anyhow::Result<u64> {
    let why = crate::take_down::maintenance_cancel();
    let going: Vec<(weft_core::RunId, &weft_core::run::CancelCause)> =
        crate::take_down::live_runs(state, id).await?.into_iter().filter(|run| !run.suspended).map(|run| (run.run_id, &why)).collect();
    Ok(crate::api::run::cancel_run_ids(state, &going).await?.len() as u64)
}

// =================================================================
// The lift
// =================================================================

/// Lift the maintenance of `tenant`'s install, every project side by side
/// (nothing of one waits on another): the project built again on this
/// weft ([`rebuild`]), then its recorded copies started and its marked
/// triggers brought back on that build ([`run_steps`]). Then the
/// maintenance ends, whatever a project could not bring back: that stays
/// recorded, is in the report, and comes back with the project's next
/// `weft resync` ([`bring_back_project`]).
pub(crate) async fn lift(state: &DispatcherState, tenant: &str, task: uuid::Uuid) -> anyhow::Result<LiftReport> {
    let names = project_names(state, tenant).await.map_err(|(_, why)| anyhow::anyhow!(why))?;
    let tell = Telling::on(Some(task));
    let setups = tokio::sync::Semaphore::new(SETUPS_AT_ONCE);
    let lifts = left(&state.pg_pool).await?;
    futures::future::try_join_all(lifts.iter().map(|lift| async {
        let id = lift.project_id;
        let mut lifted = ProjectLifted { id, name: names.get(&id).cloned().unwrap_or_default(), ..Default::default() };
        // A build that failed brings nothing back: what is recorded waits
        // for a build that runs on this weft.
        if rebuild(state, tenant, id, &mut lifted, &tell).await? {
            run_steps(state, lift, &mut lifted, &tell, &setups).await?;
        }
        tell.done(&state.pg_pool, lifted).await
    }))
    .await?;
    let report = LiftReport { projects: tell.into_done() };
    end(&state.pg_pool).await?;
    // What weft brings back on its own was refused while the maintenance
    // lasted, and the copies' changes that would wake it again came while
    // it still did: what went down with its infra, the events that waited
    // on infra and a resync stopped on either get their look now.
    if let Err(e) = crate::api::infra::bring_back_triggers_whose_infra_returned(state.clone()).await {
        tracing::error!(target: "weft_dispatcher::maintenance", "the maintenance is lifted, but bringing back what went down with its infra failed; the next change of a copy tries again: {e:#}");
    }
    if let Err(e) = crate::task_kinds::resync::retry_stopped(state).await {
        tracing::error!(target: "weft_dispatcher::maintenance", "the maintenance is lifted, but taking up again the resyncs stopped on its infra failed; the next change of a copy tries again: {e:#}");
    }
    Ok(report)
}

/// Bring back what the maintenance took down in project `id` and could
/// not bring back then, on the build it has registered now: what a
/// `weft resync` does first, once the project's code builds on this weft
/// again. `None` when nothing of it is left, or while the install is still
/// in maintenance (the lift is what brings things back then).
pub(crate) async fn bring_back_project(state: &DispatcherState, id: uuid::Uuid) -> anyhow::Result<Option<ProjectLifted>> {
    if is_on(&state.pg_pool).await? {
        return Ok(None);
    }
    let Some(lift) = left(&state.pg_pool).await?.into_iter().find(|lift| lift.project_id == id) else { return Ok(None) };
    let name = state.projects.get(id).await?.map(|project| project.name).unwrap_or_default();
    let mut lifted = ProjectLifted { id, name, ..Default::default() };
    run_steps(state, &lift, &mut lifted, &Telling::on(None), &tokio::sync::Semaphore::new(SETUPS_AT_ONCE)).await?;
    Ok(Some(lifted))
}

/// How many owners' infra starts and trigger setups a lift runs at once,
/// across every project it lifts side by side (a resync bringing back what
/// a lift left runs as many for its one project). Each trigger setup run
/// holds database connections of the dispatcher for its whole window and
/// calls the listener, which calls the broker, which takes connections of
/// its own; an infra start's beginning does the same through the
/// supervisor. A lift that started every owner of every project at once
/// ran a small database out of connections, and the setups that could not
/// get one failed. Four keeps a lift well inside the connections of the
/// smallest database weft runs on, and still brings a project with many
/// instances back in a fraction of the time one at a time would.
///
/// A turn is held only while connections are: an infra start holds one
/// for its beginning, never while it waits for its apply to land
/// ([`start_copies`]), since an apply runs the user's infra and has no
/// deadline, and four slow ones would hold back every other project of
/// the lift for as long as they take.
const SETUPS_AT_ONCE: usize = 4;

/// Run every one of `parts`, at most as many at once as `setups` has
/// permits, answering each one's output in the order given.
async fn a_few_at_a_time<T>(setups: &tokio::sync::Semaphore, parts: impl IntoIterator<Item = impl std::future::Future<Output = T>>) -> Vec<T> {
    futures::future::join_all(parts.into_iter().map(|part| async move {
        let _turn = setups.acquire().await.expect("a lift's semaphore is never closed");
        part.await
    }))
    .await
}

/// Bring back `lift` on the build its project has registered, saying in
/// `lifted` what came back and what did not: every recorded copy first,
/// every owner's side by side, since a trigger's setup reads the infra in
/// its cut; then every marked trigger, every owner's side by side. Owners
/// take their turn from `setups`, which the projects a lift brings back
/// side by side share ([`SETUPS_AT_ONCE`]): for a trigger setup the whole
/// of it, for an infra start its beginning alone.
async fn run_steps(
    state: &DispatcherState,
    lift: &ProjectLift,
    lifted: &mut ProjectLifted,
    tell: &Telling,
    setups: &tokio::sync::Semaphore,
) -> anyhow::Result<()> {
    let id = lift.project_id;
    if !lift.copies.is_empty() {
        let copies = lift
            .copies
            .iter()
            .flat_map(|(owner, nodes)| nodes.iter().map(|node| CopyRef { node: node.clone(), instance: owner.instance().cloned() }))
            .collect();
        tell.at(&state.pg_pool, lifted, LiftDoing::StartingInfra { copies }).await?;
        // Every owner's side by side: each takes a turn of `setups` only
        // for its start's beginning (`start_copies`).
        let parts = futures::future::join_all(lift.copies.iter().map(|(owner, nodes)| async {
            let mut part = ProjectLifted::default();
            start_copies(state, id, owner.instance(), nodes, setups, &mut part).await?;
            anyhow::Ok(part)
        }))
        .await;
        for part in parts {
            lifted.absorb(part?);
        }
    }
    if !lift.triggers.is_empty() {
        let triggers = lift
            .triggers
            .iter()
            .map(|(owner, triggers)| weft_core::activation::OwnerTriggers { owner: owner.clone(), triggers: triggers.clone() })
            .collect();
        tell.at(&state.pg_pool, lifted, LiftDoing::TurningOn { triggers }).await?;
        let parts = a_few_at_a_time(
            setups,
            lift.triggers.iter().map(|(owner, triggers)| async {
                let mut part = ProjectLifted::default();
                bring_back(state, id, owner, triggers.clone(), &mut part).await;
                part
            }),
        )
        .await;
        parts.into_iter().for_each(|part| lifted.absorb(part));
    }
    Ok(())
}

/// Where a lift is, said on its task as it goes ([`LiftProgress`]), which
/// `weft maintenance off` shows while it follows. Says nothing for a
/// project's resync bringing back what a lift left (`task` is `None`).
/// The projects the lift goes through side by side share it: each write
/// says all of them, one write at a time.
struct Telling {
    task: Option<uuid::Uuid>,
    progress: tokio::sync::Mutex<LiftProgress>,
}

impl Telling {
    fn on(task: Option<uuid::Uuid>) -> Self {
        Self { task, progress: tokio::sync::Mutex::new(LiftProgress::default()) }
    }

    /// `lifted`'s project is on step `doing` from now.
    async fn at(&self, pool: &Pool, lifted: &ProjectLifted, doing: LiftDoing) -> anyhow::Result<()> {
        let at = LiftAt { id: lifted.id, name: lifted.name.clone(), since_unix: crate::lease::now_unix(), doing };
        let mut progress = self.progress.lock().await;
        progress.going.retain(|going| going.id != lifted.id);
        progress.going.push(at);
        self.say(pool, &progress).await
    }

    /// `lifted`'s project goes on with its step, as `doing` now (a build
    /// whose images started).
    async fn still(&self, pool: &Pool, lifted: &ProjectLifted, doing: LiftDoing) -> anyhow::Result<()> {
        let mut progress = self.progress.lock().await;
        if let Some(at) = progress.going.iter_mut().find(|going| going.id == lifted.id) {
            at.doing = doing;
        }
        self.say(pool, &progress).await
    }

    /// The lift is done with `lifted`'s project.
    async fn done(&self, pool: &Pool, lifted: ProjectLifted) -> anyhow::Result<()> {
        let mut progress = self.progress.lock().await;
        progress.going.retain(|going| going.id != lifted.id);
        progress.done.push(lifted);
        self.say(pool, &progress).await
    }

    fn into_done(self) -> Vec<ProjectLifted> {
        self.progress.into_inner().done
    }

    async fn say(&self, pool: &Pool, progress: &LiftProgress) -> anyhow::Result<()> {
        let Some(task) = self.task else { return Ok(()) };
        weft_task_store::tasks::set_progress(pool, task, &serde_json::to_value(progress)?).await
    }
}

/// Build project `id` of `tenant` again on this weft, from the version its
/// running build came from and the resolutions it was built with, and
/// register it, through the same path a build takes
/// (`crate::actions::build::build_and_register`), waiting for its images
/// when they build. `false` when it failed, said in `lifted`.
/// A project with no stored request (its build an older weft registered)
/// is not built, said in `lifted`, and comes back on the build it has.
async fn rebuild(state: &DispatcherState, tenant: &str, id: uuid::Uuid, lifted: &mut ProjectLifted, tell: &Telling) -> anyhow::Result<bool> {
    use crate::actions::build::Built;
    let Some(source) = state.projects.running_build(id).await? else {
        lifted.skipped.push(
            "building it again on this weft: its running build is one an older weft registered, which kept nothing to build \
             it again from, so it comes back on that build; run `weft resync` in its folder to build it on this weft"
                .into(),
        );
        return Ok(true);
    };
    let tenant = crate::tenant::TenantId(tenant.to_string());
    let kept = crate::build::Kept(&source.inputs.assets);
    tell.at(&state.pg_pool, lifted, LiftDoing::Building { build: None }).await?;
    let ended = match crate::actions::build::build_and_register(state, id, &tenant, &source, &kept).await {
        Ok(Built::Registered(_)) => None,
        Ok(Built::Underway(underway)) => {
            tell.still(&state.pg_pool, lifted, LiftDoing::Building { build: Some(underway.build) }).await?;
            let ended = crate::build::waiting::wait_ended(state, id, underway.build).await?;
            match ended.state {
                weft_core::builds::VersionBuildStatus::Registered => None,
                _ => Some(format!("its build did not register: {}", ended.reason.unwrap_or_else(|| "it said not why".into()))),
            }
        }
        Err(failed) => Some(failed.message),
    };
    match ended {
        None => {
            lifted.rebuilt = true;
            Ok(true)
        }
        Some(why) => {
            lifted.failed.push(format!("building it again on this weft: {why}"));
            Ok(false)
        }
    }
}

/// Start `nodes`, `instance`'s recorded copies of project `id`, on the
/// registered build (the one [`rebuild`] just registered), and wait until they land. A copy terminated since, or
/// one the program no longer has, is skipped and forgotten; one that runs
/// already is forgotten. The start's beginning takes a turn of `setups`;
/// waiting for it to land takes none ([`SETUPS_AT_ONCE`]).
async fn start_copies(
    state: &DispatcherState,
    id: uuid::Uuid,
    instance: Option<&InstanceId>,
    nodes: &[String],
    setups: &tokio::sync::Semaphore,
    lifted: &mut ProjectLifted,
) -> anyhow::Result<()> {
    let Some(project) = state.projects.project(id).await? else { return Ok(()) };
    // Read as `weft status` reads them: a copy whose stop has not landed
    // yet reads `stopping`, so it is started again rather than taken for
    // one that runs.
    let observed = crate::infra_node::observe(&state.pg_pool, id, &project).await?;
    let mut done = Vec::new();
    let mut to_start = Vec::new();
    for node in nodes {
        let declared = crate::api::infra::resolve_infra_nodes(&project, std::slice::from_ref(node), instance).is_ok();
        match (observed.status_of(node, instance), declared) {
            (None, _) => {
                lifted.skipped.push(format!("infra copy {} was terminated during the maintenance", copy_words(node, instance)));
                done.push(node.clone());
            }
            (Some(_), false) => {
                lifted.skipped.push(format!("infra copy {} is no infra node of the program any more", copy_words(node, instance)));
                done.push(node.clone());
            }
            (Some(InfraNodeStatus::Running | InfraNodeStatus::Flaky), true) => done.push(node.clone()),
            (Some(_), true) => to_start.push(node.clone()),
        }
    }
    forget_copies(&state.pg_pool, id, instance, &done).await?;
    if to_start.is_empty() {
        return Ok(());
    }
    // A setup a lift before this one left in flight (its process died) is
    // waited out first: a second start while it runs would be refused.
    for run_id in crate::api::project::infra_setup_run_ids(state, id, Some(instance)).await? {
        let follow = crate::api::project::InfraSetupRun::follow(state, id, run_id).await;
        if let Err(ended) = crate::api::project::await_infra_setup(state, follow).await {
            tracing::info!(project_id = %id, %run_id, ?ended, "an earlier infra setup ended before the lift's start");
        }
    }
    let started = async {
        let begun = {
            let _turn = setups.acquire().await.expect("a lift's semaphore is never closed");
            crate::api::infra::begin_sync(state, id, &Default::default(), instance, &to_start, false, crate::api::infra::SyncKind::Lift)
                .await
                .map_err(|e| e.into_parts().1)?
        };
        if let Some(begun) = begun {
            crate::api::infra::finish_sync(state, begun).await.map_err(|ended| match ended {
                crate::api::project::SyncNotLanded::Cancelled(why) => format!("its start was cancelled ({why})"),
                crate::api::project::SyncNotLanded::Failed(_, why) => why,
            })?;
        }
        Ok::<(), String>(())
    }
    .await;
    let copies = || to_start.iter().map(|node| copy_words(node, instance)).collect::<Vec<_>>().join(", ");
    match started {
        Ok(()) => {
            forget_copies(&state.pg_pool, id, instance, &to_start).await?;
            lifted.started.extend(to_start.iter().map(|node| CopyRef { node: node.clone(), instance: instance.cloned() }));
        }
        Err(why) => lifted.failed.push(format!("infra {}: {why}", copies())),
    }
    Ok(())
}

/// Bring back `triggers`, `owner`'s marked triggers of project `id`, on the
/// registered build. The claim clears their mark; a failure puts it back.
/// A marked trigger the registered program no longer has (renamed or
/// removed since) is skipped and its mark forgotten, as a copy the program
/// no longer has is ([`start_copies`]): kept, it would fail the whole
/// owner's activation, its live triggers included, on every lift and
/// resync.
///
/// Each owner's activation is its own `activate` task, whoever brings it
/// back (the lift's task, a resync's request): the task's claim rests on
/// that owner's setup run alone, so the owners a lift brings back side by
/// side never share one claim's rest, and a resync's request, which holds
/// no claim, brings back the same way.
async fn bring_back(state: &DispatcherState, id: uuid::Uuid, owner: &Owner, triggers: Vec<String>, lifted: &mut ProjectLifted) {
    let (triggers, gone) = match state.projects.project(id).await {
        Ok(Some(project)) => {
            let declared: std::collections::BTreeSet<String> = weft_core::project::trigger_places(&project)
                .iter()
                .map(|place| weft_core::project::address_of(&project, &place.id, &place.path))
                .collect();
            triggers.into_iter().partition::<Vec<String>, _>(|trigger| declared.contains(trigger))
        }
        Ok(None) => (triggers, Vec::new()),
        Err(e) => {
            let words = weft_core::activation::OwnerTriggers { owner: owner.clone(), triggers }.words();
            lifted.failed.push(format!("triggers {words}: the registered program: {e:#}"));
            return;
        }
    };
    if !gone.is_empty() {
        let words = weft_core::activation::OwnerTriggers { owner: owner.clone(), triggers: gone.clone() }.words();
        match forget_marks(&state.pg_pool, id, owner.instance(), &gone).await {
            Ok(()) => lifted.skipped.push(format!("triggers {words} are no triggers of the program any more")),
            Err(e) => lifted.failed.push(format!("triggers {words} are no triggers of the program any more, and forgetting them failed: {e:#}")),
        }
    }
    if triggers.is_empty() {
        return;
    }
    let words = weft_core::activation::OwnerTriggers { owner: owner.clone(), triggers: triggers.clone() }.words();
    let request = weft_core::activation::ActivateRequest {
        scope: weft_core::activation::ActivationScope::of_owner(triggers, owner),
        ..Default::default()
    };
    match crate::task_kinds::activate::through_task(state, id, request, &crate::api::project::ActivateAsker::Lift).await.map(Json) {
        Ok(Json(answer)) => lifted.turned_on.extend(answer.turned_on),
        Err(error) => {
            let (_, why) = <(StatusCode, String)>::from(error);
            lifted.failed.push(format!("triggers {words}: {why}"));
        }
    }
}

// =================================================================
// Helpers
// =================================================================

/// The name of each of `tenant`'s projects, by id.
async fn project_names(state: &DispatcherState, tenant: &str) -> Result<BTreeMap<uuid::Uuid, String>, ApiError> {
    Ok(state
        .projects
        .list(tenant)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list the projects: {e:#}")))?
        .into_iter()
        .map(|project| (project.id, project.name))
        .collect())
}

/// `node` of `instance`, as a person reads it: `'db'`, or `'db' of
/// instance 'ada'`.
fn copy_words(node: &str, instance: Option<&InstanceId>) -> String {
    match instance {
        Some(instance) => format!("'{node}' of instance '{instance}'"),
        None => format!("'{node}'"),
    }
}

fn copy_refs(copies: &BTreeMap<Owner, Vec<String>>) -> Vec<CopyRef> {
    copies
        .iter()
        .flat_map(|(owner, nodes)| nodes.iter().map(|node| CopyRef { node: node.clone(), instance: owner.instance().cloned() }))
        .collect()
}

fn owner_triggers(triggers: &BTreeMap<Owner, Vec<String>>) -> Vec<weft_core::activation::OwnerTriggers> {
    triggers
        .iter()
        .map(|(owner, triggers)| weft_core::activation::OwnerTriggers { owner: owner.clone(), triggers: triggers.clone() })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ada() -> Owner {
        Owner::Instance(InstanceId::new("ada").unwrap())
    }

    /// A lift goes project by project, each one's copies grouped by owner
    /// before its triggers, grouped the same way.
    #[test]
    fn a_lift_groups_each_projects_copies_and_triggers_by_owner() {
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let copies = vec![
            RecordedCopy { project_id: a, node: "db".into(), instance: None },
            RecordedCopy { project_id: a, node: "cache".into(), instance: Some(InstanceId::new("ada").unwrap()) },
            RecordedCopy { project_id: a, node: "queue".into(), instance: None },
        ];
        let triggers = vec![
            (b, ActivationKey::new("cron", Owner::Shared)),
            (a, ActivationKey::new("door", Owner::Shared)),
            (a, ActivationKey::new("inbox", ada())),
        ];
        let lifts = plan(copies, triggers);
        assert_eq!(lifts.iter().map(|l| l.project_id).collect::<Vec<_>>(), vec![a, b]);
        assert_eq!(
            lifts[0].copies,
            BTreeMap::from([(Owner::Shared, vec!["db".into(), "queue".into()]), (ada(), vec!["cache".into()])])
        );
        assert_eq!(lifts[0].triggers, BTreeMap::from([(Owner::Shared, vec!["door".into()]), (ada(), vec!["inbox".into()])]));
        assert!(lifts[1].copies.is_empty());
        assert_eq!(lifts[1].triggers, BTreeMap::from([(Owner::Shared, vec!["cron".into()])]));
    }

    #[test]
    fn nothing_recorded_is_nothing_to_lift() {
        assert!(plan(Vec::new(), Vec::new()).is_empty());
    }

    weft_core::stress_test!(
        name: setups_go_a_few_at_a_time_across_every_project,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            // Two projects lifted side by side, each with many owners,
            // sharing one lift's turns: never more setups at once than it
            // allows, and every one of them runs.
            let setups = tokio::sync::Semaphore::new(SETUPS_AT_ONCE);
            let (now, most, ran) = (AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0));
            let setup = |owner: usize| {
                let (now, most, ran) = (&now, &most, &ran);
                async move {
                    let at_once = now.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(at_once, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    now.fetch_sub(1, Ordering::SeqCst);
                    ran.fetch_add(1, Ordering::SeqCst);
                    owner
                }
            };
            let project = |owners: std::ops::Range<usize>| a_few_at_a_time(&setups, owners.map(setup));
            let (a, b) = tokio::join!(project(0..10), project(10..25));
            assert_eq!(a, (0..10).collect::<Vec<_>>(), "each owner's answer, in order");
            assert_eq!(b, (10..25).collect::<Vec<_>>());
            assert_eq!(ran.load(Ordering::SeqCst), 25);
            assert_eq!(most.load(Ordering::SeqCst), SETUPS_AT_ONCE, "the bound holds across both projects, and is reached");
        }
    );
}
