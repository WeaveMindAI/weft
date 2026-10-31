//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh run.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use weft_core::activation::{ActivateRequest, ActivateResponse, ActivationUrl, BakeRequest};
use weft_core::deactivation::{DeactivateRequest, DeactivateResponse, ResyncAnswer, ResyncRequest};
use weft_core::projects::{DeclareRequest, InstanceInfraEntry, LimitedEntry, ProjectInfraEntry, ProjectStatusResponse, ProjectSummary, StatusQuery};
use weft_core::take_down::ReleaseHeld;
use weft_core::infra::wire::{INFRA_NOT_STARTED, INFRA_PER_INSTANCE};
use weft_core::frames::Located;
use weft_core::infra::run_gate::{copies_read, infra_not_running, missing_and_fix, MissingCopy};
use weft_core::ProjectDefinition;
use weft_task_store::db::Client as _;

use crate::actions::{project as project_actions, triggers as trigger_actions, Actor, Refused};
use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

fn operator(caller: CallerTenant) -> Actor {
    Actor::operator(caller.0)
}

pub use weft_core::run_spec::KickPlan as Kick;


/// Every image the system still needs, across ALL tenants: the keep-set
/// image reclamation deletes against (`crate::build::prune::keep_set`),
/// so under-reporting here deletes an image something still runs.
/// `worker_hashes` are binary hashes (the tag of `<registry>/weft-worker:<hash>`):
/// each project's `running_binary_hash` (what a fresh spawn would run),
/// the `binary_hash` stamped on every pending/claimed task, and every live
/// run's. `infra_refs` are full infra image refs, registry-qualified as the
/// build registered them: every project's complete infra image-tag map
/// (`infra_image_tags_json`, written atomically with the running-hash
/// trio on every infra sync) AND the image refs recorded on every
/// `infra_node` unit (an UP unit is deliberately left frozen at its old
/// image across syncs until it is force-stopped, so its recorded ref can
/// be older than the project's current map and must stay referenced while
/// the unit runs).
#[derive(Debug)]
pub struct ReferencedImages {
    pub worker_hashes: Vec<String>,
    pub infra_refs: Vec<String>,
}

/// `POST /images/prune`: delete from the install's registry every image it
/// built that nothing references (`weft clean --images`); one project's
/// when the body names it.
pub async fn prune_images(
    State(state): State<DispatcherState>,
    Json(req): Json<weft_core::images::PruneRequest>,
) -> Result<Json<weft_core::images::PruneReport>, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // Images a build in progress claims, or registers after this read, are
    // spared inside `prune`, one by one under each image's lock, so this
    // never waits for builds to finish.
    let referenced = {
        let conn = state.pg_pool.get().await.map_err(|e| internal("referenced images", e.into()))?;
        crate::build::prune::keep_set(&conn, crate::build::prune::ImageScope::All, state.builder.images.as_ref())
            .await
            .map_err(|e| internal("referenced images", e))?
    };
    let mut built = crate::build::ledger::built_images(&state.pg_pool, req.project)
        .await
        .map_err(|e| internal("built images", e))?;
    if let Some(project) = req.project {
        // One project's clean takes what it does not run, never what
        // another project is on or could go back to.
        let uses = crate::build::ledger::image_uses(&state.pg_pool).await.map_err(|e| internal("image uses", e))?;
        built = crate::build::prune::not_recent_elsewhere(&uses, built, project);
    }
    let report = crate::build::prune::prune(
        &state.pg_pool,
        state.builder.images.as_ref(),
        state.runner.as_ref(),
        &built,
        &referenced,
    )
    .await
    .map_err(|e| internal("prune images", e))?;
    Ok(Json(report))
}

/// The queries behind [`ReferencedImages`], restricted to `scope`: the
/// whole set for a prune's keep-set, one image's part when a prune
/// re-checks that image under its lock (`crate::build::ledger::decide_reclaim`).
/// One definition for both, so the bulk read and the per-image re-check
/// can never disagree on what "referenced" means.
pub async fn referenced_images_query(
    db: &impl weft_task_store::db::Client,
    scope: crate::build::prune::ImageScope<'_>,
) -> anyhow::Result<ReferencedImages> {
    // Every image something may still run: each project's current one,
    // each run's that has not ended (a run waiting on a form or a timer
    // resumes on the image it started on, however many times the project
    // was rebuilt since), and each live worker's (it may be driving a run
    // its door just bore, before anything of it is on record). `$1`
    // narrows every arm to one binary hash; NULL keeps them all.
    let query = format!(
        "SELECT DISTINCT running_binary_hash FROM project \
         WHERE running_binary_hash IS NOT NULL AND running_binary_hash <> '' \
           AND ($1::TEXT IS NULL OR running_binary_hash = $1) \
         UNION \
         SELECT DISTINCT binary_hash FROM run \
         WHERE state <> 'ended' AND binary_hash IS NOT NULL \
           AND ($1::TEXT IS NULL OR binary_hash = $1) \
         UNION \
         SELECT DISTINCT binary_hash FROM worker_lease l \
         WHERE {alive} AND binary_hash IS NOT NULL \
           AND ($1::TEXT IS NULL OR binary_hash = $1)",
        alive = weft_task_store::worker_alive!("l"),
    );
    let rows: Vec<(String,)> = if scope.covers_workers() {
        db.all(&query, &[&scope.worker_hash()]).await?
    } else {
        Vec::new()
    };
    let mut infra_refs = std::collections::BTreeSet::new();
    // A worker image's re-check needs no infra row.
    if !scope.covers_infra() {
        return Ok(ReferencedImages { worker_hashes: rows.into_iter().map(|(h,)| h).collect(), infra_refs: Vec::new() });
    }
    // One row per project, each the COMPLETE per-node tag map written by
    // its last committed infra sync. Rows are small and few (one per
    // project), so decoding in Rust beats a JSON-path query nobody can
    // read. The canonical decode names the project in its error, so one
    // corrupt row among many is diagnosable, and a row that fails it is a
    // loud error, never a skipped keep-set entry (skipping it would
    // condemn images the supervisor may still apply). Blank refs are
    // skipped, mirroring the `<> ''` convention of the worker-hash arms
    // (a blank matches no image; keeping it would only launder a
    // buggy-writer blank into the set).
    let tag_rows: Vec<(uuid::Uuid, weft_task_store::db::JsonText<serde_json::Value>)> =
        db.all("SELECT id, infra_image_tags_json FROM project", &[]).await?;
    for (project_id, weft_task_store::db::JsonText(tags)) in tag_rows {
        let by_node = weft_broker_client::protocol::decode_infra_image_tags(
            tags,
            &format!("project={project_id}"),
        )?;
        for node_tags in by_node.values() {
            for tag in node_tags.values() {
                if !tag.is_empty() && scope.covers_infra_ref(tag) {
                    infra_refs.insert(tag.clone());
                }
            }
        }
    }
    // The refs live units actually run, recorded on each unit's
    // `infra_unit` row at every apply (see `UnitRuntime::image_refs`).
    // Covers what the project-row map cannot: an UP unit frozen at an
    // older image across one or more syncs keeps running that ref until
    // it is force-stopped, so its old ref must stay referenced.
    // All rows, all units, regardless of status: over-keeping a ref is
    // bounded (rows die with the node on terminate) and the safe
    // direction for a set reclamation deletes against.
    let unit_refs: Vec<String> = db.scalars("SELECT DISTINCT unnest(image_refs) FROM infra_unit", &[]).await?;
    for image_ref in unit_refs {
        if !image_ref.is_empty() && scope.covers_infra_ref(&image_ref) {
            infra_refs.insert(image_ref);
        }
    }
    Ok(ReferencedImages {
        worker_hashes: rows.into_iter().map(|(h,)| h).collect(),
        infra_refs: infra_refs.into_iter().collect(),
    })
}

/// `GET /install`: what this install tells about itself (its addresses
/// and, on a cloud, what a project's CI deploys a frontend with). Behind
/// the admin surface's authentication, so `weft login` proves a key with
/// it.
pub async fn install(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
) -> Result<Json<weft_core::install::InstallInfo>, (StatusCode, String)> {
    let public_url = state.address.stable().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    let service_url = state.address.service_url().to_string();
    Ok(Json(weft_core::install::InstallInfo { public_url, service_url, ..state.install_info.clone() }))
}

/// `GET /install/database`: the roles whose copies waited for a database
/// connection lately, and what gives the install more
/// (`weft_task_store::throttle`), for `weft status`.
pub async fn install_database(
    State(state): State<DispatcherState>,
    _caller: CallerTenant,
) -> Result<Json<weft_core::install::DatabaseLoad>, Refused> {
    let stretches = weft_task_store::throttle::current(&state.pg_pool)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("read whether the database held the install back", e))?;
    let held_back: Vec<weft_core::install::HeldBack> = stretches
        .into_iter()
        .map(|s| weft_core::install::HeldBack {
            role: s.role,
            since_unix: s.since_ms / 1000,
            last_unix: s.last_ms / 1000,
            waits: s.waits,
            max_connections: s.max_connections,
            share: s.share,
        })
        .collect();
    let more = (!held_back.is_empty()).then(|| more_connections(state.install_info.cloud.is_some()).to_string());
    Ok(Json(weft_core::install::DatabaseLoad { held_back, more }))
}

/// What gives an install more database connections: a cloud install's
/// levers on its fork, or, on a local one, nothing to turn (its one
/// process holds its pools at their most already).
// SYNC: the levers <-> deploy/terraform/gcp/variables.tf (cloud_sql_tier, cloud_sql_max_connections, dispatcher_max_copies, broker_max_copies)
fn more_connections(cloud: bool) -> &'static str {
    match cloud {
        true => "to give it more, raise WEFT_CLOUD_SQL_TIER on your fork (and WEFT_CLOUD_SQL_MAX_CONNECTIONS with it, if you set one), \
                 then run the install workflow again; with a database you bring (WEFT_DATABASE_URL), move to a plan of your provider's \
                 that takes more connections. Each copy then takes a bigger share",
        false => "the install's one process holds as many connections as it uses at its busiest, so this passes with the load",
    }
}

/// `GET /projects` (`weft ps`).
pub async fn list(State(state): State<DispatcherState>, caller: CallerTenant) -> Result<Json<Vec<ProjectSummary>>, Refused> {
    Ok(Json(project_actions::list(&state, &operator(caller)).await?))
}

/// `POST /projects`: see [`project_actions::declare`].
pub async fn declare(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<DeclareRequest>,
) -> Result<Json<ProjectSummary>, Refused> {
    Ok(Json(project_actions::declare(&state, &operator(caller), &req).await?))
}


/// `POST /projects/{id}/builds`: build this version of the project inside
/// the install (`crate::build`).
///
/// When every image the version needs is in the registry, register what
/// it produced and answer it (200, [`weft_core::builds::BuiltProgram`]):
/// the compiled program, its three hashes, the implementations its worker
/// carries, the infra images every place runs, and the version's files as
/// the program's source. The project's running pointers move to it in the
/// same transaction, so the next verb acts on exactly this build.
///
/// When images are missing, start their builds (or join the ones already
/// running for the same content), write the version down as waiting on
/// them, and answer at once (202, [`weft_core::builds::BuildsUnderway`]),
/// never after the minutes the builds take: a request held open that long
/// with nothing on the wire is dropped by an idle network path. The
/// install registers the version itself once they all ended
/// (`crate::build::waiting`); the caller follows it (`version_build`).
///
/// This is the ONE way a project's program and images come to exist on an
/// install: nothing the client computed is believed.
pub async fn build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(req): Json<weft_core::builds::BuildRequest>,
) -> Result<axum::response::Response, axum::response::Response> {
    use axum::response::IntoResponse;
    let actor = crate::actions::Actor::operator(caller.0);
    match crate::actions::build::build(&state, &actor, id, &req).await.map_err(IntoResponse::into_response)? {
        crate::actions::build::Built::Registered(built) => Ok(Json(*built).into_response()),
        crate::actions::build::Built::Underway(underway) => Ok((StatusCode::ACCEPTED, Json(underway)).into_response()),
    }
}

/// `GET /projects/{id}`: see [`project_actions::get`].
pub async fn get(State(state): State<DispatcherState>, caller: CallerTenant, Path(id): Path<uuid::Uuid>) -> Result<Json<ProjectSummary>, StatusError> {
    Ok(Json(project_actions::get(&state, &operator(caller), id).await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct RemoveQuery {
    /// `weft rm --force` sets this. When true, the dispatcher skips
    /// the supervisor terminate-wait window and drops the project's
    /// infra rows straight away, and goes on past cloud leftovers it
    /// names. Use when the supervisor is wedged and the user wants the
    /// project gone NOW. It never goes past workers that cannot be
    /// killed: nothing is erased while one runs.
    #[serde(default)]
    pub force: bool,
}

/// `DELETE /projects/{id}`: see [`project_actions::remove`].
pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(query): axum::extract::Query<RemoveQuery>,
) -> Result<Json<weft_core::projects::ProjectRemoved>, StatusError> {
    Ok(Json(project_actions::remove(&state, &operator(caller), id, query.force).await?))
}

/// The project's CURRENT definition as one coherent (hash, shape)
/// pair: read `running_definition_hash` once, then fetch the
/// definition recorded under THAT hash. Every run-starting path
/// MUST use this instead of pairing a live `project_json` read with a
/// separate hash read: a concurrent re-register between two separate
/// reads would journal hash B while the kick set was computed from
/// shape A, and the worker (which fetches the definition BY the
/// journaled hash) would fold kicks that may not exist in the shape
/// it actually runs.
pub(crate) async fn coherent_definition(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(weft_core::project::hash::ProgramIdentity, std::sync::Arc<ProjectDefinition>), (StatusCode, String)> {
    let running = state
        .projects
        .running_program(id)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("running program identity: {e}"))
        })?
        .ok_or_else(|| {
            (
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "project {id} has no production code identity; build it (`weft build`, or any \
                     verb that runs it) before starting a run"
                ),
            )
        })?;
    let hash = &running.identity.definition_hash;
    let project = state
        .program(id, hash)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("recorded definition {hash}: {e:#}")))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "project {id} has no recorded definition for hash {hash}; the \
                     definition history must cover the running hash"
                ),
            )
        })?;
    let project = with_left_out(project, &running.left_out, running.worker_log.as_deref());
    Ok((running.identity, project))
}

/// `project` as its worker can run it: the node packages the worker's
/// build left out because they did not compile (`left_out`, recorded with
/// the registered worker, `project_store::ProgramCode`) added to its
/// broken parts, so a run, fire or setup whose cut uses one of their node
/// types is refused up front, naming the package, and every other cut
/// runs. Kept beside the definition rather than in it: the definition is
/// stored and cached under its hash, and a fixed node changes the worker,
/// never that hash.
fn with_left_out(
    project: std::sync::Arc<ProjectDefinition>,
    left_out: &[weft_core::builds::LeftOutPackage],
    log: Option<&str>,
) -> std::sync::Arc<ProjectDefinition> {
    if left_out.is_empty() {
        return project;
    }
    let mut runnable = (*project).clone();
    runnable.broken.extend(weft_core::builds::LeftOut::broken_parts(left_out, &project, log));
    std::sync::Arc::new(runnable)
}


/// What a setup phase of `seeds` runs: its cut of `project` (everything
/// the seeds need, a trigger's inputs included) and the roots that cut
/// kicks, payload-less (`weft_core::run_spec::setup_cut`). `None` with
/// nothing to set up.
fn setup_cut(project: &ProjectDefinition, seeds: &[Located]) -> Result<Option<(ProjectDefinition, Vec<Kick>)>, String> {
    if seeds.is_empty() {
        return Ok(None);
    }
    weft_core::run_spec::setup_cut(project, seeds).map(Some)
}

/// What a setup run is recorded as started by: its first kick, or, when
/// every root it runs starts with the group around it (kicked as that group
/// opens, not at the start), the first place it runs.
fn setup_entry(project: &ProjectDefinition, cut: &ProjectDefinition, kicks: &[Kick]) -> String {
    match kicks.first() {
        Some(kick) => kick_place(project, kick),
        None => weft_core::project::places::running_places(cut)
            .into_iter()
            .next()
            .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
            .expect("a setup's cut runs the places it sets up"),
    }
}

/// The program a run the dispatcher starts runs: `cut`, a cut of the
/// registered program `registered` (`weft_core::project::cut`), as its
/// birth names it and its start keeps it.
pub(crate) fn runs_program(
    registered: &weft_core::project::hash::ProgramIdentity,
    cut: &ProjectDefinition,
) -> Result<weft_journal::record::RunsProgram, (StatusCode, String)> {
    weft_journal::record::RunsProgram::of(&registered.definition_hash, cut.id, cut)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the run's program: {e:#}")))
}

/// The place a kick fires at, spelled the way the program reads it
/// (`keep.db` for the db of a file included as `keep`): the node under
/// the call sites on the kick's frames. What a run is recorded as
/// started by, setup runs here and fire runs in `versions::run`, so
/// `weft runs` lists every run by a place and never by a
/// compiled id.
pub(crate) fn kick_place(project: &ProjectDefinition, kick: &Kick) -> String {
    let place = Located::at(&kick.node, &kick.frames);
    weft_core::project::address_of(project, &place.id, &place.path)
}

/// The InfraSetup runs of the project that have not ended: the durable
/// "infra sync in flight" state, cancellable through the per-run cancel and
/// visible to every dispatcher. A run that has not ended is always on its
/// way somewhere (queued for a worker, driven by one, or parked on a
/// wait; a worker that went away has its runs requeued or ended by the
/// lost-run sweep), so nothing here can be a setup nobody will advance.
/// Sync rejects while any exists (two concurrent syncs would race the
/// provisioning subworkflow); the infra-cancel verb interrupts them.
pub(crate) async fn infra_setup_run_ids(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: `None` for any owner's setup, `Some(None)` for the
    // shared copies' setup, `Some(Some(m))` for instance m's.
    owner: Option<Option<&weft_core::instance::InstanceId>>,
) -> anyhow::Result<Vec<weft_core::RunId>> {
    Ok(state
        .pg_pool
        .scalars(
            "SELECT run_id FROM run \
             WHERE project_id = $1 AND phase = 'infra_setup' AND state <> 'ended' \
               AND ($2 OR instance_id IS NOT DISTINCT FROM $3) \
             ORDER BY started_at, run_id",
            &[&project_id, &owner.is_none(), &owner.flatten().map(|m| m.as_str())],
        )
        .await?)
}

/// Whether an infra setup of `owner` (see [`infra_setup_run_ids`])
/// is in flight: one instance starting its copy does not hold another
/// instance's back.
pub(crate) async fn infra_setup_in_flight(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    owner: Option<Option<&weft_core::instance::InstanceId>>,
) -> anyhow::Result<bool> {
    Ok(!infra_setup_run_ids(state, project_id, owner).await?.is_empty())
}

/// A started InfraSetup sub-run: the run to await plus the
/// subscriptions opened BEFORE the enqueue (so the worker can't beat the
/// waiter to the terminal event): the project's events, and the signal
/// watch, which hears the run's ending (`weft_journal::RUN_ENDED_CHANNEL`:
/// the run is queued watched).
pub struct InfraSetupRun {
    pub(crate) run_id: weft_core::RunId,
    events: crate::events::Follower,
    heard: weft_task_store::pg_signal::Subscription,
    project_id: uuid::Uuid,
}

impl InfraSetupRun {
    /// Follow a setup run already in flight (one a dispatcher that died
    /// mid-upgrade left behind), for [`await_infra_setup`] to wait out.
    pub(crate) async fn follow(state: &DispatcherState, project_id: uuid::Uuid, run_id: weft_core::RunId) -> Self {
        let heard = state.signals.subscribe();
        let events = state.events.subscribe_project(project_id).await;
        Self { run_id, events, heard, project_id }
    }
}

/// Queue a run of the program `program` started by the dispatcher (a setup
/// run), recording the program's source as the version it ran.
async fn start_queued_run(
    state: &DispatcherState,
    program: &weft_core::project::hash::ProgramIdentity,
    defaults: &weft_core::project::ProjectDefaults,
    mut birth: weft_journal::birth::Birth<'_>,
    queue_as: QueueAs<'_>,
) -> Result<(), (StatusCode, String)> {
    let source_version = crate::actions::versions::record_program_source(state, birth.project_id, program).await?;
    birth.source_version = Some(&source_version);
    start_queued_run_with(state, defaults, birth, &[], queue_as).await
}

/// What a run the dispatcher queues is born with beside its birth.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct QueueAs<'a> {
    /// A trigger setup an activation asked for (the activation is the
    /// setup's own run): queued only while that activation still
    /// owns its rows.
    pub for_activation: bool,
    /// Somebody waits on its ending, which then announces itself
    /// (`weft_journal::RUN_ENDED_CHANNEL`): an infra setup's waiter, or a
    /// trigger setup whose bake is the dispatcher's (`crate::run_ends`).
    pub watch_end: bool,
    /// What the version tree shows of a run started by hand: the places it
    /// ran again rather than inherit, what it was asked to run, and the
    /// saved example it came from.
    pub stale: &'a [String],
    pub spec: Option<&'a weft_core::run_spec::RunSpec>,
    pub example: Option<&'a str>,
}

/// THE one way the dispatcher starts a run (entrance 3): its birth (its
/// `RunStarted`, one `NodeKicked` per root) and `extra_rows` (birth
/// facts beyond the kicks: the log lines of the baked infra it reads),
/// written as its first record row with its `run` row, `queued`, and the
/// cut its birth names (`Birth::program`), in one transaction; delivery
/// hands it to a worker. Every
/// start path (`run`, trigger setup, infra setup) goes through here.
pub(crate) async fn start_queued_run_with(
    state: &DispatcherState,
    // The program's defaults (`weft.toml`): how long the run is kept when
    // what started it says nothing.
    defaults: &weft_core::project::ProjectDefaults,
    birth: weft_journal::birth::Birth<'_>,
    extra_rows: &[weft_journal::RunEvent],
    queue_as: QueueAs<'_>,
) -> Result<(), (StatusCode, String)> {
    let tenant = state
        .tenant_router
        .tenant_for_project(birth.project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let run_id = birth.run_id;
    let keep_for = birth.settings.kept_for(defaults.keep_for());
    let program = birth.program;
    let (start, kicks) = weft_journal::birth::birth_events(birth);
    let mut events = Vec::with_capacity(1 + kicks.len() + extra_rows.len());
    events.push(start);
    events.extend(kicks);
    events.extend_from_slice(extra_rows);
    let queued = weft_journal::record::Queued {
        events: &events,
        tenant: tenant.as_str(),
        keep_for,
        watch_end: queue_as.watch_end,
        stale: queue_as.stale,
        spec: queue_as.spec,
        example: queue_as.example,
        program: Some(program),
    };
    let inserted = state
        .runs
        .queue_run(queued, queue_as.for_activation)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("start run: {e:#}")))?;
    if !inserted {
        return Err((StatusCode::CONFLICT, format!("run {run_id} was started already")));
    }
    Ok(())
}

/// Is every infra node the project's TRIGGERS depend on Running?
///
/// The one rule joining the two lifetimes: a trigger reads the address
/// off the infra node feeding it, so arming it before that node is up
/// would register a signal against an address that does not answer.
/// Infra no trigger depends on is the RUN's to wait on and does not
/// hold an activation back.
///
/// Vacuously true when the triggers depend on none, which includes
/// every project without a trigger. A node with no row at all is not
/// Running: nothing was ever provisioned for it.
pub(crate) fn trigger_infra_ready(
    depends_on: &std::collections::BTreeSet<String>,
    rows: &[crate::infra_node::InfraNodeRow],
) -> bool {
    // The shared copies: the project's own triggers read those (a
    // instance's triggers are checked against the instance's copies when
    // they are activated).
    depends_on.iter().all(|id| {
        rows.iter().any(|r| {
            &r.node_id == id && r.instance.is_none() && r.status == crate::infra_node::InfraNodeStatus::Running
        })
    })
}

/// Is every infra node the source declares Running, which is what a
/// WHOLE-GRAPH run touches? The one definition of the fact the table
/// reports about run (`ActionInputs::run_infra_ready`).
pub(crate) fn run_infra_ready(has_infra: bool, infra_rollup: &str) -> bool {
    !has_infra || infra_rollup == "running"
}

/// The infra copies that are NOT currently Running (a Stopped, Failed,
/// Flaky or missing one is not running). Empty means every one is up. The
/// ONE definition of "is the infra up": a run's gate ([`require_run_infra`]),
/// arming triggers ([`require_trigger_infra`]) and activate's note about
/// idle infra all read it, each wording its own message.
///
/// `within` narrows the check to the named places: a targeted run only
/// touches its own upstream subgraph, and arming the triggers only touches
/// the infra they depend on, so infra outside either has no bearing on it
/// and must not gate it. `None` checks the whole project.
pub(crate) async fn missing_infra_nodes(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    within: Option<&HashSet<String>>,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<Vec<MissingCopy>, (StatusCode, String)> {
    // The project's copies as this dispatcher holds them (`crate::held`).
    let wanted = copies_read(project, within, instance);
    let missing_of = |copies: &[crate::infra_node::CopyStatus]| -> Vec<MissingCopy> {
        let up: Vec<weft_core::infra::run_gate::InfraCopyUp> = copies.iter().map(crate::infra_node::CopyStatus::up).collect();
        weft_core::infra::run_gate::missing_copies(&wanted, &up)
    };
    if wanted.iter().all(|(_, copy)| copy.is_none()) {
        return Ok(missing_of(&[]));
    }
    // A copy that came up a moment ago may not have been heard yet: a
    // refusal is made on the rows themselves.
    if let Some(held) = state.held.infra_status.held(&project_id) {
        let missing = missing_of(&held);
        if missing.iter().all(|m| m.needs_instance) {
            return Ok(missing);
        }
    }
    let fresh = state
        .held
        .infra_status
        .load_fresh(project_id, || crate::infra_node::statuses(&state.pg_pool, project_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e:#}")))?;
    Ok(missing_of(&fresh))
}


/// What the infra places a run for `instance` may read have saved
/// (`weft_core::infra::bake::saved_for_run`), from the project's copies
/// as this dispatcher holds them: a value saved a moment ago and not
/// heard yet leaves its node to run this time.
pub(crate) async fn saved_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<weft_core::infra::bake::Saved, (StatusCode, String)> {
    if project.nodes.iter().all(|node| node.baked_outputs.is_empty()) {
        return Ok(Default::default());
    }
    let copies = state
        .held
        .infra_status
        .get_or_load(project_id, || crate::infra_node::statuses(&state.pg_pool, project_id))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e:#}")))?;
    let up: Vec<weft_core::infra::run_gate::InfraCopyUp> = copies.iter().map(crate::infra_node::CopyStatus::up).collect();
    Ok(weft_core::infra::bake::saved_for_run(project, &up, instance))
}

/// What a run of `program` (its cut) for `instance` starts with: the
/// instance's stored values (with `overlay` made: the change a store call
/// is about to commit, which the setup it re-arms has to run with),
/// checked against the program by `weft_core::run_spec::instance_run_values`.
/// Answers a 422 carrying the refusal, the same shape every run refusal
/// has, naming every field the instance left unfilled or filled wrong.
pub(crate) async fn instance_values_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    program: &ProjectDefinition,
    instance: &weft_core::instance::InstanceId,
    overlay: Option<&weft_core::instance::ValueChanges>,
) -> Result<weft_core::instance::InstanceValues, (StatusCode, String)> {
    let stored = stored_instance_values(state, project_id, instance, overlay, Read::Held).await?;
    match weft_core::run_spec::instance_run_values(program, instance, &stored) {
        Ok(values) => Ok(values),
        // Refused on what this process kept: confirmed against the rows,
        // since a value stored a moment ago may not have been heard yet.
        Err(_) => {
            let stored = stored_instance_values(state, project_id, instance, overlay, Read::Fresh).await?;
            weft_core::run_spec::instance_run_values(program, instance, &stored).map_err(|refusal| refusal_error(&refusal))
        }
    }
}

/// Whether a read may answer from what this process keeps
/// (`crate::held::Held`), or reads the rows themselves: a refusal made on
/// what was kept is confirmed with a fresh read.
#[derive(Clone, Copy)]
enum Read {
    Held,
    Fresh,
}

/// What `instance` has stored, with `overlay` (a change about to be stored)
/// applied on top.
async fn stored_instance_values(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    instance: &weft_core::instance::InstanceId,
    overlay: Option<&weft_core::instance::ValueChanges>,
    read: Read,
) -> Result<weft_core::instance::InstanceValues, (StatusCode, String)> {
    let load = || async {
        let tenant = crate::instance_values::owning_tenant(state, project_id).await?;
        let rows = weft_access_store::instance_values(&state.pg_pool, &tenant, project_id, instance)
            .await
            .map_err(crate::api::access_store_error)?;
        Ok::<_, (StatusCode, String)>(crate::held::OfTenant { tenant, rows })
    };
    let key = (project_id, instance.clone());
    let held = match read {
        Read::Held => state.held.instance_values.get_or_load(key, load).await?,
        Read::Fresh => state.held.instance_values.load_fresh(key, load).await?,
    };
    let mut stored = held.rows.clone();
    if let Some(change) = overlay {
        change.apply(&mut stored);
    }
    Ok(stored)
}

/// A run refusal as the API answers it: 422, the refusal as JSON.
pub(crate) fn refusal_error(refusal: &weft_core::run_spec::Refusal) -> (StatusCode, String) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        serde_json::to_string(refusal).expect("a Refusal serializes"),
    )
}

/// THE infra gate on a run of `program` (its cut of the project), scoped
/// to the places it executes (`weft_core::infra::run_gate::copies_read`):
/// a run aimed at part of the graph waits on that part's infra alone, a
/// whole-graph run on all of it, and an instance's run on that instance's
/// copies of the per-instance ones. A run that reaches something per
/// instance and names no instance is told so first: which instance's
/// copies to check is what it has not said.
pub(crate) async fn require_run_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    program: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<(), (StatusCode, String)> {
    weft_core::run_spec::refuse_instanceless(program, instance).map_err(|refusal| refusal_error(&refusal))?;
    let missing = missing_infra_nodes(state, project_id, program, None, instance).await?;
    if !missing.is_empty() {
        return Err((StatusCode::PRECONDITION_REQUIRED, infra_not_running(&missing)));
    }
    Ok(())
}


/// START the InfraSetup sub-run for every `requiresInfra` node
/// in the project: journal `RunStarted` + the upstream-closure
/// root kicks (so programmatic-infra patterns, text → compute →
/// infra, flow values into the infra body), then enqueue the execute
/// task. Returns `None` when the project has no infra nodes (nothing
/// to provision). Split from `await_infra_setup` so the caller (sync)
/// can perform the start under the short per-project transition lock
/// (serializing the flip) and do the unbounded wait OUTSIDE it.
pub async fn start_infra_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: an instance's copies of the per-instance nodes, or the
    // shared nodes.
    instance: Option<&weft_core::instance::InstanceId>,
    // Only these nodes (places); every one of the owner's kind when empty.
    nodes: &[String],
) -> Result<Option<InfraSetupRun>, (StatusCode, String)> {
    // One coherent (hash, shape) pair: the kicks below and the
    // journaled/enqueued hash must come from the SAME definition (see
    // `coherent_definition`).
    let (program, project) = coherent_definition(state, project_id).await?;
    let places: Vec<Located> = crate::api::infra::resolve_infra_nodes(&project, nodes, instance)?.into_values().collect();
    let Some((cut, kicks)) = setup_cut(&project, &places).map_err(|error| (StatusCode::BAD_REQUEST, error))? else {
        return Ok(None);
    };
    let run_program = runs_program(&program, &cut)?;
    // An instance's copy is set up from what reaches it, which may be what
    // that instance provides.
    let instance_values = match instance {
        Some(instance) => instance_values_for_run(state, project_id, &cut, instance, None).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, &cut).await?;
    let run_id = weft_core::new_run_id();

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to the
    // completion event.
    let heard = state.signals.subscribe();
    let events = state.events.subscribe_project(project_id).await;

    // Infra reconciliation already holds the project transition lock.
    let source_version = crate::actions::versions::record_program_source_locked(state, project_id, &program).await?;
    let entry_node = setup_entry(&project, &cut, &kicks);
    start_queued_run_with(
        state,
        &project.defaults,
        weft_journal::birth::Birth {
            run_id,
            project_id,
            phase: weft_core::context::Phase::InfraSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            program: &run_program,
            // A setup is handed nothing and stands nothing in.
            given: &Default::default(),
            binary_hash: &program.binary_hash,
            seed: None,
            source_version: Some(&source_version),
            instance: instance.map(|instance| weft_journal::birth::RunFor { instance, values: &instance_values }),
            picks: &picks,
            fired_trigger: None,
            stand_in: None,
            settings: weft_core::run_settings::RunSettings::bookkeeping(),
            at_unix: crate::lease::now_unix() as u64,
        },
        &[],
        // Its waiter hears it end ([`await_infra_setup`]).
        QueueAs { watch_end: true, ..QueueAs::default() },
    )
    .await?;
    Ok(Some(InfraSetupRun { run_id, events, heard, project_id }))
}

/// Why a sync (its InfraSetup, then the landing) did not land.
#[derive(Debug)]
pub(crate) enum SyncNotLanded {
    /// The setup run was cancelled (an infra cancel, a `weft stop`):
    /// the state is what someone asked for.
    Cancelled(String),
    Failed(StatusCode, String),
}

impl From<(StatusCode, String)> for SyncNotLanded {
    fn from((code, message): (StatusCode, String)) -> Self {
        SyncNotLanded::Failed(code, message)
    }
}

impl From<SyncNotLanded> for (StatusCode, String) {
    fn from(end: SyncNotLanded) -> Self {
        match end {
            // 409, not 500: the state is exactly what was asked for;
            // per-node partial state stays visible for per-node
            // terminate/retry.
            SyncNotLanded::Cancelled(message) => (StatusCode::CONFLICT, message),
            SyncNotLanded::Failed(code, message) => (code, message),
        }
    }
}

impl From<SyncNotLanded> for StatusError {
    fn from(end: SyncNotLanded) -> Self {
        <(StatusCode, String)>::from(end).into()
    }
}

/// Wait for a started InfraSetup sub-run to settle.
/// How the infra setup `run_id` ended, as its journal says: `None`
/// while it is still in flight.
async fn setup_ended(
    state: &DispatcherState,
    run_id: weft_core::RunId,
) -> anyhow::Result<Option<Result<(), SyncNotLanded>>> {
    use crate::api::run::TerminalOutcome;
    Ok(crate::api::run::terminal_outcome(&state.pg_pool, run_id).await?.map(|outcome| match outcome {
        TerminalOutcome::Completed => Ok(()),
        TerminalOutcome::Cancelled => Err(SyncNotLanded::Cancelled("infra setup cancelled".into())),
        _ => Err(SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, "infra setup failed".into())),
    }))
}

pub(crate) async fn await_infra_setup(
    state: &DispatcherState,
    run: InfraSetupRun,
) -> Result<(), SyncNotLanded> {
    let InfraSetupRun { run_id, mut events, mut heard, project_id } = run;
    // Wait for completion. NO deadline: the InfraSetup run runs
    // user-authored upstream nodes (text -> compute -> infra), and
    // user code may legitimately be slow; a hard cap would refuse
    // legitimate provisioning. Instead, a periodic breadcrumb keeps
    // the stuck-state legible in the dispatcher logs, and the user
    // can always cancel it (`weft infra cancel`) to unblock.
    let started = std::time::Instant::now();
    // The journal is authoritative, and the bus is not: a terminal event
    // raised while nothing was subscribed (a setup that ended before this
    // waiter followed it, a dispatcher restart mid-wait) never reaches the
    // bus arm below. So the journal is read now, when the run's ending is
    // announced (it is queued watched), and whenever the watch or the bus
    // may have missed something; never on a clock.
    let ended_now = || async {
        setup_ended(state, run_id)
            .await
            .map_err(|e| SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, format!("infra setup terminal lookup: {e:#}")))
    };
    if let Some(landed) = ended_now().await? {
        return landed;
    }
    let mut breadcrumb = tokio::time::interval(std::time::Duration::from_secs(30));
    breadcrumb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    breadcrumb.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            heard = heard.next() => {
                let look = match heard {
                    Ok(weft_task_store::pg_signal::Heard::Signal { channel, payload }) => {
                        channel == weft_journal::RUN_ENDED_CHANNEL && weft_journal::run_ended_of_payload(&payload).contains(&run_id)
                    }
                    Ok(weft_task_store::pg_signal::Heard::Recheck | weft_task_store::pg_signal::Heard::Resumed) => true,
                    Ok(weft_task_store::pg_signal::Heard::Lost) => false,
                    Err(e) => {
                        return Err(SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, format!("the signal watch stopped during infra setup: {e:#}")));
                    }
                };
                if look {
                    if let Some(landed) = ended_now().await? {
                        return landed;
                    }
                }
            }
            _ = breadcrumb.tick() => {
                tracing::info!(
                    target: "weft_dispatcher::infra_setup",
                    project_id = %project_id,
                    run_id = %run_id,
                    elapsed_secs = started.elapsed().as_secs(),
                    "infra setup still running; waiting on the InfraSetup run \
                     (cancel it with `weft infra cancel` to unblock)"
                );
            }
            res = events.recv() => {
                match res.map(|record| record.event) {
                    Ok(crate::events::DispatcherEvent::RunCompleted { run_id: c, .. })
                        if c == run_id => return Ok(()),
                    Ok(crate::events::DispatcherEvent::RunFailed { run_id: c, error, .. })
                        if c == run_id => {
                        return Err(SyncNotLanded::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("infra setup failed: {error}"),
                        ));
                    }
                    Ok(crate::events::DispatcherEvent::RunCancelled { run_id: c, reason, .. })
                        if c == run_id => {
                        // The user's infra-cancel (or a per-run stop)
                        // interrupted the provisioning run.
                        return Err(SyncNotLanded::Cancelled(format!("infra setup cancelled: {reason}")));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // The bus dropped a batch that may have held this
                        // run's terminal event. The journal is
                        // authoritative: re-query it rather than wait
                        // blind (which would spuriously time out even
                        // though infra setup already finished).
                        if let Some(landed) = ended_now().await? {
                            return landed;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(SyncNotLanded::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "event bus closed during infra setup".into(),
                        ));
                    }
                }
            }
        }
    }
}

/// The project's entries that refused calls in this minute and the one
/// before.
pub(crate) async fn limited_entries(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<LimitedEntry>> {
    let entries: Vec<(String, String)> = state
        .pg_pool
        .all(
            "SELECT token, node_id FROM signal \
             WHERE project_id = $1 AND surface_kind = 'public_entry' AND NOT is_resume",
            &[&project_id],
        )
        .await?;
    let now = crate::lease::now_unix();
    let mut out = Vec::new();
    for (token, node) in entries {
        for (reason, refused) in crate::entry_limits::recent_refusals(&state.pg_pool, project_id, &token, now).await? {
            out.push(LimitedEntry { node: node.clone(), limit: reason.describe().to_string(), refused });
        }
    }
    Ok(out)
}

/// weft's own work the project's work waits on that keeps failing: every
/// role weft cannot wake, every loop of weft's that gave up, stored files
/// the bucket would not remove, the project's workers, infra, infra
/// commands, repairs and triggers kept past their hibernation, and the
/// triggers a resync a person gave up left down, each said in a person's
/// words. A resync that stopped with triggers down is not among them: the
/// project's status says it once, as its resync
/// (`weft_core::projects::ProjectStatusResponse::resync`), read with the rest
/// of its state.
pub(crate) async fn unanswered_for(state: &DispatcherState, project_id: uuid::Uuid) -> Result<Vec<weft_core::projects::Unanswered>, (StatusCode, String)> {
    let failing = weft_task_store::unanswered::failing_for(&state.pg_pool, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("work that keeps failing: {e:#}")))?;
    let given_up = given_up_resync_left_down(state, project_id).await?;
    Ok(failing
        .into_iter()
        .map(|f| weft_core::projects::Unanswered {
            what: f.what.what(f.label.as_deref()),
            then: f.what.then().to_string(),
            error: f.error,
            since_unix: f.since_ms / 1000,
            last_unix: f.last_ms / 1000,
            as_of_unix: f.as_of_ms / 1000,
        })
        .chain(given_up)
        .collect())
}

/// The triggers a resync of the project a person gave up left down, that
/// nobody activated or took down since and that the program still has,
/// said in a person's words; `None` when its last resync was not given
/// up, or no such trigger is left.
async fn given_up_resync_left_down(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<Option<weft_core::projects::Unanswered>, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    let Some(given_up) = state.projects.given_up_resync(project_id).await.map_err(|e| internal("the project's given-up resync", e))? else {
        return Ok(None);
    };
    if let Some(unreadable) = given_up.unreadable {
        return Ok(Some(weft_core::projects::Unanswered {
            what: "weft gave up a resync of this project: what it wrote down on the project no longer reads, so which triggers it had taken down is not known".to_string(),
            then: "the project's triggers, as `weft status` lists them, show which are off: `weft activate` turns on the ones that should be on, or `weft resync` starts the resync over; this clears when the next resync begins".to_string(),
            error: unreadable.error,
            since_unix: given_up.at_unix,
            last_unix: given_up.at_unix,
            as_of_unix: crate::lease::now_unix(),
        }));
    }
    if given_up.left_down.is_empty() {
        return Ok(None);
    }
    let project = state
        .projects
        .project(project_id)
        .await
        .map_err(|e| internal("the project", e))?
        .ok_or_else(|| internal("the project", anyhow::anyhow!("project {project_id} has no registered program")))?;
    let down = given_up.still_down(&project);
    if down.is_empty() {
        return Ok(None);
    }
    Ok(Some(weft_core::projects::Unanswered {
        what: format!("a given-up resync left these triggers down: {}", weft_core::activation::OwnerTriggers::list_words(&down)),
        then: "`weft activate` turns them on; a trigger leaves this once it is activated or deactivated, and it all clears when the next resync begins".to_string(),
        error: "the resync was given up with `weft cancel-activate` before it brought them back".to_string(),
        since_unix: given_up.at_unix,
        last_unix: given_up.at_unix,
        as_of_unix: crate::lease::now_unix(),
    }))
}

/// The runs each of the project's triggers started in this minute and the
/// one before, named by the trigger's node.
pub(crate) async fn trigger_runs(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<weft_core::projects::TriggerRuns>> {
    let counts = crate::entry_limits::recent_runs(&state.pg_pool, project_id, crate::lease::now_unix()).await?;
    if counts.is_empty() {
        return Ok(Vec::new());
    }
    let triggers: Vec<(String, String)> =
        state.pg_pool.all("SELECT token, node_id FROM signal WHERE project_id = $1 AND NOT is_resume", &[&project_id]).await?;
    let mut runs: Vec<weft_core::projects::TriggerRuns> = triggers
        .into_iter()
        .filter_map(|(token, node)| counts.get(&token).map(|&(started, failed)| weft_core::projects::TriggerRuns { node, started, failed }))
        .collect();
    runs.sort_by(|a, b| a.node.cmp(&b.node));
    Ok(runs)
}

/// Error envelope for project-scoped handlers whose 404 must be
/// machine-readable (`status`, `remove`). Most arms are a plain
/// `(StatusCode, String)` (via `From`, so existing `.map_err`
/// closures are unchanged); the one special case is `NotMyProject`,
/// which renders a 404 carrying the `x-weft-not-found: project`
/// marker header, so a client can tell "no project I may see under
/// this id" apart from a bare routing 404 (e.g. a version-skewed
/// dispatcher missing the route), which must bubble as an error.
/// `remove` needs it so the CLI's `delete_idempotent` can treat
/// "already gone" as rm's desired end state on a retry.
///
/// `NotMyProject` covers BOTH "no such project" AND "exists but owned by
/// another tenant", with the SAME response. Collapsing them is the
/// no-existence-leak property: an authenticated caller probing another
/// tenant's project id cannot distinguish "exists but not yours" from
/// "does not exist" (both get 404 + the marker).
pub enum StatusError {
    /// No project the caller may run under this id: either no row, or a row
    /// owned by another tenant. Marker header set; the two are
    /// indistinguishable on the wire (no existence leak).
    NotMyProject,
    /// Any other failure: status + body, no marker.
    Other(StatusCode, String),
}

impl From<(StatusCode, String)> for StatusError {
    fn from((code, msg): (StatusCode, String)) -> Self {
        StatusError::Other(code, msg)
    }
}

impl StatusError {
    /// The status and message, for an in-process caller that logs a
    /// refusal rather than answering it over HTTP.
    pub fn into_parts(self) -> (StatusCode, String) {
        match self {
            StatusError::NotMyProject => (StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()),
            StatusError::Other(code, msg) => (code, msg),
        }
    }
}

impl IntoResponse for StatusError {
    fn into_response(self) -> Response {
        match self {
            StatusError::NotMyProject => (
                StatusCode::NOT_FOUND,
                [("x-weft-not-found", "project")],
                crate::authenticator::NO_SUCH_PROJECT,
            )
                .into_response(),
            StatusError::Other(code, msg) => (code, msg).into_response(),
        }
    }
}


/// `GET /projects/{id}/status` (`weft status`): see [`project_actions::status`].
pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Result<Json<ProjectStatusResponse>, StatusError> {
    Ok(Json(project_actions::status(&state, &operator(caller), id, &query).await?))
}

/// `GET /projects/{id}/instances` (`weft instances`): every instance weft
/// holds anything for in the project, with what it holds, the same as a
/// program's `ctx.instances().list()` answers.
pub async fn instances(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<Vec<weft_core::program::InstanceHoldings>>, Refused> {
    Ok(Json(crate::actions::instances::list(&state, &operator(caller), id).await?))
}

/// What the program as it is now holds, that a trigger on and an infra
/// copy are compared with to tell whether they are behind it: the hashes
/// the caller computed from its source (`StatusQuery`), each left out read
/// off the build registered last.
pub(crate) struct Desired {
    pub binary: Option<String>,
    /// The same build with every node compiled in: a worker built either
    /// way is current.
    pub full_binary: Option<String>,
    pub definition: Option<String>,
    /// Each trigger's cut fingerprint, by address: `None` for a trigger
    /// the program has whose cut cannot be walked (it needs a part that
    /// does not compile), which no trigger on was set up from, so one on
    /// is behind and bringing it up is refused, saying why.
    pub trigger_cuts: std::collections::BTreeMap<String, Option<String>>,
    /// Each infra place's hash, by place; empty when neither the caller nor
    /// the registered build named them.
    pub infra_places: std::collections::BTreeMap<String, String>,
}

impl Desired {
    /// The registered build, as it is.
    pub(crate) async fn registered(
        state: &DispatcherState,
        id: uuid::Uuid,
        project: &ProjectDefinition,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
    ) -> Result<Self, (StatusCode, String)> {
        Ok(Self {
            binary: binary_hash.map(str::to_string),
            full_binary: None,
            definition: definition_hash.map(str::to_string),
            trigger_cuts: weft_core::run_spec::trigger_cuts(project)
                .into_iter()
                .map(|(trigger, cut)| (trigger, cut.ok().map(|cut| cut.fingerprint)))
                .collect(),
            infra_places: match definition_hash {
                Some(hash) => state
                    .projects
                    .infra_places(id, hash)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the build's infra places: {e:#}")))?,
                None => Default::default(),
            },
        })
    }

    /// The hashes `query` names, the registered build's for the rest: each
    /// trigger's and each place's on its own, so one the client could not
    /// work out (a cut that cannot be walked) is read off the registered
    /// build rather than taken as absent.
    pub(crate) async fn of_query(
        state: &DispatcherState,
        id: uuid::Uuid,
        project: &ProjectDefinition,
        query: &StatusQuery,
        binary_hash: Option<&str>,
        definition_hash: Option<&str>,
    ) -> Result<Self, (StatusCode, String)> {
        let registered = Self::registered(state, id, project, binary_hash, definition_hash).await?;
        Ok(Self {
            binary: query.desired_binary_hash.clone().or(registered.binary),
            full_binary: query.desired_full_binary_hash.clone(),
            definition: query.desired_definition_hash.clone().or(registered.definition),
            trigger_cuts: Self::named_over(registered.trigger_cuts, query.desired_trigger_cuts.as_deref(), "desiredTriggerCuts", Some)?,
            infra_places: Self::named_over(registered.infra_places, query.desired_infra_places.as_deref(), "desiredInfraPlaces", |hash| hash)?,
        })
    }

    /// The registered build's hashes by name, each the query named
    /// (`encoded`, the `what` parameter, each hash read by `read`) put in
    /// its place: one the client left out is the registered one.
    fn named_over<T>(
        registered: std::collections::BTreeMap<String, T>,
        encoded: Option<&str>,
        what: &str,
        read: impl Fn(String) -> T,
    ) -> Result<std::collections::BTreeMap<String, T>, (StatusCode, String)> {
        let mut merged = registered;
        if let Some(encoded) = encoded {
            let named = weft_core::projects::NamedHashes::decode(encoded).map_err(|e| (StatusCode::BAD_REQUEST, format!("{what}: {e}")))?;
            merged.extend(named.0.into_iter().map(|(name, hash)| (name, read(hash))));
        }
        Ok(merged)
    }

    /// Whether a worker built from `have` runs other code than this.
    pub(crate) fn binary_drifts(&self, have: Option<&str>) -> bool {
        match (self.binary.as_deref(), have) {
            (Some(want), Some(have)) => want != have && self.full_binary.as_deref() != Some(have),
            _ => false,
        }
    }

    /// Whether the activation is on and behind: its cut changed since it
    /// was set up (or can no longer be walked), or the code it fires did.
    /// One set up before cuts were recorded reads as behind, so a resync
    /// records its cut.
    pub(crate) fn behind(&self, activation: &crate::activation_store::Activation) -> bool {
        if activation.lifecycle.status != crate::activation_store::ProjectStatus::Active {
            return false;
        }
        let cut_moved = match self.trigger_cuts.get(&activation.key.trigger) {
            // The program has it no more: nothing to bring it up to.
            None => false,
            // Its cut cannot be walked: it is not what the trigger was set
            // up from, and the resync's refusal names the part it needs.
            Some(None) => true,
            Some(Some(want)) => activation.cut.as_ref().map(|cut| &cut.fingerprint) != Some(want),
        };
        let code_moved = activation.program.as_ref().is_some_and(|fired| self.binary_drifts(Some(fired.binary_hash.as_str())));
        cut_moved || code_moved
    }

    /// Whether a copy applied from `applied` (its place's hash, `None`
    /// when it was applied before hashes were recorded) is behind `place`.
    pub(crate) fn infra_behind(&self, place: &str, applied: Option<&str>) -> bool {
        self.infra_places.get(place).is_some_and(|want| applied != Some(want.as_str()))
    }
}


/// Every trigger of the program, one entry per place (see
/// `TriggerEntry`), then the activations of triggers it has no more.
pub(crate) fn trigger_entries(
    project: &ProjectDefinition,
    activations: &[crate::activation_store::Activation],
    held: &std::collections::BTreeMap<weft_core::activation::ActivationKey, weft_core::projects::HeldWork>,
    waiting: &std::collections::BTreeMap<weft_core::activation::ActivationKey, weft_core::program::WaitingFires>,
    desired: &Desired,
    registered_binary: Option<&str>,
    now_unix: i64,
) -> Vec<weft_core::projects::TriggerEntry> {
    use weft_core::projects::{TriggerCopy, TriggerEntry, TriggerState};
    let down_with = |activation: &crate::activation_store::Activation| match activation.lifecycle.status {
        crate::activation_store::ProjectStatus::Inactive => activation.lifecycle.went_down.map(|w| w.with()),
        _ => None,
    };
    let copy = |activation: &crate::activation_store::Activation| TriggerCopy {
        instance: activation.key.instance().cloned(),
        state: match activation.lifecycle.status {
            crate::activation_store::ProjectStatus::Activating => TriggerState::TurningOn,
            crate::activation_store::ProjectStatus::Active if desired.behind(activation) => TriggerState::Behind,
            crate::activation_store::ProjectStatus::Active => TriggerState::On,
            crate::activation_store::ProjectStatus::Registered | crate::activation_store::ProjectStatus::Inactive => TriggerState::Off,
        },
        mode: activation.lifecycle.mode(),
        down_with: down_with(activation),
        // What brings it back activates it on the registered build, which
        // `activate_with` refuses while another trigger on fires another.
        held_back: down_with(activation).zip(registered_binary).and_then(|(_, binary)| {
            let elsewhere = on_another_binary(activations, std::slice::from_ref(&activation.key), binary);
            (!elsewhere.is_empty()).then(|| held_back_words(&elsewhere))
        }),
        held: held.get(&activation.key).copied().unwrap_or_default(),
        waiting: waiting.get(&activation.key).cloned(),
        version: activation.source_version.clone(),
        progress: activation.activating.zip(activation.lifecycle.activating_run_id).map(|(activating, run)| {
            weft_core::infra::wire::ChangeProgress { since_unix: activating.since_unix, as_of_unix: now_unix, waiting: Some(activating.step.waiting(run)) }
        }),
    };
    let never_on = || TriggerCopy {
        instance: None,
        state: TriggerState::Off,
        mode: weft_core::activation::ActivationMode::Registered,
        down_with: None,
        held_back: None,
        held: Default::default(),
        waiting: None,
        version: None,
        progress: None,
    };
    // Each trigger place by the way its activation is keyed, kept with the
    // place itself.
    let places: std::collections::BTreeMap<String, Located> = weft_core::project::trigger_places(project)
        .into_iter()
        .map(|place| (weft_core::project::address_of(project, &place.id, &place.path), place))
        .collect();
    let mut entries: Vec<TriggerEntry> = places
        .iter()
        .map(|(node, place)| {
            let per_instance = weft_core::project::is_per_instance(project, &place.id);
            let rows: Vec<&crate::activation_store::Activation> = activations.iter().filter(|a| &a.key.trigger == node).collect();
            let shared = rows.iter().copied().find(|a| a.key.instance().is_none());
            TriggerEntry {
                node: node.clone(),
                per_instance,
                gone: false,
                shared: (!per_instance).then(|| shared.map(copy).unwrap_or_else(never_on)),
                copies: rows.iter().copied().filter(|a| a.key.instance().is_some()).map(copy).collect(),
                depends_on: match shared {
                    Some(shared) => crate::infra_reach::depends_on(shared, project),
                    None => weft_core::run_spec::trigger_cut(project, node).map(|cut| cut.depends_on).unwrap_or_default(),
                },
            }
        })
        .collect();
    let mut gone: std::collections::BTreeMap<&str, Vec<&crate::activation_store::Activation>> = Default::default();
    for activation in activations.iter().filter(|a| !places.contains_key(&a.key.trigger)) {
        gone.entry(activation.key.trigger.as_str()).or_default().push(activation);
    }
    entries.extend(gone.into_iter().map(|(node, rows)| {
        let shared = rows.iter().copied().find(|a| a.key.instance().is_none());
        TriggerEntry {
            node: node.to_string(),
            per_instance: shared.is_none(),
            gone: true,
            shared: shared.map(copy),
            copies: rows.iter().copied().filter(|a| a.key.instance().is_some()).map(copy).collect(),
            depends_on: shared.and_then(|a| a.cut.as_ref()).map(|cut| cut.depends_on.clone()).unwrap_or_default(),
        }
    }));
    entries
}

/// The status response's two infra lists, from every copy as the readers
/// show it: one entry per infra place the program declares (a shared one
/// with its copy's state, `not_started` when it has none; a `@per_instance`
/// one as `per_instance` with its instances' copies counted), and every
/// instance's copy on its own, whatever its place (one the source no
/// longer declares included, so it never vanishes from view). Each says
/// whether it is behind the source and which triggers go down with it. A
/// shared copy whose place the source no longer declares (an orphan) has
/// no entry of its own: it is counted project-wide (`orphaned_infra`, the
/// rollup) so the infra controls never vanish while it lives.
pub(crate) fn infra_entries(
    project: &ProjectDefinition,
    copies: &crate::infra_node::ObservedCopies,
    activations: &[crate::activation_store::Activation],
    desired: &Desired,
) -> (Vec<ProjectInfraEntry>, Vec<InstanceInfraEntry>) {
    // The triggers on (or being switched on) whose cut contains the copy:
    // one already down has nothing to lose to it.
    let up: Vec<crate::activation_store::Activation> = activations
        .iter()
        .filter(|a| {
            matches!(a.lifecycle.status, crate::activation_store::ProjectStatus::Active | crate::activation_store::ProjectStatus::Activating)
        })
        .cloned()
        .collect();
    let takes_down = |place: &str, copies: weft_core::instance::Copies| -> Vec<weft_core::activation::OwnerTriggers> {
        let reach = crate::infra_reach::InfraReach { places: std::collections::BTreeSet::from([place.to_string()]), copies };
        let keys: Vec<weft_core::activation::ActivationKey> =
            crate::infra_reach::activations_reached(&reach, &up, project).into_iter().map(|a| a.key.clone()).collect();
        weft_core::activation::by_owner(&keys)
    };
    let mut instance_infra: Vec<InstanceInfraEntry> = copies
        .rows
        .iter()
        .filter_map(|row| {
            Some(InstanceInfraEntry {
                node: row.node_id.clone(),
                instance: row.instance.clone()?,
                status: row.status.as_str().to_string(),
                progress: copies.progress_of(&row.node_id, row.instance.as_ref()),
                behind: desired.infra_behind(&row.node_id, row.source_hash.as_deref()),
                last_restart: row.last_restart(),
            })
        })
        .chain(copies.starting.iter().filter_map(|(node, instance)| {
            Some(InstanceInfraEntry {
                node: node.clone(),
                instance: instance.clone()?,
                status: crate::infra_node::InfraNodeStatus::Provisioning.as_str().to_string(),
                progress: copies.progress_of(node, instance.as_ref()),
                behind: false,
                last_restart: None,
            })
        }))
        .collect();
    instance_infra.sort_by(|a, b| (&a.node, &a.instance).cmp(&(&b.node, &b.instance)));
    let mut infra = Vec::new();
    for (spelled, place) in weft_core::project::infra_place_spellings(project) {
        let Some(node) = project.nodes.iter().find(|n| n.id == place.id) else {
            continue;
        };
        let entry = |status: String| ProjectInfraEntry {
            node: spelled.clone(),
            node_type: node.node_type.clone(),
            status,
            endpoint_url: None,
            failure_stage: None,
            failure_message: None,
            instance_copy_count: None,
            progress: None,
            behind: false,
            takes_down: Vec::new(),
            last_restart: None,
        };
        if node.per_instance.is_some() {
            infra.push(ProjectInfraEntry {
                instance_copy_count: Some(instance_infra.iter().filter(|c| c.node == spelled).count()),
                takes_down: takes_down(&spelled, weft_core::instance::Copies::Every),
                ..entry(INFRA_PER_INSTANCE.to_string())
            });
            continue;
        }
        let takes_down = takes_down(&spelled, weft_core::instance::Copies::Shared);
        match copies.rows.iter().find(|r| r.node_id == spelled && r.instance.is_none()) {
            Some(row) => infra.push(ProjectInfraEntry {
                // Coarse UI hint: first endpoint by name (BTreeMap = stable).
                endpoint_url: row.endpoints.values().next().cloned(),
                failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
                failure_message: row.failure_message.clone(),
                progress: copies.progress_of(&spelled, None),
                behind: desired.infra_behind(&spelled, row.source_hash.as_deref()),
                takes_down,
                last_restart: row.last_restart(),
                ..entry(row.status.as_str().to_string())
            }),
            None => infra.push(match copies.status_of(&spelled, None) {
                Some(status) => ProjectInfraEntry {
                    progress: copies.progress_of(&spelled, None),
                    takes_down,
                    ..entry(status.as_str().to_string())
                },
                None => ProjectInfraEntry { takes_down, ..entry(INFRA_NOT_STARTED.to_string()) },
            }),
        }
    }
    (infra, instance_infra)
}

/// Every reconciliation input gathered from live state, shared by the
/// status handler (renders the list) and `require_action` (enforces
/// against the same list). One producer so the two can never disagree.
pub(crate) struct ActionSnapshot {
    /// The project's shared activations as one lifecycle
    /// (`activation_store::aggregate`).
    pub lifecycle: crate::activation_store::ActivationLifecycle,
    /// Every activation row, shared and per instance.
    pub activations: Vec<crate::activation_store::Activation>,
    /// Some trigger, the program's or an instance's, is on.
    pub any_on: bool,
    /// Some trigger that is on is behind the build it is compared with.
    pub any_behind: bool,
    pub transition: crate::project_store::ProjectTransition,
    /// The project's resync, while its transition is `resyncing`.
    pub resync: Option<crate::task_kinds::resync::ResyncState>,
    pub has_triggers: bool,
    pub has_infra: bool,
    /// Every shared infra node the project's shared triggers' setup reads
    /// is Running, walked from the REGISTERED definition (see
    /// `ActionInputs::trigger_infra_ready` for what reads it).
    pub trigger_infra_ready: bool,
    pub orphaned_infra: bool,
    pub infra_rollup: String,
    pub infra_busy: bool,
    /// Every copy, as every status reader shows it.
    pub infra: crate::infra_node::ObservedCopies,
}

impl ActionSnapshot {
    /// The table's inputs this snapshot answers, the ones only the caller
    /// knows left at what `require_action` assumes.
    pub(crate) fn inputs(&self) -> ActionInputs<'_> {
        ActionInputs {
            removing: false,
            lifecycle: &self.lifecycle,
            transition: self.transition,
            // Stopped with triggers down, nothing driving it.
            resync_stopped: self.resync.as_ref().is_some_and(|held| held.standing_still().is_some()),
            has_triggers: self.has_triggers,
            has_infra: self.has_infra,
            trigger_infra_ready: self.trigger_infra_ready,
            run_infra_ready: true,
            orphaned_infra: self.orphaned_infra,
            infra_rollup: &self.infra_rollup,
            infra_busy: self.infra_busy,
            any_on: self.any_on,
            any_behind: self.any_behind,
            infra_behind: true,
        }
    }
}

pub(crate) async fn gather_action_snapshot(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    desired: &Desired,
) -> Result<ActionSnapshot, (StatusCode, String)> {
    let activations = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let lifecycle = crate::activation_store::aggregate(
        activations.iter().filter(|a| a.key.owner == weft_core::instance::Owner::Shared).map(|a| &a.lifecycle),
    );
    let any_on = activations.iter().any(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active);
    let any_behind = activations.iter().any(|a| desired.behind(a));
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let resync = match transition {
        crate::project_store::ProjectTransition::Resyncing => state
            .projects
            .resync(id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("the project's resync: {e:#}")))?,
        _ => None,
    };
    // Every copy as every reader shows it: a start, stop or terminate
    // under way reads as such before the supervisor reaches the row.
    let observed = crate::infra_node::observe(&state.pg_pool, id, project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let infra_rows = &observed.rows;

    // Every SHARED infra place the source declares, spelled the way its
    // row is keyed (a file included twice declares two placements): the
    // project's infra. An instance's copies are their program's to run;
    // they count here only when they are orphans, so live infra never
    // vanishes from view.
    // SYNC: shared roles <-> packages/weft-graph/src/webview/lib/utils/node-roles.ts projectHasInfra, projectHasTriggers
    // SYNC: shared roles <-> crates/weft-compiler/src/weft_compiler.rs collect_included_contents
    let declared = weft_core::project::DeclaredInfra::of(project);
    let source_infra: std::collections::BTreeSet<String> = weft_core::project::infra_place_spellings(project)
        .into_keys()
        .filter(|n| declared.declares(n, false))
        .collect();
    let has_infra = !source_infra.is_empty();
    // The program's own triggers, and the shared infra their setup reads:
    // an instance's triggers read that instance's copies, which have no
    // shared row, and are armed per instance.
    let shared_triggers: Vec<Located> = weft_core::project::trigger_places(project)
        .into_iter()
        .filter(|place| !weft_core::project::is_per_instance(project, &place.id))
        .collect();
    // Any trigger, shared or per instance: one that exists per instance
    // is turned on for an instance, and the verb is offered for it all
    // the same.
    let has_triggers = has_triggers(project);
    let shared_trigger_infra: std::collections::BTreeSet<String> =
        weft_core::project::cut::walk_bounds(&weft_core::project::graph::ProjectGraph::new(project), &weft_core::project::cut::Bounds::setup(&shared_triggers))
            .runs
            .iter()
            .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
            .filter(|spelled| source_infra.contains(spelled))
            .collect();
    let trigger_infra_ready = trigger_infra_ready(&shared_trigger_infra, infra_rows);
    // Live rows the source does not declare: the user deleted the node
    // from source, or it sits behind a part that does not compile
    // (`DeclaredInfra::gone` tells the two apart, for what takes copies
    // down on its own). Either way they count into the rollup (below) and
    // raise the project-level signal, so live infra the source does not
    // show never collapses the rollup to `none` and never makes the infra
    // controls vanish (Model 1's never-lose-track guarantee): what to do
    // with them is the person's call.
    let is_undeclared = |r: &crate::infra_node::InfraNodeRow| !declared.declares(&r.node_id, r.instance.is_some());
    let undeclared_count = infra_rows.iter().filter(|r| is_undeclared(r)).count();
    let orphaned_infra = undeclared_count > 0;

    // Aggregate infra state across SOURCE nodes + orphan rows:
    //   - none:    no source infra node and no live row.
    //   - running: every counted slot is Running.
    //   - stopped: every counted slot is Stopped or absent.
    //   - partial: mixed (some running, some not, no failures).
    //   - failed:  at least one Failed.
    //   - flaky:   at least one Flaky and the rest Running.
    // Transient states take precedence (terminating > stopping >
    // provisioning) so the action bar shows the in-flight state and
    // offers only its cancel.
    let total = source_infra.len() + undeclared_count;
    let infra_rollup = if total == 0 {
        "none".to_string()
    } else {
        use crate::infra_node::InfraNodeStatus;
        let mut running = 0usize;
        let mut stopped = 0usize;
        let mut absent = 0usize; // source node never provisioned OR terminated
        let mut failed = 0usize;
        let mut flaky = 0usize;
        let mut stopping = 0usize;
        let mut terminating = 0usize;
        let mut provisioning = 0usize;
        // One slot per source node (absent counted) + one per undeclared
        // row (it always HAS a row, by definition).
        let mut count_status = |status: InfraNodeStatus| match status {
            InfraNodeStatus::Running => running += 1,
            InfraNodeStatus::Failed => failed += 1,
            InfraNodeStatus::Flaky => flaky += 1,
            InfraNodeStatus::Stopping => stopping += 1,
            InfraNodeStatus::Terminating => terminating += 1,
            InfraNodeStatus::Provisioning => provisioning += 1,
            InfraNodeStatus::Stopped => stopped += 1,
        };
        for spelled in &source_infra {
            match observed.status_of(spelled, None) {
                Some(status) => count_status(status),
                // No row and nothing starting one: never provisioned OR
                // terminated (terminate removes the row). Nothing lives
                // in the namespace for this node.
                None => absent += 1,
            }
        }
        for r in infra_rows.iter().filter(|r| is_undeclared(r)) {
            count_status(r.status);
        }
        if terminating > 0 {
            "terminating".to_string()
        } else if stopping > 0 {
            "stopping".to_string()
        } else if provisioning > 0 {
            "provisioning".to_string()
        } else if failed > 0 {
            "failed".to_string()
        } else if flaky > 0 && running + flaky == total {
            "flaky".to_string()
        } else if running == total {
            "running".to_string()
        } else if absent == total {
            "none".to_string()
        } else if stopped + absent == total {
            // Some stopped + some never-provisioned. Pragmatic bucket
            // as `stopped`: the user can re-start, and Terminate still
            // makes sense for the actually-stopped subset.
            "stopped".to_string()
        } else {
            "partial".to_string()
        }
    };

    // Infra-op-in-flight fact the rollup can't always see (a claimed
    // stop still draining; a provisioning run before any row flips).
    let infra_busy = crate::infra_lifecycle_command::any_in_flight(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("commands in flight: {e}")))?
        || infra_setup_in_flight(state, id, Some(None))
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}"))
            })?;
    Ok(ActionSnapshot {
        lifecycle,
        activations,
        any_on,
        any_behind,
        transition,
        resync,
        has_triggers,
        has_infra,
        trigger_infra_ready,
        orphaned_infra,
        infra_rollup,
        infra_busy,
        infra: observed,
    })
}

/// Enforce a verb against the SAME reconciliation the status handler
/// renders, against the registered build: a stale screen's button is
/// refused here. `verbs` is the acceptable set for the handler.
pub(crate) async fn require_action(
    state: &DispatcherState,
    id: uuid::Uuid,
    verbs: &[&str],
) -> Result<ActionSnapshot, (StatusCode, String)> {
    // A project `weft rm` marked takes no verb, said in its own words
    // rather than as a verb the table does not offer.
    state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?
        .unless_removing()?;
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e:#}"));
    let binary_hash = state.projects.running_binary_hash(id).await.map_err(internal)?;
    let definition_hash = state.projects.running_definition_hash(id).await.map_err(internal)?;
    let desired = Desired::registered(state, id, &project, binary_hash.as_deref(), definition_hash.as_deref()).await?;
    let snapshot = gather_action_snapshot(state, id, &project, &desired).await?;
    // The registered definition lags the source the moment a person
    // edits it, and a verb's own handler checks the infra against the
    // definition it builds (`require_trigger_infra`, `versions::run`'s
    // gate), so those facts are taken as satisfied here: refusing over an
    // infra node the person already deleted would be a dead end. The same
    // for a verb that triggers or infra have not caught up with yet.
    let allowed = compute_available_actions(&ActionInputs {
        has_triggers: true,
        trigger_infra_ready: true,
        any_on: true,
        any_behind: true,
        infra_behind: true,
        ..snapshot.inputs()
    });
    if verbs.iter().any(|v| allowed.iter().any(|a| a == v)) {
        return Ok(snapshot);
    }
    Err((StatusCode::CONFLICT, unavailable_action(verbs[0], snapshot.transition, &snapshot.infra_rollup, &allowed)))
}

/// Why `verb` is refused, as a person reads it.
fn unavailable_action(
    verb: &str,
    transition: crate::project_store::ProjectTransition,
    infra_rollup: &str,
    allowed: &[String],
) -> String {
    // A build or a resync in flight blocks every verb but its cancels, so
    // it is named as the blocker rather than buried in the triggers' state.
    if let Some(why) = transition.refusal() {
        return format!("'{verb}' is not available right now: {why}");
    }
    // Infra being started, stopped or terminated blocks every verb but
    // its cancel (the table's master rule), so the infra is named as the
    // blocker rather than the triggers' state, which is not why.
    if allowed == ["infra_cancel"] {
        return format!(
            "'{verb}' is not available right now: this program's infra is changing (infra {infra_rollup}). \
             `weft infra status` shows where each piece is and `weft infra cancel` stops the change; \
             run this again once it settles"
        );
    }
    if allowed == ["cancel_activate"] {
        return format!(
            "'{verb}' is not available right now: triggers of this program are being switched on; wait for it, \
             or `weft cancel-activate` stops it"
        );
    }
    // Starting infra that already runs is somebody who changed it and wants
    // the change live (an upgrade) or who did not know it was up.
    if verb == "infra_start" && infra_rollup == "running" {
        return "every piece of this program's infrastructure is already running. If you changed it, \
                `weft infra upgrade` puts the change live"
            .into();
    }
    format!("'{verb}' is not available right now (infra {infra_rollup}); allowed actions: [{}]", allowed.join(", "))
}

/// Every input the reconciliation reads, gathered in one place so the
/// status handler (UI list) and verb enforcement (`require_action`)
/// feed the SAME pure function from the SAME facts.
pub(crate) struct ActionInputs<'a> {
    /// `weft rm` marked the project and has not finished removing it
    /// (`StoredProjectSummary::removing`).
    pub removing: bool,
    pub lifecycle: &'a crate::activation_store::ActivationLifecycle,
    pub transition: crate::project_store::ProjectTransition,
    /// The project's resync stopped with triggers down (`ActionSnapshot::resync_stopped`).
    pub resync_stopped: bool,
    /// Source declares a trigger, shared or per instance (frontend-parse
    /// fact, derived here from the stored definition).
    pub has_triggers: bool,
    /// Source declares any infra node. This is the SOURCE fact only;
    /// orphaned live infra does not count (Model 1).
    pub has_infra: bool,
    /// Every infra node the shared triggers' setup reads is running
    /// (vacuously true when they read none). Infra only a fire runs is
    /// gated at run time and does not hold an activation back.
    ///
    /// This is what the table REPORTS about activate, from the
    /// registered definition. It is not what the door enforces:
    /// `require_action` passes it satisfied, because the registered
    /// definition lags the source, and `require_trigger_infra` does
    /// the honest check after the build, on the definition the
    /// activation will register.
    pub trigger_infra_ready: bool,
    /// Every infra node the source declares is running, which is what
    /// a WHOLE-GRAPH run touches (`run_infra_ready`). The same posture
    /// as `trigger_infra_ready`.
    pub run_infra_ready: bool,
    /// Live `infra_node` rows exist whose node is NOT in the current
    /// source (the user deleted the node while it was deployed, or it sits
    /// behind a part that does not compile). Does
    /// NOT gate run/activate; DOES keep the infra controls offered so
    /// the user never loses track of live infra.
    pub orphaned_infra: bool,
    pub infra_rollup: &'a str,
    /// An infra operation is in flight: an uncompleted lifecycle
    /// command (apply / stop / terminate) or a running InfraSetup
    /// provisioning run. Transitional per the master rule even
    /// BEFORE the supervisor flips any node status (the window where
    /// the rollup alone would still read as stable).
    pub infra_busy: bool,
    /// Some trigger is on (`ActionSnapshot::any_on`).
    pub any_on: bool,
    /// Some trigger that is on is behind (`ActionSnapshot::any_behind`).
    pub any_behind: bool,
    /// Some infra copy is behind the source.
    pub infra_behind: bool,
}

/// THE reconciliation table as a pure function: verbs the dispatcher will
/// currently accept, computed from the concern-lifecycles + source facts,
/// never stored. The CLI's `weft status` lists it, and the verb handlers
/// enforce against the same list.
///
/// A project `weft rm` marked offers nothing: only `weft rm` again goes on
/// with it.
///
/// The master rule collapses the table: in ANY transitional state
/// (building / cancelling_build / resyncing / activating / infra
/// provisioning / stopping / terminating) the only offered action is the
/// matching CANCEL. Stable states then enumerate:
///
///   - run           : allowed iff the source's infra (if any) is
///                     running. Never gated on a build (run builds as
///                     step 0) and never gated on an orphan.
///   - activate      : the source has triggers and the infra their
///                     setup reads runs. Turning on what is on already
///                     does nothing, so it is offered whatever is on.
///   - deactivate    : some trigger is on.
///   - resync        : some trigger that is on is behind, and the infra
///                     its setup reads runs (resync arms what activate
///                     arms).
///   - cancel_*      : the transitional rows.
///   - infra_*       : offered whenever live-or-declared infra exists
///                     (`has_infra || orphaned_infra`), per rollup.
///                     start/upgrade additionally need the SOURCE to
///                     declare infra (there is no spec to provision an
///                     orphan from); stop/terminate work on the live
///                     rows themselves, so they stay offered for a
///                     pure orphan (the never-lose-track guarantee).
// SYNC: compute_available_actions <-> packages/weft-graph/src/protocol.ts ActionVerb
pub(crate) fn compute_available_actions(inputs: &ActionInputs<'_>) -> Vec<String> {
    use crate::project_store::ProjectStatus;

    if inputs.removing {
        return Vec::new();
    }
    // Master rule, build axis: a build in flight offers only its
    // cancel (idempotent while already cancelling).
    if inputs.transition.is_building() {
        return vec!["cancel_build".to_string()];
    }
    // A resync drives its triggers down and back on itself, so only the
    // cancel of the step it is at is offered: stopping it as the triggers
    // come back. One that stopped with triggers down is taken up again, or
    // given up.
    if inputs.transition == crate::project_store::ProjectTransition::Resyncing {
        if inputs.resync_stopped {
            return vec!["resync".to_string(), "cancel_activate".to_string()];
        }
        return match inputs.lifecycle.status {
            ProjectStatus::Activating => vec!["cancel_activate".to_string()],
            _ => Vec::new(),
        };
    }

    // Master rule, trigger axis.
    if inputs.lifecycle.status == ProjectStatus::Activating {
        return vec!["cancel_activate".to_string()];
    }

    // Master rule, infra axis: either the derived rollup reports a
    // transitional state, or an infra operation is in flight that the
    // rollup can't see yet (a claimed stop still waiting for its runs, a
    // provisioning run before any node row flips). Only meaningful when
    // infra state exists at all.
    let infra_live = inputs.has_infra || inputs.orphaned_infra;
    if infra_live
        && (inputs.infra_busy
            || matches!(inputs.infra_rollup, "provisioning" | "stopping" | "terminating"))
    {
        return vec!["infra_cancel".to_string()];
    }

    // Stable states.
    let mut out = Vec::new();

    // `run` is offered whenever it is a legal NEXT STEP, NOT gated on
    // whether a worker image is already built (a click auto-builds on
    // demand). The one gate is genuine live-state: the infra a run
    // touches must be running.
    if inputs.run_infra_ready {
        out.push("run".to_string());
    }
    // A trigger whose setup's infra is down cannot be armed: its
    // registration reads the address off the infra node feeding it. The
    // door's own copy of this is `require_trigger_infra`, which runs after
    // the build.
    if inputs.has_triggers && inputs.trigger_infra_ready {
        out.push("activate".to_string());
    }
    if inputs.any_on {
        out.push("deactivate".to_string());
    }
    if inputs.any_behind && inputs.trigger_infra_ready {
        out.push("resync".to_string());
    }

    // Infra controls, per rollup. `partial` (some units up, some down)
    // is a valid steady state, not a transient: Start brings the down
    // units up, Stop takes the up units down, Terminate kills
    // everything; all three are meaningful at once.
    if infra_live {
        let can_provision = inputs.has_infra; // needs a source spec
        match inputs.infra_rollup {
            "running" => {
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if can_provision && inputs.infra_behind {
                    out.push("infra_upgrade".to_string());
                }
            }
            "stopped" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
                out.push("infra_terminate".to_string());
            }
            "none" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
            }
            "partial" | "failed" | "flaky" => {
                if can_provision {
                    out.push("infra_start".to_string());
                }
                out.push("infra_stop".to_string());
                out.push("infra_terminate".to_string());
                if can_provision && inputs.infra_behind {
                    out.push("infra_upgrade".to_string());
                }
            }
            // provisioning/stopping/terminating returned above.
            _ => {}
        }
    }

    out
}

/// Every instance weft holds anything for in the project (what `--all-
/// instances` reaches), sorted.
pub(crate) async fn known_instances(state: &DispatcherState, id: uuid::Uuid) -> Result<Vec<weft_core::instance::InstanceId>, (StatusCode, String)> {
    let tenant = crate::tenant::TenantId(crate::instance_values::owning_tenant(state, id).await?);
    Ok(crate::actions::instances::instances(state, &tenant, id).await?.into_iter().map(|held| held.instance).collect())
}

/// Refuse to turn on triggers the build `project` cannot run
/// ([`cannot_come_on`]), naming every part they need that does not
/// compile, so the person is told now rather than by a setup that fails
/// halfway or by every caller later, and refuse a program whose nodes need
/// what the machine lacks (`crate::needs`). Asked of the triggers an activate
/// switches on, whoever asks, and of the ones a resync puts back (before
/// it takes any down, and again before it brings each owner back); never
/// of a re-arm, whose triggers are on and whose change has nothing to do
/// with it.
pub(crate) async fn refuse_unrunnable(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<(), Unrunnable> {
    let mut refused = Vec::new();
    for (_, owned) in keys_by_owner(keys) {
        let saved = saved_for_run(state, id, project, keys_instance(&owned)).await.map_err(Unrunnable::Unchecked)?;
        refused.extend(cannot_come_on(project, &owned, &saved));
    }
    if !refused.is_empty() {
        return Err(Unrunnable::Cannot(refused.join("\n")));
    }
    crate::needs::refuse_unmet(state, id, project).await
}

/// Why [`refuse_unrunnable`] did not let triggers come on. The two are
/// kept apart because they call for opposite things: a build that cannot
/// run them fails the same way on every try and is fixed in the source,
/// while a check that could not be made (a read of what the infra saved
/// failed) says nothing about the build and goes through once the read
/// does.
#[derive(Debug)]
pub(crate) enum Unrunnable {
    /// The build cannot run them: every part they need that does not
    /// compile, one per line.
    Cannot(String),
    /// The check could not be made: the failed read's (or the machine
    /// check's) status and message.
    Unchecked((StatusCode, String)),
    /// The machine lacks what the program's nodes need (`crate::needs`):
    /// installing it lets the same build through.
    Lacks(String),
}

impl From<Unrunnable> for (StatusCode, String) {
    fn from(unrunnable: Unrunnable) -> Self {
        match unrunnable {
            Unrunnable::Cannot(why) => (StatusCode::UNPROCESSABLE_ENTITY, why),
            Unrunnable::Unchecked(failed) => failed,
            Unrunnable::Lacks(why) => (StatusCode::PRECONDITION_FAILED, why),
        }
    }
}

/// Why the build `project` cannot turn on `keys`, one owner's triggers,
/// `saved` being what that owner's infra has saved: their setup run
/// (`setup_cut`, which reaches what only feeds a trigger, like an auth
/// node a gated route reads at setup) or a fire of one
/// (`run_spec::compute_trigger_fire`) needs a part that does not compile,
/// a node whose metadata did not load among them. Empty when both run.
fn cannot_come_on(
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
    saved: &weft_core::infra::bake::Saved,
) -> Vec<String> {
    let setup = setup_cut(project, &key_places(project, keys)).err();
    let fires = keys
        .iter()
        .filter_map(|key| weft_core::run_spec::compute_trigger_fire(project, &key.trigger, &serde_json::Value::Null, None, saved).err());
    setup.into_iter().chain(fires).collect()
}

/// The activations `scope` names in `project`, refusing a mismatch (a
/// per-instance trigger without an instance, a shared one with one, an
/// address that is not a trigger) and a scope that names nothing.
pub(crate) async fn resolve_scope(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let known = match scope.instances.every_instance() {
        true => known_instances(state, id).await?,
        false => Vec::new(),
    };
    resolve_against(project, scope, &known)
}

/// [`resolve_scope`] for a verb that turns triggers on or brings them up
/// to date (`weft activate`, `weft resync`): a scope that names nothing
/// (a program whose every trigger exists once per instance, asked for its
/// shared ones; every instance, with none yet) is nothing to do, never a
/// refusal, so a deploy can always run them. A mismatch is refused all the
/// same.
pub(crate) async fn resolve_scope_to_turn_on(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let known = match scope.instances.every_instance() {
        true => known_instances(state, id).await?,
        false => Vec::new(),
    };
    scope.resolve(project, &known).map_err(|why| (StatusCode::BAD_REQUEST, why))
}

/// [`resolve_scope`] once every instance weft holds anything for (`known`)
/// has been read.
fn resolve_against(
    project: &ProjectDefinition,
    scope: &weft_core::activation::ActivationScope,
    known: &[weft_core::instance::InstanceId],
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let keys = scope.resolve(project, known).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    if keys.is_empty() && !has_triggers(project) {
        return Err((StatusCode::PRECONDITION_FAILED, "this program has no triggers".to_string()));
    }
    if keys.is_empty() {
        let what = match &scope.instances {
            weft_core::instance::InstanceTarget::Shared => {
                "no shared trigger (every trigger it has exists once per instance; name instances with --instance or --all-instances)"
                    .to_string()
            }
            weft_core::instance::InstanceTarget::EveryInstance => "no instance weft holds anything for yet".to_string(),
            weft_core::instance::InstanceTarget::Everything => "no trigger yet, shared or of an instance weft holds anything for".to_string(),
            weft_core::instance::InstanceTarget::Named(_) => {
                "no trigger that exists once per instance, so there is nothing to act on for an instance".to_string()
            }
        };
        return Err((StatusCode::PRECONDITION_FAILED, format!("this program has {what}")));
    }
    Ok(keys)
}

/// The activations a verb that only takes triggers down acts on: the rows
/// `scope` admits. Picked from the rows rather than the source, because
/// what is listening is what the rows say: a trigger the source has since
/// dropped, or an instance's copy of one the program no longer runs per
/// instance, still has a row, and taking it down is the only way to stop
/// it. A named trigger with no row for any owner named is refused: nothing
/// of it is on, so the name is a mistake.
pub(crate) fn keys_to_take_down(
    rows: &[crate::activation_store::Activation],
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let keys: Vec<weft_core::activation::ActivationKey> = rows.iter().map(|a| &a.key).filter(|key| scope.admits(key)).cloned().collect();
    if let Some(unknown) = scope.triggers.iter().find(|name| !keys.iter().any(|key| &&key.trigger == name)) {
        let whose = match &scope.instances {
            weft_core::instance::InstanceTarget::Shared => "the program".to_string(),
            weft_core::instance::InstanceTarget::Named(ids) => {
                format!("instance {}", ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>().join(", "))
            }
            weft_core::instance::InstanceTarget::EveryInstance => "any instance".to_string(),
            weft_core::instance::InstanceTarget::Everything => "the program or any instance".to_string(),
        };
        return Err((
            StatusCode::NOT_FOUND,
            format!("trigger '{unknown}' was never activated for {whose}, so there is nothing of it to take down"),
        ));
    }
    Ok(keys)
}

/// Whether the program has any trigger, shared or per instance.
fn has_triggers(project: &ProjectDefinition) -> bool {
    !weft_core::project::trigger_places(project).is_empty()
}

/// The places the activations' triggers sit at: the seeds of their
/// setup run.
fn key_places(
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Vec<Located> {
    keys.iter()
        .map(|key| {
            let (id, path) = weft_core::project::resolve_address(project, &key.trigger);
            Located::new(id, path)
        })
        .collect()
}

/// The keys grouped by owner, the program first: one setup run sets up
/// one owner's triggers, against its own infra copies and values.
pub(crate) fn keys_by_owner(keys: &[weft_core::activation::ActivationKey]) -> Vec<(weft_core::instance::Owner, Vec<weft_core::activation::ActivationKey>)> {
    let mut grouped: std::collections::BTreeMap<weft_core::instance::Owner, Vec<weft_core::activation::ActivationKey>> = Default::default();
    for key in keys {
        grouped.entry(key.owner.clone()).or_default().push(key.clone());
    }
    grouped.into_iter().collect()
}

/// The owner the keys share: every setup acts on one owner's triggers at
/// a time ([`keys_by_owner`]).
fn keys_instance(keys: &[weft_core::activation::ActivationKey]) -> Option<&weft_core::instance::InstanceId> {
    keys.first().and_then(|key| key.instance())
}

/// `POST /projects/{id}/bake` (`weft bake`): see [`trigger_actions::bake`].
pub async fn bake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<BakeRequest>>,
) -> Result<Json<weft_core::program::triggers::BakeAnswer>, Refused> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    Ok(Json(trigger_actions::bake(&state, &operator(caller), id, &body).await?))
}


/// Refuse unless every infra copy the named activations' triggers' setup
/// reads is Running (the instance's own copy for a per-instance one).
///
/// The one rule joining the two lifetimes: a trigger reads its address
/// off the infra node feeding it, so arming it before that node is up
/// registers a signal against an address that does not answer. Nothing
/// here starts infra to satisfy itself; infra is the user's own verb (or
/// their program's), and provisioning on a click that said nothing about
/// it would spend their money for them.
///
/// `project` must be the definition the caller is about to REGISTER,
/// not the one on file: the registered one lags the source the moment
/// the user writes a node, and refusing over an infra node they have
/// already deleted is a dead end (see `require_action`, which exempts
/// the same fact for the same reason).
pub(crate) async fn require_trigger_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<(), (StatusCode, String)> {
    // Validation first: a trigger setup that cannot be selected at all
    // (a trigger inside a half-selected loop) is a 400 about the graph,
    // not a story about infra.
    let places = key_places(project, keys);
    let walked = weft_core::project::cut::walk_setup(&weft_core::project::graph::ProjectGraph::new(project), &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let within: HashSet<String> = walked
        .runs
        .iter()
        .filter(|place| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra))
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    let missing = missing_infra_nodes(state, project_id, project, Some(&within), keys_instance(keys)).await?;
    if missing.is_empty() {
        return Ok(());
    }
    let (listed, fix) = missing_and_fix(&missing);
    Err((
        StatusCode::PRECONDITION_REQUIRED,
        format!("these triggers' infra is not running: {listed}. Start it with {fix}, then run this again."),
    ))
}

/// Who asks for an activate.
pub(crate) enum ActivateAsker<'a> {
    /// A person (`weft activate`, the editor) or a program
    /// (`ctx.triggers().activate(..)`): held to the verb table
    /// ([`require_action`]), so it is refused while a build, a resync or
    /// an infra change is in flight, and by the claim while the install is
    /// in maintenance (`crate::maintenance`).
    Outside,
    /// weft bringing back triggers that went down with their infra, now
    /// it runs (and is healthy) again: past the verb table, since an infra
    /// change elsewhere in the project is no reason to leave them down;
    /// the claim still refuses a build, a resync and a removal.
    InfraBack,
    /// A resync bringing back the triggers it took down, under its drive
    /// (`crate::task_kinds::resync::ResyncPlan::driver`): the one
    /// activation that goes through while the resync holds the project.
    Resync(uuid::Uuid),
    /// A change of an instance's values re-arming its live triggers.
    Rearm(&'a Rearm<'a>),
    /// The maintenance lift bringing back the triggers `weft maintenance
    /// on` took down (`crate::maintenance`): past the verb table like an
    /// infra's return, and the one activation that goes through while the
    /// install is in maintenance.
    Lift,
}

/// Ready the registered build's workers for the triggers' setup, and
/// refuse first if their infra is not up. Workers of other builds are left
/// as they are: what runs on them finishes there, and they stop once idle.
pub(crate) async fn prepare_trigger_setup(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<(), (StatusCode, String)> {
    require_trigger_infra(state, id, project, keys).await?;
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running image: {e:#}"));
    match state.projects.running_binary_hash(id).await.map_err(internal)? {
        Some(hash) => prepare_running_image(state, id, &hash).await,
        None => Ok(()),
    }
}

/// `GET /projects/{id}/builds/{build}`: where the version build a 202 from
/// the build request named stands (`build`), with each image build it
/// waits on and, once registered, what registered, or why it ended
/// otherwise. What the caller following a build reads until it ends.
pub async fn version_build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, build)): Path<(uuid::Uuid, uuid::Uuid)>,
) -> Result<Json<weft_core::builds::VersionBuildState>, Refused> {
    authorize_project(&state, &caller.0, id).await?;
    // The database out of reach is the install unable to serve this right
    // now, never the build's state: a 503, which the caller following the
    // build reads as "look again". Anything else (a row that no longer
    // reads) is a 500 asking again would not fix.
    crate::build::waiting::state(&state.pg_pool, id, build)
        .await
        .map_err(|e| crate::api::unavailable_or_internal("read where the build is", e))?
        .map(Json)
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("no build {build} of this project")))
}

/// `POST /projects/{id}/activations` (`weft activate`): see [`trigger_actions::begin_activate`].
pub async fn activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<ActivateRequest>>,
) -> Result<Json<weft_core::activation::ActivateStarted>, Refused> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    Ok(Json(trigger_actions::begin_activate(&state, &operator(caller), id, body).await?))
}

/// `POST /projects/{id}/activate`: where a weft CLI from before an
/// activation ran as a task of the install's asked for one, waiting for
/// what it turned on. It could not read what [`activate`] answers, so it
/// is refused with the words it prints, and nothing is activated.
pub async fn activate_from_an_older_cli() -> (StatusCode, &'static str) {
    (StatusCode::GONE, OLDER_CLI_ACTIVATES)
}

/// What a weft CLI from before activations ran as tasks is told.
const OLDER_CLI_ACTIVATES: &str = "this weft CLI is older than the install, which now carries out `weft activate` as a task the CLI follows; \
     nothing was activated. Update the CLI to the install's weft (`./setup.sh --cli` in a weft checkout at the install's commit) and run it again";

/// How `activate_with` must roll back a failed Activating-window. Every
/// signal it armed is taken back first, so the rollback puts each
/// trigger back as the claim found it, and turns them off only when an
/// armed signal could not be put back and nothing held would be lost.
#[derive(Clone, Copy)]
enum ActivateRollback {
    /// Put every trigger back as the claim found it (a held one held, a
    /// live one listening as before, one never activated without a row)
    /// and cancel the setup run. Keeps existing signals and what waits on
    /// them: held arrivals and the waits of runs those triggers fired,
    /// which a transient error must never cost.
    Restore,
    /// End the activation refusing, cancel its setup run and drop its
    /// triggers' own entry rows: for a re-arm, or triggers that held
    /// nothing, whose armed signals could not all be put back.
    WipeSignals,
}

struct ActivateWindowError {
    status: StatusCode,
    msg: String,
    rollback: ActivateRollback,
    /// The database did not take the step that failed
    /// (`crate::api::database_did_not_take`): [`ActivateError::Database`].
    database: bool,
}

/// The Activating-window of one owner's activation: every step that runs
/// while the named activations are `Activating`, ending with the CAS to
/// `Active`. Factored out so `activate_owner` has ONE rollback site for
/// the whole window (a failure anywhere here must un-stick them; the
/// caller does that once on Err).
///
/// The error carries the rollback MODE (`ActivateRollback`): failures
/// before the first arm restore (keep signals); from arming on, what was
/// armed is taken back and the triggers restored (`take_back_arms`).
///
/// Single-flight against a concurrent activate of the same triggers is
/// the exclusive Activating claim (`try_begin_activating`), not a lock.
#[allow(clippy::too_many_arguments)]
async fn activate_trigger_setup_window(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    drops: bool,
    project: &ProjectDefinition,
    program: &weft_core::project::hash::ProgramIdentity,
    rearm: Option<&Rearm<'_>>,
    task: &weft_task_store::executor::TaskClaim,
) -> Result<Vec<ActivationUrl>, ActivateWindowError> {
    // Failures before the first arm haven't touched signal state beyond
    // dropping what was held (the person's answer), so they only need the
    // triggers put back.
    // A step answering 503 is one the database did not take: said as
    // `database`, so the window's one decision (`activate_owner`) is the
    // only thing that answers 503, and only once the triggers are back as
    // the claim found them.
    let unstick = |(status, msg): (StatusCode, String)| {
        let database = status == StatusCode::SERVICE_UNAVAILABLE;
        ActivateWindowError {
            status: if database { StatusCode::INTERNAL_SERVER_ERROR } else { status },
            msg,
            rollback: ActivateRollback::Restore,
            database,
        }
    };

    // The infra precondition runs FIRST: an activation whose triggers need
    // infra that is down is refused here, against the definition this
    // activation just built, before anything is touched. Then the
    // registered build's workers are readied; what runs on others is left
    // to finish there.
    prepare_trigger_setup(state, id, project, keys).await.map_err(unstick)?;

    // Drop what they held, when asked to, now that this activation holds
    // the exclusive Activating claim (a losing concurrent activate never
    // drops the winner's). Asked once, of what they held when it was
    // asked: an activation carried on dropped it already, and what its
    // triggers held since is released as they land.
    let kept = drop_held(state, id, keys, activation, drops).await.map_err(unstick)?;
    // Held work dropped stays dropped whatever the rollback puts back, so
    // from here a step the database did not take is no longer one nothing
    // of the activation ran (here, in `record_activation_source`'s failure,
    // and in `take_back_arms`).
    let unstick = move |failed| {
        let error = unstick(failed);
        ActivateWindowError { database: error.database && !drops, ..error }
    };
    // An arm that cannot be put back leaves its signal row as the failed
    // put-back left it. Triggers coming up from down with work kept on
    // them stay down, so that work is never lost; live triggers a re-arm
    // set up again no longer match what they ran, so they are turned off,
    // as are triggers that kept nothing.
    let if_not_put_back = if rearm.is_none() && kept { ActivateRollback::Restore } else { ActivateRollback::WipeSignals };

    // Capture first, then arm that completed capture. The capture
    // registers nothing, so until the first arm a failure puts the
    // triggers back as they were. Arming refreshes existing entry rows
    // while retaining their tokens and held arrivals; a re-arm sets its
    // live triggers up again with the values it stores as it lands. A
    // trigger that fails to arm takes back the ones armed before it (a
    // row it created goes, a row it replaced is written back and the
    // listener runs it again), so the triggers end exactly as they were:
    // a held one held with its signals and what waits on them.
    step(state, id, activation, crate::activation_store::ActivationStep::SettingUp).await.map_err(unstick)?;
    let bake = run_trigger_setup(state, id, project, keys, program, Some((activation, task)), rearm.and_then(|r| r.overlay))
        .await
        .map_err(unstick)?;
    step(state, id, activation, crate::activation_store::ActivationStep::Arming).await.map_err(unstick)?;
    // Each trigger's cut, recorded with the code its listeners fire: what
    // tells whether it is behind, and what an infra command takes down
    // with it.
    let cuts = trigger_cuts_of(project, keys).map_err(unstick)?;
    let recorded = state
        .activations
        .record_activation_source(id, activation, &bake.program, &bake.source_version, &cuts)
        .await
        .map_err(|e| ActivateWindowError {
            database: crate::api::database_did_not_take(&e) && !drops,
            ..unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("record activation source: {e:#}")))
        })?;
    if !recorded {
        return Err(unstick((StatusCode::CONFLICT, format!("activation {activation} no longer owns its triggers"))));
    }
    // How each stood as the claim found it says whether it listened while
    // it was down, which decides what its signal carries over; one armed
    // already by this setup (the task was run again after arming it)
    // arms nothing more.
    let (claimed, already) = match armed_already(state, id, keys, activation).await {
        Ok(found) => found,
        Err(e) => return Err(unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("read what this activation armed: {e:#}")))),
    };
    let mut armed = Vec::new();
    for key in keys {
        // A trigger its setup skipped (behind a closed gate) arms nothing.
        let Some(capture) = bake.captured.get(&key.trigger) else { continue };
        if already.contains(key) {
            continue;
        }
        let listened = claimed.iter().find(|a| &a.key == key).and_then(|a| a.activating).is_some_and(|a| a.listened);
        let arming = crate::task_kinds::register_signal::RegisterSignalExecutor::arm(state,
            weft_task_store::RegisterSignalPayload {
                run_id: bake.run_id, node_id: key.trigger.clone(), frames: Vec::new(),
                spec: capture.spec.clone(), is_resume: false, call_index: 0,
                port_snapshot: Some(capture.ports.clone()),
                asked_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            }, listened).await;
        let error = match arming {
            Ok(done) => {
                armed.push(done);
                continue;
            }
            Err(error) => error,
        };
        return Err(take_back_arms(state, if_not_put_back, drops, armed, format!("arm trigger {key}"), error).await);
    }

    // Every step that can fail runs before the one delete (the orphan
    // rows below) except the flip to Active after it, so up to the delete
    // a re-arm can still be put back exactly as it was, and after it only
    // the orphan rows stay gone. The orphans are read first: the listener
    // is told to leave them down, since they are about to go and one that
    // cannot come up must not fail the activation deleting it.
    let orphans = match orphan_entry_tokens(state, id, project, keys_instance(keys)).await {
        Ok(orphans) => orphans,
        Err(e) => return Err(take_back_arms(state, if_not_put_back, drops, armed, "read orphan entry rows".into(), e).await),
    };

    // Reconcile what the listener holds of this project with the durable
    // signal table (resume signals belong to suspended runs whose
    // workers are gone). /rehydrate is idempotent, and fails only on this
    // project's own rows.
    if let Err(e) = state.listener.rehydrate(id, &orphans).await {
        return Err(take_back_arms(state, if_not_put_back, drops, armed, "listener rehydrate".into(), e).await);
    }

    // The addresses the armed triggers listen at, read before the flip:
    // past it nothing may fail the call. The orphans are other triggers'
    // rows, so they are not among these keys'.
    let urls = match collect_listener_urls(state, id, keys).await {
        Ok(urls) => urls,
        Err(e) => return Err(take_back_arms(state, if_not_put_back, drops, armed, "collect_listener_urls".into(), e).await),
    };

    // Last, the one step that cannot be taken back once it lands: drop
    // this owner's entry rows for triggers the source no longer has (the
    // user edited it while they were down). The delete is one
    // transaction, and the listener is told to forget the rows only
    // after it commits, so a failure deletes nothing: every orphan row is
    // still there (held, left down by the rehydrate above) and a re-arm
    // is put back like any earlier failure.
    if let Err(e) = drop_orphan_entry_rows(state, id, &orphans, activation).await {
        let what = format!("drop the entry rows of triggers the source no longer has ({} row(s), none deleted)", orphans.len());
        return Err(take_back_arms(state, if_not_put_back, drops, armed, what, e).await);
    }

    // Flip Activating → Active. The claim guards against a concurrent
    // cancel_activate that ended the activation: in that case the cancel
    // already put them down, so we surrender with no further rollback. The
    // setup run is terminal by now (`run_trigger_setup` returned on its
    // terminal), so the run the flip clears is only bookkeeping. A
    // re-arm's values are stored in the same transaction: until the flip,
    // the triggers' fires are held (an activating trigger's are not
    // visible yet), and the release after it routes them with the values
    // the setup ran with.
    let ended = match state
        .activations
        .end_activating(
            id,
            activation,
            &crate::activation_store::ActivationLifecycle::active(),
            false,
            rearm.and_then(|r| r.store.as_ref()),
        )
        .await
    {
        Ok(ended) => ended.is_some(),
        Err(e) => return Err(take_back_arms(state, if_not_put_back, drops, armed, "end_activating".into(), e).await),
    };
    if !ended {
        return Err(ActivateWindowError {
            status: StatusCode::CONFLICT,
            msg: "activate raced with cancel-activate; the triggers are now down".into(),
            rollback: ActivateRollback::Restore,
            database: false,
        });
    }
    Ok(urls)
}

/// Say activation `activation` of project `id` is at `step` now, for
/// `weft status`: refused when it no longer owns its triggers (a cancel
/// ended it), which is the end of its window.
async fn step(
    state: &DispatcherState,
    id: uuid::Uuid,
    activation: uuid::Uuid,
    step: crate::activation_store::ActivationStep,
) -> Result<(), (StatusCode, String)> {
    match state.activations.set_step(id, activation, step).await {
        Ok(true) => {
            crate::transition::publish_transition_changed(state, id).await;
            Ok(())
        }
        Ok(false) => Err((StatusCode::CONFLICT, format!("activation {activation} no longer owns its triggers"))),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("say where activation {activation} is: {e:#}"))),
    }
}

/// The rows activation `activation` claimed among `keys`, and those of
/// `keys` whose entry signal its own setup armed already: a task run again
/// after arming them carries on past them.
async fn armed_already(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
) -> anyhow::Result<(Vec<crate::activation_store::Activation>, Vec<weft_core::activation::ActivationKey>)> {
    let claimed: Vec<crate::activation_store::Activation> = state
        .activations
        .list(id)
        .await?
        .into_iter()
        .filter(|a| a.lifecycle.activating_run_id == Some(activation) && keys.contains(&a.key))
        .collect();
    let signals = crate::store::postgres::activation_signals(&state.pg_pool, id, keys).await?;
    let already = keys
        .iter()
        .filter(|key| {
            signals.iter().any(|signal| {
                !signal.is_resume
                    && signal.setup_run_id == Some(activation)
                    && signal.node_id == key.trigger
                    && signal.instance.as_ref() == key.instance()
            })
        })
        .cloned()
        .collect();
    Ok((claimed, already))
}

/// Each of `keys`' trigger's cut in `project`, by trigger, as its
/// activation records it.
fn trigger_cuts_of(
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<std::collections::BTreeMap<String, crate::activation_store::RecordedCut>, (StatusCode, String)> {
    keys.iter()
        .map(|key| {
            weft_core::run_spec::trigger_cut(project, &key.trigger)
                .map(|cut| (key.trigger.clone(), cut.into()))
                .map_err(|why| (StatusCode::BAD_REQUEST, format!("the cut of trigger {key}: {why}")))
        })
        .collect()
}


/// Why [`activate_with`] did not activate.
#[derive(Debug)]
pub(crate) enum ActivateError {
    /// The single-flight claim was refused, and why, as the claim's own
    /// transaction decided it.
    Refused(crate::activation_store::ClaimRefused),
    Failed(StatusCode, String),
    /// The install's database did not take a step of it (too busy, or out
    /// of reach: `crate::api::database_did_not_take`), and what the step
    /// was: the same activation goes through once the database answers.
    Database(String),
}

impl ActivateError {
    /// A step doing `what` that failed with `e`: [`ActivateError::Database`]
    /// when the database did not take it, a 500 otherwise.
    pub(crate) fn failed(what: &str, e: anyhow::Error) -> Self {
        if crate::api::database_did_not_take(&e) {
            return ActivateError::Database(format!("{what}: the install's database was busy or out of reach ({e:#})"));
        }
        ActivateError::Failed(StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"))
    }
}

impl From<(StatusCode, String)> for ActivateError {
    fn from((code, message): (StatusCode, String)) -> Self {
        ActivateError::Failed(code, message)
    }
}

impl From<ActivateError> for (StatusCode, String) {
    fn from(error: ActivateError) -> Self {
        use crate::activation_store::ClaimRefused;
        match error {
            ActivateError::Refused(refused) => (
                StatusCode::CONFLICT,
                match refused {
                    ClaimRefused::Building => "the project is building; wait for it to finish or cancel it with `weft cancel-build`",
                    ClaimRefused::Claimed => "these triggers are already activating; wait for it to finish or cancel it",
                    ClaimRefused::NotExpected => "a trigger this re-arm read as on was taken down meanwhile",
                    ClaimRefused::Resyncing => crate::project_store::ProjectTransition::Resyncing.refusal().unwrap_or_default(),
                    ClaimRefused::ResyncGone => "the resync bringing these triggers back was given up, or taken up again, meanwhile",
                    ClaimRefused::Removing => weft_core::projects::PROJECT_BEING_REMOVED,
                    ClaimRefused::Maintenance => weft_core::maintenance::IN_MAINTENANCE,
                }
                .into(),
            ),
            ActivateError::Failed(code, message) => (code, message),
            ActivateError::Database(message) => (StatusCode::SERVICE_UNAVAILABLE, message),
        }
    }
}

/// Refuse a verb that names a build other than the registered one: a
/// teammate's build landed between the caller's build and this verb, and
/// acting would run it under the caller's name. A verb that names none
/// (by id, or a program's own) acts on the registered build.
pub(crate) async fn require_registered_build(
    state: &DispatcherState,
    id: uuid::Uuid,
    named: &weft_core::builds::BuildHashes,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e}"));
    let running = (
        state.projects.running_binary_hash(id).await.map_err(internal)?,
        state.projects.running_definition_hash(id).await.map_err(internal)?,
        state.projects.running_infra_hash(id).await.map_err(internal)?,
    );
    let differs = |named: &Option<String>, running: &Option<String>| named.is_some() && named != running;
    if differs(&named.binary_hash, &running.0)
        || differs(&named.definition_hash, &running.1)
        || differs(&named.infra_hash, &running.2)
    {
        return Err((
            StatusCode::CONFLICT,
            "the project's build moved since this was prepared (another build registered after \
             it); run the command again"
                .into(),
        ));
    }
    Ok(())
}

/// A change of an instance's values, and the instance's live triggers it
/// re-arms (see `crate::instance_values::change`): the activation window
/// sets those triggers up again with `values`, arms them on what that
/// setup captured, and stores `store` in the transaction that lands them
/// Active. A failure before arming puts the triggers back as they were;
/// one after takes them down, and either way the values stay as they were.
pub(crate) struct Rearm<'a> {
    /// The instance's change the setup runs with, in place of what is
    /// stored; none when what is stored is already the answer (a pick
    /// change, stored before its re-arm: `crate::install_picks`).
    pub overlay: Option<&'a weft_core::instance::ValueChanges>,
    /// The same change as the store writes it; `None` for a trigger
    /// re-armed on a change another re-arm already stored.
    pub store: Option<crate::activation_store::ValuesStore<'a>>,
}

/// The install's picks a run of `program` (its cut) carries, checked by
/// `weft_core::picks::run_picks`: a 422 naming every required connection
/// nobody picked on this install.
pub(crate) async fn picks_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    program: &ProjectDefinition,
) -> Result<weft_core::picks::Picks, (StatusCode, String)> {
    if weft_core::picks::picked_places(program).is_empty() {
        return Ok(Default::default());
    }
    let stored = held_picks(state, project_id, Read::Held).await?;
    match weft_core::picks::run_picks(program, &stored.rows) {
        Ok(picks) => Ok(picks),
        // Confirmed against the rows before refusing (see `Read`).
        Err(_) => {
            let stored = held_picks(state, project_id, Read::Fresh).await?;
            weft_core::picks::run_picks(program, &stored.rows).map_err(|refusal| refusal_error(&refusal))
        }
    }
}

/// Every pick this install keeps for the project.
pub(crate) async fn stored_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<weft_core::picks::Picks, (StatusCode, String)> {
    Ok(held_picks(state, project_id, Read::Fresh).await?.rows.clone())
}

async fn held_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    read: Read,
) -> Result<std::sync::Arc<crate::held::OfTenant<weft_core::picks::Picks>>, (StatusCode, String)> {
    let load = || async {
        let tenant = crate::instance_values::owning_tenant(state, project_id).await?;
        let rows = weft_access_store::install_picks(&state.pg_pool, &tenant, project_id)
            .await
            .map_err(crate::api::access_store_error)?;
        Ok::<_, (StatusCode, String)>(crate::held::OfTenant { tenant, rows })
    };
    match read {
        Read::Held => state.held.picks.get_or_load(project_id, load).await,
        Read::Fresh => state.held.picks.load_fresh(project_id, load).await,
    }
}

/// Turn on the triggers `request` names that are off, one owner at a time
/// (one setup run sets up one owner's triggers), and leave the ones on as
/// they are: with nothing off it turns nothing on, and says so. A re-arm
/// is the other way round: its triggers are on, and it sets them up again.
/// It never touches the runs going and never starts infra.
///
/// An activation is always carried out by a task, since it waits on its
/// setup run, which may wait days on a person, and whoever asked may go
/// away meanwhile (a request whose dispatcher copy stopped): under a
/// task's claim (a program's call, a resync, the maintenance lift, an
/// `activate` task) it runs here; anywhere else it is handed to an
/// `activate` task, and this waits for that task's end
/// (`crate::task_kinds::activate`). The rows it claims name the task
/// (`crate::activation_store::Activation::driver_task`), so a task run
/// again (its claimant died, the claim was handed back) carries on what
/// it had started ([`plan_activation`]).
pub(crate) async fn activate_with(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
    asker: ActivateAsker<'_>,
) -> Result<Json<ActivateResponse>, ActivateError> {
    let Some(claim) = weft_task_store::executor::current_claim() else {
        return crate::task_kinds::activate::through_task(state, id, request, &asker).await.map(Json);
    };
    let ActivateRequest { build, scope, held, port } = request;
    let activations = state.activations.list(id).await.map_err(|e| ActivateError::failed("activations", e))?;
    let carried = activations.iter().any(|a| a.driver_task == Some(claim.task()));
    // Enforce against the reconciliation: a build or a resync in flight,
    // triggers being switched on, infra changing, a project `weft rm`
    // marked refuse it here. The table is the shared triggers' (an
    // instance's are on or off of their own), so an instance's activate
    // goes by the claim below alone, which refuses a build, a resync and a
    // removal under the row's lock, for one that starts meanwhile too. A
    // task run again that carries on what it started passed this check
    // then, and its own triggers turning on are what the table would
    // refuse now; only one that also has triggers to claim afresh is
    // checked again, below, once its plan says so.
    let checks_table = matches!(asker, ActivateAsker::Outside) && scope.instances.is_shared();
    if checks_table && !carried {
        require_action(state, id, &["activate"]).await?;
    }
    // Every trigger an activate switches on is checked for a setup or a
    // fire that could never run, whoever asks (a person, a program's run,
    // an infra or health check bringing it back); a re-arm's triggers are
    // on already, and a resync checks its own itself, before taking any
    // down and before bringing each owner back.
    let checks_runnable = !matches!(asker, ActivateAsker::Rearm(_) | ActivateAsker::Resync(_));
    use crate::activation_store::Claimant;
    let (rearm, claimant) = match asker {
        ActivateAsker::Outside | ActivateAsker::InfraBack => (None, Claimant::Anyone),
        ActivateAsker::Resync(driver) => (None, Claimant::Resync(driver)),
        ActivateAsker::Rearm(rearm) => (Some(rearm), Claimant::Anyone),
        ActivateAsker::Lift => (None, Claimant::Lift),
    };
    // What runs is what the last build registered (its program, hashes
    // and every place's images together, `POST /projects/{id}/builds`);
    // hashes named here only check the caller means that build.
    require_registered_build(state, id, &build).await?;

    // One coherent (hash, shape) pair for everything below: the
    // trigger checks here AND the kicks the activate window computes
    // must come from the definition recorded under the hash that
    // trigger-setup will journal (see `coherent_definition`).
    let (program, project) = coherent_definition(state, id).await?;

    // Activate is the trigger-setup verb. A program without a trigger
    // for this owner has nothing to register; refuse loudly so the CLI /
    // extension / program knows the verb doesn't apply.
    if !project.nodes.iter().any(|n| n.features.is_trigger) {
        return Err(ActivateError::Failed(
            StatusCode::PRECONDITION_FAILED,
            "project has no trigger nodes; nothing to activate. \
             Use `weft run` to fire a run directly."
                .into(),
        ));
    }
    let keys = resolve_scope_to_turn_on(state, id, &project, &scope).await?;
    let plan = plan_activation(keys, &activations, claim.task(), rearm.is_some());
    if checks_table && carried && !plan.claim.is_empty() {
        require_action(state, id, &["activate"]).await?;
    }
    if plan.is_empty() {
        // Nothing to turn on is no error: CI runs this on every deploy.
        // The address the routes answer at, as the front already serves
        // it, is said all the same: the front is never moved onto a build
        // the triggers on do not fire.
        return Ok(Json(ActivateResponse {
            per_instance_left_out: scope.per_instance_left_out(&project),
            address: project_address(state, id).await,
            broken: project.broken.clone(),
            ..Default::default()
        }));
    }
    // One binary answers the project's routes: these come on on the
    // registered build's, so every other trigger on must be on it too.
    // Refused before anything changes. What this task started before was
    // checked then.
    let units = plan.units();
    let moving: Vec<weft_core::activation::ActivationKey> = units.iter().flat_map(|unit| unit.keys.iter().cloned()).collect();
    let elsewhere = on_another_binary(&activations, &moving, &program.binary_hash);
    if !plan.claim.is_empty() && !elsewhere.is_empty() {
        return Err(ActivateError::Failed(StatusCode::CONFLICT, on_another_binary_refusal(&elsewhere)));
    }
    if checks_runnable && !plan.claim.is_empty() {
        refuse_unrunnable(state, id, &project, &plan.claim).await.map_err(<(StatusCode, String)>::from)?;
    }
    // Every connection the program needs picked on this install, before
    // any trigger moves: a run born without one is refused at its first
    // call, long after the activation said yes. A re-arm skips it: its
    // triggers are live already, and the change it carries is the
    // instance's or a pick's own, checked where it was asked.
    if rearm.is_none() && !plan.claim.is_empty() {
        crate::install_picks::require_activation_picks(state, id, &project).await?;
    }
    // The shared infra that is not running, read now for the note the
    // answer carries: past the Active flip nothing may fail the call.
    let idle: Vec<MissingCopy> = missing_infra_nodes(state, id, &project, None, None)
        .await?
        .into_iter()
        .filter(|copy| !copy.needs_instance)
        .collect();

    let mut answer = ActivateResponse::default();
    let mut released = weft_core::projects::HeldWork::default();
    for unit in units {
        let owned = match unit.how {
            Unit::Landed => land_turned_on(state, id, &unit.keys).await,
            Unit::Claim | Unit::Resume { .. } => {
                activate_owner(state, id, &unit.keys, held, &project, &program, rearm, claimant, &claim, unit.how).await
            }
        }
        .map_err(|error| match error {
            // Owners turned on before this one stay on: name them, so the
            // failure is read as what it is.
            ActivateError::Failed(code, message) if !answer.turned_on.is_empty() => {
                let on: Vec<String> = answer.turned_on.iter().map(|owned| weft_core::deactivation::whose_triggers(&owned.owner)).collect();
                ActivateError::Failed(code, format!("{message} ({} are on; this one is not)", on.join(", ")))
            }
            error => error,
        })?;
        released += owned.held;
        answer.urls.extend(owned.urls);
        answer.turned_on.push(weft_core::activation::OwnerTriggers { owner: unit.owner, triggers: unit.keys.iter().map(|k| k.trigger.clone()).collect() });
    }
    answer.held = (!released.is_empty()).then_some(weft_core::activation::HeldReleased { held: released, release: held });
    answer.infra_not_running = (!idle.is_empty()).then(|| {
        let (listed, fix) = missing_and_fix(&idle);
        format!("infra not running: {listed}. Activating does not start it; if anything uses it, run {fix}.")
    });
    answer.per_instance_left_out = scope.per_instance_left_out(&project);
    // The program the project now runs answers its callers at the
    // project's own address, straight (`crate::front`).
    answer.address = crate::front::serve_or_log(state, id, port).await;
    answer.broken = project.broken.clone();
    Ok(Json(answer))
}

/// What an activate does with the triggers it names, from how their rows
/// stand ([`plan_activation`]).
#[derive(Debug, Default, PartialEq)]
pub(crate) struct ActivationPlan {
    /// Activations the asking task claimed before (it was run again) and
    /// that are still activating: carried on, each under its own setup run,
    /// with every row it claimed (a claim is taken whole).
    pub resume: Vec<Resumed>,
    /// Triggers the asking task turned on before: what is left of their
    /// activation is releasing what they held, and saying they came on.
    pub landed: Vec<weft_core::activation::ActivationKey>,
    /// Triggers to claim now: the ones that are off (for a re-arm, the
    /// ones named, which are on).
    pub claim: Vec<weft_core::activation::ActivationKey>,
}

/// An activation the asking task claimed before and carries on.
#[derive(Debug, PartialEq)]
pub(crate) struct Resumed {
    /// Its setup run.
    pub run: uuid::Uuid,
    /// Whether it got past dropping what its triggers held, when that was
    /// asked (`ActivationStep::Preparing` is the step that does it): one
    /// that did never drops again what they held since.
    pub prepared: bool,
    pub keys: Vec<weft_core::activation::ActivationKey>,
}

/// How one owner's triggers go through [`activate_owner`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unit {
    /// Claimed now, under a new setup run.
    Claim,
    /// Claimed by the asking task before, under setup run `run`: carried
    /// on, past what it did before it was run again ([`Resumed`]).
    Resume { run: uuid::Uuid, prepared: bool },
    /// Turned on by the asking task before: only finished.
    Landed,
}

/// One owner's triggers and how they go.
#[derive(Debug, PartialEq)]
pub(crate) struct PlannedUnit {
    pub how: Unit,
    pub owner: weft_core::instance::Owner,
    /// Never empty.
    pub keys: Vec<weft_core::activation::ActivationKey>,
}

impl ActivationPlan {
    fn is_empty(&self) -> bool {
        self.resume.is_empty() && self.landed.is_empty() && self.claim.is_empty()
    }

    /// The units in the order they go: what the task started before first
    /// (carried on, then finished), then the claims, one owner each.
    pub(crate) fn units(&self) -> Vec<PlannedUnit> {
        // A claim is one owner's (one setup run sets up one owner's
        // triggers), so a resumed one has a single owner.
        let resumed = self.resume.iter().flat_map(|resumed| {
            let how = Unit::Resume { run: resumed.run, prepared: resumed.prepared };
            keys_by_owner(&resumed.keys).into_iter().map(move |(owner, keys)| PlannedUnit { how, owner, keys })
        });
        let landed = keys_by_owner(&self.landed).into_iter().map(|(owner, keys)| PlannedUnit { how: Unit::Landed, owner, keys });
        let claims = keys_by_owner(&self.claim).into_iter().map(|(owner, keys)| PlannedUnit { how: Unit::Claim, owner, keys });
        resumed.chain(landed).chain(claims).collect()
    }
}

/// Decide what an activate of `keys` by task `task` does, from `rows`, the
/// project's activations as they stand. Pure.
///
/// A row the task claimed and that is still activating is an activation it
/// started before and carries on: every row of that claim, whichever of
/// them `keys` names, since a claim is taken and landed whole; one it
/// claimed that is on since is one it turned on, whose activation only
/// has its held work left to release. Of the rest, an activate claims the
/// ones that are off and leaves the ones on (or being switched on by
/// someone else) as they are; a re-arm (`rearm`) claims every one named,
/// which are on, and the claim refuses one that is not.
pub(crate) fn plan_activation(
    keys: Vec<weft_core::activation::ActivationKey>,
    rows: &[crate::activation_store::Activation],
    task: uuid::Uuid,
    rearm: bool,
) -> ActivationPlan {
    use crate::project_store::ProjectStatus;
    let row = |key: &weft_core::activation::ActivationKey| rows.iter().find(|a| &a.key == key);
    let mine = |a: &crate::activation_store::Activation| a.driver_task == Some(task);
    let mut plan = ActivationPlan::default();
    for key in &keys {
        let Some(a) = row(key).filter(|a| mine(a) && a.lifecycle.status == ProjectStatus::Activating) else { continue };
        let Some(run) = a.lifecycle.activating_run_id else { continue };
        if plan.resume.iter().any(|resumed| resumed.run == run) {
            continue;
        }
        let prepared = a.activating.is_none_or(|activating| activating.step != crate::activation_store::ActivationStep::Preparing);
        let claimed: Vec<weft_core::activation::ActivationKey> = rows
            .iter()
            .filter(|a| a.lifecycle.status == ProjectStatus::Activating && a.lifecycle.activating_run_id == Some(run))
            .map(|a| a.key.clone())
            .collect();
        plan.resume.push(Resumed { run, prepared, keys: claimed });
    }
    let resumed = |key: &weft_core::activation::ActivationKey| plan.resume.iter().any(|resumed| resumed.keys.contains(key));
    let (landed, rest): (Vec<_>, Vec<_>) = keys
        .into_iter()
        .filter(|key| !resumed(key))
        .partition(|key| row(key).is_some_and(|a| mine(a) && a.lifecycle.status == ProjectStatus::Active));
    plan.landed = landed;
    plan.claim = match rearm {
        true => rest,
        false => rest
            .into_iter()
            .filter(|key| !row(key).is_some_and(|a| matches!(a.lifecycle.status, ProjectStatus::Active | ProjectStatus::Activating)))
            .collect(),
    };
    plan
}

/// The triggers on, other than `moving` (the ones a verb is bringing onto
/// the registered build), that fire another binary than `binary`, the
/// registered build's: what keeps a verb from moving `moving` onto it,
/// since one binary answers a project's routes. A change that made no new
/// binary (the graph alone) leaves every trigger on it already.
pub(crate) fn on_another_binary(
    activations: &[crate::activation_store::Activation],
    moving: &[weft_core::activation::ActivationKey],
    binary: &str,
) -> Vec<weft_core::activation::ActivationKey> {
    activations
        .iter()
        .filter(|a| a.lifecycle.status == crate::project_store::ProjectStatus::Active && !moving.contains(&a.key))
        .filter(|a| a.program.as_ref().is_some_and(|program| program.binary_hash != binary))
        .map(|a| a.key.clone())
        .collect()
}

/// The refusal [`on_another_binary`]'s triggers make, naming them and the
/// way through.
pub(crate) fn on_another_binary_refusal(elsewhere: &[weft_core::activation::ActivationKey]) -> String {
    format!("{}; then run this again", on_another_binary_words(elsewhere))
}

/// Why a trigger taken down by the health check or with its infra stays
/// down while [`on_another_binary`]'s triggers are on (`TriggerCopy::held_back`).
fn held_back_words(elsewhere: &[weft_core::activation::ActivationKey]) -> String {
    format!("{}; after it, this one comes back on its own", on_another_binary_words(elsewhere))
}

fn on_another_binary_words(elsewhere: &[weft_core::activation::ActivationKey]) -> String {
    format!(
        "one binary answers a project's routes, and these triggers are on another build than the registered one: {}. \
         `weft resync --all` moves every trigger that is on to the new build",
        weft_core::activation::OwnerTriggers::list_words(&weft_core::activation::by_owner(elsewhere))
    )
}

/// What one owner's activation did.
struct OwnerActivated {
    urls: Vec<ActivationUrl>,
    /// What its triggers held as they came on, released or dropped.
    held: weft_core::projects::HeldWork,
}

/// Turn on `keys`, one owner's triggers that are off (or, for a re-arm, on),
/// under one claim and one setup run, driven by the task whose claim is
/// `task`: claimed now (`Unit::Claim`), or carried on from where the rows of
/// a claim that task took before say it is (`Unit::Resume`, the task was
/// run again). A carried-on activation that fails is ended the way an
/// unfinished one is (`end_unfinished_activation`): how its triggers stood
/// before its claim was known only to the process that took it.
#[allow(clippy::too_many_arguments)]
async fn activate_owner(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    held: ReleaseHeld,
    project: &ProjectDefinition,
    program: &weft_core::project::hash::ProgramIdentity,
    rearm: Option<&Rearm<'_>>,
    claimant: crate::activation_store::Claimant,
    task: &weft_task_store::executor::TaskClaim,
    how: Unit,
) -> Result<OwnerActivated, ActivateError> {
    let (activation, previous) = match how {
        // Single-flight gate: atomically claim Activating for every named
        // activation. While a trigger is activating (registering its
        // signals), no second activation of it may start. The claim wins
        // or loses whole; a losing caller (a concurrent activate from
        // another dispatcher, a double-click, CLI + extension racing, a
        // program activating the same instance twice) bails here with 409
        // BEFORE any signal is touched. Every later write is guarded by
        // this activation's reserved run.
        Unit::Claim => {
            let activation = weft_core::new_run_id();
            let expect = rearm.is_some().then_some(crate::activation_store::ProjectStatus::Active);
            let previous = state
                .activations
                .try_begin_activating(id, keys, activation, expect, claimant, task.task())
                .await
                .map_err(|e| ActivateError::failed("try_begin_activating", e))?
                .map_err(ActivateError::Refused)?;
            // Won the claim: broadcast it (other tabs' screens flip to
            // "turning on..." without a verb round-trip).
            crate::transition::publish_transition_changed(state, id).await;
            (activation, Some(previous))
        }
        Unit::Resume { run: activation, .. } => {
            tracing::info!(target: "weft_dispatcher::activate", project_id = %id, %activation, "carrying on an activation this task started before");
            (activation, None)
        }
        Unit::Landed => unreachable!("a unit already landed is finished by `land_turned_on`, never driven"),
    };
    // What the triggers held is dropped, when asked to, once: by the claim
    // that asked, or by one carried on that had not got that far before
    // its task was run again (the step it is at says so).
    let drops = held == ReleaseHeld::Drop && matches!(how, Unit::Claim | Unit::Resume { prepared: false, .. });
    // While the task's claim is renewed, the `lapses` loop looks when it
    // would lapse, which is when it tells whether the task ended with this
    // activation still activating (`crate::lapses::earliest_lapse`); this
    // look starts that chain.
    crate::lapses::book_after_or_log(state, weft_task_store::tasks::claim_duration_secs()).await;

    // Everything from here to the Active flip happens while the
    // activations are Activating. ANY failure in this window must
    // un-stick them (a stranded Activating locks out every future
    // activate of those triggers). Rather than hand-roll a rollback at
    // each `?`, the whole window is ONE fallible block with ONE rollback
    // site below. A future step added here can't forget the un-stick.
    let setup = async {
        // What they hold as they come on, for the answer: read under the
        // claim, before the window drops any of it, and inside the
        // rollback's reach, so a read that fails un-sticks the claim too.
        let held_now = crate::store::postgres::held_on(&state.pg_pool, id, keys).await.map_err(|e| ActivateWindowError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            msg: format!("held work: {e:#}"),
            rollback: ActivateRollback::Restore,
            database: crate::api::database_did_not_take(&e),
        })?;
        let urls = activate_trigger_setup_window(state, id, keys, activation, drops, project, program, rearm, task).await?;
        Ok((held_now, urls))
    }
    .await;
    let (held_now, urls) = match setup {
        Ok(setup) => setup,
        Err(ActivateWindowError { status, msg, rollback, database }) => {
            // Single rollback site for the whole window. The mode says
            // how much to undo: put the triggers back as the claim found
            // them (what was armed is already taken back), or turn them off
            // when an arm could not be taken back and nothing held would be
            // lost. Both are idempotent and safe if a concurrent
            // cancel/success already ended the activation. One carried on
            // from an earlier claim has no account of how the claim found
            // them, so it ends as an unfinished one does.
            let cause = weft_core::run::CancelCause::Runtime {
                detail: "the activation failed and rolled back its setup run".into(),
            };
            let restored = matches!(rollback, ActivateRollback::Restore) && previous.is_some();
            let rb = match (rollback, &previous) {
                (ActivateRollback::Restore, Some(previous)) => match state.activations.restore(id, activation, previous).await {
                    // The reserved run may not have started yet. Cancellation
                    // is harmless for an ownerless run and fences any born run.
                    Ok(true) => crate::api::run::cancel_run_id(state, activation, &cause)
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_run_id: {e}"))),
                    Ok(false) => Ok(()),
                    Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("restore the triggers: {e}"))),
                },
                (_, None) => end_unfinished_activation(state, id, activation, &cause).await,
                // The activation failed on its own; the setup run it
                // spawned is cancelled by that failure, not by anyone's
                // decision.
                (ActivateRollback::WipeSignals, Some(_)) => wipe_activating_state(state, id, activation, &cause).await,
            };
            let rolled_back = rb.is_ok();
            if let Err((rb_status, rb_msg)) = rb {
                tracing::error!(
                    target: "weft_dispatcher::activate",
                    project_id = %id, %activation, rb_status = %rb_status, rb_error = %rb_msg,
                    "activate rollback failed; the lapses loop ends the activation once this task has ended"
                );
            }
            // Broadcast whatever state the rollback landed (down on
            // success, or the still-stuck state the lapses loop repairs).
            crate::transition::publish_transition_changed(state, id).await;
            // Waiting on the database is the answer only when nothing of
            // the activation is left: the triggers are back as the claim
            // found them. Turned off, or a rollback that failed, changed
            // them, which a 503 would hide (`weft_core::install::asks_again`).
            return Err(match database && restored && rolled_back {
                true => ActivateError::Database(msg),
                false => ActivateError::Failed(status, msg),
            });
        }
    };
    // Past here the activations are Active (the window's final step
    // flipped them), so nothing fails the call.
    release_landed(state, id, keys, &urls).await;
    Ok(OwnerActivated { urls, held: held_now })
}

/// Finish `keys`, one owner's triggers the asking task turned on before it
/// was run again (`Unit::Landed`): what they still hold is released (it
/// was not, or not all of it, the first time), and their addresses said.
async fn land_turned_on(state: &DispatcherState, id: uuid::Uuid, keys: &[weft_core::activation::ActivationKey]) -> Result<OwnerActivated, ActivateError> {
    let held = crate::store::postgres::held_on(&state.pg_pool, id, keys).await.map_err(|e| ActivateError::failed("held work", e))?;
    let urls = collect_listener_urls(state, id, keys).await.map_err(|e| ActivateError::failed("collect_listener_urls", e))?;
    release_landed(state, id, keys, &urls).await;
    Ok(OwnerActivated { urls, held })
}

/// What follows `keys` landing on (Active): hand over every arrival they
/// held through their time down, now that the triggers take them, whatever
/// they waited on, and announce their addresses and the activation. Each
/// arrival is admitted
/// within half the trigger's own limits
/// (`weft_core::signal::ResolvedLimits::for_held`), so new arrivals get
/// through beside them; one that finds no room waits its turn in its
/// queue, which the sweep (`crate::parked_drain::drain_due`) is booked for.
/// Nothing here fails the activation, which has landed: a hand-over that
/// failed leaves what is left in their queues for the sweep's next look or
/// the next activation.
async fn release_landed(state: &DispatcherState, id: uuid::Uuid, keys: &[weft_core::activation::ActivationKey], urls: &[ActivationUrl]) {
    if let Err(e) = crate::parked_drain::drain_activations(state, id, keys).await {
        tracing::error!(
            target: "weft_dispatcher::activate",
            project_id = %id, triggers = ?keys, error = %format!("{e:#}"),
            "the triggers are active, but handing over the arrivals they held failed; \
             what is left waits in their queues for the sweep's next look or the next activation"
        );
    }
    for url in urls {
        state
            .events
            .publish(DispatcherEvent::TriggerUrlChanged {
                project_id: id,
                // User strings on a NOTIFY-path event: node ids are
                // user-authored and the url embeds the user's mount
                // path. Bound at construction.
                node_id: weft_core::truncate_user_string(&url.node_id, 4096),
                url: weft_core::truncate_user_string(&url.url, 4096),
            })
            .await;
    }
    state.events.publish(DispatcherEvent::ProjectActivated { project_id: id }).await;
}

/// The project's own address, as its front last answered it.
pub(crate) async fn project_address(state: &DispatcherState, id: uuid::Uuid) -> Option<weft_core::projects::ProjectAddress> {
    crate::front::address(state, id).await.unwrap_or_else(|e| {
        tracing::warn!(target: "weft_dispatcher::api::project", project_id = %id, error = %format!("{e:#}"), "could not read the project's address");
        None
    })
}

/// Sweep this owner's entry-trigger rows whose trigger no longer exists
/// in the source. Called after TriggerSetup so any trigger the user
/// removed while it was down has its leftover signal row dropped.
/// Resume rows (per-suspension) skip this: the corresponding suspended
/// run is the source of truth for them. None while a part that does not
/// compile may hide triggers (`ProjectDefinition::hides_nodes`): a row
/// missing from such a program may be a trigger inside it, and dropping
/// it would kill an address the person still has.
async fn orphan_entry_tokens(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    instance: Option<&weft_core::instance::InstanceId>,
) -> anyhow::Result<Vec<String>> {
    if project.hides_nodes() {
        return Ok(Vec::new());
    }
    let triggers: HashSet<String> = weft_core::project::trigger_places(project)
        .iter()
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    Ok(state
        .registrations
        .signal_list_for_project(project_id)
        .await?
        .into_iter()
        .filter(|s| !s.is_resume && s.instance.as_ref() == instance && !triggers.contains(&s.node_id))
        .map(|s| s.token)
        .collect())
}

/// Delete the entry rows [`orphan_entry_tokens`] named, while the
/// activation still owns its triggers, and have the listener forget them.
async fn drop_orphan_entry_rows(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    orphans: &[String],
    activation: uuid::Uuid,
) -> anyhow::Result<()> {
    let mut conn = state.pg_pool.get().await?;
    let tx = conn.transaction().await?;
    hold_activation(&tx, project_id, activation).await?;
    let removed = crate::store::postgres::remove_signals(&tx, orphans).await?;
    tx.commit().await?;
    state.listener.unregister_many(&removed).await;
    Ok(())
}

/// The failure of an activate window step from arming on: any step
/// before the one delete, and the flip to Active after it (the orphan
/// rows it deleted belong to triggers the source no longer has, so they
/// stay gone either way). Every trigger armed is taken back (a row it
/// created goes, a row it replaced is written back and the listener runs
/// it again) and the triggers are put back as they were. When one cannot
/// be put back, `if_not_put_back` says what becomes of them (see the
/// window). `what` is the step that failed, with `error`. Held work the
/// window dropped (`dropped`) stays dropped, so the failure is then never
/// one nothing of the activation ran.
async fn take_back_arms(
    state: &DispatcherState,
    if_not_put_back: ActivateRollback,
    dropped: bool,
    armed: Vec<crate::task_kinds::register_signal::Armed>,
    what: String,
    error: anyhow::Error,
) -> ActivateWindowError {
    let status = StatusCode::INTERNAL_SERVER_ERROR;
    // An arm whose lease the database could not renew in time stopped
    // before its row was written (past that, its bring-up finishes
    // whatever the lease does), so it is a wait on the database too.
    let database = (crate::api::database_did_not_take(&error) || weft_task_store::locks::lease_lost(&error)) && !dropped;
    let msg = format!("{what}: {error:#}");
    match crate::task_kinds::register_signal::disarm(state.registrations.as_ref(), &state.listener, armed).await {
        Ok(()) => ActivateWindowError { status, msg, rollback: ActivateRollback::Restore, database },
        // What could not be put back stays changed: never the database's
        // to wait out.
        Err(undo) => {
            let fate = match if_not_put_back {
                ActivateRollback::Restore => "so the triggers stay down as they were, with what they kept",
                ActivateRollback::WipeSignals => "so the triggers are turned off",
            };
            ActivateWindowError {
                status,
                msg: format!("{msg}; putting back the triggers armed before it failed, {fate}: {undo:#}"),
                rollback: if_not_put_back,
                database: false,
            }
        }
    }
}

/// Hold the activation identity in `tx` while it changes the activation's
/// persistent signal state: the rows it claimed stay Activating under it
/// for the whole transaction, so a concurrent cancel waits behind this
/// write.
async fn hold_activation(
    tx: &weft_task_store::db::Transaction<'_>,
    project_id: uuid::Uuid,
    activation: uuid::Uuid,
) -> anyhow::Result<()> {
    let owned: Vec<String> = tx
        .scalars(
            "SELECT trigger FROM trigger_activation \
             WHERE project_id = $1 AND status = 'activating' AND activating_run_id = $2 FOR UPDATE",
            &[&project_id, &activation],
        )
        .await?;
    anyhow::ensure!(!owned.is_empty(), "activation {activation} no longer owns any trigger of project {project_id}");
    Ok(())
}

/// Drop what the activations `keys` held while they were down, when an
/// activate is asked to (`weft activate --held drop`, `drops`): their held
/// arrivals, in a write under the activation's claim, and the runs their
/// triggers fired that are paused, cancelled. Answers whether they still
/// govern signals (an entry row of a trigger that was down, a wait of a
/// run its trigger fired), which a failed activation must never cost.
async fn drop_held(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    drops: bool,
) -> Result<bool, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    if drops {
        let conflict = |error: String| (StatusCode::CONFLICT, error);
        let mut conn = state.pg_pool.get().await.map_err(|e| conflict(e.to_string()))?;
        let tx = conn.transaction().await.map_err(|e| conflict(e.to_string()))?;
        hold_activation(&tx, project_id, activation).await.map_err(|e| conflict(e.to_string()))?;
        crate::store::postgres::drop_held_arrivals(&tx, project_id, keys).await.map_err(|e| internal("drop the held arrivals", e))?;
        tx.commit().await.map_err(|e| internal("drop the held arrivals", e.into()))?;
        // A run started by hand and another trigger's runs are not this
        // activation's to touch.
        let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
        let runs = crate::take_down::live_runs(state, project_id).await.map_err(|e| internal("live runs", e))?;
        let cause = weft_core::run::CancelCause::User;
        let paused: Vec<(weft_core::RunId, &weft_core::run::CancelCause)> = crate::take_down::affected_runs(&target, &runs, None)
            .into_iter()
            .filter(|run| run.suspended)
            .map(|run| (run.run_id, &cause))
            .collect();
        crate::api::run::cancel_run_ids(state, &paused).await.map_err(|e| internal("cancel the paused runs", e))?;
    }
    Ok(!crate::store::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await
        .map_err(|e| internal("activation signals", e))?
        .is_empty())
}

/// End the named activation, its door refusing, and delete its triggers'
/// entry rows atomically (the waits of runs those triggers fired stay
/// with their runs; an instance's rows go). Listener cleanup receives only
/// those deleted rows, so a newer activation's tokens survive. The caller
/// supplies the cause of the setup run's cancellation.
pub(crate) async fn wipe_activating_state(
    state: &DispatcherState,
    id: uuid::Uuid,
    activation: uuid::Uuid,
    cause: &weft_core::run::CancelCause,
) -> Result<(), (StatusCode, String)> {
    let Some(removed) = state.activations.end_activating(
        id, activation, &crate::activation_store::ActivationLifecycle::refused(), true, None,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation: {e}")))? else {
        return Ok(());
    };
    state.listener.unregister_many(&removed).await;
    crate::api::run::cancel_run_id(state, activation, cause)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation {id}: {e}")))?;
    Ok(())
}

/// End an activation nobody will finish (a person pressed Cancel, its task
/// ended without landing it: `crate::lapses`, or a carried-on activation
/// failed), its setup run cancelled for `cause`: its
/// triggers land down with their door taking and what they held kept, or
/// refusing when they held nothing (`ActivationStoreOps::end_unfinished`).
/// An activation already ended (its driver finished, or another cancel)
/// is left as it is.
pub(crate) async fn end_unfinished_activation(
    state: &DispatcherState,
    id: uuid::Uuid,
    activation: uuid::Uuid,
    cause: &weft_core::run::CancelCause,
) -> Result<(), (StatusCode, String)> {
    let Some(removed) = state
        .activations
        .end_unfinished(id, activation)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("end the activation: {e:#}")))?
    else {
        return Ok(());
    };
    state.listener.unregister_many(&removed).await;
    crate::api::run::cancel_run_id(state, activation, cause)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation {id}: {e:#}")))
}

/// The instances of project `id` with at least one trigger on, in id
/// order: what a deactivate of the shared triggers reports as still on.
pub(crate) async fn instances_with_triggers_on(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::instance::InstanceId>, (StatusCode, String)> {
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let live = rows.iter().filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active).map(|a| &a.key);
    Ok(weft_core::activation::owners_of(live).into_iter().filter_map(|owner| owner.instance().cloned()).collect())
}

/// Take `keys` down as `take_down` says, as `asker` asked, marked by
/// `cause` when what takes them down is not a person (an infra verb over
/// infra in their cut, whose commands it issues with them; the health
/// loop). What it did, for the command's report.
pub(crate) async fn take_down_triggers(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: Vec<weft_core::activation::ActivationKey>,
    take_down: &weft_core::take_down::TakeDown,
    cause: crate::take_down::DownCause<'_>,
    asker: crate::take_down::TakeDownAsker,
) -> Result<crate::take_down::Taken, (StatusCode, String)> {
    let target = crate::take_down::TakeDownTarget::Activations(keys);
    match crate::take_down::take_down(state, id, &target, take_down, cause, asker).await? {
        crate::take_down::TakenDown::Done(taken) => Ok(taken),
        crate::take_down::TakenDown::ProjectGone => Err((StatusCode::NOT_FOUND, "the project was removed, or is being removed, while its triggers were taken down".into())),
        crate::take_down::TakenDown::ResyncGone => {
            Err((StatusCode::CONFLICT, "the resync taking these triggers down was given up, or taken up again, meanwhile".into()))
        }
    }
}

/// `POST /projects/{id}/deactivate` (`weft deactivate`): see [`trigger_actions::deactivate`].
pub async fn deactivate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<DeactivateRequest>,
) -> Result<Json<DeactivateResponse>, StatusError> {
    let deactivated = trigger_actions::deactivate(&state, &operator(caller), id, &body, weft_core::StopSelf::Keep).await?;
    Ok(Json(deactivated.answer))
}

/// Take down the activations `scope` admits, as `take_down` says, for
/// `asker`: what a person's `weft deactivate` and a program's
/// `ctx.triggers().deactivate(..)` both do. Picked from the activation rows
/// ([`keys_to_take_down`]): a trigger already down is taken down again
/// with these answers (its door becomes this one), and one with no row is
/// nothing to take down. The runs going are left to finish on their own
/// version under `running: wait`: a person's deactivate waits for them
/// apart ([`deactivate`]); a program's never does, since a run waiting
/// inside its own call on the runs of its trigger would wait on its
/// siblings doing the same, for good. `None` when the project is gone.
pub(crate) async fn deactivate_scope(
    state: &DispatcherState,
    id: uuid::Uuid,
    scope: &weft_core::activation::ActivationScope,
    take_down: &weft_core::take_down::TakeDown,
    asker: crate::take_down::TakeDownAsker,
) -> Result<Option<crate::take_down::Taken>, (StatusCode, String)> {
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let keys = keys_to_take_down(&rows, scope)?;
    match crate::take_down::take_down(state, id, &crate::take_down::TakeDownTarget::Activations(keys), take_down, crate::take_down::DownCause::Person, asker).await? {
        crate::take_down::TakenDown::Done(taken) => Ok(Some(taken)),
        crate::take_down::TakenDown::ProjectGone => Ok(None),
        crate::take_down::TakenDown::ResyncGone => unreachable!("a deactivate is no resync's"),
    }
}

/// `POST /projects/{id}/resync` (`weft resync`): see [`trigger_actions::resync`].
pub async fn resync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<ResyncRequest>>,
) -> Result<Json<ResyncAnswer>, Refused> {
    let body = body.map(|Json(b)| b).unwrap_or_default();
    Ok(Json(trigger_actions::resync(&state, &operator(caller), id, &body).await?))
}



/// `POST /projects/{id}/resync/resume` (`weft resync --resume`): see
/// [`trigger_actions::resume_resync`].
pub async fn resume_resync(State(state): State<DispatcherState>, caller: CallerTenant, Path(id): Path<uuid::Uuid>) -> Result<Json<ResyncAnswer>, Refused> {
    Ok(Json(trigger_actions::resume_resync(&state, &operator(caller), id).await?))
}

/// `GET /projects/{id}/follow/{task}?wait_ms=<n>`: see [`project_actions::follow`].
pub async fn follow(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, task)): Path<(uuid::Uuid, uuid::Uuid)>,
    axum::extract::Query(hold): axum::extract::Query<super::HoldQuery>,
) -> Result<Json<weft_core::deactivation::Following>, StatusError> {
    Ok(Json(project_actions::follow(&state, &operator(caller), id, task, hold.hold()).await?))
}


/// The first half of owner `at`'s resync, for its task
/// (`crate::task_kinds::resync`): write down which of the owner's
/// triggers that were behind when the resync began are still on, and the
/// builds they fire, take those down with the plan's door and held
/// answers, and move the runs they fired that are paused onto the new
/// build (when what is held is kept). The runs going are the task's to
/// wait for or cancel once every owner is down
/// (`crate::task_kinds::resync::let_the_old_version_finish`). Run again
/// over an owner it got part way through (its task was run again), it
/// takes down only what is still on.
pub(crate) async fn resync_take_down_owner(
    state: &DispatcherState,
    id: uuid::Uuid,
    plan: &mut crate::task_kinds::resync::ResyncPlan,
    at: usize,
) -> Result<(), crate::task_kinds::resync::Halt> {
    use crate::activation_store::ProjectStatus;
    use crate::task_kinds::resync::{failing, Halt, ResyncWait};
    let owner = plan.owners[at].clone();
    let rows = state.activations.list(id).await.map_err(failing("activations"))?;
    let status_of = |trigger: &String| {
        let key = weft_core::activation::ActivationKey::new(trigger.clone(), owner.owner.clone());
        rows.iter().find(|a| a.key == key).map(|a| a.lifecycle.status)
    };
    if owner.on.iter().any(|trigger| status_of(trigger) == Some(ProjectStatus::Activating)) {
        return Err(Halt::waiting(
            ResyncWait::Activation,
            (StatusCode::CONFLICT, "an activation of these triggers is still going; the resync brings them back once it has landed".into()),
        ));
    }
    // The ones still on and behind the build: a run of the task again
    // finds the ones an earlier run took down off already. Written down,
    // with the builds they fire and their entry tokens, before they come
    // down, so a run of the task again brings back exactly these, never
    // one that was off, and waits for exactly their runs.
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(failing("project"))?
        .ok_or_else(|| Halt::from((StatusCode::NOT_FOUND, "project not found".to_string())))?;
    // Against the registered build, its own hashes beside its own cuts:
    // what the setup brings them to (the request's build is checked
    // against it as they come back on).
    let binary_hash = state.projects.running_binary_hash(id).await.map_err(failing("running hashes"))?;
    let definition_hash = state.projects.running_definition_hash(id).await.map_err(failing("running hashes"))?;
    let desired = Desired::registered(state, id, &registered, binary_hash.as_deref(), definition_hash.as_deref()).await.map_err(Halt::from)?;
    let still_on: Vec<&crate::activation_store::Activation> = owner
        .on
        .iter()
        .filter_map(|trigger| {
            let key = weft_core::activation::ActivationKey::new(trigger.clone(), owner.owner.clone());
            rows.iter().find(|a| a.key == key && desired.behind(a))
        })
        .collect();
    let newly: Vec<String> = still_on.iter().map(|a| a.key.trigger.clone()).filter(|trigger| !owner.down.contains(trigger)).collect();
    let fired_builds: Vec<String> = still_on
        .iter()
        .filter_map(|a| a.program.as_ref().map(|program| program.binary_hash.clone()))
        .filter(|build| Some(build) != binary_hash.as_ref() && !plan.old_builds.contains(build))
        .collect();
    let still_on_keys: Vec<weft_core::activation::ActivationKey> = still_on.iter().map(|a| a.key.clone()).collect();
    let tokens: Vec<String> = crate::store::postgres::entry_tokens(&state.pg_pool, id, &still_on_keys)
        .await
        .map_err(failing("the triggers' entry tokens"))?
        .into_iter()
        .filter(|token| !plan.old_tokens.contains(token))
        .collect();
    if !newly.is_empty() || !fired_builds.is_empty() || !tokens.is_empty() {
        plan.owners[at].down.extend(newly);
        plan.old_builds.extend(fired_builds);
        plan.old_builds.sort();
        plan.old_builds.dedup();
        plan.old_tokens.extend(tokens);
        if !state.projects.write_resync(id, plan).await.map_err(failing("write down the triggers it takes down"))? {
            return Err(Halt::Given);
        }
    }
    let owner = plan.owners[at].clone();
    // The door and what is held only: the runs going are waited for or
    // cancelled once every owner is down, the old version's all at once.
    let take_down = weft_core::take_down::TakeDown { running: weft_core::take_down::Running::Wait, ..plan.take_down };
    // The take-down, marked with no cause: triggers a stopped resync
    // leaves down are never brought back by the infra in their cut coming
    // up. It goes through only while this drive holds the project, decided
    // in the same transaction as the lifecycle write: a person who gave the
    // resync up meanwhile finds every trigger it had not taken down still
    // on, and is told exactly what stays down.
    let still_on: Vec<String> = still_on.iter().map(|a| a.key.trigger.clone()).collect();
    if !still_on.is_empty() {
        let target = crate::take_down::TakeDownTarget::Activations(owner.keys(&still_on));
        let asker = crate::take_down::TakeDownAsker::Resync(plan.driver);
        match crate::take_down::take_down(state, id, &target, &take_down, crate::take_down::DownCause::Person, asker).await? {
            crate::take_down::TakenDown::Done(taken) => {
                plan.held += taken.held;
                if !state.projects.write_resync(id, plan).await.map_err(failing("write down what the take-down did"))? {
                    return Err(Halt::Given);
                }
            }
            crate::take_down::TakenDown::ResyncGone => return Err(Halt::Given),
            crate::take_down::TakenDown::ProjectGone => {
                return Err(Halt::from((StatusCode::NOT_FOUND, "the project was removed during its resync".to_string())));
            }
        }
    }
    // What they held stays theirs: the runs they fired that are paused
    // resume on the new build, each on its trigger's cut of it. A run whose
    // record does not fit that cut fails, saying so, when it resumes.
    if take_down.held == weft_core::take_down::Held::Keep && !owner.down.is_empty() {
        plan.moved_runs +=
            move_paused_runs(state, id, &registered, &owner.keys(&owner.down)).await.map_err(failing("move the paused runs onto the new build"))?;
        if !state.projects.write_resync(id, plan).await.map_err(failing("write down the runs moved"))? {
            return Err(Halt::Given);
        }
    }
    Ok(())
}

/// The second half of owner `at`'s resync, once every owner is down and
/// the old version has nothing left running: turn back on exactly the
/// triggers it took down, on the plan's build, releasing what they held
/// meanwhile. Answers the addresses the reactivation answers on.
pub(crate) async fn resync_bring_back_owner(
    state: &DispatcherState,
    id: uuid::Uuid,
    plan: &crate::task_kinds::resync::ResyncPlan,
    at: usize,
) -> Result<Vec<weft_core::activation::ActivationUrl>, crate::task_kinds::resync::Halt> {
    use crate::activation_store::ClaimRefused;
    use crate::task_kinds::resync::{Halt, ResyncWait};
    let owner = &plan.owners[at];
    if owner.down.is_empty() {
        return Ok(Vec::new());
    }
    let registered = state
        .projects
        .project(id)
        .await
        .map_err(crate::task_kinds::resync::failing("project"))?
        .ok_or_else(|| Halt::from((StatusCode::NOT_FOUND, "project not found".to_string())))?;
    let down = owner.keys(&owner.down);
    // The infra their setup reads, checked apart so a stop over it waits
    // for that infra to run again ([`ResyncWait::Infra`]).
    require_trigger_infra(state, id, &registered, &down).await.map_err(|refused| Halt::waiting(ResyncWait::Infra, refused))?;
    // A build that cannot run them fails the same way however often it is
    // tried, and a resync taken up again keeps its build: stopped at once,
    // saying what does bring them back. A check that could not be made
    // fails as any step does, and is tried again ([`Halt`]'s `From<Unrunnable>`).
    let (_, runnable) = coherent_definition(state, id).await.map_err(Halt::from)?;
    refuse_unrunnable(state, id, &runnable, &down).await?;
    // Reactivate, through the same path as an activate (hash
    // persistence, cleanup on failure), past the activate handler's gates:
    // the resync checked them before its take-down, and its claim is the
    // one that goes through while it holds the project, only while this
    // drive holds it (a person who gave it up meanwhile leaves the
    // triggers down). What the take-down kept is released as it comes back
    // on. The port stays the one the project has (a resync names none).
    // Triggers already back (an earlier run of the task brought them back,
    // and stopped before writing so) are not off, and are left as they are.
    let request = ActivateRequest { build: plan.build.clone(), scope: owner.scope(&owner.down), held: ReleaseHeld::Run, port: None };
    match activate_with(state, id, request, ActivateAsker::Resync(plan.driver)).await {
        Ok(Json(activated)) => Ok(activated.urls),
        Err(ActivateError::Refused(ClaimRefused::ResyncGone)) => Err(Halt::Given),
        Err(ActivateError::Refused(ClaimRefused::Removing)) => Err(Halt::Removing),
        Err(ActivateError::Database(error)) => Err(Halt::on_the_database(error)),
        Err(error) => Err(<(StatusCode, String)>::from(error).into()),
    }
}

/// Move the runs `keys`' triggers fired that are paused onto the
/// registered build `registered`: each now runs its trigger's cut of it,
/// on its workers, from its own record. Answers how many moved.
async fn move_paused_runs(
    state: &DispatcherState,
    id: uuid::Uuid,
    registered: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<u64> {
    let identity = state
        .projects
        .running_program_identity(id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("project {id} has no registered build"))?;
    let source_version = crate::actions::versions::record_program_source(state, id, &identity).await.map_err(|(_, e)| anyhow::anyhow!(e))?;
    let runs = crate::take_down::live_runs(state, id).await?;
    let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
    let mut moved = 0;
    for key in keys {
        let paused: Vec<weft_core::RunId> = crate::take_down::affected_runs(&target, &runs, None)
            .into_iter()
            .filter(|run| run.suspended && run.fired_by(key))
            .map(|run| run.run_id)
            .collect();
        if paused.is_empty() {
            continue;
        }
        let saved = saved_for_run(state, id, registered, key.instance()).await.map_err(|(_, e)| anyhow::anyhow!(e))?;
        let fire = weft_core::run_spec::compute_trigger_fire(registered, &key.trigger, &serde_json::Value::Null, None, &saved)
            .map_err(|why| anyhow::anyhow!("the cut of trigger {key}: {why}"))?;
        let program = weft_journal::record::RunsProgram::of(&identity.definition_hash, id, &fire.program)?;
        moved += crate::store::postgres::move_paused_runs(&state.pg_pool, &paused, &program, &identity.binary_hash, &source_version).await?;
    }
    Ok(moved)
}

/// Tell the platform to have the workers of the image `binary_hash`
/// ready (`Runner::prepare`). Workers of other images are left as they
/// are: what runs there finishes there, and they stop once idle.
async fn prepare_running_image(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    binary_hash: &str,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("prepare the workers: {e:#}"));
    let tenant = state.tenant_router.tenant_for_project(project_id).await.map_err(internal)?;
    let target = crate::delivery::worker_target(state, tenant.as_str(), project_id, binary_hash).await.map_err(internal)?;
    state.runner.prepare(&target).await.map_err(internal)
}


/// Wait until every last call the project's cancelled runs queued
/// (`weft_task_store::TaskKind::CancelCall`) has gone out, or failed (its
/// task logs why): what `weft rm` deletes next includes what a call needs
/// to go out (an instance's connections, the project's infra addresses).
/// No deadline: each call is bounded where it is sent
/// (`weft_listener::kinds::cancel_call`), and how long the queue takes to
/// reach it is not this call's to cut short.
pub(crate) async fn last_calls_sent(state: &DispatcherState, id: uuid::Uuid) -> anyhow::Result<()> {
    let calls: Vec<uuid::Uuid> = state
        .pg_pool
        .scalars(
            "SELECT id FROM task WHERE project_id = $1 AND kind = $2 AND status IN ('pending', 'claimed')",
            &[&id, &weft_task_store::TaskKind::CancelCall.as_str()],
        )
        .await
        .map_err(|e| anyhow::anyhow!("could not read the project's last calls: {e}"))?;
    for call in calls {
        weft_task_store::terminal::wait_for_terminal_in_project(&state.pg_pool, &state.signals, call, id, std::time::Duration::MAX)
            .await
            .map_err(|e| anyhow::anyhow!("could not wait for a last call of a cancelled run: {e:#}"))?;
    }
    Ok(())
}

/// `POST /projects/{id}/quiesce` (`weft rm`'s first step): see [`project_actions::quiesce`].
pub async fn quiesce(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<weft_core::projects::QuiesceRequest>,
) -> Result<Json<weft_core::projects::Quiesced>, StatusError> {
    Ok(Json(project_actions::quiesce(&state, &operator(caller), id, &body).await?))
}

/// `POST /projects/{id}/cancel-build` (`weft cancel-build`): see
/// [`project_actions::cancel_build`]. Answers 202: what the build shows
/// next is whatever the install reports.
pub async fn cancel_build(State(state): State<DispatcherState>, caller: CallerTenant, Path(id): Path<uuid::Uuid>) -> Result<StatusCode, Refused> {
    project_actions::cancel_build(&state, &operator(caller), id).await?;
    Ok(StatusCode::ACCEPTED)
}

/// `POST /projects/{id}/cancel-activate` (`weft cancel-activate`): see
/// [`trigger_actions::cancel_activate`].
pub async fn cancel_activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<weft_core::activation::ActivationScope>>,
) -> Result<Json<weft_core::activation::CancelActivateResponse>, Refused> {
    let scope = body.map(|Json(s)| s).unwrap_or_default();
    Ok(Json(trigger_actions::cancel_activate(&state, &operator(caller), id, &scope).await?))
}

/// Spawn a worker for the TriggerSetup sub-run and block
/// until it settles. The run is recorded on the project row
/// before it starts, so a rollback (or a cancel-activate, or the
/// reaper) cancels just THIS run and never touches
/// suspended/running work from prior cycles.
///
/// For an activation (`activation`: its setup run, and the claim of the
/// task driving it), a setup run born already (the task was run again) is
/// waited for, never started twice. While the run goes, from the moment a
/// worker took it until its ending, the task's claim rests on it: the run
/// may wait days on a person, and is judged meanwhile by its own worker's
/// lease and its ending (`crate::lapses`), so the claim is not renewed for
/// nothing; it is taken back at the ending, or handed back by the
/// `lapses` loop if nobody took it back.
pub(crate) async fn run_trigger_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    // The activations whose triggers this setup captures; they share one
    // owner, whose run it is (an instance's triggers read that instance's
    // values).
    keys: &[weft_core::activation::ActivationKey],
    // The hash of the SAME definition the caller resolved `keys`
    // against (one `coherent_definition` pair). Reading the project
    // row's hash here instead would race a concurrent re-register:
    // kicks from shape A journaled under hash B.
    program: &weft_core::project::hash::ProgramIdentity,
    activation: Option<(uuid::Uuid, &weft_task_store::executor::TaskClaim)>,
    // An instance's change about to be stored, which this setup runs with in
    // place of what is stored (a store re-arming the triggers that read
    // it); nothing for every other setup.
    overlay: Option<&weft_core::instance::ValueChanges>,
) -> Result<crate::store::TriggerBake, (StatusCode, String)> {
    let claim = activation.map(|(_, claim)| claim);
    let activation = activation.map(|(run, _)| run);
    let run_id = activation.unwrap_or_else(weft_core::new_run_id);
    let places = key_places(project, keys);
    let (cut, kicks) = setup_cut(project, &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "a trigger setup was asked for with no trigger to set up".to_string()))?;
    let run_program = runs_program(program, &cut)?;
    let instance = keys_instance(keys);
    // An instance's triggers are set up with what that instance provides.
    let instance_values = match instance {
        Some(instance) => instance_values_for_run(state, project_id, &cut, instance, overlay).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, &cut).await?;

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to
    // the completion event.
    let mut events = state.events.subscribe_project(project_id).await;

    // A setup run this activation started before its task was run again
    // is the one it waits for: started twice, it would be refused, and the
    // first one may be waiting on a person.
    let born = match activation {
        Some(run) => weft_task_store::runs::born(&state.pg_pool, run)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read whether the setup run started: {e:#}")))?,
        None => false,
    };
    if !born {
        start_trigger_setup(state, project_id, project, program, activation, run_id, &cut, &kicks, &run_program, instance, &instance_values, &picks).await?;
    }

    // No backend-imposed deadline. Trigger setup spans worker
    // spawn + image pull + fold + run + bridge wakeup; on a cold
    // install the legitimate path is just slow, and a hard timeout
    // would surface that as a 504 even though the work is still in
    // flight. The CLI / extension is the right layer to choose a
    // client-side patience budget.
    let mut resting = SetupRest { claim, run: run_id, resting: false };
    let ended = wait_for_setup(state, &mut events, run_id, &mut resting).await;
    resting.take_back(state).await?;
    ended?;
    // A trigger setup's record is a few rows, read whole.
    let events: Vec<weft_journal::RunEvent> = state.runs.events_log(run_id).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read trigger setup: {error:#}")))?
        .into_iter().map(|row| row.event).collect();
    let program = state.run_program_identity(&events).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")))?;
    let mut bake = crate::store::TriggerBake::from_events(&events, &program)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| (StatusCode::CONFLICT, "trigger setup did not complete successfully".into()))?;
    bake.targets = keys.iter().map(|key| key.trigger.clone()).collect();
    crate::run_ends::land_trigger_bake(state, run_id, project_id, Some(&bake)).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("publish trigger bake: {error:#}")))?;
    Ok(bake)
}

/// Start setup run `run_id` (an activation's own, `activation`, or one of
/// its own for a bake), queued for a worker. An activation's run is born
/// only while that activation still owns its rows, and one born as a
/// cancel ended it is cancelled at once.
#[allow(clippy::too_many_arguments)]
async fn start_trigger_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    program: &weft_core::project::hash::ProgramIdentity,
    activation: Option<uuid::Uuid>,
    run_id: uuid::Uuid,
    cut: &ProjectDefinition,
    kicks: &[Kick],
    run_program: &weft_journal::record::RunsProgram,
    instance: Option<&weft_core::instance::InstanceId>,
    instance_values: &weft_core::instance::InstanceValues,
    picks: &weft_core::picks::Picks,
) -> Result<(), (StatusCode, String)> {
    // The activation reserved this run when it began. A cancelled
    // driver cannot replace a newer activation's identity.
    let owned = |activations: Vec<crate::activation_store::Activation>| {
        activations.iter().any(|a| {
            a.lifecycle.status == crate::activation_store::ProjectStatus::Activating
                && a.lifecycle.activating_run_id == Some(run_id)
        })
    };
    let recorded = activation.is_none() || owned(
        state
            .activations
            .list(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read activation: {e}")))?,
    );
    if !recorded {
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled before trigger setup started".into(),
        ));
    }

    // The birth is one transaction, so a start failure leaves no journal
    // rows and no task; the run the row recorded above then names a
    // run that never existed, which the rollback's cancel finds ownerless
    // and skips.
    let entry_node = setup_entry(project, cut, kicks);
    start_queued_run(
        state,
        program,
        &project.defaults,
        weft_journal::birth::Birth {
            run_id,
            project_id,
            phase: weft_core::context::Phase::TriggerSetup,
            entry_node: &entry_node,
            kicks,
            program: run_program,
            // A setup is handed nothing and stands nothing in.
            given: &Default::default(),
            binary_hash: &program.binary_hash,
            seed: None,
            source_version: None,
            instance: instance.map(|instance| weft_journal::birth::RunFor { instance, values: instance_values }),
            picks,
            fired_trigger: None,
            stand_in: None,
            settings: weft_core::run_settings::RunSettings::bookkeeping(),
            at_unix: crate::lease::now_unix() as u64,
        },
        QueueAs { for_activation: activation.is_some(), watch_end: true, ..QueueAs::default() },
    )
    .await?;

    // Cancellation may happen immediately after the atomic birth commits.
    // Recheck ownership so this driver also cancels a run born during that race.
    let still_ours = activation.is_none() || owned(
        state
            .activations
            .list(project_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?,
    );
    if !still_ours {
        crate::api::run::cancel_run_id(
            state,
            run_id,
            &weft_core::run::CancelCause::Runtime {
                detail: "the activation ended before this trigger setup run started".into(),
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_run_id: {e}")))?;
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled as trigger setup started".into(),
        ));
    }
    Ok(())
}

/// Where the claim of the task driving an activation stands while it waits
/// on the setup run `run` ([`run_trigger_setup`]): resting on it while a
/// worker drives it, or renewed. `claim` is `None` for a setup no task
/// drives as an activation (a bake), which rests nothing.
struct SetupRest<'a> {
    claim: Option<&'a weft_task_store::executor::TaskClaim>,
    run: uuid::Uuid,
    resting: bool,
}

impl SetupRest<'_> {
    /// Rest the claim on the run, once a worker drives it.
    async fn rest(&mut self) {
        let Some(claim) = self.claim.filter(|_| !self.resting) else { return };
        match claim.rest_on(self.run).await {
            Ok(()) => self.resting = true,
            Err(e) => {
                tracing::warn!(target: "weft_dispatcher::activate", run = %self.run, error = %format!("{e:#}"), "could not rest the task's claim on its setup run; it stays renewed");
            }
        }
    }

    /// Take the claim back from the run, if it rests on it: the run ended,
    /// or the wait did. A claim handed back meanwhile is another
    /// claimant's now, and the task stops here (its lease is lost). The
    /// `lapses` loop looks when the claim taken back would lapse
    /// (`crate::lapses::earliest_lapse`).
    async fn take_back(&mut self, state: &DispatcherState) -> Result<(), (StatusCode, String)> {
        let Some(claim) = self.claim.filter(|_| self.resting) else { return Ok(()) };
        self.resting = false;
        claim
            .resume()
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("take the task's claim back from its setup run: {e:#}")))?;
        crate::lapses::book_after_or_log(state, weft_task_store::tasks::claim_duration_secs()).await;
        Ok(())
    }
}

/// Wait for setup run `run_id` to end, on `events` (subscribed before the
/// run could end), answering its failure or its cancel as an error. A run
/// born before the wait (its task was run again) may be driven, or over,
/// already: its row says so first. The claim rests on the run from the
/// moment a worker drives it (`rest`).
async fn wait_for_setup(
    state: &DispatcherState,
    events: &mut crate::events::Follower,
    run_id: uuid::Uuid,
    rest: &mut SetupRest<'_>,
) -> Result<(), (StatusCode, String)> {
    let ended = |outcome: crate::api::run::TerminalOutcome, why: Option<String>| match outcome {
        crate::api::run::TerminalOutcome::Completed => Ok(()),
        crate::api::run::TerminalOutcome::Failed => {
            Err((StatusCode::INTERNAL_SERVER_ERROR, why.map_or("trigger setup failed".into(), |error| format!("trigger setup failed: {error}"))))
        }
        crate::api::run::TerminalOutcome::Cancelled => {
            Err((StatusCode::INTERNAL_SERVER_ERROR, why.map_or("trigger setup cancelled".into(), |reason| format!("trigger setup cancelled: {reason}"))))
        }
    };
    // The journal is authoritative: what the row says is read whenever
    // what was heard may have missed it (before the first event, and after
    // a gap).
    let look = |rest: &mut SetupRest<'_>| {
        let pool = state.pg_pool.clone();
        let resting = rest.resting;
        async move {
            let outcome = crate::api::run::terminal_outcome(&pool, run_id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("trigger setup terminal lookup: {e}")))?;
            let driven = match (outcome, resting) {
                (None, false) => weft_task_store::runs::in_flight(&pool, run_id)
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("trigger setup lookup: {e:#}")))?,
                _ => false,
            };
            Ok::<_, (StatusCode, String)>((outcome, driven))
        }
    };
    let (outcome, driven) = look(rest).await?;
    if let Some(outcome) = outcome {
        return ended(outcome, None);
    }
    if driven {
        rest.rest().await;
    }
    loop {
        match events.recv().await.map(|record| record.event) {
            Ok(crate::events::DispatcherEvent::RunCompleted { run_id: c, .. }) if c == run_id => return Ok(()),
            Ok(crate::events::DispatcherEvent::RunFailed { run_id: c, error, .. }) if c == run_id => {
                return ended(crate::api::run::TerminalOutcome::Failed, Some(error));
            }
            Ok(crate::events::DispatcherEvent::RunCancelled { run_id: c, reason, .. }) if c == run_id => {
                return ended(crate::api::run::TerminalOutcome::Cancelled, Some(reason));
            }
            // Its first event after its birth: a worker took it.
            Ok(event) if event.run_id() == Some(run_id) && !matches!(event, crate::events::DispatcherEvent::RunStarted { .. }) => {
                rest.rest().await;
            }
            Ok(_) => continue,
            // Dropped batch may have held this run's terminal.
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                let (outcome, driven) = look(rest).await?;
                if let Some(outcome) = outcome {
                    return ended(outcome, None);
                }
                if driven {
                    rest.rest().await;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "trigger setup event stream closed".into()));
            }
        }
    }
}

/// After a trigger-setup sub-run, collect every persisted signal
/// for the project that has a user-facing URL. These become the
/// `urls` in ActivateResponse.
async fn collect_listener_urls(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<ActivationUrl>> {
    let signals = crate::store::postgres::activation_signals(&state.pg_pool, project_id, keys).await?;
    // Read only once a signal has an outside address, so a project
    // with none (cron only) activates while the tunnel is down.
    let base = state.external_base();
    let mut out = Vec::new();
    for meta in signals {
        if meta.is_resume {
            continue;
        }
        if let Some(path) = meta.public_path() {
            out.push(ActivationUrl {
                node_id: meta.node_id.clone(),
                url: base.url(&path).await?,
            });
        }
    }
    Ok(out)
}


// `crate::lease::now_unix` is the canonical wall-clock reader.

#[cfg(test)]
mod trigger_kick_tests {
    use serde_json::Value;
    use weft_core::run_spec::{compute_trigger_fire, TriggerFire};
    use super::*;
    use weft_core::project::{infra_places, trigger_places};

    /// Build a minimal ProjectDefinition from a JSON spec. Tests
    /// only care about node id, trigger status, the scope a node sits
    /// in, and edges; everything else is defaulted. The third element
    /// is the node's scope chain (empty at the top level). Every edge
    /// is `v -> v`: these tests are about which nodes a kick reaches,
    /// not which port feeds which.
    fn project(nodes: &[(&str, bool, &[&str])], edges: &[(&str, &str)]) -> ProjectDefinition {
        let wires: Vec<Value> = edges.iter().map(|(s, t)| wire(s, t, &[])).collect();
        project_with(nodes, wires, Vec::new())
    }

    /// A wire `source.v -> target.v` through the group ports `via`, each
    /// a group, its port and the side it is crossed on.
    fn wire(source: &str, target: &str, via: &[(&str, &str, &str)]) -> Value {
        serde_json::json!({
            "id": format!("e_{source}_{target}"),
            "source": source,
            "sourceHandle": "v",
            "target": target,
            "targetHandle": if via.last().is_some_and(|(group, _, _)| *group == target) { via.last().unwrap().1 } else { "v" },
            "via": via.iter().map(|(group, port, side)| serde_json::json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>(),
        })
    }

    fn project_with(nodes: &[(&str, bool, &[&str])], edges: Vec<Value>, groups: Vec<Value>) -> ProjectDefinition {
        let n_json: Vec<Value> = nodes.iter().map(|(id, is_trigger, scope)| serde_json::json!({
            "id": id,
            "nodeType": "T",
            "label": null,
            "config": {},
            "position": { "x": 0, "y": 0 },
            "scope": scope,
            "features": { "isTrigger": is_trigger },
        })).collect();
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": edges,
            "groups": groups
        })).expect("valid test project")
    }

    /// The group `g` holding `members`, with an input `x` and an output `y`.
    fn group_g(members: &[&str]) -> Value {
        let port = |name: &str| serde_json::json!({ "name": name, "portType": "Number", "required": false });
        serde_json::json!({ "id": "g", "kind": "group", "nodeIds": members, "inPorts": [port("x")], "outPorts": [port("y")] })
    }

    fn ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node.clone()).collect();
        v.sort();
        v
    }

    /// Every place a program runs, spelled.
    fn sorted(program: &ProjectDefinition) -> Vec<String> {
        weft_core::project::places::running_places(program).iter().map(|place| place.to_string()).collect()
    }

    fn fire(p: &ProjectDefinition, node: &str, payload: Value) -> TriggerFire {
        compute_trigger_fire(p, node, &payload, None, &Default::default()).expect("the fire reaches an output")
    }

    /// The program and kicks of the setup of `targets`.
    fn setup(p: &ProjectDefinition, targets: Vec<weft_core::frames::Located>) -> Result<(ProjectDefinition, Vec<Kick>), String> {
        setup_cut(p, &targets).map(|cut| cut.expect("a setup with targets"))
    }

    #[test]
    fn trigger_only_upstream_node_is_skipped() {
        // A ──► TriggerX ──► B ──► Out
        // A feeds only the trigger; nothing past the trigger reaches Out
        // through A, so A must not run at fire time.
        let p = project(
            &[
                ("a", false, &[]),
                ("trigger_x", true, &[]),
                ("b", false, &[]),
                ("out", false, &[]),
            ],
            &[("a", "trigger_x"), ("trigger_x", "b"), ("b", "out")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["trigger_x".to_string()],
            "only the firing trigger should be a kick"
        );
        assert_eq!(kicks[0].payload, Some(Value::String("payload".into())));
        assert_eq!(
            sorted(&program),
            vec!["b".to_string(), "out".to_string(), "trigger_x".to_string()],
            "A feeds only the trigger, so it is outside the fire's subgraph"
        );
    }

    /// A node that only feeds a trigger (an auth node a gated route reads
    /// at setup) runs in the trigger's setup and in none of its fires: when
    /// it does not load, the fires alone would let the trigger come on, and
    /// its setup would fail halfway. The trigger is refused up front,
    /// naming the loader's message; with the node loading, it comes on.
    #[test]
    fn a_trigger_whose_setup_needs_a_node_that_did_not_load_cannot_come_on() {
        let mut p = project(
            &[("auth", false, &[]), ("hook", true, &[]), ("out", false, &[])],
            &[("auth", "hook"), ("hook", "out")],
        );
        let keys = [weft_core::activation::ActivationKey::new("hook", weft_core::instance::Owner::Shared)];
        assert!(cannot_come_on(&p, &keys, &Default::default()).is_empty(), "everything loads");
        p.broken.push(weft_core::project::Broken {
            parts: vec!["auth".into()],
            why: "node 'HmacAuth' failed to load: service.verify sets signatureHeader; run `weft catalog update`".into(),
            hides: true,
        });
        assert!(fire(&p, "hook", Value::Null).kicks.iter().all(|kick| kick.node == "hook"), "the fire does not run the auth node");
        let refused = cannot_come_on(&p, &keys, &Default::default());
        assert_eq!(refused.len(), 1, "{refused:?}");
        assert!(
            refused[0].starts_with("setting up hook cannot run") && refused[0].contains("HmacAuth' failed to load") && refused[0].contains("weft catalog update"),
            "{}",
            refused[0]
        );
    }

    #[test]
    fn node_shared_with_non_trigger_path_runs() {
        //        ┌──► TriggerX ──► C ──► Out
        //  A ────┤
        //        └──► B ───────────────► Out
        // A feeds both the trigger AND B. B → Out is a non-trigger
        // path. A must run at fire time (via the B path).
        let p = project(
            &[
                ("a", false, &[]),
                ("trigger_x", true, &[]),
                ("b", false, &[]),
                ("c", false, &[]),
                ("out", false, &[]),
            ],
            &[
                ("a", "trigger_x"),
                ("a", "b"),
                ("trigger_x", "c"),
                ("c", "out"),
                ("b", "out"),
            ],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["a".to_string(), "trigger_x".to_string()],
            "A must run via its non-trigger path; trigger carries payload"
        );
        assert_eq!(sorted(&program), vec!["a", "b", "c", "out", "trigger_x"]);
        for k in &kicks {
            if k.node == "trigger_x" {
                assert_eq!(k.payload, Some(Value::String("payload".into())));
            } else {
                assert_eq!(k.payload, None);
            }
        }
    }

    #[test]
    fn non_firing_triggers_in_subgraph_get_no_payload() {
        // TriggerX ──► Out ◄── TriggerY
        // Firing TriggerX: TriggerY still gets kicked (without payload)
        // because it's reachable upstream from Out.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("trigger_y", true, &[]),
                ("out", false, &[]),
            ],
            &[("trigger_x", "out"), ("trigger_y", "out")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("fire".into()));
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string(), "trigger_y".to_string()]);
        assert_eq!(sorted(&program), vec!["out", "trigger_x", "trigger_y"]);
        for k in &kicks {
            if k.node == "trigger_x" {
                assert_eq!(k.payload, Some(Value::String("fire".into())));
            } else {
                assert_eq!(k.payload, None);
            }
        }
    }

    #[test]
    fn a_fire_carves_out_the_other_trigger_s_own_path() {
        // TriggerX ──► NodeX      TriggerY ──► NodeY
        // Nothing joins the two, so firing one records a subgraph holding
        // only its own path: the graph view dims the rest of the program
        // for that run, the same way it does for an authored cut.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("node_x", false, &[]),
                ("trigger_y", true, &[]),
                ("node_y", false, &[]),
            ],
            &[("trigger_x", "node_x"), ("trigger_y", "node_y")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&program), vec!["node_x", "trigger_x"]);
    }

    #[test]
    fn everything_the_trigger_reaches_runs() {
        // TriggerX ──► DeadEnd: nothing declares itself a deliverable
        // any more; whatever the fire reaches runs.
        let p = project(
            &[("trigger_x", true, &[]), ("dead_end", false, &[])],
            &[("trigger_x", "dead_end")],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&program), vec!["dead_end", "trigger_x"]);
    }

    /// A trigger sitting in the middle of a group fires there: it is
    /// the run's one firing kick, the group comes whole around it (the
    /// sibling its output feeds, and the sibling's other input's source),
    /// and nothing outside the group is kicked to reach it but that source.
    #[test]
    fn a_trigger_in_the_middle_of_a_group_starts_the_run_from_itself() {
        // cfg ──► g.x ──► g.a ──► g.join ──► g.y ──► out
        //                 g.trig ─────────┘
        let p = project_with(
            &[
                ("cfg", false, &[]),
                ("g.a", false, &["g"]),
                ("g.trig", true, &["g"]),
                ("g.join", false, &["g"]),
                ("out", false, &[]),
            ],
            vec![
                wire("cfg", "g.a", &[("g", "x", "in")]),
                wire("g.a", "g.join", &[]),
                wire("g.trig", "g.join", &[]),
                wire("g.join", "out", &[("g", "y", "out")]),
            ],
            vec![group_g(&["g.a", "g.trig", "g.join"])],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "g.trig", serde_json::json!({ "hi": 1 }));
        assert_eq!(
            sorted(&program),
            vec!["cfg", "g.a", "g.join", "g.trig", "out"],
            "the run includes the join's dependencies and the group path"
        );
        let firing: Vec<&str> = kicks.iter().filter(|k| k.firing).map(|k| k.node.as_str()).collect();
        assert_eq!(firing, vec!["g.trig"], "the trigger is the one firing kick");
        assert_eq!(
            ids(&kicks),
            vec!["cfg".to_string(), "g.trig".to_string()],
            "the group's input source is kicked so g.join's other side arrives"
        );
    }

    #[test]
    fn a_scope_the_fire_touches_excludes_unrelated_instances() {
        // TriggerX ──► g.x ──► g.a ──► g.y ──► Out
        //                      g.seed (no wire feeds it)
        // Reaching the group's input follows only connected work. Its
        // unwired member is outside this trigger's program.
        let p = project_with(
            &[
                ("trigger_x", true, &[]),
                ("g.a", false, &["g"]),
                ("g.seed", false, &["g"]),
                ("out", false, &[]),
            ],
            vec![wire("trigger_x", "g.a", &[("g", "x", "in")]), wire("g.a", "out", &[("g", "y", "out")])],
            vec![group_g(&["g.a", "g.seed"])],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()], "the group's unwired member is not the run's root");
        assert_eq!(sorted(&program), vec!["g.a", "out", "trigger_x"]);
    }

    /// Each setup keeps its own target inside the group, which opens with
    /// nothing reaching it and starts that target itself. Neither the
    /// unused group input nor its source joins the setup.
    #[test]
    fn setup_phases_reach_a_trigger_or_infra_node_inside_a_group() {
        // cfg ──► g.x (read by nobody inside)
        //         g.db (infra), g.trig (trigger), both unwired
        let mut p = project_with(
            &[
                ("cfg", false, &[]),
                ("g.db", false, &["g"]),
                ("g.trig", true, &["g"]),
            ],
            vec![wire("cfg", "g", &[("g", "x", "in")])],
            vec![group_g(&["g.db", "g.trig"])],
        );
        p.nodes.iter_mut().find(|n| n.id == "g.db").unwrap().requires_infra = true;
        let (program, kicks) = setup(&p, infra_places(&p)).unwrap();
        assert_eq!(sorted(&program), vec!["g.db"]);
        assert!(kicks.is_empty(), "the group starts g.db as it opens: {kicks:?}");
        assert_eq!(setup_entry(&p, &program, &kicks), "g.db");
        let (program, _) = setup(&p, trigger_places(&p)).unwrap();
        assert_eq!(sorted(&program), vec!["g.trig"]);
        p.groups[0].kind = weft_core::project::GroupKind::Loop { loop_config: serde_json::json!({}) };
        assert!(setup(&p, infra_places(&p)).unwrap_err().contains("inside loop"));
        assert!(setup(&p, trigger_places(&p)).unwrap_err().contains("inside loop"));
        assert!(compute_trigger_fire(&p, "g.trig", &Value::Null, None, &Default::default()).unwrap_err().contains("inside loop"));
    }

    #[test]
    fn firing_non_trigger_returns_an_error() {
        let p = project(
            &[("a", false, &[]), ("out", false, &[])],
            &[("a", "out")],
        );
        assert!(compute_trigger_fire(&p, "a", &Value::Null, None, &Default::default()).unwrap_err().contains("not a trigger"));
    }

    /// Two programs in one file sharing an upstream node (a database, a
    /// provider). Firing one trigger must leave the other program's
    /// consumers OUTSIDE the subgraph: the shared node still runs (it
    /// feeds the fired program) and still emits down every wire, so the
    /// engine needs the boundary to skip the other program's nodes
    /// instead of parking their partial input sets forever. This is the
    /// shape that ended every fire of a two-trigger project as Stuck.
    #[test]
    fn a_shared_upstream_node_does_not_pull_the_other_program_in() {
        //  Shared ──┬──► TriggerX ──► Bx ──► OutX
        //           └──► TriggerY ──► By ──► OutY
        // (Shared also wires straight into Bx and By.)
        let p = project(
            &[
                ("shared", false, &[]),
                ("trigger_x", true, &[]),
                ("trigger_y", true, &[]),
                ("bx", false, &[]),
                ("by", false, &[]),
                ("out_x", false, &[]),
                ("out_y", false, &[]),
            ],
            &[
                ("shared", "bx"),
                ("shared", "by"),
                ("trigger_x", "bx"),
                ("trigger_y", "by"),
                ("bx", "out_x"),
                ("by", "out_y"),
            ],
        );
        let TriggerFire { kicks, program, .. } = fire(&p, "trigger_x", Value::String("msg".into()));
        assert_eq!(ids(&kicks), vec!["shared".to_string(), "trigger_x".to_string()]);
        assert_eq!(
            sorted(&program),
            vec!["bx", "out_x", "shared", "trigger_x"],
            "the other program's trigger and consumers stay outside the fire"
        );
    }
}

#[cfg(test)]
pub(crate) mod infra_kick_and_dep_tests {
    use super::*;
    use weft_core::project::infra_places;

    /// (id, is_trigger, requires_infra). Shared with `run_subgraph_tests`.
    pub(crate) fn project(nodes: &[(&str, bool, bool)], edges: &[(&str, &str)]) -> ProjectDefinition {
        let n_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(id, is_trigger, requires_infra)| {
                serde_json::json!({
                    "id": id,
                    "nodeType": "T",
                    "label": null,
                    "config": {},
                    "position": { "x": 0, "y": 0 },
                    "features": { "isTrigger": is_trigger },
                    "requiresInfra": requires_infra,
                })
            })
            .collect();
        let e_json: Vec<serde_json::Value> = edges
            .iter()
            .map(|(s, t)| {
                // `sourceHandle` / `targetHandle` are the names `Edge`
                // reads. Writing `sourcePort` here dropped them silently
                // (no `deny_unknown_fields`), so every wire was
                // default -> default and no port-aware walk could be
                // told apart from a port-blind one.
                serde_json::json!({
                    "id": format!("e_{}_{}", s, t),
                    "source": s,
                    "sourceHandle": "out",
                    "target": t,
                    "targetHandle": "in",
                })
            })
            .collect();
        let body = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": e_json,
            "groups": []
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn kick_ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node.clone()).collect();
        v.sort();
        v
    }

    /// The kicks of the setup of `targets`: none when there is nothing to
    /// set up.
    fn setup_kicks(p: &ProjectDefinition, targets: Vec<weft_core::frames::Located>) -> Result<Vec<Kick>, String> {
        setup_cut(p, &targets).map(|cut| cut.map(|(_, kicks)| kicks).unwrap_or_default())
    }

    #[test]
    fn infra_kicks_are_upstream_roots_not_infra_nodes() {
        // text → compute → infra
        let p = project(
            &[("text", false, false), ("compute", false, false), ("infra", false, true)],
            &[("text", "compute"), ("compute", "infra")],
        );
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        // The kick is the upstream root (text), NOT the infra node.
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn infra_kicks_skip_unreachable_branches() {
        // unrelated standalone node + a real text → infra chain.
        let p = project(
            &[
                ("standalone", false, false),
                ("text", false, false),
                ("infra", false, true),
            ],
            &[("text", "infra")],
        );
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }

    #[test]
    fn infra_node_with_no_upstream_kicks_itself() {
        // A parameterless infra node (no upstream edges) IS its own
        // root: has to kick something to fire.
        let p = project(&[("infra", false, true)], &[]);
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        assert_eq!(kick_ids(&kicks), vec!["infra".to_string()]);
    }

    #[test]
    fn infra_kicks_empty_when_no_infra_nodes() {
        let p = project(&[("a", false, false), ("b", false, false)], &[("a", "b")]);
        assert!(setup_kicks(&p, infra_places(&p)).unwrap().is_empty());
    }

    #[test]
    fn infra_kicks_handle_multiple_infra_nodes_with_shared_root() {
        // text → infraA ; text → infraB
        let p = project(
            &[("text", false, false), ("infraA", false, true), ("infraB", false, true)],
            &[("text", "infraA"), ("text", "infraB")],
        );
        let kicks = setup_kicks(&p, infra_places(&p)).unwrap();
        // text is the only root reaching both.
        assert_eq!(kick_ids(&kicks), vec!["text".to_string()]);
    }
}

#[cfg(test)]
mod resolve_scope_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;
    use weft_core::activation::ActivationScope;
    use weft_core::instance::{InstanceId, InstanceTarget};

    fn alice() -> InstanceId {
        "alice".parse().expect("instance id")
    }

    fn bob() -> InstanceId {
        "bob".parse().expect("instance id")
    }

    fn shared() -> ActivationScope {
        ActivationScope::default()
    }

    fn instance() -> ActivationScope {
        ActivationScope { triggers: Vec::new(), instances: InstanceTarget::named(vec![alice()]) }
    }

    fn all() -> ActivationScope {
        ActivationScope { triggers: Vec::new(), instances: InstanceTarget::EveryInstance }
    }

    /// A program whose only trigger exists once per instance.
    fn per_instance_only() -> ProjectDefinition {
        let mut p = project(&[("t", true, false), ("a", false, false)], &[("t", "a")]);
        p.nodes.iter_mut().find(|n| n.id == "t").expect("t").per_instance = Some(weft_core::instance::PerInstance::Marked);
        p
    }

    /// No triggers at all: the verbs that set triggers up refuse and say
    /// so, for the program and for an instance alike.
    #[test]
    fn no_triggers_refuses_with_its_own_message() {
        let p = project(&[("a", false, false)], &[]);
        for scope in [shared(), instance(), all()] {
            let (status, msg) = resolve_against(&p, &scope, &[alice()]).expect_err("nothing to resolve");
            assert_eq!(status, StatusCode::PRECONDITION_FAILED);
            assert_eq!(msg, "this program has no triggers");
        }
    }

    fn row(trigger: &str, instance: Option<&str>) -> crate::activation_store::Activation {
        crate::activation_store::Activation {
            key: weft_core::activation::ActivationKey::new(
                trigger,
                weft_core::instance::Owner::from_instance(instance.map(|m| m.parse().expect("instance id"))),
            ),
            lifecycle: crate::activation_store::ActivationLifecycle::active(),
            program: None,
            source_version: None,
            driver_task: None,
            activating: None,
            cut: None,
        }
    }

    /// Nothing activated: a take-down (deactivate, `weft rm --journal`'s
    /// quiesce) has nothing to do.
    #[test]
    fn nothing_activated_takes_down_nothing() {
        for scope in [shared(), instance(), all()] {
            assert!(keys_to_take_down(&[], &scope).expect("no-op").is_empty());
        }
    }

    /// A take-down picks the owners' rows, whatever the source says now:
    /// a trigger the source dropped is still taken down, and the other
    /// owners' rows are left alone.
    #[test]
    fn a_take_down_picks_the_owners_rows() {
        let rows = [row("gone", None), row("door", None), row("door", Some("alice")), row("door", Some("bob"))];
        let triggers = |keys: Vec<weft_core::activation::ActivationKey>| {
            keys.into_iter().map(|k| (k.trigger, k.owner.instance().map(|m| m.to_string()))).collect::<Vec<_>>()
        };
        assert_eq!(
            triggers(keys_to_take_down(&rows, &shared()).unwrap()),
            vec![("gone".to_string(), None), ("door".to_string(), None)]
        );
        assert_eq!(triggers(keys_to_take_down(&rows, &instance()).unwrap()), vec![("door".to_string(), Some("alice".to_string()))]);
        assert_eq!(
            triggers(keys_to_take_down(&rows, &all()).unwrap()),
            vec![("door".to_string(), Some("alice".to_string())), ("door".to_string(), Some("bob".to_string()))]
        );
        let named = ActivationScope { triggers: vec!["gone".into()], instances: InstanceTarget::Shared };
        assert_eq!(triggers(keys_to_take_down(&rows, &named).unwrap()), vec![("gone".to_string(), None)]);
    }

    /// A named trigger with no row for any owner named is a mistake.
    #[test]
    fn a_named_trigger_never_activated_is_refused() {
        let rows = [row("door", None)];
        let scope = ActivationScope { triggers: vec!["door".into()], instances: InstanceTarget::named(vec![alice()]) };
        let (status, msg) = keys_to_take_down(&rows, &scope).expect_err("alice's door never activated");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(msg.contains("for instance 'alice'"), "{msg}");
    }

    /// Only per-instance triggers: the program's own scope names nothing,
    /// and the refusal points at --instance; every instance reaches each
    /// known one, and none known is refused in its own words.
    #[test]
    fn per_instance_only_refuses_shared_scope() {
        let p = per_instance_only();
        let (status, msg) = resolve_against(&p, &shared(), &[]).expect_err("no shared trigger");
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert!(msg.contains("no shared trigger"), "{msg}");
        assert_eq!(resolve_against(&p, &instance(), &[]).expect("instance's trigger").len(), 1);
        assert_eq!(resolve_against(&p, &all(), &[alice(), bob()]).expect("every instance's trigger").len(), 2);
        let (_, msg) = resolve_against(&p, &all(), &[]).expect_err("no instance known");
        assert!(msg.contains("no instance weft holds anything for yet"), "{msg}");
    }
}

#[cfg(test)]
mod run_subgraph_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;

    /// What a run aimed at `targets` keeps.
    fn aimed_at(p: &ProjectDefinition, targets: &[&str]) -> std::collections::BTreeSet<Located> {
        weft_core::project::cut::walk(&weft_core::project::graph::ProjectGraph::new(p), &weft_core::project::cut::SelectionBounds {
            target: targets.iter().map(|s| s.to_string()).collect(), ..Default::default()
        }).unwrap().kept()
    }

    /// Diamond: src feeds two branches, only one is aimed at. The
    /// other branch's exclusive nodes stay out of the subgraph, so
    /// they are neither infra-gated nor dispatched.
    #[test]
    fn aiming_at_one_output_excludes_the_other_branch() {
        let p = project(
            &[
                ("src", false, false),
                ("a", false, false),
                ("out1", false, false),
                ("b", false, false),
                ("out2", false, false),
            ],
            &[("src", "a"), ("a", "out1"), ("src", "b"), ("b", "out2")],
        );
        let sub = aimed_at(&p, &["out1"]);
        let mut nodes: Vec<&str> = sub.iter().map(|s| s.id.as_str()).collect();
        nodes.sort();
        assert_eq!(nodes, vec!["a", "out1", "src"]);
    }

    /// A trigger in the walk is a terminator: included (it gets kicked
    /// payload-less so its branch prunes), but its own upstream is not.
    #[test]
    fn a_trigger_terminates_the_upstream_walk_and_kicks_bare() {
        let p = project(
            &[
                ("setup", false, false),
                ("trig", true, false),
                ("mid", false, false),
                ("out", false, false),
            ],
            &[("setup", "trig"), ("trig", "mid"), ("mid", "out")],
        );
        let sub = aimed_at(&p, &["out"]);
        assert!(sub.contains(&Located::top("trig")));
        assert!(!sub.contains(&Located::top("setup")), "the trigger's own upstream stays out");

        let resolved = weft_core::run_spec::resolve_spec(&weft_core::run_spec::RunSpec {
            target: vec!["out".into()], ..weft_core::run_spec::RunSpec::whole("x")
        }, &p, &Default::default()).unwrap();
        let trig = resolved.kicks.iter().find(|k| k.node == "trig").expect("trigger is a root");
        assert!(trig.payload.is_none(), "a manual run kicks triggers payload-less");
        assert!(!trig.firing);
    }

    /// Fire time: only the firing trigger carries the wake payload
    /// and the snapshot; every other root is a bare kick.
    #[test]
    fn only_the_firing_trigger_carries_the_payload() {
        let p = project(
            &[
                ("trig", true, false),
                ("other", true, false),
                ("out", false, false),
            ],
            &[("trig", "out"), ("other", "out")],
        );
        let payload = serde_json::json!({ "msg": "hi" });
        let snapshot = serde_json::json!({ "port": "v" });
        let kicks = weft_core::run_spec::compute_trigger_fire(&p, "trig", &payload, Some(&snapshot), &Default::default()).unwrap().kicks;
        let trig = kicks.iter().find(|k| k.node == "trig").unwrap();
        assert!(trig.firing);
        assert_eq!(trig.payload.as_ref(), Some(&payload));
        assert_eq!(trig.port_snapshot.as_ref(), Some(&snapshot));
        let other = kicks.iter().find(|k| k.node == "other").unwrap();
        assert!(!other.firing);
        assert!(other.payload.is_none() && other.port_snapshot.is_none());
    }
}

#[cfg(test)]
mod trigger_infra_ready_tests {
    use super::trigger_infra_ready;
    use crate::infra_node::{InfraNodeRow, InfraNodeStatus};

    fn row(node_id: &str, status: InfraNodeStatus) -> InfraNodeRow {
        InfraNodeRow {
            project_id: uuid::Uuid::nil(),
            node_id: node_id.into(),
            instance: None,
            copy_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
            source_hash: None,
        }
    }

    fn deps(ids: &[&str]) -> std::collections::BTreeSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    #[test]
    fn a_project_whose_triggers_need_no_infra_is_ready() {
        assert!(trigger_infra_ready(&deps(&[]), &[]));
        // Including one with infra running for something else entirely.
        assert!(trigger_infra_ready(&deps(&[]), &[row("svc", InfraNodeStatus::Running)]));
    }

    #[test]
    fn every_node_the_triggers_need_has_to_be_running() {
        let both = deps(&["bridge", "db"]);
        assert!(trigger_infra_ready(
            &both,
            &[row("bridge", InfraNodeStatus::Running), row("db", InfraNodeStatus::Running)]
        ));
        assert!(!trigger_infra_ready(
            &both,
            &[row("bridge", InfraNodeStatus::Running), row("db", InfraNodeStatus::Stopped)]
        ));
    }

    #[test]
    fn a_node_that_is_not_running_in_any_way_holds_the_activation_back() {
        for status in [
            InfraNodeStatus::Stopped,
            InfraNodeStatus::Failed,
            InfraNodeStatus::Flaky,
            InfraNodeStatus::Provisioning,
        ] {
            assert!(!trigger_infra_ready(&deps(&["bridge"]), &[row("bridge", status)]), "{status:?}");
        }
    }

    #[test]
    fn a_node_with_no_row_at_all_is_not_running() {
        // Nothing was ever provisioned for it, which is the ordinary
        // state of a project whose infra has never been started.
        assert!(!trigger_infra_ready(&deps(&["bridge"]), &[]));
        // And a row for a DIFFERENT node does not stand in for it.
        assert!(!trigger_infra_ready(&deps(&["bridge"]), &[row("db", InfraNodeStatus::Running)]));
    }
}

#[cfg(test)]
mod available_actions_tests {
    //! Layer-1 tests for the reconciliation table
    //! (`docs/project-lifecycle-state-model.md` §8): one case per
    //! table row, pure inputs -> expected verb set.

    use super::*;
    use crate::activation_store::ActivationLifecycle as ProjectLifecycle;
    use crate::project_store::ProjectTransition;

    struct Case {
        removing: bool,
        lifecycle: ProjectLifecycle,
        transition: ProjectTransition,
        resync_stopped: bool,
        has_triggers: bool,
        has_infra: bool,
        trigger_infra_ready: bool,
        orphaned_infra: bool,
        infra_rollup: &'static str,
        infra_busy: bool,
        any_on: bool,
        any_behind: bool,
        infra_behind: bool,
    }

    impl Default for Case {
        fn default() -> Self {
            Self {
                removing: false,
                lifecycle: ProjectLifecycle::refused(),
                transition: ProjectTransition::None,
                resync_stopped: false,
                has_triggers: true,
                has_infra: false,
                trigger_infra_ready: true,
                orphaned_infra: false,
                infra_rollup: "none",
                infra_busy: false,
                any_on: false,
                any_behind: false,
                infra_behind: false,
            }
        }
    }

    /// Some trigger on, none behind.
    fn on() -> Case {
        Case { lifecycle: ProjectLifecycle::active(), any_on: true, ..Case::default() }
    }

    fn actions(case: &Case) -> Vec<String> {
        compute_available_actions(&ActionInputs {
            removing: case.removing,
            lifecycle: &case.lifecycle,
            transition: case.transition,
            resync_stopped: case.resync_stopped,
            has_triggers: case.has_triggers,
            has_infra: case.has_infra,
            trigger_infra_ready: case.trigger_infra_ready,
            run_infra_ready: super::run_infra_ready(case.has_infra, case.infra_rollup),
            orphaned_infra: case.orphaned_infra,
            infra_rollup: case.infra_rollup,
            infra_busy: case.infra_busy,
            any_on: case.any_on,
            any_behind: case.any_behind,
            infra_behind: case.infra_behind,
        })
    }

    fn assert_actions(case: &Case, expected: &[&str]) {
        assert_eq!(actions(case), expected.to_vec());
    }

    /// A project `weft rm` marked offers nothing, whatever else holds:
    /// `weft rm` again is the one way on.
    #[test]
    fn a_project_being_removed_offers_nothing() {
        assert_actions(&Case { removing: true, ..Case::default() }, &[]);
        assert_actions(&Case { removing: true, transition: ProjectTransition::Building, ..Case::default() }, &[]);
        assert_actions(
            &Case { removing: true, has_infra: true, infra_rollup: "running", ..Case::default() },
            &[],
        );
    }

    // ---- master rule: any transitional state offers only its cancel ----

    #[test]
    fn building_offers_only_cancel_build() {
        assert_actions(
            &Case { transition: ProjectTransition::Building, ..Case::default() },
            &["cancel_build"],
        );
        // Idempotent while already cancelling.
        assert_actions(
            &Case { transition: ProjectTransition::CancellingBuild, ..Case::default() },
            &["cancel_build"],
        );
    }

    #[test]
    fn activating_offers_only_cancel_activate() {
        assert_actions(
            &Case { lifecycle: ProjectLifecycle::activating(uuid::Uuid::nil()), ..Case::default() },
            &["cancel_activate"],
        );
    }

    #[test]
    fn a_resync_offers_only_the_cancel_of_the_step_it_is_at() {
        let resyncing = |lifecycle| Case {
            lifecycle,
            transition: ProjectTransition::Resyncing,
            has_infra: true,
            infra_rollup: "running",
            any_on: true,
            any_behind: true,
            ..Case::default()
        };
        assert_actions(&resyncing(ProjectLifecycle::holding()), &[]);
        assert_actions(&resyncing(ProjectLifecycle::activating(uuid::Uuid::nil())), &["cancel_activate"]);
        assert_actions(&resyncing(ProjectLifecycle::active()), &[]);
        // Stopped with triggers down: taken up again, or given up.
        assert_actions(&Case { resync_stopped: true, ..resyncing(ProjectLifecycle::holding()) }, &["resync", "cancel_activate"]);
    }

    #[test]
    fn infra_transitional_offers_only_infra_cancel() {
        for rollup in ["provisioning", "stopping", "terminating"] {
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, ..Case::default() },
                &["infra_cancel"],
            );
        }
    }

    #[test]
    fn infra_op_in_flight_is_transitional_even_with_a_stable_rollup() {
        // A claimed stop still waiting for its runs (or an InfraSetup run
        // before any node row flips) leaves the rollup reading "running";
        // the in-flight fact must still collapse the row to cancel-only.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                infra_busy: true,
                ..Case::default()
            },
            &["infra_cancel"],
        );
    }

    // ---- stable rows ----

    #[test]
    fn nothing_on_no_infra_offers_run_and_activate() {
        assert_actions(&Case::default(), &["run", "activate"]);
    }

    #[test]
    fn no_triggers_hides_activate_but_run_stands() {
        assert_actions(&Case { has_triggers: false, ..Case::default() }, &["run"]);
    }

    #[test]
    fn infra_resting_still_offers_activate_when_no_trigger_needs_it() {
        // The infra sits on a branch no trigger touches, so arming the
        // triggers is sound with it down. Run still waits: a run touches
        // the whole graph.
        assert_actions(
            &Case { has_infra: true, infra_rollup: "none", ..Case::default() },
            &["activate", "infra_start"],
        );
    }

    #[test]
    fn a_trigger_whose_infra_is_down_cannot_be_armed() {
        // The bridge the trigger DEPENDS ON is not up, so registering
        // the signal would point it at an address that does not answer.
        // Start the infra, and activate comes back.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "none",
                trigger_infra_ready: false,
                ..Case::default()
            },
            &["infra_start"],
        );
    }

    #[test]
    fn infra_running_offers_everything_stable_and_behind_lights_upgrade() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "running", ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
        assert_actions(
            &Case { has_infra: true, infra_rollup: "running", infra_behind: true, ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate", "infra_upgrade"],
        );
    }

    #[test]
    fn degraded_infra_holds_activate_back_only_when_a_trigger_needs_it() {
        // `partial` is a steady state, not a transient. Whether it holds
        // activation back depends on WHICH half is down.
        for rollup in ["failed", "flaky", "partial"] {
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, ..Case::default() },
                &["activate", "infra_start", "infra_stop", "infra_terminate"],
            );
            assert_actions(
                &Case {
                    has_infra: true,
                    infra_rollup: rollup,
                    trigger_infra_ready: false,
                    ..Case::default()
                },
                &["infra_start", "infra_stop", "infra_terminate"],
            );
            assert_actions(
                &Case { has_infra: true, infra_rollup: rollup, infra_behind: true, ..Case::default() },
                &["activate", "infra_start", "infra_stop", "infra_terminate", "infra_upgrade"],
            );
        }
    }

    #[test]
    fn each_verb_waits_on_the_infra_it_would_touch() {
        // `run` touches the whole graph, so the whole-graph rollup gates
        // it. `activate` arms the triggers, so only the infra those
        // triggers DEPEND ON gates it. The two answers differ exactly
        // when an infra node is not upstream of any trigger, which is
        // the pair below: same rollup, and the only thing that moves is
        // whether a trigger needs the node that is down.
        for rollup in ["none", "stopped", "partial", "failed", "flaky"] {
            let unrelated =
                actions(&Case { has_infra: true, infra_rollup: rollup, ..Case::default() });
            assert!(!unrelated.contains(&"run".to_string()), "{rollup}: run");
            assert!(unrelated.contains(&"activate".to_string()), "{rollup}: activate");
            let needed = actions(&Case {
                has_infra: true,
                infra_rollup: rollup,
                trigger_infra_ready: false,
                ..Case::default()
            });
            assert!(!needed.contains(&"run".to_string()), "{rollup}: run");
            assert!(!needed.contains(&"activate".to_string()), "{rollup}: activate");
        }
        let ready = actions(&Case { has_infra: true, infra_rollup: "running", ..Case::default() });
        assert!(ready.contains(&"run".to_string()));
        assert!(ready.contains(&"activate".to_string()));
    }

    #[test]
    fn infra_stopped_offers_start_and_terminate() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["activate", "infra_start", "infra_terminate"],
        );
    }

    /// Turning on what is on does nothing, so activate stays offered beside
    /// deactivate whatever is on: it turns on the rest.
    #[test]
    fn some_on_offers_deactivate_beside_activate() {
        assert_actions(&on(), &["run", "activate", "deactivate"]);
        assert_actions(&Case { trigger_infra_ready: false, ..on() }, &["run", "deactivate"]);
    }

    /// Resync is offered only while a trigger that is on is behind, and the
    /// infra its setup reads runs: it arms what activate arms.
    #[test]
    fn a_trigger_behind_lights_resync() {
        assert_actions(&Case { any_behind: true, ..on() }, &["run", "activate", "deactivate", "resync"]);
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                trigger_infra_ready: false,
                any_behind: true,
                ..on()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn on_with_last_trigger_deleted_keeps_deactivate() {
        // The source no longer has triggers but some are still on;
        // deactivate must stay offered so the user can bring them down.
        assert_actions(&Case { has_triggers: false, ..on() }, &["run", "deactivate"]);
    }

    #[test]
    fn on_with_infra_running_keeps_stop_and_terminate() {
        // An infra stop takes down only the triggers whose cut reads it,
        // so it stays offered while triggers are on.
        assert_actions(
            &Case { has_infra: true, infra_rollup: "running", ..on() },
            &["run", "activate", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    // ---- Model 1: orphaned live infra ----

    #[test]
    fn orphan_never_gates_run_and_keeps_stop_terminate_visible() {
        // Source has NO infra (the user deleted the node) but live
        // rows still run: run/activate are free (the plain graph runs
        // in the shared pool, unlinked) AND the infra controls stay so
        // the user never loses track of the orphan. No start/upgrade:
        // there is no source spec to provision from.
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "running", infra_behind: true, ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn stopped_orphan_still_offers_terminate() {
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["run", "activate", "infra_terminate"],
        );
    }

    #[test]
    fn orphan_mid_terminate_offers_infra_cancel() {
        assert_actions(
            &Case { orphaned_infra: true, infra_rollup: "terminating", ..Case::default() },
            &["infra_cancel"],
        );
    }
}

/// Layer-1 tests for what reads a trigger copy or an infra copy as behind
/// the program.
#[cfg(test)]
mod behind_tests {
    use super::*;

    /// What the client names replaces the registered build's hash of the
    /// same name, and what it leaves out (a cut it could not walk) is read
    /// off the registered build; a malformed list is refused naming it.
    #[test]
    fn the_queried_hashes_go_over_the_registered_ones() {
        let registered: std::collections::BTreeMap<String, String> =
            [("door".to_string(), "r1".to_string()), ("feed".to_string(), "r2".to_string())].into();
        let merged = Desired::named_over(registered.clone(), Some("feed:q2,inbox:q3"), "desiredTriggerCuts", |hash| hash).unwrap();
        let want: std::collections::BTreeMap<String, String> =
            [("door", "r1"), ("feed", "q2"), ("inbox", "q3")].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        assert_eq!(merged, want);
        assert_eq!(Desired::named_over(registered.clone(), None, "desiredTriggerCuts", |hash| hash).unwrap(), registered);
        let (status, message) = Desired::named_over(registered, Some("feed"), "desiredTriggerCuts", |hash| hash).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.starts_with("desiredTriggerCuts"), "{message}");
    }

    use crate::activation_store::{Activation, ActivationLifecycle, RecordedCut};
    use weft_core::activation::ActivationKey;
    use weft_core::instance::Owner;

    fn desired() -> Desired {
        Desired {
            binary: Some("new-code".into()),
            full_binary: Some("full-code".into()),
            definition: Some("graph".into()),
            trigger_cuts: [("door".to_string(), Some("cut-2".to_string())), ("broken".to_string(), None)].into(),
            infra_places: [("db".to_string(), "db-2".to_string())].into(),
        }
    }

    fn activation(fingerprint: Option<&str>, binary: Option<&str>) -> Activation {
        Activation {
            key: ActivationKey::new("door", Owner::Shared),
            lifecycle: ActivationLifecycle::active(),
            program: binary.map(|binary| weft_core::project::hash::ProgramIdentity {
                binary_hash: binary.into(),
                definition_hash: "graph".into(),
                implementations: Default::default(),
            }),
            source_version: None,
            driver_task: None,
            activating: None,
            cut: fingerprint.map(|fingerprint| RecordedCut { fingerprint: fingerprint.into(), depends_on: Vec::new() }),
        }
    }

    /// A trigger on is behind when its cut moved or the code it fires
    /// did; a worker built with every node compiled in is current; one set
    /// up before cuts were recorded reads as behind, so a resync records
    /// its cut.
    #[test]
    fn a_trigger_is_behind_when_its_cut_or_its_code_moved() {
        let d = desired();
        assert!(!d.behind(&activation(Some("cut-2"), Some("new-code"))));
        assert!(!d.behind(&activation(Some("cut-2"), Some("full-code"))));
        assert!(d.behind(&activation(Some("cut-1"), Some("new-code"))));
        assert!(d.behind(&activation(Some("cut-2"), Some("old-code"))));
        assert!(d.behind(&activation(None, Some("new-code"))));
    }

    /// A trigger the program still has, whose cut can no longer be walked
    /// (it needs a part that does not compile), is behind: never read as
    /// gone, so a resync takes it up and refuses naming the part.
    #[test]
    fn a_trigger_whose_cut_cannot_be_walked_is_behind() {
        let mut broken = activation(Some("cut-2"), Some("new-code"));
        broken.key = ActivationKey::new("broken", Owner::Shared);
        assert!(desired().behind(&broken));
    }

    /// Only a trigger that is on can be behind; one the program no longer
    /// has has nothing to be brought up to.
    #[test]
    fn a_trigger_off_or_gone_is_never_behind() {
        let d = desired();
        let mut down = activation(Some("cut-1"), Some("old-code"));
        down.lifecycle = ActivationLifecycle::holding();
        assert!(!d.behind(&down));
        let mut gone = activation(Some("cut-1"), None);
        gone.key = ActivationKey::new("removed", Owner::Shared);
        assert!(!d.behind(&gone));
    }

    /// An infra copy is behind when its place's hash moved since it was
    /// applied, or it was applied before hashes were recorded; a place the
    /// program names no hash for is never behind.
    #[test]
    fn an_infra_copy_is_behind_when_its_place_moved() {
        let d = desired();
        assert!(!d.infra_behind("db", Some("db-2")));
        assert!(d.infra_behind("db", Some("db-1")));
        assert!(d.infra_behind("db", None));
        assert!(!d.infra_behind("cache", None));
    }
}

#[cfg(test)]
mod status_query_wire_shape_tests {
    use super::*;

    /// `StatusQuery`'s wire shape is the contract with the CLI's URL
    /// builder (and any future direct caller from the extension).
    /// A typo in one of the `rename = "desiredXyz"`
    /// attributes silently sets the field to `None`, the drift
    /// comparison bypasses, and no compile error fires. Pin the
    /// camelCase wire shape here so a rename is loud.
    ///
    /// Tested via the JSON serde shape because Query at runtime
    /// flows through `serde_urlencoded` (transitive dep of axum,
    /// not directly in the dispatcher's dev-dep set). The `rename`
    /// attribute applies to BOTH formats, so a JSON round-trip is
    /// enough to catch a typo in the rename string.
    #[test]
    fn status_query_round_trips_camelcase_keys() {
        let json = serde_json::json!({
            "desiredBinaryHash": "abc",
            "desiredFullBinaryHash": "full",
            "desiredDefinitionHash": "def",
            "desiredTriggerCuts": "door:c1",
            "desiredInfraPlaces": "db:h1",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("camelCase keys must deserialize");
        assert_eq!(q.desired_binary_hash.as_deref(), Some("abc"));
        assert_eq!(q.desired_full_binary_hash.as_deref(), Some("full"));
        assert_eq!(q.desired_definition_hash.as_deref(), Some("def"));
        assert_eq!(q.desired_trigger_cuts.as_deref(), Some("door:c1"));
        assert_eq!(q.desired_infra_places.as_deref(), Some("db:h1"));
    }

    /// Snake_case used to be accepted via `alias`. The Round 4
    /// alias-drop removed that compatibility; pin it as not-aliased
    /// so a future regression that re-adds the alias is loud.
    #[test]
    fn status_query_ignores_snake_case_keys() {
        let json = serde_json::json!({
            "desired_binary_hash": "abc",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("deserialize never fails on optional-only fields");
        // The snake_case key is unknown; the camelCase field stays
        // None. If anyone re-adds `alias = "desired_binary_hash"`,
        // this flips to Some("abc") and the test fails.
        assert_eq!(q.desired_binary_hash, None);
    }
}

#[cfg(test)]
mod infra_entries_tests {
    use super::{infra_entries, Desired};
    use crate::infra_node::{InfraNodeRow, InfraNodeStatus, ObservedCopies};
    use weft_core::instance::InstanceId;

    fn row(node_id: &str, instance: Option<&str>, status: InfraNodeStatus) -> InfraNodeRow {
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
            doors: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
            source_hash: None,
        }
    }

    fn no_hashes() -> Desired {
        Desired {
            binary: None,
            full_binary: None,
            definition: None,
            trigger_cuts: Default::default(),
            infra_places: Default::default(),
        }
    }

    /// Every infra place the program declares is listed, started or not:
    /// a shared one with its copy's state (or `not_started`, or
    /// `provisioning` while a start brings it up), a `@per_instance` one as
    /// `per_instance` with its instances' copies counted. Instance copies are
    /// listed on their own, a starting one included; a node that needs no
    /// infra is not listed.
    #[test]
    fn every_declared_infra_place_is_listed() {
        let mut project = super::infra_kick_and_dep_tests::project(
            &[("db", false, true), ("cache", false, true), ("queue", false, true), ("bridge", false, true), ("plain", false, false)],
            &[],
        );
        project.nodes.iter_mut().find(|n| n.id == "bridge").unwrap().per_instance = Some(weft_core::instance::PerInstance::Marked);
        let copies = ObservedCopies {
            rows: vec![row("db", None, InfraNodeStatus::Running), row("bridge", Some("ada"), InfraNodeStatus::Stopped)],
            starting: vec![("queue".into(), None), ("bridge".into(), Some(InstanceId::new("bob").unwrap()))],
            change_asked_unix: [(("queue".to_string(), None), 100), (("bridge".to_string(), Some(InstanceId::new("bob").unwrap())), 110)]
                .into(),
            as_of_unix: 130,
        };
        let (infra, instance_entries) = infra_entries(&project, &copies, &[], &no_hashes());
        let listed: Vec<(&str, &str, Option<usize>)> =
            infra.iter().map(|e| (e.node.as_str(), e.status.as_str(), e.instance_copy_count)).collect();
        assert_eq!(
            listed,
            [
                ("bridge", "per_instance", Some(2)),
                ("cache", "not_started", None),
                ("db", "running", None),
                ("queue", "provisioning", None),
            ]
        );
        let instances: Vec<(&str, &str, &str)> =
            instance_entries.iter().map(|c| (c.node.as_str(), c.instance.as_str(), c.status.as_str())).collect();
        assert_eq!(instances, [("bridge", "ada", "stopped"), ("bridge", "bob", "provisioning")]);
        // A copy being started counts from its request, row or no row, as
        // of the clock the copies were read on.
        let since = |progress: Option<&weft_core::infra::wire::ChangeProgress>| progress.map(|p| (p.since_unix, p.as_of_unix));
        let shared = |node: &str| infra.iter().find(|e| e.node == node).unwrap().progress.as_ref();
        assert_eq!(since(shared("queue")), Some((100, 130)), "a shared copy with no row");
        assert_eq!(since(shared("db")), None, "a running copy shows none");
        let bob = instance_entries.iter().find(|e| e.instance.as_str() == "bob").unwrap();
        assert_eq!(since(bob.progress.as_ref()), Some((110, 130)), "an instance's copy with no row");
    }

    /// A program with no infra node lists none, which is the one case the
    /// CLI says no node declares `requiresInfra`.
    #[test]
    fn a_program_without_infra_lists_none() {
        let project = super::infra_kick_and_dep_tests::project(&[("plain", false, false)], &[]);
        let (infra, instances) = infra_entries(&project, &ObservedCopies::default(), &[], &no_hashes());
        assert!(infra.is_empty() && instances.is_empty());
    }
}

#[cfg(test)]
mod trigger_entries_tests {
    use super::{infra_entries, trigger_entries, Desired};
    use crate::activation_store::{Activation, ActivationLifecycle, RecordedCut};
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};
    use weft_core::projects::TriggerState;

    /// `db` feeds the shared trigger `door`; `inbox` exists once per
    /// instance.
    fn program() -> weft_core::ProjectDefinition {
        let mut p = super::infra_kick_and_dep_tests::project(
            &[("db", false, true), ("door", true, false), ("inbox", true, false)],
            &[("db", "door")],
        );
        p.nodes.iter_mut().find(|n| n.id == "inbox").unwrap().per_instance = Some(weft_core::instance::PerInstance::Marked);
        p
    }

    /// A program whose only trigger exists once per instance still has
    /// triggers: `activate` is offered for its instances.
    #[test]
    fn a_trigger_per_instance_alone_is_a_trigger() {
        let mut p = super::infra_kick_and_dep_tests::project(&[("inbox", true, false)], &[]);
        assert!(super::has_triggers(&p));
        p.nodes.iter_mut().find(|n| n.id == "inbox").unwrap().per_instance = Some(weft_core::instance::PerInstance::Marked);
        assert!(super::has_triggers(&p), "per instance");
        let none = super::infra_kick_and_dep_tests::project(&[("db", false, true)], &[]);
        assert!(!super::has_triggers(&none));
    }

    fn row(trigger: &str, owner: Owner, lifecycle: ActivationLifecycle, cut: Option<(&str, &[&str])>) -> Activation {
        Activation {
            key: ActivationKey::new(trigger, owner),
            lifecycle,
            program: None,
            source_version: None,
            driver_task: None,
            activating: None,
            cut: cut.map(|(fingerprint, depends_on)| RecordedCut {
                fingerprint: fingerprint.into(),
                depends_on: depends_on.iter().map(|s| s.to_string()).collect(),
            }),
        }
    }

    fn desired(cuts: &[(&str, &str)]) -> Desired {
        Desired {
            binary: None,
            full_binary: None,
            definition: None,
            trigger_cuts: cuts.iter().map(|(t, c)| (t.to_string(), Some(c.to_string()))).collect(),
            infra_places: Default::default(),
        }
    }

    /// One entry per trigger of the program, the shared one with its own
    /// row (or off with none), one that exists per instance with each
    /// instance's copy, then the triggers the program has no more that
    /// still have a row; each reads behind when its cut moved.
    #[test]
    fn every_trigger_is_listed_with_its_copies_and_whether_it_is_behind() {
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        let rows = [
            row("door", Owner::Shared, ActivationLifecycle::active(), Some(("door-1", &["db"]))),
            row("inbox", ada, ActivationLifecycle::active(), Some(("inbox-1", &[]))),
            row("old", Owner::Shared, ActivationLifecycle::holding(), None),
        ];
        let entries = trigger_entries(&program(), &rows, &Default::default(), &Default::default(), &desired(&[("door", "door-1"), ("inbox", "inbox-2")]), None, 0);
        let nodes: Vec<&str> = entries.iter().map(|e| e.node.as_str()).collect();
        assert_eq!(nodes, ["door", "inbox", "old"]);
        let door = &entries[0];
        assert_eq!((door.per_instance, door.shared.as_ref().map(|c| c.state)), (false, Some(TriggerState::On)));
        assert_eq!(door.depends_on, vec!["db".to_string()]);
        let inbox = &entries[1];
        assert!(inbox.per_instance && inbox.shared.is_none());
        assert_eq!(inbox.copies.iter().map(|c| (c.instance.as_ref().map(|i| i.as_str()), c.state)).collect::<Vec<_>>(), [(Some("ada"), TriggerState::Behind)]);
        let old = &entries[2];
        assert!(old.gone && !old.per_instance);
        assert_eq!(old.shared.as_ref().map(|c| c.state), Some(TriggerState::Off));

        // A shared trigger never turned on is listed off, its cut read off
        // the program.
        let never = trigger_entries(&program(), &[], &Default::default(), &Default::default(), &desired(&[]), None, 0);
        assert_eq!(never[0].shared.as_ref().map(|c| c.state), Some(TriggerState::Off));
        assert_eq!(never[0].depends_on, vec!["db".to_string()]);
    }

    /// A trigger the health check took down whose bring-back the routes'
    /// one binary refuses (another trigger is on another build than the
    /// registered one) reads held back, naming that trigger and the way
    /// through; one a person took down, or with every trigger on the
    /// registered build, does not.
    #[test]
    fn a_trigger_whose_bring_back_is_refused_reads_held_back() {
        let identity = |binary: &str| weft_core::project::hash::ProgramIdentity {
            definition_hash: "d".into(),
            binary_hash: binary.into(),
            implementations: Default::default(),
        };
        let mut door = row("door", Owner::Shared, ActivationLifecycle::holding(), Some(("door-1", &["db"])));
        door.lifecycle.went_down = Some(weft_broker_client::activation::WentDown::Health);
        let mut inbox = row("inbox", Owner::Instance(InstanceId::new("ada").unwrap()), ActivationLifecycle::active(), Some(("inbox-1", &[])));
        inbox.program = Some(identity("old"));
        let rows = [door.clone(), inbox];
        let held_back = |rows: &[Activation], registered: &str| {
            trigger_entries(&program(), rows, &Default::default(), &Default::default(), &desired(&[]), Some(registered), 0)[0]
                .shared
                .as_ref()
                .unwrap()
                .held_back
                .clone()
        };
        let why = held_back(&rows, "new").expect("held back");
        assert!(why.contains("'inbox' of instance 'ada'") && why.contains("weft resync --all") && why.contains("comes back on its own"), "{why}");
        assert_eq!(held_back(&rows, "old"), None, "every trigger on is on the registered build");
        let mut by_a_person = rows.clone();
        by_a_person[0].lifecycle.went_down = None;
        assert_eq!(held_back(&by_a_person, "new"), None, "a person brings it back");
    }

    /// What taking infra down takes down with it is the triggers on whose
    /// cut contains it, by owner: one already down is not listed.
    #[test]
    fn taking_infra_down_names_only_the_triggers_on_that_read_it() {
        let rows = [
            row("door", Owner::Shared, ActivationLifecycle::active(), Some(("door-1", &["db"]))),
            row("door", Owner::Instance(InstanceId::new("bob").unwrap()), ActivationLifecycle::holding(), Some(("door-1", &["db"]))),
        ];
        let (infra, _) = infra_entries(&program(), &Default::default(), &rows, &desired(&[]));
        let db = infra.iter().find(|e| e.node == "db").unwrap();
        assert_eq!(db.takes_down.iter().map(|owned| owned.words()).collect::<Vec<_>>(), ["'door'"]);
    }
}

#[cfg(test)]
mod unavailable_action_tests {
    use super::unavailable_action;
    use crate::project_store::ProjectTransition;

    #[test]
    fn a_verb_not_offered_lists_what_is() {
        let allowed = vec!["run".to_string(), "deactivate".to_string(), "resync".to_string()];
        let other = unavailable_action("infra_stop", ProjectTransition::None, "stopped", &allowed);
        assert!(other.contains("allowed actions: [run, deactivate, resync]"), "{other}");
    }

    #[test]
    fn starting_infra_that_already_runs_points_at_upgrade() {
        let allowed = vec!["infra_stop".to_string(), "infra_upgrade".to_string()];
        let message = unavailable_action("infra_start", ProjectTransition::None, "running", &allowed);
        assert!(message.contains("already running") && message.contains("weft infra upgrade"), "{message}");
    }

    #[test]
    fn triggers_being_switched_on_are_named_as_the_blocker() {
        let message = unavailable_action("deactivate", ProjectTransition::None, "none", &["cancel_activate".to_string()]);
        assert!(message.contains("being switched on") && message.contains("weft cancel-activate"), "{message}");
    }

    #[test]
    fn infra_that_is_changing_is_named_as_the_blocker() {
        let message = unavailable_action("activate", ProjectTransition::None, "provisioning", &["infra_cancel".to_string()]);
        assert!(message.contains("infra is changing (infra provisioning)") && message.contains("weft infra status"), "{message}");
    }

    #[test]
    fn a_build_in_flight_is_named_as_the_blocker() {
        let allowed = vec!["cancel_build".to_string()];
        let message = unavailable_action("activate", ProjectTransition::Building, "none", &allowed);
        assert!(message.contains("the project is building") && message.contains("weft cancel-build"), "{message}");
        let cancelling = unavailable_action("activate", ProjectTransition::CancellingBuild, "none", &allowed);
        assert!(cancelling.contains("being cancelled") && !cancelling.contains("weft cancel-build"), "{cancelling}");
    }
}

#[cfg(test)]
mod one_binary_tests {
    use super::{on_another_binary, on_another_binary_refusal};
    use crate::activation_store::{Activation, ActivationLifecycle};
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};

    fn on(trigger: &str, owner: Owner, binary: Option<&str>) -> Activation {
        Activation {
            key: ActivationKey::new(trigger, owner),
            lifecycle: ActivationLifecycle::active(),
            program: binary.map(|binary| weft_core::project::hash::ProgramIdentity {
                definition_hash: "d".into(),
                binary_hash: binary.into(),
                implementations: Default::default(),
            }),
            source_version: None,
            driver_task: None,
            activating: None,
            cut: None,
        }
    }

    /// The triggers on another binary than the registered one keep a verb
    /// from moving others onto it: the ones it moves, the ones on the
    /// registered binary already (a graph-only change), and the ones down
    /// never do.
    #[test]
    fn only_a_trigger_left_on_another_binary_splits_the_routes() {
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        let mut down = on("cron", Owner::Shared, Some("old"));
        down.lifecycle = ActivationLifecycle::holding();
        let rows = [
            on("door", Owner::Shared, Some("old")),
            on("feed", Owner::Shared, Some("new")),
            on("inbox", ada.clone(), Some("old")),
            down,
        ];
        let door = ActivationKey::new("door", Owner::Shared);
        let inbox = ActivationKey::new("inbox", ada);
        assert_eq!(on_another_binary(&rows, &[], "new"), vec![door.clone(), inbox.clone()]);
        assert_eq!(on_another_binary(&rows, std::slice::from_ref(&door), "new"), vec![inbox.clone()], "the instance's is left on the old build");
        assert!(on_another_binary(&rows, &[door, inbox.clone()], "new").is_empty(), "every trigger on moves");
        assert!(on_another_binary(&rows, &[], "old").len() == 1, "the old binary's own triggers are fine on it");
        let refusal = on_another_binary_refusal(&[inbox]);
        assert!(refusal.contains("'inbox' of instance 'ada'") && refusal.contains("weft resync --all"), "{refusal}");
    }
}

#[cfg(test)]
mod plan_activation_tests {
    use super::{plan_activation, ActivationPlan, PlannedUnit, Resumed, Unit};
    use crate::activation_store::{Activating, Activation, ActivationLifecycle, ActivationStep};
    use weft_core::activation::ActivationKey;
    use weft_core::instance::{InstanceId, Owner};

    fn key(trigger: &str) -> ActivationKey {
        ActivationKey::new(trigger, Owner::Shared)
    }

    /// A row of `trigger` as `lifecycle` has it, claimed last by `task`.
    fn row(trigger: &str, lifecycle: ActivationLifecycle, task: Option<uuid::Uuid>) -> Activation {
        let activating = lifecycle.activating_run_id.map(|_| Activating { since_unix: 1, step: ActivationStep::SettingUp, listened: true });
        Activation { key: key(trigger), lifecycle, program: None, source_version: None, cut: None, driver_task: task, activating }
    }

    /// Run again, an activate goes on from where its rows are, for every
    /// combination: nothing claimed yet (claim them all), a claim of its
    /// own still activating, its setup run going or ended, armed or not
    /// (carried on, whole), triggers it turned on already (only their held
    /// work left to release), and others' (left alone).
    #[test]
    fn a_task_run_again_goes_on_from_where_its_rows_are() {
        let (task, other) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let setup = uuid::Uuid::now_v7();
        let keys = || vec![key("door"), key("feed"), key("cron"), key("tick")];

        // Never claimed: every trigger off is claimed now.
        let fresh = plan_activation(keys(), &[], task, false);
        assert_eq!(fresh, ActivationPlan { claim: keys(), ..Default::default() });

        let rows = [
            // Its own claim, activating: carried on whole, the row the
            // request no longer names (`gone`) included.
            row("door", ActivationLifecycle::activating(setup), Some(task)),
            row("gone", ActivationLifecycle::activating(setup), Some(task)),
            // Turned on by it before it was run again.
            row("feed", ActivationLifecycle::active(), Some(task)),
            // Someone else's activation in flight, and someone else's on.
            row("cron", ActivationLifecycle::activating(uuid::Uuid::now_v7()), Some(other)),
            row("tick", ActivationLifecycle::active(), Some(other)),
        ];
        let plan = plan_activation(keys(), &rows, task, false);
        assert_eq!(plan.resume, vec![Resumed { run: setup, prepared: true, keys: vec![key("door"), key("gone")] }]);
        assert_eq!(plan.landed, vec![key("feed")]);
        assert!(plan.claim.is_empty(), "the others' are on, or being switched on, already");
        assert_eq!(
            plan.units(),
            vec![
                PlannedUnit { how: Unit::Resume { run: setup, prepared: true }, owner: Owner::Shared, keys: vec![key("door"), key("gone")] },
                PlannedUnit { how: Unit::Landed, owner: Owner::Shared, keys: vec![key("feed")] },
            ],
            "what it started first, then what it landed"
        );

        // A claim of its own that a cancel ended (down now) is no longer
        // its to carry on: off, it is claimed again.
        let ended = [row("door", ActivationLifecycle::holding(), Some(task))];
        assert_eq!(plan_activation(vec![key("door")], &ended, task, false).claim, vec![key("door")]);
    }

    /// A re-arm claims the triggers it names, which are on; one it re-armed
    /// before it was run again is only finished, and its own claim still
    /// activating carried on.
    #[test]
    fn a_rearm_claims_what_is_on_and_goes_on_from_its_own() {
        let task = uuid::Uuid::now_v7();
        let setup = uuid::Uuid::now_v7();
        let ada = Owner::Instance(InstanceId::new("ada").unwrap());
        let inbox = ActivationKey::new("inbox", ada.clone());
        let rows = [Activation { key: inbox.clone(), ..row("inbox", ActivationLifecycle::active(), Some(uuid::Uuid::now_v7())) }];
        let plan = plan_activation(vec![inbox.clone()], &rows, task, true);
        assert_eq!(plan.claim, vec![inbox.clone()], "on, by someone else: re-armed");
        assert_eq!(plan.units()[0].owner, ada);

        let landed = [Activation { key: inbox.clone(), ..row("inbox", ActivationLifecycle::active(), Some(task)) }];
        assert_eq!(plan_activation(vec![inbox.clone()], &landed, task, true).landed, vec![inbox.clone()]);

        let going = [Activation { key: inbox.clone(), ..row("inbox", ActivationLifecycle::activating(setup), Some(task)) }];
        assert_eq!(plan_activation(vec![inbox.clone()], &going, task, true).resume, vec![Resumed { run: setup, prepared: true, keys: vec![inbox] }]);
    }

    /// A claim carried on that its task had not got past preparing (where
    /// it drops what its triggers held, when asked to) still does that;
    /// one past it never drops again what they held since.
    #[test]
    fn a_claim_carried_on_drops_held_work_only_if_it_had_not_yet() {
        let (task, setup) = (uuid::Uuid::now_v7(), uuid::Uuid::now_v7());
        let at = |step| {
            let mut row = row("door", ActivationLifecycle::activating(setup), Some(task));
            row.activating = Some(Activating { since_unix: 1, step, listened: true });
            [row]
        };
        let prepared = |step| plan_activation(vec![key("door")], &at(step), task, false).resume[0].prepared;
        assert!(!prepared(ActivationStep::Preparing));
        assert!(prepared(ActivationStep::SettingUp));
        assert!(prepared(ActivationStep::Arming));
    }
}

#[cfg(test)]
mod older_cli_tests {
    use axum::routing::post;
    use tower::ServiceExt as _;

    /// A weft CLI from before activations ran as tasks posts where it
    /// always did, and is told to update in the body it prints, with a
    /// status it neither retries nor reads as an answer.
    #[tokio::test]
    async fn an_older_cli_activating_is_told_to_update() {
        let app = axum::Router::new().route("/projects/{id}/activate", post(super::activate_from_an_older_cli));
        let request = axum::http::Request::post(format!("/projects/{}/activate", uuid::Uuid::nil()))
            .header("content-type", "application/json")
            .body(axum::body::Body::from("{}"))
            .unwrap();
        let answer = app.oneshot(request).await.unwrap();
        assert_eq!(answer.status(), axum::http::StatusCode::GONE);
        let body = axum::body::to_bytes(answer.into_body(), usize::MAX).await.unwrap();
        let said = String::from_utf8(body.to_vec()).unwrap();
        assert!(said.contains("Update the CLI") && said.contains("nothing was activated"), "{said}");
    }
}
