//! Project lifecycle HTTP handlers. `POST /projects` registers a
//! project; `POST /projects/{id}/run` kicks off a fresh execution.
//! `weft run` on the CLI calls these.

use std::collections::HashSet;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use weft_core::deactivation::{whose_triggers, DeactivateResponse};
use weft_core::frames::Located;
use weft_core::{ProjectDefinition, RunningChoice};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

pub use weft_core::run_spec::KickPlan as Kick;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProjectSummary {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub status: String,
}

impl ProjectSummary {
    /// The summary a list shows: the project's shared activations as one
    /// status (`activation_store::aggregate`).
    pub fn of(p: crate::project_store::StoredProjectSummary, activations: &[crate::activation_store::Activation]) -> Self {
        let status = crate::activation_store::aggregate(
            activations.iter().filter(|a| a.key.owner == weft_core::member::Owner::Shared).map(|a| &a.lifecycle),
        )
        .status;
        Self {
            id: p.id.to_string(),
            name: p.name,
            description: p.description,
            status: status.as_str().to_string(),
        }
    }
}

/// The summary of one project, with its activations read.
async fn summary_of(
    state: &DispatcherState,
    summary: crate::project_store::StoredProjectSummary,
) -> Result<ProjectSummary, (StatusCode, String)> {
    let activations = state
        .activations
        .list(summary.id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    Ok(ProjectSummary::of(summary, &activations))
}

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
/// when the body names it. Control-plane only, like the referenced set it
/// deletes against.
pub async fn prune_images(
    State(state): State<DispatcherState>,
    _ops: crate::authenticator::ControlPlaneCaller,
    Json(req): Json<weft_core::images::PruneRequest>,
) -> Result<Json<weft_core::images::PruneReport>, (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // Images a build in progress claims, or registers after this read, are
    // spared inside `prune`, one by one under each image's lock, so this
    // never waits for builds to finish.
    let referenced = {
        let mut conn = state.pg_pool.acquire().await.map_err(|e| internal("referenced images", e.into()))?;
        crate::build::prune::keep_set(&mut conn, crate::build::prune::ImageScope::All)
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
/// re-checks that image under its lock (`crate::build::ledger::begin_reclaim`).
/// One definition for both, so the bulk read and the per-image re-check
/// can never disagree on what "referenced" means.
pub async fn referenced_images_query(
    conn: &mut sqlx::PgConnection,
    scope: crate::build::prune::ImageScope<'_>,
) -> anyhow::Result<ReferencedImages> {
    // Every image something may still run: each project's current one,
    // each queued or claimed task's, and each live run's (a run waiting on
    // a form or a timer resumes on the image it started on, however many
    // times the project was rebuilt since). `$1` narrows every arm to one
    // binary hash; NULL keeps them all.
    let query = format!(
        "SELECT DISTINCT running_binary_hash FROM project \
         WHERE running_binary_hash IS NOT NULL AND running_binary_hash <> '' \
           AND ($1::TEXT IS NULL OR running_binary_hash = $1) \
         UNION \
         SELECT DISTINCT binary_hash FROM task \
         WHERE status IN ('pending', 'claimed') \
           AND binary_hash IS NOT NULL AND binary_hash <> '' \
           AND ($1::TEXT IS NULL OR binary_hash = $1) \
         UNION \
         SELECT DISTINCT started.payload_json::jsonb->'program'->>'binary_hash' FROM execution ec \
         JOIN exec_event started ON started.execution_id = ec.execution_id AND started.kind = 'execution_started' \
         WHERE {} AND started.payload_json::jsonb->'program'->>'binary_hash' IS NOT NULL \
           AND ($1::TEXT IS NULL OR started.payload_json::jsonb->'program'->>'binary_hash' = $1)",
        weft_journal::unrecorded::LIVE_RUN_SQL
    );
    let rows: Vec<(String,)> = if scope.covers_workers() {
        sqlx::query_as(&query).bind(scope.worker_hash()).fetch_all(&mut *conn).await?
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
    let tag_rows: Vec<(uuid::Uuid, serde_json::Value)> =
        sqlx::query_as("SELECT id, infra_image_tags_json FROM project")
            .fetch_all(&mut *conn)
            .await?;
    for (project_id, tags) in tag_rows {
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
    // The refs live units actually run, recorded per unit on the
    // infra_node row at every apply (see `UnitRuntime::image_refs`).
    // Covers what the project-row map cannot: an UP unit frozen at an
    // older image across one or more syncs keeps running that ref until
    // it is force-stopped, so its old ref must stay referenced.
    // All rows, all units, regardless of status: over-keeping a ref is
    // bounded (rows die with the node on terminate) and the safe
    // direction for a set reclamation deletes against.
    let unit_rows: Vec<(uuid::Uuid, String, serde_json::Value)> =
        sqlx::query_as("SELECT project_id, node_id, units_json FROM infra_node")
            .fetch_all(&mut *conn)
            .await?;
    for (project_id, node_id, units) in unit_rows {
        let by_unit =
            weft_broker_client::protocol::decode_units_json(units, project_id, &node_id)?;
        for runtime in by_unit.into_values() {
            for image_ref in runtime.image_refs {
                if !image_ref.is_empty() && scope.covers_infra_ref(&image_ref) {
                    infra_refs.insert(image_ref);
                }
            }
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
) -> Json<weft_core::install::InstallInfo> {
    Json(state.install_info.clone())
}

pub async fn list(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
) -> Result<Json<Vec<ProjectSummary>>, (StatusCode, String)> {
    let items = state
        .projects
        .list(caller.0.as_str())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("list projects: {e}")))?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(summary_of(&state, item).await?);
    }
    Ok(Json(out))
}

/// `POST /projects`: the project exists on this install, under the
/// caller's tenant, with nothing built yet. What a version needs before
/// its first build (`weft checkpoint` records one without building), and
/// the first thing a build does. Idempotent: an existing project of the
/// caller's is left as it is; one of another tenant answers as if it did
/// not exist.
// SYNC: DeclareRequest <-> crates/weft-cli/src/commands/ensure.rs (ensure_project_known)
#[derive(Debug, Deserialize)]
pub struct DeclareRequest {
    pub id: uuid::Uuid,
    pub name: String,
}

pub async fn declare(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Json(req): Json<DeclareRequest>,
) -> Result<Json<ProjectSummary>, (StatusCode, String)> {
    let summary = declare_project(&state, req.id, &req.name, &caller.0).await?;
    Ok(Json(summary_of(&state, summary).await?))
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
                .ok_or((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string()));
        }
        Some(_) => return Err((StatusCode::NOT_FOUND, crate::authenticator::NO_SUCH_PROJECT.to_string())),
        None => {}
    }
    let empty: ProjectDefinition = serde_json::from_value(serde_json::json!({ "id": id, "nodes": [], "edges": [] }))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("empty program: {e}")))?;
    let summary = state
        .projects
        .register_with_hashes(empty, name, "", tenant.as_str(), None, None, None, None, None, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("declare the project: {e}")))?;
    state
        .events
        .publish(DispatcherEvent::ProjectRegistered {
            project_id: summary.id,
            name: weft_core::truncate_user_string(&summary.name, 4096),
        })
        .await;
    Ok(summary)
}

/// `POST /projects/{id}/builds`: build this version of the project inside
/// the install (`crate::build`) and register what it produced: the
/// compiled program, its three hashes, the implementations its worker
/// carries, the infra images every place runs, and the version's files as
/// the program's source. The project's running pointers move to it in the
/// same transaction, so the next verb acts on exactly this build.
///
/// This is the ONE way a project's program and images come to exist on an
/// install: nothing the client computed is believed.
pub async fn build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(req): Json<crate::build::VersionBuildRequest>,
) -> Result<Json<crate::build::BuiltProgram>, (StatusCode, String)> {
    declare_project(&state, id, &req.name, &caller.0).await?;
    crate::api::versions::validate_manifest(&req.manifest)?;
    // Claims every image the build plans from its first look at the
    // registry until the registration commits, so no prune deletes an
    // image this build found or built before anything references it
    // (`crate::build::prune::ImageHold`).
    let hold = crate::build::prune::ImageHold::new(&state.pg_pool);
    let built = crate::transition::build_version_gated(&state, id, &caller.0, &req, &hold).await?;
    hold.confirm().await.map_err(|e| (StatusCode::CONFLICT, format!("{e:#}")))?;
    let infra_images: crate::project_store::InfraImageTags = built
        .infra_images
        .iter()
        .map(|(place, images)| (place.clone(), images.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
        .collect();
    let summary = state
        .projects
        .register_with_hashes(
            built.definition.clone(),
            &req.name,
            "",
            caller.0.as_str(),
            Some(&built.binary_hash),
            Some(&built.definition_hash),
            Some(&built.infra_hash),
            Some(&infra_images),
            Some(&built.implementations),
            Some(&req.manifest),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("register the build: {e}")))?;
    // Still under the hold: what the project now runs is how recent each
    // of its images is when a prune picks what to reclaim.
    crate::build::ledger::note_running(&state.pg_pool, id, &built.images, crate::lease::now_unix())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    // Let go only now that the registration above has committed: a prune
    // deciding about one of these images under its lock then sees either
    // this claim or the committed `running_binary_hash` / infra tags
    // (`crate::build::ledger::begin_reclaim`), never neither.
    hold.release().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    // The images this project's earlier builds left, bounded now rather
    // than whenever somebody cleans: its current one and the one before
    // stay (`crate::build::prune::AfterBuildPrunes`).
    state.builder.prunes.request(&state, Some(id));
    state
        .events
        .publish(DispatcherEvent::ProjectRegistered {
            project_id: summary.id,
            name: weft_core::truncate_user_string(&summary.name, 4096),
        })
        .await;
    Ok(Json(built))
}

/// Answers through `StatusError` so "no project you may see under this
/// id" carries the `x-weft-not-found: project` marker. A client asking
/// whether a project is known yet (the CLI's checkpoint, before it
/// registers the source) must be able to tell that apart from a
/// version-skewed dispatcher missing the route.
pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<ProjectSummary>, StatusError> {
    authorize_project_marked(&state, &caller.0, id).await?;
    let summary = state
        .projects
        .get(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("get project: {e}")))?
        .ok_or(StatusError::NotMyProject)?;
    Ok(Json(summary_of(&state, summary).await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct RemoveQuery {
    /// `weft rm --force` sets this. When true, the dispatcher skips
    /// the supervisor terminate-wait window and drops the project's
    /// infra rows straight away. Use when the supervisor is wedged and
    /// the user wants the project gone NOW.
    #[serde(default)]
    pub force: bool,
}

pub async fn remove(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(query): axum::extract::Query<RemoveQuery>,
) -> Result<StatusCode, StatusError> {
    // Marked gate, not `authorize_project`: rm is a delete, so the CLI
    // retries it idempotently and needs the `x-weft-not-found` marker
    // to treat "already gone" as the desired end state instead of an
    // error (a headerless 404 must still bubble as version skew).
    authorize_project_marked(&state, &caller.0, id).await?;
    // Deactivate first: cancels any in-flight executions, unregisters
    // every wake signal (entry + resume) from the listener processes holding them,
    // drops entry tokens. If deactivate fails on a DB / listener
    // error we must abort: removing the project row while signals
    // remain in the listener leaves dangling registrations that
    // outlive their owning project.
    deactivate_project(&state, id).await?;
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
    // Tear down infra: issues a supervisor terminate command, waits
    // up to 120s for completion (unless --force), then drops all
    // infra_* rows. MUST succeed: if any of the DB cascade writes fail,
    // the project row stays (so a retry replays cleanly).
    crate::api::infra::delete_project(&state, id, &tenant, query.force).await?;
    // Its images stop counting as anybody's builds: the ones no other
    // project names become unused, and the pass asked for here reclaims
    // them (`crate::build::prune::AfterBuildPrunes`).
    crate::build::ledger::forget_project(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    state.builder.prunes.request(&state, None);
    // Reclaim the project's instance-specific stored data through the ONE hook.
    // The default frees the project's `project/`-scoped runtime files; a reclaimer
    // that also stores project content elsewhere (a versioned project-editing
    // history) extends it. `shared/`-scoped runtime files are the owner's and are
    // deliberately NOT touched (they outlive the project). MUST run before the
    // project row is dropped: a row-cascade would otherwise erase the bookkeeping
    // this reclaim reads, stranding bytes. A failure aborts the rm (a retry
    // replays cleanly).
    state
        .project_reclaimer
        .reclaim(&state, tenant.as_str(), id)
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                // `{e:#}` prints the full anyhow cause chain, so the real reason
                // (a SQL error, a ledger inconsistency, a missing tree) reaches the
                // caller instead of just the top wrapper.
                format!("could not reclaim the project's stored data: {e:#}; retry `weft rm`"),
            )
        })?;
    let removed = state
        .projects
        .remove(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("remove: {e}")))?;
    if removed {
        // Removing a project erases its history. The project row and
        // its version tree went with `remove`; this takes the runs
        // themselves, every event and every recorded value, so the
        // space comes back. What a person keeps is what they still
        // have: their files on disk.
        //
        // Keeping the runs is the shape this used to have and it was
        // the worst of both: the tree row that said which version a run
        // was on had already gone, so what survived was less readable
        // than before AND still took the space. There is also no way
        // back to it: `rm` cannot run again with the project row gone,
        // and `weft clean` has no project to clean.
        // Its queued work and its workers go too: nothing can serve
        // them now. Logged, not fatal, for the same reason as below;
        // the reaper's loop repeats it.
        if let Err(e) = crate::reaper::sweep_removed_projects(&state).await {
            tracing::warn!(
                target: "weft_dispatcher::project",
                project_id = %id, error = %e,
                "could not clear the removed project's queued work and workers; the reaper will retry"
            );
        }
        match state.journal.delete_project_executions(id).await {
            Ok(erased) => tracing::info!(
                target: "weft_dispatcher::project",
                project_id = %id, executions = erased,
                "erased the removed project's executions"
            ),
            // Logged, not fatal: the project is already gone, and
            // answering an error would tell the user the removal failed
            // when it did not. The rows left behind are unreachable
            // rather than dangerous, and they are what the reaper's
            // sweep is for.
            Err(e) => tracing::warn!(
                target: "weft_dispatcher::project",
                project_id = %id, error = %e,
                "could not erase the removed project's executions; the reaper will retry"
            ),
        }
        if let Err(e) = retire_what_no_run_needs(&state, id).await {
            // Same reasoning: logged, not fatal, the reaper retries.
            tracing::warn!(
                target: "weft_dispatcher::project",
                project_id = %id, error = %e,
                "could not retire what the removed project left behind; the reaper will retry"
            );
        }
        Ok(StatusCode::NO_CONTENT)
    } else {
        // A concurrent rm dropped the row after our gate: still rm's
        // desired end state, so it gets the marker.
        Err(StatusError::NotMyProject)
    }
}

/// Drop what a REMOVED project left behind that no surviving run needs:
/// the programs those runs were started against, and their rows in the
/// version tree.
///
/// On the ordinary removal path this finds nothing, and that is the
/// point rather than a waste. A removal takes the project, its whole
/// tree and every one of its executions, so there is no survivor left
/// to need anything. What this is for is a removal that failed halfway:
/// each of those three steps can fail on its own, and once the project
/// row is gone nothing else can reach what they left (`weft rm` refuses
/// a project it cannot find, and `weft clean` needs a project). Without
/// this and the reaper sweep behind it, one transient failure would
/// keep those rows for good.
///
/// Callers: the removal itself, `weft clean` (which may have just
/// deleted the last run that needed either), and the reaper's periodic
/// sweep. On a project that still exists both stores keep everything,
/// so calling it there is a no-op rather than a mistake.
pub(crate) async fn retire_what_no_run_needs(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> anyhow::Result<u64> {
    // The cheap discriminator first. Both stores refuse to drop anything
    // while the project row is there, so on a live project everything
    // below is thrown away, and `definition_hashes_in_use` is not cheap:
    // it reads and decodes every birth row of the project. `weft clean`
    // calls this unconditionally and `prune` calls clean once per run in
    // the subtree, so pruning N runs paid for N full scans, all of them
    // wasted.
    if state.projects.get(project_id).await?.is_some() {
        return Ok(0);
    }
    let in_use = state.journal.definition_hashes_in_use(project_id).await?;
    let definitions = state.projects.retire_unused_definitions(project_id, &in_use).await?;
    // The executions the JOURNAL still holds. A tree row outside that set
    // describes a run nothing can read, and with the project row gone
    // nothing else could ever delete it.
    let known = state.journal.execution_ids_for_project(project_id).await?;
    let versions = state.versions.retire_unused_versions(project_id, &known).await?;
    Ok(definitions + versions)
}

/// The project's CURRENT definition as one coherent (hash, shape)
/// pair: read `running_definition_hash` once, then fetch the
/// definition recorded under THAT hash. Every execution-starting path
/// MUST use this instead of pairing a live `project_json` read with a
/// separate hash read: a concurrent re-register between two separate
/// reads would journal hash B while the kick set was computed from
/// shape A, and the worker (which fetches the definition BY the
/// journaled hash) would fold kicks that may not exist in the shape
/// it actually runs.
pub(crate) async fn coherent_definition(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<(weft_core::project::hash::ProgramIdentity, ProjectDefinition), (StatusCode, String)> {
    let program = state
        .projects
        .running_program_identity(id)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("running program identity: {e}"))
        })?
        .ok_or_else(|| {
            (
                StatusCode::PRECONDITION_FAILED,
                format!(
                    "project {id} has no production code identity; build it (`weft build`, or any \
                     verb that runs it) before starting an execution"
                ),
            )
        })?;
    let hash = &program.definition_hash;
    let json = state
        .projects
        .definition_for_hash(id, hash)
        .await
        .map_err(|e| {
            (StatusCode::INTERNAL_SERVER_ERROR, format!("definition_for_hash: {e}"))
        })?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "project {id} has no recorded definition for hash {hash}; the \
                     definition history must cover the running hash"
                ),
            )
        })?;
    let project: ProjectDefinition = serde_json::from_str(&json).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("parse recorded definition {hash}: {e}"),
        )
    })?;
    Ok((program, project))
}


/// For each `requires_infra` node in the project, list the
/// `is_trigger` nodes that have it in their upstream closure.
///
/// Re-export from weft-core. The function lives there because both
/// the dispatcher (per-node safety check) and the broker
/// (supervisor_trigger_deps) need it; one definition, no clones.
pub use weft_core::project::compute_trigger_deps;

/// A setup phase kicks the roots of `seeds`' run subgraph, payload-less
/// and with no terminators (a trigger's setup needs its inputs). The
/// engine bounds the same phase with the same subgraph.
fn setup_kicks(project: &ProjectDefinition, seeds: Vec<Located>) -> Result<Vec<Kick>, String> {
    if seeds.is_empty() {
        return Ok(Vec::new());
    }
    let selection = weft_core::project::selection::RunSelection::setup(project, &seeds)?;
    Ok(selection.roots(project)
        .into_iter()
        .map(|place| Kick {
            frames: place.frames(),
            node: place.id,
            firing: false,
            payload: None,
            port_snapshot: None,
        })
        .collect())
}

/// The place a kick fires at, spelled the way the program reads it
/// (`keep.db` for the db of a file included as `keep`): the node under
/// the call sites on the kick's frames. What a run is recorded as
/// started by, setup runs here and fire runs in `versions::run`, so
/// `weft executions` lists every run by a place and never by a
/// compiled id.
pub(crate) fn kick_place(project: &ProjectDefinition, kick: &Kick) -> String {
    let place = Located::at(&kick.node, &kick.frames);
    weft_core::project::address_of(project, &place.id, &place.path)
}

/// Non-terminal InfraSetup executions for the project. The journaled
/// non-terminal execution IS the durable "infra sync in flight" state:
/// cancellable via the per-execution cancel, crash-recovered by the
/// orphaned-task reaper, visible to every dispatcher. Sync rejects
/// while any exists (two concurrent syncs would race the provisioning
/// subworkflow); the infra-cancel verb interrupts them.
pub(crate) async fn non_terminal_infra_setup_execution_ids(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: `None` for any owner's setup, `Some(None)` for the
    // shared copies' setup, `Some(Some(m))` for member m's.
    owner: Option<Option<&weft_core::member::MemberId>>,
) -> anyhow::Result<Vec<weft_core::ExecutionId>> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT ec.execution_id FROM execution ec \
         WHERE ec.project_id = $1 AND ec.phase = 'infra_setup' \
           AND ($2 OR ec.member_id IS NOT DISTINCT FROM $3) \
           AND NOT EXISTS ( \
             SELECT 1 FROM exec_event e \
             WHERE e.execution_id = ec.execution_id \
               AND e.kind IN ('execution_completed', 'execution_failed', 'execution_cancelled') \
           )",
    )
    .bind(project_id)
    .bind(owner.is_none())
    .bind(owner.flatten().map(|m| m.as_str()))
    .fetch_all(&state.pg_pool)
    .await?;
    let mut out = Vec::new();
    for row in rows {
        let execution_id_str: String = row.try_get("execution_id")?;
        match execution_id_str.parse::<weft_core::ExecutionId>() {
            Ok(c) => out.push(c),
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::api::project",
                    %project_id, %execution_id_str, error = %e,
                    "skipping infra_setup execution with bad uuid"
                );
            }
        }
    }
    Ok(out)
}

/// Whether an InfraSetup provisioning execution is in flight.
/// Is an infra setup genuinely in flight, or only RECORDED as one?
///
/// A setup that no worker will ever advance is not in flight, and the
/// difference matters because this answer refuses a new start. An
/// interrupted one leaves a run with no terminal event, nothing ages it
/// out, and every later start then collides with a run that is over:
/// the project can never bring its infrastructure up again, and the
/// only way out is a person noticing and cancelling by hand. That
/// wedged a real project, twice.
///
/// So a recorded setup whose worker is gone is ENDED here, on the way
/// past, rather than believed. Cancelling it is honest (it is what the
/// interruption meant to do) and it lands the terminal event the
/// journal was missing, which is what lets the next start through.
pub(crate) async fn infra_setup_in_flight(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies' setup (see `non_terminal_infra_setup_execution_ids`): one
    // member starting their copy does not hold another member's back.
    owner: Option<Option<&weft_core::member::MemberId>>,
) -> anyhow::Result<bool> {
    Ok(!live_infra_setup_execution_ids(state, project_id, owner).await?.is_empty())
}

/// The infra setups of `owner` (see `non_terminal_infra_setup_execution_ids`)
/// something is still working on, after ending, as
/// [`infra_setup_in_flight`] describes, every one nothing will advance.
pub(crate) async fn live_infra_setup_execution_ids(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    owner: Option<Option<&weft_core::member::MemberId>>,
) -> anyhow::Result<Vec<weft_core::ExecutionId>> {
    let mut alive = Vec::new();
    for execution_id in non_terminal_infra_setup_execution_ids(state, project_id, owner).await? {
        if crate::api::execution::execution_is_being_worked_on(&state.pg_pool, execution_id).await? {
            alive.push(execution_id);
            continue;
        }
        tracing::warn!(
            target: "weft_dispatcher::infra_setup",
            %project_id, %execution_id,
            "an infra setup is recorded as running but nothing is working on it \
             (an earlier start was interrupted); ending it so this project can \
             provision again"
        );
        crate::api::execution::cancel_execution_id(
            state,
            execution_id,
            &weft_core::exec::CancelCause::User,
        )
        .await?;
    }
    Ok(alive)
}

/// A started InfraSetup sub-execution: the execution to await plus the
/// event subscription opened BEFORE the enqueue (so the worker can't
/// beat the waiter to the terminal event).
pub struct InfraSetupRun {
    execution_id: weft_core::ExecutionId,
    events: tokio::sync::broadcast::Receiver<crate::events::LiveEvent>,
    project_id: uuid::Uuid,
}

impl InfraSetupRun {
    /// Follow a setup run already in flight (one a dispatcher that died
    /// mid-upgrade left behind), for [`await_infra_setup`] to wait out.
    pub(crate) async fn follow(state: &DispatcherState, project_id: uuid::Uuid, execution_id: weft_core::ExecutionId) -> Self {
        let events = state.events.subscribe_project(project_id).await;
        Self { execution_id, events, project_id }
    }
}

/// Start an execution: journal `ExecutionStarted` + one `NodeKicked` per kick
/// AND enqueue the `execute` task, all in ONE transaction
/// (`Journal::start_execution`). The reads (tenant, task spec) happen first;
/// a failure anywhere rolls the whole birth back, so a journaled execution
/// with no task row (a "ghost" nothing would ever run or reclaim, which would
/// wedge a later drain) is impossible by construction. Every start path
/// (`run`, trigger setup, infra setup) goes through here. `birth.source_version`
/// is recorded here.
async fn start_queued_execution(
    state: &DispatcherState,
    mut birth: Birth<'_>,
    expected_activation: Option<weft_core::ExecutionId>,
) -> Result<(), (StatusCode, String)> {
    let source_version = super::versions::record_program_source(state, birth.project_id, birth.program).await?;
    birth.source_version = Some(&source_version);
    start_queued_execution_with(state, birth, &[], expected_activation, None).await
}

/// THE one way a queued execution is born: its birth rows in one
/// transaction with its execute task. `extra_rows` are birth facts
/// beyond the kicks (a scoped run's provided values as `PortEmitted
/// { provided: true }`), written right after them. `live_connection` is
/// set only when this run FIRES a caller trigger: the request it serves
/// and the body that stands in for a socket nobody opened.
pub(crate) async fn start_queued_execution_with(
    state: &DispatcherState,
    birth: Birth<'_>,
    extra_rows: &[weft_journal::ExecEvent],
    expected_activation: Option<weft_core::ExecutionId>,
    live_connection: Option<weft_task_store::kinds::LiveConnectionStart>,
) -> Result<(), (StatusCode, String)> {
    let tenant = state
        .tenant_router
        .tenant_for_project(birth.project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let task = crate::task_kinds::execute::execution_task_spec(crate::task_kinds::execute::ExecutionTask {
        kind: weft_task_store::TaskKind::Execute,
        project_id: birth.project_id,
        execution_id: birth.execution_id,
        definition_hash: &birth.program.definition_hash,
        binary_hash: &birth.program.binary_hash,
        tenant_id: tenant.as_str(),
        run_class: birth.run_class,
        pinned_to: None,
        live_connection,
        unrecorded_birth: None,
    })
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("execute task spec: {e}")))?;
    let (start, mut kick_events) = execution_birth_events(birth);
    kick_events.extend_from_slice(extra_rows);
    state
        .journal
        .start_execution(&start, &kick_events, task, expected_activation)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("start execution: {e}")))?;
    Ok(())
}

/// Everything an execution is born with. Every start path (manual run,
/// setup phases, entry-trigger fire, live-trigger fire) fills one, so a
/// field added to the birth has one home.
pub(crate) struct Birth<'a> {
    pub execution_id: weft_core::ExecutionId,
    pub project_id: uuid::Uuid,
    pub phase: weft_core::context::Phase,
    pub entry_node: &'a str,
    pub kicks: &'a [Kick],
    pub program: &'a weft_core::project::hash::ProgramIdentity,
    /// The run's subgraph, journaled so the engine dispatches nothing
    /// outside it and a resume rebuilds the same boundary. Every trigger
    /// fire, targeted manual run and setup phase carries one (a setup
    /// phase's is `RunSelection::setup` over its triggers or infra
    /// nodes, and the engine refuses a setup row without it); `None`
    /// only for a manual run of the whole graph.
    pub subgraph: Option<&'a weft_core::project::selection::RunSelection>,
    /// The run this one inherits from (`weft run --seed`); `None` for a
    /// run from nothing, which is every fire and every setup phase.
    pub seed: Option<weft_journal::Seed>,
    pub source_version: Option<&'a str>,
    /// Who the run is for and what they provide; `None` for a run for
    /// nobody in particular.
    pub member: Option<RunFor<'a>>,
    /// The install's picks for the run's connections (`picks_for_run`).
    pub picks: &'a weft_core::picks::Picks,
    /// The trigger whose firing starts this run, spelled; `None` for a
    /// run started by hand and for a setup run.
    pub fired_trigger: Option<&'a str>,
    /// What the run is: a recorded project run, or one whose rows stay in
    /// the worker's memory (a trigger set not to record its runs).
    pub run_kind: weft_core::exec::RunKind,
    /// How long the run may run: the starting signal's, or `weft run
    /// --long`'s.
    pub run_class: weft_core::run_class::RunClass,
    pub at_unix: u64,
}

/// The two event shapes that give an execution its identity: the one
/// `ExecutionStarted` and one `NodeKicked` per root. The COMMIT differs
/// per path (one transaction here, dedup-keyed writes in route_entry, an
/// admission transaction for a live fire); the events do not.
pub(crate) fn execution_birth_events(birth: Birth<'_>) -> (weft_journal::ExecEvent, Vec<weft_journal::ExecEvent>) {
    let Birth {
        execution_id, project_id, phase, entry_node, kicks, program, subgraph, seed, source_version, member, picks,
        fired_trigger, run_kind, run_class, at_unix,
    } = birth;
    let start = weft_journal::ExecEvent::ExecutionStarted {
        execution_id,
        project_id,
        entry_node: entry_node.to_string(),
        phase,
        definition_hash: Some(program.definition_hash.clone()),
        program: Some(program.clone()),
        source_version: source_version.map(str::to_string),
        run_kind,
        subgraph: subgraph.cloned(),
        seed,
        member: member.map(|m| m.member.clone()),
        member_values: Box::new(member.map(|m| m.values.clone()).unwrap_or_default()),
        picks: Box::new(picks.clone()),
        fired_trigger: fired_trigger.map(str::to_string),
        run_class,
        at_unix,
    };
    let kick_events = kicks
        .iter()
        .map(|kick| weft_journal::ExecEvent::NodeKicked {
            execution_id,
            node_id: kick.node.clone(),
            frames: kick.frames.clone(),
            firing: kick.firing,
            payload: kick.payload.clone(),
            port_snapshot: kick.port_snapshot.clone(),
            at_unix,
        })
        .collect();
    (start, kick_events)
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
    // member's triggers are checked against the member's copies when
    // they are activated).
    depends_on.iter().all(|id| {
        rows.iter().any(|r| {
            &r.node_id == id && r.member.is_none() && r.status == crate::infra_node::InfraNodeStatus::Running
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
    member: Option<&weft_core::member::MemberId>,
) -> Result<Vec<MissingCopy>, (StatusCode, String)> {
    // Per PLACE, spelled: an infra node inside a file included twice is
    // two instances with two rows, and `within` names places the same
    // way, so a run cut to one call waits on that call's instance alone.
    // A per-member node is looked up in `member`'s copy: that is the
    // one the run or the member's triggers read.
    let mut missing: Vec<MissingCopy> = Vec::new();
    for place in weft_core::project::infra_places(project) {
        let spelled = weft_core::project::address_of(project, &place.id, &place.path);
        if within.is_some_and(|set| !set.contains(&spelled)) {
            continue;
        }
        let per_member = weft_core::project::is_per_member(project, &place.id);
        let copy = if per_member { member } else { None };
        if per_member && copy.is_none() {
            // Nobody named, so there is no copy to look at: activate's note
            // over the whole project lands here (it leaves these out), and
            // a run never does ([`require_run_infra`] refuses it first).
            missing.push(MissingCopy { place: spelled, member: None, needs_member: true });
            continue;
        }
        let row = crate::infra_node::get(&state.pg_pool, project_id, &spelled, copy)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_node: {e}")))?;
        let running = row
            .map(|r| r.status == crate::infra_node::InfraNodeStatus::Running)
            .unwrap_or(false);
        if !running {
            missing.push(MissingCopy { place: spelled, member: copy.cloned(), needs_member: false });
        }
    }
    Ok(missing)
}

/// One infra copy a run or a trigger needs and that is not running.
pub(crate) struct MissingCopy {
    /// The node, spelled.
    pub place: String,
    /// Whose copy: `None` for the shared one.
    pub member: Option<weft_core::member::MemberId>,
    /// The node exists once per member and no member was named.
    pub needs_member: bool,
}

impl std::fmt::Display for MissingCopy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (&self.member, self.needs_member) {
            (_, true) => write!(f, "{} (it exists once per member, and no member was named)", self.place),
            (Some(member), _) => write!(f, "{} (member '{member}')", self.place),
            (None, _) => f.write_str(&self.place),
        }
    }
}

/// The copies spelled for a message, and how to bring them up: the
/// shared ones with `weft infra start`, a member's with `--member` (or the
/// program's own `ctx.infra(..).member(..).start()`).
fn missing_and_fix(missing: &[MissingCopy]) -> (String, String) {
    let listed = missing.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ");
    let mut fixes: Vec<String> = Vec::new();
    if missing.iter().any(|m| m.member.is_none()) {
        fixes.push("`weft infra start`".to_string());
    }
    let mut members: Vec<&weft_core::member::MemberId> = missing.iter().filter_map(|m| m.member.as_ref()).collect();
    members.sort();
    members.dedup();
    for member in members {
        fixes.push(format!(
            "`weft infra start --member {member}` (or your program's ctx.infra(..).member(\"{member}\").start())"
        ));
    }
    (listed, fixes.join(" and "))
}

/// Who a run is for, and what they provide for its `@member_filled`
/// fields: the two travel together from the check that read the values
/// to the birth that journals them, so a run for a member can never be
/// born without the values that check approved.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunFor<'a> {
    pub member: &'a weft_core::member::MemberId,
    pub values: &'a weft_core::member::MemberValues,
}

/// What a run for `member` over `selection` starts with: the member's
/// stored values (with `overlay` made: the change a store call is about
/// to commit, which the setup it re-arms has to run with), checked
/// against the program by `weft_core::run_spec::member_run_values`.
/// Answers a 422 carrying the refusal, the same shape every run refusal
/// has, naming every field the member left unfilled or filled wrong.
pub(crate) async fn member_values_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
    member: &weft_core::member::MemberId,
    overlay: Option<&weft_core::member::ValueChanges>,
) -> Result<weft_core::member::MemberValues, (StatusCode, String)> {
    let stored = stored_member_values(state, project_id, member, overlay).await?;
    weft_core::run_spec::member_run_values(project, selection, member, &stored).map_err(|refusal| refusal_error(&refusal))
}

/// What `member` has stored, with `overlay` (a change about to be stored)
/// applied on top.
async fn stored_member_values(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    member: &weft_core::member::MemberId,
    overlay: Option<&weft_core::member::ValueChanges>,
) -> Result<weft_core::member::MemberValues, (StatusCode, String)> {
    let tenant = crate::member_values::owning_tenant(state, project_id).await?;
    let mut stored = weft_access_store::member_values(&state.pg_pool, &tenant, project_id, member)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read member values: {e:#}")))?;
    if let Some(change) = overlay {
        change.apply(&mut stored);
    }
    Ok(stored)
}

/// Why [`refuse_member_gaps`] refused a fired run.
pub(crate) enum RunGap {
    /// What the member provides: a field the run needs that they never
    /// gave, or a value the node's rules refuse. The refusal names each
    /// field; the member changing their values is what closes it.
    MemberValues(weft_core::run_spec::Refusal),
    /// Anything else: no member named, infra the run reads down, a read
    /// failing.
    Other((StatusCode, String)),
}

impl From<RunGap> for (StatusCode, String) {
    fn from(gap: RunGap) -> Self {
        match gap {
            RunGap::MemberValues(refusal) => refusal_error(&refusal),
            RunGap::Other(error) => error,
        }
    }
}

/// A run refusal as the API answers it: 422, the refusal as JSON.
pub(crate) fn refusal_error(refusal: &weft_core::run_spec::Refusal) -> (StatusCode, String) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        serde_json::to_string(refusal).expect("a Refusal serializes"),
    )
}

/// Everything a fired run needs about its member, checked before it is
/// born: [`require_run_infra`], then what that member provides filled and
/// valid at every `@member_filled` field it reaches. Answers the member's
/// values the run carries (empty for a run for nobody). The run door
/// (`api::versions::run`) makes the same two checks, each against the
/// selection it has at that point.
pub(crate) async fn refuse_member_gaps(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
    member: Option<&weft_core::member::MemberId>,
) -> Result<weft_core::member::MemberValues, RunGap> {
    require_run_infra(state, project_id, project, selection, member).await.map_err(RunGap::Other)?;
    match member {
        Some(member) => {
            let stored = stored_member_values(state, project_id, member, None).await.map_err(RunGap::Other)?;
            weft_core::run_spec::member_run_values(project, selection, member, &stored).map_err(RunGap::MemberValues)
        }
        None => Ok(Default::default()),
    }
}

/// THE infra gate on a run over `selection`, scoped to the places it
/// executes (spelled the way their infra rows are keyed): a run aimed at
/// part of the graph waits on that part's infra alone, a whole-graph run
/// on all of it, and a member's run on that member's copies of the
/// per-member ones. A run that reaches something per member and names
/// nobody is told so first: which member's copies to check is what it
/// has not said.
pub(crate) async fn require_run_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
    member: Option<&weft_core::member::MemberId>,
) -> Result<(), (StatusCode, String)> {
    weft_core::run_spec::refuse_memberless(project, selection, member).map_err(|refusal| refusal_error(&refusal))?;
    let within: HashSet<String> = selection
        .nodes
        .iter()
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    let missing = missing_infra_nodes(state, project_id, project, Some(&within), member).await?;
    if !missing.is_empty() {
        return Err((StatusCode::PRECONDITION_REQUIRED, infra_not_running(&missing)));
    }
    Ok(())
}

/// Why a run cannot start while infra it reads is down, and how to bring
/// it up.
pub(crate) fn infra_not_running(missing: &[MissingCopy]) -> String {
    let (listed, fix) = missing_and_fix(missing);
    format!("infra not running for: {listed}. Run {fix} first.")
}

/// START the InfraSetup sub-execution for every `requires_infra` node
/// in the project: journal `ExecutionStarted` + the upstream-closure
/// root kicks (so programmatic-infra patterns, text → compute →
/// infra, flow values into the infra body), then enqueue the execute
/// task. Returns `None` when the project has no infra nodes (nothing
/// to provision). Split from `await_infra_setup` so the caller (sync)
/// can perform the start under the short per-project transition lock
/// (serializing the flip) and do the unbounded wait OUTSIDE it.
pub async fn start_infra_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: a member's copies of the per-member nodes, or the
    // shared nodes.
    member: Option<&weft_core::member::MemberId>,
    // Only these nodes (places); every one of the owner's kind when empty.
    nodes: &[String],
) -> Result<Option<InfraSetupRun>, (StatusCode, String)> {
    // One coherent (hash, shape) pair: the kicks below and the
    // journaled/enqueued hash must come from the SAME definition (see
    // `coherent_definition`).
    let (program, project) = coherent_definition(state, project_id).await?;
    let targeted = crate::api::infra::resolve_infra_nodes(&project, nodes, member)?;
    let places: Vec<Located> = targeted
        .iter()
        .map(|spelled| {
            let (id, path) = weft_core::project::resolve_address(&project, spelled);
            Located::new(id, path)
        })
        .collect();
    let selection = weft_core::project::selection::RunSelection::setup(&project, &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let kicks = setup_kicks(&project, places).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    if kicks.is_empty() {
        return Ok(None);
    }
    // A member's copy is set up from what reaches it, which may be what
    // that member provides.
    let member_values = match member {
        Some(member) => member_values_for_run(state, project_id, &project, &selection, member, None).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, &project, &selection).await?;
    let execution_id = uuid::Uuid::new_v4();

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to the
    // completion event.
    let events = state.events.subscribe_project(project_id).await;

    // Infra reconciliation already holds the project transition lock.
    let source_version = super::versions::record_program_source_locked(state, project_id, &program).await?;
    let entry_node = kick_place(&project, &kicks[0]);
    start_queued_execution_with(
        state,
        Birth {
            execution_id,
            project_id,
            phase: weft_core::context::Phase::InfraSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            program: &program,
            subgraph: Some(&selection),
            seed: None,
            source_version: Some(&source_version),
            member: member.map(|member| RunFor { member, values: &member_values }),
            picks: &picks,
            fired_trigger: None,
            run_kind: weft_core::exec::RunKind::Execution,
            run_class: weft_core::run_class::RunClass::Short,
            at_unix: crate::lease::now_unix() as u64,
        },
        &[],
        None,
        // An infra setup answers nobody.
        None,
    )
    .await?;
    Ok(Some(InfraSetupRun { execution_id, events, project_id }))
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

/// Wait for a started InfraSetup sub-execution to settle.
pub(crate) async fn await_infra_setup(
    state: &DispatcherState,
    run: InfraSetupRun,
) -> Result<(), SyncNotLanded> {
    let InfraSetupRun { execution_id, mut events, project_id } = run;
    // Wait for completion. NO deadline: the InfraSetup execution runs
    // user-authored upstream nodes (text -> compute -> infra), and
    // user code may legitimately be slow; a hard cap would refuse
    // legitimate provisioning. Instead, a periodic breadcrumb keeps
    // the stuck-state legible in the dispatcher logs, and the user
    // can always cancel the execution (`weft stop`) to unblock.
    let started = std::time::Instant::now();
    let mut breadcrumb = tokio::time::interval(std::time::Duration::from_secs(30));
    breadcrumb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    breadcrumb.tick().await; // the first tick fires immediately; skip it
    loop {
        tokio::select! {
            _ = breadcrumb.tick() => {
                // The journal is authoritative, and the bus is not: a
                // terminal event raised while nothing was subscribed
                // (this waiter replacing an earlier one, a dispatcher
                // restart mid-wait) never reaches the arm below, and
                // waiting on it is waiting for a message that has
                // already been and gone. That is how a start left
                // behind by an interrupted one wedged a project for
                // ever: the run was over, and the only thing that did
                // not know was the waiter.
                match crate::api::execution::terminal_outcome(&state.pg_pool, execution_id).await {
                    Ok(Some(crate::api::execution::TerminalOutcome::Completed)) => return Ok(()),
                    Ok(Some(crate::api::execution::TerminalOutcome::Cancelled)) => {
                        return Err(SyncNotLanded::Cancelled("infra setup cancelled".into()))
                    }
                    Ok(Some(_)) => {
                        return Err(SyncNotLanded::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "infra setup failed".into(),
                        ))
                    }
                    // Genuinely still in flight, or the lookup itself
                    // failed (which is not this wait's to report: the
                    // breadcrumb below keeps the state legible and the
                    // next tick tries again).
                    Ok(None) | Err(_) => {}
                }
                tracing::info!(
                    target: "weft_dispatcher::infra_setup",
                    project_id = %project_id,
                    execution_id = %execution_id,
                    elapsed_secs = started.elapsed().as_secs(),
                    "infra setup still running; waiting on the InfraSetup execution \
                     (cancel it with `weft stop` to unblock)"
                );
            }
            res = events.recv() => {
                match res.map(|record| record.event) {
                    Ok(crate::events::DispatcherEvent::ExecutionCompleted { execution_id: c, .. })
                        if c == execution_id => return Ok(()),
                    Ok(crate::events::DispatcherEvent::ExecutionFailed { execution_id: c, error, .. })
                        if c == execution_id => {
                        return Err(SyncNotLanded::Failed(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("infra setup failed: {error}"),
                        ));
                    }
                    Ok(crate::events::DispatcherEvent::ExecutionCancelled { execution_id: c, reason, .. })
                        if c == execution_id => {
                        // The user's infra-cancel (or a per-execution stop)
                        // interrupted the provisioning execution.
                        return Err(SyncNotLanded::Cancelled(format!("infra setup cancelled: {reason}")));
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // The bus dropped a batch that may have held this
                        // execution's terminal event. The journal is
                        // authoritative: re-query it rather than wait
                        // blind (which would spuriously time out even
                        // though infra setup already finished).
                        match crate::api::execution::terminal_outcome(&state.pg_pool, execution_id).await {
                            Ok(Some(crate::api::execution::TerminalOutcome::Completed)) => {
                                return Ok(())
                            }
                            Ok(Some(crate::api::execution::TerminalOutcome::Cancelled)) => {
                                return Err(SyncNotLanded::Cancelled("infra setup cancelled".into()))
                            }
                            Ok(Some(_)) => {
                                return Err(SyncNotLanded::Failed(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "infra setup failed".into(),
                                ))
                            }
                            Ok(None) => {} // still in flight; keep waiting
                            Err(e) => {
                                return Err(SyncNotLanded::Failed(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("infra setup terminal lookup: {e}"),
                                ))
                            }
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

/// What one trigger fire runs: the roots to kick and the node set they
/// were computed from, the fire's PROGRAM. The set is journaled on
/// `ExecutionStarted` as the execution's subgraph, so the engine holds
/// the run to it and absorbs, silently, a pulse into anything outside.
/// Both come out of one `RunSubgraph`, so "what runs" and "what gets
/// kicked" cannot disagree.
///
/// Why the boundary matters for a fire: emission is scope-blind, so a
/// node shared by two programs in one file (a database, a provider)
/// pushes a pulse into the OTHER program's consumers too. Unbounded,
/// those consumers hold a partial input set forever and the run ends
/// Stuck after all its real work completed. Bounded, they never appear.
#[derive(Debug)]
pub struct TriggerFire {
    pub kicks: Vec<Kick>,
    pub subgraph: weft_core::project::selection::RunSelection,
}

/// Kicks for a trigger fire.
///
/// Rule: from the FIRING trigger, walk downstream: everything it
/// reaches is the fire's. Then walk back up from all of that for what
/// it needs, treating every trigger node as a terminator. Triggers
/// themselves are included as kicks: the firing trigger carries the
/// payload and its setup-time port snapshot; any other trigger in the
/// subgraph is kicked payload-less, which the engine turns into "close
/// all its
/// output ports" (the skip cascade prunes its exclusive branches).
///
/// Why terminators: at fire time a trigger's outputs are the payload,
/// not a function of its inputs (its ports replay the setup-time
/// snapshot). Nodes that exist only to produce inputs for triggers
/// must not re-run every time the trigger fires. If a node also feeds
/// non-trigger paths that reach a targeted output, it re-runs via
/// those paths.
///
/// Why start from the fired trigger: an output with no path from it
/// (a sibling branch fed by another trigger or by static sources
/// alone) is someone else's work; this fire must not re-run it.
///
/// The trigger itself runs even when it has no downstream consumer.
pub fn compute_trigger_fire(
    project: &ProjectDefinition,
    firing_node_id: &str,
    payload: &Value,
    port_snapshot: Option<&Value>,
) -> Result<TriggerFire, String> {
    // All trigger nodes register signals during TriggerSetup; that set
    // is what fires route to. A fire names the trigger by its address
    // (`door`, or `one.door` for the `door` inside the file the site
    // `one` includes), which resolves to the node and the call path
    // its kick runs under.
    let (fired, path) = weft_core::project::resolve_address(project, firing_node_id);
    if !project.nodes.iter().any(|node| node.id == fired && node.features.is_trigger) {
        return Err(format!("'{firing_node_id}' is not a trigger"));
    }

    // Targets = everything the FIRED trigger reaches downstream (the
    // trigger itself included, so a trigger with nothing behind it
    // still fires and runs alone).

    // Upstream closure from all of that, stopping at triggers (include
    // the trigger but do not walk through its incoming edges). Only the
    // firing trigger's kick carries the wake payload and the snapshot.
    let selection = weft_core::project::selection::RunSelection::carve(project,
        &weft_core::project::selection::SelectionBounds {
            fire: Some(firing_node_id.into()), ..Default::default()
        })?;
    let kicks = Kick::for_selection(project, &selection, Some((&Located::new(fired, path), payload)), port_snapshot);
    Ok(TriggerFire { kicks, subgraph: selection })
}

#[derive(Debug, Serialize)]
pub struct ActivateResponse {
    pub urls: Vec<ActivationUrl>,
    /// The program's shared infra that is not running, and how to start
    /// it. Activating starts only what a trigger reads (and refuses when
    /// that is down), so a piece nothing in the program touches (a
    /// database only a website uses) would otherwise stay off unnoticed.
    // SYNC: infra_not_running <-> crates/weft-cli/src/commands/activate.rs run_inner
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infra_not_running: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ActivationUrl {
    pub node_id: String,
    pub url: String,
}

#[derive(Debug, Serialize)]
pub struct ProjectStatusResponse {
    pub id: uuid::Uuid,
    pub name: String,
    /// Raw status enum: "registered" | "activating" | "active" |
    /// "deactivating" | "inactive". Mirrors `project.status`.
    /// SYNC: ProjectStatus <-> crates/weft-broker-client/src/protocol.rs ProjectStatus, packages/weft-graph/src/protocol.ts projectStatus
    pub status: String,
    /// The build transition axis, orthogonal to `status`: "none" |
    /// "building" | "cancelling_build". While not "none", the only
    /// offered action is cancel_build.
    /// SYNC: ProjectTransition <-> crates/weft-dispatcher/src/project_store.rs ProjectTransition, packages/weft-graph/src/protocol.ts ProjectTransition, packages/weft-graph/src/status.ts VALID_TRANSITIONS
    pub transition: String,
    /// User-facing mode label derived from the lifecycle axes:
    /// "registered" | "active" | "deactivating" | "wipe" |
    /// "hibernate" | "park". The action bar reads this verbatim.
    /// The accepting/visible booleans the gate keys on are NOT
    /// exposed: the user-facing mode label is the only thing
    /// clients need; the booleans are an internal projection.
    pub mode: String,
    /// Unix-second deadline after which `accepting_fires=true`
    /// flips to refusal (hibernate's grace window). `None` outside
    /// hibernate. Surfaced so the action bar can render a countdown.
    pub fires_deadline_unix: Option<i64>,
    /// Count of running, non-suspended executions right now.
    /// Drives the deactivating-state UI: progress towards drain.
    pub running_count: usize,
    pub listener_running: bool,
    /// True when the program has a shared trigger (one not marked
    /// `@per_member`): what the action bar arms. Clients show the listening/activation status indicator ONLY when this
    /// is true: a project with no trigger has nothing to listen with, so no
    /// indicator (not even an off one). `listener_running` then says whether
    /// it is currently listening (live) vs registered-but-not-listening (off).
    pub has_triggers: bool,
    pub infra: Vec<ProjectInfraEntry>,
    pub executions: ProjectExecutionsSummary,
    /// True when project has any infra-typed nodes in its source.
    /// Used by clients to decide whether to even show the
    /// Start/Stop/Upgrade infra controls.
    pub has_infra: bool,
    /// True when live `infra_node` rows exist whose node is NOT in the
    /// current source (the user deleted the node while it was
    /// deployed). Never gates run/activate (the no-infra graph runs in
    /// the shared pool, unlinked); clients OR it into their
    /// infra-slot-visibility check so the controls for live infra
    /// never vanish (the never-lose-track guarantee).
    pub orphaned_infra: bool,
    /// Aggregate infra state across the project's infra nodes, one of nine values:
    /// "none" (no infra nodes defined), "running" (all up), "provisioning",
    /// "stopping", "terminating" (transitional), "stopped" (all down), "partial"
    /// (mixed), "flaky", "failed". Clients map these to a status glyph.
    /// SYNC: infra_rollup values <-> packages/weft-graph/src/status.ts (infra_rollup consumer)
    pub infra_rollup: String,
    /// An infra operation is in flight that the rollup cannot see yet
    /// (a claimed stop still draining, a provisioning run before any
    /// node row flips). The window where the rollup still reads
    /// "stopped" and every verb but cancel is already refused, so a
    /// client that renders buttons from the rollup alone offers one
    /// that can only fail.
    pub infra_busy: bool,
    /// Desired vs running source/infra-hash drift. Either bit is
    /// only meaningful when the caller passed the corresponding
    /// `desired_*_hash` query param.
    pub drift: ProjectDrift,
    /// Verbs the dispatcher will currently accept. Driven by the
    /// state machine: project status, infra state, drift bits, etc.
    /// Clients render the action bar from this list directly; no
    /// client-side state machine.
    pub available_actions: Vec<String>,
    /// Every member's copy of a per-member infra node, and its state.
    /// `infra` above lists the shared copies only.
    pub member_copies: Vec<MemberCopyEntry>,
    /// Every trigger activation, shared and per member: what is
    /// listening, for whom, and how what stopped went down. `status` and
    /// `mode` above are the aggregate over the shared ones.
    pub activations: Vec<ActivationEntry>,
    /// Counts of preserved state, for the reactivate-time prompt.
    pub preservation: PreservationCounts,
    /// The public entries whose limits refused calls in the last two
    /// minutes, so an author sees a limit acting. Empty when none did.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub limited: Vec<LimitedEntry>,
}

/// One public entry that refused calls recently, and by which limit.
#[derive(Debug, Serialize)]
pub struct LimitedEntry {
    /// The entry's node, as the program spells it.
    pub node: String,
    /// What refused: `this caller's calls per minute`, `this entry's
    /// calls per minute`, `this entry's runs at once`.
    pub limit: &'static str,
    pub refused: u64,
}

/// The project's entries that refused calls in this minute and the one
/// before.
async fn limited_entries(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<LimitedEntry>> {
    let entries: Vec<(String, String)> = sqlx::query_as(
        "SELECT token, node_id FROM signal \
         WHERE project_id = $1 AND surface_kind = 'public_entry' AND NOT is_resume",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    let now = crate::lease::now_unix();
    let mut out = Vec::new();
    for (token, node) in entries {
        for (reason, refused) in crate::entry_limits::recent_refusals(&state.pg_pool, &token, now).await? {
            out.push(LimitedEntry { node: node.clone(), limit: reason.describe(), refused });
        }
    }
    Ok(out)
}

/// One member's copy of an infra node in the status response.
// SYNC: MemberCopyEntry <-> packages/weft-graph/src/protocol.ts MemberCopyEntry
#[derive(Debug, Serialize, Deserialize)]
pub struct MemberCopyEntry {
    pub node: String,
    pub member: weft_core::member::MemberId,
    pub status: String,
}

/// One trigger activation in the status response.
// SYNC: ActivationEntry <-> packages/weft-graph/src/protocol.ts ActivationEntry, crates/weft-cli/src/commands/status.rs (the triggers lines)
#[derive(Debug, Serialize, Deserialize)]
pub struct ActivationEntry {
    pub trigger: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<weft_core::member::MemberId>,
    pub status: crate::activation_store::ProjectStatus,
    /// Where it stands as a person reads it: its status, or for an
    /// inactive one the way it went down.
    pub mode: weft_core::activation::ActivationMode,
    /// A member's fires parked because the member has not given (or gave
    /// an invalid) value they need: how many, and why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<weft_core::program::WaitingFires>,
}

#[derive(Debug, Default, Serialize)]
pub struct PreservationCounts {
    /// Total fires queued across every signal in the project. Sum of
    /// `jsonb_array_length(parked_fires)`. Entry triggers append one
    /// element per fire; resume signals at most one. Drives the
    /// "execute parked / drop parked / wipe" choice on reactivate.
    pub parked: usize,
    /// Resume signals whose `parked_fires` queue is empty: registered
    /// but no submission yet. Stay across the inactive window so the
    /// corresponding suspended execution can resume later. Entries
    /// have no equivalent state; an entry with an empty queue is just
    /// "registered, idle" and isn't preserved per se.
    pub suspended: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct ProjectDrift {
    /// "Infra is stale relative to source." Drives the Upgrade
    /// button. Computed from desired_infra_hash != running_infra_hash.
    pub infra_drift: bool,
    /// "Worker BINARY needs rebuilding." Computed from
    /// desired_binary_hash != running_binary_hash. Flips on engine /
    /// node-impl / type-set / weft.toml edits; the dialog before
    /// running asks the user about killing running executions when
    /// this drift is non-zero.
    pub binary_drift: bool,
    /// "Project SHAPE has changed." Computed from
    /// desired_definition_hash != running_definition_hash. A pure
    /// config / topology edit flips this without flipping
    /// `binary_drift`; the next execution picks up the new
    /// definition via the worker's broker fetch.
    pub definition_drift: bool,
    /// "The listeners fire an older program." The registrations pin
    /// the exact code and graph they were made against; a rebuild or a
    /// definition edit since then leaves them firing the old one until
    /// `weft resync`. The verb list offers `resync` on this bit, and a
    /// human reading `weft status` needs the reason spelled out too.
    pub activation_drift: bool,
}

#[derive(Debug, Serialize)]
pub struct ProjectInfraEntry {
    /// The instance's place, spelled the way the PROGRAM writes the
    /// node (`db`, or `one.db` inside the file the site `one` includes):
    /// what a person is shown (`weft status`, `weft infra status`), what
    /// the editor matches against the node under the calls it walked
    /// into, and what every per-node verb takes. One entry per
    /// instance, so a file included twice lists its infra twice.
    pub node: String,
    /// Infra node type (e.g. "whatsapp_bridge"). Sourced from the
    /// project definition so the extension can decorate the node
    /// without re-parsing the source.
    pub node_type: String,
    /// The copy's state (`provisioning`, `running`, `flaky`, `stopping`,
    /// `stopped`, `terminating`, `failed`), `not_started` for a place
    /// with no copy and nothing starting one, or `per_member` for a
    /// `@per_member` place, which has no shared copy.
    pub status: String,
    pub endpoint_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "failureStage")]
    pub failure_stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "failureMessage")]
    pub failure_message: Option<String>,
    /// For a `per_member` place: how many members have a copy (each is
    /// listed in `member_copies`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_copies: Option<usize>,
}

// SYNC: ProjectExecutionsSummary <-> packages/weft-graph/src/status.ts RawStatusPayload.executions
#[derive(Debug, Serialize)]
pub struct ProjectExecutionsSummary {
    pub total: usize,
    pub last_completed_at: Option<u64>,
    pub last_execution_id: Option<String>,
    pub last_status: Option<String>,
    /// Every execution running right now (suspended ones excluded),
    /// the same set `running_count` counts. The editor REPLACES its
    /// own running set with this on every status refresh: the live
    /// stream is the fast path, this is the reconciliation, so a
    /// terminal event lost to a dropped stream never leaves a Stop
    /// button on a run that ended.
    ///
    /// OLDEST FIRST, so the last one is the most recently started.
    /// That is part of the contract, not an accident: the editor's
    /// action bar follows "the latest run" and nothing else here says
    /// which that is. A queued run with no journal row yet sorts last,
    /// which is right, it is the newest thing in the list.
    ///
    /// Each carries its phase: the setup an `infra start` or an
    /// activation runs is running too, and the editor shows it as that
    /// verb working instead of as a run with a Stop button.
    pub running: Vec<RunningExecution>,
}

// SYNC: RunningExecution <-> packages/weft-graph/src/status.ts RunningExecution
#[derive(Debug, Serialize)]
pub struct RunningExecution {
    pub execution_id: String,
    pub phase: weft_core::context::Phase,
}

#[derive(Debug, Default, Deserialize)]
pub struct StatusQuery {
    /// Binary hash the CLI computed for the current build inputs.
    /// Compared against `project.running_binary_hash` to surface
    /// "the worker image needs rebuilding" drift.
    #[serde(default, rename = "desiredBinaryHash")]
    pub desired_binary_hash: Option<String>,
    /// The same build's binary hash with the whole catalog compiled in
    /// (`weft run --full`). A running image built either way is
    /// current: drift means the running hash matches neither.
    #[serde(default, rename = "desiredFullBinaryHash")]
    pub desired_full_binary_hash: Option<String>,
    /// Definition hash the CLI computed for the current canonical
    /// `ProjectDefinition`. Compared against
    /// `project.running_definition_hash` to surface "the project
    /// shape has changed" drift (a config / topology edit that
    /// hasn't been resynced into the running project).
    #[serde(default, rename = "desiredDefinitionHash")]
    pub desired_definition_hash: Option<String>,
    /// Infra hash the CLI computed for the current infra closure.
    /// Compared against `project.running_infra_hash` for the upgrade
    /// drift signal.
    #[serde(default, rename = "desiredInfraHash")]
    pub desired_infra_hash: Option<String>,
}

/// Aggregate view for `weft status`. Returns registration,
/// listener state, per-node infra state, a rollup of recent
/// executions, drift signals (when desired hashes are passed in
/// query params), and the list of currently-valid action verbs.
/// One response, no stitching required by the CLI.
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
    /// Triggers are on and the request did not say how they come down:
    /// 428 with [`weft_core::TRIGGER_CHOICE_REQUIRED_HEADER`], so a client
    /// tells it apart from the 428 for infra that is not running and asks
    /// the person (or names the flags) instead of failing on it.
    NeedsTriggerChoice(String),
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
            StatusError::NeedsTriggerChoice(msg) => (StatusCode::PRECONDITION_REQUIRED, msg),
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
            StatusError::NeedsTriggerChoice(msg) => (
                StatusCode::PRECONDITION_REQUIRED,
                [(weft_core::TRIGGER_CHOICE_REQUIRED_HEADER, "triggerDeactivation")],
                msg,
            )
                .into_response(),
            StatusError::Other(code, msg) => (code, msg).into_response(),
        }
    }
}

/// Ownership gate for marked handlers: `authorize_project` (the ONE
/// ownership lookup, existence-leak collapse included) with its 404
/// upgraded to `NotMyProject` so the response carries the
/// `x-weft-not-found: project` marker instead of a headerless 404 the
/// client cannot tell apart from a version-skew routing 404.
async fn authorize_project_marked(
    state: &DispatcherState,
    caller: &crate::tenant::TenantId,
    id: uuid::Uuid,
) -> Result<(), StatusError> {
    authorize_project(state, caller, id).await.map_err(|(status, msg)| {
        if status == StatusCode::NOT_FOUND {
            StatusError::NotMyProject
        } else {
            (status, msg).into()
        }
    })
}

pub async fn status(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    axum::extract::Query(query): axum::extract::Query<StatusQuery>,
) -> Result<Json<ProjectStatusResponse>, StatusError> {
    // The `x-weft-not-found: project` marker lets a client tell "no
    // project I may see under this id" apart from a version-skew
    // routing 404 it must bubble. We must NOT call
    // `authorize_project` first: it returns a headerless 404, which would
    // make the marker unreachable and a brand-new `weft run` could never
    // pass the gate. So resolve ownership through the marked gate.
    authorize_project_marked(&state, &caller.0, id).await?;
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

    let snapshot = gather_action_snapshot(&state, id, &project, None).await?;
    let (infra, member_copies) = infra_entries(&project, &snapshot.infra);
    let waiting = crate::api::signal::member_waits(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("waiting fires: {e:#}")))?;
    let has_infra = snapshot.has_infra;
    let has_triggers = snapshot.has_triggers;
    let infra_rollup = snapshot.infra_rollup.clone();

    // Project-filtered, newest first: `total` is the true count of the project's
    // executions (from SQL, not capped at a fetched window) and the first row is
    // the latest for the `last_*` fields.
    let execs = state
        .journal
        .list_executions(
            caller.0.as_str(),
            &crate::journal::ExecutionQuery {
                limit: 1,
                offset: 0,
                project_id: Some(id),
                started_after: None,
                started_before: None,
                phase: None,
                entry_node: None,
                status: None,
                member: None,
                tag: None,
                below: None,
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let last = execs.executions.first();
    let (running, _) = running_execution_ids(&state, id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_execution_ids: {e}")))?;
    // Oldest first, so the LAST is the most recently started. The
    // editor's action bar follows "the latest run" and nothing else on
    // the wire says which that is; sorting these by id, as this used
    // to, made "latest" mean whichever uuid happened to sort highest.
    let running: Vec<RunningExecution> = running
        .into_iter()
        .map(|(execution_id, phase)| RunningExecution { execution_id: execution_id.to_string(), phase })
        .collect();
    let executions = ProjectExecutionsSummary {
        total: execs.total as usize,
        last_completed_at: last.and_then(|l| l.completed_at),
        last_execution_id: last.map(|l| l.execution_id.to_string()),
        last_status: last.map(|l| l.status.clone()),
        running,
    };

    let binary_hash = state
        .projects
        .running_binary_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_binary_hash: {e}")))?;
    let definition_hash = state
        .projects
        .running_definition_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_definition_hash: {e}")))?;
    let infra_hash = state
        .projects
        .running_infra_hash(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_infra_hash: {e}")))?;
    let mut drift = compute_drift(
        &query,
        binary_hash.as_deref(),
        definition_hash.as_deref(),
        infra_hash.as_deref(),
    );
    // The listeners fire an older program when any live activation, the
    // program's own or a member's, was set up on code other than what is
    // registered now: a plain `weft resync` brings every one of them up.
    drift.activation_drift = snapshot
        .activations
        .iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .any(|a| activation_has_drifted(&query, binary_hash.as_deref(), definition_hash.as_deref(), a.program.as_ref(), drift.definition_drift));
    let available_actions = compute_available_actions(&ActionInputs {
        lifecycle: &snapshot.lifecycle,
        transition: snapshot.transition,
        has_triggers,
        has_infra,
        trigger_infra_ready: snapshot.trigger_infra_ready,
        run_infra_ready: run_infra_ready(has_infra, &infra_rollup),
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup: &infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: &drift,
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });

    Ok(Json(ProjectStatusResponse {
        id,
        name: summary.name,
        status: snapshot.lifecycle.status.as_str().to_string(),
        transition: snapshot.transition.as_str().to_string(),
        mode: snapshot.lifecycle.mode().as_str().to_string(),
        fires_deadline_unix: snapshot.lifecycle.fires_deadline_unix,
        running_count: snapshot.running_count,
        listener_running,
        has_triggers,
        infra,
        executions,
        has_infra,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: ProjectDrift {
            infra_drift: drift.infra_drift,
            binary_drift: drift.binary_drift,
            definition_drift: drift.definition_drift,
            activation_drift: drift.activation_drift,
        },
        available_actions,
        preservation: snapshot.preservation,
        member_copies,
        activations: snapshot
            .activations
            .iter()
            .map(|a| ActivationEntry {
                trigger: a.key.trigger.clone(),
                member: a.key.member().cloned(),
                status: a.lifecycle.status,
                mode: a.lifecycle.mode(),
                waiting: waiting.get(&a.key).cloned(),
            })
            .collect(),
        limited: limited_entries(&state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit refusals: {e}")))?,
    }))
}

/// The status response's two infra lists, from every copy as the readers
/// show it: one entry per infra place the program declares (a shared one
/// with its copy's state, `not_started` when it has none; a `@per_member`
/// one as `per_member` with its members' copies counted), and every
/// member's copy on its own. A copy whose place the source no longer
/// declares (an orphan) is left out of both: it is counted project-wide
/// (`orphaned_infra`, the rollup) so the infra controls never vanish while
/// it lives.
fn infra_entries(
    project: &ProjectDefinition,
    copies: &crate::infra_node::ObservedCopies,
) -> (Vec<ProjectInfraEntry>, Vec<MemberCopyEntry>) {
    let mut member_copies: Vec<MemberCopyEntry> = copies
        .rows
        .iter()
        .filter_map(|row| Some((row.node_id.clone(), row.member.clone()?, row.status)))
        .chain(copies.starting.iter().filter_map(|(node, member)| {
            Some((node.clone(), member.clone()?, crate::infra_node::InfraNodeStatus::Provisioning))
        }))
        .map(|(node, member, status)| MemberCopyEntry { node, member, status: status.as_str().to_string() })
        .collect();
    member_copies.sort_by(|a, b| (&a.node, &a.member).cmp(&(&b.node, &b.member)));
    let mut infra = Vec::new();
    for spelled in weft_core::project::infra_place_spellings(project) {
        let (node_id, _) = weft_core::project::resolve_address(project, &spelled);
        let Some(node) = project.nodes.iter().find(|n| n.id == node_id && n.requires_infra) else {
            continue;
        };
        let entry = |status: String| ProjectInfraEntry {
            node: spelled.clone(),
            node_type: node.node_type.clone(),
            status,
            endpoint_url: None,
            failure_stage: None,
            failure_message: None,
            member_copies: None,
        };
        if node.per_member.is_some() {
            infra.push(ProjectInfraEntry {
                member_copies: Some(member_copies.iter().filter(|c| c.node == spelled).count()),
                ..entry(INFRA_PER_MEMBER.to_string())
            });
            continue;
        }
        match copies.rows.iter().find(|r| r.node_id == spelled && r.member.is_none()) {
            Some(row) => infra.push(ProjectInfraEntry {
                // Coarse UI hint: first endpoint by name (BTreeMap = stable).
                endpoint_url: row.install_endpoints.values().next().cloned(),
                failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
                failure_message: row.failure_message.clone(),
                ..entry(row.status.as_str().to_string())
            }),
            None => infra.push(entry(match copies.status_of(&spelled, None) {
                Some(status) => status.as_str().to_string(),
                None => INFRA_NOT_STARTED.to_string(),
            })),
        }
    }
    (infra, member_copies)
}

/// A shared infra place with no copy and nothing starting one, in the
/// status response.
// SYNC: INFRA_NOT_STARTED <-> packages/weft-graph/src/protocol.ts InfraInstanceStatus.status, crates/weft-cli/src/commands/status.rs
pub(crate) const INFRA_NOT_STARTED: &str = "not_started";
/// A `@per_member` infra place in the status response: it has no shared
/// copy, only its members'.
// SYNC: INFRA_PER_MEMBER <-> packages/weft-graph/src/protocol.ts InfraInstanceStatus.status, crates/weft-cli/src/commands/status.rs
pub(crate) const INFRA_PER_MEMBER: &str = "per_member";

/// Every reconciliation input gathered from live state, shared by the
/// status handler (renders the list) and `require_action` (enforces
/// against the same list). One producer so the two can never disagree.
pub(crate) struct ActionSnapshot {
    /// The project's shared activations as one lifecycle
    /// (`activation_store::aggregate`), or the named scope's when the
    /// snapshot was taken for a verb aimed at some triggers.
    pub lifecycle: crate::activation_store::ActivationLifecycle,
    /// Every activation row, shared and per member.
    pub activations: Vec<crate::activation_store::Activation>,
    pub transition: crate::project_store::ProjectTransition,
    pub has_triggers: bool,
    pub has_infra: bool,
    /// Every shared infra node the project's shared TRIGGERS depend on is
    /// Running, walked from the REGISTERED definition (see
    /// `ActionInputs::trigger_infra_ready` for what reads it).
    pub trigger_infra_ready: bool,
    pub orphaned_infra: bool,
    pub infra_rollup: String,
    pub infra_busy: bool,
    /// Every copy, as every status reader shows it.
    pub infra: crate::infra_node::ObservedCopies,
    pub preservation: PreservationCounts,
    pub running_count: usize,
}

pub(crate) async fn gather_action_snapshot(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    scope: Option<&weft_core::activation::ActivationScope>,
) -> Result<ActionSnapshot, (StatusCode, String)> {
    let activations = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    // What the verb is aimed at: the scope's activations, or every
    // shared one (what the action bar acts on). A scope that no longer
    // resolves against the registered definition (the source moved on)
    // is judged by what is recorded under its owner, and the verb's own
    // resolve against the built definition refuses it properly.
    let in_scope = |key: &weft_core::activation::ActivationKey| match scope {
        None => key.owner == weft_core::member::Owner::Shared,
        Some(scope) => match scope.resolve(project) {
            Ok(keys) => keys.contains(key),
            Err(_) => key.owner == scope.owner(),
        },
    };
    let lifecycle = crate::activation_store::aggregate(
        activations.iter().filter(|a| in_scope(&a.key)).map(|a| &a.lifecycle),
    );
    let transition = state
        .projects
        .transition(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("transition: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    // Every copy as every reader shows it: a start, stop or terminate
    // under way reads as such before the supervisor reaches the row.
    let observed = crate::infra_node::observe(&state.pg_pool, id, project)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("infra copies: {e:#}")))?;
    let infra_rows = &observed.rows;

    // Every SHARED infra place the source declares, spelled the way its
    // row is keyed (a file included twice declares two instances): the
    // project's infra, which the action bar starts and stops. A member's
    // copies are their program's to run; they count here only when they
    // are orphans, so live infra never vanishes from view.
    // SYNC: shared roles <-> packages/weft-graph/src/webview/lib/utils/node-roles.ts projectHasInfra, projectHasTriggers
    // SYNC: shared roles <-> crates/weft-compiler/src/weft_compiler.rs collect_included_contents
    let declared = weft_core::project::DeclaredInfra::of(project);
    let source_infra: std::collections::BTreeSet<String> = weft_core::project::infra_place_spellings(project)
        .into_iter()
        .filter(|n| declared.declares(n, false))
        .collect();
    let has_infra = !source_infra.is_empty();
    // The program's own triggers, which the action bar arms, and the
    // shared infra they read: a member's triggers read that member's
    // copies, which have no shared row, and are theirs to arm.
    let shared_triggers: Vec<Located> = weft_core::project::trigger_places(project)
        .into_iter()
        .filter(|place| !weft_core::project::is_per_member(project, &place.id))
        .collect();
    let has_triggers = !shared_triggers.is_empty();
    let shared_trigger_infra: std::collections::BTreeSet<String> =
        weft_core::project::selection::RunSelection::dependencies(project, &shared_triggers)
            .nodes
            .iter()
            .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
            .filter(|spelled| source_infra.contains(spelled))
            .collect();
    let trigger_infra_ready = trigger_infra_ready(&shared_trigger_infra, infra_rows);
    // Orphans: live rows whose node the user deleted from source.
    // They count into the rollup (below) and raise the project-level
    // signal, so a FULLY-orphaned live infra set never collapses the
    // rollup to `none` and never makes the infra controls vanish
    // (Model 1's never-lose-track guarantee).
    let is_orphan = |r: &crate::infra_node::InfraNodeRow| !declared.declares(&r.node_id, r.member.is_some());
    let orphan_count = infra_rows.iter().filter(|r| is_orphan(r)).count();
    let orphaned_infra = orphan_count > 0;

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
    let total = source_infra.len() + orphan_count;
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
        // One slot per source node (absent counted) + one per orphan
        // row (an orphan always HAS a row, by definition).
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
        for r in infra_rows.iter().filter(|r| is_orphan(r)) {
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
    // stop mid-drain; a provisioning execution before any row flips).
    let infra_busy = crate::infra_lifecycle_command::any_in_flight(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("commands in flight: {e}")))?
        || infra_setup_in_flight(state, id, None)
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}"))
            })?;

    let preservation = preservation_counts(state, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("preservation_counts: {e}")))?;
    let running_now = running_count(state, id, None)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}")))?;
    Ok(ActionSnapshot {
        lifecycle,
        activations,
        transition,
        has_triggers,
        has_infra,
        trigger_infra_ready,
        orphaned_infra,
        infra_rollup,
        infra_busy,
        infra: observed,
        preservation,
        running_count: running_now,
    })
}

/// Enforce a verb against the SAME reconciliation the status handler
/// renders: the greyed-out button prevents the common case, this
/// rejection is the race safety net (a stale tab whose bar hasn't
/// refreshed). `verbs` is the acceptable set for the handler (some
/// verbs are aliases of one endpoint, e.g. activate serves activate /
/// reactivate / resume_active). Drift bits are client-side facts, so
/// enforcement treats them as set (drift-gated verbs are never
/// spuriously rejected here; their own handlers validate further).
pub(crate) async fn require_action(
    state: &DispatcherState,
    id: uuid::Uuid,
    scope: Option<&weft_core::activation::ActivationScope>,
    verbs: &[&str],
) -> Result<(), (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let snapshot = gather_action_snapshot(state, id, &project, scope).await?;
    let allowed = compute_available_actions(&ActionInputs {
        lifecycle: &snapshot.lifecycle,
        transition: snapshot.transition,
        // Client-side fact, treated as satisfied (same posture as the
        // drift bits below): when the build happens at verb time the user
        // clicks activate on the SAVED source's triggers before a build
        // has re-registered them, so the REGISTERED definition's
        // has_triggers may lag. `activate_inner` re-checks honestly
        // against the built definition and 412s a genuinely
        // trigger-less project after its build.
        has_triggers: true,
        has_infra: snapshot.has_infra,
        // Same staleness posture, and for the same reason: this is
        // walked from the REGISTERED definition, which lags the source
        // the user just saved. Enforcing it here would refuse an
        // activate over an infra node the user has already deleted,
        // BEFORE the build that would have said so. `require_trigger_infra`
        // runs after the build and refuses against the fresh definition,
        // naming the nodes to start.
        trigger_infra_ready: true,
        // And the same for a run: the door is `versions::run`'s own
        // pre-flight, after the build, scoped to the places the run
        // executes. Enforcing the whole-graph fact here would refuse a
        // run aimed at nodes whose infra is up because some other node's
        // is down, which that run never touches.
        run_infra_ready: true,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup: &snapshot.infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift: &DriftBits { infra_drift: true, binary_drift: true, definition_drift: true, activation_drift: true },
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });
    if verbs.iter().any(|v| allowed.iter().any(|a| a == v)) {
        return Ok(());
    }
    Err((
        StatusCode::CONFLICT,
        format!(
            "'{}' is not available right now: the triggers it names are {} (transition {}, infra {}); \
             allowed actions: [{}]",
            verbs[0],
            snapshot.lifecycle.status.as_str(),
            snapshot.transition.as_str(),
            snapshot.infra_rollup,
            allowed.join(", "),
        ),
    ))
}

/// Count parked vs purely-suspended resume signals for a project.
/// Drives the reactivate-time prompt: caller decides whether to
/// ask the user "execute parked / keep suspended only / wipe all"
/// based on whether either count is non-zero.
async fn preservation_counts(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> anyhow::Result<PreservationCounts> {
    let (parked, suspended): (i64, i64) = sqlx::query_as(
        "SELECT \
            COALESCE(SUM(jsonb_array_length(parked_fires)), 0)::bigint AS parked, \
            COUNT(*) FILTER (WHERE is_resume = TRUE \
                              AND jsonb_array_length(parked_fires) = 0) AS suspended \
         FROM signal WHERE project_id = $1",
    )
    .bind(project_id)
    .fetch_one(&state.pg_pool)
    .await?;
    Ok(PreservationCounts {
        parked: parked as usize,
        suspended: suspended as usize,
    })
}

/// Pure drift comparison: desired (CLI's view) vs running (DB view).
/// Both sides compute the SAME hash function for each signal, so the
/// comparison is a string equality check.
fn compute_drift(
    query: &StatusQuery,
    running_binary_hash: Option<&str>,
    running_definition_hash: Option<&str>,
    running_infra_hash: Option<&str>,
) -> DriftBits {
    // Each drift bit is only meaningful when both sides have a
    // hash. No running hash means the project was never built /
    // activated; the action bar shouldn't surface drift then.
    let binary_drift = match (query.desired_binary_hash.as_deref(), running_binary_hash) {
        (Some(want), Some(have)) => {
            want != have && query.desired_full_binary_hash.as_deref() != Some(have)
        }
        _ => false,
    };
    let definition_drift = match (
        query.desired_definition_hash.as_deref(),
        running_definition_hash,
    ) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    let infra_drift = match (query.desired_infra_hash.as_deref(), running_infra_hash) {
        (Some(want), Some(have)) => want != have,
        _ => false,
    };
    DriftBits {
        activation_drift: false,
        infra_drift,
        binary_drift,
        definition_drift,
    }
}

/// Whether the listeners' registrations pin a program other than the
/// one the user is looking at. A registration pins the exact code
/// identity it was made from; a project activated before that identity
/// was recorded (`activation_program` is null) has only the graph hash
/// to go by, so it falls back to `definition_drift`.
fn activation_has_drifted(query: &StatusQuery, registered_binary: Option<&str>, registered_definition: Option<&str>, activated: Option<&weft_core::project::hash::ProgramIdentity>, definition_drift: bool) -> bool {
    let Some(activated) = activated else { return definition_drift };
    let desired_binary = query.desired_binary_hash.as_deref().or(registered_binary);
    let desired_definition = query.desired_definition_hash.as_deref().or(registered_definition);
    desired_definition != Some(activated.definition_hash.as_str())
        || (desired_binary != Some(activated.binary_hash.as_str())
            && query.desired_full_binary_hash.as_deref() != Some(activated.binary_hash.as_str()))
}

#[derive(Default, Clone, Copy)]
pub(crate) struct DriftBits {
    pub activation_drift: bool,
    pub infra_drift: bool,
    pub binary_drift: bool,
    pub definition_drift: bool,
}

/// Every input the reconciliation reads, gathered in one place so the
/// status handler (UI list) and verb enforcement (`require_action`)
/// feed the SAME pure function from the SAME facts. `drift` is the one
/// input only a client can supply (the desired hashes live on the
/// client); enforcement passes all-true so drift-gated verbs are never
/// spuriously rejected by the dispatcher.
pub(crate) struct ActionInputs<'a> {
    pub lifecycle: &'a crate::activation_store::ActivationLifecycle,
    pub transition: crate::project_store::ProjectTransition,
    /// Source declares a shared trigger (frontend-parse fact, derived
    /// here from the stored definition); a member's triggers are theirs.
    pub has_triggers: bool,
    /// Source declares any infra node. This is the SOURCE fact only;
    /// orphaned live infra does not count (Model 1).
    pub has_infra: bool,
    /// Every infra node the project's TRIGGERS DEPEND ON is running
    /// (vacuously true when they depend on none). Infra that only a
    /// RUN touches is gated at run time and does not hold an
    /// activation back. See `weft_core::project::infra_triggers_depend_on`.
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
    /// as `trigger_infra_ready`: what the table reports about run, from
    /// the registered definition, for the bar's unaimed Run button.
    /// `require_action` passes it satisfied, and `versions::run` does
    /// the honest check after the build, on the definition the run will
    /// execute and scoped to the places it executes: a run aimed at
    /// part of the graph waits on that part's infra alone.
    pub run_infra_ready: bool,
    /// Live `infra_node` rows exist whose node is NOT in the current
    /// source (the user deleted the node while it was deployed). Does
    /// NOT gate run/activate; DOES keep the infra controls offered so
    /// the user never loses track of live infra.
    pub orphaned_infra: bool,
    pub infra_rollup: &'a str,
    /// An infra operation is in flight: an uncompleted lifecycle
    /// command (apply / stop / terminate) or a running InfraSetup
    /// provisioning execution. Transitional per the master rule even
    /// BEFORE the supervisor flips any node status (the window where
    /// the rollup alone would still read as stable), so a drain in
    /// progress is never starved by new runs.
    pub infra_busy: bool,
    pub drift: &'a DriftBits,
    pub preservation: &'a PreservationCounts,
    pub running_count: usize,
}

/// THE reconciliation table (`docs/project-lifecycle-state-model.md`
/// §8) as a pure function: verbs the dispatcher will currently accept,
/// computed from the concern-lifecycles + source facts, never stored.
/// Clients render the action bar from this list directly AND the verb
/// handlers enforce against the same list, so the button state and the
/// enforcement cannot disagree.
///
/// The master rule collapses the table: in ANY transitional state
/// (building / cancelling_build / activating / deactivating / infra
/// provisioning / stopping / terminating) the only offered action is
/// the matching CANCEL. Stable states then enumerate:
///
///   - run           : allowed iff the source's infra (if any) is
///                     running. Never gated on a build (run builds as
///                     step 0) and never gated on an orphan (a
///                     no-infra graph runs in the shared pool,
///                     unlinked). Allowed while Active (manual run
///                     alongside live triggers; the bar may not show
///                     a button, the backend permits it).
///   - activate /
///     reactivate    : Registered/Inactive, source has triggers, and
///                     every infra node the TRIGGERS DEPEND ON running
///                     (infra only a run touches is gated at run time
///                     and does not hold activation back; the door
///                     re-checks this against the definition it
///                     builds). Reactivate variant when preserved
///                     state exists.
///   - deactivate    : Active. NOT gated on has_triggers: deleting
///                     the last trigger from source while active must
///                     keep Deactivate offered (trigger divergence).
///   - resync        : activation drift, which counts only triggers
///                     that are on (the program's or a member's), so
///                     it does not wait on the shared status.
///                     Deactivate-then-reactivate under the hood;
///                     re-picks placement from the CURRENT source.
///                     Never touches orphaned infra.
///   - cancel_*      : the transitional rows.
///   - infra_*       : offered whenever live-or-declared infra exists
///                     (`has_infra || orphaned_infra`), per rollup.
///                     start/upgrade additionally need the SOURCE to
///                     declare infra (there is no spec to provision an
///                     orphan from); stop/terminate work on the live
///                     rows themselves, so they stay offered for a
///                     pure orphan (the never-lose-track guarantee).
// SYNC: compute_available_actions <-> packages/weft-graph/src/webview/lib/verb-gates.ts (the starter verbs the bar offers on top of this table), packages/weft-graph/src/protocol.ts ActionVerb
fn compute_available_actions(inputs: &ActionInputs<'_>) -> Vec<String> {
    use crate::project_store::ProjectStatus;

    // Master rule, build axis: a build in flight offers only its
    // cancel (idempotent while already cancelling).
    if inputs.transition.is_building() {
        return vec!["cancel_build".to_string()];
    }

    // Master rule, trigger axis.
    match inputs.lifecycle.status {
        ProjectStatus::Activating => {
            return vec!["cancel_activate".to_string()];
        }
        ProjectStatus::Deactivating => {
            // Mid-deactivate: give up on the wait (cancel_running,
            // the drain finishes immediately) or change your mind
            // (resume_active rolls forward into Active).
            let mut out = Vec::new();
            if inputs.running_count > 0 {
                out.push("cancel_running".to_string());
            }
            out.push("resume_active".to_string());
            return out;
        }
        ProjectStatus::Registered | ProjectStatus::Inactive | ProjectStatus::Active => {}
    }

    // Master rule, infra axis: either the derived rollup reports a
    // transitional state, or an infra operation is in flight that the
    // rollup can't see yet (a claimed stop still draining, a
    // provisioning execution before any node row flips). Only
    // meaningful when infra state exists at all.
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
    // touches must be running (a run would fail fetching infra outputs;
    // the user starts infra first, its own verb). What the table can
    // answer is the WHOLE-GRAPH run; a run aimed at part of the graph
    // waits on that part alone, which only the door can tell.
    if inputs.run_infra_ready {
        out.push("run".to_string());
    }

    match inputs.lifecycle.status {
        ProjectStatus::Active => {
            out.push("deactivate".to_string());
        }
        ProjectStatus::Registered | ProjectStatus::Inactive => {
            // A trigger whose infra is down cannot be armed: its
            // registration reads the address off the infra node feeding
            // it. Infra the trigger does not depend on is gated at run
            // time instead, so it does not hold the activation back.
            // The door's own copy of this is `require_trigger_infra`,
            // which runs after the build; this one is what the bar and
            // `weft status` are told.
            if inputs.has_triggers && inputs.trigger_infra_ready {
                let has_preserved =
                    inputs.preservation.parked + inputs.preservation.suspended > 0;
                if has_preserved && inputs.lifecycle.status == ProjectStatus::Inactive {
                    out.push("reactivate".to_string());
                } else {
                    out.push("activate".to_string());
                }
            }
        }
        ProjectStatus::Activating | ProjectStatus::Deactivating => {
            unreachable!("transitional statuses returned above")
        }
    }

    // Registrations pin both the graph and the worker code. Either change
    // needs resync before listeners can fire the new program. The drift
    // bit counts only activations that are on, the program's or a
    // member's, so it is offered whatever the shared status says: a
    // member's triggers can be on while the program's are off. Resync
    // re-arms exactly what activate arms, so it waits on the same infra:
    // the triggers' own, checked again by the door after its build
    // (`require_trigger_infra`).
    if inputs.drift.activation_drift && inputs.trigger_infra_ready {
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
                if can_provision && inputs.drift.infra_drift {
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
                if can_provision && inputs.drift.infra_drift {
                    out.push("infra_upgrade".to_string());
                }
            }
            // provisioning/stopping/terminating returned above.
            _ => {}
        }
    }

    out
}

/// Body for `POST /projects/{id}/activate`. Optional `binaryHash`
/// refreshes the running image-tag for the next worker spawn.
///
/// `reactivateChoice` matters only when there's preserved state
/// from a prior deactivate (status=Inactive AND any signal row
/// exists for the project). Three choices, applied as 1-line
/// pre-flight against the existing rows; the rest of the activate
/// path is identical regardless of choice.
///
///   - `execute_parked_keep_suspended` (default): no pre-flight.
///     The drain step at the end of activate replays every element
///     of every signal's `parked_fires` queue through
///     `dispatch_listener_outcome` (same chain a live fire takes).
///     Suspended rows whose queue is empty stay waiting.
///   - `keep_suspended_only`: clear `parked_fires` on every row
///     before draining, so the drain finds nothing to replay.
///     Suspended-but-not-yet-fired stay waiting.
///   - `wipe_all`: drop every signal row + cancel every execution
///     before TriggerSetup runs. Equivalent to having deactivated
///     with `wipe`; the project starts entirely fresh.
///
/// If the project has no preserved state (status=Registered, or
/// signals were already wiped), the choice is irrelevant and the
/// activate is a fresh boot.
///
/// `runningPolicy` / `drainTimeoutSecs` (the flattened
/// [`RunningChoice`]) say what happens to a worker built from an older
/// image than the one being activated (see `reconcile_worker`):
/// `cancel` (the default) cancels what it runs and replaces it now;
/// `wait` lets its in-flight work land first, up to the cap, then
/// replaces it. The CLI's `--running-policy` and `--drain-timeout` on
/// `weft activate`.
#[derive(Debug, Default, Deserialize)]
pub struct ActivateRequest {
    #[serde(flatten)]
    pub target: ActivationTarget,
    #[serde(flatten)]
    pub running: RunningChoice,
    /// Which activations: the triggers named (every one of the owner's
    /// when none is) for the member named (the shared ones when none
    /// is). What `weft activate --trigger --member` and a program's
    /// `ctx.trigger(..).member(..).activate()` send; the action bar
    /// sends nothing, which is every shared trigger.
    #[serde(default)]
    pub scope: weft_core::activation::ActivationScope,
}

/// What an activate points the project at: the hashes of the version
/// that built (absent when activating by id, which keeps the recorded
/// ones) and the answer to "what about the state a hibernated or
/// parked project kept". The half of an activate a resync carries
/// verbatim; the running-work half it takes from its own picker.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ActivationTarget {
    #[serde(default, rename = "binaryHash")]
    pub binary_hash: Option<String>,
    #[serde(default, rename = "definitionHash")]
    pub definition_hash: Option<String>,
    #[serde(default, rename = "infraHash")]
    pub infra_hash: Option<String>,
    #[serde(default, rename = "reactivateChoice")]
    pub reactivate_choice: Option<String>,
}

/// Body for `POST /projects/{id}/bake`: which triggers to prepare, and
/// what happens to a worker built from an older image first.
#[derive(Debug, Default, Deserialize)]
pub struct BakeRequest {
    #[serde(flatten)]
    pub running: RunningChoice,
    #[serde(default)]
    pub scope: weft_core::activation::ActivationScope,
}

/// The activations `scope` names in `project`, refusing a mismatch (a
/// per-member trigger without a member, a shared one with one, an
/// address that is not a trigger) and a scope that names nothing.
pub(crate) fn resolve_scope(
    project: &ProjectDefinition,
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let keys = scope.resolve(project).map_err(|why| (StatusCode::BAD_REQUEST, why))?;
    if keys.is_empty() && !has_triggers(project) {
        return Err((StatusCode::PRECONDITION_FAILED, "this program has no triggers".to_string()));
    }
    if keys.is_empty() {
        let what = match &scope.member {
            None => "no shared trigger (every trigger it has exists once per member; name one with --member)".to_string(),
            Some(member) => format!("no trigger that exists once per member, so there is nothing to activate for '{member}'"),
        };
        return Err((StatusCode::PRECONDITION_FAILED, format!("this program has {what}")));
    }
    Ok(keys)
}

/// The activations a verb that only takes triggers down acts on:
/// `scope`'s owner's rows among `rows`, every one of them, or the named
/// triggers'. Picked from the rows rather than the source, because what
/// is listening is what the rows say: a trigger the source has since
/// dropped, or a member's copy of one the program no longer runs per
/// member, still has a row, and taking it down is the only way to stop
/// it. A named trigger with no row for that owner is refused: nothing of
/// it is on, so the name is a mistake. No row at all is no keys (`weft rm
/// --journal` quiesces a trigger-less program through deactivate).
fn keys_to_take_down(
    rows: &[crate::activation_store::Activation],
    scope: &weft_core::activation::ActivationScope,
) -> Result<Vec<weft_core::activation::ActivationKey>, (StatusCode, String)> {
    let owner = scope.owner();
    let owned = rows.iter().map(|a| &a.key).filter(|key| key.owner == owner);
    if scope.triggers.is_empty() {
        return Ok(owned.cloned().collect());
    }
    let keys: Vec<weft_core::activation::ActivationKey> =
        owned.filter(|key| scope.triggers.contains(&key.trigger)).cloned().collect();
    if let Some(unknown) = scope.triggers.iter().find(|name| !keys.iter().any(|key| &&key.trigger == name)) {
        let named = weft_core::activation::ActivationKey::new(unknown.clone(), owner);
        return Err((
            StatusCode::NOT_FOUND,
            format!("trigger {named} was never activated, so there is nothing of it to take down"),
        ));
    }
    Ok(keys)
}

/// Whether the program has any trigger, shared or per member.
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

/// The owner the keys share: every verb acts on one owner's triggers at
/// a time.
fn keys_member(keys: &[weft_core::activation::ActivationKey]) -> Option<&weft_core::member::MemberId> {
    keys.first().and_then(|key| key.member())
}

/// Prepare trigger settings without changing what is listening. The
/// body names which triggers (every shared one by default) and the
/// [`RunningChoice`] for a worker built from an older image, the same
/// question an activate answers (the setup runs on a worker, so a
/// stale one is replaced first).
pub async fn bake(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<BakeRequest>>,
) -> Result<Json<crate::journal::TriggerBake>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let (program, project) = coherent_definition(&state, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let (running_policy, drain_timeout_secs) = body.running.resolve(None);
    let keys = resolve_scope(&project, &body.scope)?;
    prepare_trigger_setup(&state, id, &project, &keys, StaleWorkerChoice::Replace(running_policy, drain_timeout_secs)).await?;
    Ok(Json(run_trigger_setup(&state, id, &project, &keys, &program, None, None).await?))
}

/// Refuse unless every infra copy the named activations' triggers read
/// is Running (the member's own copy for a per-member one).
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
async fn require_trigger_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
) -> Result<(), (StatusCode, String)> {
    // Validation first: a trigger setup that cannot be selected at all
    // (a trigger inside a half-selected loop) is a 400 about the graph,
    // not a story about infra.
    let places = key_places(project, keys);
    let selection = weft_core::project::selection::RunSelection::setup(project, &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let within: HashSet<String> = selection
        .nodes
        .iter()
        .filter(|place| project.nodes.iter().any(|n| n.id == place.id && n.requires_infra))
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    let missing = missing_infra_nodes(state, project_id, project, Some(&within), keys_member(keys)).await?;
    if missing.is_empty() {
        return Ok(());
    }
    let (listed, fix) = missing_and_fix(&missing);
    Err((
        StatusCode::PRECONDITION_REQUIRED,
        format!("these triggers' infra is not running: {listed}. Start it with {fix}, then run this again."),
    ))
}

/// What a trigger setup does first to executions still running on an
/// older image.
enum StaleWorkerChoice {
    /// Settle them under the caller's running policy and drain cap (see
    /// [`reconcile_worker`]); the caller's choice, never a fixed one here,
    /// because a `wait` sits inside the request for as long as those
    /// executions run.
    Replace(crate::infra_lifecycle_command::RunningPolicy, u64),
    /// Leave them be and only ready the new image: the setup runs on it
    /// either way. A re-arm from a member's change of values takes this,
    /// because the change may come from one of those very runs, and
    /// waiting on it would wait on the run waiting for the change.
    Retire,
}

/// Who asks for an activate, which decides what happens to a worker
/// built from an older image.
pub(crate) enum ActivateAsker<'a> {
    /// A person or a lifecycle command: the stale worker is replaced
    /// under the request's running policy.
    Outside,
    /// A program's run (`ctx.trigger(..).activate()`): that run may sit
    /// on the stale worker, so waiting on or cancelling its work would
    /// wait on or cancel the asker. The worker is retired instead.
    Run,
    /// A change of a member's values re-arming their live triggers; it
    /// may come from a run too, so it retires the same way.
    Rearm(&'a Rearm<'a>),
}

/// Make the workers match what the triggers will need, and refuse first
/// if their infra is not up.
async fn prepare_trigger_setup(
    state: &DispatcherState,
    id: uuid::Uuid,
    project: &ProjectDefinition,
    keys: &[weft_core::activation::ActivationKey],
    stale: StaleWorkerChoice,
) -> Result<(), (StatusCode, String)> {
    require_trigger_infra(state, id, project, keys).await?;
    match stale {
        StaleWorkerChoice::Replace(running_policy, drain_timeout_secs) => {
            reconcile_worker(state, id, running_policy, drain_timeout_secs).await
        }
        StaleWorkerChoice::Retire => {
            let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running image: {e:#}"));
            match state.projects.running_binary_hash(id).await.map_err(internal)? {
                Some(hash) => prepare_running_image(state, id, &hash).await,
                None => Ok(()),
            }
        }
    }
}

pub async fn activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<ActivateRequest>>,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // Enforce against the reconciliation, over the activations this call
    // names: this one endpoint serves three table verbs (activate from
    // Registered/Inactive, reactivate when preserved state exists,
    // resume_active while Deactivating), so any of the three being
    // offered admits the call. Everything else (already Active,
    // transitional, infra not ready) rejects here.
    require_action(&state, id, Some(&body.scope), &["activate", "reactivate", "resume_active"]).await?;
    activate_inner(&state, id, body).await
}

/// How `activate_inner` must roll back a failed Activating-window.
/// Before `run_trigger_setup` starts, the activate touched no signal
/// state, so rollback puts each trigger back as the claim found it.
/// Once it starts, a fresh activation's signals may be half-registered
/// and are wiped; a re-arm first takes back what it armed (each row
/// written back and run again) and then restores, and wipes only when
/// that take-back failed.
enum ActivateRollback {
    /// Put every trigger back as the claim found it (a parked one
    /// parked, a live one listening as before, one never activated
    /// without a row) and cancel the setup run. Keeps existing signals: a
    /// failure before trigger-setup never touched them, and wiping would
    /// nuke prior suspended/parked work on a transient error. Also a
    /// re-arm of live triggers that failed before anything was stored (a
    /// re-arm stores its values only as the activation lands) and whose
    /// armed signals, if any, were all put back.
    Restore,
    /// Full cleanup: end the activation at Inactive, cancel its setup
    /// run, drop the signals its activations govern. Used once a fresh
    /// activation's trigger-setup started (signals are in flux), and for a
    /// re-arm whose armed signals could not all be put back.
    WipeSignals,
}

struct ActivateWindowError {
    status: StatusCode,
    msg: String,
    rollback: ActivateRollback,
}

/// The Activating-window of `activate_inner`: every step that runs
/// while the named activations are `Activating`, ending with the CAS to
/// `Active`. Factored out so `activate_inner` has ONE rollback site
/// for the whole window (a failure anywhere here must un-stick them;
/// the caller does that once on Err).
///
/// The error carries the rollback MODE (`ActivateRollback`): failures
/// before `run_trigger_setup` restore (keep signals). From trigger-setup
/// on, a fresh activation wipes (signals are in flux), and a re-arm takes
/// back what it armed and restores (`take_back_arms`).
///
/// Single-flight against a concurrent activate of the same triggers is
/// the exclusive Activating claim (`try_begin_activating`), not a lock.
async fn activate_trigger_setup_window(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    choice: &str,
    project: &ProjectDefinition,
    program: &weft_core::project::hash::ProgramIdentity,
    rearm: Option<&Rearm<'_>>,
    stale: StaleWorkerChoice,
) -> Result<Vec<ActivationUrl>, ActivateWindowError> {
    // Failures up to (not including) run_trigger_setup haven't touched
    // signal state, so they only need the triggers put back.
    let unstick = |(status, msg): (StatusCode, String)| ActivateWindowError {
        status,
        msg,
        rollback: ActivateRollback::Restore,
    };

    // A binary-hash change must kill the stale-image worker before the
    // activate's TriggerSetup exec runs, otherwise it's dispatched
    // against a worker that doesn't know about the new trigger nodes.
    // Idempotent (kill-by-binary-hash + respawn). It runs inside the
    // claim, so from the moment the verb is taken the triggers read
    // `activating` (a worker move can take a while), and a failure puts
    // them back as they were. The request's running policy says what
    // happens to the stale worker's executions (cancel unless the
    // caller asked to wait); either way the replacement is loud, never a
    // silent kill.
    //
    // This also carries the infra precondition, and it runs FIRST: an
    // activation whose triggers need infra that is down is refused here,
    // against the definition this activation just built, before any
    // worker work happens. An activate a run asked for only retires the
    // stale worker (see `ActivateAsker`).
    prepare_trigger_setup(state, id, project, keys, stale).await.map_err(unstick)?;

    // Apply the reactivate choice's destructive effect now that we
    // hold the exclusive Activating claim (validated by caller).
    apply_reactivate_choice(state, id, keys, activation, choice).await.map_err(unstick)?;

    // Capture first, then arm that completed capture. Arming refreshes
    // existing entry rows while retaining their tokens and parked fires.
    // A re-arm sets its live triggers up again with the values it stores
    // as it lands; until it arms, nothing listening has changed, so a
    // failure puts the triggers back as they were. A trigger that fails
    // to arm takes back the ones armed before it (each row written back
    // and the listener running it again), so a re-arm still ends with
    // its triggers exactly as they were.
    let before_arming = if rearm.is_some() { ActivateRollback::Restore } else { ActivateRollback::WipeSignals };
    let bake = run_trigger_setup(state, id, project, keys, program, Some(activation), rearm.and_then(|r| r.overlay))
        .await
        .map_err(|(status, msg)| ActivateWindowError { status, msg, rollback: before_arming })?;
    let recorded = state
        .activations
        .record_activation_source(id, activation, &bake.program, &bake.source_version)
        .await
        .map_err(|e| unstick((StatusCode::INTERNAL_SERVER_ERROR, format!("record activation source: {e}"))))?;
    if !recorded {
        return Err(unstick((StatusCode::CONFLICT, format!("activation {activation} no longer owns its triggers"))));
    }
    let mut armed = Vec::new();
    for key in keys {
        // A trigger its setup skipped (behind a closed gate) arms nothing.
        let Some(capture) = bake.captured.get(&key.trigger) else { continue };
        let arming = crate::task_kinds::register_signal::RegisterSignalExecutor::arm(state,
            crate::task_kinds::register_signal::RegisterSignalPayload {
                execution_id: bake.execution_id.to_string(), node_id: key.trigger.clone(), frames: Vec::new(),
                spec: capture.spec.clone(), is_resume: false, call_index: 0,
                port_snapshot: Some(capture.ports.clone()),
                asked_at_unix_ms: chrono::Utc::now().timestamp_millis(),
            }).await;
        let error = match arming {
            Ok(done) => {
                armed.push(done);
                continue;
            }
            Err(error) => error,
        };
        return Err(take_back_arms(state, rearm.is_some(), armed, format!("arm trigger {key}: {error:#}")).await);
    }

    // Every step that can fail runs before the one delete (the orphan
    // rows below) except the flip to Active after it, so up to the delete
    // a re-arm can still be put back exactly as it was, and after it only
    // the orphan rows stay gone. The orphans are read first: the listener
    // is told to leave them down, since they are about to go and one that
    // cannot come up must not fail the activation deleting it.
    let orphans = match orphan_entry_tokens(state, id, project, keys_member(keys)).await {
        Ok(orphans) => orphans,
        Err(e) => return Err(take_back_arms(state, rearm.is_some(), armed, format!("read orphan entry rows: {e:#}")).await),
    };

    // Reconcile what the listener holds of this project with the durable
    // signal table (resume signals belong to suspended executions whose
    // workers are gone). /rehydrate is idempotent, and fails only on this
    // project's own rows.
    if let Err(e) = state.listener.rehydrate(id, &orphans).await {
        return Err(take_back_arms(state, rearm.is_some(), armed, format!("listener rehydrate: {e:#}")).await);
    }

    // The addresses the armed triggers listen at, read before the flip:
    // past it nothing may fail the call. The orphans are other triggers'
    // rows, so they are not among these keys'.
    let urls = match collect_listener_urls(state, id, keys).await {
        Ok(urls) => urls,
        Err(e) => return Err(take_back_arms(state, rearm.is_some(), armed, format!("collect_listener_urls: {e:#}")).await),
    };

    // Last, the one step that cannot be taken back once it lands: drop
    // this owner's entry rows for triggers the source no longer has (the
    // user edited it while they were down). The delete is one
    // transaction, and the listener is told to forget the rows only
    // after it commits, so a failure deletes nothing: every orphan row is
    // still there (held, left down by the rehydrate above) and a re-arm
    // is put back like any earlier failure.
    if let Err(e) = drop_orphan_entry_rows(state, id, &orphans, activation).await {
        let msg = format!("drop the entry rows of triggers the source no longer has ({} row(s), none deleted): {e:#}", orphans.len());
        return Err(take_back_arms(state, rearm.is_some(), armed, msg).await);
    }

    // Flip Activating → Active. The claim guards against a concurrent
    // cancel_activate that ended the activation: in that case the cancel
    // already wiped its signals, so we surrender with no further
    // rollback. The setup run is terminal by now (`run_trigger_setup`
    // returned on its terminal), so the execution the flip clears is only
    // bookkeeping. A re-arm's values are stored in the same transaction:
    // until the flip, the triggers' fires park (an activating trigger's
    // are not visible yet), and the drain after it routes them with the
    // values the setup ran with.
    // A flip that fails still leaves a re-arm's own triggers able to go
    // back; only the orphan rows stay gone, and those belong to triggers
    // the source no longer has.
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
        Err(e) => return Err(take_back_arms(state, rearm.is_some(), armed, format!("end_activating: {e:#}")).await),
    };
    if !ended {
        return Err(ActivateWindowError {
            status: StatusCode::CONFLICT,
            msg: "activate raced with cancel-activate; the triggers are now inactive".into(),
            rollback: ActivateRollback::Restore,
        });
    }
    Ok(urls)
}

/// In-process callable for `activate`: the axum handler, `resync`'s
/// reactivate step, the supervisor's auto-recover path
/// (`lifecycle_claimer`) and a program's `ctx.trigger(..).activate()`
/// all land here. Same body as the handler minus the extractor plumbing.
pub async fn activate_inner(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    activate_with(state, id, request, ActivateAsker::Outside).await.map_err(Into::into)
}

/// Why [`activate_with`] did not activate.
#[derive(Debug)]
pub(crate) enum ActivateError {
    /// The single-flight claim was refused, and why, as the claim's own
    /// transaction decided it.
    Refused(crate::activation_store::ClaimRefused),
    Failed(StatusCode, String),
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
                    ClaimRefused::Building => "the project is building; wait for it to finish or cancel it",
                    ClaimRefused::Claimed => "these triggers are already activating; wait for it to finish or cancel it",
                    ClaimRefused::NotExpected => "a trigger this re-arm read as on was taken down meanwhile",
                }
                .into(),
            ),
            ActivateError::Failed(code, message) => (code, message),
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
    binary: Option<&str>,
    definition: Option<&str>,
    infra: Option<&str>,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("running hashes: {e}"));
    let running = (
        state.projects.running_binary_hash(id).await.map_err(internal)?,
        state.projects.running_definition_hash(id).await.map_err(internal)?,
        state.projects.running_infra_hash(id).await.map_err(internal)?,
    );
    let differs = |named: Option<&str>, running: &Option<String>| named.is_some() && named != running.as_deref();
    if differs(binary, &running.0) || differs(definition, &running.1) || differs(infra, &running.2) {
        return Err((
            StatusCode::CONFLICT,
            "the project's build moved since this was prepared (another build registered after \
             it); run the command again"
                .into(),
        ));
    }
    Ok(())
}

/// A change of a member's values, and the member's live triggers it
/// re-arms (see `crate::member_values::change`): the activation window
/// sets those triggers up again with `values`, arms them on what that
/// setup captured, and stores `store` in the transaction that lands them
/// Active. A failure before arming puts the triggers back as they were;
/// one after takes them down, and either way the values stay as they were.
pub(crate) struct Rearm<'a> {
    /// The member's change the setup runs with, in place of what is
    /// stored; none when what is stored is already the answer (a pick
    /// change, stored before its re-arm: `crate::install_picks`).
    pub overlay: Option<&'a weft_core::member::ValueChanges>,
    /// The same change as the store writes it; `None` for a trigger
    /// re-armed on a change another re-arm already stored.
    pub store: Option<crate::activation_store::ValuesStore<'a>>,
}

/// The install's picks a run over `selection` carries, checked by
/// `weft_core::picks::run_picks`: a 422 naming every required connection
/// nobody picked on this install.
pub(crate) async fn picks_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
) -> Result<weft_core::picks::Picks, (StatusCode, String)> {
    if weft_core::picks::picked_places(project).is_empty() {
        return Ok(Default::default());
    }
    let stored = stored_picks(state, project_id).await?;
    weft_core::picks::run_picks(project, selection, &stored).map_err(|refusal| refusal_error(&refusal))
}

/// Every pick this install keeps for the project.
pub(crate) async fn stored_picks(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> Result<weft_core::picks::Picks, (StatusCode, String)> {
    let tenant = crate::member_values::owning_tenant(state, project_id).await?;
    weft_access_store::install_picks(&state.pg_pool, &tenant, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the install's picks: {e:#}")))
}

pub(crate) async fn activate_with(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
    asker: ActivateAsker<'_>,
) -> Result<Json<ActivateResponse>, ActivateError> {
    let ActivateRequest {
        target: ActivationTarget { binary_hash, definition_hash, infra_hash, reactivate_choice },
        running,
        scope,
    } = request;
    // No picker on an activate: the body's answer, or the default.
    let (running_policy, drain_timeout_secs) = running.resolve(None);
    let (stale, rearm) = match asker {
        ActivateAsker::Outside => (StaleWorkerChoice::Replace(running_policy, drain_timeout_secs), None),
        ActivateAsker::Run => (StaleWorkerChoice::Retire, None),
        ActivateAsker::Rearm(rearm) => (StaleWorkerChoice::Retire, Some(rearm)),
    };
    // What runs is what the last build registered (its program, hashes
    // and every place's images together, `POST /projects/{id}/builds`);
    // hashes named here only check the caller means that build.
    require_registered_build(state, id, binary_hash.as_deref(), definition_hash.as_deref(), infra_hash.as_deref())
        .await?;

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
             Use `weft run` to fire an execution directly."
                .into(),
        ));
    }
    let keys = resolve_scope(&project, &scope)?;
    // The shared infra that is not running, read now for the note the
    // answer carries: past the Active flip nothing may fail the call.
    let idle: Vec<MissingCopy> = missing_infra_nodes(state, id, &project, None, None)
        .await?
        .into_iter()
        .filter(|copy| !copy.needs_member)
        .collect();

    // Validate (don't yet apply) the reactivate choice. Validation is
    // a pure rejection and belongs in the read-only pre-flight; the
    // choice's DESTRUCTIVE effect (clearing parked / wiping signals)
    // is applied AFTER the single-flight claim below, so a losing
    // concurrent activate that 409s never wipes the winner's state.
    let choice = reactivate_choice.as_deref().unwrap_or("execute_parked_keep_suspended");
    if !matches!(choice, "execute_parked_keep_suspended" | "keep_suspended_only" | "wipe_all") {
        return Err(ActivateError::Failed(
            StatusCode::BAD_REQUEST,
            format!(
                "unknown reactivate_choice '{choice}'; must be one of: \
                 execute_parked_keep_suspended, keep_suspended_only, wipe_all"
            ),
        ));
    }

    // Single-flight gate: atomically claim Activating for every named
    // activation. While a trigger is activating (registering its
    // signals), no second activation of it may start. The claim wins or
    // loses whole; a losing caller (a concurrent activate from another
    // dispatcher, a double-click, CLI + extension racing, a program
    // activating the same member twice) bails here with 409 BEFORE any
    // signal cleanup. Every later write is guarded by this activation's
    // reserved execution.
    let activation = uuid::Uuid::new_v4();
    let previous = state
        .activations
        .try_begin_activating(id, &keys, activation, rearm.is_some().then_some(crate::activation_store::ProjectStatus::Active))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("try_begin_activating: {e}")))?
        .map_err(ActivateError::Refused)?;
    // Won the claim: broadcast it (other tabs' action bars flip to
    // "Activating… (cancel)" without a verb round-trip) and keep the
    // heartbeat fresh for the whole in-process window so the
    // stuck-transition reaper only ever repairs a DEAD driver's claim.
    // The guard drops when this function returns, on every path.
    crate::transition::publish_transition_changed(state, id).await;
    let _activation_heartbeat =
        crate::transition::ActivationHeartbeat::spawn(state.activations.clone(), activation);

    // Everything from here to the Active flip happens while the
    // activations are Activating. ANY failure in this window must
    // un-stick them (a stranded Activating locks out every future
    // activate of those triggers). Rather than hand-roll a rollback at
    // each `?`, the whole window is ONE fallible block with ONE rollback
    // site below. A future step added here can't forget the un-stick.
    let setup = activate_trigger_setup_window(
        state,
        id,
        &keys,
        activation,
        choice,
        &project,
        &program,
        rearm,
        stale,
    )
    .await;
    let urls = match setup {
        Ok(urls) => urls,
        Err(ActivateWindowError { status, msg, rollback }) => {
            // Single rollback site for the whole window. The mode says
            // how much to undo: a pre-trigger-setup failure puts the triggers
            // back (signals were never touched, so wiping would destroy prior
            // suspended/parked work); a trigger-setup-or-later failure wipes
            // (signals are in flux). Both are idempotent and safe if a
            // concurrent cancel/success already ended the activation.
            let cause = weft_core::exec::CancelCause::Runtime {
                detail: "the activation failed and rolled back its setup run".into(),
            };
            let rb = match rollback {
                ActivateRollback::Restore => match state.activations.restore(id, activation, &previous).await {
                    // The reserved execution may not have started yet. Cancellation
                    // is harmless for an ownerless execution and fences any born run.
                    Ok(true) => crate::api::execution::cancel_execution_id(state, activation, &cause)
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_execution_id: {e}"))),
                    Ok(false) => Ok(()),
                    Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, format!("restore the triggers: {e}"))),
                },
                // The activation failed on its own; the setup run it
                // spawned is cancelled by that failure, not by anyone's
                // decision.
                ActivateRollback::WipeSignals => wipe_activating_state(state, id, activation, &cause).await,
            };
            if let Err((rb_status, rb_msg)) = rb {
                tracing::error!(
                    target: "weft_dispatcher::activate",
                    project_id = %id, %activation, rb_status = %rb_status, rb_error = %rb_msg,
                    "activate rollback failed; the stuck-transition reaper will repair \
                     the activation once its heartbeat goes stale"
                );
            }
            // Broadcast whatever state the rollback landed (inactive on
            // success, or the still-stuck state the reaper will repair).
            crate::transition::publish_transition_changed(state, id).await;
            return Err(ActivateError::Failed(status, msg));
        }
    };
    // Past here the activations are Active (the window's final step
    // flipped them), so nothing fails the call.

    // Drain every queued fire those activations' signals kept through
    // their inactive window. Single loop, kind-agnostic:
    // dispatch_listener_outcome routes Resume vs Entry vs Drop based on
    // what the listener returns, exactly like a live fire. Runs after
    // the Active flip so the gate relays instead of re-queueing. A fire
    // this pass could not move stays parked, and the reaper's parked-fire
    // sweep (`drain_due_parked_fires`) drains it.
    if let Err(e) = drain_parked_fires(state, id, &keys).await {
        tracing::error!(
            target: "weft_dispatcher::activate",
            project_id = %id, %activation, error = %e,
            "the triggers are active, but draining the fires they parked failed; \
             the reaper's parked-fire sweep drains what is left"
        );
    }

    for url in &urls {
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
    state
        .events
        .publish(DispatcherEvent::ProjectActivated { project_id: id })
        .await;
    let infra_not_running = (!idle.is_empty()).then(|| {
        let (listed, fix) = missing_and_fix(&idle);
        format!("infra not running: {listed}. Activating does not start it; if anything uses it, run {fix}.")
    });
    warm_workers(state, id).await;
    Ok(Json(ActivateResponse { urls, infra_not_running }))
}

/// Have the platform ready the workers of a project that was just
/// activated (`Runner::prepare`: a service revision on a cloud, the image
/// pulled locally), so the first caller does not wait for it. Nothing
/// waits on it, and a failure only costs that first caller the wait, so
/// it is logged, never raised.
async fn warm_workers(state: &DispatcherState, id: uuid::Uuid) {
    let warmed = async {
        let program = state
            .projects
            .running_program_identity(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no running program"))?;
        let tenant = state.projects.tenant_for(id).await?.ok_or_else(|| anyhow::anyhow!("no project"))?;
        state.runner.prepare(&crate::delivery::worker_target(state, &tenant, id, &program.binary_hash).await?).await
    }
    .await;
    if let Err(e) = warmed {
        tracing::warn!(target: "weft_dispatcher::api::project", project_id = %id, error = %format!("{e:#}"), "could not ready the workers after activation; the first call waits for them");
    }
}

/// One drain pass: every signal row in the project with at least
/// one queued fire gets replayed through `dispatch_listener_outcome`.
/// The listener's `/process` returns the right `ProcessTarget`
/// (Resume for is_resume rows, Entry for entry rows, Drop if
/// obsolete) so the drain doesn't need to know what kind it's
/// draining.
///
/// Per row (see `drain_one_token`):
///   1. Atomically claim: UPDATE drain_claimed_at = now,
///      drain_claimed_by = <fresh nonce> WHERE drain_claimed_at IS
///      NULL. If 0 rows updated, another pass raced us and won; skip.
///   2. Pop-then-dispatch loop: read head (`parked_fires -> 0`),
///      dispatch, then pop it (`parked_fires - 0::int`) FENCED on our
///      claim nonce (`WHERE drain_claimed_by = <ours>`). One element
///      commits at a time, so a mid-loop failure leaves the unsent
///      remainder intact in FIFO order for the next activate. Appends
///      from concurrent fires land at the tail, so index 0 is stable
///      across the dispatch window. If a stale-claim sweep handed the
///      row to a sibling process mid-drain, our fenced pop matches 0 rows
///      and we abort: the element we just dispatched dedups at the
///      task table (`ParkedFire.id` -> `enqueue_dedup`), and the new
///      owner re-drives from the same head.
///   3. Release the row claim FENCED on our nonce (so we never clear a
///      sibling's claim that took over), regardless of outcome.
///
/// A process crash between steps 1 and 3 leaves the claim set;
/// [`release_stale_drain_claims`] (run by this pass's pre-step and by
/// the reaper's parked-fire sweep) releases claims older than the
/// threshold so the row becomes drainable again.
async fn drain_parked_fires(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<()> {
    // Pre-pass: release stale claims. A crashed process could have left
    // drain_claimed_at set; the shared release clears any claim older
    // than the threshold (globally: a claim that old is dead whichever
    // pass notices it) so this pass can claim the rows itself.
    release_stale_drain_claims(&state.pg_pool).await?;

    // Bound on the snapshot-loop below. A fire whose
    // `lookup_signal_routing` saw status=Activating just before the
    // CAS to Active commits will append to parked_fires AFTER our
    // first snapshot. We rerun the snapshot until it returns empty
    // so those fires drain in the same activate pass. Cap at 3
    // iterations so a stuck token (something appending faster than
    // we can dispatch) cannot livelock the activate handler; an
    // operator-visible failure beats an infinite loop.
    const MAX_DRAIN_PASSES: u32 = 3;
    let queued = |claimed_filter: &'static str| async move {
        let tokens: Vec<String> = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys)
            .await?
            .into_iter()
            .map(|signal| signal.token)
            .collect();
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT token FROM signal \
             WHERE token = ANY($1) AND jsonb_array_length(parked_fires) > 0 {claimed_filter} \
             ORDER BY created_at ASC"
        ))
        .bind(&tokens)
        .fetch_all(&state.pg_pool)
        .await?;
        anyhow::Ok(rows.into_iter().map(|(token,)| token).collect::<Vec<String>>())
    };
    for pass in 0..MAX_DRAIN_PASSES {
        let tokens = queued("AND drain_claimed_at_unix IS NULL").await?;
        if tokens.is_empty() {
            return Ok(());
        }
        tracing::debug!(
            target: "weft_dispatcher::activate",
            %project_id, pass, count = tokens.len(),
            "drain_parked_fires pass"
        );
        for token in tokens {
            drain_one_token(state, project_id, &token).await?;
        }
    }
    // Final check: any leftover queued fires get one warn line so an
    // operator can investigate. They drain on the next activate.
    let leftover = (queued("").await?.len() as i64,);
    if leftover.0 > 0 {
        tracing::warn!(
            target: "weft_dispatcher::activate",
            %project_id, leftover = leftover.0,
            "drain_parked_fires exceeded MAX_DRAIN_PASSES; \
             leftover fires will drain on next activate"
        );
    }
    Ok(())
}

/// The reaper's half of the parked-fire retry: every unclaimed signal row
/// of an Active project whose queue head is due gets one drain pass. A
/// fire that failed to route re-parked itself with a backoff stamp
/// (`ParkedFire::not_before_unix`); nothing else drains an Active
/// project's queue (activate drains once, at activation), so without this
/// sweep a re-parked fire would wait for the next activate, which may
/// never come. Idempotent across processes: `drain_one_token` claims the row.
pub(crate) async fn drain_due_parked_fires(state: &DispatcherState) -> anyhow::Result<()> {
    // Stale drain claims first: a process that died mid-drain leaves its
    // claim set, and this sweep is the only thing that re-drives an
    // Active project's queue, so a stale claim would starve that
    // token's retries until the next activate. The same release the
    // activate pre-pass runs; a mistaken release is safe because every
    // pop and re-stamp is fenced on the claim nonce.
    release_stale_drain_claims(&state.pg_pool).await?;
    for (token, project_id) in due_parked_tokens(&state.pg_pool, crate::lease::now_unix()).await? {
        if let Err(e) = drain_one_token(state, project_id, &token).await {
            // One token's dispatch failure must not stop the others;
            // the failed head was re-stamped with a longer backoff by
            // the drain itself, so this token comes back when due.
            tracing::warn!(
                target: "weft_dispatcher::reaper",
                project_id = %project_id,
                token = %token,
                error = %e,
                "parked-fire sweep: dispatch failed; the head's backoff was lengthened"
            );
        }
    }
    Ok(())
}

/// The sweep's selection: every signal row whose governing activation is
/// ACTIVE (or that none governs) whose
/// queue holds at least one element, is unclaimed, and whose HEAD is due
/// (`not_before_unix` in the past; a head without it is due now, the
/// field's serde default). Pool-level on purpose so the db-tests can
/// pin the predicate against the real statement. Head-only on purpose:
/// the queue is FIFO, so a backing-off head blocks its token's tail (a
/// later fire overtaking it would reorder one trigger's events) and the
/// sweep simply comes back for the token once the head is due. A head
/// waiting on its member's values (`ParkedFire::member_gap`) is never
/// due on a timer: that member's next change of values routes it again.
pub async fn due_parked_tokens(
    pool: &sqlx::PgPool,
    now: i64,
) -> anyhow::Result<Vec<(String, uuid::Uuid)>> {
    Ok(sqlx::query_as::<_, (String, uuid::Uuid)>(&format!(
        "SELECT s.token, s.project_id FROM signal s {} \
         WHERE COALESCE(a.status, 'active') = 'active' \
           AND jsonb_array_length(s.parked_fires) > 0 \
           AND s.drain_claimed_at_unix IS NULL \
           AND NOT ((s.parked_fires -> 0) ? 'member_gap') \
           AND COALESCE((s.parked_fires -> 0 ->> 'not_before_unix')::bigint, 0) <= $1",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .bind(now)
    .fetch_all(pool)
    .await?)
}

/// When the earliest queued head the sweep would drain becomes due
/// (`None` when no Active project has one waiting): the same rows as
/// [`due_parked_tokens`], without the "due now" cut.
pub async fn next_parked_fire_due(pool: &sqlx::PgPool) -> anyhow::Result<Option<i64>> {
    Ok(sqlx::query_scalar::<_, Option<i64>>(&format!(
        "SELECT MIN(COALESCE((s.parked_fires -> 0 ->> 'not_before_unix')::bigint, 0)) \
         FROM signal s {} \
         WHERE COALESCE(a.status, 'active') = 'active' \
           AND jsonb_array_length(s.parked_fires) > 0 \
           AND s.drain_claimed_at_unix IS NULL \
           AND NOT ((s.parked_fires -> 0) ? 'member_gap')",
        weft_broker_client::protocol::SIGNAL_ACTIVATION_JOIN,
    ))
    .fetch_one(pool)
    .await?)
}

/// How old a drain claim must be before any owner is considered dead.
/// A claim is held for one pop-dispatch pass, so five minutes is far
/// beyond any live drain; a takeover younger than this would race a
/// healthy drain for nothing (the fence keeps it safe, not pointless).
const DRAIN_CLAIM_STALE_SECS: i64 = 300;

/// Release every drain claim older than [`DRAIN_CLAIM_STALE_SECS`],
/// clearing both claim columns. Run by the activate pre-pass (so an
/// activation's own drain can claim rows a crashed process left held) and
/// by the reaper's parked-fire sweep (so a stale claim cannot starve an
/// Active project's retries). Global on purpose: a claim that old is
/// dead whichever pass notices it, and the fenced pops make a mistaken
/// release safe (the old owner aborts on its next fenced write, the new
/// owner re-drives, and dispatched elements dedup at the task table).
pub async fn release_stale_drain_claims(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = NULL, drain_claimed_by = NULL \
         WHERE drain_claimed_at_unix IS NOT NULL \
           AND drain_claimed_at_unix < $1",
    )
    .bind(crate::lease::now_unix() - DRAIN_CLAIM_STALE_SECS)
    .execute(pool)
    .await?;
    Ok(())
}

/// Drain one signal row's queue. Internal helper: caller has already
/// established the row has fires and is unclaimed. Idempotent under
/// retry: if the row's queue becomes empty mid-loop (e.g. another
/// drain raced; or our pop sequence finished), we exit cleanly.
pub(crate) async fn drain_one_token(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    token: &str,
) -> anyhow::Result<()> {
    use sqlx::Row;

    // Atomic claim with a per-claim owner nonce. If another pass beat
    // us, bail. The nonce fences every subsequent pop + the release:
    // if a stale-claim sweep on a sibling process takes the row over
    // mid-drain, our fenced pop matches 0 rows and we abort, rather
    // than popping an element the new owner already dispatched.
    let owner = uuid::Uuid::new_v4().to_string();
    let claim = sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = $2, drain_claimed_by = $3 \
         WHERE token = $1 AND drain_claimed_at_unix IS NULL",
    )
    .bind(token)
    .bind(crate::lease::now_unix())
    .bind(&owner)
    .execute(&state.pg_pool)
    .await?;
    if claim.rows_affected() == 0 {
        return Ok(());
    }

    // The signal's own tenant (frozen on the row at register), used to stamp the
    // dispatched fire's tasks/spawns, so the fire path never re-derives it.
    let tenant: String =
        sqlx::query_scalar("SELECT tenant_id FROM signal WHERE token = $1")
            .bind(token)
            .fetch_one(&state.pg_pool)
            .await?;

    // Pop-then-dispatch loop. Head-stable invariant: appends from
    // concurrent fires land at the tail of the array, so `index 0`
    // is always the next element to dispatch even under concurrent
    // /signal/{token} writes. Crash between dispatch-success and
    // pop is safe: each queue element carries its own per-fire UUID
    // (`ParkedFire.id`), passed to `dispatch_listener_outcome` as
    // the dedup nonce. On retry, the same element produces the same
    // dedup key, so the RouteEntry task collapses at the task table.
    // Distinct queued fires have distinct UUIDs and never collapse.
    let outcome = loop {
        let head_row = sqlx::query(
            "SELECT parked_fires -> 0 AS head \
             FROM signal WHERE token = $1",
        )
        .bind(token)
        .fetch_optional(&state.pg_pool)
        .await?;
        let Some(head_row) = head_row else {
            // Row vanished mid-drain (CASCADE delete?). Nothing to do.
            break Ok(());
        };
        let head: Option<Value> = head_row.try_get("head")?;
        // `parked_fires -> 0` returns SQL NULL (decoded as None) for
        // an empty array; an actual queued element decodes to
        // `Some(Value::Object(_))`. Anything else is a schema bug;
        // we fail rather than guess.
        let Some(head) = head else {
            break Ok(());
        };
        // Typed deserialize against the shared ParkedFire schema so
        // the writer (apply_lifecycle_gate) and the reader stay in
        // lockstep; a typo on either side becomes a compile error.
        let fire: crate::api::signal::ParkedFire =
            serde_json::from_value(head).map_err(|e| {
                anyhow::anyhow!("malformed parked_fires element for token {token}: {e}")
            })?;
        // A re-parked fire in its backoff window blocks its token's
        // queue: the queue is FIFO, and letting a later fire overtake it
        // would reorder one trigger's events. The reaper's parked-fire
        // sweep re-drives this token once the head is due.
        let now = crate::lease::now_unix();
        if fire.not_before_unix > now {
            tracing::debug!(
                target: "weft_dispatcher::activate",
                %project_id, token, fire_id = %fire.id, attempts = fire.attempts,
                due_in_secs = fire.not_before_unix - now,
                "head of the parked queue is backing off; leaving the token for the sweep"
            );
            break Ok(());
        }
        // Keep the id and the attempt count: the dispatch moves
        // `fire.payload`, and both are needed afterward (the id to
        // remove or re-stamp this exact element by id, the count to
        // lengthen the backoff when the dispatch failed).
        let fire_id = fire.id.clone();
        let attempts_before = fire.attempts;

        match crate::api::signal::dispatch_listener_outcome(
            state,
            token,
            project_id,
            &tenant,
            fire.payload,
            Some(crate::api::signal::ParkedRef { id: &fire_id, attempts: attempts_before }),
        )
        .await
        {
            Ok(_) => {
                // Remove the element we just dispatched BY ID (not by
                // array index), FENCED on our claim nonce. By-id removal
                // commutes with a concurrent success-path removal of a
                // different fire, so a sibling removing the head out from
                // under us can't make us delete the wrong element (the
                // old index-0 pop assumed a head-stable array, which the
                // route_entry success-path removal breaks). If our claim
                // was taken over (0 rows), abort: the dispatched element
                // dedups at the task table via its fire id, and the new
                // owner re-drives.
                let removed = crate::api::signal::remove_parked_fire(
                    &state.pg_pool,
                    token,
                    &fire_id,
                    Some(&owner),
                )
                .await?;
                if removed == 0 {
                    break Ok(());
                }
            }
            Err((status, msg)) => {
                // The dispatch failed without popping the element, so
                // the fire is still the queue's head. Re-stamp it IN
                // PLACE with a longer backoff (a pop-and-re-append would
                // send it to the back of the queue, reordering one
                // trigger's events), and leave the remainder behind it.
                // The retry is whoever drains next: this activate's next
                // pass re-selects the token but stops at the future
                // stamp, and the reaper's parked-fire sweep comes back
                // once the head is due. Without the re-stamp that sweep
                // would retry a persistently failing dispatch every 5s
                // forever.
                let attempts = attempts_before + 1;
                let backoff = crate::api::signal::park_backoff_secs(attempts);
                let restamped = crate::api::signal::restamp_parked_fire(
                    &state.pg_pool,
                    token,
                    &fire_id,
                    attempts,
                    crate::lease::now_unix() + backoff,
                    Some(&owner),
                )
                .await?;
                if restamped == 0 {
                    // Our claim was taken over mid-drain: nothing of
                    // ours committed (the dispatch failed), so there is
                    // nothing to finish; the new owner re-reads the same
                    // head and retries.
                    break Ok(());
                }
                tracing::warn!(
                    target: "weft_dispatcher::activate",
                    %project_id, token, fire_id = %fire_id, %status,
                    attempts, retry_in_secs = backoff,
                    error = %msg,
                    "drain: dispatch failed; head re-stamped, retries when due"
                );
                break Err(anyhow::anyhow!("dispatch failed: {msg}"));
            }
        }
    };

    // Release the claim regardless of outcome, FENCED on our nonce: if
    // a sibling already took the row over via a stale-claim sweep, we
    // must not clear ITS claim. A no-op release (0 rows) is fine.
    sqlx::query(
        "UPDATE signal SET drain_claimed_at_unix = NULL, drain_claimed_by = NULL \
         WHERE token = $1 AND drain_claimed_by = $2",
    )
    .bind(token)
    .bind(&owner)
    .execute(&state.pg_pool)
    .await?;

    // Surface the dispatch error to the activate caller so the
    // operator sees the failure. Subsequent rows in the snapshot
    // still won't be drained this pass; the next activate gets them.
    outcome
}

/// Sweep this owner's entry-trigger rows whose trigger no longer exists
/// in the source. Called after TriggerSetup so any trigger the user
/// removed while it was down has its leftover signal row dropped.
/// Resume rows (per-suspension) skip this: the corresponding suspended
/// execution is the source of truth for them.
async fn orphan_entry_tokens(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    member: Option<&weft_core::member::MemberId>,
) -> anyhow::Result<Vec<String>> {
    let triggers: HashSet<String> = weft_core::project::trigger_places(project)
        .iter()
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    Ok(state
        .journal
        .signal_list_for_project(project_id)
        .await?
        .into_iter()
        .filter(|s| !s.is_resume && s.member.as_ref() == member && !triggers.contains(&s.node_id))
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
    let mut tx = activation_write(state, project_id, activation).await?;
    let removed = crate::journal::postgres::remove_signals(&mut *tx, orphans).await?;
    tx.commit().await?;
    state.listener.unregister_many(&removed).await;
    Ok(())
}

/// The failure of an activate window step from arming on: any step
/// before the one delete, and the flip to Active after it (the orphan
/// rows it deleted belong to triggers the source no longer has, so they
/// stay gone either way). A fresh activation has nothing to go back to,
/// so its rows go. A re-arm takes
/// back every trigger it armed (row written back, the listener running it
/// again) and puts the triggers back as they were; when one cannot be put
/// back, the triggers no longer match what they were, so they are turned
/// off rather than left half re-armed.
async fn take_back_arms(
    state: &DispatcherState,
    is_rearm: bool,
    armed: Vec<crate::task_kinds::register_signal::Armed>,
    msg: String,
) -> ActivateWindowError {
    let status = StatusCode::INTERNAL_SERVER_ERROR;
    if !is_rearm {
        return ActivateWindowError { status, msg, rollback: ActivateRollback::WipeSignals };
    }
    match crate::task_kinds::register_signal::disarm(state.journal.as_ref(), &state.listener, armed).await {
        Ok(()) => ActivateWindowError { status, msg, rollback: ActivateRollback::Restore },
        Err(undo) => ActivateWindowError {
            status,
            msg: format!("{msg}; putting back the triggers armed before it failed, so the triggers are turned off: {undo:#}"),
            rollback: ActivateRollback::WipeSignals,
        },
    }
}

/// Hold the activation identity while changing its persistent signal
/// state: the rows it claimed stay Activating under it for the whole
/// transaction, so a concurrent cancel waits behind this write.
async fn activation_write(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    activation: uuid::Uuid,
) -> anyhow::Result<sqlx::Transaction<'static, sqlx::Postgres>> {
    let mut tx = state.pg_pool.begin().await?;
    let owned: Vec<(String,)> = sqlx::query_as(
        "SELECT trigger FROM trigger_activation \
         WHERE project_id = $1 AND status = 'activating' AND activating_execution_id = $2 FOR UPDATE",
    ).bind(project_id).bind(activation).fetch_all(&mut *tx).await?;
    anyhow::ensure!(!owned.is_empty(), "activation {activation} no longer owns any trigger of project {project_id}");
    Ok(tx)
}

/// Apply an activate `reactivate_choice`'s destructive effect on the
/// project's parked/suspended signal state. Run AFTER the
/// single-flight CAS so a losing concurrent activate can't wipe the
/// winner's state. `choice` must already be validated.
///   - `execute_parked_keep_suspended`: no-op (the drain at the end
///     of activate replays every parked fire).
///   - `keep_suspended_only`: clear parked fires, keep suspensions.
///   - `wipe_all`: drop every signal row.
async fn apply_reactivate_choice(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    choice: &str,
) -> Result<(), (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // What these activations kept: their signals, and the runs their
    // triggers fired. Another trigger's, another member's, and a run
    // started by hand are not this activation's to touch.
    let governed: Vec<String> = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await
        .map_err(|e| internal("activation signals", e))?
        .into_iter()
        .map(|s| s.token)
        .collect();
    let mut tx = activation_write(state, project_id, activation).await
        .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;
    let mut cancelled = Vec::new();
    let mut removed = Vec::new();
    match choice {
        "execute_parked_keep_suspended" => {},
        "keep_suspended_only" => {
            sqlx::query(
                "UPDATE signal SET parked_fires = '[]'::jsonb \
                 WHERE token = ANY($1) AND jsonb_array_length(parked_fires) > 0",
            )
            .bind(&governed)
            .execute(&mut *tx)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("clear parked: {e}")))?;
        }
        "wipe_all" => {
            let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
            let runs = crate::take_down::live_runs(state, project_id).await.map_err(|e| internal("live runs", e))?;
            cancelled = crate::take_down::affected_runs(&target, &runs, None).into_iter().map(|r| r.execution_id).collect();
            removed = crate::journal::postgres::remove_signals(&mut *tx, &governed).await
                .map_err(|e| internal("clear activation signals", e))?;
        }
        _ => unreachable!("reactivate_choice validated by caller"),
    }
    tx.commit().await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("apply activation choice: {e}")))?;
    state.listener.unregister_many(&removed).await;
    let cause = weft_core::exec::CancelCause::User;
    let targets: Vec<_> = cancelled.iter().map(|execution_id| (*execution_id, &cause)).collect();
    crate::api::execution::cancel_execution_ids(state, &targets).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation's prior runs: {e}")))?;
    Ok(())
}

/// End the named activation and delete its signal rows atomically. Listener
/// cleanup receives only those deleted rows, so a newer activation's tokens
/// survive. The caller supplies the cause of the setup run's cancellation.
pub(crate) async fn wipe_activating_state(
    state: &DispatcherState,
    id: uuid::Uuid,
    activation: uuid::Uuid,
    cause: &weft_core::exec::CancelCause,
) -> Result<(), (StatusCode, String)> {
    let Some(removed) = state.activations.end_activating(
        id, activation, &crate::activation_store::ActivationLifecycle::wiped(), true, None,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation: {e}")))? else {
        return Ok(());
    };
    state.listener.unregister_many(&removed).await;
    crate::api::execution::cancel_execution_id(state, activation, cause)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel activation {id}: {e}")))?;
    Ok(())
}

/// Body for `POST /projects/{id}/deactivate`: the take-down choice
/// (mode + runningPolicy + drain cap, see [`crate::take_down`]) and which
/// activations: every shared trigger by default, or the ones named, for
/// the member named; or, with `allMembers`, for every member whose
/// triggers are on.
#[derive(Debug, Deserialize)]
pub struct DeactivateRequest {
    #[serde(flatten)]
    pub spec: weft_broker_client::protocol::DeactivateSpec,
    #[serde(default)]
    pub scope: weft_core::activation::ActivationScope,
    /// Every member with a trigger on, each taken down with the same
    /// spec and the scope's triggers (all of that member's when none are
    /// named). The program's own triggers are left as they are: a plain
    /// deactivate is theirs. Refused beside `scope.member`, which names
    /// one member instead.
    #[serde(default, rename = "allMembers")]
    pub all_members: bool,
}

/// Every owner of project `id` with at least one trigger on, each once:
/// the program itself first, then members in id order. The activation
/// rows are the whole answer, so nothing is guessed. What a plain resync
/// brings up to date, and (members only) what `deactivate --all-members`
/// takes down and what a plain deactivate reports as still on.
pub async fn owners_with_triggers_on(
    activations: &dyn crate::activation_store::ActivationStoreOps,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::member::Owner>, (StatusCode, String)> {
    let rows = activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let live = rows.iter().filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active).map(|a| &a.key);
    Ok(weft_core::activation::owners_of(live))
}

/// The members among [`owners_with_triggers_on`].
async fn members_with_triggers_on(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::member::MemberId>, (StatusCode, String)> {
    let owners = owners_with_triggers_on(state.activations.as_ref(), id).await?;
    Ok(owners.into_iter().filter_map(|owner| owner.member().cloned()).collect())
}

// `DeactivationMode` is the wire contract for the `mode` field on a
// `DeactivateSpec`. It lives in `weft-broker-client::protocol` so
// the supervisor + dispatcher share one source of truth. Re-export
// here so this module stays the canonical home for deactivation glue.
pub use weft_broker_client::protocol::DeactivationMode;

/// Take down the triggers an infra verb's containers feed, with the
/// person's choice (Stop / Terminate / Upgrade send it). Validation
/// lives on `DeactivateSpec::validate` (broker, dispatcher, supervisor
/// share one validator); this site adds the `triggerDeactivation:`
/// prefix so clients see which field tripped the check.
pub async fn execute_trigger_deactivation(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: Vec<weft_core::activation::ActivationKey>,
    spec: &weft_broker_client::protocol::DeactivateSpec,
) -> Result<(), (StatusCode, String)> {
    spec.validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, format!("triggerDeactivation: {m}")))?;
    let target = crate::take_down::TakeDownTarget::Activations(keys);
    let existed = crate::take_down::take_down(state, id, &target, spec, false, None).await?;
    if !existed {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "triggerDeactivation: project disappeared mid-deactivate".into(),
        ));
    }
    Ok(())
}

pub async fn deactivate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<DeactivateRequest>,
) -> Result<Json<DeactivateResponse>, StatusError> {
    // Marked gate: deactivate is part of teardown flows (`weft rm
    // --journal` quiesces through it), so its "no such project" must
    // be tellable apart from a routing 404, same as `remove`/`status`.
    authorize_project_marked(&state, &caller.0, id).await?;
    body.spec.validate().map_err(|m| (StatusCode::BAD_REQUEST, m.to_string()))?;
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let owners: Vec<Option<weft_core::member::MemberId>> = if body.all_members {
        if let Some(member) = &body.scope.member {
            return Err(StatusError::Other(
                StatusCode::BAD_REQUEST,
                format!(
                    "--all-members takes down every member's triggers and --member names one \
                     ('{member}'); pass one of the two"
                ),
            ));
        }
        members_with_triggers_on(&state, id).await?.into_iter().map(Some).collect()
    } else {
        vec![body.scope.member.clone()]
    };
    let mut deactivated = Vec::with_capacity(owners.len());
    for member in owners {
        let scope = weft_core::activation::ActivationScope { triggers: body.scope.triggers.clone(), member };
        let keys = keys_to_take_down(&rows, &scope)?;
        let target = crate::take_down::TakeDownTarget::Activations(keys);
        // user-initiated (the standalone Deactivate verb)
        let existed = crate::take_down::take_down(&state, id, &target, &body.spec, false, None).await?;
        if !existed {
            // Vanished between the gate and the deactivate (a concurrent
            // rm): same "no project I may see" answer, marker included.
            return Err(StatusError::NotMyProject);
        }
        deactivated.push(scope.owner());
    }
    let members_still_on = members_with_triggers_on(&state, id).await?;
    Ok(Json(DeactivateResponse { deactivated, members_still_on }))
}

/// Body for `POST /projects/{id}/resync`: what the activate points at
/// (hashes, reactivate choice) plus the trigger-deactivation choice
/// (mode + runningPolicy + drain cap, the SAME picker as the
/// standalone Deactivate), required because resync only acts on
/// triggers that are on (428 without it). The running-work answer is
/// the picker's, so the body carries no `runningPolicy` of its own: one
/// answer governs the trigger drain and the worker replacement alike.
#[derive(Debug, Default, Deserialize)]
pub struct ResyncRequest {
    #[serde(flatten)]
    pub target: ActivationTarget,
    #[serde(default, rename = "triggerDeactivation")]
    pub trigger_deactivation: Option<weft_broker_client::protocol::DeactivateSpec>,
    /// Which activations. Left out (no trigger, no member): every
    /// owner with a trigger on, the program's and each member's; see
    /// [`resync`].
    #[serde(default)]
    pub scope: weft_core::activation::ActivationScope,
}

/// What a resync answers: the listener URLs its reactivations minted,
/// and whose triggers it brought up to date, in the order it did them.
#[derive(Debug, Serialize)]
pub struct ResyncResponse {
    pub urls: Vec<ActivationUrl>,
    pub resynced: Vec<weft_core::member::Owner>,
}

/// `POST /projects/{id}/resync`. Deactivate-then-activate the named
/// activations against (optionally) fresh source / infra hashes,
/// bringing the deployed trigger/worker shape in line with the current
/// source.
///
/// A scope naming a member, or triggers, is that one owner's. The plain
/// scope (nothing named) is every owner with a trigger on: the program's
/// shared triggers when any is on, then each such member in id order,
/// each taken as a whole, exactly as `weft resync` / `weft resync
/// --member <id>` would for it alone. The activation rows say who has
/// triggers on, so nothing is guessed.
///
/// The deactivation uses the USER'S spec (never a hardcoded wipe): with
/// `runningPolicy = wait` each owner drains through THE shared drain
/// loop up to the spec's cap, cancels the stragglers, lands, then
/// reactivates. Every refusal the request can meet up front (an owner
/// whose triggers are not all settled on, infra a trigger reads that is
/// not running against the definition this resync builds) comes BEFORE
/// the first deactivation, so triggers it cannot bring back are left
/// exactly as they were.
pub async fn resync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<ResyncRequest>>,
) -> Result<Json<ResyncResponse>, StatusError> {
    use crate::activation_store::ProjectStatus;
    use weft_core::activation::ActivationScope;
    authorize_project(&state, &caller.0, id).await?;
    let body = body.map(|Json(b)| b).unwrap_or_default();

    // 1. Whose triggers, and refuse unless each owner's are Active.
    //    Resync means "the listeners fire an older program; register
    //    them against this one": on a hibernated or parked trigger there
    //    is nothing registered to bring across, and silently turning it
    //    into an activate would revive something the user parked on
    //    purpose.
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
    let scopes: Vec<ActivationScope> = if body.scope == ActivationScope::shared() {
        let owners = owners_with_triggers_on(state.activations.as_ref(), id).await?;
        if owners.is_empty() {
            return Err(StatusError::Other(
                StatusCode::CONFLICT,
                "no trigger of this program is on, and resync only re-registers triggers that \
                 are on; `weft activate` brings them up on the current source"
                    .into(),
            ));
        }
        owners
            .into_iter()
            .map(|owner| ActivationScope { triggers: Vec::new(), member: owner.member().cloned() })
            .collect()
    } else {
        vec![body.scope.clone()]
    };
    let mut owner_keys = Vec::with_capacity(scopes.len());
    for scope in &scopes {
        let keys = resolve_scope(&registered, scope)?;
        let lifecycle = crate::activation_store::aggregate(
            activations.iter().filter(|a| keys.contains(&a.key)).map(|a| &a.lifecycle),
        );
        if lifecycle.status != ProjectStatus::Active {
            return Err(StatusError::Other(
                StatusCode::CONFLICT,
                format!(
                    "{} are not active (their mode is {}), and resync only re-registers \
                     active ones; `weft activate` brings them up on the current source",
                    whose_triggers(&scope.owner()),
                    lifecycle.mode().as_str()
                ),
            ));
        }
        owner_keys.push(keys);
    }
    let Some(spec) = body.trigger_deactivation.as_ref() else {
        return Err(StatusError::NeedsTriggerChoice(weft_core::trigger_choice_required(
            "resync takes the triggers that are on down before registering them again",
        )));
    };
    // 2. Refuse BEFORE tearing anything down. Resync takes the triggers
    //    off and puts them back; a refusal after the first half leaves
    //    them down over a condition it could have checked while they
    //    were still up. The question is about the definition the last
    //    build registered, which is what this resync re-arms. Every
    //    owner's infra is checked before the first owner comes down.
    //    Each activate below re-reads and re-checks; this is what keeps
    //    the project whole.
    let built = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "project not found".to_string()))?;
    for scope in &scopes {
        let built_keys = resolve_scope(&built, scope)?;
        require_trigger_infra(&state, id, &built, &built_keys).await?;
    }

    // 3. One owner at a time, the program's first.
    let several = scopes.len() > 1;
    let mut urls = Vec::new();
    let mut resynced: Vec<weft_core::member::Owner> = Vec::with_capacity(scopes.len());
    for (scope, keys) in scopes.into_iter().zip(owner_keys) {
        let owner = scope.owner();
        let target = body.target.clone();
        let activated = resync_owner(&state, &caller, id, keys, spec, target, scope).await.map_err(|(status, msg)| {
            if !several {
                return (status, msg);
            }
            let done = if resynced.is_empty() {
                "none resynced before it".to_string()
            } else {
                format!("already resynced: {}", resynced.iter().map(whose_triggers).collect::<Vec<_>>().join(", "))
            };
            (status, format!("resyncing {}: {msg} ({done})", whose_triggers(&owner)))
        })?;
        urls.extend(activated.0.urls);
        resynced.push(owner);
    }
    Ok(Json(ResyncResponse { urls, resynced }))
}

/// One owner's half of [`resync`], after every check passed: take
/// `keys` (all of one owner's) down with the user's spec, wait out the
/// drain when the spec asked to, then activate `scope` again.
async fn resync_owner(
    state: &DispatcherState,
    caller: &CallerTenant,
    id: uuid::Uuid,
    keys: Vec<weft_core::activation::ActivationKey>,
    spec: &weft_broker_client::protocol::DeactivateSpec,
    target: ActivationTarget,
    scope: weft_core::activation::ActivationScope,
) -> Result<Json<ActivateResponse>, (StatusCode, String)> {
    execute_trigger_deactivation(state, id, keys.clone(), spec).await?;
    if spec.drains() {
        // Wait for the drain through THE shared loop, capped at the
        // spec's cap; stragglers past it are cancelled with the ONE
        // cancel helper, and the ONE landing CAS flips each row before
        // the reactivate.
        let cap = spec
            .drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
        let clock = weft_platform_traits::SystemClock;
        let outcome = weft_platform_traits::drain_until_zero(
            &clock,
            std::time::Duration::from_secs(cap),
            "resync",
            || async {
                running_count_for(state, id, &keys, None)
                    .await
                    .map(|n| n as i64)
                    .map_err(|e| {
                        (StatusCode::INTERNAL_SERVER_ERROR, format!("running_count: {e}"))
                    })
            },
        )
        .await?;
        if let weft_platform_traits::DrainOutcome::TimedOut { still_running } = outcome {
            tracing::warn!(
                target: "weft_dispatcher::api::project",
                project_id = %id,
                still_running,
                drain_timeout_secs = cap,
                "resync drain cap reached; cancelling remaining executions"
            );
            cancel_running_for(state, id, &keys, weft_core::exec::CancelCause::User).await?;
        }
        for key in &keys {
            crate::journal_bridge::try_finish_drain(state, id, key, None)
                .await
                .map_err(|e| {
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("try_finish_drain: {e}"))
                })?;
        }
    }

    // Reactivate. Reuses the activate handler so hash persistence +
    // atomic-cleanup-on-failure semantics are identical. The caller was
    // already authorized against this project at the top of resync;
    // activate re-checks the same gate (cheap, and keeps activate
    // self-contained). The person answered "what about the running
    // executions" ONCE, in the picker, and the answer governs the
    // reactivate's worker replacement as much as the trigger-side drain
    // above.
    let reactivate = ActivateRequest { target, running: spec.running_choice(), scope };
    activate(State(state.clone()), CallerTenant(caller.0.clone()), Path(id), Some(Json(reactivate))).await
}

/// Settle what the project's workers still run on an older image, once a
/// new image went live.
///
/// Every execution runs on the image it was born with (its task carries
/// the binary hash), so a new image never disturbs anything by itself:
/// new executions go to the new image's workers, and one suspended on the
/// old image resumes there. What the person chooses is the fate of the
/// executions being driven on an older image right now:
/// `RunningPolicy::Wait` waits for them to end, up to
/// `drain_timeout_secs`, then cancels what is left with a loud warning;
/// `RunningPolicy::Cancel` cancels them at once. Then the platform is
/// told to have the new image's workers ready (`Runner::prepare`).
///
/// Because Wait can sit for minutes, callers MUST NOT invoke this while
/// holding the per-project advisory lock. Idempotent.
pub async fn reconcile_worker(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    running_policy: crate::infra_lifecycle_command::RunningPolicy,
    drain_timeout_secs: u64,
) -> Result<(), (StatusCode, String)> {
    let internal = |e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("reconcile_worker: {e:#}"));
    let Some(want_hash) = state.projects.running_binary_hash(project_id).await.map_err(internal)? else {
        return Ok(());
    };
    use crate::infra_lifecycle_command::RunningPolicy;
    let driven = || async {
        driven_on_other_images(&state.pg_pool, project_id, &want_hash).await.map_err(internal)
    };
    let older = driven().await?;
    if !older.is_empty() {
        tracing::info!(
            target: "weft_dispatcher::api::project",
            %project_id,
            running = older.len(),
            policy = running_policy.as_str(),
            "a new image went live; settling what still runs on older ones"
        );
        let cancel_cause = match running_policy {
            RunningPolicy::Wait => {
                let clock = weft_platform_traits::SystemClock;
                let outcome = weft_platform_traits::drain_until_zero(
                    &clock,
                    std::time::Duration::from_secs(drain_timeout_secs),
                    "new image: executions on older images",
                    || async { Ok::<_, (StatusCode, String)>(driven().await?.len() as i64) },
                )
                .await?;
                match outcome {
                    weft_platform_traits::DrainOutcome::TimedOut { still_running } => {
                        tracing::warn!(
                            target: "weft_dispatcher::api::project",
                            %project_id,
                            still_running,
                            drain_timeout_secs,
                            "runningPolicy=wait drain cap reached; cancelling what still runs on older images"
                        );
                        Some(weft_core::exec::CancelCause::Runtime {
                            detail: format!(
                                "this execution ran on an older image of the program, and the wait for it \
                                 reached its cap of {drain_timeout_secs}s after a new one went live"
                            ),
                        })
                    }
                    _ => None,
                }
            }
            // The cause is the person's: cancel is the policy they chose
            // (or the standing default they left in place).
            RunningPolicy::Cancel => Some(weft_core::exec::CancelCause::User),
        };
        if let Some(cause) = cancel_cause {
            let execution_ids = driven().await?;
            let targets: Vec<(weft_core::ExecutionId, &weft_core::exec::CancelCause)> =
                execution_ids.iter().map(|c| (*c, &cause)).collect();
            // Every run is attempted before any error is reported, so one
            // failing cancel never leaves the runs after it live.
            crate::api::execution::cancel_execution_ids(state, &targets)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))?;
        }
    }
    prepare_running_image(state, project_id, &want_hash).await
}

/// Tell the platform to have the workers of the image `binary_hash`
/// ready (`Runner::prepare`).
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

/// The executions of `project_id` a worker drives right now (a live claim
/// on their execute or resume task) on an image other than `want_hash`.
async fn driven_on_other_images(
    pool: &sqlx::PgPool,
    project_id: uuid::Uuid,
    want_hash: &str,
) -> anyhow::Result<Vec<weft_core::ExecutionId>> {
    let rows: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT execution_id FROM task \
         WHERE project_id = $1 AND kind IN ('execute', 'resume') AND status = 'claimed' \
           AND claimed_until_unix >= EXTRACT(EPOCH FROM NOW())::BIGINT \
           AND execution_id IS NOT NULL AND binary_hash IS DISTINCT FROM $2",
    )
    .bind(project_id)
    .bind(want_hash)
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|c| c.parse().map_err(|e| anyhow::anyhow!("task execution '{c}': {e}")))
        .collect()
}

/// Take the whole project down for good: every activation of every
/// owner and every live run, wiped (project removal). Returns true if
/// the project existed.
pub async fn deactivate_project(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<bool, (StatusCode, String)> {
    let spec = weft_broker_client::protocol::DeactivateSpec {
        mode: DeactivationMode::Wipe,
        grace_minutes: 0,
        running_policy: crate::infra_lifecycle_command::RunningPolicy::Cancel,
        drain_timeout_secs: None,
    };
    crate::take_down::take_down(state, id, &crate::take_down::TakeDownTarget::WholeProject, &spec, false, None).await
}

/// `POST /projects/{id}/quiesce`: take the whole project down (every
/// owner's triggers wiped, every live run cancelled, those started by
/// hand and unrecorded ones included) and answer once none of its runs
/// is live. What `weft rm --journal` asks before deleting the project's
/// history, so nothing is still writing a run it erases.
pub async fn quiesce(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, StatusError> {
    // Marked gate: rm tells "already gone" apart from a routing 404.
    authorize_project_marked(&state, &caller.0, id).await?;
    let signals = state.signals.subscribe();
    if !deactivate_project(&state, id).await? {
        return Err(StatusError::NotMyProject);
    }
    crate::take_down::wait_until_no_live_runs(state.journal.as_ref(), signals, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("waiting for cancelled runs to end: {e:#}")))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/cancel-running`. Force a waiting deactivation
/// to finish now: cancel every running, non-suspended execution the
/// named deactivating activations are waiting for (every shared one by
/// default). The drain-watcher then lands them Inactive. Nothing to do
/// when none of them is deactivating.
pub async fn cancel_running(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<weft_core::activation::ActivationScope>>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let scope = body.map(|Json(s)| s).unwrap_or_default();
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let keys = keys_to_take_down(&rows, &scope)?;
    let draining: Vec<weft_core::activation::ActivationKey> = rows
        .into_iter()
        .filter(|a| keys.contains(&a.key) && a.lifecycle.status == crate::activation_store::ProjectStatus::Deactivating)
        .map(|a| a.key)
        .collect();
    cancel_running_for(&state, id, &draining, weft_core::exec::CancelCause::User).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /projects/{id}/cancel-build`. Cancel the in-flight build
/// transition: CAS `transition` building → cancelling_build, the
/// durable, cross-process cancel signal the driving process's build gate polls
/// between looks at its build processes (it stops them on seeing it).
/// Cancel reconciles, never asserts: the response is 202 and the
/// displayed state is whatever the backend reports next (the build
/// may still complete if it beat the cancel).
///
/// 412 when no build is in flight (stale tab; the client refetches
/// `/status` and reconciles).
pub async fn cancel_build(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
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
    crate::transition::publish_transition_changed(&state, id).await;
    Ok(StatusCode::ACCEPTED)
}

/// Cancel in-flight activations of the named triggers (every shared
/// one by default): each activation among them ends Inactive, its setup
/// run is cancelled, and every signal it registered so far is wiped.
///
/// 412 if none of them is activating: the user (or a stale UI) clicked
/// cancel against triggers that are already up or already down.
pub async fn cancel_activate(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    body: Option<Json<weft_core::activation::ActivationScope>>,
) -> Result<StatusCode, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let scope = body.map(|Json(s)| s).unwrap_or_default();
    let rows = state
        .activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let keys = keys_to_take_down(&rows, &scope)?;
    let activations: std::collections::BTreeSet<uuid::Uuid> = rows
        .into_iter()
        .filter(|a| keys.contains(&a.key))
        .filter_map(|a| a.lifecycle.activating_execution_id)
        .collect();
    if activations.is_empty() {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            "cancel-activate: none of these triggers is activating".into(),
        ));
    }
    // The cause is the person's: they pressed Cancel.
    for activation in activations {
        wipe_activating_state(&state, id, activation, &weft_core::exec::CancelCause::User).await?;
    }
    state
        .events
        .publish(DispatcherEvent::ProjectDeactivated { project_id: id })
        .await;
    Ok(StatusCode::NO_CONTENT)
}

/// Set of executions holding at least one resume signal: the canonical
/// "this execution is suspended" record (the engine doesn't journal
/// a terminal event for stalls).
pub(crate) async fn suspended_execution_id_set(
    state: &DispatcherState,
    project_id: uuid::Uuid,
) -> anyhow::Result<std::collections::HashSet<weft_core::ExecutionId>> {
    let signals = state.journal.signal_list_for_project(project_id).await?;
    Ok(signals
        .into_iter()
        .filter(|s| s.is_resume)
        .filter_map(|s| s.execution_id)
        .collect())
}

/// Count how many non-settled non-suspended executions a project
/// has right now, PLUS in-flight task rows that are about to become
/// one. `0` means deactivate-with-wait can flip status to Inactive
/// immediately.
///
/// The task rows matter for the lifecycle CAS: a `route_entry` task
/// is a fire that passed the gate but has not journaled
/// `ExecutionStarted` yet, and a pending `resume` task belongs to an
/// execution the suspended-set still excludes. Counting only the
/// journal would let the CAS flip a project Inactive while such a
/// fire is mid-route. Executions are unioned (a journaled execution with a
/// live execute task counts once); `route_entry` rows with no execution yet add
/// one each (each will mint a distinct execution).
///
/// `exclude_task`: discount one still-claimed task row; see
/// `journal_bridge::try_finish_drain`.
pub(crate) async fn running_count(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    exclude_task: Option<uuid::Uuid>,
) -> anyhow::Result<usize> {
    let (running, without_execution_id) = running_execution_ids(state, project_id, exclude_task).await?;
    Ok(running.len() + without_execution_id)
}

/// The executions running right now: every non-terminal, non-suspended
/// execution the journal knows, plus the executions of live tasks (a queued run
/// is running as far as a person is concerned), and beside them the
/// count of live tasks that have no execution yet (a run about to be born,
/// counted but not nameable).
pub(crate) async fn running_execution_ids(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    exclude_task: Option<uuid::Uuid>,
) -> anyhow::Result<(Vec<(weft_core::ExecutionId, weft_core::context::Phase)>, usize)> {
    let suspended_execution_ids = suspended_execution_id_set(state, project_id).await?;
    let execution_ids = state
        .journal
        .list_non_terminal_execution_ids_for_project(project_id)
        .await?;
    // Executions the journal already records as finished. A task row must
    // NEVER resurrect one of these: a completed/failed/cancelled
    // execution is not "running" even if a stray `pending`/`claimed`
    // task for its execution lingers (an orphaned task is a separate
    // concern, not a live execution).
    let terminal_execution_ids = state
        .journal
        .list_terminal_execution_ids_for_project(project_id)
        .await?;
    // A Vec, not a set, and the journal's own order is kept: oldest
    // first, so the last is the most recently started. The editor reads
    // it that way and a set would throw that away.
    let mut running: Vec<(weft_core::ExecutionId, weft_core::context::Phase)> = execution_ids
        .into_iter()
        .filter(|(c, _)| !suspended_execution_ids.contains(c))
        .collect();
    let task_rows: Vec<(uuid::Uuid, Option<String>)> = sqlx::query_as(
        "SELECT id, execution_id FROM task \
         WHERE project_id = $1 \
           AND kind IN ('route_entry', 'execute', 'resume') \
           AND status IN ('pending', 'claimed')",
    )
    .bind(project_id)
    .fetch_all(&state.pg_pool)
    .await?;
    let mut without_execution_id = 0usize;
    for (task_id, execution_id) in task_rows {
        if Some(task_id) == exclude_task {
            continue;
        }
        match execution_id {
            Some(c) => {
                let parsed: weft_core::ExecutionId = c
                    .parse()
                    .map_err(|e| anyhow::anyhow!("corrupt task.execution '{c}': {e}"))?;
                // Skip a task whose execution is journal-terminal (finished)
                // or suspended: neither is a running execution.
                if terminal_execution_ids.contains(&parsed) || suspended_execution_ids.contains(&parsed) {
                    continue;
                }
                // A queued run has no journal row yet, so it has no
                // start time to sort by and belongs after everything
                // that has one: it is the newest thing here. It is a
                // run of the graph: a setup journals its start before
                // it queues anything.
                if !running.iter().any(|(c, _)| *c == parsed) {
                    running.push((parsed, weft_core::context::Phase::Fire));
                }
            }
            None => without_execution_id += 1,
        }
    }
    Ok((running, without_execution_id))
}

/// How many executions the activations `keys` are still waiting on: the
/// running (non-suspended) runs their triggers fired, plus the fires of
/// their signals still being routed (a `route_entry` task has no execution
/// yet, and becomes a run of the activation). What a waiting deactivation
/// of those activations drains to zero. `exclude_task` discounts one
/// still-claimed task row (see `journal_bridge::try_finish_drain`).
pub(crate) async fn running_count_for(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    exclude_task: Option<uuid::Uuid>,
) -> anyhow::Result<usize> {
    if keys.is_empty() {
        return Ok(0);
    }
    let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
    let runs = crate::take_down::live_runs(state, project_id).await?;
    let running: std::collections::HashSet<weft_core::ExecutionId> = crate::take_down::affected_runs(&target, &runs, None)
        .into_iter()
        .filter(|r| !r.suspended)
        .map(|r| r.execution_id)
        .collect();
    let tokens: Vec<String> = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys)
        .await?
        .into_iter()
        .filter(|s| !s.is_resume)
        .map(|s| s.token)
        .collect();
    let routing: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task \
         WHERE project_id = $1 AND kind = 'route_entry' AND status IN ('pending', 'claimed') \
           AND execution_id IS NULL AND payload->>'token' = ANY($2) AND id IS DISTINCT FROM $3",
    )
    .bind(project_id)
    .bind(&tokens)
    .bind(exclude_task)
    .fetch_one(&state.pg_pool)
    .await?;
    Ok(running.len() + routing as usize)
}

/// Cancel every running (non-suspended) execution the activations `keys`
/// fired. What `cancel-running` and a waiting drain's cap do; `cause` is
/// what the cancelled runs' terminal records say happened.
pub(crate) async fn cancel_running_for(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    cause: weft_core::exec::CancelCause,
) -> Result<(), (StatusCode, String)> {
    let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
    let runs = crate::take_down::live_runs(state, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("live runs: {e}")))?;
    let targets: Vec<(weft_core::ExecutionId, &weft_core::exec::CancelCause)> =
        crate::take_down::affected_runs(&target, &runs, None)
            .into_iter()
            .filter(|r| !r.suspended)
            .map(|r| (r.execution_id, &cause))
            .collect();
    crate::api::execution::cancel_execution_ids(state, &targets)
        .await
        .map(|_| ())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e}")))
}

/// Spawn a worker for the TriggerSetup sub-execution and block
/// until it settles. The run's execution is recorded on the project row
/// before it starts, so a rollback (or a cancel-activate, or the
/// reaper) cancels just THIS execution and never touches
/// suspended/running work from prior cycles.
async fn run_trigger_setup(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    // The activations whose triggers this setup captures; they share one
    // owner, whose run it is (a member's triggers read that member's
    // values).
    keys: &[weft_core::activation::ActivationKey],
    // The hash of the SAME definition the caller resolved `keys`
    // against (one `coherent_definition` pair). Reading the project
    // row's hash here instead would race a concurrent re-register:
    // kicks from shape A journaled under hash B.
    program: &weft_core::project::hash::ProgramIdentity,
    activation: Option<uuid::Uuid>,
    // A member's change about to be stored, which this setup runs with in
    // place of what is stored (a store re-arming the triggers that read
    // it); nothing for every other setup.
    overlay: Option<&weft_core::member::ValueChanges>,
) -> Result<crate::journal::TriggerBake, (StatusCode, String)> {
    let execution_id = activation.unwrap_or_else(uuid::Uuid::new_v4);
    let places = key_places(project, keys);
    let selection = weft_core::project::selection::RunSelection::setup(project, &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let kicks = setup_kicks(project, places).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let member = keys_member(keys);
    // A member's triggers are set up with what that member provides.
    let member_values = match member {
        Some(member) => member_values_for_run(state, project_id, project, &selection, member, overlay).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, project, &selection).await?;

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to
    // the completion event.
    let mut events = state.events.subscribe_project(project_id).await;

    // The activation reserved this execution when it began. A cancelled
    // driver cannot replace a newer activation's identity.
    let owned = |activations: Vec<crate::activation_store::Activation>| {
        activations.iter().any(|a| {
            a.lifecycle.status == crate::activation_store::ProjectStatus::Activating
                && a.lifecycle.activating_execution_id == Some(execution_id)
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
    // rows and no task; the execution the row recorded above then names a
    // run that never existed, which the rollback's cancel finds ownerless
    // and skips.
    let entry_node = kick_place(project, &kicks[0]);
    start_queued_execution(
        state,
        Birth {
            execution_id,
            project_id,
            phase: weft_core::context::Phase::TriggerSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            program,
            subgraph: Some(&selection),
            seed: None,
            source_version: None,
            member: member.map(|member| RunFor { member, values: &member_values }),
            picks: &picks,
            fired_trigger: None,
            run_kind: weft_core::exec::RunKind::Execution,
            run_class: weft_core::run_class::RunClass::Short,
            at_unix: crate::lease::now_unix() as u64,
        },
        activation,
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
        crate::api::execution::cancel_execution_id(
            state,
            execution_id,
            &weft_core::exec::CancelCause::Runtime {
                detail: "the activation ended before this trigger setup run started".into(),
            },
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel_execution_id: {e}")))?;
        return Err((
            StatusCode::CONFLICT,
            "the activation was cancelled as trigger setup started".into(),
        ));
    }

    // No backend-imposed deadline. Trigger setup spans worker
    // spawn + image pull + fold + run + bridge wakeup; on a cold
    // install the legitimate path is just slow, and a hard timeout
    // would surface that as a 504 even though the work is still in
    // flight. The CLI / extension is the right layer to choose a
    // client-side patience budget.
    loop {
        match events.recv().await.map(|record| record.event) {
            Ok(crate::events::DispatcherEvent::ExecutionCompleted { execution_id: c, .. })
                if c == execution_id =>
            {
                break;
            }
            Ok(crate::events::DispatcherEvent::ExecutionFailed { execution_id: c, error, .. })
                if c == execution_id =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup failed: {error}"),
                ));
            }
            Ok(crate::events::DispatcherEvent::ExecutionCancelled { execution_id: c, reason, .. })
                if c == execution_id =>
            {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("trigger setup cancelled: {reason}"),
                ));
            }
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Dropped batch may have held this execution's terminal.
                // The journal is authoritative: re-query rather than
                // fail the trigger-setup on a transient lag.
                match crate::api::execution::terminal_outcome(&state.pg_pool, execution_id).await {
                    Ok(Some(crate::api::execution::TerminalOutcome::Completed)) => break,
                    Ok(Some(crate::api::execution::TerminalOutcome::Failed)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup failed".into(),
                        ))
                    }
                    Ok(Some(crate::api::execution::TerminalOutcome::Cancelled)) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "trigger setup cancelled".into(),
                        ))
                    }
                    Ok(None) => continue, // still in flight; keep waiting
                    Err(e) => {
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("trigger setup terminal lookup: {e}"),
                        ))
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "trigger setup event stream closed".into(),
                ));
            }
        }
    }
    let events = state.journal.events_log(execution_id).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("read trigger setup: {error:#}")))?;
    let mut bake = crate::journal::TriggerBake::from_events(&events)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| (StatusCode::CONFLICT, "trigger setup did not complete successfully".into()))?;
    bake.targets = keys.iter().map(|key| key.trigger.clone()).collect();
    state.journal.finish_trigger_setup(execution_id, Some(&bake)).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("publish trigger bake: {error:#}")))?;
    Ok(bake)
}

/// After a trigger-setup sub-exec, collect every persisted signal
/// for the project that has a user-facing URL. These become the
/// `urls` in ActivateResponse.
async fn collect_listener_urls(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
) -> anyhow::Result<Vec<ActivationUrl>> {
    let signals = crate::journal::postgres::activation_signals(&state.pg_pool, project_id, keys).await?;
    let mut out = Vec::new();
    for meta in signals {
        if meta.is_resume {
            continue;
        }
        if let Some(url) = meta.public_url(state.external_base_url()) {
            out.push(ActivationUrl {
                node_id: meta.node_id.clone(),
                url,
            });
        }
    }
    Ok(out)
}


// `crate::lease::now_unix` is the canonical wall-clock reader.

#[cfg(test)]
mod trigger_kick_tests {
    use super::*;
    use weft_core::project::{infra_places, trigger_places};
    use weft_core::project::{GroupBoundary, GroupBoundaryRole};

    /// Build a minimal ProjectDefinition from a JSON spec. Tests
    /// only care about node id, trigger status, the scope a node sits
    /// in, and edges; everything else is defaulted. The third element
    /// is the node's scope chain (empty at the top level).
    fn project(nodes: &[(&str, bool, &[&str])], edges: &[(&str, &str)]) -> ProjectDefinition {
        let mut n_json = Vec::new();
        for (id, is_trigger, scope) in nodes {
            n_json.push(serde_json::json!({
                "id": id,
                "nodeType": "T",
                "label": null,
                "config": {},
                "position": { "x": 0, "y": 0 },
                "scope": scope,
                "features": {
                    "isTrigger": is_trigger,
                },
            }));
        }
        let e_json: Vec<Value> = edges
            .iter()
            .map(|(s, t)| {
                // The names `Edge` reads (see `infra_kick_and_dep_tests`
                // for what the older `sourcePort` spelling silently did).
                // One port name on both ends: a boundary's port keeps its
                // name across it, and these tests are about which nodes a
                // kick reaches, not which port feeds which.
                serde_json::json!({
                    "id": format!("e_{}_{}", s, t),
                    "source": s,
                    "sourceHandle": "v",
                    "target": t,
                    "targetHandle": "v"
                })
            })
            .collect();
        // A `{g}__in` id is the In boundary of the group `g`, the way the
        // compiler flattens one, and the group has to EXIST for the walk
        // to treat the boundary as one: with no `groups` entry the
        // boundary is an ordinary node and every "group" test here would
        // pass without touching the boundary logic it is about.
        let groups: Vec<Value> = nodes
            .iter()
            .filter_map(|(id, _, _)| id.strip_suffix("__in"))
            .map(|g| serde_json::json!({ "id": g, "kind": "group" }))
            .collect();
        let body = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": n_json,
            "edges": e_json,
            "groups": groups
        });
        serde_json::from_value(body).expect("valid test project")
    }

    fn ids(kicks: &[Kick]) -> Vec<String> {
        let mut v: Vec<String> = kicks.iter().map(|k| k.node.clone()).collect();
        v.sort();
        v
    }

    fn sorted(set: &weft_core::project::selection::RunSelection) -> Vec<String> {
        set.nodes.iter().map(|place| place.to_string()).collect()
    }

    fn fire(p: &ProjectDefinition, node: &str, payload: Value) -> TriggerFire {
        compute_trigger_fire(p, node, &payload, None).expect("the fire reaches an output")
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
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["trigger_x".to_string()],
            "only the firing trigger should be a kick"
        );
        assert_eq!(kicks[0].payload, Some(Value::String("payload".into())));
        assert_eq!(
            sorted(&subgraph),
            vec!["b".to_string(), "out".to_string(), "trigger_x".to_string()],
            "A feeds only the trigger, so it is outside the fire's subgraph"
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
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("payload".into()));
        assert_eq!(
            ids(&kicks),
            vec!["a".to_string(), "trigger_x".to_string()],
            "A must run via its non-trigger path; trigger carries payload"
        );
        assert_eq!(sorted(&subgraph), vec!["a", "b", "c", "out", "trigger_x"]);
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
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("fire".into()));
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string(), "trigger_y".to_string()]);
        assert_eq!(sorted(&subgraph), vec!["out", "trigger_x", "trigger_y"]);
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
        // TriggerX ──► StepX      TriggerY ──► StepY
        // Nothing joins the two, so firing one records a subgraph holding
        // only its own path: the graph view dims the rest of the program
        // for that execution, the same way it does for an authored cut.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("step_x", false, &[]),
                ("trigger_y", true, &[]),
                ("step_y", false, &[]),
            ],
            &[("trigger_x", "step_x"), ("trigger_y", "step_y")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&subgraph), vec!["step_x", "trigger_x"]);
    }

    #[test]
    fn everything_the_trigger_reaches_runs() {
        // TriggerX ──► DeadEnd: nothing declares itself a deliverable
        // any more; whatever the fire reaches runs.
        let p = project(
            &[("trigger_x", true, &[]), ("dead_end", false, &[])],
            &[("trigger_x", "dead_end")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()]);
        assert_eq!(sorted(&subgraph), vec!["dead_end", "trigger_x"]);
    }

    /// A trigger sitting in the middle of a group fires there: it is
    /// the run's one kick, the group comes whole around it (the sibling
    /// its output feeds, and the sibling's other input's source), and
    /// nothing outside the group is kicked to reach it.
    #[test]
    fn a_trigger_in_the_middle_of_a_group_starts_the_run_from_itself() {
        // cfg ──► g__in ──► g.a ──► g.join ──► g__out ──► out
        //                   g.trig ─────────┘
        let mut p = project(
            &[
                ("cfg", false, &[]),
                ("g__in", false, &[]),
                ("g.a", false, &["g"]),
                ("g.trig", true, &["g"]),
                ("g.join", false, &["g"]),
                ("g__out", false, &[]),
                ("out", false, &[]),
            ],
            &[
                ("cfg", "g__in"),
                ("g__in", "g.a"),
                ("g.a", "g.join"),
                ("g.trig", "g.join"),
                ("g.join", "g__out"),
                ("g__out", "out"),
            ],
        );
        for (id, role) in [("g__in", GroupBoundaryRole::In), ("g__out", GroupBoundaryRole::Out)] {
            let node = p.nodes.iter_mut().find(|n| n.id == id).expect(id);
            node.group_boundary = Some(GroupBoundary { group_id: "g".into(), role });
        }
        let TriggerFire { kicks, subgraph } = fire(&p, "g.trig", serde_json::json!({ "hi": 1 }));
        assert_eq!(
            sorted(&subgraph),
            vec!["cfg", "g.a", "g.join", "g.trig", "g__in", "g__out", "out"],
            "the run includes the join's dependencies and the selected group path"
        );
        let firing: Vec<&str> = kicks.iter().filter(|k| k.firing).map(|k| k.node.as_str()).collect();
        assert_eq!(firing, vec!["g.trig"], "the trigger is the one firing kick");
        assert_eq!(
            ids(&kicks),
            vec!["cfg".to_string(), "g.trig".to_string()],
            "the group's input source is kicked so g.join's other side arrives; the group's \
             members start when the group does"
        );
    }

    #[test]
    fn a_scope_the_fire_touches_excludes_unrelated_members() {
        // TriggerX ──► g__in ──► g.a ──► g__out ──► Out
        //              g.seed (no wire feeds it; the scope launcher's)
        // Reaching the group's input follows only connected work.
        // Its unwired seed is outside this trigger's program.
        let p = project(
            &[
                ("trigger_x", true, &[]),
                ("g__in", false, &[]),
                ("g.a", false, &["g"]),
                ("g.seed", false, &["g"]),
                ("g__out", false, &[]),
                ("out", false, &[]),
            ],
            &[("trigger_x", "g__in"), ("g__in", "g.a"), ("g.a", "g__out"), ("g__out", "out")],
        );
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::Null);
        assert_eq!(ids(&kicks), vec!["trigger_x".to_string()], "the body's seed is not the run's root");
        assert_eq!(sorted(&subgraph), vec!["g.a", "g__in", "g__out", "out", "trigger_x"]);
    }

    #[test]
    fn setup_phases_reach_a_trigger_or_infra_node_inside_a_group() {
        // cfg ──► g__in ──► g.db (infra, unwired inside the body)
        //                   g.trig (trigger, unwired inside the body)
        // Each setup keeps its own target and enclosing gate. Neither
        // the unused group input nor its source joins the selection.
        let mut p = project(
            &[
                ("cfg", false, &[]),
                ("g__in", false, &[]),
                ("g.db", false, &["g"]),
                ("g.trig", true, &["g"]),
                ("g__out", false, &[]),
            ],
            &[("cfg", "g__in")],
        );
        for (id, role) in [("g__in", GroupBoundaryRole::In), ("g__out", GroupBoundaryRole::Out)] {
            let node = p.nodes.iter_mut().find(|n| n.id == id).expect(id);
            node.group_boundary = Some(GroupBoundary { group_id: "g".into(), role });
        }
        p.nodes.iter_mut().find(|n| n.id == "g.db").unwrap().requires_infra = true;
        p.groups.push(serde_json::from_value(serde_json::json!({
            "id": "g", "kind": "group", "nodeIds": ["g.db", "g.trig"]
        })).unwrap());
        assert_eq!(ids(&setup_kicks(&p, infra_places(&p)).unwrap()), vec!["g.db".to_string(), "g__in".to_string()]);
        assert_eq!(ids(&setup_kicks(&p, trigger_places(&p)).unwrap()), vec!["g.trig".to_string(), "g__in".to_string()]);
        p.groups[0].kind = weft_core::project::GroupKind::Loop { loop_config: serde_json::json!({}) };
        assert!(setup_kicks(&p, infra_places(&p)).unwrap_err().contains("inside loop"));
        assert!(setup_kicks(&p, trigger_places(&p)).unwrap_err().contains("inside loop"));
        assert!(compute_trigger_fire(&p, "g.trig", &Value::Null, None).unwrap_err().contains("inside loop"));
    }

    #[test]
    fn firing_non_trigger_returns_an_error() {
        let p = project(
            &[("a", false, &[]), ("out", false, &[])],
            &[("a", "out")],
        );
        assert!(compute_trigger_fire(&p, "a", &Value::Null, None).unwrap_err().contains("not a trigger"));
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
        let TriggerFire { kicks, subgraph } = fire(&p, "trigger_x", Value::String("msg".into()));
        assert_eq!(ids(&kicks), vec!["shared".to_string(), "trigger_x".to_string()]);
        assert_eq!(
            sorted(&subgraph),
            vec!["bx", "out_x", "shared", "trigger_x"],
            "the other program's trigger and consumers stay outside the fire"
        );
    }
}

#[cfg(test)]
mod infra_kick_and_dep_tests {
    use super::*;
    use weft_core::project::infra_places;

    /// (id, is_trigger, requires_infra). Shared with `run_subgraph_tests`.
    pub(super) fn project(nodes: &[(&str, bool, bool)], edges: &[(&str, &str)]) -> ProjectDefinition {
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

    #[test]
    fn trigger_deps_direct_chain() {
        // infra → trigger
        let p = project(
            &[("infra", false, true), ("trigger", true, false)],
            &[("infra", "trigger")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_indirect_chain() {
        // infra → middle → trigger
        let p = project(
            &[
                ("infra", false, true),
                ("middle", false, false),
                ("trigger", true, false),
            ],
            &[("infra", "middle"), ("middle", "trigger")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_skip_unrelated_triggers() {
        // infraA → triggerA ; infraB and triggerB are not connected.
        let p = project(
            &[
                ("infraA", false, true),
                ("infraB", false, true),
                ("triggerA", true, false),
                ("triggerB", true, false),
            ],
            &[("infraA", "triggerA")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infraA".to_string(), "triggerA".to_string())]);
    }

    #[test]
    fn trigger_deps_one_infra_two_triggers() {
        // infra → t1 ; infra → t2
        let p = project(
            &[("infra", false, true), ("t1", true, false), ("t2", true, false)],
            &[("infra", "t1"), ("infra", "t2")],
        );
        let deps = compute_trigger_deps(&p);
        // Sorted by (infra, trigger).
        assert_eq!(
            deps,
            vec![
                ("infra".to_string(), "t1".to_string()),
                ("infra".to_string(), "t2".to_string()),
            ]
        );
    }

    #[test]
    fn trigger_deps_empty_when_no_triggers() {
        let p = project(
            &[("infra", false, true), ("downstream", false, false)],
            &[("infra", "downstream")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }

    #[test]
    fn trigger_deps_empty_when_no_infra() {
        let p = project(
            &[("trigger", true, false), ("upstream", false, false)],
            &[("upstream", "trigger")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }

    #[test]
    fn trigger_deps_reach_through_an_ordinary_node() {
        // infra -> text -> trigger: the dependency is just as real one
        // hop further out.
        let p = project(
            &[
                ("text", false, false),
                ("trigger", true, false),
                ("infra", false, true),
            ],
            &[("text", "trigger"), ("infra", "text")],
        );
        let deps = compute_trigger_deps(&p);
        assert_eq!(deps, vec![("infra".to_string(), "trigger".to_string())]);
    }

    #[test]
    fn trigger_deps_skip_when_trigger_does_not_reach_infra() {
        // trigger -> text, and infra sits downstream of the trigger.
        // Stopping it cannot break an armed registration, so the guard
        // must not refuse that stop. The name of this test claimed the
        // negative case for a long time while its body asserted the
        // positive one; this is the negative case.
        let p = project(
            &[
                ("trigger", true, false),
                ("text", false, false),
                ("infra", false, true),
            ],
            &[("trigger", "text"), ("text", "infra")],
        );
        assert!(compute_trigger_deps(&p).is_empty());
    }
}

#[cfg(test)]
mod resolve_scope_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;
    use weft_core::activation::ActivationScope;

    fn shared() -> ActivationScope {
        ActivationScope::default()
    }

    fn member() -> ActivationScope {
        ActivationScope { triggers: Vec::new(), member: Some("alice".parse().expect("member id")) }
    }

    /// A program whose only trigger exists once per member.
    fn per_member_only() -> ProjectDefinition {
        let mut p = project(&[("t", true, false), ("a", false, false)], &[("t", "a")]);
        p.nodes.iter_mut().find(|n| n.id == "t").expect("t").per_member = Some(weft_core::member::PerMember::Marked);
        p
    }

    /// No triggers at all: the verbs that set triggers up refuse and say
    /// so, for the program and for a member alike.
    #[test]
    fn no_triggers_refuses_with_its_own_message() {
        let p = project(&[("a", false, false)], &[]);
        for scope in [shared(), member()] {
            let (status, msg) = resolve_scope(&p, &scope).expect_err("nothing to resolve");
            assert_eq!(status, StatusCode::PRECONDITION_FAILED);
            assert_eq!(msg, "this program has no triggers");
        }
    }

    fn row(trigger: &str, member: Option<&str>) -> crate::activation_store::Activation {
        crate::activation_store::Activation {
            key: weft_core::activation::ActivationKey::new(
                trigger,
                weft_core::member::Owner::from_member(member.map(|m| m.parse().expect("member id"))),
            ),
            lifecycle: crate::activation_store::ActivationLifecycle::active(),
            program: None,
            source_version: None,
        }
    }

    /// Nothing activated: a take-down (deactivate, cancel-running,
    /// `weft rm --journal`'s quiesce) has nothing to do.
    #[test]
    fn nothing_activated_takes_down_nothing() {
        for scope in [shared(), member()] {
            assert!(keys_to_take_down(&[], &scope).expect("no-op").is_empty());
        }
    }

    /// A take-down picks the owner's rows, whatever the source says now:
    /// a trigger the source dropped is still taken down, and the other
    /// owners' rows are left alone.
    #[test]
    fn a_take_down_picks_the_owners_rows() {
        let rows = [row("gone", None), row("door", None), row("door", Some("alice")), row("door", Some("bob"))];
        let triggers = |keys: Vec<weft_core::activation::ActivationKey>| {
            keys.into_iter().map(|k| (k.trigger, k.owner.member().map(|m| m.to_string()))).collect::<Vec<_>>()
        };
        assert_eq!(
            triggers(keys_to_take_down(&rows, &shared()).unwrap()),
            vec![("gone".to_string(), None), ("door".to_string(), None)]
        );
        assert_eq!(triggers(keys_to_take_down(&rows, &member()).unwrap()), vec![("door".to_string(), Some("alice".to_string()))]);
        let named = ActivationScope { triggers: vec!["gone".into()], member: None };
        assert_eq!(triggers(keys_to_take_down(&rows, &named).unwrap()), vec![("gone".to_string(), None)]);
    }

    /// A named trigger with no row for the owner is a mistake.
    #[test]
    fn a_named_trigger_never_activated_is_refused() {
        let rows = [row("door", None)];
        let scope = ActivationScope { triggers: vec!["door".into()], member: Some("alice".parse().expect("member id")) };
        let (status, msg) = keys_to_take_down(&rows, &scope).expect_err("alice's door never activated");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(msg.contains("'door' of member 'alice'"), "{msg}");
    }

    /// Only per-member triggers: the program's own scope names nothing,
    /// and the refusal points at --member.
    #[test]
    fn per_member_only_refuses_shared_scope() {
        let p = per_member_only();
        let (status, msg) = resolve_scope(&p, &shared()).expect_err("no shared trigger");
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert!(msg.contains("no shared trigger"), "{msg}");
        assert_eq!(resolve_scope(&p, &member()).expect("member's trigger").len(), 1);
    }
}

#[cfg(test)]
mod run_subgraph_tests {
    use super::infra_kick_and_dep_tests::project;
    use super::*;

    /// The fire-side aim: what a fire that reached `targets` runs.
    fn aimed_at(p: &ProjectDefinition, targets: &[&str]) -> weft_core::project::selection::RunSelection {
        weft_core::project::selection::RunSelection::carve(p, &weft_core::project::selection::SelectionBounds {
            target: targets.iter().map(|s| s.to_string()).collect(), ..Default::default()
        }).unwrap()
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
        let mut nodes: Vec<&str> = sub.nodes.iter().map(|s| s.id.as_str()).collect();
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
        assert!(sub.nodes.contains(&Located::top("trig")));
        assert!(!sub.nodes.contains(&Located::top("setup")), "the trigger's own upstream stays out");

        let resolved = weft_core::run_spec::resolve_spec(&weft_core::run_spec::RunSpec {
            target: vec!["out".into()], ..weft_core::run_spec::RunSpec::whole("x")
        }, &p).unwrap();
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
        let kicks = compute_trigger_fire(&p, "trig", &payload, Some(&snapshot)).unwrap().kicks;
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
            member: None,
            instance_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: Default::default(),
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
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
    use crate::activation_store::{ActivationLifecycle as ProjectLifecycle, ProjectStatus};
    use crate::project_store::ProjectTransition;

    struct Case {
        lifecycle: ProjectLifecycle,
        transition: ProjectTransition,
        has_triggers: bool,
        has_infra: bool,
        trigger_infra_ready: bool,
        orphaned_infra: bool,
        infra_rollup: &'static str,
        infra_busy: bool,
        drift: DriftBits,
        preservation: PreservationCounts,
        running_count: usize,
    }

    impl Default for Case {
        fn default() -> Self {
            Self {
                lifecycle: ProjectLifecycle::wiped(),
                transition: ProjectTransition::None,
                has_triggers: true,
                has_infra: false,
                trigger_infra_ready: true,
                orphaned_infra: false,
                infra_rollup: "none",
                infra_busy: false,
                drift: DriftBits::default(),
                preservation: PreservationCounts::default(),
                running_count: 0,
            }
        }
    }

    fn actions(case: &Case) -> Vec<String> {
        compute_available_actions(&ActionInputs {
            lifecycle: &case.lifecycle,
            transition: case.transition,
            has_triggers: case.has_triggers,
            has_infra: case.has_infra,
            trigger_infra_ready: case.trigger_infra_ready,
            run_infra_ready: super::run_infra_ready(case.has_infra, case.infra_rollup),
            orphaned_infra: case.orphaned_infra,
            infra_rollup: case.infra_rollup,
            infra_busy: case.infra_busy,
            drift: &case.drift,
            preservation: &case.preservation,
            running_count: case.running_count,
        })
    }

    fn assert_actions(case: &Case, expected: &[&str]) {
        assert_eq!(actions(case), expected.to_vec());
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
    fn deactivating_offers_cancel_running_and_resume() {
        let deactivating = ProjectLifecycle::deactivating_to(ProjectLifecycle::parked(), 0);
        assert_actions(
            &Case { lifecycle: deactivating.clone(), running_count: 2, ..Case::default() },
            &["cancel_running", "resume_active"],
        );
        // Nothing left to cancel once the running set empties.
        assert_actions(
            &Case { lifecycle: deactivating, running_count: 0, ..Case::default() },
            &["resume_active"],
        );
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
        // A claimed stop still draining (or an InfraSetup execution
        // before any node row flips) leaves the rollup reading
        // "running"; the in-flight fact must still collapse the row
        // to cancel-only, or new runs would starve the drain.
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
    fn inactive_no_infra_offers_run_and_activate() {
        assert_actions(&Case::default(), &["run", "activate"]);
    }

    #[test]
    fn no_triggers_hides_activate_but_run_stands() {
        assert_actions(&Case { has_triggers: false, ..Case::default() }, &["run"]);
    }

    #[test]
    fn inactive_infra_resting_still_offers_activate_when_no_trigger_needs_it() {
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
    fn inactive_infra_running_offers_everything_stable() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "running", ..Case::default() },
            &["run", "activate", "infra_stop", "infra_terminate"],
        );
        // Infra drift additionally lights upgrade.
        assert_actions(
            &Case {
                has_infra: true,
                infra_rollup: "running",
                drift: DriftBits { infra_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "activate", "infra_stop", "infra_terminate", "infra_upgrade"],
        );
    }

    #[test]
    fn inactive_infra_degraded_holds_activate_back_only_when_a_trigger_needs_it() {
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
    fn inactive_infra_stopped_offers_start_and_terminate() {
        assert_actions(
            &Case { has_infra: true, infra_rollup: "stopped", ..Case::default() },
            &["activate", "infra_start", "infra_terminate"],
        );
    }

    /// The program's own triggers are off, a member's are on and lag the
    /// code: the drift bit counts that member's, and resync is offered
    /// beside the activate that would turn the program's own back on.
    #[test]
    fn a_members_drift_lights_resync_while_the_program_is_off() {
        assert_actions(
            &Case { drift: DriftBits { activation_drift: true, ..Default::default() }, ..Case::default() },
            &["run", "activate", "resync"],
        );
    }

    #[test]
    fn active_offers_run_and_deactivate_and_drift_lights_resync() {
        let active = ProjectLifecycle::active();
        assert_actions(
            &Case { lifecycle: active.clone(), ..Case::default() },
            &["run", "deactivate"],
        );
        assert_actions(
            &Case {
                lifecycle: active.clone(),
                drift: DriftBits { activation_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "deactivate", "resync"],
        );
        // Resync re-arms the triggers, so the infra they depend on
        // gates it the way it gates activate: the door would 412.
        assert_actions(
            &Case {
                lifecycle: active,
                has_infra: true,
                infra_rollup: "running",
                trigger_infra_ready: false,
                drift: DriftBits { activation_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn a_build_does_not_erase_activation_drift() {
        let program = weft_core::project::hash::ProgramIdentity {
            binary_hash: "old-code".into(), definition_hash: "same-graph".into(), implementations: Default::default(),
        };
        let query = StatusQuery { desired_binary_hash: Some("new-code".into()), desired_definition_hash: Some("same-graph".into()), ..Default::default() };
        assert!(activation_has_drifted(&query, Some("new-code"), Some("same-graph"), Some(&program), false));
        assert!(activation_has_drifted(&StatusQuery::default(), Some("new-code"), Some("same-graph"), Some(&program), false));
        assert!(!activation_has_drifted(&StatusQuery::default(), Some("old-code"), Some("same-graph"), Some(&program), false));
        // Activated before the code identity was recorded: only the graph
        // hash can speak, so an upgrade does not light resync on its own.
        assert!(!activation_has_drifted(&query, Some("new-code"), Some("same-graph"), None, false));
        assert!(activation_has_drifted(&query, Some("new-code"), Some("same-graph"), None, true));
    }

    #[test]
    fn active_with_last_trigger_deleted_keeps_deactivate() {
        // Trigger divergence: the source no longer has triggers but the
        // backend is still active; deactivate must stay offered so the
        // user can bring the live trigger down.
        assert_actions(
            &Case { lifecycle: ProjectLifecycle::active(), has_triggers: false, ..Case::default() },
            &["run", "deactivate"],
        );
    }

    #[test]
    fn active_infra_running_keeps_stop_and_terminate() {
        // Resolved cell Q2: stop/terminate while active auto-deactivate
        // first (one click), so both stay offered.
        assert_actions(
            &Case {
                lifecycle: ProjectLifecycle::active(),
                has_infra: true,
                infra_rollup: "running",
                ..Case::default()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    #[test]
    fn preserved_state_flips_activate_to_reactivate_only_when_inactive() {
        let preserved = PreservationCounts { parked: 2, suspended: 1 };
        assert_actions(
            &Case {
                preservation: PreservationCounts { ..preserved },
                ..Case::default()
            },
            &["run", "reactivate"],
        );
        // Registered (never activated) always offers plain activate.
        let mut registered = ProjectLifecycle::wiped();
        registered.status = ProjectStatus::Registered;
        assert_actions(
            &Case {
                lifecycle: registered,
                preservation: PreservationCounts { parked: 2, suspended: 1 },
                ..Case::default()
            },
            &["run", "activate"],
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
            &Case { orphaned_infra: true, infra_rollup: "running", ..Case::default() },
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

#[cfg(test)]
mod status_query_wire_shape_tests {
    use super::*;

    /// `StatusQuery`'s wire shape is the contract with the CLI's URL
    /// builder (and any future direct caller from the extension).
    /// A typo in one of the three `rename = "desiredXyzHash"`
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
            "desiredDefinitionHash": "def",
            "desiredInfraHash": "ghi",
        });
        let q: StatusQuery = serde_json::from_value(json)
            .expect("camelCase keys must deserialize");
        assert_eq!(q.desired_binary_hash.as_deref(), Some("abc"));
        assert_eq!(q.desired_definition_hash.as_deref(), Some("def"));
        assert_eq!(q.desired_infra_hash.as_deref(), Some("ghi"));
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
    use super::infra_entries;
    use crate::infra_node::{InfraNodeRow, InfraNodeStatus, ObservedCopies};
    use weft_core::member::MemberId;

    fn row(node_id: &str, member: Option<&str>, status: InfraNodeStatus) -> InfraNodeRow {
        InfraNodeRow {
            project_id: uuid::Uuid::nil(),
            node_id: node_id.into(),
            member: member.map(|m| MemberId::new(m).unwrap()),
            instance_id: String::new(),
            status,
            failure_stage: None,
            failure_message: None,
            applied_spec_hash: None,
            applied_at_unix: None,
            endpoints: Default::default(),
            public_paths: Default::default(),
            doors: Default::default(),
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
        }
    }

    /// Every infra place the program declares is listed, started or not:
    /// a shared one with its copy's state (or `not_started`, or
    /// `provisioning` while a start brings it up), a `@per_member` one as
    /// `per_member` with its members' copies counted. Member copies are
    /// listed on their own, a starting one included; a node that needs no
    /// infra is not listed.
    #[test]
    fn every_declared_infra_place_is_listed() {
        let mut project = super::infra_kick_and_dep_tests::project(
            &[("db", false, true), ("cache", false, true), ("queue", false, true), ("bridge", false, true), ("plain", false, false)],
            &[],
        );
        project.nodes.iter_mut().find(|n| n.id == "bridge").unwrap().per_member = Some(weft_core::member::PerMember::Marked);
        let copies = ObservedCopies {
            rows: vec![row("db", None, InfraNodeStatus::Running), row("bridge", Some("ada"), InfraNodeStatus::Stopped)],
            starting: vec![("queue".into(), None), ("bridge".into(), Some(MemberId::new("bob").unwrap()))],
        };
        let (infra, members) = infra_entries(&project, &copies);
        let listed: Vec<(&str, &str, Option<usize>)> =
            infra.iter().map(|e| (e.node.as_str(), e.status.as_str(), e.member_copies)).collect();
        assert_eq!(
            listed,
            [
                ("bridge", "per_member", Some(2)),
                ("cache", "not_started", None),
                ("db", "running", None),
                ("queue", "provisioning", None),
            ]
        );
        let members: Vec<(&str, &str, &str)> =
            members.iter().map(|c| (c.node.as_str(), c.member.as_str(), c.status.as_str())).collect();
        assert_eq!(members, [("bridge", "ada", "stopped"), ("bridge", "bob", "provisioning")]);
    }

    /// A program with no infra node lists none, which is the one case the
    /// CLI says no node declares `requires_infra`.
    #[test]
    fn a_program_without_infra_lists_none() {
        let project = super::infra_kick_and_dep_tests::project(&[("plain", false, false)], &[]);
        let (infra, members) = infra_entries(&project, &ObservedCopies::default());
        assert!(infra.is_empty() && members.is_empty());
    }
}
