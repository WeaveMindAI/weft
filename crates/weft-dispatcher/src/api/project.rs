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
use serde::Deserialize;

use weft_core::activation::{
    ActivateRequest, ActivateResponse, ActivationTarget, ActivationUrl, BakeRequest, ReactivateChoice, ResyncResponse,
};
use weft_core::deactivation::{whose_triggers, DeactivateRequest, DeactivateResponse, ResyncRequest};
use weft_core::projects::{
    ActivationEntry, DeclareRequest, InstanceInfraEntry, LimitedEntry, PreservationCounts, ProjectDrift,
    ProjectExecutionsSummary, ProjectInfraEntry, ProjectStatusResponse, ProjectSummary, RunningExecution, StatusQuery,
};
use weft_core::infra::wire::{INFRA_NOT_STARTED, INFRA_PER_INSTANCE};
use weft_core::frames::Located;
use weft_core::infra::run_gate::{copies_read, infra_not_running, missing_and_fix, MissingCopy};
use weft_core::ProjectDefinition;

use crate::authenticator::{authorize_project, CallerTenant};
use crate::events::DispatcherEvent;
use crate::state::DispatcherState;

pub use weft_core::run_spec::KickPlan as Kick;

/// The summary of one project: its shared activations read as one
/// status (`activation_store::aggregate`).
async fn summary_of(
    state: &DispatcherState,
    summary: crate::project_store::StoredProjectSummary,
) -> Result<ProjectSummary, (StatusCode, String)> {
    let activations = state
        .activations
        .list(summary.id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let status = crate::activation_store::aggregate(
        activations.iter().filter(|a| a.key.owner == weft_core::instance::Owner::Shared).map(|a| &a.lifecycle),
    )
    .status;
    Ok(ProjectSummary {
        id: summary.id.to_string(),
        name: summary.name,
        description: summary.description,
        status: status.as_str().to_string(),
    })
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
    Json(req): Json<weft_core::builds::VersionBuildRequest>,
) -> Result<Json<weft_core::builds::BuiltProgram>, (StatusCode, String)> {
    declare_project(&state, id, &req.name, &caller.0).await?;
    crate::api::versions::validate_manifest(&req.manifest)?;
    // Claims every image the build plans from its first look at the
    // registry until the registration commits, so no prune deletes an
    // image this build found or built before anything references it
    // (`crate::build::prune::ImageHold`).
    let hold = crate::build::prune::ImageHold::new(&state.pg_pool, id);
    let crate::build::Build { program: mut built, images } =
        crate::transition::build_version_gated(&state, id, &caller.0, &req, &hold).await?;
    hold.confirm().await.map_err(|e| (StatusCode::CONFLICT, format!("{e:#}")))?;
    // Read before registering: which infra places this build moves onto
    // another image, for the caller to say so.
    let before = state
        .projects
        .running_infra_image_tags(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the registered infra images: {e:#}")))?
        .unwrap_or_default();
    built.replaced_infra_images = crate::build::replaced_infra_images(&before, &built.infra_images);
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
    crate::build::ledger::note_running(&state.pg_pool, id, &images, crate::lease::now_unix())
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
) -> Result<Json<weft_core::projects::ProjectRemoved>, StatusError> {
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
    // Its frontends first: one whose service cannot be removed stops the
    // removal with everything else still whole (`--force` forgets it, and
    // the answer names what stays on the cloud).
    let mut left = crate::frontends::remove_project(&state, &tenant, id, query.force).await.map_err(|e| {
        (StatusCode::BAD_GATEWAY, format!("remove the project's frontends: {e:#}; retry `weft rm`, or `weft rm --force`"))
    })?;
    // Its workers, and what the platform keeps for them (on a cloud the
    // service, its revisions and its account; here stopped containers),
    // the same way: what cannot be removed stops the removal, unless
    // `--force`, whose answer names what stays.
    match state.runner.retire(tenant.as_str(), id).await {
        Ok(()) => {}
        Err(e) if query.force => left.push(format!("the project's workers and what the platform keeps for them: {e:#}")),
        Err(e) => {
            return Err(StatusError::Other(
                StatusCode::BAD_GATEWAY,
                format!("remove the project's workers: {e:#}; retry `weft rm`, or `weft rm --force`"),
            ))
        }
    }
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
    // Its domains go with the row, and the platform's door follows the
    // rows on its own (`crate::domains::drain_loop`).
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
        // Its queued work goes too: nothing can serve it now. Logged,
        // not fatal, for the same reason as below; the reaper's loop
        // repeats it.
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
        Ok(Json(weft_core::projects::ProjectRemoved { left }))
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
    let versions = state.versions.retire_unused_versions(project_id).await?;
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
) -> Result<(weft_core::project::hash::ProgramIdentity, std::sync::Arc<ProjectDefinition>), (StatusCode, String)> {
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

/// The InfraSetup runs of the project that have not ended: the durable
/// "infra sync in flight" state, cancellable through the per-run cancel and
/// visible to every dispatcher. A run that has not ended is always on its
/// way somewhere (queued for a worker, driven by one, or parked on a
/// wait; a worker that went away has its runs requeued or ended by the
/// lost-run sweep), so nothing here can be a setup nobody will advance.
/// Sync rejects while any exists (two concurrent syncs would race the
/// provisioning subworkflow); the infra-cancel verb interrupts them.
pub(crate) async fn infra_setup_execution_ids(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    // Whose copies: `None` for any owner's setup, `Some(None)` for the
    // shared copies' setup, `Some(Some(m))` for instance m's.
    owner: Option<Option<&weft_core::instance::InstanceId>>,
) -> anyhow::Result<Vec<weft_core::ExecutionId>> {
    Ok(sqlx::query_scalar(
        "SELECT execution_id FROM run \
         WHERE project_id = $1 AND phase = 'infra_setup' AND state <> 'ended' \
           AND ($2 OR instance_id IS NOT DISTINCT FROM $3) \
         ORDER BY started_at, execution_id",
    )
    .bind(project_id)
    .bind(owner.is_none())
    .bind(owner.flatten().map(|m| m.as_str()))
    .fetch_all(&state.pg_pool)
    .await?)
}

/// Whether an infra setup of `owner` (see [`infra_setup_execution_ids`])
/// is in flight: one instance starting its copy does not hold another
/// instance's back.
pub(crate) async fn infra_setup_in_flight(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    owner: Option<Option<&weft_core::instance::InstanceId>>,
) -> anyhow::Result<bool> {
    Ok(!infra_setup_execution_ids(state, project_id, owner).await?.is_empty())
}

/// A started InfraSetup sub-execution: the execution to await plus the
/// event subscription opened BEFORE the enqueue (so the worker can't
/// beat the waiter to the terminal event).
pub struct InfraSetupRun {
    pub(crate) execution_id: weft_core::ExecutionId,
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

/// Queue a run of the program `program` started by the dispatcher (a setup
/// run), recording the program's source as the version it ran.
async fn start_queued_execution(
    state: &DispatcherState,
    program: &weft_core::project::hash::ProgramIdentity,
    defaults: &weft_core::project::ProjectDefaults,
    mut birth: weft_journal::birth::Birth<'_>,
    queue_as: QueueAs<'_>,
) -> Result<(), (StatusCode, String)> {
    let source_version = super::versions::record_program_source(state, birth.project_id, program).await?;
    birth.source_version = Some(&source_version);
    start_queued_execution_with(state, defaults, birth, &[], queue_as).await
}

/// What a run the dispatcher queues is born with beside its birth.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct QueueAs<'a> {
    /// A trigger setup an activation asked for (the activation is the
    /// setup's own execution): queued only while that activation still
    /// owns its rows.
    pub for_activation: bool,
    /// Its ending leaves the dispatcher work to do (a trigger setup's
    /// bake, `crate::run_ends`).
    pub watch_end: bool,
    /// What the version tree shows of a run started by hand: the places it
    /// ran again rather than inherit, what it was asked to run, and the
    /// saved example it came from.
    pub stale: &'a [String],
    pub spec: Option<&'a serde_json::Value>,
    pub example: Option<&'a str>,
}

/// THE one way the dispatcher starts a run (entrance 3): its birth (its
/// `ExecutionStarted`, one `NodeKicked` per root) and `extra_rows` (birth
/// facts beyond the kicks: a scoped run's provided values as `PortEmitted
/// { provided: true }`), written as its first record row with its `run`
/// row, `queued`, in one transaction; delivery hands it to a worker. Every
/// start path (`run`, trigger setup, infra setup) goes through here.
pub(crate) async fn start_queued_execution_with(
    state: &DispatcherState,
    // The program's defaults (`weft.toml`): how long the run is kept when
    // what started it says nothing.
    defaults: &weft_core::project::ProjectDefaults,
    birth: weft_journal::birth::Birth<'_>,
    extra_rows: &[weft_journal::ExecEvent],
    queue_as: QueueAs<'_>,
) -> Result<(), (StatusCode, String)> {
    let tenant = state
        .tenant_router
        .tenant_for_project(birth.project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let execution_id = birth.execution_id;
    let keep_for = birth.settings.kept_for(defaults.keep_for());
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
    };
    let inserted = state
        .journal
        .queue_run(queued, queue_as.for_activation)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("start execution: {e:#}")))?;
    if !inserted {
        return Err((StatusCode::CONFLICT, format!("execution {execution_id} was started already")));
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

/// What a run for `instance` over `selection` starts with: the instance's
/// stored values (with `overlay` made: the change a store call is about
/// to commit, which the setup it re-arms has to run with), checked
/// against the program by `weft_core::run_spec::instance_run_values`.
/// Answers a 422 carrying the refusal, the same shape every run refusal
/// has, naming every field the instance left unfilled or filled wrong.
pub(crate) async fn instance_values_for_run(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
    instance: &weft_core::instance::InstanceId,
    overlay: Option<&weft_core::instance::ValueChanges>,
) -> Result<weft_core::instance::InstanceValues, (StatusCode, String)> {
    let stored = stored_instance_values(state, project_id, instance, overlay, Read::Held).await?;
    match weft_core::run_spec::instance_run_values(project, selection, instance, &stored) {
        Ok(values) => Ok(values),
        // Refused on what this process kept: confirmed against the rows,
        // since a value stored a moment ago may not have been heard yet.
        Err(_) => {
            let stored = stored_instance_values(state, project_id, instance, overlay, Read::Fresh).await?;
            weft_core::run_spec::instance_run_values(project, selection, instance, &stored).map_err(|refusal| refusal_error(&refusal))
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
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read instance values: {e:#}")))?;
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

/// THE infra gate on a run over `selection`, scoped to the places it
/// executes (spelled the way their infra rows are keyed): a run aimed at
/// part of the graph waits on that part's infra alone, a whole-graph run
/// on all of it, and an instance's run on that instance's copies of the
/// per-instance ones. A run that reaches something per instance and names
/// no instance is told so first: which instance's copies to check is what it
/// has not said.
pub(crate) async fn require_run_infra(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    project: &ProjectDefinition,
    selection: &weft_core::project::selection::RunSelection,
    instance: Option<&weft_core::instance::InstanceId>,
) -> Result<(), (StatusCode, String)> {
    weft_core::run_spec::refuse_instanceless(project, selection, instance).map_err(|refusal| refusal_error(&refusal))?;
    let within: HashSet<String> = selection
        .nodes
        .iter()
        .map(|place| weft_core::project::address_of(project, &place.id, &place.path))
        .collect();
    let missing = missing_infra_nodes(state, project_id, project, Some(&within), instance).await?;
    if !missing.is_empty() {
        return Err((StatusCode::PRECONDITION_REQUIRED, infra_not_running(&missing)));
    }
    Ok(())
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
    let targeted = crate::api::infra::resolve_infra_nodes(&project, nodes, instance)?;
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
    // An instance's copy is set up from what reaches it, which may be what
    // that instance provides.
    let instance_values = match instance {
        Some(instance) => instance_values_for_run(state, project_id, &project, &selection, instance, None).await?,
        None => Default::default(),
    };
    let picks = picks_for_run(state, project_id, &project, &selection).await?;
    let execution_id = weft_core::new_execution_id();

    // Subscribe BEFORE journaling+enqueueing so the worker can't beat us to the
    // completion event.
    let events = state.events.subscribe_project(project_id).await;

    // Infra reconciliation already holds the project transition lock.
    let source_version = super::versions::record_program_source_locked(state, project_id, &program).await?;
    let entry_node = kick_place(&project, &kicks[0]);
    start_queued_execution_with(
        state,
        &project.defaults,
        weft_journal::birth::Birth {
            execution_id,
            project_id,
            phase: weft_core::context::Phase::InfraSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            definition_hash: &program.definition_hash,
            binary_hash: &program.binary_hash,
            selection: Some(&weft_core::project::selection::RecordedSelection::new(selection)),
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
        QueueAs::default(),
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
/// How the infra setup `execution_id` ended, as its journal says: `None`
/// while it is still in flight.
async fn setup_ended(
    state: &DispatcherState,
    execution_id: weft_core::ExecutionId,
) -> anyhow::Result<Option<Result<(), SyncNotLanded>>> {
    use crate::api::execution::TerminalOutcome;
    Ok(crate::api::execution::terminal_outcome(&state.pg_pool, execution_id).await?.map(|outcome| match outcome {
        TerminalOutcome::Completed => Ok(()),
        TerminalOutcome::Cancelled => Err(SyncNotLanded::Cancelled("infra setup cancelled".into())),
        _ => Err(SyncNotLanded::Failed(StatusCode::INTERNAL_SERVER_ERROR, "infra setup failed".into())),
    }))
}

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
    // can always cancel it (`weft infra cancel`) to unblock.
    let started = std::time::Instant::now();
    // A setup that ended before this waiter subscribed (one it follows
    // rather than started) raised its terminal event to nobody: the
    // journal answers at once rather than at the first breadcrumb.
    if let Ok(Some(landed)) = setup_ended(state, execution_id).await {
        return landed;
    }
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
                // Still in flight, or the lookup itself failed (which is
                // not this wait's to report: the breadcrumb below keeps
                // the state legible and the next tick tries again).
                if let Ok(Some(landed)) = setup_ended(state, execution_id).await {
                    return landed;
                }
                tracing::info!(
                    target: "weft_dispatcher::infra_setup",
                    project_id = %project_id,
                    execution_id = %execution_id,
                    elapsed_secs = started.elapsed().as_secs(),
                    "infra setup still running; waiting on the InfraSetup execution \
                     (cancel it with `weft infra cancel` to unblock)"
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
                        match setup_ended(state, execution_id).await {
                            Ok(Some(landed)) => return landed,
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
        for (reason, refused) in crate::entry_limits::recent_refusals(&state.pg_pool, project_id, &token, now).await? {
            out.push(LimitedEntry { node: node.clone(), limit: reason.describe().to_string(), refused });
        }
    }
    Ok(out)
}

/// weft's own calls the project's work waits on that keep failing: every
/// role weft cannot wake, and the project's workers when runs cannot be
/// handed to them.
pub(crate) async fn unanswered_for(state: &DispatcherState, project_id: uuid::Uuid) -> Result<Vec<weft_core::projects::Unanswered>, (StatusCode, String)> {
    let failing = weft_task_store::unanswered::failing_for(&state.pg_pool, project_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("calls that keep failing: {e:#}")))?;
    Ok(failing
        .into_iter()
        .map(|f| weft_core::projects::Unanswered {
            callee: match f.role.as_deref() {
                None => "this project's workers, which runs are handed to,".to_string(),
                Some("supervisor") => "weft's supervisor, which starts and stops infra,".to_string(),
                Some("dispatcher") => "weft's dispatcher, which hands runs to workers,".to_string(),
                Some(role) => format!("weft's {role}"),
            },
            error: f.error,
            since_unix: f.since_ms / 1000,
            last_unix: f.last_ms / 1000,
            as_of_unix: f.as_of_ms / 1000,
        })
        .collect())
}

/// The runs each of the project's triggers started in this minute and the
/// one before, named by the trigger's node.
async fn trigger_runs(state: &DispatcherState, project_id: uuid::Uuid) -> anyhow::Result<Vec<weft_core::projects::TriggerRuns>> {
    let counts = crate::entry_limits::recent_runs(&state.pg_pool, project_id, crate::lease::now_unix()).await?;
    if counts.is_empty() {
        return Ok(Vec::new());
    }
    let triggers: Vec<(String, String)> = sqlx::query_as("SELECT token, node_id FROM signal WHERE project_id = $1 AND NOT is_resume")
        .bind(project_id)
        .fetch_all(&state.pg_pool)
        .await?;
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

/// Aggregate view for `weft status`: registration, listener state,
/// per-node infra state, a rollup of recent executions, drift (when the
/// desired hashes ride the query), and the verbs valid right now. One
/// answer, no stitching required by a client.
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

    // The verbs and what they would decide about, for the triggers the
    // query names (the ones a verb is about to act on), else the
    // program's own.
    let scope = query.scope();
    let snapshot = gather_action_snapshot(&state, id, &project, scope.as_ref()).await?;
    let (infra, instance_infra) = infra_entries(&project, &snapshot.infra);
    let waiting = crate::api::signal::instance_waits(&state.pg_pool, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("waiting fires: {e:#}")))?;
    let has_infra = snapshot.has_infra;
    let has_triggers = snapshot.has_triggers;
    let infra_rollup = snapshot.infra_rollup.clone();

    // Project-filtered, newest first: `total` is the true count of the
    // program's own runs (from SQL, not capped at a fetched window), the
    // set `weft tree` lists, and the first row is the latest for the
    // `last_*` fields. The runs that set it up are counted beside them.
    let runs_of = |phase: Option<weft_core::context::Phase>| crate::journal::ExecutionQuery {
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
    let (execs, every) = tokio::try_join!(
        state.journal.list_executions(caller.0.as_str(), &program_runs),
        state.journal.list_executions(caller.0.as_str(), &every_run),
    )
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("journal: {e}")))?;
    let last = execs.executions.first();
    let running = state
        .journal
        .going_execution_ids_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs going: {e}")))?;
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
        setup: every.total.saturating_sub(execs.total) as usize,
        last_completed_at: last.and_then(|l| l.completed_at),
        last_execution_id: last.map(|l| l.execution_id.to_string()),
        last_status: last.map(|l| l.status),
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
    // program's own or an instance's, was set up on code other than what is
    // registered now: a plain `weft resync` brings every one of them up.
    drift.activation_drift = snapshot
        .activations
        .iter()
        .filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active)
        .any(|a| activation_has_drifted(&query, binary_hash.as_deref(), definition_hash.as_deref(), a.program.as_ref(), drift.definition_drift));
    let available_actions = compute_available_actions(&ActionInputs {
        lifecycle: &snapshot.lifecycle,
        partly_down: snapshot.partly_down,
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
        status: snapshot.lifecycle.status,
        transition: snapshot.transition,
        mode: snapshot.lifecycle.mode(),
        fires_deadline_unix: snapshot.lifecycle.fires_deadline_unix,
        running_count: snapshot.running_count,
        listener_running,
        has_triggers,
        infra,
        executions,
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
        address: project_address(&state, id).await,
        orphaned_infra: snapshot.orphaned_infra,
        infra_rollup,
        infra_busy: snapshot.infra_busy,
        drift,
        available_actions,
        preservation: snapshot.preservation,
        instance_infra,
        activations: snapshot
            .activations
            .iter()
            .map(|a| ActivationEntry {
                trigger: a.key.trigger.clone(),
                instance: a.key.instance().cloned(),
                status: a.lifecycle.status,
                mode: a.lifecycle.mode(),
                waiting: waiting.get(&a.key).cloned(),
                version: a.source_version.clone(),
            })
            .collect(),
        limited: limited_entries(&state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("limit refusals: {e}")))?,
        runs: trigger_runs(&state, id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("run counts: {e}")))?,
        unanswered: unanswered_for(&state, id).await?,
    }))
}

/// The status response's two infra lists, from every copy as the readers
/// show it: one entry per infra place the program declares (a shared one
/// with its copy's state, `not_started` when it has none; a `@per_instance`
/// one as `per_instance` with its instances' copies counted), and every
/// instance's copy on its own. A copy whose place the source no longer
/// declares (an orphan) is left out of both: it is counted project-wide
/// (`orphaned_infra`, the rollup) so the infra controls never vanish while
/// it lives.
fn infra_entries(
    project: &ProjectDefinition,
    copies: &crate::infra_node::ObservedCopies,
) -> (Vec<ProjectInfraEntry>, Vec<InstanceInfraEntry>) {
    let mut instance_infra: Vec<InstanceInfraEntry> = copies
        .rows
        .iter()
        .filter_map(|row| {
            Some(InstanceInfraEntry {
                node: row.node_id.clone(),
                instance: row.instance.clone()?,
                status: row.status.as_str().to_string(),
                progress: copies.progress_of(&row.node_id, row.instance.as_ref()),
            })
        })
        .chain(copies.starting.iter().filter_map(|(node, instance)| {
            Some(InstanceInfraEntry {
                node: node.clone(),
                instance: instance.clone()?,
                status: crate::infra_node::InfraNodeStatus::Provisioning.as_str().to_string(),
                progress: copies.progress_of(node, instance.as_ref()),
            })
        }))
        .collect();
    instance_infra.sort_by(|a, b| (&a.node, &a.instance).cmp(&(&b.node, &b.instance)));
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
            instance_copy_count: None,
            progress: None,
        };
        if node.per_instance.is_some() {
            infra.push(ProjectInfraEntry {
                instance_copy_count: Some(instance_infra.iter().filter(|c| c.node == spelled).count()),
                ..entry(INFRA_PER_INSTANCE.to_string())
            });
            continue;
        }
        match copies.rows.iter().find(|r| r.node_id == spelled && r.instance.is_none()) {
            Some(row) => infra.push(ProjectInfraEntry {
                // Coarse UI hint: first endpoint by name (BTreeMap = stable).
                endpoint_url: row.install_endpoints.values().next().cloned(),
                failure_stage: row.failure_stage.map(|f| f.as_str().to_string()),
                failure_message: row.failure_message.clone(),
                progress: copies.progress_of(&spelled, None),
                ..entry(row.status.as_str().to_string())
            }),
            None => infra.push(match copies.status_of(&spelled, None) {
                Some(status) => ProjectInfraEntry {
                    progress: copies.progress_of(&spelled, None),
                    ..entry(status.as_str().to_string())
                },
                None => entry(INFRA_NOT_STARTED.to_string()),
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
    /// (`activation_store::aggregate`), or the named scope's when the
    /// snapshot was taken for a verb aimed at some triggers.
    pub lifecycle: crate::activation_store::ActivationLifecycle,
    /// Whether some of those activations are on and others off (an infra
    /// stop took down only the triggers reading it): the aggregate reads
    /// active, and activate still has triggers to turn on.
    pub partly_down: bool,
    /// Every activation row, shared and per instance.
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
        None => key.owner == weft_core::instance::Owner::Shared,
        Some(scope) => match scope.resolve(project) {
            Ok(keys) => keys.contains(key),
            Err(_) => key.owner == scope.owner(),
        },
    };
    let lifecycle = crate::activation_store::aggregate(
        activations.iter().filter(|a| in_scope(&a.key)).map(|a| &a.lifecycle),
    );
    let partly_down = lifecycle.status == crate::project_store::ProjectStatus::Active
        && activations.iter().filter(|a| in_scope(&a.key)).any(|a| is_down(&a.lifecycle));
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
    // row is keyed (a file included twice declares two placements): the
    // project's infra, which the action bar starts and stops. An instance's
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
    // shared infra they read: an instance's triggers read that instance's
    // copies, which have no shared row, and are armed per instance.
    let shared_triggers: Vec<Located> = weft_core::project::trigger_places(project)
        .into_iter()
        .filter(|place| !weft_core::project::is_per_instance(project, &place.id))
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
    let is_orphan = |r: &crate::infra_node::InfraNodeRow| !declared.declares(&r.node_id, r.instance.is_some());
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
        || infra_setup_in_flight(state, id, Some(None))
            .await
            .map_err(|e| {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("infra_setup_in_flight: {e}"))
            })?;

    // What the verb's triggers that are off kept: what a reactivate's
    // choice decides about, and nothing else (a run started by hand, a
    // trigger still on).
    let off: Vec<weft_core::activation::ActivationKey> =
        activations.iter().filter(|a| in_scope(&a.key) && is_down(&a.lifecycle)).map(|a| a.key.clone()).collect();
    let preservation = match off.is_empty() {
        true => PreservationCounts::default(),
        false => crate::journal::postgres::activation_preservation(&state.pg_pool, id, &off)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?,
    };
    let running_now = state
        .journal
        .going_execution_ids_for_project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("runs going: {e}")))?
        .len();
    Ok(ActionSnapshot {
        lifecycle,
        partly_down,
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

/// Whether an activation's triggers are off: never turned on, or taken
/// down (whatever it keeps of their fires).
fn is_down(lifecycle: &crate::activation_store::ActivationLifecycle) -> bool {
    matches!(lifecycle.status, crate::project_store::ProjectStatus::Registered | crate::project_store::ProjectStatus::Inactive)
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
) -> Result<ActionSnapshot, (StatusCode, String)> {
    let project = state
        .projects
        .project(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("project: {e}")))?
        .ok_or((StatusCode::NOT_FOUND, "project not found".into()))?;
    let snapshot = gather_action_snapshot(state, id, &project, scope).await?;
    let allowed = compute_available_actions(&ActionInputs {
        lifecycle: &snapshot.lifecycle,
        partly_down: snapshot.partly_down,
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
        drift: &ProjectDrift { infra_drift: true, binary_drift: true, definition_drift: true, activation_drift: true },
        preservation: &snapshot.preservation,
        running_count: snapshot.running_count,
    });
    if verbs.iter().any(|v| allowed.iter().any(|a| a == v)) {
        return Ok(snapshot);
    }
    let status = snapshot.lifecycle.status;
    Err((StatusCode::CONFLICT, unavailable_action(verbs[0], status, snapshot.transition, &snapshot.infra_rollup, &allowed)))
}

/// What an activate over triggers that are all on already is told. It is
/// nearly always somebody who changed the source and wants it live, which
/// is resync's job.
const ALREADY_ON: &str = "these triggers are already on. If you changed the source, run `weft resync --mode <park|hibernate|wipe>` \
     to put the change live (park and hibernate keep the work waiting on them, wipe drops it); \
     `weft deactivate` turns them off instead";

/// Why `verb` is refused, as a person reads it. An activate over triggers
/// that are already on is nearly always somebody who changed the source
/// and wants it live, which is resync's job, so that case says so plainly
/// instead of listing the table's state.
fn unavailable_action(
    verb: &str,
    status: crate::project_store::ProjectStatus,
    transition: crate::project_store::ProjectTransition,
    infra_rollup: &str,
    allowed: &[String],
) -> String {
    if verb == "activate" && status == crate::project_store::ProjectStatus::Active {
        return ALREADY_ON.into();
    }
    // A build in flight blocks every verb but its cancel, so it is named
    // as the blocker rather than buried in the triggers' state.
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
    // Starting infra that already runs is somebody who changed it and wants
    // the change live (an upgrade) or who did not know it was up.
    if verb == "infra_start" && infra_rollup == "running" && allowed.iter().any(|a| a == "infra_upgrade") {
        return "every piece of this program's infrastructure is already running. If you changed it, \
                `weft infra upgrade` puts the change live"
            .into();
    }
    format!(
        "'{verb}' is not available right now: the triggers it names are {} (infra {infra_rollup}); \
         allowed actions: [{}]",
        status.as_str(),
        allowed.join(", "),
    )
}

/// Pure drift comparison: desired (CLI's view) vs running (DB view).
/// Both sides compute the SAME hash function for each signal, so the
/// comparison is a string equality check.
fn compute_drift(
    query: &StatusQuery,
    running_binary_hash: Option<&str>,
    running_definition_hash: Option<&str>,
    running_infra_hash: Option<&str>,
) -> ProjectDrift {
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
    ProjectDrift {
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

/// Every input the reconciliation reads, gathered in one place so the
/// status handler (UI list) and verb enforcement (`require_action`)
/// feed the SAME pure function from the SAME facts. `drift` is the one
/// input only a client can supply (the desired hashes live on the
/// client); enforcement passes all-true so drift-gated verbs are never
/// spuriously rejected by the dispatcher.
pub(crate) struct ActionInputs<'a> {
    pub lifecycle: &'a crate::activation_store::ActivationLifecycle,
    /// Some of the triggers are on and others off (`ActionSnapshot::partly_down`).
    pub partly_down: bool,
    pub transition: crate::project_store::ProjectTransition,
    /// Source declares a shared trigger (frontend-parse fact, derived
    /// here from the stored definition); an instance's triggers are its own.
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
    pub drift: &'a ProjectDrift,
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
///                     that are on (the program's or an instance's), so
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

    // A trigger whose infra is down cannot be armed: its registration
    // reads the address off the infra node feeding it. Infra the trigger
    // does not depend on is gated at run time instead, so it does not
    // hold the activation back. The door's own copy of this is
    // `require_trigger_infra`, which runs after the build; this one is
    // what the bar and `weft status` are told.
    let turn_on = |out: &mut Vec<String>, some_inactive: bool| {
        if inputs.has_triggers && inputs.trigger_infra_ready {
            let has_preserved = inputs.preservation.parked + inputs.preservation.suspended > 0;
            out.push(if has_preserved && some_inactive { "reactivate" } else { "activate" }.to_string());
        }
    };
    match inputs.lifecycle.status {
        ProjectStatus::Active => {
            out.push("deactivate".to_string());
            // Some triggers on and others off (an infra stop took down the
            // ones reading it): turning the rest on is still a step.
            if inputs.partly_down {
                turn_on(&mut out, true);
            }
        }
        ProjectStatus::Registered | ProjectStatus::Inactive => {
            turn_on(&mut out, inputs.lifecycle.status == ProjectStatus::Inactive);
        }
        ProjectStatus::Activating | ProjectStatus::Deactivating => {
            unreachable!("transitional statuses returned above")
        }
    }

    // Registrations pin both the graph and the worker code. Either change
    // needs resync before listeners can fire the new program. The drift
    // bit counts only activations that are on, the program's or a
    // instance's, so it is offered whatever the shared status says: a
    // instance's triggers can be on while the program's are off. Resync
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

/// The activations `scope` names in `project`, refusing a mismatch (a
/// per-instance trigger without an instance, a shared one with one, an
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
        let what = match &scope.instance {
            None => "no shared trigger (every trigger it has exists once per instance; name one with --instance)".to_string(),
            Some(instance) => format!("no trigger that exists once per instance, so there is nothing to activate for '{instance}'"),
        };
        return Err((StatusCode::PRECONDITION_FAILED, format!("this program has {what}")));
    }
    Ok(keys)
}

/// The activations a verb that only takes triggers down acts on:
/// `scope`'s owner's rows among `rows`, every one of them, or the named
/// triggers'. Picked from the rows rather than the source, because what
/// is listening is what the rows say: a trigger the source has since
/// dropped, or an instance's copy of one the program no longer runs per
/// instance, still has a row, and taking it down is the only way to stop
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

/// The owner the keys share: every verb acts on one owner's triggers at
/// a time.
fn keys_instance(keys: &[weft_core::activation::ActivationKey]) -> Option<&weft_core::instance::InstanceId> {
    keys.first().and_then(|key| key.instance())
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
/// is Running (the instance's own copy for a per-instance one).
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

/// What a trigger setup does first to executions still running on an
/// older image.
enum StaleWorkerChoice {
    /// Settle them under the caller's running policy and drain cap (see
    /// [`reconcile_worker`]); the caller's choice, never a fixed one here,
    /// because a `wait` sits inside the request for as long as those
    /// executions run.
    Replace(crate::infra_lifecycle_command::RunningPolicy, u64),
    /// Leave them be and only ready the new image: the setup runs on it
    /// either way. A re-arm from an instance's change of values takes this,
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
    /// A change of an instance's values re-arming its live triggers; it
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

/// Where one of the project's image builds is (`build`, the builder's id,
/// as the status names it): what a person watching the build is told when
/// an image stops building.
pub async fn build_state(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, build)): Path<(uuid::Uuid, String)>,
) -> Result<Json<weft_core::projects::BuildStateResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let state = crate::build::ledger::state_of(&state.pg_pool, id, &build)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")))?;
    Ok(Json(weft_core::projects::BuildStateResponse { state }))
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
    let snapshot = require_action(&state, id, Some(&body.scope), &["activate", "reactivate", "resume_active"]).await?;
    // Work kept waiting on the triggers being turned on is the asker's to
    // decide about; a choice is never made for a person who was not asked.
    let kept = &snapshot.preservation;
    if body.target.reactivate_choice.is_none() && kept.parked + kept.suspended > 0 {
        return Err((
            StatusCode::CONFLICT,
            format!(
                "these triggers kept work while they were off ({} fires parked, {} runs waiting on a person): \
                 say what to do with it with --reactivate-choice execute_parked_keep_suspended (run the parked \
                 fires, keep the waits), keep_suspended_only (drop the parked fires) or wipe_all (drop both)",
                kept.parked, kept.suspended
            ),
        ));
    }
    // A port the person names is checked before anything is activated, so
    // a taken one refuses the activation and changes nothing.
    if let Some(port) = body.port {
        let Some(ports) = &state.project_ports else {
            return Err((StatusCode::BAD_REQUEST, "this install gives projects no port: a project's own address is the platform's".into()));
        };
        let taken = ports
            .check_asked(&state.pg_pool, id, port)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("check port {port}: {e:#}")))?;
        if let Some(why) = taken {
            return Err((
                StatusCode::CONFLICT,
                format!("{why}, so nothing was activated: free it, or leave out `--port` and the project keeps a port of its own"),
            ));
        }
    }
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
    choice: ReactivateChoice,
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
    // hold the exclusive Activating claim.
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
                execution_id: bake.execution_id, node_id: key.trigger.clone(), frames: Vec::new(),
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
    let orphans = match orphan_entry_tokens(state, id, project, keys_instance(keys)).await {
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
                    ClaimRefused::Building => "the project is building; wait for it to finish or cancel it with `weft cancel-build`",
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
    let stored = held_picks(state, project_id, Read::Held).await?;
    match weft_core::picks::run_picks(project, selection, &stored.rows) {
        Ok(picks) => Ok(picks),
        // Confirmed against the rows before refusing (see `Read`).
        Err(_) => {
            let stored = held_picks(state, project_id, Read::Fresh).await?;
            weft_core::picks::run_picks(project, selection, &stored.rows).map_err(|refusal| refusal_error(&refusal))
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
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("read the install's picks: {e:#}")))?;
        Ok::<_, (StatusCode, String)>(crate::held::OfTenant { tenant, rows })
    };
    match read {
        Read::Held => state.held.picks.get_or_load(project_id, load).await,
        Read::Fresh => state.held.picks.load_fresh(project_id, load).await,
    }
}

pub(crate) async fn activate_with(
    state: &DispatcherState,
    id: uuid::Uuid,
    request: ActivateRequest,
    asker: ActivateAsker<'_>,
) -> Result<Json<ActivateResponse>, ActivateError> {
    let ActivateRequest {
        target: ActivationTarget { build, reactivate_choice },
        running,
        scope,
        port,
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
             Use `weft run` to fire an execution directly."
                .into(),
        ));
    }
    let keys = resolve_scope(&project, &scope)?;
    // An activate turns on the triggers that are off and leaves the ones
    // already on (or being switched on) as they are: after an infra stop
    // took down only the triggers reading it, this is what brings them
    // back. One being switched off is not on: an activate on it is the
    // change of mind that rolls its drain back (`resume_active`). A re-arm
    // is the other way round: its triggers are on, and it sets them up
    // again.
    let keys = match rearm {
        Some(_) => keys,
        None => {
            let on: std::collections::HashSet<weft_core::activation::ActivationKey> = state
                .activations
                .list(id)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?
                .into_iter()
                .filter(|a| {
                    matches!(
                        a.lifecycle.status,
                        crate::project_store::ProjectStatus::Active | crate::project_store::ProjectStatus::Activating
                    )
                })
                .map(|a| a.key)
                .collect();
            let off: Vec<weft_core::activation::ActivationKey> = keys.into_iter().filter(|k| !on.contains(k)).collect();
            if off.is_empty() {
                return Err(ActivateError::Failed(StatusCode::CONFLICT, ALREADY_ON.into()));
            }
            off
        }
    };
    // Every connection the program needs picked on this install, before
    // any trigger moves: a run born without one is refused at its first
    // call, long after the activation said yes. A re-arm skips it: its
    // triggers are live already, and the change it carries is the
    // instance's or a pick's own, checked where it was asked.
    if rearm.is_none() {
        crate::install_picks::require_activation_picks(state, id, &project).await?;
    }
    // The shared infra that is not running, read now for the note the
    // answer carries: past the Active flip nothing may fail the call.
    let idle: Vec<MissingCopy> = missing_infra_nodes(state, id, &project, None, None)
        .await?
        .into_iter()
        .filter(|copy| !copy.needs_instance)
        .collect();

    // The choice's DESTRUCTIVE effect (clearing parked / wiping
    // signals) is applied AFTER the single-flight claim below, so a
    // losing concurrent activate that 409s never wipes the winner's
    // state.
    let choice = reactivate_choice.unwrap_or_default();

    // Single-flight gate: atomically claim Activating for every named
    // activation. While a trigger is activating (registering its
    // signals), no second activation of it may start. The claim wins or
    // loses whole; a losing caller (a concurrent activate from another
    // dispatcher, a double-click, CLI + extension racing, a program
    // activating the same instance twice) bails here with 409 BEFORE any
    // signal cleanup. Every later write is guarded by this activation's
    // reserved execution.
    let activation = weft_core::new_execution_id();
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

    // Hand over every event those activations' signals kept through their
    // inactive window, now that the triggers take them. One this pass
    // could not hand over stays queued, and the reaper's sweep
    // (`crate::parked_drain::drain_due`) comes back for it.
    if let Err(e) = crate::parked_drain::drain_activations(state, id, &keys).await {
        tracing::error!(
            target: "weft_dispatcher::activate",
            project_id = %id, %activation, error = %format!("{e:#}"),
            "the triggers are active, but handing over the events they kept failed; \
             the reaper's sweep hands over what is left"
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
    // The program the project now runs answers its callers at the
    // project's own address, straight (`crate::front`).
    let address = crate::front::serve_or_log(state, id, port).await;
    Ok(Json(ActivateResponse { urls, infra_not_running, per_instance_left_out: scope.per_instance_left_out(&project), address }))
}

/// The project's own address, as its front last answered it.
async fn project_address(state: &DispatcherState, id: uuid::Uuid) -> Option<weft_core::projects::ProjectAddress> {
    crate::front::address(state, id).await.unwrap_or_else(|e| {
        tracing::warn!(target: "weft_dispatcher::api::project", project_id = %id, error = %format!("{e:#}"), "could not read the project's address");
        None
    })
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
    instance: Option<&weft_core::instance::InstanceId>,
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
/// winner's state.
///   - `execute_parked_keep_suspended`: no-op (the drain at the end
///     of activate replays every parked fire).
///   - `keep_suspended_only`: clear parked fires, keep suspensions.
///   - `wipe_all`: drop every signal row.
async fn apply_reactivate_choice(
    state: &DispatcherState,
    project_id: uuid::Uuid,
    keys: &[weft_core::activation::ActivationKey],
    activation: uuid::Uuid,
    choice: ReactivateChoice,
) -> Result<(), (StatusCode, String)> {
    let internal = |what: &str, e: anyhow::Error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{what}: {e:#}"));
    // What these activations kept: their signals, and the runs their
    // triggers fired. Another trigger's, another instance's, and a run
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
        ReactivateChoice::ExecuteParkedKeepSuspended => {}
        ReactivateChoice::KeepSuspendedOnly => {
            sqlx::query("DELETE FROM parked_fire WHERE token = ANY($1) AND execution_id IS NULL")
                .bind(&governed)
                .execute(&mut *tx)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("clear parked: {e}")))?;
        }
        ReactivateChoice::WipeAll => {
            let target = crate::take_down::TakeDownTarget::Activations(keys.to_vec());
            let runs = crate::take_down::live_runs(state, project_id).await.map_err(|e| internal("live runs", e))?;
            cancelled = crate::take_down::affected_runs(&target, &runs, None).into_iter().map(|r| r.execution_id).collect();
            removed = crate::journal::postgres::remove_signals(&mut *tx, &governed).await
                .map_err(|e| internal("clear activation signals", e))?;
        }
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

/// Every owner of project `id` with at least one trigger on, each once:
/// the program itself first, then instances in id order. The activation
/// rows are the whole answer, so nothing is guessed. What a plain resync
/// brings up to date, and (instances only) what `deactivate --all-instances`
/// takes down and what a plain deactivate reports as still on.
pub async fn owners_with_triggers_on(
    activations: &dyn crate::activation_store::ActivationStoreOps,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::instance::Owner>, (StatusCode, String)> {
    let rows = activations
        .list(id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("activations: {e}")))?;
    let live = rows.iter().filter(|a| a.lifecycle.status == crate::activation_store::ProjectStatus::Active).map(|a| &a.key);
    Ok(weft_core::activation::owners_of(live))
}

/// The instances among [`owners_with_triggers_on`].
async fn instances_with_triggers_on(
    state: &DispatcherState,
    id: uuid::Uuid,
) -> Result<Vec<weft_core::instance::InstanceId>, (StatusCode, String)> {
    let owners = owners_with_triggers_on(state.activations.as_ref(), id).await?;
    Ok(owners.into_iter().filter_map(|owner| owner.instance().cloned()).collect())
}

// `DeactivationMode` is the wire contract for the `mode` field on a
// `DeactivateSpec`. It lives in `weft-broker-client::protocol` so
// the supervisor + dispatcher share one source of truth. Re-export
// here so this module stays the canonical home for deactivation glue.
pub use weft_broker_client::protocol::DeactivationMode;

/// Take down `keys` with the person's choice of how (Stop, Terminate and
/// Upgrade send it for the triggers their copies feed, a resync for the
/// ones it registers again), marked `went_down_with` when what takes them
/// down is not the person. Validation lives on `DeactivateSpec::validate`
/// (broker, dispatcher, supervisor share one validator); this site adds
/// the `triggerDeactivation:` prefix so clients see which field tripped
/// the check.
pub async fn execute_trigger_deactivation(
    state: &DispatcherState,
    id: uuid::Uuid,
    keys: Vec<weft_core::activation::ActivationKey>,
    spec: &weft_broker_client::protocol::DeactivateSpec,
    went_down_with: Option<crate::take_down::DownWith>,
) -> Result<(), (StatusCode, String)> {
    spec.validate()
        .map_err(|m| (StatusCode::BAD_REQUEST, format!("triggerDeactivation: {m}")))?;
    let target = crate::take_down::TakeDownTarget::Activations(keys);
    let existed = crate::take_down::take_down(state, id, &target, spec, went_down_with, None).await?;
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
    let owners: Vec<Option<weft_core::instance::InstanceId>> = if body.all_instances {
        if let Some(instance) = &body.scope.instance {
            return Err(StatusError::Other(
                StatusCode::BAD_REQUEST,
                format!(
                    "--all-instances takes down every instance's triggers and --instance names one \
                     ('{instance}'); pass one of the two"
                ),
            ));
        }
        instances_with_triggers_on(&state, id).await?.into_iter().map(Some).collect()
    } else {
        vec![body.scope.instance.clone()]
    };
    let mut deactivated = Vec::with_capacity(owners.len());
    for instance in owners {
        let scope = weft_core::activation::ActivationScope { triggers: body.scope.triggers.clone(), instance };
        let keys = keys_to_take_down(&rows, &scope)?;
        let target = crate::take_down::TakeDownTarget::Activations(keys);
        // user-initiated (the standalone Deactivate verb)
        let existed = crate::take_down::take_down(&state, id, &target, &body.spec, None, None).await?;
        if !existed {
            // Vanished between the gate and the deactivate (a concurrent
            // rm): same "no project I may see" answer, marker included.
            return Err(StatusError::NotMyProject);
        }
        deactivated.push(scope.owner());
    }
    let instances_still_on = instances_with_triggers_on(&state, id).await?;
    Ok(Json(DeactivateResponse { deactivated, instances_still_on }))
}

/// `POST /projects/{id}/resync`. Deactivate-then-activate the named
/// activations against (optionally) fresh source / infra hashes,
/// bringing the deployed trigger/worker shape in line with the current
/// source.
///
/// A scope naming an instance, or triggers, is that one owner's. The plain
/// scope (nothing named) is every owner with a trigger on: the program's
/// shared triggers when any is on, then each such instance in id order,
/// each taken as a whole, exactly as `weft resync` / `weft resync
/// --instance <id>` would for it alone. The activation rows say who has
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
            .map(|owner| ActivationScope { triggers: Vec::new(), instance: owner.instance().cloned() })
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
    crate::install_picks::require_activation_picks(&state, id, &built).await?;

    // 3. One owner at a time, the program's first.
    let several = scopes.len() > 1;
    let mut urls = Vec::new();
    let mut resynced: Vec<weft_core::instance::Owner> = Vec::with_capacity(scopes.len());
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
    // The person's own take-down, marked with no cause: a reactivation
    // below that fails leaves the triggers down until they are activated
    // again, never brought back by the infra they read coming up.
    execute_trigger_deactivation(state, id, keys.clone(), spec, None).await?;
    if spec.drains() {
        let cap = spec
            .drain_timeout_secs
            .unwrap_or(weft_broker_client::protocol::DEFAULT_DRAIN_TIMEOUT_SECS);
        let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Triggers(&keys), except: None };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(cap);
        crate::drain::drain(state, &scope, Some(deadline), spec.running_policy)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("drain: {e:#}")))?;
        for key in &keys {
            crate::drain::land_if_drained(state, id, key)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("land the drain: {e:#}")))?;
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
    // The port stays the one the project has (a resync names none).
    let reactivate = ActivateRequest { target, running: spec.running_choice(), scope, port: None };
    activate(State(state.clone()), CallerTenant(caller.0.clone()), Path(id), Some(Json(reactivate))).await
}

/// Settle what the project's workers still run on an older image, once a
/// new image went live.
///
/// Every execution runs on the image it was born with (its run row
/// carries the binary hash), so a new image never disturbs anything by
/// itself: new executions go to the new image's workers, and one parked on
/// the old image resumes there. What the person chooses is the fate of the
/// work going on an older image right now (`crate::drain`, the runs the
/// older images' workers drive and the ones queued for them):
/// `RunningPolicy::Wait` waits for it to end, up to `drain_timeout_secs`,
/// then cancels what is left; `RunningPolicy::Cancel` cancels it at once.
/// Then the platform is told to have the new image's workers ready
/// (`Runner::prepare`).
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
    let scope = crate::drain::DrainScope { project_id, reaching: crate::drain::Reaching::OtherImages(&want_hash), except: None };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(drain_timeout_secs);
    let drained = crate::drain::drain(state, &scope, Some(deadline), running_policy).await.map_err(internal)?;
    if drained != crate::drain::Drained::Empty {
        tracing::info!(
            target: "weft_dispatcher::api::project",
            %project_id,
            ?drained,
            policy = running_policy.as_str(),
            "a new image went live; what still ran on older ones was cancelled"
        );
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
    crate::take_down::take_down(state, id, &crate::take_down::TakeDownTarget::WholeProject, &spec, None, None).await
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
    if !deactivate_project(&state, id).await? {
        return Err(StatusError::NotMyProject);
    }
    // Every run of the project, whatever copy it may use, waited for
    // with no deadline: how long a worker takes to let go of what it
    // was cancelled out of is not this call's to cut short.
    let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Copies(&weft_core::instance::Copies::Every), except: None };
    crate::drain::drain(&state, &scope, None, crate::infra_lifecycle_command::RunningPolicy::Wait)
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
    let scope = crate::drain::DrainScope { project_id: id, reaching: crate::drain::Reaching::Triggers(&draining), except: None };
    crate::drain::cancel_left(&state, &scope, &weft_core::exec::CancelCause::User)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("cancel: {e:#}")))?;
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
    // owner, whose run it is (an instance's triggers read that instance's
    // values).
    keys: &[weft_core::activation::ActivationKey],
    // The hash of the SAME definition the caller resolved `keys`
    // against (one `coherent_definition` pair). Reading the project
    // row's hash here instead would race a concurrent re-register:
    // kicks from shape A journaled under hash B.
    program: &weft_core::project::hash::ProgramIdentity,
    activation: Option<uuid::Uuid>,
    // An instance's change about to be stored, which this setup runs with in
    // place of what is stored (a store re-arming the triggers that read
    // it); nothing for every other setup.
    overlay: Option<&weft_core::instance::ValueChanges>,
) -> Result<crate::journal::TriggerBake, (StatusCode, String)> {
    let execution_id = activation.unwrap_or_else(weft_core::new_execution_id);
    let places = key_places(project, keys);
    let selection = weft_core::project::selection::RunSelection::setup(project, &places)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let kicks = setup_kicks(project, places).map_err(|error| (StatusCode::BAD_REQUEST, error))?;
    let instance = keys_instance(keys);
    // An instance's triggers are set up with what that instance provides.
    let instance_values = match instance {
        Some(instance) => instance_values_for_run(state, project_id, project, &selection, instance, overlay).await?,
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
        program,
        &project.defaults,
        weft_journal::birth::Birth {
            execution_id,
            project_id,
            phase: weft_core::context::Phase::TriggerSetup,
            entry_node: &entry_node,
            kicks: &kicks,
            definition_hash: &program.definition_hash,
            binary_hash: &program.binary_hash,
            selection: Some(&weft_core::project::selection::RecordedSelection::new(selection)),
            seed: None,
            source_version: None,
            instance: instance.map(|instance| weft_journal::birth::RunFor { instance, values: &instance_values }),
            picks: &picks,
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
    let program = state.run_program_identity(&events).await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("{error:#}")))?;
    let mut bake = crate::journal::TriggerBake::from_events(&events, &program)
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
    use serde_json::Value;
    use weft_core::run_spec::{compute_trigger_fire, TriggerFire};
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
        compute_trigger_fire(p, node, &payload, None, &Default::default()).expect("the fire reaches an output")
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::String("payload".into()));
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::String("payload".into()));
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::String("fire".into()));
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::Null);
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::Null);
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "g.trig", serde_json::json!({ "hi": 1 }));
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
             instances start when the group does"
        );
    }

    #[test]
    fn a_scope_the_fire_touches_excludes_unrelated_instances() {
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::Null);
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
        let TriggerFire { kicks, subgraph, .. } = fire(&p, "trigger_x", Value::String("msg".into()));
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

    fn instance() -> ActivationScope {
        ActivationScope { triggers: Vec::new(), instance: Some("alice".parse().expect("instance id")) }
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
        for scope in [shared(), instance()] {
            let (status, msg) = resolve_scope(&p, &scope).expect_err("nothing to resolve");
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
        }
    }

    /// Nothing activated: a take-down (deactivate, cancel-running,
    /// `weft rm --journal`'s quiesce) has nothing to do.
    #[test]
    fn nothing_activated_takes_down_nothing() {
        for scope in [shared(), instance()] {
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
            keys.into_iter().map(|k| (k.trigger, k.owner.instance().map(|m| m.to_string()))).collect::<Vec<_>>()
        };
        assert_eq!(
            triggers(keys_to_take_down(&rows, &shared()).unwrap()),
            vec![("gone".to_string(), None), ("door".to_string(), None)]
        );
        assert_eq!(triggers(keys_to_take_down(&rows, &instance()).unwrap()), vec![("door".to_string(), Some("alice".to_string()))]);
        let named = ActivationScope { triggers: vec!["gone".into()], instance: None };
        assert_eq!(triggers(keys_to_take_down(&rows, &named).unwrap()), vec![("gone".to_string(), None)]);
    }

    /// A named trigger with no row for the owner is a mistake.
    #[test]
    fn a_named_trigger_never_activated_is_refused() {
        let rows = [row("door", None)];
        let scope = ActivationScope { triggers: vec!["door".into()], instance: Some("alice".parse().expect("instance id")) };
        let (status, msg) = keys_to_take_down(&rows, &scope).expect_err("alice's door never activated");
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(msg.contains("'door' of instance 'alice'"), "{msg}");
    }

    /// Only per-instance triggers: the program's own scope names nothing,
    /// and the refusal points at --instance.
    #[test]
    fn per_instance_only_refuses_shared_scope() {
        let p = per_instance_only();
        let (status, msg) = resolve_scope(&p, &shared()).expect_err("no shared trigger");
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert!(msg.contains("no shared trigger"), "{msg}");
        assert_eq!(resolve_scope(&p, &instance()).expect("instance's trigger").len(), 1);
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
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
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
        partly_down: bool,
        transition: ProjectTransition,
        has_triggers: bool,
        has_infra: bool,
        trigger_infra_ready: bool,
        orphaned_infra: bool,
        infra_rollup: &'static str,
        infra_busy: bool,
        drift: ProjectDrift,
        preservation: PreservationCounts,
        running_count: usize,
    }

    impl Default for Case {
        fn default() -> Self {
            Self {
                lifecycle: ProjectLifecycle::wiped(),
                partly_down: false,
                transition: ProjectTransition::None,
                has_triggers: true,
                has_infra: false,
                trigger_infra_ready: true,
                orphaned_infra: false,
                infra_rollup: "none",
                infra_busy: false,
                drift: ProjectDrift::default(),
                preservation: PreservationCounts::default(),
                running_count: 0,
            }
        }
    }

    fn actions(case: &Case) -> Vec<String> {
        compute_available_actions(&ActionInputs {
            lifecycle: &case.lifecycle,
            partly_down: case.partly_down,
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
                drift: ProjectDrift { infra_drift: true, ..Default::default() },
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

    /// The program's own triggers are off, an instance's are on and lag the
    /// code: the drift bit counts that instance's, and resync is offered
    /// beside the activate that would turn the program's own back on.
    #[test]
    fn a_instances_drift_lights_resync_while_the_program_is_off() {
        assert_actions(
            &Case { drift: ProjectDrift { activation_drift: true, ..Default::default() }, ..Case::default() },
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
                drift: ProjectDrift { activation_drift: true, ..Default::default() },
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
                drift: ProjectDrift { activation_drift: true, ..Default::default() },
                ..Case::default()
            },
            &["run", "deactivate", "infra_stop", "infra_terminate"],
        );
    }

    /// Some triggers on and some off (an infra stop took down the ones
    /// reading it): activate turns the rest on, and reactivate when what
    /// they kept waits on them.
    #[test]
    fn partly_on_still_offers_turning_the_rest_on() {
        let active = ProjectLifecycle::active();
        assert_actions(&Case { lifecycle: active.clone(), partly_down: true, ..Case::default() }, &["run", "deactivate", "activate"]);
        assert_actions(
            &Case { lifecycle: active.clone(), partly_down: true, preservation: PreservationCounts { parked: 1, suspended: 0 }, ..Case::default() },
            &["run", "deactivate", "reactivate"],
        );
        assert_actions(
            &Case { lifecycle: active, partly_down: true, trigger_infra_ready: false, ..Case::default() },
            &["run", "deactivate"],
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
            install_endpoints: Default::default(),
            keep_disks: Vec::new(),
            units: Default::default(),
            notes: Vec::new(),
            waiting: None,
            provisioning_since_unix: None,
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
            start_asked_unix: [(("queue".to_string(), None), 100), (("bridge".to_string(), Some(InstanceId::new("bob").unwrap())), 110)]
                .into(),
            as_of_unix: 130,
        };
        let (infra, instance_entries) = infra_entries(&project, &copies);
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
        let since = |progress: Option<&weft_core::infra::wire::ApplyProgress>| progress.map(|p| (p.since_unix, p.as_of_unix));
        let shared = |node: &str| infra.iter().find(|e| e.node == node).unwrap().progress.as_ref();
        assert_eq!(since(shared("queue")), Some((100, 130)), "a shared copy with no row");
        assert_eq!(since(shared("db")), None, "a running copy shows none");
        let bob = instance_entries.iter().find(|e| e.instance.as_str() == "bob").unwrap();
        assert_eq!(since(bob.progress.as_ref()), Some((110, 130)), "an instance's copy with no row");
    }

    /// A program with no infra node lists none, which is the one case the
    /// CLI says no node declares `requires_infra`.
    #[test]
    fn a_program_without_infra_lists_none() {
        let project = super::infra_kick_and_dep_tests::project(&[("plain", false, false)], &[]);
        let (infra, instances) = infra_entries(&project, &ObservedCopies::default());
        assert!(infra.is_empty() && instances.is_empty());
    }
}

#[cfg(test)]
mod unavailable_action_tests {
    use super::unavailable_action;
    use crate::project_store::{ProjectStatus, ProjectTransition};

    #[test]
    fn activating_what_is_already_on_points_at_resync() {
        let allowed = vec!["run".to_string(), "deactivate".to_string(), "resync".to_string()];
        let message = unavailable_action("activate", ProjectStatus::Active, ProjectTransition::None, "none", &allowed);
        assert!(message.contains("already on") && message.contains("weft resync"), "{message}");
        let other = unavailable_action("run", ProjectStatus::Active, ProjectTransition::None, "stopped", &allowed);
        assert!(other.contains("allowed actions: [run, deactivate, resync]"), "{other}");
    }

    #[test]
    fn starting_infra_that_already_runs_points_at_upgrade() {
        let allowed = vec!["infra_stop".to_string(), "infra_upgrade".to_string()];
        let message = unavailable_action("infra_start", ProjectStatus::Active, ProjectTransition::None, "running", &allowed);
        assert!(message.contains("already running") && message.contains("weft infra upgrade"), "{message}");
        // Without an upgrade to offer (the infra left running belongs to
        // nodes no longer in the program), the plain answer stands.
        let leftover = unavailable_action("infra_start", ProjectStatus::Active, ProjectTransition::None, "running", &["infra_terminate".to_string()]);
        assert!(leftover.contains("allowed actions: [infra_terminate]"), "{leftover}");
    }

    #[test]
    fn infra_that_is_changing_is_named_as_the_blocker() {
        let message = unavailable_action("activate", ProjectStatus::Registered, ProjectTransition::None, "provisioning", &["infra_cancel".to_string()]);
        assert!(message.contains("infra is changing (infra provisioning)") && message.contains("weft infra status"), "{message}");
        assert!(!message.contains("registered"), "the triggers' state is not why: {message}");
    }

    #[test]
    fn a_build_in_flight_is_named_as_the_blocker() {
        let allowed = vec!["cancel_build".to_string()];
        let message = unavailable_action("activate", ProjectStatus::Registered, ProjectTransition::Building, "none", &allowed);
        assert!(message.contains("the project is building") && message.contains("weft cancel-build"), "{message}");
        let cancelling = unavailable_action("activate", ProjectStatus::Registered, ProjectTransition::CancellingBuild, "none", &allowed);
        assert!(cancelling.contains("being cancelled") && !cancelling.contains("weft cancel-build"), "{cancelling}");
    }
}
