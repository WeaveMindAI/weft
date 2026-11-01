//! The version tree's and the working copies' HTTP surface. Each handler
//! parses, names the caller as an operator, and calls its action
//! (`crate::actions::versions`, `crate::actions::runs`), where every gate
//! lives; see `crate::versions` and `crate::working_copies` for the model.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use weft_core::versions::{CheckpointRequest, PruneResponse, RunUpdate, RunningSource, StartRun, StartingRun, SweepResponse, VersionTree, VersionUpsert};
use weft_core::working_copy::{Applied, ChangeFiles, RegisterFolder, SwitchRequest, Switched, SyncReport, WorkingCopy, WorkingCopyRef, WorkingState};
use weft_core::RunId;

use crate::actions::{versions, Actor, Refused};
use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;

fn operator(caller: CallerTenant) -> Actor {
    Actor::operator(caller.0)
}

/// `POST /projects/{id}/versions`: stamp a working copy into a version.
pub async fn checkpoint(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<CheckpointRequest>,
) -> Result<Json<VersionUpsert>, Refused> {
    Ok(Json(versions::checkpoint(&state, &operator(caller), id, &body).await?))
}

/// `POST /projects/{id}/versions/runs`: start a run of a version, through
/// its task, answered at once to follow.
pub async fn run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(ask): Json<StartRun>,
) -> Result<(StatusCode, Json<StartingRun>), Refused> {
    Ok((StatusCode::ACCEPTED, Json(crate::actions::runs::start(&state, &operator(caller), id, &ask).await?)))
}

/// Whose bakes a preview asks about: the shared triggers' by default.
#[derive(Debug, Default, Deserialize)]
pub struct BakesQuery {
    #[serde(default)]
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// Code identities and captured trigger names for preview validation.
pub async fn trigger_bakes(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Query(query): Query<BakesQuery>,
) -> Result<Json<Vec<weft_core::run_spec::BakeSummary>>, Refused> {
    authorize_project(&state, &caller.0, id).await?;
    let bakes = state
        .bakes
        .trigger_bakes(id, query.instance.as_ref())
        .await
        .map_err(|error| crate::actions::internal("read trigger bakes", error))?;
    // One that no longer reads previews as none: the trigger reads as
    // not prepared, which `weft bake` fixes, and the run that fires it
    // names the row (`crate::actions::runs::run`).
    Ok(Json(bakes.read.iter().map(|bake| bake.summary()).collect()))
}

pub async fn running(State(state): State<DispatcherState>, caller: CallerTenant, Path(id): Path<uuid::Uuid>) -> Result<Json<RunningSource>, Refused> {
    Ok(Json(versions::running(&state, &operator(caller), id).await?))
}

pub async fn tree(State(state): State<DispatcherState>, caller: CallerTenant, Path(id): Path<uuid::Uuid>) -> Result<Json<VersionTree>, Refused> {
    Ok(Json(versions::tree(&state, &operator(caller), id).await?))
}

/// `POST /projects/{id}/versions/switch`.
pub async fn switch(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<SwitchRequest>,
) -> Result<Json<Switched>, Refused> {
    Ok(Json(versions::switch(&state, &operator(caller), id, &body).await?))
}

/// Associate a recorded run with a saved example.
pub async fn update_run(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, run_id)): Path<(uuid::Uuid, String)>,
    Json(body): Json<RunUpdate>,
) -> Result<StatusCode, Refused> {
    let run_id: RunId = run_id.parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad run".to_string()))?;
    versions::update_run(&state, &operator(caller), id, run_id, &body).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Deserialize)]
pub struct PruneQuery {
    /// Answer the plan without deleting anything.
    #[serde(default)]
    pub plan: bool,
    /// Every `frozen_from.version` in the project's examples, comma
    /// separated: the CLI reads `examples/` and the dispatcher refuses
    /// to prune under one.
    #[serde(default)]
    pub frozen: Option<String>,
}

/// `DELETE /projects/{id}/versions/{version}` (`?plan=true` only says what
/// it would remove).
pub async fn prune(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, version)): Path<(uuid::Uuid, String)>,
    Query(query): Query<PruneQuery>,
) -> Result<Json<PruneResponse>, Refused> {
    let frozen: Vec<String> =
        query.frozen.as_deref().map(|f| f.split(',').filter(|s| !s.is_empty()).map(str::to_string).collect()).unwrap_or_default();
    Ok(Json(versions::prune(&state, &operator(caller), id, &version, &frozen, query.plan).await?))
}

pub async fn sweep(State(state): State<DispatcherState>, caller: CallerTenant, Path(id): Path<uuid::Uuid>) -> Result<Json<SweepResponse>, Refused> {
    Ok(Json(versions::sweep(&state, &operator(caller), id).await?))
}

// ----- working copies ----------------------------------------------------

pub async fn working_copies(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<Vec<WorkingCopy>>, Refused> {
    Ok(Json(versions::working_copies(&state, &operator(caller), id).await?))
}

/// `GET /projects/{id}/working-copies/state[?workingCopy=]`.
pub async fn working_state(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Query(wanted): Query<WorkingCopyRef>,
) -> Result<Json<WorkingState>, Refused> {
    Ok(Json(versions::working_state(&state, &operator(caller), id, wanted).await?))
}

#[derive(Debug, Deserialize)]
pub struct FolderQuery {
    pub host: String,
    pub path: String,
}

/// `GET /projects/{id}/working-copies/folder?host=&path=`: the folder's
/// working copy, or a 404 marked as no working copy when it has none yet.
pub async fn find_folder(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Query(query): Query<FolderQuery>,
) -> Result<Json<WorkingState>, axum::response::Response> {
    use axum::response::IntoResponse;
    match versions::find_folder(&state, &operator(caller), id, &query.host, &query.path).await.map_err(IntoResponse::into_response)? {
        Some(found) => Ok(Json(found)),
        None => Err((
            StatusCode::NOT_FOUND,
            [(weft_core::working_copy::NO_WORKING_COPY_HEADER, "1")],
            format!("{} on {} is no working copy of this project yet", query.path, query.host),
        )
            .into_response()),
    }
}

/// `POST /projects/{id}/working-copies/folder`.
pub async fn register_folder(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<RegisterFolder>,
) -> Result<Json<WorkingState>, Refused> {
    Ok(Json(versions::register_folder(&state, &operator(caller), id, &body).await?))
}

/// `POST /projects/{id}/working-copies/{wc}/sync`.
pub async fn sync(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, copy)): Path<(uuid::Uuid, uuid::Uuid)>,
    Json(body): Json<SyncReport>,
) -> Result<Json<Applied>, Refused> {
    Ok(Json(versions::sync(&state, &operator(caller), id, copy, &body).await?))
}

/// `DELETE /projects/{id}/working-copies/{wc}`: keep the copy's files as
/// a version and remove it.
pub async fn forget(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path((id, copy)): Path<(uuid::Uuid, uuid::Uuid)>,
) -> Result<Json<weft_core::working_copy::Forgotten>, Refused> {
    Ok(Json(versions::forget(&state, &operator(caller), id, copy).await?))
}

/// `POST /projects/{id}/working-copies/changes`.
pub async fn change_files(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<ChangeFiles>,
) -> Result<Json<Applied>, Refused> {
    Ok(Json(versions::change_files(&state, &operator(caller), id, &body).await?))
}
