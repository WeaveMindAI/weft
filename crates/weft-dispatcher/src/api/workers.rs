//! `GET/PUT /projects/{id}/workers`: the project's own worker levers.
//!
//! Every lever the install sets for workers (copies kept warm, the most
//! copies, calls Cloud Run sends one copy at once, CPU, memory) a project can set for itself; what
//! it leaves unset follows the install. A change applies at once: the
//! platform is told to run the project's current image with the new
//! levers (a new Cloud Run revision; a warm local worker started or left
//! to go idle).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use weft_platform_traits::{WorkerOverrides, WorkersResponse};

use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;

fn internal(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
}

async fn answer(state: &DispatcherState, id: uuid::Uuid) -> Result<WorkersResponse, (StatusCode, String)> {
    let project = state
        .projects
        .worker_overrides(id)
        .await
        .map_err(internal)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, format!("no project {id}")))?;
    Ok(WorkersResponse { install: state.worker_defaults.clone(), effective: state.worker_defaults.with(&project), project })
}

pub async fn get(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<WorkersResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(answer(&state, id).await?))
}

/// Replace the project's levers with `overrides` (an unset lever follows
/// the install again). Refused, naming the lever, when the result is a
/// shape no platform could run.
pub async fn put(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(overrides): Json<WorkerOverrides>,
) -> Result<Json<WorkersResponse>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    state.worker_defaults.with(&overrides).validate().map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    state.projects.set_worker_overrides(id, &overrides).await.map_err(internal)?;
    if let Some(program) = state.projects.running_program_identity(id).await.map_err(internal)? {
        let target = crate::delivery::worker_target(&state, caller.0.as_str(), id, &program.binary_hash).await.map_err(internal)?;
        state.runner.prepare(&target).await.map_err(|e| {
            (StatusCode::BAD_GATEWAY, format!("the levers are saved, and the platform refused them for the running workers: {e:#}"))
        })?;
        // A project that takes calls has its front started again with the
        // new levers (a container's or a revision's are fixed at its start).
        if crate::front::address(&state, id).await.map_err(internal)?.is_some() {
            let unserved = |e: &anyhow::Error| (StatusCode::BAD_GATEWAY, format!("the levers are saved, and the project's front could not start with them: {e:#}"));
            let served = crate::front::serve(&state, id, None).await.map_err(|e| unserved(&e))?;
            if let Some(e) = &served.failed {
                return Err(unserved(e));
            }
        }
    }
    Ok(Json(answer(&state, id).await?))
}
