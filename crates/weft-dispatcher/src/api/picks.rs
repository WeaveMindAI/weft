//! `/projects/{id}/picks`: the connections a program's own access nodes
//! use on this install (`weft_core::picks`), listed and changed. What
//! `weft connect` and the editor's Connect button call; the change itself
//! is `crate::install_picks`.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use crate::authenticator::{authorize_project, CallerTenant};
use crate::state::DispatcherState;

/// `GET /projects/{id}/picks`: every pick this install keeps for the
/// project, by place and field, each as its `{id, identity}` handle. The
/// caller matches them against the program it holds (the one it is about
/// to build may have places the install has not built yet).
pub async fn list(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<weft_core::picks::Picks>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    Ok(Json(crate::api::project::stored_picks(&state, id).await?))
}

/// `PUT /projects/{id}/picks`: pick and forget, all or none, re-arming
/// the live triggers that read a changed pick.
pub async fn change(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<weft_core::picks::ChangePicks>,
) -> Result<Json<weft_core::member_door::ValuesChanged>, (StatusCode, String)> {
    authorize_project(&state, &caller.0, id).await?;
    let clear: Vec<(String, String)> = body.clear.into_iter().map(|f| (f.step, f.field)).collect();
    Ok(Json(crate::install_picks::change(&state, id, &body.set, &clear).await?))
}
