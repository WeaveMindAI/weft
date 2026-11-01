//! `/projects/{id}/picks`: the connections a program's own access nodes
//! use on this install (`weft_core::picks`), listed and changed. What
//! `weft connect` and the editor's Connect button call. Each handler names
//! the caller as an operator and calls its action
//! (`crate::actions::settings`); the change itself is
//! `crate::install_picks`.

use axum::{
    extract::{Path, State},
    Json,
};

use crate::actions::{settings, Actor, Refused};
use crate::authenticator::CallerTenant;
use crate::state::DispatcherState;

/// `GET /projects/{id}/picks`: every pick this install keeps for the
/// project. The caller matches them against the program it holds (the one
/// it is about to build may have places the install has not built yet).
pub async fn list(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<weft_core::picks::Picks>, Refused> {
    Ok(Json(settings::picks_list(&state, &Actor::operator(caller.0), id).await?))
}

/// `PUT /projects/{id}/picks`: pick and forget, all or none.
pub async fn change(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<weft_core::picks::ChangePicks>,
) -> Result<Json<weft_core::instance_door::ValuesChanged>, Refused> {
    Ok(Json(settings::picks_change(&state, &Actor::operator(caller.0), id, &body).await?))
}

/// `GET /projects/{id}/picks/kept`: what this install keeps for the
/// project's connections, which an activation checks the program against.
// SYNC: kept <-> crates/weft-core/src/picks.rs Kept, crates/weft-cli/src/commands/target.rs check_picks
pub async fn kept(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<weft_core::picks::Kept>, Refused> {
    Ok(Json(settings::picks_kept(&state, &Actor::operator(caller.0), id).await?))
}

/// `POST /projects/{id}/picks/move`: carry a place's picks and every
/// instance's values to another place, after a node moved in the source.
pub async fn move_picks(
    State(state): State<DispatcherState>,
    caller: CallerTenant,
    Path(id): Path<uuid::Uuid>,
    Json(body): Json<weft_core::picks::MovePicks>,
) -> Result<Json<weft_core::picks::PicksMoved>, Refused> {
    Ok(Json(settings::picks_move(&state, &Actor::operator(caller.0), id, &body).await?))
}
