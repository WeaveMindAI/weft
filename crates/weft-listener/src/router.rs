//! HTTP router for the listener. Three surfaces:
//!
//!   User-facing (Webhook + Form fires):
//!     - `POST /signal/{token}`        fire a signal with a JSON body
//!     - `POST /signal/{token}/{*path}` fire with extra path segment
//!     - `GET  /signal/{token}`        read kind-specific metadata (form schema)
//!
//!   Dispatcher-internal (requires admin token):
//!     - `POST /register`    add a signal
//!     - `POST /unregister`  remove a signal
//!     - `GET  /signals`     list all active signals (debug)
//!
//!   Health (unauthed):
//!     - `GET /health`

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use serde_json::Value;

use crate::kinds;
use crate::protocol::{RegisterRequest, RegisterResponse, UnregisterRequest};
use crate::ListenerState;

pub fn router(state: ListenerState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/signal/{token}", get(get_signal).post(fire_signal))
        .route("/signal/{token}/{*path}", post(fire_signal_with_path))
        .route("/register", post(register))
        .route("/unregister", post(unregister))
        .route("/signals", get(list_signals))
        .with_state(state)
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn fire_signal(
    State(state): State<ListenerState>,
    Path(token): Path<String>,
    body: Option<Json<Value>>,
) -> Result<StatusCode, (StatusCode, String)> {
    fire_inner(state, token, body.map(|Json(v)| v).unwrap_or(Value::Null)).await
}

async fn fire_signal_with_path(
    State(state): State<ListenerState>,
    Path((token, _path)): Path<(String, String)>,
    body: Option<Json<Value>>,
) -> Result<StatusCode, (StatusCode, String)> {
    fire_inner(state, token, body.map(|Json(v)| v).unwrap_or(Value::Null)).await
}

async fn fire_inner(
    state: ListenerState,
    token: String,
    body: Value,
) -> Result<StatusCode, (StatusCode, String)> {
    if state.registry.get(&token).is_none() {
        return Err((StatusCode::NOT_FOUND, "unknown token".into()));
    }
    state.relay.fire(token, body).await;
    Ok(StatusCode::ACCEPTED)
}

async fn get_signal(
    State(state): State<ListenerState>,
    Path(token): Path<String>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let signal = state
        .registry
        .get(&token)
        .ok_or((StatusCode::NOT_FOUND, "unknown token".into()))?;
    // Minimal metadata exposure. Form schema lives inside the
    // signal spec's kind, so we return the whole kind. Kinds that
    // don't want to expose anything are still covered because the
    // WakeSignalKind enum controls what's visible.
    Ok(Json(
        serde_json::to_value(&signal.spec.kind)
            .unwrap_or(Value::Null),
    ))
}

async fn register(
    State(state): State<ListenerState>,
    headers: HeaderMap,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let user_url = kinds::register_spec(
        req.token,
        req.spec,
        req.node_id,
        state.registry.clone(),
        state.relay.clone(),
        state.config.clone(),
    )
    .await
    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(RegisterResponse { user_url }))
}

async fn unregister(
    State(state): State<ListenerState>,
    headers: HeaderMap,
    Json(req): Json<UnregisterRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    // Dropping the registered signal aborts its task via TaskGuard.
    state.registry.remove(&req.token);
    Ok(StatusCode::NO_CONTENT)
}

async fn list_signals(
    State(state): State<ListenerState>,
    headers: HeaderMap,
) -> Result<Json<Value>, (StatusCode, String)> {
    require_admin(&state, &headers)?;
    let rows: Vec<Value> = state
        .registry
        .list()
        .into_iter()
        .map(|(token, sig)| {
            serde_json::json!({
                "token": token,
                "node_id": sig.node_id,
                "kind": &sig.spec.kind,
            })
        })
        .collect();
    Ok(Json(Value::Array(rows)))
}

fn require_admin(state: &ListenerState, headers: &HeaderMap) -> Result<(), (StatusCode, String)> {
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    if got != state.config.admin_token {
        return Err((StatusCode::UNAUTHORIZED, "bad admin token".into()));
    }
    Ok(())
}
