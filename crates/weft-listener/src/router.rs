//! HTTP router for the listener. Every endpoint is network-trusted:
//! only Pods in the dispatcher's namespace can reach the listener
//! port (NetworkPolicy enforces this). No bearer auth in arch-5.
//!
//!   POST /register     add a signal to the registry
//!   POST /unregister   remove a signal
//!   POST /process      run kind-specific logic for one fire,
//!                      return a `ProcessOutcome` (value + target)
//!                      for the dispatcher to journal on
//!   POST /render       render the consumer-facing payload for one
//!                      token. Pure over the spec; called once at
//!                      register time and the result cached on the
//!                      signal row.
//!   GET  /signals      debug: list registry entries
//!   GET  /health       liveness probe

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::Value;

use crate::kinds;
use crate::protocol::{
    ActionRequest, ActionResponse, DisplayRequest, DisplayResponse, ProcessOutcome,
    ProcessRequest, RegisterRequest, RegisterResponse, UnregisterRequest,
};
use crate::ListenerState;

pub fn router(state: ListenerState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/load", get(load))
        .route("/register", post(register))
        .route("/unregister", post(unregister))
        .route("/process", post(process))
        .route("/render", post(render))
        .route("/display", post(display))
        .route("/action", post(action))
        .route("/signals", get(list_signals))
        .route("/rehydrate", post(rehydrate_handler))
        .with_state(state)
}

/// Reconcile the in-memory registry with the durable signal table.
/// Idempotent: existing entries are left alone, missing ones are
/// inserted. Called by the dispatcher's activate flow after
/// TriggerSetup completes, so resume signals (which TriggerSetup
/// can't replay) come back from the DB before the gate flips to
/// Active.
async fn rehydrate_handler(
    State(state): State<ListenerState>,
) -> Result<StatusCode, (StatusCode, String)> {
    let broker_url = Arc::new(state.config.broker_url.clone());
    crate::registry::rehydrate(
        state.tasks.clone(),
        broker_url,
        state.token_source.clone(),
        &state.config.pod_name,
        state.registry.clone(),
        state.config.clone(),
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

/// Load surface for the dispatcher's placement. Returns the pod's
/// current load + its own saturation call.
async fn load(State(state): State<ListenerState>) -> Json<crate::protocol::LoadReport> {
    Json(state.load_report())
}

async fn register(
    State(state): State<ListenerState>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<RegisterResponse>, (StatusCode, String)> {
    // Admission gate: a saturated pod refuses new signals so a
    // placement race (the dispatcher chose this pod from a stale load
    // read) fails loudly with 503 instead of overloading it. The
    // dispatcher retries placement onto another pod / spawns one.
    if state.load_report().saturated {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "listener saturated; place on another pod".into(),
        ));
    }
    let (routing, kind_state) = kinds::register_in_registry(
        req.token,
        req.tenant_id,
        req.spec,
        req.node_id,
        req.is_resume,
        req.color,
        req.placement_generation,
        kinds::RoutingSource::Mint {
            secret_cache: state.secret_cache.clone(),
        },
        state.registry.clone(),
        state.fire_sink.clone(),
        state.config.clone(),
    )
    .await
    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(RegisterResponse { routing, kind_state }))
}

async fn display(
    State(state): State<ListenerState>,
    Json(req): Json<DisplayRequest>,
) -> Result<Json<DisplayResponse>, (StatusCode, String)> {
    let sig = state
        .registry
        .get(&req.token)
        .ok_or((StatusCode::NOT_FOUND, format!("unknown token: {}", req.token)))?;
    let display = kinds::compute_display(&req.token, &sig, &state.secret_cache);
    Ok(Json(DisplayResponse { display }))
}

async fn action(
    State(state): State<ListenerState>,
    Json(req): Json<ActionRequest>,
) -> Result<Json<ActionResponse>, (StatusCode, String)> {
    let (result, routing) = kinds::handle_action(
        &req.token,
        &req.kind,
        req.payload,
        &state.registry,
        &state.secret_cache,
    )
    .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(ActionResponse { result, routing }))
}

async fn unregister(
    State(state): State<ListenerState>,
    Json(req): Json<UnregisterRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.registry.remove(&req.token);
    Ok(StatusCode::NO_CONTENT)
}

async fn process(
    State(state): State<ListenerState>,
    Json(req): Json<ProcessRequest>,
) -> Result<Json<ProcessOutcome>, (StatusCode, String)> {
    let outcome = kinds::process(&req.token, req.payload, state.registry.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(outcome))
}

async fn list_signals(
    State(state): State<ListenerState>,
) -> Result<Json<Value>, (StatusCode, String)> {
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

#[derive(Debug, Deserialize)]
struct RenderRequest {
    token: String,
}

/// Render the consumer payload for one signal. Pure function over
/// the registered spec; the dispatcher caches the result on the
/// signal row at register time. Park-mode projects can therefore
/// serve consumer enumeration with the listener pod reaped.
async fn render(
    State(state): State<ListenerState>,
    Json(req): Json<RenderRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let rendered = kinds::render(&req.token, state.registry.clone())
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    Ok(Json(rendered.unwrap_or(Value::Null)))
}

