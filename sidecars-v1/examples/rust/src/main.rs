//! WeaveMind Sidecar Example (Rust)
//!
//! A minimal self-contained sidecar implementing the full WeaveMind sidecar contract.
//! Clone this directory and extend it to build your own sidecar.
//!
//! Contract endpoints:
//!   GET  /health   liveness probe (required)
//!   POST /action   { action, payload } → { result } (required)
//!   GET  /outputs  runtime values for node output ports (required, return {} if none)
//!   GET  /live     dashboard live data rendering (required, no-op default)
//!   GET  /events   SSE stream for real-time triggers (optional)
//!
//! Required actions (via POST /action):
//!   ping  readiness check. Must return { ready: true } when the sidecar
//!          can process actions. "Ready" means the server is operational and
//!          core dependencies are initialized (e.g., DB pool connected).
//!          Do NOT gate on user-initiated connections (QR scans, OAuth, etc.).
//!          The orchestrator blocks provisioning until ping returns ready.
//!          Returns { ready: true } or { ready: false, reason: "..." }.
//!
//! Environment variables:
//!   PORt: HTTP port (default: 8090)

use std::sync::Arc;
use std::convert::Infallible;
use axum::{Router, Json, extract::State, response::sse::{Event, Sse}, routing::{get, post}};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};

// =============================================================================
// CONTRACT TYPES
// =============================================================================

#[derive(Debug, Deserialize)]
struct ActionRequest {
    action: String,
    #[serde(default)]
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ActionResponse {
    result: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

// =============================================================================
// YOUR STATE, put your connections, caches, etc. here
// =============================================================================

struct AppState {
    // Example: add your database pool, API clients, etc.
    // pool: PgPool,
}

// =============================================================================
// ACTION DISPATCH, add your actions here
// =============================================================================

async fn dispatch_action(
    state: &AppState,
    action: &str,
    payload: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let _ = (state, payload); // remove when you use state/payload
    match action {
        // Required by contract: readiness check.
        // Return ready: true when the server can process actions.
        // Gate on core dependencies (DB pool, etc.), NOT on user-initiated
        // connections (QR scans, OAuth flows).
        // The orchestrator blocks provisioning until this returns ready.
        "ping" => {
            // TODO: add your own checks here, e.g.:
            //   sqlx::query("SELECT 1").execute(&state.pool).await
            //     .map_err(|e| format!("DB not ready: {}", e))?;
            Ok(serde_json::json!({ "ready": true }))
        }
        other => Err(format!("Unknown action: {}", other)),
    }
}

// =============================================================================
// CONTRACT HANDLERS, you usually don't need to modify these
// =============================================================================

async fn health() -> &'static str {
    "ok"
}

async fn handle_action(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ActionRequest>,
) -> Result<Json<ActionResponse>, Json<ErrorResponse>> {
    match dispatch_action(&state, &req.action, req.payload).await {
        Ok(result) => Ok(Json(ActionResponse { result })),
        Err(e) => Err(Json(ErrorResponse { error: e })),
    }
}

async fn handle_outputs(
    State(_state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    // Return runtime-computed values exposed as node output ports.
    // Platform values (instanceId, endpointUrl) are added automatically.
    // Add your own here if needed.
    Json(serde_json::json!({}))
}

async fn handle_live(
    State(_state): State<Arc<AppState>>,
) -> Json<serde_json::Value> {
    // Return data for the dashboard live panel.
    // No-op default, override if your sidecar has a live view.
    Json(serde_json::json!({}))
}

async fn handle_events(
    State(_state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    // SSE stream for real-time trigger events.
    // The orchestrator connects here to receive events like incoming messages.
    // Override this to push events from your sidecar.
    // Example: yield Event::default().event("message.received").data(json_payload)
    let stream = futures::stream::pending();
    Sse::new(stream).keep_alive(axum::response::sse::KeepAlive::default())
}

// =============================================================================
// MAIN
// =============================================================================

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8090);

    let state = Arc::new(AppState {
        // Initialize your connections here
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/action", post(handle_action))
        .route("/outputs", get(handle_outputs))
        .route("/live", get(handle_live))
        .route("/events", get(handle_events))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("Sidecar listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await
        .expect("Failed to bind");
    axum::serve(listener, app).await
        .expect("Server error");
}
