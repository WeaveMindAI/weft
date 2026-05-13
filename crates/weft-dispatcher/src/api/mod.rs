//! HTTP API surface. The CLI, the VS Code extension, the browser
//! extension, and end users (via webhook URLs minted by the
//! listener) all talk to this surface.
//!
//! Route categories:
//! - `/projects/*`: project registration, run, stop, logs.
//! - `/executions/*`: execution state queries and control.
//! - `/events/*`: SSE streams for project and execution state.
//! - `/ext/*`: browser extension API (token-scoped).
//! - `/dashboard/*`: the ops dashboard UI (static assets + SSE).
//! - `/describe/*`: catalog introspection for tooling.

use axum::{routing::{get, post}, Router};
use tower_http::cors::CorsLayer;

use crate::state::DispatcherState;

pub mod project;
mod execution;
mod events;
mod extension;
mod extension_names;
mod dashboard;
mod describe;
mod infra;
mod parse;
pub(crate) mod signal;

pub fn router(state: DispatcherState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/projects", get(project::list).post(project::register))
        .route("/projects/{id}", get(project::get).delete(project::remove))
        .route("/projects/{id}/run", post(project::run))
        .route("/projects/{id}/status", get(project::status))
        .route("/projects/{id}/executions/latest", get(execution::latest_for_project))
        .route("/projects/{id}/activate", post(project::activate))
        // Cancel an in-flight activate (status=Activating). Wipes
        // every signal row registered so far, cancels the
        // TriggerSetup color, CASes status to Inactive.
        .route("/projects/{id}/cancel-activate", post(project::cancel_activate))
        .route("/projects/{id}/deactivate", post(project::deactivate))
        // While the project is in `deactivating`, this endpoint
        // cancels every running, non-suspended execution; the
        // journal-bridge drain-watcher then CASes status to
        // `inactive` (the lifecycle target the original deactivate
        // already wrote to the row stays in place).
        .route("/projects/{id}/cancel-running", post(project::cancel_running))
        .route("/projects/{id}/resync", post(project::resync))
        .route("/projects/{id}/infra/start", post(infra::start))
        .route("/projects/{id}/infra/stop", post(infra::stop))
        .route("/projects/{id}/infra/terminate", post(infra::terminate))
        .route("/projects/{id}/infra/upgrade", post(infra::upgrade))
        .route(
            "/signal/{token}",
            post(signal::fire_signal).delete(signal::cancel_signal),
        )
        .route("/signal/{token}/skip", post(signal::skip_signal))
        .route("/projects/{id}/infra/status", get(infra::status))
        .route("/projects/{id}/infra/nodes/{node_id}/live", get(infra::live))
        .route("/executions/{color}/cancel", post(execution::cancel))
        .route("/executions/{color}/logs", get(execution::list_logs))
        .route("/executions/{color}/replay", get(execution::replay))
        .route(
            "/executions/{color}",
            get(execution::get).delete(execution::delete_execution),
        )
        .route("/executions", get(execution::list_executions))
        .route("/events/project/{id}", get(events::project_stream))
        .route("/events/execution/{color}", get(events::execution_stream))
        // Token-scoped enumeration. The api_token authenticates +
        // scopes; signal_token is the per-signal credential that
        // fires (POST /signal/{token}) and cancels
        // (DELETE /signal/{token}, also gated by the api_token via
        // Authorization: Bearer header).
        .route(
            "/api-token/{token}/signals",
            get(signal::list_signals_for_token).delete(signal::clear_all_signals),
        )
        .route("/api-token/{token}/health", get(signal::api_token_health))
        // Token administration. Mint requires localhost auth (see
        // CorsLayer). Listing + revoke same.
        .route(
            "/api-tokens",
            get(extension::list_tokens).post(extension::mint_token),
        )
        .route("/api-tokens/{token}", axum::routing::delete(extension::revoke_token))
        .route("/", get(dashboard::serve_root))
        .route("/dashboard", get(dashboard::serve_root))
        .route("/dashboard/{*path}", get(dashboard::serve))
        .route("/describe/nodes", get(describe::nodes))
        .route("/parse", post(parse::parse))
        .route("/validate", post(parse::validate))
        .route("/listener/inspect", get(signal::listener_inspect))
        // Inspector proxy: project-scoped read of signal display
        // info (mount_path, plaintext key while listener still
        // holds it, etc). Project-token gated.
        .route(
            "/projects/{id}/signals/{node_id}/display",
            get(signal::display_signal),
        )
        // Inspector proxy: project-scoped action invocation. The
        // listener's kind impl owns the action's payload schema.
        // Project-token gated.
        .route(
            "/projects/{id}/signals/{node_id}/action",
            post(signal::action_signal),
        )
        // Catch-all PublicEntry route: external HTTP fires land
        // here when no more-specific route matches. The handler
        // looks up the signal row by `mount_path`, applies the
        // auth gate (api_key check, future schemes), then
        // forwards to dispatch_listener_outcome. Webhook + ApiPost
        // signals fire via this route. Methods other than POST or
        // unmatched paths fall to axum's default 404.
        .route("/{*mount_path}", post(signal::fire_public_entry))
        // Permissive CORS so the browser extension popup / hosted
        // task page (origins like `moz-extension://<id>` or
        // `chrome-extension://<id>`) can hit /ext/*. Localhost
        // dev only; if the dispatcher is ever exposed publicly,
        // tighten this to specific origins.
        .layer(
            CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any),
        )
        .with_state(state)
}
