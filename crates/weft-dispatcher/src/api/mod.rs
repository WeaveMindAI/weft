//! HTTP API surface. All dispatcher routes are defined here. The CLI,
//! the VS Code extension, the browser extension, the workspace runner,
//! and end users (via webhooks) all talk to this surface.
//!
//! Route categories:
//! - `/projects/*`: project registration, run, stop, logs.
//! - `/executions/*`: execution state queries and control.
//! - `/events/*`: SSE streams for project and execution state.
//! - `/w/*`: webhook entry (user-facing, token-scoped URLs).
//! - `/f/*`: form submission entry (user-facing, token-scoped URLs).
//! - `/ext/*`: browser extension API (token-scoped, moved from v1
//!   dashboard proxy).
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
pub(crate) mod internal;
mod parse;
mod signal;
pub mod ws;

pub fn router(state: DispatcherState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/projects", get(project::list).post(project::register))
        .route("/projects/{id}", get(project::get).delete(project::remove))
        .route("/projects/{id}/run", post(project::run))
        .route("/projects/{id}/status", get(project::status))
        .route("/projects/{id}/executions/latest", get(execution::latest_for_project))
        .route("/projects/{id}/activate", post(project::activate))
        .route("/projects/{id}/deactivate", post(project::deactivate))
        .route("/projects/{id}/infra/start", post(infra::start))
        .route("/projects/{id}/infra/stop", post(infra::stop))
        .route("/projects/{id}/infra/terminate", post(infra::terminate))
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
        .route("/ext/{token}/tasks", get(extension::list_tasks))
        .route("/ext/{token}/tasks/{execution_id}/complete", post(extension::complete_task))
        .route("/ext/{token}/tasks/{execution_id}/cancel", post(extension::cancel_task))
        .route("/ext/{token}/triggers/{trigger_task_id}/submit", post(extension::submit_trigger))
        .route("/ext/{token}/actions/{action_id}/dismiss", post(extension::dismiss_action))
        .route("/ext/{token}/health", get(extension::health))
        .route("/ext/{token}/cleanup/all", post(extension::cleanup_all))
        .route("/ext/{token}/cleanup/execution/{execution_id}", post(extension::cleanup_execution))
        .route("/ext-tokens", get(extension::list_tokens).post(extension::mint_token))
        .route("/ext-tokens/{token}", axum::routing::delete(extension::revoke_token))
        .route("/", get(dashboard::serve_root))
        .route("/dashboard", get(dashboard::serve_root))
        .route("/dashboard/{*path}", get(dashboard::serve))
        .route("/describe/nodes", get(describe::nodes))
        .route("/describe/project/{id}", get(describe::project_catalog))
        .route("/parse", post(parse::parse))
        .route("/validate", post(parse::validate))
        .route("/signal-fired", post(signal::signal_fired))
        .route("/signal-failed", post(signal::signal_failed))
        .route("/listener/empty", post(signal::listener_empty))
        .route("/listener/inspect", get(signal::listener_inspect))
        .route("/listener/register-me", post(signal::listener_register_me))
        .route("/internal/deliver-color", post(internal::deliver_color))
        .route("/internal/cancel-color", post(internal::cancel_color))
        .route("/ws/executions/{color}", get(ws::connect))
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
