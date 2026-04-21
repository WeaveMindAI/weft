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

use crate::state::DispatcherState;

mod project;
mod execution;
mod events;
mod webhook;
mod form;
mod extension;
mod dashboard;
mod describe;
mod parse;

pub fn router(state: DispatcherState) -> Router {
    Router::new()
        .route("/projects", get(project::list).post(project::register))
        .route("/projects/{id}", get(project::get).delete(project::remove))
        .route("/projects/{id}/run", post(project::run))
        .route("/projects/{id}/activate", post(project::activate))
        .route("/projects/{id}/deactivate", post(project::deactivate))
        .route("/executions/{color}/cancel", post(execution::cancel))
        .route("/executions/{color}/cost", post(execution::record_cost))
        .route("/executions/{color}/suspensions", post(execution::record_suspension))
        .route("/executions/{color}/status", post(execution::report_status))
        .route("/executions/{color}/logs", post(execution::append_log).get(execution::list_logs))
        .route("/executions/{color}/events", post(execution::record_node_event))
        .route("/executions/{color}/replay", get(execution::replay))
        .route(
            "/executions/{color}",
            get(execution::get).delete(execution::delete_execution),
        )
        .route("/executions", get(execution::list_executions))
        .route("/events/project/{id}", get(events::project_stream))
        .route("/events/execution/{color}", get(events::execution_stream))
        .route("/w/{token}", post(webhook::handle_root))
        .route("/w/{token}/{*path}", post(webhook::handle))
        .route("/f/{token}", post(form::submit))
        .route("/ext/{token}/tasks", get(extension::list_tasks))
        .route("/ext/{token}/tasks/{execution_id}/complete", post(extension::complete_task))
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
        .with_state(state)
}
