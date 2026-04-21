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

pub fn router(state: DispatcherState) -> Router {
    Router::new()
        .route("/projects", get(project::list).post(project::register))
        .route("/projects/{id}", get(project::get).delete(project::remove))
        .route("/projects/{id}/run", post(project::run))
        .route("/projects/{id}/activate", post(project::activate))
        .route("/projects/{id}/deactivate", post(project::deactivate))
        .route("/executions/{color}/cancel", post(execution::cancel))
        .route("/executions/{color}/cost", post(execution::record_cost))
        .route("/executions/{color}", get(execution::get))
        .route("/events/project/{id}", get(events::project_stream))
        .route("/events/execution/{color}", get(events::execution_stream))
        .route("/w/{token}/{*path}", post(webhook::handle))
        .route("/f/{token}", post(form::submit))
        .route("/ext/{token}/tasks", get(extension::list_tasks))
        .route("/ext/{token}/tasks/{execution_id}/complete", post(extension::complete_task))
        .route("/ext/{token}/triggers/{trigger_task_id}/submit", post(extension::submit_trigger))
        .route("/ext/{token}/actions/{action_id}/dismiss", post(extension::dismiss_action))
        .route("/ext/{token}/health", get(extension::health))
        .route("/dashboard/{*path}", get(dashboard::serve))
        .route("/describe/nodes", get(describe::nodes))
        .route("/describe/project/{id}", get(describe::project_catalog))
        .with_state(state)
}
