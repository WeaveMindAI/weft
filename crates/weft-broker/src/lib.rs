//! weft-broker: scoped HTTP frontend in front of Postgres for
//! tenant pods. Every endpoint validates the caller's k8s SA token
//! via TokenReview, derives `(tenant, role)`, and runs a scope check
//! before delegating to the underlying Postgres-direct client.
//!
//! Trust model:
//!   - tenant pods: untrusted; their SA token says who they are, the
//!     broker enforces what they can touch.
//!   - dispatcher: skips the broker entirely (god-mode DB).
//!   - broker: trusted, has the DB credentials, isolated in its own
//!     namespace `weft-db` with k8s NetworkPolicy.

pub mod auth;
pub mod handlers;
pub mod scope;
pub mod state;

use std::sync::Arc;

use axum::{routing::post, Router};

pub use auth::AuthConfig;
pub use state::BrokerState;

pub fn router(state: Arc<BrokerState>) -> Router {
    Router::new()
        .route("/health", axum::routing::get(handlers::health))
        // Journal
        .route("/v1/journal/record", post(handlers::journal_record))
        .route("/v1/journal/fetch", post(handlers::journal_fetch))
        .route(
            "/v1/journal/has_terminal",
            post(handlers::journal_has_terminal),
        )
        // Tasks
        .route("/v1/task/enqueue_dedup", post(handlers::task_enqueue_dedup))
        .route(
            "/v1/task/wait_terminal",
            post(handlers::task_wait_terminal),
        )
        .route("/v1/task/claim_one", post(handlers::task_claim_one))
        .route("/v1/task/heartbeat", post(handlers::task_heartbeat))
        .route("/v1/task/complete", post(handlers::task_complete))
        .route("/v1/task/fail", post(handlers::task_fail))
        // worker_pod
        .route(
            "/v1/worker_pod/register_alive",
            post(handlers::worker_pod_register_alive),
        )
        .route(
            "/v1/worker_pod/heartbeat",
            post(handlers::worker_pod_heartbeat),
        )
        .route(
            "/v1/worker_pod/mark_done",
            post(handlers::worker_pod_mark_done),
        )
        // Infra reads
        .route(
            "/v1/infra/sidecar_endpoint",
            post(handlers::infra_sidecar_endpoint),
        )
        // Signals (listener-only rehydrate read)
        .route(
            "/v1/signal/list_for_tenant",
            post(handlers::signal_list_for_tenant),
        )
        .with_state(state)
}
