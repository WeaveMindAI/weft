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
pub mod cost_gate;
pub mod credential;
pub mod entitlement;
pub mod handlers;
pub mod provider_proxy;
pub mod runtime_storage;
pub mod runtime_store;
pub mod scope;
pub mod state;

use std::sync::Arc;

use axum::{
    routing::{any, post},
    Router,
};

pub use auth::AuthConfig;
pub use state::BrokerState;

/// Spawn the periodic expiry sweeps: expired provider-access stand-ins
/// always; kept runtime files when an object-store slot is configured. The
/// broker owns both stores, so it runs the sweeps itself (not the
/// dispatcher). Stateless + idempotent, so every broker replica may run them
/// concurrently.
pub fn spawn_expiry_sweep(state: Arc<BrokerState>) {
    {
        let pool = state.pool.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                tick.tick().await;
                match provider_proxy::sweep_expired_standins(&pool).await {
                    Ok(n) if n > 0 => tracing::info!(
                        target: "weft_broker::provider_proxy",
                        swept = n,
                        "expired provider stand-ins"
                    ),
                    Ok(_) => {}
                    Err(e) => tracing::error!(
                        target: "weft_broker::provider_proxy", "stand-in sweep failed: {e:#}"
                    ),
                }
            }
        });
    }
    let Some(store) = state.runtime_store.clone() else {
        return;
    };
    tokio::spawn(async move {
        // The sweep cadence: kept files are access-bumped, so only genuinely
        // idle survivors expire; a minute's granularity is plenty.
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tick.tick().await;
            match store.sweep_expired().await {
                Ok(n) if n > 0 => tracing::info!(
                    target: "weft_broker::runtime_store", swept = n, "expired kept runtime files"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    target: "weft_broker::runtime_store", error = %format!("{e:#}"),
                    "runtime-file expiry sweep failed; will retry next tick"
                ),
            }
        }
    });
}

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
        .route(
            "/v1/worker_pod/mark_done_if_idle",
            post(handlers::worker_pod_mark_done_if_idle),
        )
        // Infra reads
        .route(
            "/v1/infra/endpoint_url",
            post(handlers::infra_endpoint_url),
        )
        // Project (worker fetches its own ProjectDefinition)
        .route(
            "/v1/project/fetch_definition",
            post(handlers::project_fetch_definition),
        )
        // Provider access + cost provisioning (worker data path)
        .route("/v1/access/open", post(handlers::open_provider_access))
        .route("/v1/access/close", post(handlers::close_provider_access))
        // The provider proxy: a worker addresses it like the provider's own
        // API, with a stand-in where the key goes; the broker swaps in the
        // real key and forwards (see `provider_proxy`).
        .route("/v1/provider/{provider}/{*path}", any(provider_proxy::serve))
        .route("/v1/cost/provision", post(handlers::provision_cost))
        .route("/v1/cost/settle", post(handlers::settle_cost))
        // Signals (listener-only rehydrate read, by placement = pod)
        .route(
            "/v1/signal/list_for_pod",
            post(handlers::signal_list_for_pod),
        )
        // Supervisor surface (pooled, trusted control-plane;
        // InfraSupervisor role only). A supervisor acts only on the
        // projects whose infra it owns (the `infra_owner` exclusive
        // lease), claimed + renewed via sync_ownership.
        .route(
            "/v1/supervisor/sync_ownership",
            post(handlers::supervisor_sync_ownership),
        )
        .route(
            "/v1/supervisor/owned_projects",
            post(handlers::supervisor_owned_projects),
        )
        .route(
            "/v1/supervisor/infra_nodes",
            post(handlers::supervisor_infra_nodes),
        )
        .route(
            "/v1/supervisor/health_protocols",
            post(handlers::supervisor_health_protocols),
        )
        .route(
            "/v1/supervisor/claim_command",
            post(handlers::supervisor_claim_command),
        )
        .route(
            "/v1/supervisor/event_record",
            post(handlers::supervisor_event_record),
        )
        .route(
            "/v1/supervisor/set_status",
            post(handlers::supervisor_set_status),
        )
        .route(
            "/v1/supervisor/remove_node",
            post(handlers::supervisor_remove_node),
        )
        .route(
            "/v1/supervisor/command_complete",
            post(handlers::supervisor_command_complete),
        )
        .route(
            "/v1/supervisor/command_cancel_requested",
            post(handlers::supervisor_command_cancel_requested),
        )
        .route(
            "/v1/supervisor/running_count",
            post(handlers::supervisor_running_count),
        )
        .route(
            "/v1/supervisor/infra_command_in_flight",
            post(handlers::supervisor_infra_command_in_flight),
        )
        .route(
            "/v1/supervisor/trigger_deps",
            post(handlers::supervisor_trigger_deps),
        )
        .route(
            "/v1/supervisor/set_applied",
            post(handlers::supervisor_set_applied),
        )
        .route(
            "/v1/supervisor/set_provisioning",
            post(handlers::supervisor_set_provisioning),
        )
        .route(
            "/v1/supervisor/enqueue_lifecycle",
            post(handlers::supervisor_enqueue_lifecycle),
        )
        .route(
            "/v1/supervisor/project_image_tags",
            post(handlers::supervisor_project_image_tags),
        )
        .route(
            "/v1/infra/enqueue_apply",
            post(handlers::infra_enqueue_apply),
        )
        .route(
            "/v1/infra/wait_apply",
            post(handlers::infra_wait_apply),
        )
        // Runtime-file plane (`ctx.storage`): worker data path + the
        // control-plane admin verbs the CLI proxies through the dispatcher.
        // The broker is the single gatekeeper (resolves the caller in-process,
        // runs the key wall, enforces quota, signs the bucket).
        .merge(runtime_storage::router())
        .with_state(state)
}
