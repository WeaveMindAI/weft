//! weft-broker: the scoped HTTP door to Postgres for everything that is
//! not the dispatcher. Every endpoint verifies the caller's platform
//! identity (`weft_platform_traits::CallerIdentity`), derives what it may
//! act for, and runs a scope check before delegating to the underlying
//! Postgres-direct client.
//!
//! Trust model:
//!   - workers: untrusted (they run the user's program); their identity
//!     says which project they are, the broker enforces what they touch.
//!   - the listener and the supervisor: weft's own code, trusted to act
//!     for any tenant, still checked per op against the resource.
//!   - the dispatcher: reaches Postgres directly.

pub mod auth;
pub mod credential;
pub mod entitlement;
pub mod handlers;
pub mod lifecycle_writes;
pub mod program_tokens;
pub mod access_admin;
pub mod app_provider;
pub mod caller_auth;
pub mod events;
pub mod runtime_storage;
pub mod runtime_store;
pub mod scope;
pub mod held_signals;
pub mod line;
pub mod state;

use std::sync::Arc;

use axum::{routing::post, Router};

pub use auth::AuthConfig;
pub use state::BrokerState;

/// The broker's background loops: the expiry of kept runtime files and of
/// abandoned connect rows. Neither is announced by any write (they are
/// about time passing), so each runs on its interval alone. Both are
/// stateless and idempotent, so every copy of the broker may run them.
/// The runtime runs them where the broker is placed
/// (`weft_task_store::drain`).
pub fn drain_loops(state: Arc<BrokerState>) -> Vec<weft_task_store::drain::DrainLoop> {
    use weft_task_store::drain::{DrainLoop, DrainStep};
    let files = state.clone();
    let connects = state;
    vec![
        // Kept files are access-bumped, so only idle survivors expire; a
        // minute's granularity is plenty.
        DrainLoop::new("runtime_file_expiry", &[], std::time::Duration::from_secs(60), move || {
            let store = files.runtime_store.clone();
            async move {
                let n = store.sweep_expired().await?;
                if n > 0 {
                    tracing::info!(target: "weft_broker::runtime_store", swept = n, "expired kept runtime files");
                }
                Ok(DrainStep::Done)
            }
        }),
        // Their TTL was only ever checked on read, so without this they
        // live forever. The rows go stale after 15 minutes; a five-minute
        // cadence keeps the backlog at most a handful of rows.
        DrainLoop::new("connect_expiry", &[], std::time::Duration::from_secs(300), move || {
            let pool = connects.pool.clone();
            async move {
                let n = weft_access_store::sweep_expired_connects(&pool).await?;
                if n > 0 {
                    tracing::info!(target: "weft_broker::access_admin", swept = n, "abandoned connect rows deleted");
                }
                Ok(DrainStep::Done)
            }
        }),
    ]
}

pub use weft_broker_client::protocol::JOURNAL_RECORD_BODY_LIMIT;

/// The cap on a failed unrecorded run's whole record
/// (`/v1/journal/record_retroactive`): sixteen of the largest single
/// rows. An unrecorded run is a short request answered in one go, so a
/// record past this is refused by name rather than read into memory.
pub const JOURNAL_RETROACTIVE_BODY_LIMIT: usize = 16 * JOURNAL_RECORD_BODY_LIMIT;

pub fn router(state: Arc<BrokerState>) -> Router {
    line::routes(api(state.clone()), state)
}

/// Every call the broker answers, as a request and on a line alike.
fn api(state: Arc<BrokerState>) -> Router {
    Router::new()
        .route("/health", axum::routing::get(handlers::health))
        // Journal
        .route(
            "/v1/journal/record",
            post(handlers::journal_record).layer(axum::extract::DefaultBodyLimit::max(JOURNAL_RECORD_BODY_LIMIT)),
        )
        // A whole failed run at once, so it may carry many rows of the
        // size `record` takes one of.
        .route(
            "/v1/journal/record_retroactive",
            post(handlers::journal_record_retroactive)
                .layer(axum::extract::DefaultBodyLimit::max(JOURNAL_RETROACTIVE_BODY_LIMIT)),
        )
        .route("/v1/journal/forget_unrecorded", post(handlers::journal_forget_unrecorded))
        .route("/v1/journal/wait", post(handlers::journal_wait))
        .route(
            "/v1/journal/has_terminal",
            post(handlers::journal_has_terminal),
        )
        // Execution steering (`ctx.tag_execution` / `ctx.stop_tagged`)
        .route("/v1/execution/tag", post(handlers::execution_tag))
        .route(
            "/v1/execution/stop_tagged",
            post(handlers::execution_stop_tagged),
        )
        // Tasks
        .route("/v1/task/enqueue_dedup", post(handlers::task_enqueue_dedup))
        .route(
            "/v1/task/wait_terminal",
            post(handlers::task_wait_terminal),
        )
        .route("/v1/task/claim_execution", post(handlers::task_claim_execution))
        .route("/v1/task/heartbeat", post(handlers::task_heartbeat))
        .route("/v1/task/requeue", post(handlers::task_requeue))
        .route("/v1/task/complete", post(handlers::task_complete))
        .route("/v1/task/fail", post(handlers::task_fail))
        // The cancels for the executions a worker drives.
        .route("/v1/task/cancels_asked", post(handlers::task_cancels_asked))
        // Infra reads
        .route(
            "/v1/infra/endpoint_url",
            post(handlers::infra_endpoint_url),
        )
        // A machine running a project's infra asking for a look at it.
        .route("/v1/infra/look", post(handlers::infra_look))
        // Project (worker fetches its own ProjectDefinition)
        .route(
            "/v1/project/fetch_definition",
            post(handlers::project_fetch_definition),
        )
        // Connections (worker data path). Cost records ride the generic
        // task rail (`/v1/task/enqueue_dedup`, kind `record_cost`).
        // One resolve for every connection: the worker sends a
        // connection id and the row decides everything (whose
        // credential, lazy refresh store-side, or the runtime's
        // credential source for an ours-owned row).
        .route("/v1/access/resolve", post(handlers::resolve_connection))
        .route("/v1/access/close", post(handlers::release_connection))
        // A node handing out a connection to something it runs itself
        // (the database its own infra spec brought up). Worker-only,
        // and the row it writes is always the user's own credential:
        // publishing can never reach the runtime's.
        .route("/v1/access/publish", post(handlers::publish_access))
        .route("/v1/access/published", post(handlers::published_access))
        .route("/v1/program/mint_instance_token", post(program_tokens::mint_instance_token))
        // Signals (the listener's rehydrate read)
        .route("/v1/signal/list_held", post(handlers::signal_list_held))
        .route("/v1/signal/get_held", post(handlers::signal_get_held))
        .route("/v1/signal/hold", post(handlers::signal_hold))
        .route("/v1/signal/let_go", post(handlers::signal_let_go))
        .route("/v1/signal/set_holds", post(handlers::signal_set_holds))
        .route("/v1/signal/write_kind_state", post(handlers::signal_write_kind_state))
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
        .route("/v1/supervisor/gone_copies", post(handlers::supervisor_gone_copies))
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
            "/v1/supervisor/set_waiting",
            post(handlers::supervisor_set_waiting),
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
        // Access admin: the connect/lookup verbs whose work makes an
        // outbound call to a tenant-influenced URL; they run here
        // because the broker's egress is locked to the public internet
        // (the dispatcher forwards them, like the file admin verbs).
        .merge(access_admin::routes())
        // Provider events: the listener's serving surface and the
        // receive-side verification the dispatcher forwards to.
        .merge(events::routes())
        .merge(caller_auth::routes())
        .with_state(state)
}
