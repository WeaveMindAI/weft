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
pub mod door;
pub mod events;
pub mod runtime_storage;
pub mod runtime_store;
pub mod scope;
pub mod held_signals;
pub mod line;
pub mod news;
pub mod notices;
pub mod records;
pub mod state;
pub mod wire;

use std::sync::Arc;

use axum::{routing::post, Router};

pub use auth::AuthConfig;
pub use state::BrokerState;

/// The broker's background loops: the expiry of stored files. It removes
/// what expired and books its next look at the next expiry; a write that
/// sets an expiry sooner than any this copy rang for rings it
/// (`runtime_store::FILE_EXPIRY_CHANNEL`). Stateless and idempotent, so
/// every copy of the broker may run it. The runtime runs it where the
/// broker is placed (`weft_task_store::drain`). Refuses to boot when the
/// loops and [`loop_wakes`] disagree: a broker at zero is woken by name
/// for what that list says.
pub fn drain_loops(state: Arc<BrokerState>) -> anyhow::Result<Vec<weft_task_store::drain::DrainLoop>> {
    loops_over(state.runtime_store.clone())
}

fn loops_over(store: Arc<runtime_store::RuntimeStore>) -> anyhow::Result<Vec<weft_task_store::drain::DrainLoop>> {
    use weft_task_store::drain::DrainLoop;
    let loops = vec![DrainLoop::new(
        runtime_store::FILE_EXPIRY_LOOP,
        runtime_store::FILE_EXPIRY_LABEL,
        runtime_store::FILE_EXPIRY_WAKE,
        std::time::Duration::from_secs(60),
        move || {
            let store = store.clone();
            async move { store.expire().await }
        },
    )];
    let listed = loop_wakes();
    for l in loops.iter().filter(|l| !l.wake_on.is_empty()) {
        anyhow::ensure!(
            listed.iter().any(|(name, wake_on)| *name == l.name && std::ptr::eq(*wake_on, l.wake_on)),
            "the broker's loop '{}' wakes by rules `loop_wakes` does not name",
            l.name
        );
    }
    for (name, _) in &listed {
        anyhow::ensure!(loops.iter().any(|l| l.name == *name), "`loop_wakes` names '{name}', which is no loop of the broker");
    }
    Ok(loops)
}

/// What wakes each of the broker's loops that a write wakes, by name: what
/// a writer reads to wake a broker at zero for exactly the loops a write
/// concerns (`weft_runtime::role_waker`). Read at run time from each
/// loop's own static, so a rule here IS the loop's.
pub fn loop_wakes() -> Vec<(&'static str, &'static [weft_task_store::drain::WakeOn])> {
    vec![(runtime_store::FILE_EXPIRY_LOOP, runtime_store::FILE_EXPIRY_WAKE)]
}

pub fn router(state: Arc<BrokerState>) -> Router {
    line::routes(api(state.clone()), state)
}

/// The holders' check (`weft_platform_traits::HELD_CHECK_PATH`), for the
/// runtime to mount behind the guard that lets only weft itself in: the
/// alarm calls it with weft's own account, never a role's. It holds the
/// database's listening while it is answered, so the notification its
/// release fires is heard and rings the dispatcher's `holders` loop.
pub fn held_check_route(state: Arc<BrokerState>) -> Router {
    let signals = state.signals.clone();
    Router::new()
        .route(weft_platform_traits::HELD_CHECK_PATH, post(handlers::held_check))
        .layer(axum::middleware::from_fn_with_state(signals, weft_task_store::pg_signal::keep_listening))
        .with_state(state)
}

/// Every call the broker answers, as a request and on a line alike.
fn api(state: Arc<BrokerState>) -> Router {
    Router::new()
        // Run steering (`ctx.tag_run` / `ctx.stop_tagged`)
        .route("/v1/run/tag", post(handlers::run_tag))
        .route(
            "/v1/run/stop_tagged",
            post(handlers::run_stop_tagged),
        )
        // Tasks
        .route("/v1/task/enqueue_dedup", post(handlers::task_enqueue_dedup))
        .route(
            "/v1/task/wait_terminal",
            post(handlers::task_wait_terminal),
        )
        // Infra reads
        .route(
            "/v1/infra/endpoint_url",
            post(handlers::infra_endpoint_url),
        )
        .route("/v1/infra/baked", post(handlers::infra_baked))
        .route("/v1/infra/bake", post(handlers::infra_bake))
        // A machine running a project's infra asking for a look at it.
        .route("/v1/infra/look", post(handlers::infra_look))
        // Project (worker fetches its own ProjectDefinition)
        .route(
            "/v1/project/fetch_definition",
            post(handlers::project_fetch_definition),
        )
        // Connections (worker data path). One resolve for every connection: the worker sends a
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
        .route("/v1/signal/fire_target", post(handlers::signal_fire_target))
        .route("/v1/signal/hold", post(handlers::signal_hold))
        .route("/v1/signal/let_go", post(handlers::signal_let_go))
        .route("/v1/signal/holder_load", post(handlers::signal_holder_load))
        .route("/v1/signal/give_up", post(handlers::signal_give_up))
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
            "/v1/supervisor/health_state",
            post(handlers::supervisor_health_state),
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
        .route("/v1/supervisor/set_health_state", post(handlers::supervisor_set_health_state))
        .route(
            "/v1/supervisor/infra_command_in_flight",
            post(handlers::supervisor_infra_command_in_flight),
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
        .route("/v1/infra/pushed", post(handlers::infra_pushed))
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
        // because the broker holds the apps file's secrets. The
        // dispatcher forwards them, like the file admin verbs.
        .merge(access_admin::routes())
        // Provider events: the listener's serving surface and the
        // receive-side verification the dispatcher forwards to.
        .merge(events::routes())
        .merge(caller_auth::routes())
        // The worker's door: its routes, its runs' facts, its limits.
        .merge(door::routes())
        // A worker's records, and the runs it drives.
        .merge(records::routes())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A broker that scales to zero boots: every one of its loops is woken
    /// by a write (`weft_task_store::drain::check_woken`, which the runtime
    /// runs at boot), and the loops agree with `loop_wakes`.
    #[tokio::test]
    async fn the_brokers_loops_boot_scaled_to_zero() {
        let nobody = weft_task_store::db::config("postgres://nobody@127.0.0.1:1/none").expect("a Postgres address");
        let pool = weft_task_store::db::Pool::open(nobody, 1, std::time::Duration::from_secs(1), weft_task_store::db::Waits::Expected);
        let store = Arc::new(runtime_store::RuntimeStore::new(
            pool,
            Arc::new(weft_platform_traits::object_store::fake::FakeObjectStore::new()),
            Arc::new(weft_platform_traits::clock::SystemClock),
        ));
        let loops = loops_over(store).expect("the loops agree with loop_wakes");
        weft_task_store::drain::check_woken(weft_platform_traits::CoreRole::Broker, &loops).expect("every loop is woken");
    }
}
