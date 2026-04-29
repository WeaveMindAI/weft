//! `weft-listener` binary. Parses config from env vars, starts the
//! HTTP server. Designed to run as a pod in the project's k8s
//! namespace; also runnable locally for testing.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use weft_listener::{router, ListenerConfig, ListenerState};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_listener=info".into()),
        )
        .init();

    let config = ListenerConfig {
        tenant_id: std::env::var("WEFT_LISTENER_TENANT_ID")
            .context("WEFT_LISTENER_TENANT_ID")?,
        http_port: std::env::var("WEFT_LISTENER_PORT")
            .unwrap_or_else(|_| "8080".into())
            .parse()
            .context("WEFT_LISTENER_PORT must be a valid port")?,
        public_base_url: std::env::var("WEFT_LISTENER_PUBLIC_BASE_URL")
            .context("WEFT_LISTENER_PUBLIC_BASE_URL")?,
        dispatcher_url: std::env::var("WEFT_LISTENER_DISPATCHER_URL")
            .context("WEFT_LISTENER_DISPATCHER_URL")?,
        relay_token: std::env::var("WEFT_LISTENER_RELAY_TOKEN")
            .context("WEFT_LISTENER_RELAY_TOKEN")?,
        admin_token: std::env::var("WEFT_LISTENER_ADMIN_TOKEN")
            .context("WEFT_LISTENER_ADMIN_TOKEN")?,
    };
    let port = config.http_port;
    let state = ListenerState::new(config);
    let app = router(state.clone());
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(target: "weft_listener", %addr, "listener started");

    // Once the HTTP server is ready, ask the dispatcher to re-push
    // any signals already on file for this tenant. Pod restart
    // wipes our in-memory Registry; this rebuilds it.
    let bootstrap_state = state.clone();
    tokio::spawn(async move {
        // Tiny delay so the HTTP server is bound before the
        // dispatcher tries to call us back via /register.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        if let Err(e) = bootstrap_state.relay.request_rehydrate().await {
            tracing::warn!(
                target: "weft_listener",
                error = %e,
                "register-me bootstrap call failed (dispatcher unreachable?)"
            );
        }
    });

    // Idle self-shutdown heartbeat. Every 5 minutes, if the local
    // registry has been empty since the last tick, re-post
    // `/listener/empty` to the dispatcher. Catches the case where
    // a previous `unregister_after_fire`'s notify lost a race or
    // hit a 409 from stale journal rows; the dispatcher can now
    // re-evaluate with a clean journal and tear us down. Cost is
    // one HTTP round-trip per tenant per 5 min, distributed across
    // listeners so the dispatcher never centralizes the work.
    let heartbeat_state = state.clone();
    tokio::spawn(async move {
        let mut last_empty = false;
        let interval = std::time::Duration::from_secs(300);
        loop {
            tokio::time::sleep(interval).await;
            let now_empty = heartbeat_state.registry.is_empty();
            if now_empty && last_empty {
                heartbeat_state.relay.maybe_notify_empty().await;
            }
            last_empty = now_empty;
        }
    });

    axum::serve(listener, app).await.context("axum serve")?;
    Ok(())
}
