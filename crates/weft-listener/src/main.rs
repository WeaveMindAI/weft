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
        project_id: std::env::var("WEFT_LISTENER_PROJECT_ID")
            .context("WEFT_LISTENER_PROJECT_ID")?,
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
    let app = router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(target: "weft_listener", %addr, "listener started");
    axum::serve(listener, app).await.context("axum serve")?;
    Ok(())
}
