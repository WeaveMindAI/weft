//! `weft-listener` binary. Parses config from env, connects to the
//! broker, starts the HTTP server, rehydrates its registry from the
//! durable `signal` table (read via the broker).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use weft_broker_client::{BrokerTaskStoreClient, TokenSource};
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
        pod_name: std::env::var("WEFT_POD_NAME").context("WEFT_POD_NAME")?,
        http_port: std::env::var("WEFT_LISTENER_PORT")
            .unwrap_or_else(|_| "8080".into())
            .parse()
            .context("WEFT_LISTENER_PORT must be a valid port")?,
        broker_url: std::env::var("WEFT_BROKER_URL").context("WEFT_BROKER_URL")?,
    };
    let port = config.http_port;
    let token_source = TokenSource::new(
        std::env::var("WEFT_BROKER_TOKEN_PATH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| TokenSource::default_path()),
    );
    let tasks = BrokerTaskStoreClient::new(config.broker_url.clone(), token_source.clone());

    let state = ListenerState::new(config, tasks.clone(), token_source.clone())
        .await
        .context("ListenerState::new")?;

    // Rehydrate the in-memory registry by asking the broker for the
    // signal rows placed on this pod. Synchronous: the HTTP server
    // only starts accepting fires after the registry matches the DB.
    weft_listener::registry::rehydrate(
        tasks.clone(),
        Arc::new(state.config.broker_url.clone()),
        token_source,
        &state.config.pod_name,
        state.registry.clone(),
        state.config.clone(),
    )
    .await
    .context("registry rehydrate")?;

    let app = router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(target: "weft_listener", %addr, "listener started");
    axum::serve(listener, app).await.context("axum serve")?;
    Ok(())
}
