//! Weft Orchestrator
//!
//! Runs two servers:
//! 1. Restate auxiliary services (TaskRegistry, NodeInstanceRegistry,
//!    InfrastructureManager)
//! 2. Axum-based in-memory ProjectExecutor
//!
//! The axum executor handles all project execution in-memory with zero
//! journaling overhead. Restate is used only for durable auxiliary services.

mod executor_axum;

use restate_sdk::prelude::*;
use weft_core::{
    TaskRegistryImpl, TaskRegistry,
    NodeInstanceRegistryImpl, NodeInstanceRegistry,
    InfrastructureManagerImpl, InfrastructureManager,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    tracing::info!("Starting Weft Orchestrator");

    let restate_port = std::env::var("ORCHESTRATOR_PORT").unwrap_or_else(|_| "9080".to_string());
    let restate_addr = format!("0.0.0.0:{}", restate_port);

    let axum_port = std::env::var("EXECUTOR_PORT").unwrap_or_else(|_| "9081".to_string());
    let axum_addr = format!("0.0.0.0:{}", axum_port);

    let restate_url = std::env::var("RESTATE_URL")
        .unwrap_or_else(|_| "http://localhost:8180".to_string());

    // The callback base is how node services reach back to the axum executor.
    // In production this would be the external URL; locally it's localhost:EXECUTOR_PORT.
    let callback_base = std::env::var("EXECUTOR_CALLBACK_URL")
        .unwrap_or_else(|_| format!("http://localhost:{}", axum_port));

    tracing::info!("Restate services on {}", restate_addr);
    tracing::info!("Axum executor on {} (callback_base={})", axum_addr, callback_base);

    // Build axum executor
    let executor_state = std::sync::Arc::new(
        executor_axum::ExecutorState::new(restate_url, callback_base)
    );
    let axum_app = executor_axum::router(executor_state);

    // Spawn axum server
    let axum_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&axum_addr).await
            .expect("Failed to bind axum executor");
        tracing::info!("Axum executor listening on {}", axum_addr);
        axum::serve(listener, axum_app).await
            .expect("Axum executor crashed");
    });

    // Run Restate services (blocking)
    HttpServer::new(
        Endpoint::builder()
            .bind(TaskRegistryImpl.serve())
            .bind(NodeInstanceRegistryImpl.serve())
            .bind(InfrastructureManagerImpl.serve())
            .build(),
    )
    .listen_and_serve(restate_addr.parse()?)
    .await;

    axum_handle.abort();
    Ok(())
}
