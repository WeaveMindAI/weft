//! The dispatcher binary. Starts the HTTP server, binds the configured
//! backends, mounts the API router.

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use weft_dispatcher::{
    api::router,
    backend::{EventStream, InfraBackend, InfraHandle, InfraSpec, WakeContext, WorkerBackend, WorkerHandle},
    journal::{Journal, WakeTarget},
    DispatcherConfig, DispatcherState,
};

#[derive(Debug, Parser)]
#[command(name = "weft-dispatcher", version)]
struct Args {
    #[arg(long, env = "WEFT_HTTP_PORT")]
    http_port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "weft_dispatcher=info,tower_http=debug".into()))
        .init();

    let args = Args::parse();
    let mut config = DispatcherConfig::default();
    if let Some(port) = args.http_port {
        config.http_port = port;
    }

    let state = DispatcherState {
        config: Arc::new(config.clone()),
        journal: Arc::new(StubJournal),
        workers: Arc::new(StubWorkerBackend),
        infra: Arc::new(StubInfraBackend),
    };

    let app = router(state);
    let addr = format!("0.0.0.0:{}", config.http_port);
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    info!("weft-dispatcher listening on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

// ----- Phase A1 stubs -------------------------------------------------
// Real impls land in phase A2. Keeping the binary runnable end-to-end
// so we can curl it even though no work actually happens.

struct StubJournal;

#[async_trait]
impl Journal for StubJournal {
    async fn record_start(&self, _color: uuid::Uuid, _project_id: &str, _entry_node: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn record_suspension(&self, _color: uuid::Uuid, _node: &str, _metadata: serde_json::Value) -> anyhow::Result<()> {
        Ok(())
    }
    async fn resolve_wake(&self, _token: &str) -> anyhow::Result<Option<WakeTarget>> {
        Ok(None)
    }
    async fn record_cost(&self, _color: uuid::Uuid, _report: weft_core::CostReport) -> anyhow::Result<()> {
        Ok(())
    }
    async fn cancel(&self, _color: uuid::Uuid) -> anyhow::Result<()> {
        Ok(())
    }
}

struct StubWorkerBackend;

#[async_trait]
impl WorkerBackend for StubWorkerBackend {
    async fn spawn_worker(&self, _binary_path: &std::path::PathBuf, _wake: WakeContext) -> anyhow::Result<WorkerHandle> {
        anyhow::bail!("worker backend not yet implemented")
    }
    async fn kill_worker(&self, _handle: WorkerHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct StubInfraBackend;

#[async_trait]
impl InfraBackend for StubInfraBackend {
    async fn provision(&self, _spec: InfraSpec) -> anyhow::Result<InfraHandle> {
        anyhow::bail!("infra backend not yet implemented")
    }
    async fn deprovision(&self, _handle: InfraHandle) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stream_events(&self, _handle: InfraHandle) -> anyhow::Result<EventStream> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}
