//! The dispatcher binary. Starts the HTTP server, binds the
//! configured backends, mounts the API router.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use weft_dispatcher::{
    api::router,
    backend::{KindInfraBackend, SubprocessWorkerBackend},
    journal::sqlite::SqliteJournal,
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
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weft_dispatcher=info,tower_http=debug".into()),
        )
        .init();

    let args = Args::parse();
    let mut config = DispatcherConfig::default();
    if let Some(port) = args.http_port {
        config.http_port = port;
    }

    let self_url = format!("http://localhost:{}", config.http_port);
    let projects_dir = config.data_dir.join("projects");
    let projects = weft_dispatcher::ProjectStore::new(projects_dir)?;
    let journal_path = config.data_dir.join("journal.sqlite");
    let journal = SqliteJournal::open(&journal_path)
        .await
        .with_context(|| format!("open journal at {}", journal_path.display()))?;

    let state = DispatcherState {
        config: Arc::new(config.clone()),
        journal: Arc::new(journal),
        workers: Arc::new(SubprocessWorkerBackend::new("", self_url)),
        infra: Arc::new(KindInfraBackend::new()),
        projects,
        events: weft_dispatcher::EventBus::new(),
        slots: weft_dispatcher::slots::Slots::new(),
        scheduler: weft_dispatcher::scheduler::Scheduler::new(),
    };

    let app = router(state);
    let addr = format!("0.0.0.0:{}", config.http_port);
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    info!("weft-dispatcher listening on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

