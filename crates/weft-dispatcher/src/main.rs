//! The dispatcher binary. Starts the HTTP server, binds the
//! configured backends, mounts the API router.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use weft_dispatcher::{
    api::router,
    backend::{
        InfraBackend, K8sWorkerBackend, KindInfraBackend, SubprocessWorkerBackend, WorkerBackend,
    },
    infra::InfraRegistry,
    journal::sqlite::SqliteJournal,
    listener::{
        K8sListenerBackend, ListenerBackend, ListenerRegistry, SignalTracker,
        SubprocessListenerBackend,
    },
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

    // Backend selection. Default = k8s (dispatcher runs inside kind).
    // WEFT_LISTENER_BACKEND=subprocess picks the local binary path
    // (useful for unit tests and legacy flows).
    let listener_backend: Arc<dyn ListenerBackend> =
        match std::env::var("WEFT_LISTENER_BACKEND").as_deref() {
            Ok("subprocess") => {
                let bin = std::env::var("WEFT_LISTENER_BIN")
                    .ok()
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| {
                        std::env::current_exe()
                            .ok()
                            .and_then(|p| p.parent().map(|d| d.join("weft-listener")))
                            .unwrap_or_else(|| std::path::PathBuf::from("weft-listener"))
                    });
                Arc::new(SubprocessListenerBackend::new(bin))
            }
            _ => {
                let namespace =
                    std::env::var("WEFT_NAMESPACE").unwrap_or_else(|_| "wm-local".into());
                let image = std::env::var("WEFT_LISTENER_IMAGE")
                    .unwrap_or_else(|_| "weft-listener:local".into());
                let suffix = std::env::var("WEFT_LISTENER_HOST_SUFFIX")
                    .unwrap_or_else(|_| "listener.weft.local".into());
                Arc::new(K8sListenerBackend::new(namespace, image, suffix))
            }
        };

    let worker_backend: Arc<dyn WorkerBackend> =
        match std::env::var("WEFT_WORKER_BACKEND").as_deref() {
            Ok("subprocess") => {
                Arc::new(SubprocessWorkerBackend::new("", self_url.clone()))
            }
            _ => {
                let namespace =
                    std::env::var("WEFT_NAMESPACE").unwrap_or_else(|_| "wm-local".into());
                // Workers talk to the dispatcher via cluster DNS
                // because they live in the same namespace. Override
                // via env for unusual setups.
                let in_cluster_url = std::env::var("WEFT_DISPATCHER_CLUSTER_URL")
                    .unwrap_or_else(|_| {
                        format!(
                            "http://weft-dispatcher.{ns}.svc.cluster.local:{port}",
                            ns = namespace,
                            port = config.http_port,
                        )
                    });
                Arc::new(K8sWorkerBackend::new(namespace, in_cluster_url))
            }
        };

    let infra_backend = Arc::new(KindInfraBackend::new());
    let infra_registry = InfraRegistry::new();

    // Rehydrate the registry from any Deployments labeled
    // `weft.dev/role=infra` in our namespace. Without this a
    // dispatcher restart orphans every sidecar: the in-memory
    // registry starts empty, `start` provisions a fresh pod, and
    // the old one stays running forever.
    match infra_backend.rehydrate().await {
        Ok(adopted) => {
            for a in adopted {
                let status = if a.running {
                    weft_dispatcher::infra::InfraStatus::Running
                } else {
                    weft_dispatcher::infra::InfraStatus::Stopped
                };
                infra_registry.insert_with_status(a.project_id, a.node_id, a.handle, status);
            }
        }
        Err(e) => {
            info!("infra rehydrate failed (cluster access?): {e}");
        }
    }

    let state = DispatcherState {
        config: Arc::new(config.clone()),
        journal: Arc::new(journal),
        workers: worker_backend,
        infra: infra_backend,
        projects,
        events: weft_dispatcher::EventBus::new(),
        slots: weft_dispatcher::slots::Slots::new(),
        listener_backend,
        listeners: ListenerRegistry::new(),
        signal_tracker: SignalTracker::new(),
        infra_registry,
    };

    let app = router(state);
    let addr = format!("0.0.0.0:{}", config.http_port);
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    info!("weft-dispatcher listening on {}", addr);
    axum::serve(listener, app).await?;
    Ok(())
}

