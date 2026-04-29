//! The dispatcher binary. Starts the HTTP server, binds the
//! configured backends, mounts the API router.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use weft_dispatcher::{
    api::router,
    backend::{InfraBackend, K8sWorkerBackend, KindInfraBackend, WorkerBackend},
    infra::InfraRegistry,
    journal::postgres::PostgresJournal,
    listener::{
        K8sListenerBackend, ListenerBackend, ListenerPool, SubprocessListenerBackend,
    },
    tenant::{self, NamespaceMapper, TenantId, TenantRouter},
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

    // Tenant routing. OSS hardcodes a single tenant `local`; cloud
    // installs a router that derives tenant from the request's
    // auth context and a namespace mapper with its own prefix.
    let tenant_router: Arc<dyn TenantRouter> = tenant::local_router();
    let namespace_mapper: Arc<dyn NamespaceMapper> = match std::env::var("WEFT_TENANT_NS_PREFIX")
    {
        Ok(p) => Arc::new(tenant::PrefixNamespaceMapper::new(p)),
        Err(_) => tenant::local_namespace_mapper(),
    };

    // Resolve the URL listeners + workers use to call us back. In
    // cluster, that's the Service DNS for `weft-dispatcher` in the
    // system namespace. Locally (subprocess mode) it's loopback.
    let system_namespace =
        std::env::var("WEFT_SYSTEM_NAMESPACE").unwrap_or_else(|_| "weft-system".into());
    let dispatcher_callback_url =
        std::env::var("WEFT_DISPATCHER_CALLBACK_URL").unwrap_or_else(|_| {
            // Default depends on whether we're in cluster. The k8s
            // downward-API mount only exists inside a Pod.
            if std::path::Path::new("/var/run/secrets/kubernetes.io").exists() {
                format!(
                    "http://weft-dispatcher.{ns}.svc.cluster.local:{port}",
                    ns = system_namespace,
                    port = config.http_port,
                )
            } else {
                format!("http://127.0.0.1:{}", config.http_port)
            }
        });
    config.dispatcher_callback_url = dispatcher_callback_url.clone();

    if let Ok(t) = std::env::var("WEFT_INTERNAL_URL_TEMPLATE") {
        config.internal_url_template = t;
    }
    if let Ok(s) = std::env::var("WEFT_INTERNAL_SECRET") {
        config.internal_secret = s;
    }

    let database_url = std::env::var("WEFT_DATABASE_URL")
        .context("WEFT_DATABASE_URL is required (postgres://user:pass@host:port/db)")?;
    let journal = PostgresJournal::connect(&database_url)
        .await
        .with_context(|| format!("connect journal at {database_url}"))?;
    weft_dispatcher::lease::migrate(journal.pool())
        .await
        .context("apply lease migrations")?;
    let projects: weft_dispatcher::ProjectStore = std::sync::Arc::new(
        weft_dispatcher::PostgresProjectStore::new(journal.pool().clone())
            .await
            .context("init project store")?,
    );

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
                let image = std::env::var("WEFT_LISTENER_IMAGE")
                    .unwrap_or_else(|_| "weft-listener:local".into());
                let suffix = std::env::var("WEFT_LISTENER_HOST_SUFFIX")
                    .unwrap_or_else(|_| "listener.weft.local".into());
                Arc::new(K8sListenerBackend::new(image, suffix))
            }
        };

    let worker_backend: Arc<dyn WorkerBackend> =
        Arc::new(K8sWorkerBackend::new(dispatcher_callback_url.clone()));

    let infra_backend = Arc::new(KindInfraBackend::new());
    let infra_registry = InfraRegistry::new();

    // Rehydrate sidecars from each known tenant namespace. Without
    // this a dispatcher restart orphans every sidecar.
    let known_namespaces: Vec<String> =
        vec![namespace_mapper.namespace_for(&TenantId::local())];
    match infra_backend.rehydrate(&known_namespaces).await {
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

    let pod_id = weft_dispatcher::state::PodId::from_env();
    info!("dispatcher pod identity: {}", pod_id);

    let pg_pool = journal.pool().clone();
    let event_bus =
        weft_dispatcher::EventBus::with_postgres(pg_pool.clone(), pod_id.to_string());

    // Rehydrate ListenerPool from `tenant_listener` rows. We adopt
    // every row whose previous owner is not us: either a Pod that
    // just shut down (graceful drop) or a dead one (lease expired).
    // The persisted handle (URL + tokens) stays the same so the
    // listener Pod's env still matches; we just take over renewal.
    let listener_pool = ListenerPool::new();
    match weft_dispatcher::lease::list_tenant_listeners(&pg_pool).await {
        Ok(rows) => {
            for row in rows {
                let handle = weft_dispatcher::listener::ListenerHandle {
                    admin_url: row.admin_url.clone(),
                    public_base_url: row.public_base_url.clone(),
                    admin_token: row.admin_token.clone(),
                    relay_token: row.relay_token.clone(),
                };
                if row.owner_pod_id != pod_id.as_str() {
                    if let Err(e) = weft_dispatcher::lease::upsert_tenant_listener(
                        &pg_pool,
                        &row.tenant_id,
                        pod_id.as_str(),
                        &row.namespace,
                        &row.deploy_name,
                        &row.admin_url,
                        &row.public_base_url,
                        &row.admin_token,
                        &row.relay_token,
                    )
                    .await
                    {
                        tracing::warn!(
                            target: "weft_dispatcher",
                            tenant = %row.tenant_id,
                            error = %e,
                            "failed to adopt tenant_listener on startup"
                        );
                        continue;
                    }
                }
                listener_pool.insert(row.tenant_id, handle);
            }
        }
        Err(e) => {
            tracing::warn!("listener rehydrate failed: {e}");
        }
    }

    let state = DispatcherState {
        config: Arc::new(config.clone()),
        pod_id,
        journal: Arc::new(journal),
        pg_pool,
        workers: worker_backend,
        infra: infra_backend,
        projects,
        events: event_bus,
        slots: weft_dispatcher::slots::Slots::new(),
        listener_backend,
        listeners: listener_pool,
        infra_registry,
        tenant_router,
        namespace_mapper,
    };

    let shutdown_state = state.clone();
    let renewer_state = state.clone();
    tokio::spawn(async move {
        lease_renewer(renewer_state).await;
    });

    // Cross-Pod event fanout: subscribe to the Postgres NOTIFY
    // channel and ingest every payload into the local EventBus.
    let event_state = state.clone();
    tokio::spawn(async move {
        event_subscriber(event_state).await;
    });

    // Periodic worker-pod sweeper. Worker pods exit cleanly with
    // restartPolicy: Never, so K8s leaves them in Succeeded/Failed
    // phase forever. They consume only an etcd record (no CPU /
    // memory), but the user's `kubectl get pods` clutters fast.
    // Active workers and stalled-grace workers are in Running
    // phase, so this sweeper never touches them.
    let sweeper_namespaces = known_namespaces.clone();
    tokio::spawn(async move {
        worker_pod_sweeper(sweeper_namespaces).await;
    });

    let app = router(state);
    let addr = format!("0.0.0.0:{}", config.http_port);
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    info!("weft-dispatcher listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(graceful_shutdown(shutdown_state))
        .await?;
    Ok(())
}

/// Subscribe to the cross-Pod NOTIFY channel and ingest every
/// payload into the local EventBus. Reconnects on errors.
async fn event_subscriber(state: DispatcherState) {
    use sqlx::postgres::PgListener;
    loop {
        let listener = match PgListener::connect_with(&state.pg_pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    target: "weft_dispatcher::events",
                    error = %e,
                    "event listener connect failed; retrying"
                );
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        let mut listener = listener;
        if let Err(e) = listener
            .listen(weft_dispatcher::events::NOTIFY_CHANNEL)
            .await
        {
            tracing::warn!(target: "weft_dispatcher::events", "LISTEN failed: {e}");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            continue;
        }
        loop {
            match listener.recv().await {
                Ok(notification) => {
                    state.events.ingest_remote(notification.payload()).await;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "weft_dispatcher::events",
                        error = %e,
                        "event listener disconnected; reconnecting"
                    );
                    break;
                }
            }
        }
    }
}

/// Periodic sweeper that deletes worker pods in terminal phase
/// (Succeeded/Failed) older than the grace window. Workers run
/// with `restartPolicy: Never`, so the K8s record stays around
/// after the process exits — eats etcd space, clutters
/// `kubectl get`. Active workers (mid-run, mid-stall-grace) are
/// in Running phase, so this sweeper never touches them.
///
/// Interval: every 5 minutes.
/// Threshold: pod must have been terminal for >10 minutes. Keeps
/// just-finished pods inspectable for `kubectl logs` for a bit.
async fn worker_pod_sweeper(namespaces: Vec<String>) {
    use tokio::process::Command;
    let interval = std::time::Duration::from_secs(300);
    let min_age_secs: i64 = 600;
    loop {
        tokio::time::sleep(interval).await;
        for ns in &namespaces {
            let out = Command::new("kubectl")
                .args([
                    "-n", ns,
                    "get", "pods",
                    "-l", "weft.dev/role=worker",
                    "--field-selector=status.phase=Succeeded",
                    "-o",
                    "jsonpath={range .items[*]}{.metadata.name}\t{.status.containerStatuses[0].state.terminated.finishedAt}\n{end}",
                ])
                .output()
                .await;
            let stdout = match out {
                Ok(o) if o.status.success() => o.stdout,
                _ => continue,
            };
            let now = chrono::Utc::now();
            let text = String::from_utf8_lossy(&stdout);
            for line in text.lines() {
                let mut it = line.splitn(2, '\t');
                let Some(name) = it.next() else { continue };
                let Some(ts) = it.next() else { continue };
                if name.is_empty() || ts.is_empty() {
                    continue;
                }
                let Ok(finished_at) = ts.parse::<chrono::DateTime<chrono::Utc>>() else {
                    continue;
                };
                if (now - finished_at).num_seconds() < min_age_secs {
                    continue;
                }
                let _ = Command::new("kubectl")
                    .args([
                        "-n", ns, "delete", "pod", name,
                        "--ignore-not-found", "--wait=false",
                    ])
                    .output()
                    .await;
            }
        }
    }
}

/// Background loop that renews every lease this Pod owns. If a
/// renewal fails (lease was stolen because we missed the window),
/// we don't try to recover; the slot/listener will be cleaned up
/// next time the owning code path runs.
async fn lease_renewer(state: DispatcherState) {
    use weft_dispatcher::lease;
    let interval = std::time::Duration::from_secs(lease::LEASE_RENEW_INTERVAL_SECS);
    let pod_id = state.pod_id.as_str().to_string();
    loop {
        tokio::time::sleep(interval).await;

        let pool = state.pg_pool.clone();
        let pid = pod_id.clone();

        // Renew slot leases.
        let slot_rows: Result<Vec<(String,)>, _> = sqlx::query_as(
            "SELECT color FROM slot_lease WHERE owner_pod_id = $1",
        )
        .bind(&pid)
        .fetch_all(&pool)
        .await;
        if let Ok(rows) = slot_rows {
            for (color_str,) in rows {
                if let Ok(color) = color_str.parse::<weft_core::Color>() {
                    let _ = lease::renew_slot(&pool, color, &pid).await;
                }
            }
        }

        // Renew tenant listener leases.
        let tenant_rows: Result<Vec<(String,)>, _> = sqlx::query_as(
            "SELECT tenant_id FROM tenant_listener WHERE owner_pod_id = $1",
        )
        .bind(&pid)
        .fetch_all(&pool)
        .await;
        if let Ok(rows) = tenant_rows {
            for (tenant_id,) in rows {
                let _ = lease::renew_tenant_listener(&pool, &tenant_id, &pid).await;
            }
        }
    }
}

/// Wait for SIGTERM / Ctrl+C, then release every lease this Pod
/// owns before letting axum drain. Fast handover: the next claim
/// from another Pod sees a free row instead of having to wait
/// `LEASE_DURATION_SECS` for expiry.
async fn graceful_shutdown(state: DispatcherState) {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("install SIGTERM handler failed: {e}");
            return;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received; releasing leases");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl+C received; releasing leases");
        }
    }

    if let Err(e) = release_owned_leases(&state).await {
        tracing::warn!("releasing leases failed (ignoring): {e}");
    }
}

async fn release_owned_leases(state: &DispatcherState) -> anyhow::Result<()> {
    let pool = state.pg_pool.clone();
    let pod_id = state.pod_id.as_str();

    // Release every slot we own. The next dispatcher Pod can claim
    // them immediately instead of waiting for the lease to expire.
    let slot_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT color FROM slot_lease WHERE owner_pod_id = $1",
    )
    .bind(pod_id)
    .fetch_all(&pool)
    .await?;
    for (color_str,) in slot_rows {
        if let Ok(color) = color_str.parse::<weft_core::Color>() {
            let _ = weft_dispatcher::lease::release_slot(&pool, color, pod_id).await;
        }
    }

    // Tenant listener rows STAY. The listener Pod itself doesn't
    // care which dispatcher Pod owns its lease; what matters is
    // the URL + tokens stored in the row. Deleting the row would
    // make the next dispatcher Pod re-spawn the listener with a
    // fresh `kubectl apply`, generating new tokens. But the old
    // listener Pod is still running with the OLD tokens until the
    // new Pod rolls out, so register requests in that window get
    // 401 from the still-alive old Pod. Keeping the row preserves
    // tokens across dispatcher restarts: the next Pod re-attaches
    // to the same listener Deployment, same tokens, no spawn.
    Ok(())
}

