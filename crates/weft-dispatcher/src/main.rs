//! The dispatcher binary. Starts the HTTP server, binds the
//! configured backends, mounts the API router.

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use weft_dispatcher::{
    api::router,
    backend::{K8sWorkerBackend, WorkerBackend},
    journal::postgres::PostgresJournal,
    listener::{
        K8sListenerBackend, ListenerBackend, ListenerPool, SubprocessListenerBackend,
    },
    tenant::{self, NamespaceMapper, TenantRouter},
    DispatcherState,
};

#[derive(Debug, Parser)]
#[command(name = "weft-dispatcher", version)]
struct Args {
    #[arg(long, env = "WEFT_HTTP_PORT", default_value_t = 9999)]
    http_port: u16,
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
    let http_port = args.http_port;

    // Tenant routing. OSS hardcodes a single tenant `local`; cloud
    // installs a router that derives tenant from the request's
    // auth context and a namespace mapper with its own prefix.
    let tenant_router: Arc<dyn TenantRouter> = tenant::local_router();
    let namespace_mapper: Arc<dyn NamespaceMapper> = match std::env::var("WEFT_TENANT_NS_PREFIX")
    {
        Ok(p) => Arc::new(tenant::PrefixNamespaceMapper::new(p)),
        Err(_) => tenant::local_namespace_mapper(),
    };

    let database_url = std::env::var("WEFT_DATABASE_URL")
        .context("WEFT_DATABASE_URL is required (postgres://user:pass@host:port/db)")?;
    let journal = PostgresJournal::connect(&database_url)
        .await
        .with_context(|| format!("connect journal at {database_url}"))?;
    weft_dispatcher::lease::migrate(journal.pool())
        .await
        .context("apply lease migrations")?;
    weft_dispatcher::namespace_registry::migrate(journal.pool())
        .await
        .context("apply namespace_registry migrations")?;
    weft_task_store::migrate(journal.pool())
        .await
        .context("apply task-store migrations")?;
    weft_dispatcher::infra_node::migrate(journal.pool())
        .await
        .context("apply infra_node migrations")?;
    weft_dispatcher::infra_event::migrate(journal.pool())
        .await
        .context("apply infra_event migrations")?;
    weft_dispatcher::infra_lifecycle_command::migrate(journal.pool())
        .await
        .context("apply infra_lifecycle_command migrations")?;
    weft_dispatcher::journal_bridge::migrate(journal.pool())
        .await
        .context("apply journal_bridge cursor migrations")?;
    weft_dispatcher::infra_event_bridge::migrate(journal.pool())
        .await
        .context("apply infra_event_bridge cursor migrations")?;
    let projects: weft_dispatcher::ProjectStore = std::sync::Arc::new(
        weft_dispatcher::PostgresProjectStore::new(journal.pool().clone())
            .await
            .context("init project store")?,
    );

    // Broker URL: every tenant pod (listener / worker / infra)
    // talks to the broker instead of touching Postgres. Required
    // in-cluster; subprocess dev wires it through env.
    let in_cluster_for_broker = std::env::var("KUBERNETES_SERVICE_HOST").is_ok();
    let broker_url = match std::env::var("WEFT_BROKER_URL") {
        Ok(v) => v,
        Err(_) => {
            if in_cluster_for_broker {
                anyhow::bail!(
                    "WEFT_BROKER_URL is required in-cluster; \
                     set it on the dispatcher Deployment to the in-cluster \
                     broker URL (typically http://weft-broker.weft-db.svc.cluster.local:9090)"
                );
            }
            "http://localhost:9090".to_string()
        }
    };

    let kube = weft_platform_traits::kube::in_cluster()
        .await
        .context("init kubectl client (dispatcher needs `kubectl` in PATH)")?;

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
                Arc::new(K8sListenerBackend::new(image, broker_url.clone(), kube.clone()))
            }
        };

    let worker_backend: Arc<dyn WorkerBackend> =
        Arc::new(K8sWorkerBackend::new(
            broker_url.clone(),
            kube.clone(),
            Arc::new(weft_platform_traits::clock::SystemClock),
        ));

    // Cluster CIDR knobs. Required in-cluster so the dispatcher can
    // render NetworkPolicies that allow internet egress while denying
    // intra-cluster Pod traffic. Subprocess dev defaults are Kind's.
    let cluster_pod_cidr = match std::env::var("WEFT_CLUSTER_POD_CIDR") {
        Ok(v) => v,
        Err(_) => {
            if in_cluster_for_broker {
                anyhow::bail!(
                    "WEFT_CLUSTER_POD_CIDR is required in-cluster; \
                     set it to the cluster's Pod CIDR (e.g. 10.244.0.0/16 for Kind)"
                );
            }
            "10.244.0.0/16".to_string()
        }
    };
    let cluster_service_cidr = match std::env::var("WEFT_CLUSTER_SERVICE_CIDR") {
        Ok(v) => v,
        Err(_) => {
            if in_cluster_for_broker {
                anyhow::bail!(
                    "WEFT_CLUSTER_SERVICE_CIDR is required in-cluster; \
                     set it to the cluster's Service CIDR (e.g. 10.96.0.0/12 for Kind)"
                );
            }
            "10.96.0.0/12".to_string()
        }
    };
    // Sanity-check CIDRs. We don't do full IP arithmetic, but a
    // typo'd value silently produces broken NetworkPolicies (allows
    // traffic the policy meant to block). The shape is `<ip>/<prefix>`
    // with both halves present.
    for (name, value) in [
        ("WEFT_CLUSTER_POD_CIDR", &cluster_pod_cidr),
        ("WEFT_CLUSTER_SERVICE_CIDR", &cluster_service_cidr),
    ] {
        let mut parts = value.splitn(2, '/');
        let ip = parts.next().unwrap_or("");
        let prefix = parts.next().unwrap_or("");
        if ip.is_empty()
            || prefix.is_empty()
            || prefix.parse::<u8>().is_err()
            || ip.split('.').count() != 4
        {
            anyhow::bail!(
                "{name}='{value}' is not a valid IPv4 CIDR (expected `a.b.c.d/N`)"
            );
        }
    }

    let cluster_ingress_namespace = std::env::var("WEFT_CLUSTER_INGRESS_NAMESPACE")
        .unwrap_or_else(|_| "ingress-nginx".to_string());
    let supervisor_image = std::env::var("WEFT_SUPERVISOR_IMAGE")
        .unwrap_or_else(|_| "weft-infra-supervisor:local".to_string());

    // Infra rehydrate on restart: the supervisor pod owns runtime
    // tracking, so the dispatcher doesn't need to scan k8s. Each
    // tenant's supervisor will list pods on its own startup and
    // reconcile `infra_node` rows accordingly.

    let pod_id = weft_dispatcher::state::PodId::from_env();
    info!("dispatcher pod identity: {}", pod_id);

    let pg_pool = journal.pool().clone();
    let event_bus = weft_dispatcher::EventBus::with_notify(pg_pool.clone()).await?;

    // ListenerPool is stateless: every `with_listener` call reads
    // the `tenant_listener` row through an advisory-locked transaction
    // and returns the fresh handle. No RAM cache means no staleness
    // possible, at the cost of one DB round-trip per listener call.
    let listener_pool = ListenerPool::new();

    // Public base URL: this is what users hit for webhooks /
    // activation URLs. In-cluster the manifest must set
    // WEFT_DISPATCHER_PUBLIC_BASE_URL to the external ingress;
    // outside the cluster (local dev), localhost is the right
    // default. Detect the cluster via KUBERNETES_SERVICE_HOST
    // (always set inside a Pod). In-cluster, the URL must be the
    // real external ingress: fail loud if the env var is unset OR
    // its host is loopback. Outside the cluster (local dev), the
    // localhost default is fine.
    let in_cluster = std::env::var("KUBERNETES_SERVICE_HOST").is_ok();
    let public_base_url = match std::env::var("WEFT_DISPATCHER_PUBLIC_BASE_URL") {
        Ok(v) => {
            if in_cluster && is_loopback_url(&v) {
                anyhow::bail!(
                    "WEFT_DISPATCHER_PUBLIC_BASE_URL='{v}' resolves to a loopback \
                     host in-cluster; set it on the dispatcher Deployment to the \
                     external ingress URL before deploying"
                );
            }
            v
        }
        Err(_) => {
            if in_cluster {
                anyhow::bail!(
                    "WEFT_DISPATCHER_PUBLIC_BASE_URL is required in-cluster; \
                     set it on the dispatcher Deployment to the external ingress URL"
                );
            }
            format!("http://localhost:{}", http_port)
        }
    };

    let state = DispatcherState {
        pod_id,
        journal: Arc::new(journal),
        pg_pool,
        workers: worker_backend,
        projects,
        events: event_bus,
        listener_backend,
        listeners: listener_pool,
        tenant_router,
        namespace_mapper,
        public_base_url,
        cluster_pod_cidr,
        cluster_service_cidr,
        cluster_ingress_namespace,
        supervisor_image,
        kube,
    };

    let renewer_state = state.clone();
    tokio::spawn(async move {
        lease_renewer(renewer_state).await;
    });

    // Journal-to-EventBus bridge: convert new exec_event rows
    // (written by workers and listeners directly) into
    // DispatcherEvent broadcasts so SSE consumers see live events.
    let bridge_state = state.clone();
    tokio::spawn(async move {
        weft_dispatcher::journal_bridge::run(bridge_state).await;
    });

    // Infra-event bridge: same pattern for `infra_event` rows
    // written by per-tenant supervisor pods (flaky / recovered /
    // failed / etc.).
    let infra_bridge_state = state.clone();
    tokio::spawn(async move {
        weft_dispatcher::infra_event_bridge::run(infra_bridge_state).await;
    });

    // Reapers: sweep stale worker_pod rows and retain-old terminal
    // task rows. Every dispatcher Pod runs them; FOR UPDATE SKIP
    // LOCKED + idempotent ops keep them safe under concurrency.
    weft_dispatcher::reaper::spawn_all(state.clone());

    // Dispatcher-owned lifecycle commands (deactivate / reactivate).
    // Claimed via SKIP LOCKED so multiple dispatcher Pods coexist;
    // one Pod takes a given row, runs the deactivate or activate,
    // stamps complete.
    weft_dispatcher::lifecycle_claimer::spawn(state.clone());

    // Task picker loop. Each dispatcher Pod runs one and competes
    // for tasks via SKIP LOCKED.
    use weft_task_store::TaskKind;
    let registry = weft_dispatcher::task_executor::TaskRegistry::builder()
        .register(
            TaskKind::SpawnPod,
            Arc::new(weft_dispatcher::task_kinds::SpawnPodExecutor),
        )
        .register(
            TaskKind::RegisterSignal,
            Arc::new(weft_dispatcher::task_kinds::RegisterSignalExecutor),
        )
        .register(
            TaskKind::RouteEntry,
            Arc::new(weft_dispatcher::task_kinds::RouteEntryExecutor),
        )
        .register(
            TaskKind::FireSignal,
            Arc::new(weft_dispatcher::task_kinds::FireSignalExecutor),
        )
        .register(
            TaskKind::RecordCost,
            Arc::new(weft_dispatcher::task_kinds::RecordCostExecutor),
        )
        .register(
            TaskKind::RecordLog,
            Arc::new(weft_dispatcher::task_kinds::RecordLogExecutor),
        )
        .build();
    let picker_state = state.clone();
    let picker_store: Arc<dyn weft_task_store::TaskStoreClient> = Arc::new(
        weft_task_store::PostgresTaskStoreClient::new(state.pg_pool.clone()),
    );
    let picker_pod = state.pod_id.as_str().to_string();
    tokio::spawn(async move {
        weft_dispatcher::task_executor::run_picker_loop(
            picker_store,
            picker_state,
            registry,
            picker_pod,
        )
        .await;
    });

    // Cold-start trigger: if pending worker tasks exist for a project
    // with no live Pod, enqueue a `spawn_pod` task. Dedup-keyed so
    // concurrent dispatchers converge on one spawn per project.
    weft_dispatcher::cold_start::spawn(state.clone());

    // Worker-pod GC (terminal-object cleanup) is a reaper sweep
    // (`reaper::spawn`), driven off the `worker_pod` table (see
    // `sweep_terminal_worker_pods`). No separate kubectl-get sweep.

    let app = router(state);
    let addr = format!("0.0.0.0:{}", http_port);
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    info!("weft-dispatcher listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(graceful_shutdown())
        .await?;
    Ok(())
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

        let tenant_rows: Result<Vec<(String,)>, _> = sqlx::query_as(
            "SELECT tenant_id FROM tenant_listener WHERE owner_pod_id = $1",
        )
        .bind(&pid)
        .fetch_all(&pool)
        .await;
        if let Ok(rows) = tenant_rows {
            for (tenant_id,) in rows {
                if let Err(e) = lease::renew_tenant_listener(&pool, &tenant_id, &pid).await {
                    tracing::warn!(
                        target: "weft_dispatcher",
                        tenant = %tenant_id,
                        error = %e,
                        "tenant_listener renewal failed"
                    );
                }
            }
        }
    }
}

/// Wait for SIGTERM / Ctrl+C and let axum drain. Tenant listener
/// rows stay so the next dispatcher Pod re-attaches with the same
/// URL + tokens; the listener Pod itself doesn't care which
/// dispatcher Pod owns its lease.
async fn graceful_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("install SIGTERM handler failed: {e}");
            return;
        }
    };
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("SIGTERM received; draining"),
        _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl+C received; draining"),
    }
}

/// Whether `url` resolves to a loopback address from the dispatcher
/// pod's perspective. Catches both `http://localhost:9999` (the
/// hostname literal) and `http://127.0.0.1` / `http://[::1]` (loopback
/// IPs). Anything that fails to parse as a URL also counts as
/// loopback, since a malformed prod URL is a bug we want to surface.
fn is_loopback_url(raw: &str) -> bool {
    let Ok(parsed) = url::Url::parse(raw) else {
        return true;
    };
    match parsed.host() {
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => true,
    }
}

#[cfg(test)]
mod is_loopback_url_tests {
    use super::is_loopback_url;

    #[test]
    fn localhost_hostname() {
        assert!(is_loopback_url("http://localhost:9999"));
        assert!(is_loopback_url("https://LOCALHOST/path"));
    }

    #[test]
    fn loopback_v4() {
        assert!(is_loopback_url("http://127.0.0.1:9999"));
        assert!(is_loopback_url("http://127.0.0.42"));
    }

    #[test]
    fn loopback_v6() {
        assert!(is_loopback_url("http://[::1]:9999"));
    }

    #[test]
    fn external_host() {
        assert!(!is_loopback_url("https://api.example.com"));
        assert!(!is_loopback_url("http://10.0.0.5:9999"));
    }

    #[test]
    fn malformed_url_is_loopback() {
        assert!(is_loopback_url("not a url"));
        assert!(is_loopback_url(""));
    }
}

