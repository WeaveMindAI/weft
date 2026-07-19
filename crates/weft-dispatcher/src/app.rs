//! The dispatcher application: its boot building blocks and the default boot.
//!
//! The dispatcher boot is a sequence of plain building blocks (run the schema,
//! build the state, register the task executors, spawn the background loops,
//! build the router, serve). `run` wires them with the built-in values. Each
//! building block is public and takes its policies as plain construction input,
//! so the boot can be assembled with different values without this module
//! carrying any knowledge of a particular caller.

use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tracing::info;

use crate::authenticator::local_authenticator;
use crate::backend::{K8sWorkerBackend, WorkerBackend};
use crate::placement::{
    default_reclaimer, local_placement_policy, no_sandbox, ProjectReclaimer, SandboxPolicy,
};
use crate::journal::postgres::PostgresJournal;
use crate::listener::{
    K8sListenerBackend, ListenerBackend, ListenerPool, SubprocessListenerBackend,
};
use crate::supervisor_pool::{
    K8sSupervisorBackend, SubprocessSupervisorBackend, SupervisorBackend, SupervisorPool,
};
use crate::tenant;
use crate::DispatcherState;

/// Read the object-store slot from env (the bucket endpoint + bucket + creds).
/// Re-exported so a composed dispatcher service can build the object-store
/// handle its own components need from the same env the bare service would,
/// without depending on the platform crate directly.
pub use weft_platform_traits::object_store_from_env;

/// Run the core dispatcher schema migrations against `pool`. Every table the
/// dispatcher itself owns is created here, in dependency order (the `project`
/// table last among the FK-able ones so later tables can reference it).
///
/// The whole run holds the cluster-wide schema advisory lock: `IF NOT EXISTS`
/// makes re-runs idempotent but is NOT concurrency-safe in Postgres (two
/// backends racing the same CREATE on a fresh DB both pass the existence check,
/// then one fails on a duplicate catalog key). Under the lock, replicas
/// serialize and the losers see the tables already present.
pub async fn run_core_migrations(pool: &sqlx::PgPool) -> anyhow::Result<crate::ProjectStore> {
    crate::lease::with_advisory_lock_blocking(pool, crate::lease::advisory_key("migrate", "schema"), || async {
        run_core_migrations_locked(pool).await
    })
    .await
}

async fn run_core_migrations_locked(pool: &sqlx::PgPool) -> anyhow::Result<crate::ProjectStore> {
    crate::listener::migrate(pool)
        .await
        .context("apply listener_pod migrations")?;
    crate::supervisor_pool::migrate(pool)
        .await
        .context("apply supervisor_pod + infra_owner migrations")?;
    crate::namespace_registry::migrate(pool)
        .await
        .context("apply namespace_registry migrations")?;
    weft_task_store::migrate(pool)
        .await
        .context("apply task-store migrations")?;
    crate::infra_node::migrate(pool)
        .await
        .context("apply infra_node migrations")?;
    crate::infra_event::migrate(pool)
        .await
        .context("apply infra_event migrations")?;
    crate::infra_lifecycle_command::migrate(pool)
        .await
        .context("apply infra_lifecycle_command migrations")?;
    crate::journal_bridge::migrate(pool)
        .await
        .context("apply journal_bridge cursor migrations")?;
    crate::infra_event_bridge::migrate(pool)
        .await
        .context("apply infra_event_bridge cursor migrations")?;
    // The durable terminate-sweep queue (no FK; the dispatcher owns it and the
    // reaper drains it by asking the broker to sweep a terminated color's files).
    crate::storage::migrate(pool)
        .await
        .context("apply storage_sweep migrations")?;
    let projects: crate::ProjectStore = std::sync::Arc::new(
        crate::PostgresProjectStore::new(pool.clone())
            .await
            .context("init project store")?,
    );
    Ok(projects)
}

/// The construction-time policies threaded into `build_state`: who a request
/// authenticates as, which tenant owns a project, where a worker lands, whether
/// a pod is sandboxed, how a deleted project's data is reclaimed, and an optional
/// on-demand project builder. `Defaults::local` fills them with the built-in
/// values. This is plain construction input, not an injection bundle: there are no
/// builders and nothing here is overridden later.
pub struct Defaults {
    pub authenticator: Arc<dyn crate::authenticator::Authenticator>,
    pub tenant_router: Arc<dyn crate::tenant::TenantRouter>,
    pub placement: Arc<dyn crate::placement::PlacementPolicy>,
    pub sandbox: Arc<dyn SandboxPolicy>,
    /// Frees a deleted project's stored data before its row is dropped. The
    /// default (`WipeProjectFiles`) frees the project's `project/`-scoped runtime
    /// files. Canonical doc on the `ProjectReclaimer` trait in `placement.rs`.
    pub project_reclaimer: Arc<dyn ProjectReclaimer>,
    /// Builds a project's latest saved source on demand so a verb can be clicked on
    /// a not-yet-built project and it builds first. `None` when there is no builder
    /// (a runnable definition is already registered before the verb), in which case
    /// the verb path never builds. The impl carries whatever it needs to build (a
    /// builder, a bucket, a registry); the dispatcher holds none of those.
    pub ensure_built: Option<Arc<dyn crate::backend::ProjectBuilder>>,
}

impl Defaults {
    /// The standalone defaults: one tenant `local`, every request authenticated
    /// as it, the structural worker placement, no sandbox, no on-demand builder.
    /// The standalone dispatcher runs on exactly these.
    pub fn local() -> Self {
        Self {
            authenticator: local_authenticator(),
            tenant_router: tenant::local_router(),
            placement: local_placement_policy(),
            sandbox: no_sandbox(),
            project_reclaimer: default_reclaimer(),
            ensure_built: None,
        }
    }
}

/// Build the dispatcher state: connect Postgres, run the core migrations, wire
/// the env-driven backends (kube, listener/supervisor/worker), resolve the
/// cluster knobs, and assemble `DispatcherState` from `defaults` plus the shared
/// env-derived infra. Everything env-driven here is identical across
/// runs; only the `defaults` values vary.
pub async fn build_state(http_port: u16, defaults: Defaults) -> anyhow::Result<DispatcherState> {
    let Defaults {
        authenticator,
        tenant_router,
        placement,
        sandbox,
        project_reclaimer,
        ensure_built,
    } = defaults;

    let database_url = std::env::var("WEFT_DATABASE_URL")
        .context("WEFT_DATABASE_URL is required (postgres://user:pass@host:port/db)")?;
    let journal = PostgresJournal::connect(&database_url)
        .await
        .with_context(|| format!("connect journal at {database_url}"))?;
    let projects = run_core_migrations(journal.pool()).await?;

    // Broker URL: every tenant pod (listener / worker / infra) talks to the
    // broker instead of touching Postgres. Required in-cluster; subprocess dev
    // wires it through env.
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

    // Worker-image registry: present iff worker images are pulled from
    // a registry it pushed to. Read from env. `None` when worker images are built
    // and loaded onto the node directly (the bare content-addressed tag is pulled
    // `IfNotPresent`), so the worker spawn resolves the local tag.
    let registry_config = crate::registry::RegistryConfig::from_env()?;

    let worker_backend: Arc<dyn WorkerBackend> = Arc::new(K8sWorkerBackend::new(
        broker_url.clone(),
        kube.clone(),
        Arc::new(weft_platform_traits::clock::SystemClock),
        sandbox.clone(),
        registry_config.clone(),
    ));

    // Cluster CIDR knobs. Required in-cluster so the dispatcher can render
    // NetworkPolicies that allow internet egress while denying intra-cluster
    // Pod traffic. Subprocess dev defaults are Kind's.
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
    // Sanity-check CIDRs: a typo'd value silently produces broken NetworkPolicies
    // (allows traffic the policy meant to block). Parse the address as a real
    // `Ipv4Addr` (so `999.1.1.1` is rejected, not just "four dot-separated parts")
    // and require the prefix length to be `0..=32`.
    for (name, value) in [
        ("WEFT_CLUSTER_POD_CIDR", &cluster_pod_cidr),
        ("WEFT_CLUSTER_SERVICE_CIDR", &cluster_service_cidr),
    ] {
        let mut parts = value.splitn(2, '/');
        let ip = parts.next().unwrap_or("");
        let prefix = parts.next().unwrap_or("");
        let ip_ok = ip.parse::<std::net::Ipv4Addr>().is_ok();
        let prefix_ok = prefix.parse::<u8>().map(|p| p <= 32).unwrap_or(false);
        if !ip_ok || !prefix_ok {
            anyhow::bail!("{name}='{value}' is not a valid IPv4 CIDR (expected `a.b.c.d/N`, N in 0..=32)");
        }
    }

    let cluster_ingress_namespace = std::env::var("WEFT_CLUSTER_INGRESS_NAMESPACE")
        .unwrap_or_else(|_| "ingress-nginx".to_string());

    // Live caller connection provisioning. The signing secret (hex) is shared
    // with every worker pod (the dispatcher injects it into the spawn spec);
    // empty = live connections disabled (handshake fails loud). The gateway base
    // URL is the public origin callers are pointed at.
    let caller_token_secret: Arc<Vec<u8>> =
        Arc::new(match std::env::var("WEFT_CALLER_TOKEN_SECRET") {
            Ok(hex) if !hex.is_empty() => hex::decode(&hex)
                .map_err(|e| anyhow::anyhow!("WEFT_CALLER_TOKEN_SECRET is not valid hex: {e}"))?,
            _ => Vec::new(),
        });
    let gateway_base_url = std::env::var("WEFT_GATEWAY_BASE_URL").unwrap_or_default();

    let pod_id = crate::state::PodId::from_env();
    info!("dispatcher pod identity: {}", pod_id);

    let pg_pool = journal.pool().clone();
    let event_bus = crate::EventBus::with_notify(pg_pool.clone()).await?;

    // The control-plane namespace: where pooled, trusted, tenant-agnostic
    // services run (the infra-supervisor; pooled listeners). They serve many
    // tenants, so they do not live in any one tenant's namespace. Defaults to
    // the dispatcher's own namespace.
    let control_plane_namespace = std::env::var("WEFT_CONTROL_PLANE_NAMESPACE")
        .unwrap_or_else(|_| "weft-system".to_string());
    let listener_pool = ListenerPool::new(control_plane_namespace.clone());

    // Pooled infra-supervisor backend + pool, mirroring the listener.
    let supervisor_backend: Arc<dyn SupervisorBackend> =
        match std::env::var("WEFT_SUPERVISOR_BACKEND").as_deref() {
            Ok("subprocess") => {
                let bin = std::env::var("WEFT_SUPERVISOR_BIN")
                    .ok()
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| {
                        std::env::current_exe()
                            .ok()
                            .and_then(|p| p.parent().map(|d| d.join("weft-infra-supervisor")))
                            .unwrap_or_else(|| {
                                std::path::PathBuf::from("weft-infra-supervisor")
                            })
                    });
                Arc::new(SubprocessSupervisorBackend::new(bin))
            }
            _ => {
                let image = std::env::var("WEFT_SUPERVISOR_IMAGE")
                    .unwrap_or_else(|_| "weft-infra-supervisor:local".into());
                Arc::new(K8sSupervisorBackend::new(image, broker_url.clone(), kube.clone()))
            }
        };
    let supervisor_pool = SupervisorPool::new(control_plane_namespace.clone());

    let in_cluster = std::env::var("KUBERNETES_SERVICE_HOST").is_ok();
    // Empty counts as unset: the k8s-backend manifest substitutes
    // `WEFT_LOCAL_DEV` to "" (no local-dev), and env-var-set-to-empty reads back
    // as Ok(""), which must NOT enable the loopback bypass.
    let local_dev = std::env::var("WEFT_LOCAL_DEV").map(|v| !v.is_empty()).unwrap_or(false);
    let public_base_url = resolve_public_base_url(
        std::env::var("WEFT_DISPATCHER_PUBLIC_BASE_URL").ok().as_deref(),
        in_cluster,
        local_dev,
        http_port,
    )?;

    // The dispatcher's own projected SA token: signed onto its broker
    // storage-admin requests so the broker resolves the dispatcher to the
    // control plane (the CLI `weft files` proxy).
    let broker_token_path = std::env::var("WEFT_BROKER_TOKEN_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("/var/run/weft/sa/token"));

    Ok(DispatcherState {
        pod_id,
        journal: Arc::new(journal),
        pg_pool,
        workers: worker_backend,
        ensure_built,
        projects,
        events: event_bus,
        listener_backend,
        listeners: listener_pool,
        supervisor_backend,
        supervisors: supervisor_pool,
        authenticator,
        tenant_router,
        placement,
        sandbox,
        project_reclaimer,
        public_base_url,
        cluster_pod_cidr,
        cluster_service_cidr,
        cluster_ingress_namespace,
        control_plane_namespace,
        broker_url,
        broker_token_path,
        http: reqwest::Client::new(),
        kube,
        caller_token_secret,
        gateway_base_url,
    })
}

/// A task-registry builder pre-loaded with the dispatcher's core task executors.
/// Extra task kinds chain `.register_str` on the returned builder before
/// `.build()`.
pub fn core_task_registry_builder() -> crate::task_executor::TaskRegistryBuilder {
    use weft_task_store::TaskKind;
    crate::task_executor::TaskRegistry::builder()
        .register(TaskKind::SpawnPod, Arc::new(crate::task_kinds::SpawnPodExecutor))
        .register(
            TaskKind::RegisterSignal,
            Arc::new(crate::task_kinds::RegisterSignalExecutor),
        )
        .register(TaskKind::RouteEntry, Arc::new(crate::task_kinds::RouteEntryExecutor))
        .register(TaskKind::FireSignal, Arc::new(crate::task_kinds::FireSignalExecutor))
        .register(TaskKind::RecordCost, Arc::new(crate::task_kinds::RecordCostExecutor))
        .register(TaskKind::RecordLog, Arc::new(crate::task_kinds::RecordLogExecutor))
}

/// Spawn a core background loop under supervision: a core loop is an infinite
/// `loop { ... }` that is only ever meant to run for the pod's whole life, so the
/// wrapped future NEVER completing is the normal case. If it DOES complete, that
/// means the loop unwound (a panic inside the task, an unwrap on a malformed row,
/// an unexpected early return): a partial pod death where one function is silently
/// dead while the pod keeps serving. A dispatcher pod is stateless cattle and its
/// coordination is rebuildable from Postgres by any sibling, so the honest recovery
/// is to crash the whole pod and let Kubernetes restart it onto a clean slate,
/// never to limp on with a missing lease-renewer / picker / bridge.
///
/// `tokio::spawn`'s JoinHandle surfaces a panic as an `Err(JoinError)` on await; a
/// clean return surfaces as `Ok(())`. Either way the loop is no longer running, so
/// both are fatal. We log loudly and `exit(1)`.
pub(crate) fn spawn_supervised<Fut>(name: &'static str, fut: Fut)
where
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let joined = tokio::spawn(fut).await;
        match joined {
            Ok(()) => tracing::error!(
                target: "weft_dispatcher",
                loop_name = name,
                "core background loop exited unexpectedly (a core loop must run for the pod's \
                 whole life); crashing the pod so Kubernetes restarts it"
            ),
            Err(e) => tracing::error!(
                target: "weft_dispatcher",
                loop_name = name,
                error = %e,
                "core background loop PANICKED; crashing the pod so Kubernetes restarts it"
            ),
        }
        std::process::exit(1);
    });
}

/// Spawn the dispatcher's core background loops against `state`: lease renewal,
/// the journal + infra-event bridges, the reapers, the lifecycle claimer, the
/// task picker (over `registry`), and the cold-start trigger. Callers that
/// add their own loops spawn them after this returns. Every loop runs under
/// [`spawn_supervised`], so a panic in any of them crashes the pod (Kubernetes
/// restarts it) rather than silently killing that one function.
pub fn spawn_core_loops(state: DispatcherState, registry: crate::task_executor::TaskRegistry) {
    let renewer_state = state.clone();
    spawn_supervised("lease_renewer", async move {
        lease_renewer(renewer_state).await;
    });

    // Journal-to-EventBus bridge: convert new exec_event rows (written by
    // workers and listeners directly) into DispatcherEvent broadcasts so SSE
    // consumers see live events.
    let bridge_state = state.clone();
    spawn_supervised("journal_bridge", async move {
        crate::journal_bridge::run(bridge_state).await;
    });

    // Infra-event bridge: same pattern for `infra_event` rows written by pooled
    // supervisor pods (flaky / recovered / failed / etc.).
    let infra_bridge_state = state.clone();
    spawn_supervised("infra_event_bridge", async move {
        crate::infra_event_bridge::run(infra_bridge_state).await;
    });

    // Reapers: sweep stale worker_pod rows and retain-old terminal task rows.
    // Every dispatcher Pod runs them; FOR UPDATE SKIP LOCKED + idempotent ops
    // keep them safe under concurrency.
    crate::reaper::spawn_all(state.clone());

    // Dispatcher-owned lifecycle commands (deactivate / reactivate). Claimed via
    // SKIP LOCKED so multiple dispatcher Pods coexist.
    crate::lifecycle_claimer::spawn(state.clone());

    // Task picker loop. Each dispatcher Pod runs one and competes for tasks via
    // SKIP LOCKED.
    let picker_state = state.clone();
    let picker_store: Arc<dyn weft_task_store::TaskStoreClient> =
        Arc::new(weft_task_store::PostgresTaskStoreClient::new(state.pg_pool.clone()));
    let picker_pod = state.pod_id.as_str().to_string();
    spawn_supervised("task_picker", async move {
        crate::task_executor::run_picker_loop(picker_store, picker_state, registry, picker_pod)
            .await;
    });

    // Cold-start trigger: if pending worker tasks exist for a project with no
    // live Pod, enqueue a `spawn_pod` task. Dedup-keyed so concurrent
    // dispatchers converge on one spawn per project.
    crate::cold_start::spawn(state);
}

/// Bind `0.0.0.0:http_port` and serve `app` until graceful shutdown.
pub async fn serve(app: axum::Router, http_port: u16) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", http_port);
    let listener = TcpListener::bind(&addr).await.with_context(|| format!("bind {addr}"))?;
    info!("weft-dispatcher listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(graceful_shutdown())
        .await?;
    Ok(())
}

/// Boot the dispatcher: build the state with the built-in defaults, register the
/// core task executors, spawn the core loops, build the core router, and serve.
pub async fn run(http_port: u16) -> anyhow::Result<()> {
    let state = build_state(http_port, Defaults::local()).await?;
    let registry = core_task_registry_builder().build();
    spawn_core_loops(state.clone(), registry);
    let app = crate::api::router(state);
    serve(app, http_port).await
}

/// Background loop that renews every lease this Pod owns (pooled listener and
/// supervisor pod registry leases) so a sibling does not adopt a live pod. A
/// failure is not recovered here; the next sweep adopts an expired lease.
async fn lease_renewer(state: DispatcherState) {
    use crate::lease;
    let interval = std::time::Duration::from_secs(lease::LEASE_RENEW_INTERVAL_SECS);
    let pod_id = state.pod_id.as_str().to_string();
    loop {
        tokio::time::sleep(interval).await;
        let pool = state.pg_pool.clone();
        let pid = pod_id.clone();

        if let Err(e) = state.listeners.renew_owned(&pool, &pid).await {
            tracing::warn!(
                target: "weft_dispatcher",
                error = %e,
                "listener_pod lease renewal failed"
            );
        }
        // Same for pooled supervisor pods (the `supervisor_pod` registry lease,
        // distinct from the per-project `infra_owner` lease the supervisor
        // renews itself).
        if let Err(e) = state.supervisors.renew_owned(&pool, &pid).await {
            tracing::warn!(
                target: "weft_dispatcher",
                error = %e,
                "supervisor_pod lease renewal failed"
            );
        }
    }
}

/// Wait for SIGTERM / Ctrl+C and let axum drain. Tenant listener rows stay so
/// the next dispatcher Pod re-attaches with the same URL + tokens; the listener
/// Pod itself doesn't care which dispatcher Pod owns its lease.
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

/// Whether `url` resolves to a loopback address from the dispatcher pod's
/// perspective. Catches both `http://localhost:9999` (the hostname literal) and
/// `http://127.0.0.1` / `http://[::1]` (loopback IPs). Anything that fails to
/// parse as a URL also counts as loopback, since a malformed prod URL is a bug
/// we want to surface.
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

/// Resolve the public base URL users hit for webhooks / activation URLs AND
/// storage file downloads (`<base>/storage/<tenant>/...`).
///
/// In a real cluster the URL must be the external ingress host; a loopback
/// there is a deploy bug (no client could reach it), so we fail loud. Local-dev
/// kind is the exception: the dispatcher still runs as a Pod (so `in_cluster` is
/// true), but the operator's machine reaches the cluster ingress via a
/// port-forward to `127.0.0.1:<port>`, so a loopback public URL is exactly
/// correct. `KUBERNETES_SERVICE_HOST` cannot tell "real-cluster Pod" from
/// "local kind Pod"; only the operator's intent (`WEFT_LOCAL_DEV`) can, which
/// makes loopback legal without weakening the strict check. Pure so the
/// security-sensitive branching is unit-tested below.
pub(crate) fn resolve_public_base_url(
    env_value: Option<&str>,
    in_cluster: bool,
    local_dev: bool,
    http_port: u16,
) -> anyhow::Result<String> {
    let strict = in_cluster && !local_dev;
    match env_value {
        Some(v) => {
            if strict && is_loopback_url(v) {
                anyhow::bail!(
                    "WEFT_DISPATCHER_PUBLIC_BASE_URL='{v}' resolves to a loopback \
                     host in-cluster; set it on the dispatcher Deployment to the \
                     external ingress URL before deploying (or set WEFT_LOCAL_DEV=1 \
                     if this really is a local kind cluster reached via port-forward)"
                );
            }
            Ok(v.to_string())
        }
        None => {
            if strict {
                anyhow::bail!(
                    "WEFT_DISPATCHER_PUBLIC_BASE_URL is required in-cluster; \
                     set it on the dispatcher Deployment to the external ingress URL"
                );
            }
            Ok(format!("http://localhost:{http_port}"))
        }
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

#[cfg(test)]
mod resolve_public_base_url_tests {
    use super::resolve_public_base_url;

    // The strict path (in_cluster, not local_dev): a public base URL is required
    // and must be externally reachable.
    #[test]
    fn strict_rejects_loopback() {
        assert!(resolve_public_base_url(Some("http://127.0.0.1:9998"), true, false, 9999).is_err());
        assert!(resolve_public_base_url(Some("http://localhost:9999"), true, false, 9999).is_err());
    }

    #[test]
    fn strict_rejects_missing() {
        assert!(resolve_public_base_url(None, true, false, 9999).is_err());
    }

    #[test]
    fn strict_accepts_external_host() {
        assert_eq!(
            resolve_public_base_url(Some("https://files.example.com"), true, false, 9999).unwrap(),
            "https://files.example.com"
        );
    }

    // Local-dev kind (in_cluster AND local_dev): loopback is correct, reached
    // via the daemon's ingress port-forward.
    #[test]
    fn local_dev_accepts_loopback() {
        assert_eq!(
            resolve_public_base_url(Some("http://127.0.0.1:9998"), true, true, 9999).unwrap(),
            "http://127.0.0.1:9998"
        );
    }

    // Outside the cluster (CLI-launched local process): default to localhost on
    // the dispatcher's own port.
    #[test]
    fn out_of_cluster_defaults_to_localhost() {
        assert_eq!(
            resolve_public_base_url(None, false, false, 9999).unwrap(),
            "http://localhost:9999"
        );
    }
}
