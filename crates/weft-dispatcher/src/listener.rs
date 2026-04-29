//! Listener-backend abstraction. The dispatcher spawns a per-tenant
//! listener on demand; the listener owns all wake signal routing
//! for every project belonging to that tenant. Two backends exist:
//!
//!   - `SubprocessListenerBackend`: spawns the `weft-listener` binary
//!     as a local subprocess. Used for local development.
//!   - `K8sListenerBackend`: spawns a Deployment + Service + Ingress
//!     in the tenant's k8s namespace so the listener lives alongside
//!     workers and infra sidecars.
//!
//! Only the backend knows how to start a listener; the dispatcher
//! holds `ListenerHandle`s (URL + admin token) and POSTs to
//! `/register`, `/unregister` on them.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::Value;
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use uuid::Uuid;
use weft_core::primitive::WakeSignalSpec;

use crate::tenant::TenantId;

/// Handle to a running listener. Held by the dispatcher for each
/// active project so it can register / unregister signals.
#[derive(Debug, Clone)]
pub struct ListenerHandle {
    /// Base URL where the listener is reachable from the dispatcher
    /// (http://127.0.0.1:PORT locally).
    pub admin_url: String,
    /// Base URL the listener mints user-facing signal URLs with.
    /// Equal to `admin_url` locally; in cloud it will be the
    /// public ingress URL (`https://triggers.weavemind.app/ns-X/`
    /// or similar).
    pub public_base_url: String,
    /// Bearer token both sides share. Dispatcher sends it on
    /// register/unregister; listener verifies.
    pub admin_token: String,
    /// Bearer token the listener sends when relaying fires.
    /// Dispatcher verifies on `/signal-fired`.
    pub relay_token: String,
}

#[async_trait]
pub trait ListenerBackend: Send + Sync {
    /// Spawn a listener instance for `tenant` in `namespace`.
    /// Returns the handle to register signals against. Idempotent
    /// at the orchestrator level: callers should serialize spawn
    /// calls via `ListenerPool::ensure`.
    async fn spawn(
        &self,
        tenant: &TenantId,
        namespace: &str,
        dispatcher_url: &str,
    ) -> Result<ListenerHandle>;

    /// Kill the listener for `tenant` in `namespace`. Called when
    /// the listener self-reports its registry is empty.
    async fn stop(&self, tenant: &TenantId, namespace: &str) -> Result<()>;
}

/// Local-development backend: fork the `weft-listener` binary as
/// a child process. Listener binds an OS-assigned port; backend
/// pre-allocates one (simpler, dev-only pattern).
pub struct SubprocessListenerBackend {
    /// Absolute path to the `weft-listener` binary.
    binary_path: PathBuf,
    /// Running children keyed by tenant id. Dropping the `Child`
    /// doesn't kill it automatically (tokio::Child leaks); we call
    /// `.kill()` explicitly on stop.
    children: Arc<DashMap<String, Arc<Mutex<Child>>>>,
}

impl SubprocessListenerBackend {
    pub fn new(binary_path: PathBuf) -> Self {
        Self {
            binary_path,
            children: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl ListenerBackend for SubprocessListenerBackend {
    async fn spawn(
        &self,
        tenant: &TenantId,
        _namespace: &str,
        dispatcher_url: &str,
    ) -> Result<ListenerHandle> {
        let port = pick_free_port()?;
        let admin_url = format!("http://127.0.0.1:{port}");
        let public_base_url = admin_url.clone();
        let admin_token = Uuid::new_v4().to_string();
        let relay_token = Uuid::new_v4().to_string();

        let mut cmd = Command::new(&self.binary_path);
        cmd.env("WEFT_LISTENER_TENANT_ID", tenant.as_str())
            .env("WEFT_LISTENER_PORT", port.to_string())
            .env("WEFT_LISTENER_PUBLIC_BASE_URL", &public_base_url)
            .env("WEFT_LISTENER_DISPATCHER_URL", dispatcher_url)
            .env("WEFT_LISTENER_ADMIN_TOKEN", &admin_token)
            .env("WEFT_LISTENER_RELAY_TOKEN", &relay_token)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        let child = cmd
            .spawn()
            .with_context(|| format!("spawn listener for tenant {tenant}"))?;
        self.children
            .insert(tenant.to_string(), Arc::new(Mutex::new(child)));

        wait_for_health(&admin_url).await?;

        Ok(ListenerHandle {
            admin_url,
            public_base_url,
            admin_token,
            relay_token,
        })
    }

    async fn stop(&self, tenant: &TenantId, _namespace: &str) -> Result<()> {
        if let Some((_, child)) = self.children.remove(tenant.as_str()) {
            let mut c = child.lock().await;
            let _ = c.kill().await;
        }
        Ok(())
    }
}

/// K8s listener backend. The dispatcher Pod applies a Deployment +
/// Service + Ingress per tenant into the tenant's namespace via
/// kubectl, then resolves URLs using cluster DNS (for internal
/// calls) and ingress hostnames (for user-facing webhook/form
/// URLs).
///
/// Same manifest pattern as `KindInfraBackend`. Listener image tag
/// comes from the `WEFT_LISTENER_IMAGE` env var on the dispatcher
/// Pod so the CLI controls which version runs.
pub struct K8sListenerBackend {
    listener_image: String,
    /// External hostname suffix for ingress. Webhook URLs end up
    /// as `http://<short>.{ingress_host_suffix}`. Defaults to
    /// `listener.weft.local` for kind; cloud deploy sets the real
    /// public suffix.
    ingress_host_suffix: String,
    /// Tracks the deployment name + namespace per spawned tenant
    /// so `stop` can address the right namespace on cleanup.
    deployments: Arc<DashMap<String, (String, String)>>,
}

impl K8sListenerBackend {
    pub fn new(listener_image: String, ingress_host_suffix: String) -> Self {
        Self {
            listener_image,
            ingress_host_suffix,
            deployments: Arc::new(DashMap::new()),
        }
    }
}

#[async_trait]
impl ListenerBackend for K8sListenerBackend {
    async fn spawn(
        &self,
        tenant: &TenantId,
        namespace: &str,
        dispatcher_url: &str,
    ) -> Result<ListenerHandle> {
        let short = short_id(tenant.as_str());
        let deploy_name = format!("listener-{short}");
        let admin_token = Uuid::new_v4().to_string();
        let relay_token = Uuid::new_v4().to_string();
        let public_host = format!("{short}.{}", self.ingress_host_suffix);
        let public_base_url = format!("http://{public_host}");
        let admin_url = format!(
            "http://{deploy_name}.{namespace}.svc.cluster.local:8080",
        );

        let manifest = render_listener_manifest(
            &deploy_name,
            namespace,
            &self.listener_image,
            tenant.as_str(),
            dispatcher_url,
            &public_base_url,
            &admin_token,
            &relay_token,
            &public_host,
        );
        kubectl_apply_manifest(&manifest).await?;
        kubectl_rollout_status(namespace, &deploy_name).await?;
        wait_for_health(&admin_url).await?;

        self.deployments.insert(
            tenant.to_string(),
            (deploy_name, namespace.to_string()),
        );
        Ok(ListenerHandle {
            admin_url,
            public_base_url,
            admin_token,
            relay_token,
        })
    }

    async fn stop(&self, tenant: &TenantId, namespace: &str) -> Result<()> {
        // Prefer the cache (it has the exact deploy name we used);
        // fall back to the deterministic name derived from tenant_id
        // for cross-Pod kill where we have no cache.
        let (deploy_name, ns) = self
            .deployments
            .remove(tenant.as_str())
            .map(|(_, v)| v)
            .unwrap_or_else(|| {
                (
                    format!("listener-{}", short_id(tenant.as_str())),
                    namespace.to_string(),
                )
            });
        for kind in ["ingress", "service", "deployment"] {
            let _ = Command::new("kubectl")
                .args([
                    "-n", &ns, "delete", kind, &deploy_name,
                    "--ignore-not-found", "--wait=false",
                ])
                .status()
                .await;
        }
        Ok(())
    }
}

/// Deterministic listener Deployment name for a tenant. Used both
/// at spawn time (`K8sListenerBackend::spawn` derives the same
/// string) and at persistence time (`ListenerPool::ensure` records
/// it) so cross-Pod re-attach + cross-Pod stop both work without a
/// shared cache.
pub fn deploy_name_for_tenant(tenant_id: &str) -> String {
    format!("listener-{}", short_id(tenant_id))
}

fn short_id(project_id: &str) -> String {
    // First 8 hex chars of the project uuid, safe for DNS labels.
    project_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>()
        .to_lowercase()
}

#[allow(clippy::too_many_arguments)]
fn render_listener_manifest(
    name: &str,
    namespace: &str,
    image: &str,
    tenant_id: &str,
    dispatcher_url: &str,
    public_base_url: &str,
    admin_token: &str,
    relay_token: &str,
    public_host: &str,
) -> String {
    format!(
        r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app: {name}
    weft.dev/role: listener
    weft.dev/tenant: "{tenant_id}"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
        weft.dev/role: listener
        weft.dev/tenant: "{tenant_id}"
    spec:
      containers:
        - name: listener
          image: {image}
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
          env:
            - name: WEFT_LISTENER_TENANT_ID
              value: "{tenant_id}"
            - name: WEFT_LISTENER_PORT
              value: "8080"
            - name: WEFT_LISTENER_PUBLIC_BASE_URL
              value: "{public_base_url}"
            - name: WEFT_LISTENER_DISPATCHER_URL
              value: "{dispatcher_url}"
            - name: WEFT_LISTENER_ADMIN_TOKEN
              value: "{admin_token}"
            - name: WEFT_LISTENER_RELAY_TOKEN
              value: "{relay_token}"
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 1
            periodSeconds: 2
---
apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {namespace}
spec:
  selector:
    app: {name}
  ports:
    - port: 8080
      targetPort: 8080
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {name}
  namespace: {namespace}
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
spec:
  ingressClassName: nginx
  rules:
    - host: {public_host}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: {name}
                port:
                  number: 8080
"#,
    )
}

async fn kubectl_apply_manifest(manifest: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn kubectl apply")?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(manifest.as_bytes()).await?;
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("kubectl apply failed: {stderr}");
    }
    Ok(())
}

async fn kubectl_rollout_status(namespace: &str, deployment: &str) -> Result<()> {
    let status = Command::new("kubectl")
        .args([
            "-n", namespace,
            "rollout", "status", &format!("deployment/{deployment}"),
            "--timeout=120s",
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("{deployment} did not reach Ready within 120s");
    }
    Ok(())
}

fn pick_free_port() -> Result<u16> {
    let s = std::net::TcpListener::bind("127.0.0.1:0")
        .context("bind ephemeral port")?;
    let port = s.local_addr()?.port();
    drop(s);
    Ok(port)
}

async fn wait_for_health(admin_url: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let health = format!("{}/health", admin_url.trim_end_matches('/'));
    for _ in 0..50 {
        if client.get(&health).send().await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    anyhow::bail!("listener at {admin_url} did not become healthy in time")
}

/// Client helper: call a listener's `/register` endpoint. Returns
/// the user-facing URL (if any) the listener minted.
pub async fn register_signal(
    handle: &ListenerHandle,
    token: &str,
    spec: &WakeSignalSpec,
    node_id: &str,
) -> Result<Option<String>> {
    let client = reqwest::Client::new();
    let url = format!("{}/register", handle.admin_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .bearer_auth(&handle.admin_token)
        .json(&serde_json::json!({
            "token": token,
            "spec": spec,
            "node_id": node_id,
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("listener /register returned {}: {}", resp.status(), resp.text().await.unwrap_or_default());
    }
    let body: Value = resp.json().await?;
    Ok(body
        .get("user_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string()))
}

/// Unregister a signal. Ignores errors — deactivate still proceeds
/// even if the listener is already dead.
pub async fn unregister_signal(handle: &ListenerHandle, token: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/unregister", handle.admin_url.trim_end_matches('/'));
    let _ = client
        .post(&url)
        .bearer_auth(&handle.admin_token)
        .json(&serde_json::json!({"token": token}))
        .send()
        .await;
    Ok(())
}

/// In-memory map of per-tenant listener handles. One listener
/// instance multiplexes every project belonging to a tenant.
/// Lives on DispatcherState.
///
/// `ensure` is the canonical entry point: it spawns the tenant
/// listener if one isn't up, and is safe under concurrent calls
/// (a per-tenant Mutex serializes the get-or-spawn check).
#[derive(Default, Clone)]
pub struct ListenerPool {
    inner: Arc<DashMap<String, ListenerHandle>>,
    /// Per-tenant spawn locks. We never remove entries from this
    /// map: the lock is cheap to keep around and removing it would
    /// race with a concurrent `ensure` that already grabbed an Arc.
    spawn_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl ListenerPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, tenant_id: &str) -> Option<ListenerHandle> {
        self.inner.get(tenant_id).map(|h| h.clone())
    }

    /// Snapshot of every (tenant_id, listener handle) this Pod
    /// owns. Used by the diagnostic `/listener/inspect` endpoint
    /// and by the periodic empty-listener sweeper.
    pub fn list(&self) -> Vec<(String, ListenerHandle)> {
        self.inner
            .iter()
            .map(|h| (h.key().clone(), h.value().clone()))
            .collect()
    }

    /// Insert a handle. Used by tests and by `ensure` after a
    /// successful spawn. Production code should call `ensure`.
    pub fn insert(&self, tenant_id: String, handle: ListenerHandle) {
        self.inner.insert(tenant_id, handle);
    }

    /// Drop the entry. Called after the backend successfully
    /// stopped the listener.
    pub fn remove(&self, tenant_id: &str) -> Option<ListenerHandle> {
        self.inner.remove(tenant_id).map(|(_, v)| v)
    }

    /// Get-or-spawn a listener for `tenant`. Idempotent under
    /// concurrent calls AND across Pods: if another Pod already
    /// has a live listener for this tenant, we re-attach to it via
    /// the persisted handle in `tenant_listener`. We don't take
    /// ownership of someone else's listener; we just point at it.
    pub async fn ensure(
        &self,
        tenant: &TenantId,
        namespace: &str,
        dispatcher_url: &str,
        backend: &dyn ListenerBackend,
        pg_pool: &sqlx::PgPool,
        deploy_name: &str,
        pod_id: &str,
    ) -> Result<ListenerHandle> {
        if let Some(h) = self.get(tenant.as_str()) {
            return Ok(h);
        }
        let lock = self
            .spawn_locks
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if let Some(h) = self.get(tenant.as_str()) {
            return Ok(h);
        }
        // Cross-Pod re-attach: if another Pod has already spawned
        // a listener for this tenant, use its persisted handle.
        if let Some(persisted) =
            crate::lease::lookup_tenant_listener(pg_pool, tenant.as_str()).await?
        {
            if crate::lease::is_lease_live(persisted.leased_until_unix) {
                let handle = ListenerHandle {
                    admin_url: persisted.admin_url,
                    public_base_url: persisted.public_base_url,
                    admin_token: persisted.admin_token,
                    relay_token: persisted.relay_token,
                };
                self.insert(tenant.to_string(), handle.clone());
                return Ok(handle);
            }
            // Lease expired: the previous owner died. We will
            // re-spawn (or re-attach to the orphan Deployment if
            // it's still up; the K8sListenerBackend's `spawn` is
            // idempotent on its kubectl apply).
        }
        let handle = backend.spawn(tenant, namespace, dispatcher_url).await?;
        self.insert(tenant.to_string(), handle.clone());
        // Persist the handle so other Pods can re-attach and so we
        // can survive a dispatcher restart without losing the URL.
        crate::lease::upsert_tenant_listener(
            pg_pool,
            tenant.as_str(),
            pod_id,
            namespace,
            deploy_name,
            &handle.admin_url,
            &handle.public_base_url,
            &handle.admin_token,
            &handle.relay_token,
        )
        .await?;
        Ok(handle)
    }

    /// Kill the listener for `tenant`. Tells the backend to stop,
    /// removes the local entry, deletes the persisted row.
    pub async fn kill(
        &self,
        tenant: &TenantId,
        namespace: &str,
        backend: &dyn ListenerBackend,
        pg_pool: &sqlx::PgPool,
    ) -> Result<()> {
        let lock = self
            .spawn_locks
            .entry(tenant.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if self.inner.remove(tenant.as_str()).is_some() {
            backend.stop(tenant, namespace).await?;
        }
        let _ = crate::lease::delete_tenant_listener(pg_pool, tenant.as_str()).await;
        Ok(())
    }
}

