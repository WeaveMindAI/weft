//! Listener-backend abstraction. The dispatcher spawns a per-project
//! listener at project activation; the listener owns all wake signal
//! routing. Two backends exist:
//!
//!   - `SubprocessListenerBackend`: spawns the `weft-listener` binary
//!     as a local subprocess. Used for local development.
//!   - `K8sListenerBackend` (future): spawns a pod in the project's
//!     k8s namespace so the listener lives alongside infra sidecars.
//!
//! Only the backend knows how to start a listener; the dispatcher
//! only holds `ListenerHandle`s (URL + admin token) and POSTs to
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
    /// Spawn a listener instance for `project_id`. Returns the
    /// handle to register signals against.
    async fn spawn(
        &self,
        project_id: &str,
        dispatcher_url: &str,
    ) -> Result<ListenerHandle>;

    /// Kill the listener for `project_id`. Called on deactivate.
    async fn stop(&self, project_id: &str) -> Result<()>;
}

/// Local-development backend: fork the `weft-listener` binary as
/// a child process. Listener binds an OS-assigned port; backend
/// reports the port back via the child's stdout (parsed line by
/// line for a "listening on :PORT" log line) OR by pre-allocating
/// a port (simpler, what we do here).
pub struct SubprocessListenerBackend {
    /// Absolute path to the `weft-listener` binary.
    binary_path: PathBuf,
    /// Running children keyed by project id. Dropping the `Child`
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
        project_id: &str,
        dispatcher_url: &str,
    ) -> Result<ListenerHandle> {
        // Pre-allocate a port by binding then dropping (classic
        // race-but-it's-dev-only pattern).
        let port = pick_free_port()?;
        let admin_url = format!("http://127.0.0.1:{port}");
        let public_base_url = admin_url.clone();
        let admin_token = Uuid::new_v4().to_string();
        let relay_token = Uuid::new_v4().to_string();

        let mut cmd = Command::new(&self.binary_path);
        cmd.env("WEFT_LISTENER_PROJECT_ID", project_id)
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
            .with_context(|| format!("spawn listener for {project_id}"))?;
        self.children
            .insert(project_id.to_string(), Arc::new(Mutex::new(child)));

        // Wait for health.
        wait_for_health(&admin_url).await?;

        Ok(ListenerHandle {
            admin_url,
            public_base_url,
            admin_token,
            relay_token,
        })
    }

    async fn stop(&self, project_id: &str) -> Result<()> {
        if let Some((_, child)) = self.children.remove(project_id) {
            let mut c = child.lock().await;
            let _ = c.kill().await;
        }
        Ok(())
    }
}

/// K8s listener backend. The dispatcher Pod applies a Deployment +
/// Service + Ingress per project into its own namespace via kubectl,
/// then resolves URLs using cluster DNS (for internal calls) and
/// ingress hostnames (for user-facing webhook/form URLs).
///
/// Same manifest pattern as `KindInfraBackend`. Hidden from the
/// manifest shape: the listener image tag is read from the
/// `WEFT_LISTENER_IMAGE` env var on the dispatcher Pod so the CLI
/// controls which version runs.
pub struct K8sListenerBackend {
    namespace: String,
    listener_image: String,
    /// External hostname suffix for ingress. Webhook URLs end up
    /// as `http://<short>.{ingress_host_suffix}`. Defaults to
    /// `listener.weft.local` for kind; cloud deploy sets the real
    /// public suffix.
    ingress_host_suffix: String,
    /// Short suffix per deployed project so we can clean up on stop.
    deployments: Arc<DashMap<String, String>>,
}

impl K8sListenerBackend {
    pub fn new(namespace: String, listener_image: String, ingress_host_suffix: String) -> Self {
        Self {
            namespace,
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
        project_id: &str,
        dispatcher_url: &str,
    ) -> Result<ListenerHandle> {
        let short = short_id(project_id);
        let deploy_name = format!("listener-{short}");
        let admin_token = Uuid::new_v4().to_string();
        let relay_token = Uuid::new_v4().to_string();
        let public_host = format!("{short}.{}", self.ingress_host_suffix);
        let public_base_url = format!("http://{public_host}");
        let admin_url = format!(
            "http://{deploy_name}.{ns}.svc.cluster.local:8080",
            ns = self.namespace,
        );

        let manifest = render_listener_manifest(
            &deploy_name,
            &self.namespace,
            &self.listener_image,
            project_id,
            dispatcher_url,
            &public_base_url,
            &admin_token,
            &relay_token,
            &public_host,
        );
        kubectl_apply_manifest(&manifest).await?;
        kubectl_rollout_status(&self.namespace, &deploy_name).await?;
        // Health-check through the cluster DNS; if the dispatcher
        // Pod can't resolve the listener there's no point
        // returning the handle.
        wait_for_health(&admin_url).await?;

        self.deployments
            .insert(project_id.to_string(), deploy_name);
        Ok(ListenerHandle {
            admin_url,
            public_base_url,
            admin_token,
            relay_token,
        })
    }

    async fn stop(&self, project_id: &str) -> Result<()> {
        let Some((_, deploy_name)) = self.deployments.remove(project_id) else {
            return Ok(());
        };
        for kind in ["ingress", "service", "deployment"] {
            let _ = Command::new("kubectl")
                .args([
                    "-n", &self.namespace, "delete", kind, &deploy_name,
                    "--ignore-not-found", "--wait=false",
                ])
                .status()
                .await;
        }
        Ok(())
    }
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
    project_id: &str,
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
    weft.dev/project: "{project_id}"
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
    spec:
      containers:
        - name: listener
          image: {image}
          imagePullPolicy: IfNotPresent
          ports:
            - containerPort: 8080
          env:
            - name: WEFT_LISTENER_PROJECT_ID
              value: "{project_id}"
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

/// In-memory map of per-project listener handles. Lives on
/// DispatcherState.
#[derive(Default, Clone)]
pub struct ListenerRegistry {
    inner: Arc<DashMap<String, ListenerHandle>>,
}

impl ListenerRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&self, project_id: String, handle: ListenerHandle) {
        self.inner.insert(project_id, handle);
    }
    pub fn get(&self, project_id: &str) -> Option<ListenerHandle> {
        self.inner.get(project_id).map(|h| h.clone())
    }
    pub fn remove(&self, project_id: &str) -> Option<ListenerHandle> {
        self.inner.remove(project_id).map(|(_, v)| v)
    }
    /// Look up which project owns a relay token. Used by
    /// `/signal-fired` to authenticate the incoming relay.
    pub fn project_for_relay_token(&self, token: &str) -> Option<String> {
        for e in self.inner.iter() {
            if e.value().relay_token == token {
                return Some(e.key().clone());
            }
        }
        None
    }
}

/// Simple metadata bundle for a signal registered with a listener.
/// The dispatcher keeps this so a fire relay can be resolved back
/// to a node id + expected next action (entry run vs resume).
#[derive(Debug, Clone)]
pub struct RegisteredSignalMeta {
    pub project_id: String,
    pub token: String,
    pub node_id: String,
    pub is_resume: bool,
    /// The listener-minted user URL (if any). Stored here so
    /// activate can surface it back without an extra round-trip.
    pub user_url: Option<String>,
    /// Kind label for the listing UI. "webhook", "timer", "form",
    /// "socket". Populated at register time.
    pub kind: String,
}

/// Per-dispatcher registry tracking every signal currently live on
/// any listener. Populated on activate + suspension-register,
/// cleared on deactivate + suspension-consume.
#[derive(Default, Clone)]
pub struct SignalTracker {
    inner: Arc<DashMap<String, RegisteredSignalMeta>>,
}

impl SignalTracker {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn list_for_project(&self, project_id: &str) -> Vec<RegisteredSignalMeta> {
        self.inner
            .iter()
            .filter(|e| e.value().project_id == project_id)
            .map(|e| e.value().clone())
            .collect()
    }
    pub fn insert(&self, token: String, meta: RegisteredSignalMeta) {
        self.inner.insert(token, meta);
    }
    pub fn get(&self, token: &str) -> Option<RegisteredSignalMeta> {
        self.inner.get(token).map(|v| v.clone())
    }
    pub fn remove(&self, token: &str) -> Option<RegisteredSignalMeta> {
        self.inner.remove(token).map(|(_, v)| v)
    }
    pub fn remove_project(&self, project_id: &str) -> Vec<RegisteredSignalMeta> {
        let mut out = Vec::new();
        let keys: Vec<String> = self
            .inner
            .iter()
            .filter(|e| e.value().project_id == project_id)
            .map(|e| e.key().clone())
            .collect();
        for k in keys {
            if let Some((_, v)) = self.inner.remove(&k) {
                out.push(v);
            }
        }
        out
    }
}
