//! kind-based infra backend for local dev.
//!
//! Provisions a local Kubernetes cluster on first use (via the
//! `kind` CLI) and `kubectl apply`s a pod + service for each
//! sidecar. This is the real thing: enterprise BYOC and cloud use
//! real k8s, and local should too. Docker-compose was a shortcut.
//!
//! The backend shells out to `kind` and `kubectl`. Both must be on
//! PATH; we fail loudly with an actionable message if they aren't.
//!
//! Phase A: provision/deprovision + port-forwarded access via
//! kubectl. Phase B: wire the SSE /events stream back into the
//! dispatcher's event bus (currently stubbed).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

use crate::backend::{EventStream, InfraBackend, InfraHandle, InfraSpec};

const CLUSTER_NAME: &str = "weft-local";
const NAMESPACE: &str = "weft-infra";

pub struct KindInfraBackend {
    /// handle_id -> (pod_name, forwarder_pid_or_none). Tracks what's
    /// deployed so deprovision can clean up.
    handles: Arc<Mutex<HashMap<String, DeployedPod>>>,
}

struct DeployedPod {
    pod_name: String,
}

impl KindInfraBackend {
    pub fn new() -> Self {
        Self { handles: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Create the kind cluster + namespace if they aren't already.
    /// Called lazily on first `provision` so we don't pay the cluster
    /// startup cost when no infra nodes are in use.
    async fn ensure_cluster(&self) -> anyhow::Result<()> {
        assert_binary("kind").await?;
        assert_binary("kubectl").await?;

        // `kind get clusters` returns the cluster names one per line.
        let existing = Command::new("kind").arg("get").arg("clusters").output().await?;
        let list = String::from_utf8_lossy(&existing.stdout);
        if !list.lines().any(|name| name == CLUSTER_NAME) {
            tracing::info!(target: "weft::infra::kind", "creating kind cluster '{CLUSTER_NAME}' (first run)");
            let status = Command::new("kind")
                .args(["create", "cluster", "--name", CLUSTER_NAME])
                .status()
                .await?;
            if !status.success() {
                anyhow::bail!("kind create cluster failed with {status}");
            }
        }

        // Ensure namespace. Idempotent via `apply`.
        let manifest = format!(
            "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {NAMESPACE}\n",
        );
        kubectl_apply_manifest(&manifest).await?;
        Ok(())
    }
}

impl Default for KindInfraBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl InfraBackend for KindInfraBackend {
    async fn provision(&self, spec: InfraSpec) -> anyhow::Result<InfraHandle> {
        self.ensure_cluster().await?;

        let image = spec
            .config
            .get("image")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("infra spec missing 'image'"))?;
        let port = spec.config.get("port").and_then(|v| v.as_u64()).unwrap_or(8080) as u16;

        // Pod name: predictable + unique so re-provisions don't
        // collide. Lowercased + stripped to match k8s naming.
        let pod_name = format!(
            "weft-{}-{}",
            sanitize(&spec.infra_node_id),
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );

        // Keep it minimal: one pod + ClusterIP service per sidecar.
        // Volumes, secrets, env vars land when the first infra-bound
        // node that needs them ships.
        let manifest = format!(
            "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {pod_name}\n  namespace: {NAMESPACE}\n  labels:\n    app: {pod_name}\n    weft-project: \"{project}\"\nspec:\n  containers:\n  - name: sidecar\n    image: {image}\n    ports:\n    - containerPort: {port}\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: {pod_name}\n  namespace: {NAMESPACE}\nspec:\n  selector:\n    app: {pod_name}\n  ports:\n  - port: {port}\n    targetPort: {port}\n",
            project = spec.project_id,
        );
        kubectl_apply_manifest(&manifest).await?;

        let handle_id = pod_name.clone();
        self.handles
            .lock()
            .await
            .insert(handle_id.clone(), DeployedPod { pod_name });
        tracing::info!(target: "weft::infra::kind", handle = %handle_id, "sidecar pod provisioned");

        Ok(InfraHandle { id: handle_id })
    }

    async fn deprovision(&self, handle: InfraHandle) -> anyhow::Result<()> {
        let deployed = self.handles.lock().await.remove(&handle.id);
        let Some(deployed) = deployed else {
            // Idempotent: unknown handle = nothing to do.
            return Ok(());
        };
        let _ = Command::new("kubectl")
            .args(["-n", NAMESPACE, "delete", "pod", &deployed.pod_name, "--ignore-not-found"])
            .status()
            .await;
        let _ = Command::new("kubectl")
            .args(["-n", NAMESPACE, "delete", "service", &deployed.pod_name, "--ignore-not-found"])
            .status()
            .await;
        Ok(())
    }

    async fn stream_events(&self, _handle: InfraHandle) -> anyhow::Result<EventStream> {
        // Phase A: empty stream. When the first infra-backed node
        // ships (e.g. DiscordReceive), wire a background task that
        // port-forwards to the pod's /events SSE and bridges every
        // payload into this channel.
        let (_tx, rx) = mpsc::channel(64);
        Ok(rx)
    }
}

/// Apply a YAML manifest by piping it to `kubectl apply -f -`.
async fn kubectl_apply_manifest(manifest: &str) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
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

async fn assert_binary(name: &str) -> anyhow::Result<()> {
    let status = Command::new(name).arg("--version").output().await;
    match status {
        Ok(out) if out.status.success() => Ok(()),
        _ => anyhow::bail!(
            "`{name}` not found on PATH. Install it to use the kind infra backend \
             (https://kind.sigs.k8s.io/ for kind, https://kubernetes.io/docs/tasks/tools/ \
             for kubectl)."
        ),
    }
}

fn sanitize(s: &str) -> String {
    // k8s names: [a-z0-9-], lowercase, <= 63 chars. Map anything
    // else to `-`, collapse doubles, truncate.
    let mut out = String::with_capacity(s.len());
    let mut last_dash = false;
    for c in s.chars() {
        let mapped = c.to_ascii_lowercase();
        if mapped.is_ascii_alphanumeric() {
            out.push(mapped);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    while out.starts_with('-') {
        out.remove(0);
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        return "sidecar".into();
    }
    if out.len() > 50 {
        out.truncate(50);
    }
    out
}
