//! K8s worker backend. Each execution runs as a short-lived Pod in
//! the tenant's namespace (passed via `WakeContext`). Image:
//! `weft-worker-<project-id>` produced by `weft build`. Container
//! args connect it back to the dispatcher over cluster DNS.
//!
//! Why Pods and not Jobs: we don't need retry semantics (the
//! dispatcher replays from the journal on crash). A naked Pod is
//! simpler and cleaner in the event log.

use async_trait::async_trait;
use tokio::process::Command;

use crate::backend::{WakeContext, WorkerBackend, WorkerHandle};

pub struct K8sWorkerBackend {
    /// DNS name of the dispatcher service, reachable from inside
    /// the cluster. e.g. `http://weft-dispatcher.weft-system.svc.cluster.local:9999`.
    dispatcher_url: String,
    /// Stored separately from `WakeContext::namespace` so kill ops
    /// on a stale handle still resolve to the right namespace via
    /// a per-handle lookup. We index this map at spawn time.
    handle_namespaces: dashmap::DashMap<String, String>,
}

impl K8sWorkerBackend {
    pub fn new(dispatcher_url: String) -> Self {
        Self {
            dispatcher_url,
            handle_namespaces: dashmap::DashMap::new(),
        }
    }
}

#[async_trait]
impl WorkerBackend for K8sWorkerBackend {
    async fn spawn_worker(&self, wake: WakeContext) -> anyhow::Result<WorkerHandle> {
        let image = format!("weft-worker-{}:latest", wake.project_id);
        let pod_name = format!(
            "worker-{}-{}",
            short_color(&wake.color.to_string()),
            &uuid::Uuid::new_v4().simple().to_string()[..6]
        );

        let manifest = render_pod_manifest(
            &pod_name,
            &wake.namespace,
            &image,
            &wake.color.to_string(),
            &self.dispatcher_url,
            &wake.project_id,
            &wake.tenant,
        );
        kubectl_apply_manifest(&manifest).await?;
        self.handle_namespaces.insert(pod_name.clone(), wake.namespace);
        Ok(WorkerHandle { id: pod_name })
    }

    fn idle_grace_seconds(&self) -> u64 {
        // K8s pod spawn is multi-second; keeping a parked worker
        // around for a few seconds saves a cold-start when a human
        // replies fast. Override via env if pod density is more
        // valuable than respawn latency.
        std::env::var("WEFT_K8S_WORKER_GRACE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10)
    }

    async fn kill_worker(&self, handle: WorkerHandle) -> anyhow::Result<()> {
        let ns = self
            .handle_namespaces
            .remove(&handle.id)
            .map(|(_, ns)| ns);
        let Some(ns) = ns else {
            // No record of this handle. Either it was already
            // killed or this is a foreign handle. Skip.
            return Ok(());
        };
        let _ = Command::new("kubectl")
            .args([
                "-n", &ns, "delete", "pod", &handle.id,
                "--ignore-not-found", "--wait=false",
            ])
            .status()
            .await;
        Ok(())
    }
}

fn short_color(color: &str) -> String {
    color.chars().filter(|c| c.is_ascii_alphanumeric()).take(8).collect()
}

fn render_pod_manifest(
    name: &str,
    namespace: &str,
    image: &str,
    color: &str,
    dispatcher_url: &str,
    project_id: &str,
    tenant: &str,
) -> String {
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    weft.dev/role: worker
    weft.dev/tenant: "{tenant}"
    weft.dev/project: "{project_id}"
    weft.dev/color: "{color}"
spec:
  restartPolicy: Never
  containers:
    - name: worker
      image: {image}
      imagePullPolicy: IfNotPresent
      args:
        - "--color"
        - "{color}"
        - "--dispatcher"
        - "{dispatcher_url}"
      resources:
        requests:
          cpu: 50m
          memory: 64Mi
"#,
    )
}

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
