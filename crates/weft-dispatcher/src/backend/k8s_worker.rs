//! K8s worker backend. Each execution runs as a short-lived Pod in
//! the dispatcher's namespace. Image: `weft-worker-<project-id>`
//! produced by `weft build`. Container args connect it back to the
//! dispatcher over cluster DNS.
//!
//! Why Pods and not Jobs: we don't need retry semantics (the
//! dispatcher replays from the journal on crash). A naked Pod is
//! simpler and cleaner in the event log.

use std::path::Path;

use async_trait::async_trait;
use tokio::process::Command;

use crate::backend::{WakeContext, WorkerBackend, WorkerHandle};

pub struct K8sWorkerBackend {
    namespace: String,
    /// DNS name of the dispatcher service, reachable from inside
    /// the cluster. e.g. `http://weft-dispatcher.wm-local.svc.cluster.local:9999`.
    dispatcher_url: String,
}

impl K8sWorkerBackend {
    pub fn new(namespace: String, dispatcher_url: String) -> Self {
        Self { namespace, dispatcher_url }
    }
}

#[async_trait]
impl WorkerBackend for K8sWorkerBackend {
    async fn spawn_worker(
        &self,
        binary_path: &Path,
        wake: WakeContext,
    ) -> anyhow::Result<WorkerHandle> {
        // The "binary_path" in this backend is a convention: the
        // stem encodes the project id so we can derive the image
        // tag. `weft run` already passes `binary_path` through the
        // run pipeline; we read the project id from `wake` and
        // ignore the physical path.
        let _ = binary_path;
        let image = format!("weft-worker-{}:latest", wake.project_id);
        let pod_name = format!(
            "worker-{}-{}",
            short_color(&wake.color.to_string()),
            &uuid::Uuid::new_v4().simple().to_string()[..6]
        );

        let manifest = render_pod_manifest(
            &pod_name,
            &self.namespace,
            &image,
            &wake.color.to_string(),
            &self.dispatcher_url,
            &wake.project_id,
        );
        kubectl_apply_manifest(&manifest).await?;
        Ok(WorkerHandle { id: pod_name })
    }

    async fn kill_worker(&self, handle: WorkerHandle) -> anyhow::Result<()> {
        let _ = Command::new("kubectl")
            .args([
                "-n", &self.namespace, "delete", "pod", &handle.id,
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
) -> String {
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    weft.dev/role: worker
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
