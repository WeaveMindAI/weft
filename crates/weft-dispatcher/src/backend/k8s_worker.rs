//! K8s worker backend. Spawns one Pod per project pool. Each Pod
//! multiplexes N concurrent executions for `weft-worker-<project_id>`
//! and idle-shuts itself down. The dispatcher's cold-start trigger
//! ensures a Pod exists whenever there's pending worker-target work
//! for a project.

use async_trait::async_trait;
use tokio::process::Command;

use crate::backend::{SpawnPodSpec, WorkerBackend, WorkerHandle};

pub struct K8sWorkerBackend {
    /// Broker URL injected into worker Pods. Workers never speak
    /// directly to Postgres in arch-5; everything goes through the
    /// broker, which validates the worker's projected SA token and
    /// scopes every operation per-tenant.
    broker_url: String,
}

impl K8sWorkerBackend {
    pub fn new(broker_url: String) -> Self {
        Self { broker_url }
    }
}

#[async_trait]
impl WorkerBackend for K8sWorkerBackend {
    async fn spawn_pod(
        &self,
        pod_name: &str,
        spec: SpawnPodSpec,
    ) -> anyhow::Result<WorkerHandle> {
        // Hash-tagged tags are the only path. If the CLI never set
        // a hash (e.g. someone POSTed /run before /projects), fail
        // loudly instead of falling back to `:latest`.
        let hash = spec.source_hash.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "spawn_pod for project {}: no running_source_hash set; \
                 register the project via the CLI (which builds + sets the hash) before \
                 calling /run, /activate, or /infra/start",
                spec.project_id,
            )
        })?;
        let image = format!("weft-worker-{}:{}", spec.project_id, hash);

        let manifest = render_pod_manifest(
            pod_name,
            &spec.namespace,
            &image,
            &spec.project_id,
            &spec.tenant,
            &self.broker_url,
            &spec.owner_dispatcher,
        );
        kubectl_apply_manifest(&manifest).await?;
        wait_for_pull_ok(pod_name, &spec.namespace).await?;
        Ok(WorkerHandle {
            pod_name: pod_name.to_string(),
        })
    }

    async fn kill_pod(&self, pod_name: String, namespace: String) -> anyhow::Result<()> {
        Command::new("kubectl")
            .args([
                "-n", &namespace, "delete", "pod", &pod_name,
                "--ignore-not-found", "--wait=false",
            ])
            .status()
            .await?;
        Ok(())
    }
}

pub(crate) fn short_project_id(project_id: &str) -> String {
    project_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(8)
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn render_pod_manifest(
    pod_name: &str,
    namespace: &str,
    image: &str,
    project_id: &str,
    tenant: &str,
    broker_url: &str,
    owner_dispatcher: &str,
) -> String {
    // Minimal pod: SA token mount (auth) + weft labels (routing /
    // cleanup). No security context, no resource limits. Tenant
    // workloads run with whatever defaults their namespace policy
    // allows; cross-tenant isolation comes from the namespace
    // boundary + NetworkPolicies, not from per-pod hardening.
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {pod_name}
  namespace: {namespace}
  labels:
    weft.dev/role: worker
    weft.dev/tenant: "{tenant}"
    weft.dev/project: "{project_id}"
spec:
  serviceAccountName: weft-worker-sa
  automountServiceAccountToken: false
  restartPolicy: OnFailure
  containers:
    - name: worker
      image: {image}
      imagePullPolicy: IfNotPresent
      env:
        - name: WEFT_PROJECT_ID
          value: "{project_id}"
        - name: WEFT_BROKER_URL
          value: "{broker_url}"
        - name: WEFT_BROKER_TOKEN_PATH
          value: "/var/run/weft/sa/token"
        - name: WEFT_NAMESPACE
          value: "{namespace}"
        - name: WEFT_OWNER_DISPATCHER
          value: "{owner_dispatcher}"
        - name: WEFT_POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: WEFT_TENANT_ID
          value: "{tenant}"
      volumeMounts:
        - name: weft-sa-token
          mountPath: /var/run/weft/sa
          readOnly: true
  volumes:
    - name: weft-sa-token
      projected:
        sources:
          - serviceAccountToken:
              audience: weft-broker
              expirationSeconds: 3600
              path: token
"#,
    )
}

async fn wait_for_pull_ok(pod_name: &str, namespace: &str) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let out = Command::new("kubectl")
            .args([
                "-n", namespace,
                "get", "pod", pod_name,
                "-o",
                "jsonpath={.status.containerStatuses[0].state.waiting.reason}",
            ])
            .output()
            .await?;
        if !out.status.success() {
            continue;
        }
        let reason = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if matches!(reason.as_str(), "ImagePullBackOff" | "ErrImagePull") {
            anyhow::bail!(
                "ImagePullBackOff for pod {pod_name}: image weft-worker-* not present in cluster"
            );
        }
    }
    Ok(())
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
