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

use std::sync::Arc;

use async_trait::async_trait;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};

use crate::backend::{EventStream, InfraBackend, InfraHandle, InfraSpec};

fn in_cluster() -> bool {
    // The k8s downward-API mount is only present inside a Pod.
    // When we're in-cluster we use the Pod's ServiceAccount via
    // kubectl's default config and skip the kind bootstrap.
    std::path::Path::new("/var/run/secrets/kubernetes.io").exists()
}

pub struct KindInfraBackend {
    /// Known handle ids. delete() uses a label-selector sweep
    /// rather than a stored per-pod manifest, so we only need to
    /// know which ids we've seen for idempotency's sake. The full
    /// sidecar state is in k8s, not here.
    handles: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl KindInfraBackend {
    pub fn new() -> Self {
        Self { handles: Arc::new(Mutex::new(std::collections::HashSet::new())) }
    }

    /// Make sure the cluster + the named namespace exist before we
    /// apply Pod/Service manifests. When the dispatcher runs
    /// in-cluster (the k8s deploy path) there's nothing to do: the
    /// cluster is us, and the namespace was created by
    /// `weft daemon start`. Outside the cluster, this path is only
    /// hit by unit tests that run the dispatcher as a host process;
    /// we skip kind bootstrap there too since `weft daemon start`
    /// owns that.
    async fn ensure_namespace(&self, ns: &str) -> anyhow::Result<()> {
        if in_cluster() {
            return Ok(());
        }
        assert_binary("kubectl").await?;
        let manifest = format!(
            "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {ns}\n",
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
        let ns = spec.namespace.clone();
        self.ensure_namespace(&ns).await?;

        let image = format!("ghcr.io/weavemindai/sidecar-{}:latest", spec.sidecar.name);
        let port = spec.sidecar.port;

        // Pod name: predictable + unique so re-provisions don't
        // collide. Lowercased + stripped to match k8s naming.
        let pod_name = format!(
            "weft-{}-{}",
            sanitize(&spec.infra_node_id),
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );

        // If the metadata provided raw manifests, apply them with
        // placeholder substitution. Otherwise fall back to a
        // minimal pod + service using the derived image/port.
        let manifest = if spec.sidecar.manifests.is_empty() {
            format!(
                "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {pod_name}\n  namespace: {ns}\n  labels:\n    app: {pod_name}\n    weft-project: \"{project}\"\nspec:\n  containers:\n  - name: sidecar\n    image: {image}\n    ports:\n    - containerPort: {port}\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: {pod_name}\n  namespace: {ns}\nspec:\n  selector:\n    app: {pod_name}\n  ports:\n  - port: {port}\n    targetPort: {port}\n",
                project = spec.project_id,
            )
        } else {
            // Each manifest doc needs to be applied separately:
            // kubectl's `apply -f -` accepts one JSON document OR
            // a YAML stream with `---` separators, but not multiple
            // JSON docs. Easiest is to apply them one at a time.
            //
            // Before apply we inject the weft.dev/* labels onto
            // every manifest's metadata so a post-restart registry
            // rebuild can find them by label selector, and so
            // delete-by-label sweeps catch everything the sidecar
            // owns (not just Deployment / Service / PVC we have
            // hard-coded names for).
            for raw in &spec.sidecar.manifests {
                let mut doc = raw.clone();
                inject_weft_labels(&mut doc, &pod_name, &spec.project_id, &spec.infra_node_id);
                let s = serde_json::to_string(&doc)
                    .map_err(|e| anyhow::anyhow!("serialize manifest: {e}"))?;
                let s = s
                    .replace("__INSTANCE_ID__", &pod_name)
                    .replace("__NAMESPACE__", &ns)
                    .replace("__SIDECAR_IMAGE__", &image);
                kubectl_apply_manifest(&s).await?;
            }
            String::new()
        };
        if !manifest.is_empty() {
            kubectl_apply_manifest(&manifest).await?;
        }

        let handle_id = pod_name.clone();
        self.handles
            .lock()
            .await
            .insert(handle_id.clone());
        tracing::info!(target: "weft::infra::kind", handle = %handle_id, "sidecar pod provisioned");

        // ClusterIP service DNS. Resolvable from any pod in the
        // cluster (including the listener and worker pods). For
        // local dev running the dispatcher/workers on the host,
        // use kubectl port-forward for access; not wired here yet.
        let endpoint_url = Some(format!(
            "http://{pod}.{ns}.svc.cluster.local:{port}",
            pod = handle_id,
            ns = ns,
            port = port,
        ));
        Ok(InfraHandle {
            id: handle_id,
            endpoint_url,
            namespace: ns,
        })
    }

    async fn scale_to_zero(&self, handle: &InfraHandle) -> anyhow::Result<()> {
        kubectl_scale(&handle.namespace, &handle.id, 0).await
    }

    async fn scale_up(&self, handle: &InfraHandle) -> anyhow::Result<()> {
        kubectl_scale(&handle.namespace, &handle.id, 1).await
    }

    async fn delete(&self, handle: InfraHandle) -> anyhow::Result<()> {
        self.handles.lock().await.remove(&handle.id);
        let ns = handle.namespace.clone();
        // Best-effort: delete resources both by name (the sidecar
        // manifests use `__INSTANCE_ID__` so Deployment / Service /
        // PVC share the handle id as their name) and by label
        // selector (catches anything the manifest tagged with
        // `app=<id>` — e.g. Ingress or additional Services we
        // haven't hard-coded names for). `--ignore-not-found` makes
        // both sweeps idempotent.
        for kind in ["deployment", "service", "ingress", "pvc", "pod"] {
            let _ = Command::new("kubectl")
                .args([
                    "-n", &ns, "delete", kind, &handle.id,
                    "--ignore-not-found", "--wait=false",
                ])
                .status()
                .await;
            let _ = Command::new("kubectl")
                .args([
                    "-n", &ns, "delete", kind, "-l",
                    &format!("app={}", handle.id),
                    "--ignore-not-found", "--wait=false",
                ])
                .status()
                .await;
        }
        // WhatsApp's sidecar creates an auth PVC named
        // `<id>-auth`; the delete-by-label sweep misses it because
        // the manifest doesn't label PVCs.
        let _ = Command::new("kubectl")
            .args([
                "-n", &ns, "delete", "pvc", &format!("{}-auth", handle.id),
                "--ignore-not-found", "--wait=false",
            ])
            .status()
            .await;
        Ok(())
    }

    async fn rehydrate(
        &self,
        namespaces: &[String],
    ) -> anyhow::Result<Vec<crate::backend::AdoptedHandle>> {
        let mut adopted = Vec::new();
        for ns in namespaces {
            // Sweep legacy orphans. Any Deployment whose name starts
            // with `weft-` but that DOESN'T carry the weft.dev
            // adoption labels is dead state from a prior dispatcher
            // that didn't know to label, or from a manual kubectl
            // apply. Best to delete it so the next `infra start`
            // provisions fresh. We explicitly skip the dispatcher's
            // own resources and listener deployments (owned by a
            // different backend).
            for kind in ["deployment", "service", "pvc"] {
                let out = Command::new("kubectl")
                    .args(["-n", ns, "get", kind, "-o", "json"])
                    .output()
                    .await?;
                if !out.status.success() {
                    continue;
                }
                let parsed: serde_json::Value = match serde_json::from_slice(&out.stdout) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                for item in parsed
                    .get("items")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default()
                {
                    let metadata = item.get("metadata").cloned().unwrap_or_default();
                    let name = metadata
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if !name.starts_with("weft-") {
                        continue;
                    }
                    if name == "weft-dispatcher"
                        || name.starts_with("weft-dispatcher-")
                        || name.starts_with("listener-")
                    {
                        continue;
                    }
                    let labels = metadata.get("labels").cloned().unwrap_or_default();
                    let has_weft_label = labels
                        .as_object()
                        .map(|m| m.contains_key("weft.dev/project"))
                        .unwrap_or(false);
                    if has_weft_label {
                        continue;
                    }
                    tracing::info!(
                        target: "weft::infra::kind",
                        %kind, %name, %ns,
                        "sweeping unlabeled legacy weft resource"
                    );
                    let _ = Command::new("kubectl")
                        .args([
                            "-n", ns, "delete", kind, name,
                            "--ignore-not-found", "--wait=false",
                        ])
                        .status()
                        .await;
                }
            }

            // Adopt everything that DID label itself in this namespace.
            let out = Command::new("kubectl")
                .args([
                    "-n", ns, "get", "deployment",
                    "-l", "weft.dev/role=infra",
                    "-o", "json",
                ])
                .output()
                .await?;
            if !out.status.success() {
                continue;
            }
            let parsed: serde_json::Value = serde_json::from_slice(&out.stdout)
                .map_err(|e| anyhow::anyhow!("parse kubectl get: {e}"))?;
            let items = parsed
                .get("items")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            for d in items {
                let metadata = d.get("metadata").cloned().unwrap_or_default();
                let name = metadata
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let labels = metadata.get("labels").cloned().unwrap_or_default();
                let project = labels
                    .get("weft.dev/project")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let node = labels
                    .get("weft.dev/node")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if project.is_empty() || node.is_empty() || name.is_empty() {
                    continue;
                }
                let replicas = d
                    .get("spec")
                    .and_then(|s| s.get("replicas"))
                    .and_then(|r| r.as_i64())
                    .unwrap_or(1);
                let port = d
                    .get("spec")
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                    .and_then(|s| s.get("containers"))
                    .and_then(|c| c.as_array())
                    .and_then(|c| c.first())
                    .and_then(|c| c.get("ports"))
                    .and_then(|p| p.as_array())
                    .and_then(|p| p.first())
                    .and_then(|p| p.get("containerPort"))
                    .and_then(|p| p.as_i64())
                    .unwrap_or(8080) as u16;
                let endpoint_url = Some(format!(
                    "http://{name}.{ns}.svc.cluster.local:{port}"
                ));
                self.handles.lock().await.insert(name.clone());
                adopted.push(crate::backend::AdoptedHandle {
                    project_id: project,
                    node_id: node,
                    handle: InfraHandle {
                        id: name,
                        endpoint_url,
                        namespace: ns.clone(),
                    },
                    running: replicas > 0,
                });
            }
        }
        Ok(adopted)
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
/// Merge weft-owned labels into every manifest's metadata.labels
/// map. On restart we scan Deployments by these labels to rebuild
/// the in-memory infra registry without touching the cluster's
/// actual state.
fn inject_weft_labels(
    doc: &mut serde_json::Value,
    instance_id: &str,
    project_id: &str,
    node_id: &str,
) {
    let Some(obj) = doc.as_object_mut() else { return };
    let metadata = obj
        .entry("metadata".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(md) = metadata.as_object_mut() else { return };
    let labels = md
        .entry("labels".to_string())
        .or_insert_with(|| serde_json::json!({}));
    let Some(lbls) = labels.as_object_mut() else { return };
    lbls.entry("weft.dev/role".to_string())
        .or_insert_with(|| serde_json::json!("infra"));
    lbls.insert(
        "weft.dev/project".to_string(),
        serde_json::Value::String(project_id.to_string()),
    );
    lbls.insert(
        "weft.dev/node".to_string(),
        serde_json::Value::String(node_id.to_string()),
    );
    lbls.insert(
        "weft.dev/instance".to_string(),
        serde_json::Value::String(instance_id.to_string()),
    );
}

/// Scale the Deployment named `id` in `ns` to `replicas`. Used by
/// stop (replicas=0) and start-after-stop (replicas=1). Target by
/// name rather than label because the sidecar manifests from node
/// metadata don't always set metadata.labels on the Deployment
/// itself (only on the pod template + service selector).
async fn kubectl_scale(ns: &str, id: &str, replicas: u32) -> anyhow::Result<()> {
    let status = Command::new("kubectl")
        .args([
            "-n", ns, "scale", &format!("deployment/{id}"),
            &format!("--replicas={replicas}"),
        ])
        .status()
        .await?;
    if !status.success() {
        anyhow::bail!("kubectl scale deployment/{id} --replicas={replicas} failed");
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
