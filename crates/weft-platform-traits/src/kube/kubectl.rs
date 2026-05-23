//! Production impl: shells out to `kubectl`. The supervisor's RBAC
//! scope is per-tenant via RoleBindings; calling `kubectl` from
//! inside the pod with the projected SA token + audience picks up
//! those permissions automatically.
//!
//! All calls are async via `tokio::process`; failures bubble up as
//! `anyhow::Error`.

use std::process::Stdio;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::{KubeClient, KubeReader, KubeWriter, WorkloadKind, WorkloadReplicaState};

#[derive(Clone)]
pub struct KubectlClient;

impl KubectlClient {
    pub async fn in_cluster() -> Result<Arc<dyn KubeClient>> {
        let out = Command::new("kubectl")
            .arg("version")
            .arg("--client")
            .output()
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "kubectl --client failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(Arc::new(Self))
    }
}

#[async_trait]
impl KubeReader for KubectlClient {
    async fn list_replica_state(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<WorkloadReplicaState>> {
        let out = Command::new("kubectl")
            .args([
                "-n",
                namespace,
                "get",
                "deployment,statefulset",
                "-l",
                selector,
                "-o",
                "json",
            ])
            .output()
            .await?;
        // Fail loud on non-zero exit. Returning an empty Vec on
        // kubectl failure hides RBAC regressions / network blips /
        // apiserver outages: the health loop would then treat every
        // node as "no workloads, ratio=1.0, healthy" : exactly the
        // failure mode this fix prevents.
        if !out.status.success() {
            anyhow::bail!(
                "kubectl get -n {namespace} deployment,statefulset -l {selector} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let parsed: serde_json::Value =
            serde_json::from_slice(&out.stdout).map_err(|e| anyhow!("parse kubectl get: {e}"))?;
        let items = parsed
            .get("items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            // `kind` is on each item; mixed-kind get returns
            // "Deployment" or "StatefulSet". Anything else is
            // schema drift; bail rather than silently drop.
            let kind_str = item
                .get("kind")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("kubectl item missing .kind"))?;
            let kind = match kind_str {
                "Deployment" => WorkloadKind::Deployment,
                "StatefulSet" => WorkloadKind::StatefulSet,
                other => anyhow::bail!(
                    "kubectl returned unexpected workload kind '{other}' in -l {selector} \
                     listing; only Deployment/StatefulSet are managed"
                ),
            };
            // Missing metadata.name is corruption, not a benign
            // default: a nameless WorkloadReplicaState can't be
            // scaled/deleted by name. Bail loud like `.kind` above
            // rather than emit a "" row. (namespace below defaults
            // to the queried ns, which is defensible since the get
            // is namespace-scoped; name has no such fallback.)
            let name = item
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .ok_or_else(|| anyhow!("kubectl item missing metadata.name"))?
                .to_string();
            let ns = item
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str())
                .unwrap_or(namespace)
                .to_string();
            let labels = item
                .get("metadata")
                .and_then(|m| m.get("labels"))
                .and_then(|l| l.as_object())
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();
            let desired = item
                .get("spec")
                .and_then(|s| s.get("replicas"))
                .and_then(|n| n.as_i64())
                .unwrap_or(0);
            let ready = item
                .get("status")
                .and_then(|s| s.get("readyReplicas"))
                .and_then(|n| n.as_i64())
                .unwrap_or(0);
            out.push(WorkloadReplicaState {
                kind,
                name,
                namespace: ns,
                desired,
                ready,
                labels,
            });
        }
        Ok(out)
    }

    async fn deployment_exists(&self, namespace: &str, name: &str) -> super::DeploymentLookup {
        use super::DeploymentLookup;
        let out = Command::new("kubectl")
            .args(["-n", namespace, "get", "deployment", name])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .output()
            .await;
        match out {
            Ok(o) if o.status.success() => DeploymentLookup::Exists,
            Ok(o) => {
                let stderr = String::from_utf8_lossy(&o.stderr);
                if stderr.contains("NotFound") || stderr.contains("not found") {
                    DeploymentLookup::NotFound
                } else {
                    DeploymentLookup::Errored(format!(
                        "kubectl status={:?} stderr={}",
                        o.status, stderr
                    ))
                }
            }
            Err(e) => DeploymentLookup::Errored(format!("kubectl run failed: {e}")),
        }
    }

    async fn pod_waiting_reason(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Option<String>> {
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
            // Pod not yet visible / transient apiserver miss. Not
            // an error: the caller polls, treat as "no reason yet".
            return Ok(None);
        }
        let reason = String::from_utf8_lossy(&out.stdout).trim().to_string();
        Ok(if reason.is_empty() { None } else { Some(reason) })
    }
}

#[async_trait]
impl KubeWriter for KubectlClient {
    async fn scale_workload(
        &self,
        namespace: &str,
        kind: WorkloadKind,
        name: &str,
        replicas: u32,
    ) -> Result<()> {
        let resource = format!("{}/{name}", kind.kubectl_prefix());
        let status = Command::new("kubectl")
            .args([
                "-n",
                namespace,
                "scale",
                &resource,
                &format!("--replicas={replicas}"),
            ])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("kubectl scale {resource} --replicas={replicas} failed");
        }
        Ok(())
    }

    async fn delete_named(
        &self,
        namespace: &str,
        kind: &str,
        name: &str,
        opts: super::DeleteOpts,
    ) -> Result<()> {
        let wait_flag = if opts.waits() { "--wait=true" } else { "--wait=false" };
        let mut args = vec![
            "-n", namespace, "delete", kind, name,
            "--ignore-not-found", wait_flag,
        ];
        if opts.cascades() {
            args.push("--cascade=foreground");
        }
        let status = Command::new("kubectl").args(&args).status().await?;
        if !status.success() {
            anyhow::bail!("kubectl delete {kind}/{name} failed in namespace {namespace}");
        }
        Ok(())
    }

    async fn delete_by_label(
        &self,
        namespace: &str,
        selector: &str,
        preserve_pvcs: &[String],
    ) -> Result<()> {
        let kinds: &[&str] = &[
            "deployment",
            "statefulset",
            "daemonset",
            "job",
            "service",
            "configmap",
            "secret",
            "ingress",
            "horizontalpodautoscaler",
            "networkpolicy",
            "pod",
        ];
        kubectl_delete_kinds(namespace, selector, kinds).await?;

        // PVCs: enumerate matching, delete those NOT in
        // preserve_pvcs. A non-trivial preserve list is rare
        // (most nodes preserve nothing), so the common case is a
        // single `kubectl delete pvc -l ...` call without any
        // per-name filtering.
        if preserve_pvcs.is_empty() {
            return kubectl_delete_kinds(namespace, selector, &["pvc"]).await;
        }
        let out = Command::new("kubectl")
            .args([
                "-n",
                namespace,
                "get",
                "pvc",
                "-l",
                selector,
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ])
            .output()
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "kubectl get pvc -l {selector}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let names = String::from_utf8_lossy(&out.stdout);
        let preserve: std::collections::HashSet<&str> =
            preserve_pvcs.iter().map(String::as_str).collect();
        let to_delete: Vec<&str> = names
            .split_whitespace()
            .filter(|n| !preserve.contains(n))
            .collect();
        if to_delete.is_empty() {
            return Ok(());
        }
        // Match `kubectl_delete_kinds` invariants:
        // `--ignore-not-found` covers the race where another
        // controller deleted a PVC between our `get` and this
        // `delete`; `--wait=false` doesn't block on finalizers
        // (the supervisor's health loop observes steady state on
        // the next tick).
        let mut args: Vec<&str> = vec![
            "-n",
            namespace,
            "delete",
            "pvc",
            "--ignore-not-found",
            "--wait=false",
        ];
        args.extend(to_delete);
        let status = Command::new("kubectl").args(args).status().await?;
        if !status.success() {
            anyhow::bail!("kubectl delete pvc (subset) failed");
        }
        Ok(())
    }

    async fn delete_pods(&self, namespace: &str, selector: &str) -> Result<()> {
        // Pods only. The Deployment / Service / ConfigMap / Secret /
        // PVC stay; the controller respawns Pods with the same spec.
        kubectl_delete_kinds(namespace, selector, &["pod"]).await
    }

    async fn apply(&self, manifest: &serde_json::Value) -> Result<()> {
        let body = serde_json::to_string(manifest)?;
        kubectl_apply_stdin(body.as_bytes()).await
    }

    async fn apply_yaml(&self, manifest: &str) -> Result<()> {
        kubectl_apply_stdin(manifest.as_bytes()).await
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        let status = Command::new("kubectl")
            .args(["delete", "namespace", name, "--ignore-not-found", "--wait=false"])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!("kubectl delete namespace {name} failed");
        }
        Ok(())
    }

    async fn wait_rollout_status(
        &self,
        namespace: &str,
        deployment: &str,
        timeout_seconds: u32,
    ) -> Result<()> {
        let target = format!("deployment/{deployment}");
        let timeout = format!("--timeout={timeout_seconds}s");
        let status = Command::new("kubectl")
            .args(["-n", namespace, "rollout", "status", &target, &timeout])
            .status()
            .await?;
        if !status.success() {
            anyhow::bail!(
                "{deployment} did not reach Ready within {timeout_seconds}s"
            );
        }
        Ok(())
    }
}

/// Shared body for `apply` (JSON) + `apply_yaml` (raw multi-doc).
/// One `kubectl apply -f -` invocation; the typed and untyped
/// surfaces only differ in how they produce the bytes.
async fn kubectl_apply_stdin(body: &[u8]) -> Result<()> {
    let mut child = Command::new("kubectl")
        .args(["apply", "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(body).await?;
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("kubectl apply failed: {stderr}");
    }
    Ok(())
}

/// Shared helper for verbs that delete a fixed set of resource
/// kinds matching a label selector. `--ignore-not-found` makes
/// missing kinds a no-op; non-zero exit on any one kind bubbles up
/// as an `anyhow::Error` so the caller surfaces real failures.
async fn kubectl_delete_kinds(
    namespace: &str,
    selector: &str,
    kinds: &[&str],
) -> Result<()> {
    for kind in kinds {
        let out = Command::new("kubectl")
            .args([
                "-n",
                namespace,
                "delete",
                kind,
                "-l",
                selector,
                "--ignore-not-found",
                "--wait=false",
            ])
            .output()
            .await?;
        if !out.status.success() {
            anyhow::bail!(
                "kubectl delete {kind} -l {selector} -n {namespace} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
    Ok(())
}

// No tests at this layer: kubectl-shelling integration belongs in
// layer-4 tests against a real kind cluster, and pure-logic checks
// (selector parsing, manifest shape) live next to their owners
// (fake.rs, supervisor's lifecycle.rs).
