//! In-memory `KubeClient` for tests. Records every call, returns
//! seeded state from `list_replica_state`. Tests inject the
//! "current k8s state of the world" by calling `set_workloads`;
//! the supervisor's loops then observe whatever is in there.

use std::collections::HashMap;
use parking_lot::Mutex;

use anyhow::Result;
use async_trait::async_trait;

use super::{KubeReader, KubeWriter, WorkloadKind, WorkloadReplicaState};

/// One recorded call. Tests assert against the log to verify the
/// subsystem issued the expected kube operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KubeCall {
    ListReplicaState {
        namespace: String,
        selector: String,
    },
    Scale {
        namespace: String,
        kind: WorkloadKind,
        name: String,
        replicas: u32,
    },
    DeleteByLabel {
        namespace: String,
        selector: String,
        preserve_pvcs: Vec<String>,
    },
    DeletePods {
        namespace: String,
        selector: String,
    },
    Apply {
        manifest: serde_json::Value,
    },
    DeleteNamed {
        namespace: String,
        kind: String,
        name: String,
        opts: super::DeleteOpts,
    },
    DeploymentExists {
        namespace: String,
        name: String,
    },
    PodWaitingReason {
        namespace: String,
        pod_name: String,
    },
    ApplyYaml {
        manifest: String,
    },
    WaitRolloutStatus {
        namespace: String,
        deployment: String,
        timeout_seconds: u32,
    },
    DeleteNamespace {
        name: String,
    },
}

#[derive(Default)]
struct Inner {
    /// Keyed by namespace. Each entry is the full list of weft-managed
    /// workloads in that namespace (analogous to what `kubectl get`
    /// would return).
    workloads: HashMap<String, Vec<WorkloadReplicaState>>,
    /// Per-(namespace, pod_name) container waiting reason. Empty =
    /// no reason (running / not waiting). Seeded by tests via
    /// `set_pod_waiting_reason`.
    pod_waiting_reasons: HashMap<(String, String), String>,
    /// When > 0, the next N `apply` / `apply_yaml` calls return an
    /// error (still recorded in the log). Lets tests exercise the
    /// apply-failure branch. Decremented per failed call.
    fail_applies: u32,
    /// When true, `delete_pods` records the call then never returns
    /// (awaits `pending()`). Lets tests exercise a hung-action path
    /// (e.g. the HealthProtocol action timeout). Sticky.
    hang_delete_pods: bool,
    /// Append-only call log.
    calls: Vec<KubeCall>,
}

pub struct FakeKube {
    inner: Mutex<Inner>,
}

impl FakeKube {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner: Mutex::new(Inner::default()),
        })
    }

    // ---------- seeding ----------

    /// Replace the workload list for a namespace. The supervisor's
    /// next `list_replica_state` call returns this.
    pub fn set_workloads(&self, namespace: &str, workloads: Vec<WorkloadReplicaState>) {
        self.inner
            .lock()
            .workloads
            .insert(namespace.to_string(), workloads);
    }

    /// Seed a container-waiting reason for a pod. The next
    /// `pod_waiting_reason(namespace, pod_name)` returns it. Used
    /// to exercise the worker spawn's ImagePullBackOff detection.
    pub fn set_pod_waiting_reason(&self, namespace: &str, pod_name: &str, reason: &str) {
        self.inner.lock().pod_waiting_reasons.insert(
            (namespace.to_string(), pod_name.to_string()),
            reason.to_string(),
        );
    }

    /// Make the next `apply` / `apply_yaml` return an error (still
    /// logged). Exercises apply-failure handling in callers.
    pub fn fail_next_apply(&self) {
        self.inner.lock().fail_applies += 1;
    }

    /// Make `delete_pods` hang forever after recording the call.
    /// Exercises a hung-action timeout path in callers.
    pub fn hang_delete_pods(&self) {
        self.inner.lock().hang_delete_pods = true;
    }

    // ---------- assertions ----------

    /// All recorded calls in order.
    pub fn calls(&self) -> Vec<KubeCall> {
        self.inner.lock().calls.clone()
    }

    /// Only `Scale` calls, in order.
    /// Returns `(namespace, kind, name, replicas)`.
    pub fn scale_calls(&self) -> Vec<(String, WorkloadKind, String, u32)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                KubeCall::Scale {
                    namespace,
                    kind,
                    name,
                    replicas,
                } => Some((namespace.clone(), *kind, name.clone(), *replicas)),
                _ => None,
            })
            .collect()
    }

    /// Only `Apply` manifests, in order.
    pub fn applied_manifests(&self) -> Vec<serde_json::Value> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                KubeCall::Apply { manifest } => Some(manifest.clone()),
                _ => None,
            })
            .collect()
    }

    /// Only `DeleteByLabel` calls, in order.
    pub fn delete_calls(&self) -> Vec<(String, String, Vec<String>)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                KubeCall::DeleteByLabel {
                    namespace,
                    selector,
                    preserve_pvcs,
                } => Some((namespace.clone(), selector.clone(), preserve_pvcs.clone())),
                _ => None,
            })
            .collect()
    }
}

impl Default for FakeKube {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl KubeReader for FakeKube {
    async fn list_replica_state(
        &self,
        namespace: &str,
        selector: &str,
    ) -> Result<Vec<WorkloadReplicaState>> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::ListReplicaState {
            namespace: namespace.to_string(),
            selector: selector.to_string(),
        });
        Ok(inner.workloads.get(namespace).cloned().unwrap_or_default())
    }

    async fn deployment_exists(&self, namespace: &str, name: &str) -> super::DeploymentLookup {
        use super::DeploymentLookup;
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::DeploymentExists {
            namespace: namespace.to_string(),
            name: name.to_string(),
        });
        let exists = inner
            .workloads
            .get(namespace)
            .map(|ws| {
                ws.iter()
                    .any(|w| w.name == name && w.kind == WorkloadKind::Deployment)
            })
            .unwrap_or(false);
        if exists {
            DeploymentLookup::Exists
        } else {
            DeploymentLookup::NotFound
        }
    }

    async fn pod_waiting_reason(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Option<String>> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::PodWaitingReason {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
        });
        Ok(inner
            .pod_waiting_reasons
            .get(&(namespace.to_string(), pod_name.to_string()))
            .cloned())
    }
}

#[async_trait]
impl KubeWriter for FakeKube {
    async fn scale_workload(
        &self,
        namespace: &str,
        kind: WorkloadKind,
        name: &str,
        replicas: u32,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::Scale {
            namespace: namespace.to_string(),
            kind,
            name: name.to_string(),
            replicas,
        });
        // Mirror the effect onto the in-memory workloads so a
        // subsequent list_replica_state reflects the scale.
        if let Some(ws) = inner.workloads.get_mut(namespace) {
            for w in ws.iter_mut() {
                if w.name == name && w.kind == kind {
                    w.desired = replicas as i64;
                    if replicas == 0 {
                        w.ready = 0;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete_by_label(
        &self,
        namespace: &str,
        selector: &str,
        preserve_pvcs: &[String],
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::DeleteByLabel {
            namespace: namespace.to_string(),
            selector: selector.to_string(),
            preserve_pvcs: preserve_pvcs.to_vec(),
        });
        // Remove matching workloads from the namespace map so a
        // subsequent list_replica_state doesn't see them. Selector
        // format here is `weft.dev/instance=<id>` or
        // `weft.dev/instance=<id>,weft.dev/unit=<u>`; we parse
        // minimally and filter.
        if let Some(ws) = inner.workloads.get_mut(namespace) {
            let needles = parse_selector(selector);
            ws.retain(|w| !label_matches(&w.labels, &needles));
        }
        Ok(())
    }

    async fn apply(&self, manifest: &serde_json::Value) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::Apply {
            manifest: manifest.clone(),
        });
        if inner.fail_applies > 0 {
            inner.fail_applies -= 1;
            anyhow::bail!("FakeKube: injected apply failure");
        }
        Ok(())
    }

    async fn delete_pods(&self, namespace: &str, selector: &str) -> Result<()> {
        // Pods-only delete: don't touch the workload list (the
        // Deployment/StatefulSet controllers respawn Pods with the
        // same spec, so the next observation should look the same).
        let hang = {
            let mut inner = self.inner.lock();
            inner.calls.push(KubeCall::DeletePods {
                namespace: namespace.to_string(),
                selector: selector.to_string(),
            });
            inner.hang_delete_pods
        };
        if hang {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn apply_yaml(&self, manifest: &str) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::ApplyYaml {
            manifest: manifest.to_string(),
        });
        if inner.fail_applies > 0 {
            inner.fail_applies -= 1;
            anyhow::bail!("FakeKube: injected apply failure");
        }
        Ok(())
    }

    async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.inner.lock().calls.push(KubeCall::DeleteNamespace {
            name: name.to_string(),
        });
        Ok(())
    }

    async fn wait_rollout_status(
        &self,
        namespace: &str,
        deployment: &str,
        timeout_seconds: u32,
    ) -> Result<()> {
        self.inner.lock().calls.push(KubeCall::WaitRolloutStatus {
            namespace: namespace.to_string(),
            deployment: deployment.to_string(),
            timeout_seconds,
        });
        Ok(())
    }

    async fn delete_named(
        &self,
        namespace: &str,
        kind: &str,
        name: &str,
        opts: super::DeleteOpts,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::DeleteNamed {
            namespace: namespace.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            opts,
        });
        // Mirror onto the workload list when applicable: Deployment
        // delete removes the row.
        let kind_matches = match kind {
            "deployment" | "Deployment" => Some(WorkloadKind::Deployment),
            "statefulset" | "StatefulSet" => Some(WorkloadKind::StatefulSet),
            _ => None,
        };
        if let Some(wk) = kind_matches {
            if let Some(ws) = inner.workloads.get_mut(namespace) {
                ws.retain(|w| !(w.name == name && w.kind == wk));
            }
        }
        Ok(())
    }

}

/// Parse the subset of the k8s label-selector grammar that the
/// fake supports: comma-separated `key=value` AND-of-equals.
///
/// Production kubectl supports `!=`, `in (...)`, `notin (...)`,
/// bare-key existence, and `!key` non-existence. The fake panics
/// on those rather than silently mismatching: tests should fail
/// loudly if they use grammar the fake can't honor, otherwise
/// they'd pass against the fake and break against real kubectl.
///
/// If you hit this panic, either (a) limit your selector to
/// `key=value,key=value` shape, or (b) extend the fake AND
/// document the new grammar here in lockstep.
fn parse_selector(s: &str) -> Vec<(String, String)> {
    assert!(
        !s.is_empty(),
        "FakeKube parse_selector: empty selector would match everything; \
         pass an explicit `key=value` filter instead"
    );
    s.split(',')
        .map(|kv| {
            let kv = kv.trim();
            assert!(
                !kv.contains("!=") && !kv.contains(" in ") && !kv.contains(" notin "),
                "FakeKube parse_selector: only 'k=v' AND-of-equals supported, got: {kv:?}. \
                 Extend the fake before using richer selector grammar."
            );
            assert!(
                !kv.starts_with('!') && kv.contains('='),
                "FakeKube parse_selector: bare-key existence checks not supported, got: {kv:?}"
            );
            let mut parts = kv.splitn(2, '=');
            let k = parts.next().expect("split has at least one part").trim();
            let v = parts.next().expect("'=' guaranteed by assert above").trim();
            (k.to_string(), v.to_string())
        })
        .collect()
}

fn label_matches(labels: &HashMap<String, String>, needles: &[(String, String)]) -> bool {
    needles
        .iter()
        .all(|(k, v)| labels.get(k).map(|s| s.as_str()) == Some(v.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workload(name: &str, instance: &str, unit: &str, desired: i64, ready: i64) -> WorkloadReplicaState {
        let mut labels = HashMap::new();
        labels.insert("weft.dev/instance".into(), instance.into());
        labels.insert("weft.dev/unit".into(), unit.into());
        WorkloadReplicaState {
            kind: WorkloadKind::Deployment,
            name: name.into(),
            namespace: "ns".into(),
            desired,
            ready,
            labels,
        }
    }

    #[tokio::test]
    async fn list_returns_seeded_workloads() {
        let k = FakeKube::new();
        k.set_workloads("ns", vec![workload("inst-a", "inst", "u", 1, 1)]);
        let result: &dyn KubeReader = &*k;
        let ws = result.list_replica_state("ns", "").await.unwrap();
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].name, "inst-a");
    }

    #[tokio::test]
    async fn scale_updates_workload_state() {
        let k = FakeKube::new();
        k.set_workloads("ns", vec![workload("inst-a", "inst", "u", 3, 3)]);
        let w: &dyn KubeWriter = &*k;
        w.scale_workload("ns", WorkloadKind::Deployment, "inst-a", 0)
            .await
            .unwrap();
        let r: &dyn KubeReader = &*k;
        let ws = r.list_replica_state("ns", "").await.unwrap();
        assert_eq!(ws[0].desired, 0);
        assert_eq!(ws[0].ready, 0);
    }

    #[tokio::test]
    async fn delete_by_label_removes_matching() {
        let k = FakeKube::new();
        k.set_workloads(
            "ns",
            vec![
                workload("a", "inst1", "u", 1, 1),
                workload("b", "inst2", "u", 1, 1),
            ],
        );
        let w: &dyn KubeWriter = &*k;
        w.delete_by_label("ns", "weft.dev/instance=inst1", &[])
            .await
            .unwrap();
        let r: &dyn KubeReader = &*k;
        let ws = r.list_replica_state("ns", "").await.unwrap();
        assert_eq!(ws.len(), 1);
        assert_eq!(ws[0].name, "b");
    }

    #[tokio::test]
    async fn calls_are_recorded_in_order() {
        let k = FakeKube::new();
        let w: &dyn KubeWriter = &*k;
        w.apply(&serde_json::json!({"kind": "Service"})).await.unwrap();
        w.scale_workload("ns", WorkloadKind::Deployment, "x", 2)
            .await
            .unwrap();
        let calls = k.calls();
        assert_eq!(calls.len(), 2);
        assert!(matches!(calls[0], KubeCall::Apply { .. }));
        assert!(matches!(calls[1], KubeCall::Scale { .. }));
    }

    #[tokio::test]
    async fn delete_pods_records_call_but_leaves_workloads() {
        let k = FakeKube::new();
        k.set_workloads("ns", vec![workload("a", "inst1", "u", 1, 1)]);
        let w: &dyn KubeWriter = &*k;
        w.delete_pods("ns", "weft.dev/instance=inst1")
            .await
            .unwrap();
        // Call log: the delete_pods call landed.
        assert!(k
            .calls()
            .iter()
            .any(|c| matches!(c, KubeCall::DeletePods { .. })));
        // Workload list unchanged: the controller respawns Pods, so
        // the next observation still sees the workload.
        let r: &dyn KubeReader = &*k;
        let ws = r.list_replica_state("ns", "").await.unwrap();
        assert_eq!(ws.len(), 1);
    }
}
