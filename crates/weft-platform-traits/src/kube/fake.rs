//! In-memory `KubeClient` for tests. Records every call, returns
//! seeded state from `list_replica_state`. Tests inject the
//! "current k8s state of the world" by calling `set_workloads`;
//! the supervisor's loops then observe whatever is in there, and every
//! open watch of that namespace is handed the new state.

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
    WatchReplicaState {
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
        kind: super::NamedKind,
        name: String,
        opts: super::DeleteOpts,
    },
    PodWaitingReason {
        namespace: String,
        pod_name: String,
    },
    PodPhase {
        namespace: String,
        pod_name: String,
    },
    PodLogs {
        namespace: String,
        pod_name: String,
        container: String,
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
    /// workloads in that namespace (what a list on the apiserver would
    /// return).
    workloads: HashMap<String, Vec<WorkloadReplicaState>>,
    /// Per-(namespace, pod_name) container waiting reason. Empty =
    /// no reason (running / not waiting). Seeded by tests via
    /// `set_pod_waiting_reason`.
    pod_waiting_reasons: HashMap<(String, String), String>,
    /// Per-(namespace, pod_name) pod phase, seeded via `set_pod_phase`.
    /// Absent = pod not visible (`pod_phase` answers `None`).
    pod_phases: HashMap<(String, String), String>,
    /// Per-(namespace, pod_name) pod logs, seeded via `set_pod_logs`.
    /// Absent = `pod_logs` errors ("no logs").
    pod_logs: HashMap<(String, String), String>,
    /// When > 0, the next N `apply` / `apply_yaml` calls return an
    /// error (still recorded in the log). Lets tests exercise the
    /// apply-failure branch. Decremented per failed call.
    fail_applies: u32,
    /// When true, `delete_pods` records the call then never returns
    /// (awaits `pending()`). Lets tests exercise a hung-action path
    /// (e.g. the HealthProtocol action timeout). Sticky.
    hang_delete_pods: bool,
    /// Open watches: the namespace, the selector's terms, and where
    /// each new set goes. A watch whose holder dropped it is pruned on
    /// the next send.
    watches: Vec<OpenWatch>,
    /// Namespaces whose new watches do not hand out the set as it is
    /// now: a watch that has not answered yet. Sticky.
    unanswered_namespaces: std::collections::HashSet<String>,
    /// Append-only call log.
    calls: Vec<KubeCall>,
}

/// One open watch: the namespace, the selector's terms, and where each
/// new set goes.
type OpenWatch = (String, Vec<(String, String)>, tokio::sync::mpsc::UnboundedSender<Result<Vec<WorkloadReplicaState>>>);

pub struct FakeKube {
    inner: Mutex<Inner>,
}

impl Inner {
    /// The workloads of `namespace` a selector's terms match.
    fn matching(&self, namespace: &str, needles: &[(String, String)]) -> Vec<WorkloadReplicaState> {
        self.workloads
            .get(namespace)
            .map(|ws| ws.iter().filter(|w| label_matches(&w.labels, needles)).cloned().collect())
            .unwrap_or_default()
    }

    /// Hand every open watch of `namespace` its set as it is now.
    fn send_to_watches(&mut self, namespace: &str) {
        self.watches.retain(|(_, _, tx)| !tx.is_closed());
        let sends: Vec<_> = self
            .watches
            .iter()
            .filter(|(ns, _, _)| ns == namespace)
            .map(|(ns, needles, tx)| (tx.clone(), self.matching(ns, needles)))
            .collect();
        for (tx, set) in sends {
            let _ = tx.send(Ok(set));
        }
    }
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
        let mut inner = self.inner.lock();
        inner.workloads.insert(namespace.to_string(), workloads);
        inner.send_to_watches(namespace);
    }

    /// Watches of `namespace` started from now on stay silent until the
    /// next `set_workloads` of it (they never hand out a first set).
    pub fn leave_watches_unanswered(&self, namespace: &str) {
        self.inner.lock().unanswered_namespaces.insert(namespace.to_string());
    }

    /// Hand every open watch of `namespace` a failed look.
    pub fn fail_watches(&self, namespace: &str, error: &str) {
        let mut inner = self.inner.lock();
        inner.watches.retain(|(_, _, tx)| !tx.is_closed());
        for (ns, _, tx) in &inner.watches {
            if ns == namespace {
                let _ = tx.send(Err(anyhow::anyhow!("{error}")));
            }
        }
    }

    /// How many watches are still held by somebody.
    pub fn live_watches(&self) -> usize {
        let mut inner = self.inner.lock();
        inner.watches.retain(|(_, _, tx)| !tx.is_closed());
        inner.watches.len()
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

    /// Seed a pod's `status.phase` for `pod_phase`.
    pub fn set_pod_phase(&self, namespace: &str, pod_name: &str, phase: &str) {
        self.inner
            .lock()
            .pod_phases
            .insert((namespace.to_string(), pod_name.to_string()), phase.to_string());
    }

    /// Seed a pod's logs for `pod_logs`.
    pub fn set_pod_logs(&self, namespace: &str, pod_name: &str, logs: &str) {
        self.inner
            .lock()
            .pod_logs
            .insert((namespace.to_string(), pod_name.to_string()), logs.to_string());
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
        // Honor the label selector the way the apiserver does for our
        // calls. Route through the SAME `parse_selector` the writer side
        // (`delete_by_label`) uses, so both paths reject unsupported grammar
        // identically: a selector/label MISMATCH (e.g. asking for `role=infra`
        // when the workload is labeled `role=infra-supervisor`) and a malformed
        // term both surface here instead of silently matching everything, which
        // is exactly the class of bug this fake must be able to catch. An empty
        // selector means "no filter" (match all), so handle it before
        // delegating (`parse_selector` loudly rejects empty for the writer).
        let needles = if selector.is_empty() {
            Vec::new()
        } else {
            parse_selector(selector)
        };
        Ok(inner.matching(namespace, &needles))
    }

    /// Hands out the set as it is now (unless the namespace was left
    /// unanswered), then again on every
    /// `set_workloads` of the namespace.
    async fn watch_replica_state(&self, namespace: &str, selector: &str) -> Result<super::ReplicaWatch> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::WatchReplicaState {
            namespace: namespace.to_string(),
            selector: selector.to_string(),
        });
        let needles = if selector.is_empty() { Vec::new() } else { parse_selector(selector) };
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if !inner.unanswered_namespaces.contains(namespace) {
            let _ = tx.send(Ok(inner.matching(namespace, &needles)));
        }
        inner.watches.push((namespace.to_string(), needles, tx));
        Ok(futures::StreamExt::boxed(futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|set| (set, rx))
        })))
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

    async fn pod_phase(&self, namespace: &str, pod_name: &str) -> Result<Option<String>> {
        let mut inner = self.inner.lock();
        // Recorded like every other call. Reading is a call: whether the
        // node-test executor polled at all, how often, and against which
        // pod are exactly the things a test of it would assert, and with
        // no record there is nothing to assert against.
        inner.calls.push(KubeCall::PodPhase {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
        });
        Ok(inner.pod_phases.get(&(namespace.to_string(), pod_name.to_string())).cloned())
    }

    async fn node_ports(&self) -> Result<Vec<super::NodePortHolder>> {
        // An untouched cluster. Nothing on the weft side picks these
        // numbers (the apiserver allocates them), so the only reader is
        // `weft infra list-doors` asking what is serving, and no test
        // fakes that ledger yet. A test that needs one seeds a field
        // here the way the other answers are seeded.
        Ok(Vec::new())
    }

    async fn pod_logs(&self, namespace: &str, pod_name: &str, container: &str) -> Result<String> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::PodLogs {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            container: container.to_string(),
        });
        inner
            .pod_logs
            .get(&(namespace.to_string(), pod_name.to_string()))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("no logs seeded for {namespace}/{pod_name}"))
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
        //
        // `desired` only. What is READY is the cluster's answer, arriving
        // whenever it arrives, and a fake that decides it has an opinion
        // about convergence. It used to zero `ready` on a scale to zero
        // and leave it alone otherwise, which is not a rule kubernetes
        // has: scaling 3 to 1 then read as "1 wanted, 3 ready" for ever,
        // a state no real cluster settles into. A test that wants a
        // settled workload seeds `ready` itself, the way it seeds every
        // other answer here.
        if let Some(ws) = inner.workloads.get_mut(namespace) {
            for w in ws.iter_mut() {
                if w.name == name && w.kind == kind {
                    w.desired = replicas as i64;
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
        kind: super::NamedKind,
        name: &str,
        opts: super::DeleteOpts,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.calls.push(KubeCall::DeleteNamed {
            namespace: namespace.to_string(),
            kind,
            name: name.to_string(),
            opts,
        });
        // Mirror onto the workload list when applicable: Deployment
        // delete removes the row.
        if kind == super::NamedKind::Deployment {
            if let Some(ws) = inner.workloads.get_mut(namespace) {
                ws.retain(|w| !(w.name == name && w.kind == WorkloadKind::Deployment));
            }
        }
        Ok(())
    }

}

/// Parse the subset of the k8s label-selector grammar that the
/// fake supports: comma-separated `key=value` AND-of-equals.
///
/// The apiserver supports `!=`, `in (...)`, `notin (...)`,
/// bare-key existence, and `!key` non-existence. The fake panics
/// on those rather than silently mismatching: tests should fail
/// loudly if they use grammar the fake can't honor, otherwise
/// they'd pass against the fake and break against the real apiserver.
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

    /// The whole point of routing `list_replica_state` through the selector
    /// filter: a selector whose value does not match the workload's label must
    /// return nothing, exactly as the real apiserver does. This pins the bug
    /// class where the reaper asked for `role=infra` while the supervisor is
    /// labeled `role=infra-supervisor`, which silently matched nothing in prod;
    /// the fake must reproduce that miss so a contract test can catch the drift.
    #[tokio::test]
    async fn list_filters_by_selector_value() {
        let mut labels = HashMap::new();
        labels.insert("weft.dev/role".into(), "infra-supervisor".into());
        let sup = WorkloadReplicaState {
            kind: WorkloadKind::Deployment,
            name: "weft-infra-supervisor".into(),
            namespace: "ns".into(),
            desired: 1,
            ready: 1,
            labels,
        };
        let k = FakeKube::new();
        k.set_workloads("ns", vec![sup]);
        let r: &dyn KubeReader = &*k;
        // Wrong value: the supervisor is `role=infra-supervisor`, not `role=infra`.
        let miss = r.list_replica_state("ns", "weft.dev/role=infra").await.unwrap();
        assert!(miss.is_empty(), "wrong selector value must match nothing");
        // Right value: matches.
        let hit = r
            .list_replica_state("ns", "weft.dev/role=infra-supervisor")
            .await
            .unwrap();
        assert_eq!(hit.len(), 1);
        // Empty selector means no filter: match all.
        let all = r.list_replica_state("ns", "").await.unwrap();
        assert_eq!(all.len(), 1);
    }

    /// A scale sets what is WANTED. What is ready is the cluster's
    /// answer and arrives when it arrives, so the fake does not invent
    /// one: a test that wants a settled workload seeds it, and a test
    /// about draining wants exactly the unsettled state in between.
    #[tokio::test]
    async fn scale_sets_what_is_wanted_and_does_not_invent_what_is_ready() {
        let k = FakeKube::new();
        k.set_workloads("ns", vec![workload("inst-a", "inst", "u", 3, 3)]);
        let w: &dyn KubeWriter = &*k;
        w.scale_workload("ns", WorkloadKind::Deployment, "inst-a", 1)
            .await
            .unwrap();
        let r: &dyn KubeReader = &*k;
        let ws = r.list_replica_state("ns", "").await.unwrap();
        assert_eq!(ws[0].desired, 1);
        assert_eq!(ws[0].ready, 3, "still three running: nothing has converged yet");
        k.set_workloads("ns", vec![workload("inst-a", "inst", "u", 1, 1)]);
        let ws = r.list_replica_state("ns", "").await.unwrap();
        assert_eq!(ws[0].ready, 1, "and the test says when it has");
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
