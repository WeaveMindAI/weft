//! In-memory `BrokerSupervisorOps` for tests.
//!
//! Mirrors what the broker's Postgres state would look like:
//! projects keyed by id, infra_nodes keyed by (project_id, node_id),
//! a pending lifecycle command queue, etc. Reads pull from the
//! state-of-the-world maps; writes update them AND append to a
//! call log so tests can assert ordering.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

use anyhow::Result;
use async_trait::async_trait;

use weft_broker_client::protocol::{
    SupervisorCommandRow, SupervisorInfraNode, SupervisorProject,
};

use super::BrokerSupervisorOps;

/// A `UnitRuntime` at the given status with default windows +
/// ScaleToZero. Seeds the fake's per-unit roster in tests.
fn unit_runtime(
    status: weft_broker_client::protocol::InfraNodeStatus,
) -> weft_broker_client::protocol::UnitRuntime {
    weft_broker_client::protocol::UnitRuntime {
        status,
        stop_behavior: weft_core::StopBehavior::ScaleToZero,
        flaky_after_seconds: 30,
        recovery_after_seconds: 30,
    }
}

/// One recorded broker call. Used for ordering / argument
/// assertions in tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerCall {
    ProjectsForTenant {
        tenant_id: String,
    },
    InfraNodes {
        project_id: String,
    },
    HealthProtocols {
        project_id: String,
    },
    ClaimCommand {
        tenant_id: String,
        claimer_pod: String,
    },
    EventRecord {
        project_id: String,
        node_id: Option<String>,
        kind: String,
        payload: serde_json::Value,
    },
    SetStatus {
        command_id: Option<i64>,
        project_id: String,
        node_id: String,
        unit: Option<String>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<String>,
    },
    RemoveNode {
        project_id: String,
        node_id: String,
    },
    CommandComplete {
        command_id: i64,
        error: Option<String>,
    },
    RunningCount {
        project_id: String,
    },
    InfraCommandInFlight {
        project_id: String,
    },
    SetProvisioning {
        command_id: i64,
        project_id: String,
        node_id: String,
        instance_id: String,
        namespace: String,
        preserve_pvcs: Vec<String>,
    },
    SetApplied {
        command_id: i64,
        project_id: String,
        node_id: String,
        instance_id: String,
        applied_spec_hash: String,
        endpoints: BTreeMap<String, String>,
        namespace: String,
        preserve_pvcs: Vec<String>,
    },
    ProjectImageTags {
        project_id: String,
        node_id: String,
    },
    EnqueueLifecycle {
        project_id: String,
        spec: weft_broker_client::protocol::LifecycleSpec,
    },
}

#[derive(Default)]
struct Inner {
    /// All projects under this tenant. Keyed by project_id.
    projects: HashMap<String, SupervisorProject>,
    /// Tenant id this fake is scoped to. Set on construction;
    /// `projects_for_tenant(other)` returns empty.
    tenant_id: String,

    /// Infra nodes keyed by (project_id, node_id).
    infra_nodes: HashMap<(String, String), SupervisorInfraNode>,

    /// Health protocols JSON per project. `None` entries return
    /// `Ok(None)` (caller falls back to `default_protocols`).
    health_protocols: HashMap<String, Option<serde_json::Value>>,

    /// Pending lifecycle commands for this tenant. `claim_command`
    /// pops the front; tests push via `enqueue_command`.
    pending_commands: VecDeque<SupervisorCommandRow>,

    /// Per-project running execution count returned by `running_count`.
    running_counts: HashMap<String, i64>,

    /// Per-project "a user infra action is in flight" flag returned by
    /// `infra_command_in_flight`. Absent = false.
    infra_commands_in_flight: HashMap<String, bool>,

    /// Per-(project, node) image tag map returned by `project_image_tags`.
    image_tags: HashMap<(String, String), HashMap<String, String>>,

    /// Completed lifecycle commands (id -> optional error message).
    completed_commands: Vec<(i64, Option<String>)>,

    calls: Vec<BrokerCall>,
}

pub struct FakeBroker {
    inner: Mutex<Inner>,
}

impl FakeBroker {
    pub fn new(tenant_id: &str) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                tenant_id: tenant_id.to_string(),
                ..Default::default()
            }),
        })
    }

    // ---------- seeding ----------

    pub fn add_project(&self, project_id: &str, project_namespace: &str) {
        self.add_project_with_status(
            project_id,
            project_namespace,
            weft_broker_client::protocol::ProjectStatus::Active,
        )
    }

    /// Same as `add_project` but with an explicit status. Used by
    /// tests that exercise `HealthCondition::ProjectStatusEq`.
    pub fn add_project_with_status(
        &self,
        project_id: &str,
        project_namespace: &str,
        status: weft_broker_client::protocol::ProjectStatus,
    ) {
        self.inner.lock().projects.insert(
            project_id.to_string(),
            SupervisorProject {
                project_id: project_id.to_string(),
                project_namespace: project_namespace.to_string(),
                status,
            },
        );
    }

    /// Seed an infra_node row. Pass `endpoints` as you'd expect a
    /// post-apply state: a non-empty map for `running`, empty for
    /// `provisioning` / `stopped`.
    /// Seed a node with ONE unit (named after `node_id`) at `status`.
    /// Tests label their single workload `weft.dev/unit = node_id`, so
    /// this gives the health loop a one-unit roster matching them.
    pub fn add_infra_node(
        &self,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        status: weft_broker_client::protocol::InfraNodeStatus,
    ) {
        let mut units = BTreeMap::new();
        units.insert(node_id.to_string(), unit_runtime(status));
        self.add_infra_node_with(
            project_id,
            node_id,
            instance_id,
            status,
            None,
            BTreeMap::new(),
            units,
        );
    }

    /// Seed a multi-unit node: `units` maps unit name -> status.
    pub fn add_infra_node_units(
        &self,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        units: &[(&str, weft_broker_client::protocol::InfraNodeStatus)],
    ) {
        let units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime> = units
            .iter()
            .map(|(u, s)| (u.to_string(), unit_runtime(*s)))
            .collect();
        let status =
            weft_broker_client::protocol::InfraNodeStatus::rollup(units.values().map(|u| &u.status));
        self.add_infra_node_with(
            project_id,
            node_id,
            instance_id,
            status,
            None,
            BTreeMap::new(),
            units,
        );
    }

    pub fn add_infra_node_with(
        &self,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        status: weft_broker_client::protocol::InfraNodeStatus,
        applied_spec_hash: Option<String>,
        endpoints: BTreeMap<String, String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) {
        self.inner.lock().infra_nodes.insert(
            (project_id.to_string(), node_id.to_string()),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                instance_id: instance_id.to_string(),
                status,
                applied_spec_hash,
                endpoints,
                preserve_pvcs: Vec::new(),
                units,
            },
        );
    }

    pub fn set_health_protocols(&self, project_id: &str, protocols: serde_json::Value) {
        self.inner
            .lock()
            .health_protocols
            .insert(project_id.to_string(), Some(protocols));
    }

    pub fn enqueue_command(&self, cmd: SupervisorCommandRow) {
        self.inner.lock().pending_commands.push_back(cmd);
    }

    pub fn set_running_count(&self, project_id: &str, n: i64) {
        self.inner
            .lock()
            .running_counts
            .insert(project_id.to_string(), n);
    }

    pub fn set_infra_command_in_flight(&self, project_id: &str, in_flight: bool) {
        self.inner
            .lock()
            .infra_commands_in_flight
            .insert(project_id.to_string(), in_flight);
    }

    pub fn set_image_tags(
        &self,
        project_id: &str,
        node_id: &str,
        tags: HashMap<String, String>,
    ) {
        self.inner
            .lock()
            .image_tags
            .insert((project_id.to_string(), node_id.to_string()), tags);
    }

    // ---------- introspection ----------

    pub fn calls(&self) -> Vec<BrokerCall> {
        self.inner.lock().calls.clone()
    }

    /// All `event_record` calls in order, returned as
    /// `(project_id, node_id, kind, payload)` for ergonomic
    /// pattern-matching in tests.
    pub fn events(&self) -> Vec<(String, Option<String>, String, serde_json::Value)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                BrokerCall::EventRecord {
                    project_id,
                    node_id,
                    kind,
                    payload,
                } => Some((
                    project_id.clone(),
                    node_id.clone(),
                    kind.clone(),
                    payload.clone(),
                )),
                _ => None,
            })
            .collect()
    }

    /// All `set_status` calls in order.
    pub fn status_writes(
        &self,
    ) -> Vec<(String, String, weft_broker_client::protocol::InfraNodeStatus)> {
        self.inner
            .lock()
            .calls
            .iter()
            .filter_map(|c| match c {
                BrokerCall::SetStatus {
                    project_id,
                    node_id,
                    status,
                    ..
                } => Some((project_id.clone(), node_id.clone(), *status)),
                _ => None,
            })
            .collect()
    }

    /// Look up an infra_node's current state (what the supervisor
    /// would observe on the next infra_nodes call).
    pub fn infra_node(&self, project_id: &str, node_id: &str) -> Option<SupervisorInfraNode> {
        self.inner
            .lock()
            .infra_nodes
            .get(&(project_id.to_string(), node_id.to_string()))
            .cloned()
    }

    /// `command_complete` calls that recorded an error.
    pub fn failed_commands(&self) -> Vec<(i64, String)> {
        self.inner
            .lock()
            .completed_commands
            .iter()
            .filter_map(|(id, err)| err.as_ref().map(|e| (*id, e.clone())))
            .collect()
    }

    pub fn completed_commands(&self) -> Vec<(i64, Option<String>)> {
        self.inner.lock().completed_commands.clone()
    }
}

impl Default for FakeBroker {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
        }
    }
}

#[async_trait]
impl BrokerSupervisorOps for FakeBroker {
    async fn projects_for_tenant(&self, tenant_id: &str) -> Result<Vec<SupervisorProject>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::ProjectsForTenant {
            tenant_id: tenant_id.to_string(),
        });
        if inner.tenant_id != tenant_id {
            return Ok(Vec::new());
        }
        Ok(inner.projects.values().cloned().collect())
    }

    async fn infra_nodes(&self, project_id: &str) -> Result<Vec<SupervisorInfraNode>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::InfraNodes {
            project_id: project_id.to_string(),
        });
        Ok(inner
            .infra_nodes
            .iter()
            .filter_map(|((p, _), n)| (p == project_id).then(|| n.clone()))
            .collect())
    }

    async fn health_protocols(
        &self,
        project_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::HealthProtocols {
            project_id: project_id.to_string(),
        });
        Ok(inner
            .health_protocols
            .get(project_id)
            .cloned()
            .unwrap_or(None))
    }

    async fn claim_command(
        &self,
        tenant_id: &str,
        claimer_pod: &str,
    ) -> Result<Option<SupervisorCommandRow>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::ClaimCommand {
            tenant_id: tenant_id.to_string(),
            claimer_pod: claimer_pod.to_string(),
        });
        Ok(inner.pending_commands.pop_front())
    }

    async fn event_record(
        &self,
        project_id: &str,
        node_id: Option<&str>,
        event: weft_broker_client::protocol::InfraEvent,
    ) -> Result<i64> {
        let (kind, payload) = event.into_record();
        let mut inner = self.inner.lock();
        let id = inner.calls.len() as i64 + 1;
        inner.calls.push(BrokerCall::EventRecord {
            project_id: project_id.to_string(),
            node_id: node_id.map(|s| s.to_string()),
            kind: kind.as_str().to_string(),
            payload,
        });
        Ok(id)
    }

    async fn set_status(
        &self,
        command_id: Option<i64>,
        project_id: &str,
        node_id: &str,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetStatus {
            command_id,
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            unit: unit.map(|s| s.to_string()),
            status,
            failure_stage,
            failure_message: failure_message.map(|s| s.to_string()),
        });
        if let Some(node) = inner
            .infra_nodes
            .get_mut(&(project_id.to_string(), node_id.to_string()))
        {
            // Mirror prod: per-unit sets that unit then rolls up; node
            // -wide sets every unit. Then `status` = rollup of units.
            match unit {
                Some(u) => {
                    if let Some(ur) = node.units.get_mut(u) {
                        ur.status = status;
                    }
                }
                None => {
                    for ur in node.units.values_mut() {
                        ur.status = status;
                    }
                }
            }
            node.status = weft_broker_client::protocol::InfraNodeStatus::rollup(
                node.units.values().map(|u| &u.status),
            );
            Ok(weft_broker_client::WriteOutcome::Applied(
                weft_broker_client::protocol::SupervisorSetStatusResponse {},
            ))
        } else {
            // No row to update; the production broker returns 410.
            Ok(weft_broker_client::WriteOutcome::Raced)
        }
    }

    async fn remove_node(&self, project_id: &str, node_id: &str) -> Result<bool> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::RemoveNode {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
        });
        Ok(inner
            .infra_nodes
            .remove(&(project_id.to_string(), node_id.to_string()))
            .is_some())
    }

    async fn command_complete(
        &self,
        command_id: i64,
        error: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorCommandCompleteResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::CommandComplete {
            command_id,
            error: error.map(|s| s.to_string()),
        });
        inner
            .completed_commands
            .push((command_id, error.map(|s| s.to_string())));
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorCommandCompleteResponse {},
        ))
    }

    async fn running_count(&self, project_id: &str) -> Result<i64> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::RunningCount {
            project_id: project_id.to_string(),
        });
        Ok(inner.running_counts.get(project_id).copied().unwrap_or(0))
    }

    async fn infra_command_in_flight(&self, project_id: &str) -> Result<bool> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::InfraCommandInFlight {
            project_id: project_id.to_string(),
        });
        Ok(inner
            .infra_commands_in_flight
            .get(project_id)
            .copied()
            .unwrap_or(false))
    }

    async fn set_provisioning(
        &self,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetProvisioningResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetProvisioning {
            command_id,
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            instance_id: instance_id.to_string(),
            namespace: namespace.to_string(),
            preserve_pvcs: preserve_pvcs.clone(),
        });
        let status = weft_broker_client::protocol::InfraNodeStatus::rollup(
            units.values().map(|u| &u.status),
        );
        inner.infra_nodes.insert(
            (project_id.to_string(), node_id.to_string()),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                instance_id: instance_id.to_string(),
                status,
                applied_spec_hash: None,
                endpoints: BTreeMap::new(),
                preserve_pvcs,
                units,
            },
        );
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorSetProvisioningResponse {},
        ))
    }

    async fn set_applied(
        &self,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        applied_spec_hash: &str,
        endpoints: BTreeMap<String, String>,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetAppliedResponse>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::SetApplied {
            command_id,
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
            instance_id: instance_id.to_string(),
            applied_spec_hash: applied_spec_hash.to_string(),
            endpoints: endpoints.clone(),
            namespace: namespace.to_string(),
            preserve_pvcs: preserve_pvcs.clone(),
        });
        let status = weft_broker_client::protocol::InfraNodeStatus::rollup(
            units.values().map(|u| &u.status),
        );
        inner.infra_nodes.insert(
            (project_id.to_string(), node_id.to_string()),
            SupervisorInfraNode {
                node_id: node_id.to_string(),
                instance_id: instance_id.to_string(),
                status,
                applied_spec_hash: Some(applied_spec_hash.to_string()),
                endpoints,
                preserve_pvcs,
                units,
            },
        );
        Ok(weft_broker_client::WriteOutcome::Applied(
            weft_broker_client::protocol::SupervisorSetAppliedResponse {},
        ))
    }

    async fn project_image_tags(
        &self,
        project_id: &str,
        node_id: &str,
    ) -> Result<HashMap<String, String>> {
        let mut inner = self.inner.lock();
        inner.calls.push(BrokerCall::ProjectImageTags {
            project_id: project_id.to_string(),
            node_id: node_id.to_string(),
        });
        Ok(inner
            .image_tags
            .get(&(project_id.to_string(), node_id.to_string()))
            .cloned()
            .unwrap_or_default())
    }
    async fn enqueue_lifecycle(
        &self,
        project_id: &str,
        spec: weft_broker_client::protocol::LifecycleSpec,
    ) -> Result<i64> {
        let mut inner = self.inner.lock();
        let id = inner.calls.len() as i64 + 1;
        inner.calls.push(BrokerCall::EnqueueLifecycle {
            project_id: project_id.to_string(),
            spec,
        });
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn projects_for_tenant_returns_seeded() {
        let b = FakeBroker::new("alice");
        b.add_project("p1", "ns1");
        let projects = b.projects_for_tenant("alice").await.unwrap();
        assert_eq!(projects.len(), 1);
        assert_eq!(projects[0].project_id, "p1");
        assert_eq!(projects[0].project_namespace, "ns1");
    }

    #[tokio::test]
    async fn projects_for_other_tenant_returns_empty() {
        let b = FakeBroker::new("alice");
        b.add_project("p1", "ns1");
        let projects = b.projects_for_tenant("bob").await.unwrap();
        assert!(projects.is_empty());
    }

    #[tokio::test]
    async fn set_status_updates_in_place() {
        let b = FakeBroker::new("alice");
        b.add_infra_node(
            "p1",
            "n1",
            "inst1",
            weft_broker_client::protocol::InfraNodeStatus::Provisioning,
        );
        b.set_status(
            None,
            "p1",
            "n1",
            Some("n1"),
            weft_broker_client::protocol::InfraNodeStatus::Running,
            None,
            None,
        )
        .await
        .unwrap();
        let node = b.infra_node("p1", "n1").unwrap();
        assert_eq!(
            node.status,
            weft_broker_client::protocol::InfraNodeStatus::Running
        );
    }

    #[tokio::test]
    async fn remove_node_drops_row() {
        let b = FakeBroker::new("alice");
        b.add_infra_node(
            "p1",
            "n1",
            "inst1",
            weft_broker_client::protocol::InfraNodeStatus::Running,
        );
        let removed = b.remove_node("p1", "n1").await.unwrap();
        assert!(removed);
        assert!(b.infra_node("p1", "n1").is_none());
    }

    #[tokio::test]
    async fn claim_command_drains_queue() {
        let b = FakeBroker::new("alice");
        b.enqueue_command(SupervisorCommandRow {
            id: 1,
            project_id: "p1".into(),
            node_id: None,
            verb: weft_broker_client::protocol::InfraLifecycleVerb::Stop,
            running_policy: Some(weft_broker_client::protocol::RunningPolicy::Wait),
            spec_json: None,
            force: false,
        });
        let cmd1 = b.claim_command("alice", "pod1").await.unwrap();
        assert!(cmd1.is_some());
        let cmd2 = b.claim_command("alice", "pod1").await.unwrap();
        assert!(cmd2.is_none());
    }

    #[tokio::test]
    async fn events_are_introspectable() {
        let b = FakeBroker::new("alice");
        b.event_record(
            "p1",
            Some("n1"),
            weft_broker_client::protocol::InfraEvent::Flaky(
                weft_broker_client::protocol::FlakyPayload {
                    desired: 1,
                    ready: 0,
                    reason: None,
                },
            ),
        )
        .await
        .unwrap();
        b.event_record(
            "p1",
            Some("n1"),
            weft_broker_client::protocol::InfraEvent::Recovered,
        )
        .await
        .unwrap();
        let ev = b.events();
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].2, "flaky");
        assert_eq!(ev[1].2, "recovered");
    }
}
