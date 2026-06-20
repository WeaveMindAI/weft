//! The supervisor's view of the broker. A consumer-side trait, so
//! adding a new broker endpoint that the dispatcher needs but the
//! supervisor doesn't doesn't pollute this surface.
//!
//! Production: `BrokerSupervisorClient` from `weft-broker-client`
//! implements this via HTTP.
//! Tests: `FakeBroker` (gated behind `test-helpers`) implements it
//! against in-memory state.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use weft_broker_client::client::BrokerSupervisorClient;
use weft_broker_client::protocol::{
    SupervisorCommandRow, SupervisorInfraNode, SupervisorProject,
};

#[async_trait]
pub trait BrokerSupervisorOps: Send + Sync {
    /// Sync this supervisor's project ownership and return the set it
    /// now owns: renew its existing exclusive `infra_owner` leases, claim
    /// up to `saturation` more unowned projects' infra, return the full
    /// owned set. The work loops act ONLY on the returned projects, so
    /// two supervisors never reconcile the same project.
    async fn sync_ownership(
        &self,
        pod_name: &str,
        mem_pressure: f64,
    ) -> Result<Vec<SupervisorProject>>;
    /// Pure read of the projects this pod owns (no claim/renew). The
    /// work loops iterate this; ownership breadth changes only via
    /// `sync_ownership` (the ownership tick).
    async fn owned_projects(&self, pod_name: &str) -> Result<Vec<SupervisorProject>>;
    async fn infra_nodes(&self, project_id: &str) -> Result<Vec<SupervisorInfraNode>>;
    async fn health_protocols(
        &self,
        project_id: &str,
    ) -> Result<Option<serde_json::Value>>;
    async fn claim_command(
        &self,
        claimer_pod: &str,
    ) -> Result<Option<SupervisorCommandRow>>;
    /// Record one typed infra_event. The kind + payload pair comes
    /// from the `InfraEvent` enum so writers can't typo the kind or
    /// drift the payload shape; see protocol.rs.
    async fn event_record(
        &self,
        project_id: &str,
        node_id: Option<&str>,
        event: weft_broker_client::protocol::InfraEvent,
    ) -> Result<i64>;
    /// Set the `infra_node.status` row.
    /// `command_id = Some(id)` for lifecycle-driven writes; the
    /// broker rejects the UPDATE if the command is no longer
    /// claimed by the caller's pod (returns `WriteOutcome::Raced`).
    /// `command_id = None` for the health loop's autonomous
    /// Flaky/Running reconciliation (tenant scope still applies).
    /// `unit = Some` sets that unit's status (and recomputes the node
    /// rollup); `None` sets the node + all units (a lifecycle-driven
    /// uniform transition).
    async fn set_status(
        &self,
        pod_name: &str,
        command_id: Option<i64>,
        project_id: &str,
        node_id: &str,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>>;
    /// Cascade-delete the node, gated on the caller still OWNING the
    /// project (via `pod_name` = the supervisor's claim id). `Raced`
    /// means ownership moved mid-Terminate; the supervisor aborts and
    /// leaves the command for the new owner.
    async fn remove_node(
        &self,
        pod_name: &str,
        project_id: &str,
        node_id: &str,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorRemoveNodeResponse>>;
    async fn command_complete(
        &self,
        pod_name: &str,
        command_id: i64,
        error: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorCommandCompleteResponse>>;
    async fn running_count(&self, project_id: &str) -> Result<i64>;
    /// True if a user infra action (any uncompleted
    /// infra_lifecycle_command: apply / stop / terminate) is in flight
    /// for the project. The health loop stands down while it holds so
    /// it never races a user action.
    async fn infra_command_in_flight(&self, project_id: &str) -> Result<bool>;
    /// Pre-apply commitment. Writes the infra_node row at
    /// Provisioning status with the locked-in (instance_id,
    /// namespace, preserve_pvcs) tuple. Subsequent kubectl-apply
    /// failure leaves a visible row the user can Terminate;
    /// apply success flips Provisioning -> Running via set_applied.
    async fn set_provisioning(
        &self,
        pod_name: &str,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetProvisioningResponse>>;

    /// Post-apply state write. Gated on the caller still OWNING the
    /// project (via `pod_name`): a displaced pod can't resurrect a
    /// `remove_node`d row or stamp over the new owner's apply.
    async fn set_applied(
        &self,
        pod_name: &str,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        applied_spec_hash: &str,
        endpoints: BTreeMap<String, String>,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetAppliedResponse>>;
    async fn project_image_tags(
        &self,
        project_id: &str,
        node_id: &str,
    ) -> Result<HashMap<String, String>>;
    /// Enqueue a dispatcher-targeted lifecycle command. The typed
    /// `LifecycleSpec` only constructs `Deactivate(...)` /
    /// `Reactivate`, so the supervisor cannot accidentally enqueue
    /// a supervisor-owned verb. Used by HealthProtocol action
    /// dispatch.
    async fn enqueue_lifecycle(
        &self,
        project_id: &str,
        spec: weft_broker_client::protocol::LifecycleSpec,
    ) -> Result<i64>;
}

#[async_trait]
impl BrokerSupervisorOps for BrokerSupervisorClient {
    async fn sync_ownership(
        &self,
        pod_name: &str,
        mem_pressure: f64,
    ) -> Result<Vec<SupervisorProject>> {
        BrokerSupervisorClient::sync_ownership(self, pod_name, mem_pressure).await
    }
    async fn owned_projects(&self, pod_name: &str) -> Result<Vec<SupervisorProject>> {
        BrokerSupervisorClient::owned_projects(self, pod_name).await
    }
    async fn infra_nodes(&self, project_id: &str) -> Result<Vec<SupervisorInfraNode>> {
        BrokerSupervisorClient::infra_nodes(self, project_id).await
    }
    async fn health_protocols(
        &self,
        project_id: &str,
    ) -> Result<Option<serde_json::Value>> {
        BrokerSupervisorClient::health_protocols(self, project_id).await
    }
    async fn claim_command(
        &self,
        claimer_pod: &str,
    ) -> Result<Option<SupervisorCommandRow>> {
        BrokerSupervisorClient::claim_command(self, claimer_pod).await
    }
    async fn event_record(
        &self,
        project_id: &str,
        node_id: Option<&str>,
        event: weft_broker_client::protocol::InfraEvent,
    ) -> Result<i64> {
        BrokerSupervisorClient::event_record(self, project_id, node_id, event).await
    }
    async fn set_status(
        &self,
        pod_name: &str,
        command_id: Option<i64>,
        project_id: &str,
        node_id: &str,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>> {
        BrokerSupervisorClient::set_status(
            self,
            pod_name,
            command_id,
            project_id,
            node_id,
            unit,
            status,
            failure_stage,
            failure_message,
        )
        .await
    }
    async fn remove_node(
        &self,
        pod_name: &str,
        project_id: &str,
        node_id: &str,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorRemoveNodeResponse>> {
        BrokerSupervisorClient::remove_node(self, pod_name, project_id, node_id).await
    }
    async fn command_complete(
        &self,
        pod_name: &str,
        command_id: i64,
        error: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorCommandCompleteResponse>> {
        BrokerSupervisorClient::command_complete(self, pod_name, command_id, error).await
    }
    async fn running_count(&self, project_id: &str) -> Result<i64> {
        BrokerSupervisorClient::running_count(self, project_id).await
    }
    async fn infra_command_in_flight(&self, project_id: &str) -> Result<bool> {
        BrokerSupervisorClient::infra_command_in_flight(self, project_id).await
    }
    async fn set_provisioning(
        &self,
        pod_name: &str,
        command_id: i64,
        project_id: &str,
        node_id: &str,
        instance_id: &str,
        namespace: &str,
        preserve_pvcs: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetProvisioningResponse>> {
        BrokerSupervisorClient::set_provisioning(
            self,
            pod_name,
            command_id,
            project_id,
            node_id,
            instance_id,
            namespace,
            preserve_pvcs,
            units,
        )
        .await
    }
    async fn set_applied(
        &self,
        pod_name: &str,
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
        BrokerSupervisorClient::set_applied(
            self,
            pod_name,
            command_id,
            project_id,
            node_id,
            instance_id,
            applied_spec_hash,
            endpoints,
            namespace,
            preserve_pvcs,
            units,
        )
        .await
    }
    async fn project_image_tags(
        &self,
        project_id: &str,
        node_id: &str,
    ) -> Result<HashMap<String, String>> {
        BrokerSupervisorClient::project_image_tags(self, project_id, node_id).await
    }
    async fn enqueue_lifecycle(
        &self,
        project_id: &str,
        spec: weft_broker_client::protocol::LifecycleSpec,
    ) -> Result<i64> {
        BrokerSupervisorClient::enqueue_lifecycle(self, project_id, spec).await
    }
}

/// Convenience for binaries: take a real `BrokerSupervisorClient` and
/// hand back a trait object. Saves wiring noise in `main.rs`.
pub fn production(client: Arc<BrokerSupervisorClient>) -> Arc<dyn BrokerSupervisorOps> {
    client
}

// ---------- fake ----------

#[cfg(any(test, feature = "test-helpers"))]
mod fake;

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::{BrokerCall, FakeBroker};
