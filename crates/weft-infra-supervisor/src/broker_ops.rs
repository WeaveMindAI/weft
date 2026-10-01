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
    SupervisorClaim, SupervisorInfraNode, SupervisorProject, SupervisorSyncOwnershipResponse,
};

#[async_trait]
pub trait BrokerSupervisorOps: Send + Sync {
    /// Sync this supervisor's project ownership: renew its existing
    /// exclusive `infra_owner` leases, claim a batch more unowned
    /// projects' infra, and return the full owned set plus the projects
    /// this tick took on. The work loops act ONLY on the owned projects,
    /// so two supervisors never reconcile the same project.
    /// `held_projects` (every project the host holds a copy of) are
    /// claimed and renewed too, so the gone-copy sweep can judge them
    /// under their lease (see `SupervisorSyncOwnershipRequest`).
    async fn sync_ownership(&self, replica: &str, held_projects: &[uuid::Uuid]) -> Result<SupervisorSyncOwnershipResponse>;
    /// Pure read of the projects this supervisor owns (no claim/renew).
    /// The work loops iterate this; ownership breadth changes only via
    /// `sync_ownership` (the ownership tick).
    async fn owned_projects(&self, replica: &str) -> Result<Vec<SupervisorProject>>;
    /// Which of `copies` (all of `project`) are gone for good, as their
    /// copy ids, or `None` when this supervisor does not hold the
    /// project's lease and so may not judge them (see
    /// `SupervisorGoneCopiesRequest`).
    async fn gone_copies(
        &self,
        replica: &str,
        project: uuid::Uuid,
        copies: &[weft_core::infra::NodeRef],
    ) -> Result<Option<Vec<String>>>;
    async fn infra_nodes(&self, project_id: uuid::Uuid) -> Result<Vec<SupervisorInfraNode>>;
    async fn health_protocols(
        &self,
        project_id: uuid::Uuid,
    ) -> Result<Option<serde_json::Value>>;
    /// The oldest waiting command of a project this supervisor owns and is not
    /// already running a command for (`busy_projects`), holding up to
    /// `wait` for one to be issued when none is waiting.
    async fn claim_command(
        &self,
        claimer: &str,
        busy_projects: &[uuid::Uuid],
        wait: std::time::Duration,
    ) -> Result<SupervisorClaim>;
    /// Record one typed infra_event. The kind + payload pair comes
    /// from the `InfraEvent` enum so writers can't typo the kind or
    /// drift the payload shape; see protocol.rs.
    async fn event_record(
        &self,
        project_id: uuid::Uuid,
        node_id: Option<&str>,
        instance: Option<&weft_core::instance::InstanceId>,
        event: weft_broker_client::protocol::InfraEvent,
    ) -> Result<i64>;
    /// Set the `infra_node.status` row.
    /// `command_id = Some(id)` for lifecycle-driven writes. The broker
    /// refuses the UPDATE when the caller no longer owns the
    /// project (its `infra_owner` lease moved), answering
    /// `WriteOutcome::Displaced`.
    /// `command_id = None` for the health loop's autonomous
    /// Flaky/Running reconciliation (tenant scope still applies).
    /// `unit = Some` sets that unit's status (and recomputes the node
    /// rollup); `None` sets the node + all units (a lifecycle-driven
    /// uniform transition).
    async fn set_status(
        &self,
        replica: &str,
        command_id: Option<i64>,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>>;
    /// Cascade-delete the node, gated on the caller still OWNING the
    /// project (via `replica` = the supervisor's claim id). `Displaced`
    /// means ownership moved mid-Terminate; the supervisor aborts and
    /// leaves the command for the new owner.
    async fn remove_node(
        &self,
        replica: &str,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        command_id: i64,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorRemoveNodeResponse>>;
    /// `cancelled = true` records outcome `cancelled` (a user-honored
    /// cancel), never a failure; `error` then carries the halt point.
    async fn command_complete(
        &self,
        replica: &str,
        command_id: i64,
        error: Option<&str>,
        cancelled: bool,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorCommandCompleteResponse>>;
    /// Whether the user requested cancellation of a claimed command.
    /// Polled between platform steps and inside readiness/drain waits.
    async fn command_cancel_requested(&self, command_id: i64) -> Result<bool>;
    async fn running_count(&self, project_id: uuid::Uuid, copies: &weft_core::instance::Copies) -> Result<i64>;
    /// The project's uncompleted supervisor commands (apply / stop /
    /// terminate), each as the copies it acts on. The health loop stands
    /// down for those copies so it never races a user action.
    async fn infra_commands_in_flight(
        &self,
        project_id: uuid::Uuid,
    ) -> Result<Vec<weft_broker_client::protocol::InFlightCommand>>;
    /// Pre-apply commitment. Writes the infra_node row at Provisioning
    /// status with the locked-in (copy_id, keep_disks) pair.
    /// Subsequent apply failure leaves a visible row the user can
    /// Terminate; apply success flips Provisioning -> Running via
    /// set_applied.
    async fn set_provisioning(
        &self,
        replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        keep_disks: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetProvisioningResponse>>;

    /// Post-apply state write. Gated on the caller still OWNING the
    /// project (via `replica`): a displaced supervisor can't resurrect
    /// a `remove_node`d row or stamp over the new owner's apply.
    async fn set_applied(
        &self,
        replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        applied_spec_hash: &str,
        addresses: weft_broker_client::protocol::AppliedEndpoints,
        keep_disks: Vec<String>,
        notes: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetAppliedResponse>>;
    async fn project_image_tags(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
    ) -> Result<HashMap<String, String>>;
    /// Enqueue a dispatcher-targeted lifecycle command. The typed
    /// `LifecycleSpec` only constructs `Deactivate(...)` /
    /// `Reactivate`, so the supervisor cannot accidentally enqueue
    /// a supervisor-owned verb. Used by HealthProtocol action
    /// dispatch.
    async fn enqueue_lifecycle(
        &self,
        project_id: uuid::Uuid,
        spec: weft_broker_client::protocol::LifecycleSpec,
    ) -> Result<i64>;
}

#[async_trait]
impl BrokerSupervisorOps for BrokerSupervisorClient {
    async fn sync_ownership(&self, replica: &str, held_projects: &[uuid::Uuid]) -> Result<SupervisorSyncOwnershipResponse> {
        BrokerSupervisorClient::sync_ownership(self, replica, held_projects).await
    }
    async fn owned_projects(&self, replica: &str) -> Result<Vec<SupervisorProject>> {
        BrokerSupervisorClient::owned_projects(self, replica).await
    }
    async fn gone_copies(
        &self,
        replica: &str,
        project: uuid::Uuid,
        copies: &[weft_core::infra::NodeRef],
    ) -> Result<Option<Vec<String>>> {
        BrokerSupervisorClient::gone_copies(self, replica, project, copies).await
    }
    async fn infra_nodes(&self, project_id: uuid::Uuid) -> Result<Vec<SupervisorInfraNode>> {
        BrokerSupervisorClient::infra_nodes(self, project_id).await
    }
    async fn health_protocols(
        &self,
        project_id: uuid::Uuid,
    ) -> Result<Option<serde_json::Value>> {
        BrokerSupervisorClient::health_protocols(self, project_id).await
    }
    async fn claim_command(
        &self,
        claimer: &str,
        busy_projects: &[uuid::Uuid],
        wait: std::time::Duration,
    ) -> Result<SupervisorClaim> {
        BrokerSupervisorClient::claim_command(self, claimer, busy_projects, wait).await
    }
    async fn event_record(
        &self,
        project_id: uuid::Uuid,
        node_id: Option<&str>,
        instance: Option<&weft_core::instance::InstanceId>,
        event: weft_broker_client::protocol::InfraEvent,
    ) -> Result<i64> {
        BrokerSupervisorClient::event_record(self, project_id, node_id, instance, event).await
    }
    async fn set_status(
        &self,
        replica: &str,
        command_id: Option<i64>,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        unit: Option<&str>,
        status: weft_broker_client::protocol::InfraNodeStatus,
        failure_stage: Option<weft_broker_client::protocol::FailureStage>,
        failure_message: Option<&str>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetStatusResponse>> {
        BrokerSupervisorClient::set_status(
            self,
            replica,
            command_id,
            project_id,
            node_id,
            instance,
            unit,
            status,
            failure_stage,
            failure_message,
        )
        .await
    }
    async fn remove_node(
        &self,
        replica: &str,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        command_id: i64,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorRemoveNodeResponse>> {
        BrokerSupervisorClient::remove_node(self, replica, project_id, node_id, instance, command_id).await
    }
    async fn command_complete(
        &self,
        replica: &str,
        command_id: i64,
        error: Option<&str>,
        cancelled: bool,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorCommandCompleteResponse>> {
        BrokerSupervisorClient::command_complete(self, replica, command_id, error, cancelled)
            .await
    }

    async fn command_cancel_requested(&self, command_id: i64) -> Result<bool> {
        BrokerSupervisorClient::command_cancel_requested(self, command_id).await
    }
    async fn running_count(&self, project_id: uuid::Uuid, copies: &weft_core::instance::Copies) -> Result<i64> {
        BrokerSupervisorClient::running_count(self, project_id, copies).await
    }
    async fn infra_commands_in_flight(
        &self,
        project_id: uuid::Uuid,
    ) -> Result<Vec<weft_broker_client::protocol::InFlightCommand>> {
        BrokerSupervisorClient::infra_commands_in_flight(self, project_id).await
    }
    async fn set_provisioning(
        &self,
        replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        keep_disks: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetProvisioningResponse>> {
        BrokerSupervisorClient::set_provisioning(
            self,
            replica,
            command_id,
            project_id,
            node_id,
            instance,
            copy_id,
            keep_disks,
            units,
        )
        .await
    }
    async fn set_applied(
        &self,
        replica: &str,
        command_id: i64,
        project_id: uuid::Uuid,
        node_id: &str,
        instance: Option<&weft_core::instance::InstanceId>,
        copy_id: &str,
        applied_spec_hash: &str,
        addresses: weft_broker_client::protocol::AppliedEndpoints,
        keep_disks: Vec<String>,
        notes: Vec<String>,
        units: BTreeMap<String, weft_broker_client::protocol::UnitRuntime>,
    ) -> Result<weft_broker_client::WriteOutcome<weft_broker_client::protocol::SupervisorSetAppliedResponse>> {
        BrokerSupervisorClient::set_applied(
            self,
            replica,
            command_id,
            project_id,
            node_id,
            instance,
            copy_id,
            applied_spec_hash,
            addresses,
            keep_disks,
            notes,
            units,
        )
        .await
    }
    async fn project_image_tags(
        &self,
        project_id: uuid::Uuid,
        node_id: &str,
    ) -> Result<HashMap<String, String>> {
        BrokerSupervisorClient::project_image_tags(self, project_id, node_id).await
    }
    async fn enqueue_lifecycle(
        &self,
        project_id: uuid::Uuid,
        spec: weft_broker_client::protocol::LifecycleSpec,
    ) -> Result<i64> {
        BrokerSupervisorClient::enqueue_lifecycle(self, project_id, spec).await
    }
}

/// Convenience for binaries: take a real `BrokerSupervisorClient` and
/// hand back a trait object.
pub fn production(client: Arc<BrokerSupervisorClient>) -> Arc<dyn BrokerSupervisorOps> {
    client
}

// ---------- fake ----------

#[cfg(any(test, feature = "test-helpers"))]
mod fake;

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::{BrokerCall, FakeBroker};
