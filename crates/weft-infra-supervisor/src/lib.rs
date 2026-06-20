//! Library surface for the pooled infra supervisor.
//!
//! A supervisor pod is tenant-agnostic: it owns the infrastructure of
//! a SET of projects (the exclusive `infra_owner` lease, claimed +
//! renewed by the ownership loop) and reconciles only those. The
//! dispatcher's `SupervisorPool` scales the number of supervisor pods
//! up and down by load.
//!
//! The binary entry point (`main.rs`) is a thin wrapper that parses
//! args, constructs a `SupervisorState` against production
//! dependencies, and spawns the three loops (ownership, lifecycle,
//! health). Everything else lives here so integration tests under
//! `tests/` can wire the same loops against fakes from
//! `weft-platform-traits` + this crate's own `FakeBroker`.

use std::sync::Arc;
use std::time::Duration;

use weft_platform_traits::clock::Clock;
use weft_platform_traits::kube::KubeClient;
use weft_platform_traits::mem_pressure::MemPressure;

pub mod broker_ops;
pub mod health;
pub mod health_engine;
pub mod lifecycle;
pub mod ownership;
pub mod protocol;

#[cfg(any(test, feature = "test-helpers"))]
pub mod testing;

/// Cloneable per-pod state threaded through the two loops.
///
/// All external dependencies are behind trait objects so tests can
/// swap them for fakes. Production wires `BrokerSupervisorClient`,
/// `KubectlClient`, and `SystemClock`.
#[derive(Clone)]
pub struct SupervisorState {
    pub broker: Arc<dyn broker_ops::BrokerSupervisorOps>,
    /// This pooled supervisor pod's name (`WEFT_POD_NAME` = the
    /// Deployment name). Identifies its command claims AND keys its
    /// `infra_owner` lease; sent on every broker write so the broker's
    /// ownership gate compares the lease against THIS, not the auth
    /// token's (suffixed) pod name. A pooled supervisor has no tenant of
    /// its own; it reconciles all tenants' namespaced projects, taking
    /// each project's tenant from the project row.
    // SYNC: supervisor pod_name <-> crates/weft-broker-client/src/protocol.rs (Supervisor*Request.pod_name), crates/weft-dispatcher/src/supervisor_pool.rs (render_supervisor_manifest WEFT_POD_NAME env), crates/weft-broker-client/src/lifecycle_command.rs (owns_project_predicate)
    pub pod_name: String,
    pub kube: Arc<dyn KubeClient>,
    pub clock: Arc<dyn Clock>,
    pub poll_interval: Duration,
    pub health: Arc<tokio::sync::Mutex<health::HealthRegistry>>,
    /// Reads this pod's real memory pressure, reported to the broker on
    /// each ownership tick. Saturation (the claim gate) and the
    /// dispatcher's placement both key on it, the SAME metric the
    /// listener uses.
    pub mem_pressure: Arc<dyn MemPressure>,
}
