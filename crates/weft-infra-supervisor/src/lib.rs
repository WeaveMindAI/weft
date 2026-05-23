//! Library surface for the per-tenant infra supervisor.
//!
//! The binary entry point (`main.rs`) is a thin wrapper that parses
//! args, constructs a `SupervisorState` against production
//! dependencies, and spawns the two loops. Everything else lives
//! here so integration tests under `tests/` can wire the same
//! loops against fakes from `weft-platform-traits` + this crate's
//! own `FakeBroker`.

use std::sync::Arc;
use std::time::Duration;

use weft_platform_traits::clock::Clock;
use weft_platform_traits::kube::KubeClient;

pub mod broker_ops;
pub mod health;
pub mod health_engine;
pub mod lifecycle;
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
    pub tenant_id: String,
    pub pod_name: String,
    pub kube: Arc<dyn KubeClient>,
    pub clock: Arc<dyn Clock>,
    pub poll_interval: Duration,
    pub health: Arc<tokio::sync::Mutex<health::HealthRegistry>>,
}
