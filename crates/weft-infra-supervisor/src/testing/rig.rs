use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;

use weft_platform_traits::clock::FakeClock;
use weft_platform_traits::kube::FakeKube;

use crate::broker_ops::FakeBroker;
use crate::{health, lifecycle, SupervisorState};

/// Drives the supervisor's two loops against in-memory fakes.
/// Construct with `new()`; seed dependencies via the public
/// `broker`/`kube`/`clock` handles; step via `tick_health` /
/// `tick_lifecycle`.
pub struct SupervisorTestRig {
    pub broker: Arc<FakeBroker>,
    pub kube: Arc<FakeKube>,
    pub clock: Arc<FakeClock>,
    pub state: SupervisorState,
}

impl SupervisorTestRig {
    pub fn new() -> Self {
        Self::with_tenant("test-tenant")
    }

    pub fn with_tenant(tenant_id: &str) -> Self {
        let broker = FakeBroker::new(tenant_id);
        let kube = FakeKube::new();
        let clock = FakeClock::new();
        let state = SupervisorState {
            broker: broker.clone() as Arc<dyn crate::broker_ops::BrokerSupervisorOps>,
            tenant_id: tenant_id.to_string(),
            pod_name: "test-pod".to_string(),
            kube: kube.clone() as Arc<dyn weft_platform_traits::kube::KubeClient>,
            clock: clock.clone() as Arc<dyn weft_platform_traits::clock::Clock>,
            poll_interval: Duration::from_secs(5),
            health: Arc::new(Mutex::new(health::HealthRegistry::default())),
        };
        Self {
            broker,
            kube,
            clock,
            state,
        }
    }

    /// Step the health loop once.
    pub async fn tick_health(&self) -> Result<()> {
        health::tick(&self.state).await
    }

    /// Step the lifecycle loop once. Returns true if a command was
    /// claimed and executed.
    pub async fn tick_lifecycle(&self) -> Result<bool> {
        lifecycle::tick(&self.state).await
    }

    /// Advance the fake clock by `d`. Does not run any loop.
    pub fn advance(&self, d: Duration) {
        self.clock.advance(d);
    }
}

impl Default for SupervisorTestRig {
    fn default() -> Self {
        Self::new()
    }
}
