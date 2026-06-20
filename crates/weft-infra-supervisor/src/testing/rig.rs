use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;

use weft_platform_traits::clock::FakeClock;
use weft_platform_traits::kube::FakeKube;
use weft_platform_traits::mem_pressure::FakeMemPressure;

use crate::broker_ops::FakeBroker;
use crate::{health, lifecycle, ownership, SupervisorState};

/// Drives the supervisor's three loops against in-memory fakes.
/// Construct with `new()`; seed dependencies via the public
/// `broker`/`kube`/`clock` handles; step via `tick_ownership` /
/// `tick_health` / `tick_lifecycle`.
pub struct SupervisorTestRig {
    pub broker: Arc<FakeBroker>,
    pub kube: Arc<FakeKube>,
    pub clock: Arc<FakeClock>,
    /// Settable memory pressure: tests drive the supervisor over the
    /// saturation threshold via `mem.set(...)`.
    pub mem: Arc<FakeMemPressure>,
    pub state: SupervisorState,
}

impl SupervisorTestRig {
    pub fn new() -> Self {
        Self::with_tenant("test-tenant")
    }

    /// `tenant_id` seeds the FakeBroker's default tenant (stamped on
    /// projects it seeds). The pooled supervisor itself is tenant-
    /// agnostic; this only controls what tenant the fake's projects
    /// claim to belong to.
    pub fn with_tenant(tenant_id: &str) -> Self {
        let broker = FakeBroker::new(tenant_id);
        let kube = FakeKube::new();
        let clock = FakeClock::new();
        let mem = FakeMemPressure::new(0.0);
        let state = SupervisorState {
            broker: broker.clone() as Arc<dyn crate::broker_ops::BrokerSupervisorOps>,
            pod_name: "test-pod".to_string(),
            kube: kube.clone() as Arc<dyn weft_platform_traits::kube::KubeClient>,
            clock: clock.clone() as Arc<dyn weft_platform_traits::clock::Clock>,
            poll_interval: Duration::from_secs(5),
            health: Arc::new(Mutex::new(health::HealthRegistry::default())),
            mem_pressure: mem.clone() as Arc<dyn weft_platform_traits::mem_pressure::MemPressure>,
        };
        Self {
            broker,
            kube,
            clock,
            mem,
            state,
        }
    }

    /// Step the ownership loop once (renew + claim owned projects).
    pub async fn tick_ownership(&self) -> Result<()> {
        ownership::tick(&self.state).await
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
