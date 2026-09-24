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
    /// The health loop's watches, kept across `tick_health` steps.
    pub watches: Mutex<health::Watches>,
    /// What this pod owned after the last `tick_ownership`, kept across
    /// steps as the running ownership loop keeps it.
    pub owned: Mutex<std::collections::HashSet<uuid::Uuid>>,
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
            ownership_interval: Duration::from_secs(15),
            health_interval: Duration::from_secs(30),
            health: Arc::new(Mutex::new(health::HealthRegistry::default())),
            mem_pressure: mem.clone() as Arc<dyn weft_platform_traits::mem_pressure::MemPressure>,
            ownership_wanted: Arc::new(tokio::sync::Notify::new()),
            install: weft_core::infra::Instance::default_install(),
        };
        Self {
            broker,
            kube,
            clock,
            mem,
            state,
            watches: Mutex::new(health::Watches::default()),
            owned: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Step the ownership loop once (renew + claim owned projects) and
    /// hand back what changed, as the running loop sends it to the
    /// lifecycle loop.
    pub async fn tick_ownership(&self) -> Result<Option<ownership::OwnershipChange>> {
        ownership::tick(&self.state, &mut *self.owned.lock().await).await
    }

    /// Step the health loop once. The rig holds the loop's watches
    /// across steps, as the running loop does.
    pub async fn tick_health(&self) -> Result<()> {
        health::tick(&self.state, &mut *self.watches.lock().await).await
    }

    /// Step the change-driven half of the health loop: wait for the next
    /// change a watch hands out, and evaluate its project.
    pub async fn health_change(&self) {
        health::on_change(&self.state, &mut *self.watches.lock().await).await
    }

    /// Run the between-ticks half of the health loop until `next_tick`
    /// resolves, as the running loop does. `changes` carries what the
    /// ownership loop would send it meanwhile.
    pub async fn health_between_ticks(
        &self,
        changes: &mut tokio::sync::mpsc::UnboundedReceiver<ownership::OwnershipChange>,
        next_tick: impl std::future::Future<Output = ()>,
    ) -> Result<()> {
        health::between_ticks(&self.state, &mut *self.watches.lock().await, changes, next_tick).await
    }

    /// Hand the health loop one ownership change, as the running
    /// ownership loop does.
    pub async fn health_ownership_change(&self, change: &ownership::OwnershipChange) -> Result<()> {
        health::on_ownership_change(&self.state, &mut *self.watches.lock().await, change).await
    }

    /// Run the real lifecycle loop in the background. Send it what
    /// `tick_ownership` hands back through the returned sender, as the
    /// running ownership loop does.
    pub fn spawn_lifecycle_loop(
        &self,
    ) -> (
        tokio::task::JoinHandle<Result<()>>,
        tokio::sync::mpsc::UnboundedSender<ownership::OwnershipChange>,
    ) {
        let (changes, changed) = tokio::sync::mpsc::unbounded_channel();
        let state = self.state.clone();
        (tokio::spawn(lifecycle::run_loop(state, changed)), changes)
    }

    /// Step the lifecycle loop once. Returns true if a command was
    /// claimed and executed.
    pub async fn tick_lifecycle(&self) -> Result<bool> {
        lifecycle::tick(&self.state, Duration::ZERO).await
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
