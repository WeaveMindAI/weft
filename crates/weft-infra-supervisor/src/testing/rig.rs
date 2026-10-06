use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;

use weft_platform_traits::clock::FakeClock;
use weft_platform_traits::FakeInfraHost;

use crate::broker_ops::FakeBroker;
use crate::{health, lifecycle, ownership, SupervisorState};

/// Drives the supervisor's three loops against in-memory fakes.
/// Construct with `new()`; seed dependencies via the public
/// `broker`/`host`/`clock` handles; step via `tick_ownership` /
/// `tick_health` / `tick_lifecycle`.
pub struct SupervisorTestRig {
    pub broker: Arc<FakeBroker>,
    pub host: Arc<FakeInfraHost>,
    pub clock: Arc<FakeClock>,
    pub state: SupervisorState,
    /// What this supervisor owned after the last `tick_ownership`, kept
    /// across steps as the running ownership loop keeps it.
    pub owned: Mutex<std::collections::HashSet<uuid::Uuid>>,
}

impl SupervisorTestRig {
    pub fn new() -> Self {
        Self::with_tenant("test-tenant")
    }

    /// `tenant_id` seeds the FakeBroker's default tenant (stamped on
    /// projects it seeds). The supervisor itself is tenant-agnostic; this
    /// only controls what tenant the fake's projects claim to belong to.
    pub fn with_tenant(tenant_id: &str) -> Self {
        let broker = FakeBroker::new(tenant_id);
        let host = Arc::new(FakeInfraHost::new());
        let clock = FakeClock::new();
        let state = SupervisorState {
            broker: broker.clone() as Arc<dyn crate::broker_ops::BrokerSupervisorOps>,
            replica: "test-supervisor".to_string(),
            host: host.clone() as Arc<dyn weft_platform_traits::InfraHost>,
            clock: clock.clone() as Arc<dyn weft_platform_traits::clock::Clock>,
            ownership_interval: Duration::from_secs(15),
            health_interval: Duration::from_secs(30),
            health: Arc::new(Mutex::new(health::HealthRegistry::default())),
            ownership_wanted: Arc::new(tokio::sync::Notify::new()),
            project_locks: Arc::default(),
            pass: Arc::default(),
        };
        Self { broker, host, clock, state, owned: Mutex::new(std::collections::HashSet::new()) }
    }

    /// Step the ownership loop once (renew + claim owned projects) and
    /// hand back what changed, as the running loop sends it to the
    /// lifecycle loop.
    pub async fn tick_ownership(&self) -> Result<Option<ownership::OwnershipChange>> {
        Ok(ownership::tick(&self.state, &mut *self.owned.lock().await).await?.change)
    }

    /// Step the health loop once; whether anything it saw is unsettled.
    pub async fn tick_health(&self) -> Result<bool> {
        health::tick(&self.state).await
    }

    /// Hand the health loop one ownership change, as the running
    /// ownership loop does.
    pub async fn health_ownership_change(&self, change: &ownership::OwnershipChange) -> Result<()> {
        health::on_ownership_change(&self.state, change).await
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
