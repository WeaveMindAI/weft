//! Live-tier node-test composition: builds the [`weft_core::LiveRig`]
//! whose handle is the PRODUCTION [`RunnerHandle`], so a live test's
//! calls go through the real connection resolution, relaying, and
//! metering, exactly like a firing inside an execution.
//!
//! What differs from a real firing, and only this:
//!   - the journal is [`weft_journal::NoopJournal`]: a node test is
//!     not an execution, nothing folds it, and it must not fabricate
//!     execution rows;
//!   - emitted outputs are captured at the ctx seam instead of routed
//!     (there is no downstream graph);
//!   - each RUNNER carries one execution identity (execution id +
//!     color), so a test's spends attribute to one real color, however
//!     many times its body fires the rig.

use std::sync::{Arc, Mutex};

use weft_core::access::Access;
use weft_core::cancellation::CancellationFlag;
use weft_core::context::ContextHandle;
use weft_core::node_test::LiveHandleFactory;
use weft_core::{Color, LiveRig};

use crate::context::{BusCoordinator, EngineClients, RunnerHandle};

/// Composes live rigs over one broker-client bundle and settles their
/// debts when the runs are over. One runner serves every live test of
/// a session; call [`Self::settle`] before the process reports its
/// results so no leased connection and no in-flight cost record is
/// dropped.
pub struct LiveTestRunner {
    clients: EngineClients,
    pod_name: String,
    tenant_id: String,
    project_id: String,
    /// THE run's execution identity: the pre-registered color the
    /// spawning runtime supplied (it registered the color so the
    /// broker can scope the run), or a fresh mint. One runner serves
    /// one test run, so the runner IS one execution: every rig and
    /// every handle it mints carries this same color, and every spend
    /// attributes to it.
    color: Color,
    /// Every handle a rig minted, so `settle` can release the
    /// runtime-owned connections their bodies opened.
    handles: Arc<Mutex<Vec<Arc<RunnerHandle>>>>,
    /// Per-rig parked-body watchdogs (see `rig`), aborted at `settle`.
    watchdogs: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl LiveTestRunner {
    /// `clients` should come from `EngineClients::from_broker`; the
    /// journal is replaced with the no-write impl here (see module
    /// doc), so callers hand in the production bundle unmodified.
    pub fn new(
        mut clients: EngineClients,
        pod_name: String,
        tenant_id: String,
        project_id: String,
        fixed_color: Option<Color>,
    ) -> Self {
        clients.journal = Arc::new(weft_journal::NoopJournal);
        Self {
            clients,
            pod_name,
            tenant_id,
            project_id,
            color: fixed_color.unwrap_or_else(Color::new_v4),
            handles: Arc::new(Mutex::new(Vec::new())),
            watchdogs: Mutex::new(Vec::new()),
        }
    }

    /// A rig for one live test: `connection_id` is the chosen grant
    /// for the test's declared `service`. The rig carries the runner's
    /// one execution identity, so a body that fires the rig several
    /// times attributes every spend to the same execution, exactly
    /// like several calls inside one firing would. Must be called from
    /// within a tokio runtime (it spawns the rig's parked-body
    /// watchdog).
    pub fn rig(&self, connection_id: &str, service: &str) -> LiveRig {
        let access = Access::new(connection_id, service, None);
        let clients = self.clients.clone();
        let pod_name = self.pod_name.clone();
        let tenant_id = self.tenant_id.clone();
        let project_id = self.project_id.clone();
        let handles = self.handles.clone();
        let color = self.color;
        // ONE coordinator per rig, shared by every handle it mints:
        // the rig is one test, and a bus the test seeds (`rig.bus`)
        // must resolve from the handle `rig.run` mints, exactly like
        // two firings of one execution share the execution's registry.
        let waits = crate::wait_tracker::WaitTracker::new();
        let bus_coordinator = BusCoordinator::new(waits.clone());
        // Parked-body watchdog: legibility only, NEVER resolution. In
        // an execution the drive loop can prove a deadlock because it
        // knows every actor; here the TEST CODE is a live actor the
        // tracker cannot see (it may be about to write the awaited
        // bus), so closing anything on a "proof" could kill a
        // legitimate test. And "parked with nothing to consume" is the
        // NORMAL momentary state of a concurrent test whose harness
        // feeds the body a beat later, so the warning only fires once
        // the state has held through a whole grace window with no
        // wake: a genuine hang holds it forever and still gets named,
        // so a hung `weft test` reports its own cause instead of
        // sitting silent until Ctrl+C.
        {
            let waits = waits.clone();
            let watchdog = tokio::spawn(async move {
                const GRACE: std::time::Duration = std::time::Duration::from_secs(15);
                let firing: std::collections::HashSet<_> =
                    [weft_core::liveness::FiringLocation::new(
                        weft_core::node_test::NODE_UNDER_TEST_ID,
                        weft_core::frames::LoopFrames::default(),
                    )]
                    .into_iter()
                    .collect();
                loop {
                    let notified = waits.wait_notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if waits.deadlock_provable(&firing) {
                        // Provably parked NOW; re-confirm after the
                        // grace window unless something wakes first.
                        if tokio::time::timeout(GRACE, notified.as_mut()).await.is_ok() {
                            continue;
                        }
                        if waits.deadlock_provable(&firing) {
                            tracing::warn!(
                                target: "weft_engine::test_rig",
                                "the node under test has been parked on a wait (a bus \
                                 wait_for / recv) with nothing left to consume for {}s; \
                                 if the test hangs here, it never writes or closes the \
                                 awaited bus",
                                GRACE.as_secs()
                            );
                            return;
                        }
                        continue;
                    }
                    notified.await;
                }
            });
            self.watchdogs.lock().unwrap().push(watchdog);
        }
        let factory: LiveHandleFactory = Arc::new(move |role, declared, has_generator_input| {
            // The role decides the firing identity: the test code's
            // waits (a cursor it reads, a bus it seeds) must never be
            // attributed to the node under test, or the parked-body
            // watchdog below reads the wrong actor in both directions.
            let (node_id, node_type) = match role {
                weft_core::node_test::HandleRole::NodeUnderTest { node_type } => {
                    (weft_core::node_test::NODE_UNDER_TEST_ID, node_type.to_string())
                }
                weft_core::node_test::HandleRole::Harness { node_type } => {
                    (weft_core::node_test::NODE_TEST_HARNESS_ID, node_type.to_string())
                }
            };
            let handle = Arc::new(RunnerHandle::new(
                format!("node-test-{}", color.simple()),
                project_id.clone(),
                color,
                node_id.to_string(),
                node_type,
                weft_core::frames::LoopFrames::default(),
                clients.clone(),
                pod_name.clone(),
                tenant_id.clone(),
                Arc::new(CancellationFlag::new()),
                waits.clone(),
                bus_coordinator.clone(),
                declared,
                // From the node's manifest, so a stream consumer's
                // `await_signal` is refused in a live test exactly as
                // in an execution (the rig DOES serve stream consumers:
                // the input bag registers real feeds).
                has_generator_input,
            ));
            handles.lock().unwrap().push(handle.clone());
            Ok(handle as Arc<dyn ContextHandle>)
        });
        LiveRig::new(factory, access)
    }

    /// The execution color this run's cost is recorded under.
    pub fn color(&self) -> Color {
        self.color
    }

    /// Settle everything the runs left open: releases every leased
    /// connection, then blocks until every in-flight cost record has
    /// landed. MUST run before the process reports and exits; skipping
    /// it drops money. Returns one entry per release that failed
    /// (naming the grant, so an operator can release it by hand);
    /// empty means everything was released.
    pub async fn settle(&self) -> Vec<String> {
        // Watchdogs are also reaped by Drop; aborting here too just
        // stops them the moment the run is over.
        for watchdog in self.watchdogs.lock().unwrap().drain(..) {
            watchdog.abort();
        }
        let handles: Vec<Arc<RunnerHandle>> =
            std::mem::take(&mut *self.handles.lock().unwrap());
        let mut failures = Vec::new();
        for handle in handles {
            failures.extend(handle.close_opened_accesses().await);
        }
        self.clients.pending_costs.wait_zero().await;
        failures
    }
}

impl Drop for LiveTestRunner {
    /// Reap any watchdog `settle` did not drain, so a runner that is
    /// dropped without settling (a panicking test, an aborted run)
    /// never leaks a background task for the runtime's lifetime.
    fn drop(&mut self) {
        for watchdog in self.watchdogs.lock().unwrap().drain(..) {
            watchdog.abort();
        }
    }
}
