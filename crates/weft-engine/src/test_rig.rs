//! Live-tier node-test composition: builds the [`weft_core::LiveRig`]
//! whose handle is the PRODUCTION [`RunnerHandle`], so a live test's
//! calls go through the real connection resolution, relaying, and
//! metering, exactly like a firing inside an execution.
//!
//! What differs from a real firing, and only this:
//!   - the run is a node test's (`RunKind::NodeTest`): this process bears
//!     it, writes what it spends and its ending, and nothing folds it;
//!   - emitted outputs are captured at the ctx seam instead of routed
//!     (there is no downstream graph);
//!   - each RUNNER is one run, so a test's spends attribute to it, however
//!     many times its body fires the rig.

use std::sync::{Arc, Mutex};

use weft_core::access::Access;
use weft_core::cancellation::CancellationFlag;
use weft_core::context::ContextHandle;
use weft_core::node_test::LiveHandleFactory;
use weft_core::{ExecutionId, LiveRig};

use crate::context::{BusCoordinator, EngineClients, RunRecord, RunnerHandle};
use crate::journal_writer::{DriveJournal, Leaving, RunSpec};

/// Composes live rigs over one broker-client bundle and settles their
/// debts when the runs are over. One runner serves every live test of
/// a session; call [`Self::settle`] before the process reports its
/// results so no leased connection and no in-flight cost record is
/// dropped.
pub struct LiveTestRunner {
    clients: EngineClients,
    replica: String,
    tenant_id: String,
    project_id: uuid::Uuid,
    /// THE run's execution identity, which the install chose. One runner
    /// serves one test run, so the runner IS one run: every rig and every
    /// handle it mints carries it, and every spend attributes to it.
    execution_id: ExecutionId,
    /// The run's record: its birth, what it spends, its ending.
    record: RunRecord,
    journal: Arc<DriveJournal>,
    /// Renews this process's lease while the run goes: a run whose
    /// driver holds no lease reads as lost.
    lease: tokio::task::JoinHandle<()>,
    /// Every handle a rig minted, so `settle` can release the
    /// runtime-owned connections their bodies opened.
    handles: Arc<Mutex<Vec<Arc<RunnerHandle>>>>,
    /// The package's catalog, so a node under test that publishes a
    /// connection resolves the service's recipe exactly as it does in
    /// a real run.
    catalog: &'static dyn weft_core::NodeCatalog,
    /// Per-rig parked-body watchdogs (see `rig`), aborted at `settle`.
    watchdogs: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// The recipe a node under test publishes against, the same one the
/// compiler would have stamped onto its definition in a real project.
/// Resolved from the package's own catalog, which a node-test binary
/// carries whole.
///
/// A node that declares nothing publishes nothing, and answers `None`.
/// A node that DECLARES a service the package cannot supply is a
/// packaging mistake, and says so here rather than letting the run
/// report the node as having declared nothing: the real compile
/// resolves against the whole catalog, so a test that quietly differed
/// would pass on something production refuses.
fn published_spec(
    catalog: &dyn weft_core::NodeCatalog,
    node_type: &str,
) -> Result<Option<weft_core::AccessSpec>, String> {
    let Some(service) = catalog.lookup(node_type).and_then(|n| n.manifest().publishes.clone())
    else {
        return Ok(None);
    };
    weft_core::access::spec::spec_for_service(
        catalog.all().into_iter().filter_map(|t| catalog.lookup(t)).map(|n| n.manifest()),
        &service,
    )
    .cloned()
    .map(Some)
    .ok_or_else(|| {
        format!(
            "this node publishes a '{service}' connection, but no node in its package \
             declares that service; a node test carries only its own package"
        )
    })
}

impl LiveTestRunner {
    /// Bear the test's run, `execution_id` (`RunKind::NodeTest`), and keep
    /// this process's lease while it goes. `clients` come from
    /// `EngineClients::from_broker`. `entry` names the test on its record.
    pub async fn start(
        clients: EngineClients,
        catalog: &'static dyn weft_core::NodeCatalog,
        replica: String,
        tenant_id: String,
        project_id: uuid::Uuid,
        execution_id: ExecutionId,
        entry: String,
    ) -> Result<Self, String> {
        // Kept like the runtime's own bookkeeping runs: what a test spent
        // is on record before its report says it ran.
        let settings = weft_core::run_settings::RunSettings::bookkeeping();
        let journal = clients.writer.run(RunSpec {
            execution_id,
            settings,
            keep_for: settings.kept_for(weft_core::run_settings::KeepFor::WEFT_DEFAULT),
            epoch: 1,
            next_seq: 0,
            redaction: Default::default(),
        });
        let birth = weft_journal::ExecEvent::ExecutionStarted {
            execution_id,
            project_id,
            entry_node: entry,
            phase: weft_core::context::Phase::Fire,
            // A node test executes the package's image, never a project
            // definition.
            definition_hash: None,
            binary_hash: None,
            source_version: None,
            run_kind: weft_core::exec::RunKind::NodeTest,
            selection: None,
            seed: None,
            // No picks: a node test runs no program, and its connections
            // are the ones the test itself hands the node, never the
            // install's.
            instance: None,
            fired_trigger: None,
            stand_in: None,
            instance_values: Default::default(),
            picks: Default::default(),
            settings,
            at_unix: crate::now_unix(),
        };
        weft_journal::JournalClient::record_event(journal.as_ref(), &birth, Some(&replica))
            .await
            .map_err(|e| format!("the test's run could not be recorded: {e:#}"))?;
        journal.flush().await.map_err(|e| format!("the test's run could not be recorded: {e:#}"))?;
        let lease = tokio::spawn(keep_lease(clients.door_broker.clone()));
        Ok(Self {
            record: RunRecord { journal: journal.clone(), costs: crate::metering::PendingCostRecords::new() },
            journal,
            lease,
            clients,
            replica,
            tenant_id,
            project_id,
            execution_id,
            handles: Arc::new(Mutex::new(Vec::new())),
            catalog,
            watchdogs: Mutex::new(Vec::new()),
        })
    }

    /// A rig for one live test: `connection_id` is the chosen grant
    /// for the test's declared `service`. The rig carries the runner's
    /// one execution identity, so a body that fires the rig several
    /// times attributes every spend to the same execution, exactly
    /// like several calls inside one firing would. Must be called from
    /// within a tokio runtime (it spawns the rig's parked-body
    /// watchdog).
    pub fn rig(&self, connection_id: &str, service: &str, fixtures: std::collections::BTreeMap<String, String>) -> LiveRig {
        let access = Access::new(connection_id, service, None);
        let clients = self.clients.clone();
        let record = self.record.clone();
        let replica = self.replica.clone();
        let tenant_id = self.tenant_id.clone();
        let project_id = self.project_id;
        let handles = self.handles.clone();
        let execution_id = self.execution_id;
        let catalog = self.catalog;
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
            // Resolved before the handle is built: `node_type` is moved
            // into the constructor, and the recipe is looked up by it.
            let published = published_spec(catalog, &node_type)
                .map_err(weft_core::WeftError::Config)?;
            let handle = Arc::new(RunnerHandle::new(
                project_id,
                execution_id,
                node_id.to_string(),
                node_id.to_string(),
                node_type,
                weft_core::frames::LoopFrames::default(),
                clients.clone(),
                record.clone(),
                published,
                replica.clone(),
                tenant_id.clone(),
                Arc::new(CancellationFlag::new()),
                waits.clone(),
                bus_coordinator.clone(),
                Arc::new(declared),
                // The rig's capturing wrapper answers the declared
                // inputs (it knows the manifest); the inner handle is
                // never asked.
                Default::default(),
                // Likewise the wired outputs: the capturing wrapper
                // answers them from the case (`LiveRig::wire_output`).
                std::collections::HashSet::new(),
                // From the node's manifest, so a stream consumer's
                // `await_signal` is refused in a live test exactly as
                // in an execution (the rig DOES serve stream consumers:
                // the input bag registers real feeds).
                has_generator_input,
            ));
            handles.lock().unwrap().push(handle.clone());
            Ok(handle as Arc<dyn ContextHandle>)
        });
        LiveRig::new(factory, access, fixtures)
    }

    /// The execution this run's cost is recorded under.
    pub fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    /// Settle everything the run left open and end it: releases every
    /// leased connection, waits until every spend is on its record, then
    /// writes its ending. MUST run before the process reports and exits;
    /// skipping it drops money. Returns one entry per release that failed
    /// (naming the grant, so an operator can release it by hand), and one
    /// when the run's record could not be finished; empty means everything
    /// was settled.
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
        self.clients.open_charges.flush_execution_id(self.execution_id, "the node test ended before the job was read back");
        self.record.costs.wait_zero().await;
        let ending = weft_journal::ExecEvent::ExecutionCompleted { execution_id: self.execution_id, at_unix: crate::now_unix() };
        let ended = match weft_journal::JournalClient::record_event(self.journal.as_ref(), &ending, Some(&self.replica)).await {
            Ok(()) => self.journal.leave(Leaving::Ended).await,
            Err(e) => Err(e),
        };
        if let Err(e) = ended {
            let why = format!("the test's run could not be ended on record: {e:#}");
            self.clients.writer.give_up(self.execution_id, &why).await;
            failures.push(why);
        }
        self.lease.abort();
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
        self.lease.abort();
    }
}

/// Renew this process's lease once a second, the way a worker's door
/// tick does (`crate::door`), for as long as the test's run goes: it
/// counts nothing at a door, so its tick says only that it is alive.
async fn keep_lease(broker: Arc<dyn crate::door::DoorBroker>) {
    let mut every = tokio::time::interval(std::time::Duration::from_secs(1));
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        every.tick().await;
        let tick = weft_broker_client::protocol::DoorTickRequest {
            binary_hash: None,
            in_flight: Default::default(),
            window_start: weft_core::signal::limits::window(crate::now_unix() as i64).0,
            counts: Vec::new(),
            tokens: Vec::new(),
        };
        if let Err(e) = broker.tick(&tick).await {
            tracing::warn!(target: "weft_engine::test_rig", error = %format!("{e:#}"), "the test's lease could not be renewed; the next second tries again");
        }
    }
}
