//! Live-tier node-test composition: builds the [`weft_core::LiveRig`]
//! whose handle is the PRODUCTION [`RunnerHandle`], so a live test's
//! calls go through the real connection resolution, relaying, and
//! metering, exactly like a firing inside a run.
//!
//! What differs from a real firing, and only this:
//!   - the run is a node test's (`RunKind::NodeTest`): this process bears
//!     it, writes what it spends and its ending, and nothing folds it;
//!   - emitted outputs are captured at the ctx seam instead of routed
//!     (there is no downstream graph);
//!   - each RUNNER is one run, so a test's spends attribute to it, however
//!     many times its body fires the rig;
//!   - the run has no program, so it never pauses: a wait the node makes
//!     (`ctx.await_signal`) holds in its call, the way a wait of a run that
//!     cannot pause does, kept by the same pieces a drive keeps it with
//!     (`crate::outside_waits::keep_held`), its answer heard on the same
//!     line a worker hears it on (`crate::line_watch`).

use std::sync::{Arc, Mutex};

use weft_core::access::Access;
use weft_core::context::ContextHandle;
use weft_core::liveness::FiringLocation;
use weft_core::node_test::LiveHandleFactory;
use weft_core::{RunId, LiveRig};

use crate::context::{BusCoordinator, EngineClients, RunRecord, RunnerHandle};
use crate::journal_writer::{DriveJournal, Leaving, RunSpec};
use crate::wait_tracker::WaitTracker;

/// Composes live rigs over one broker-client bundle and settles their
/// debts when the run is over. One runner is one live test's run
/// (`crate::test_runner` starts one per test); call [`Self::settle`]
/// before the process reports its results so no leased connection and
/// no in-flight cost record is dropped.
pub struct LiveTestRunner {
    clients: EngineClients,
    replica: String,
    tenant_id: String,
    project_id: uuid::Uuid,
    /// THE run's identity, which the install chose. One runner
    /// serves one test run, so the runner IS one run: every rig and every
    /// handle it mints carries it, and every spend attributes to it.
    run_id: RunId,
    /// The run's record: its birth, what it spends, its ending.
    record: RunRecord,
    journal: Arc<DriveJournal>,
    /// How the run is kept, as its birth says: how long a wait holds.
    settings: weft_core::run_settings::RunSettings,
    /// The run's place among the runs this process drives: its cancel,
    /// which every handle a rig mints shares, so a `weft stop` reaches
    /// the test.
    registered: crate::line_watch::Registered,
    /// Renews this process's lease while the run goes: a run whose
    /// driver holds no lease reads as lost.
    lease: tokio::task::JoinHandle<()>,
    /// Hears the run's answers and its cancel on the line to the broker,
    /// as a worker's line watch does for its runs.
    line_watch: tokio::task::JoinHandle<()>,
    /// Ends the run just before the platform's cap on the request it runs
    /// inside, when the platform has one.
    capped: Option<tokio::task::JoinHandle<()>>,
    /// Every handle a rig minted, so `settle` can release the
    /// runtime-owned connections their bodies opened.
    handles: Arc<Mutex<Vec<Arc<RunnerHandle>>>>,
    /// The one numbering of the run's `await_signal` and `ctx.run`
    /// calls, which every handle a rig mints counts on. Each `rig.run`
    /// fires the node under the same run and node id, so a
    /// numbering of its own would give its first wait the identity, and
    /// the already-given answer, of an earlier run's first wait.
    call_numbering: Arc<std::sync::atomic::AtomicU32>,
    /// The package's catalog, so a node under test that publishes a
    /// connection is known by the service it publishes.
    catalog: &'static dyn weft_core::NodeCatalog,
    /// The recipe of each service the package's nodes publish, as the
    /// build resolved it, so a node under test publishes against the
    /// recipe a real run's definition carries.
    services: Arc<weft_core::node_test::PublishedServices>,
    /// What each rig runs beside its node (see `rig`), ended at `settle`.
    rigs: Mutex<Vec<RigKeeping>>,
}

/// What one rig runs beside its node until the run settles.
struct RigKeeping {
    /// The rig's waits: one its node still holds when the run settles is
    /// given up, so its call ends.
    waits: Arc<WaitTracker>,
    /// The parked-body watchdog and the keeper of the held waits.
    tasks: [tokio::task::JoinHandle<()>; 2],
}

impl RigKeeping {
    fn stop(&self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// The recipe a node under test publishes against, the same one the
/// compiler would have stamped onto its definition in a real project:
/// the build resolved it against the whole catalog and baked it into
/// this binary (`services`).
///
/// A node that declares nothing publishes nothing, and answers `None`.
/// A node that declares a service the build did not resolve for it is
/// a binary built some other way, and says so here rather than letting
/// the run report the node as having declared nothing.
fn published_spec(
    catalog: &dyn weft_core::NodeCatalog,
    services: &weft_core::node_test::PublishedServices,
    node_type: &str,
) -> Result<Option<weft_core::AccessSpec>, String> {
    let Some(service) = catalog.lookup(node_type).and_then(|n| n.manifest().publishes.clone())
    else {
        return Ok(None);
    };
    services.get(&service).cloned().map(Some).ok_or_else(|| {
        format!(
            "this node publishes a '{service}' connection, but this test binary was built \
             without that service's recipe"
        )
    })
}

impl LiveTestRunner {
    /// Bear the test's run, `run_id` (`RunKind::NodeTest`), and keep
    /// this process's lease while it goes. `clients` come from
    /// `EngineClients::from_broker`. `entry` names the test on its record.
    /// `run_cap` is the platform's cap on the one request the test runs
    /// inside (`WEFT_RUN_CAP_SECS`), when it has one: the run is ended
    /// [`crate::worker::RUN_CAP_MARGIN`] before it, so a wait still held
    /// gives up and the test settles and reports before the platform cuts
    /// the request.
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        clients: EngineClients,
        catalog: &'static dyn weft_core::NodeCatalog,
        services: Arc<weft_core::node_test::PublishedServices>,
        replica: String,
        tenant_id: String,
        project_id: uuid::Uuid,
        run_id: RunId,
        entry: String,
        run_cap: Option<std::time::Duration>,
    ) -> Result<Self, String> {
        // Kept like the runtime's own bookkeeping runs: what a test spent
        // is on record before its report says it ran. A wait the node holds
        // lasts as long as the job behind it (a dub, a video), up to the
        // platform's cap: the person running the test is there, and stops
        // it when they want.
        let settings = weft_core::run_settings::RunSettings::bookkeeping()
            .holding_for(weft_core::run_settings::MAX_HOLD_SECS)
            .expect("the longest hold is a hold");
        let journal = clients.writer.run(RunSpec {
            run_id,
            settings,
            keep_for: settings.kept_for(weft_core::run_settings::KeepFor::WEFT_DEFAULT),
            epoch: 1,
            next_seq: 0,
            redaction: Default::default(),
            program: None,
        });
        let birth = weft_journal::RunEvent::RunStarted {
            run_id,
            project_id,
            entry_node: entry,
            phase: weft_core::context::Phase::Fire,
            // A node test executes the package's image, never a project
            // definition.
            definition_hash: None,
            binary_hash: None,
            source_version: None,
            run_kind: weft_core::run::RunKind::NodeTest,
            cut_from: None,
            // Nothing given: a node test runs no program, so there is no
            // place to give a value at.
            given: Default::default(),
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
        // The one run this process drives; nothing comes of its coming.
        let cancels = crate::line_watch::Cancels::new(Box::new(|| {}));
        let registered = cancels.claim(run_id).expect("a fresh registry holds no run");
        let line_watch = crate::line_watch::spawn_line_watch(crate::line_watch::LineWatch::new(project_id, &clients, cancels));
        let capped = run_cap.map(|cap| {
            let flag = registered.flag.clone();
            tokio::spawn(async move {
                tokio::time::sleep(cap.saturating_sub(crate::worker::RUN_CAP_MARGIN)).await;
                crate::worker::Limit::NodeTestCap { cap }.end(&flag, run_id);
            })
        });
        Ok(Self {
            record: RunRecord { journal: journal.clone(), costs: crate::metering::PendingCostRecords::new() },
            journal,
            settings,
            registered,
            lease,
            line_watch,
            capped,
            clients,
            replica,
            tenant_id,
            project_id,
            run_id,
            handles: Arc::new(Mutex::new(Vec::new())),
            call_numbering: Default::default(),
            catalog,
            services,
            rigs: Mutex::new(Vec::new()),
        })
    }

    /// A rig for one live test: `connection_id` is the chosen grant
    /// for the test's declared `service`. The rig carries the runner's
    /// one run identity, so a body that fires the rig several
    /// times attributes every spend to the same run, exactly
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
        let call_numbering = self.call_numbering.clone();
        let run_id = self.run_id;
        let catalog = self.catalog;
        let services = self.services.clone();
        let settings = self.settings;
        let cancellation = self.registered.flag.clone();
        // ONE coordinator per rig, shared by every handle it mints:
        // the rig is one test, and a bus the test seeds (`rig.bus`)
        // must resolve from the handle `rig.run` mints, exactly like
        // two firings of one run share the run's registry.
        let waits = WaitTracker::new();
        let bus_coordinator = BusCoordinator::new(waits.clone());
        let under_test = FiringLocation::new(weft_core::node_test::NODE_UNDER_TEST_ID, weft_core::frames::LoopFrames::default());
        // The node's waits hold in its call (the run cannot pause), kept
        // the way a drive keeps the waits it holds.
        let keeper = tokio::spawn({
            let (clients, waits, cancellation, under_test) = (self.clients.clone(), waits.clone(), cancellation.clone(), under_test.clone());
            async move {
                crate::outside_waits::keep_held(
                    &clients,
                    run_id,
                    &waits,
                    under_test,
                    settings.hold_secs(),
                    crate::held_waits::Unsuspendable::NoProgram,
                    &cancellation,
                )
                .await
            }
        });
        // Parked-body watchdog: legibility only, NEVER resolution. In
        // a run the drive loop can prove a deadlock because it
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
        let watchdog = {
            let waits = waits.clone();
            tokio::spawn(async move {
                const GRACE: std::time::Duration = std::time::Duration::from_secs(15);
                let firing: std::collections::HashSet<_> = [under_test].into_iter().collect();
                // A wait held in the node's call is the outside's to end,
                // and the keeper gives it up after the hold: no hang.
                let parked_on_a_bus = || waits.deadlock_provable(&firing) && !waits.held().pending();
                loop {
                    let notified = waits.wait_notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if parked_on_a_bus() {
                        // Provably parked NOW; re-confirm after the
                        // grace window unless something wakes first.
                        if tokio::time::timeout(GRACE, notified.as_mut()).await.is_ok() {
                            continue;
                        }
                        if parked_on_a_bus() {
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
            })
        };
        self.rigs.lock().unwrap().push(RigKeeping { waits: waits.clone(), tasks: [watchdog, keeper] });
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
            let published = published_spec(catalog, &services, &node_type)
                .map_err(weft_core::WeftError::Config)?;
            let handle = RunnerHandle::new(
                project_id,
                run_id,
                node_id.to_string(),
                node_id.to_string(),
                node_type,
                weft_core::frames::LoopFrames::default(),
                clients.clone(),
                record.clone(),
                published,
                replica.clone(),
                tenant_id.clone(),
                cancellation.clone(),
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
                // in a run (the rig DOES serve stream consumers:
                // the input bag registers real feeds).
                has_generator_input,
            );
            let handle =
                Arc::new(handle.with_settings(settings).without_program().with_call_numbering(call_numbering.clone()));
            handles.lock().unwrap().push(handle.clone());
            Ok(handle as Arc<dyn ContextHandle>)
        });
        LiveRig::new(factory, access, fixtures)
    }

    /// The run this run's cost is recorded under.
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Settle everything the run left open and end it: releases every
    /// leased connection, waits until every spend is on its record, then
    /// writes its ending. MUST run before the process reports and exits;
    /// skipping it drops money. Returns one entry per release that failed
    /// (naming the grant, so an operator can release it by hand), and one
    /// when the run's record could not be finished; empty means everything
    /// was settled.
    pub async fn settle(&self) -> Vec<String> {
        // What the rigs run beside their nodes stops the moment the run is
        // over (Drop reaps it too). A wait a node still holds is given up,
        // so its call ends rather than hanging; the run's ending takes down
        // the signal of every wait it registered that nobody answered (the
        // dispatcher's `run_ends`), a wait whose call was dropped included.
        for rig in self.rigs.lock().unwrap().drain(..) {
            rig.stop();
            rig.waits.held().give_up("the node test ended before its wait was answered");
        }
        self.line_watch.abort();
        if let Some(capped) = &self.capped {
            capped.abort();
        }
        let handles: Vec<Arc<RunnerHandle>> =
            std::mem::take(&mut *self.handles.lock().unwrap());
        let mut failures = Vec::new();
        for handle in handles {
            failures.extend(handle.close_opened_accesses().await);
        }
        self.clients.open_charges
            .close_run_id(
                self.run_id,
                &self.record.costs,
                crate::metering::Closing::Unknown("the node test ended before the job was read back".into()),
            )
            .await;
        // A run stopped (by a person, or at the platform's cap) ends on
        // record as stopped, saying by what.
        let at_unix = crate::now_unix();
        let ending = match self.registered.flag.cause() {
            Some(cause) => weft_journal::RunEvent::RunCancelled {
                run_id: self.run_id,
                reason: cause.to_string(),
                cause: Some(cause),
                at_unix,
            },
            None => weft_journal::RunEvent::RunCompleted { run_id: self.run_id, at_unix },
        };
        let ended = match weft_journal::JournalClient::record_event(self.journal.as_ref(), &ending, Some(&self.replica)).await {
            Ok(()) => self.journal.leave(Leaving::Ended).await,
            Err(e) => Err(e),
        };
        if let Err(e) = ended {
            let why = format!("the test's run could not be ended on record: {e:#}");
            self.clients.writer.give_up(self.run_id, &why).await;
            failures.push(why);
        }
        self.lease.abort();
        failures
    }
}

impl Drop for LiveTestRunner {
    /// Reap any task `settle` did not, so a runner that is dropped
    /// without settling (a panicking test, an aborted run) never leaks a
    /// background task for the runtime's lifetime.
    fn drop(&mut self) {
        for rig in self.rigs.lock().unwrap().drain(..) {
            rig.stop();
        }
        self.line_watch.abort();
        if let Some(capped) = &self.capped {
            capped.abort();
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
            vouch_until: None,
            shortens: false,
        };
        if let Err(e) = broker.tick(&tick).await {
            tracing::warn!(target: "weft_engine::test_rig", error = %format!("{e:#}"), "the test's lease could not be renewed; the next second tries again");
        }
    }
}

#[cfg(test)]
mod tests {
    //! Layer 3: a live test's node that waits, through the production
    //! rig, with the broker and its line faked.

    use super::*;
    use crate::run_driver::engine_test_rig::{catalog, clients, Answers, AwaitTasks, MemJournal};
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::RunContext;

    /// Waits on a timer, then emits what the wait answered.
    struct Waits;
    impl weft_core::NodeManifest for Waits {
        fn manifest(&self) -> &'static weft_core::node::NodeMetadata {
            static M: std::sync::OnceLock<weft_core::node::NodeMetadata> = std::sync::OnceLock::new();
            M.get_or_init(|| {
                serde_json::from_value(json!({
                    "type": "Waits", "label": "Waits", "description": "",
                    "outputs": [{ "name": "value", "type": "String" }]
                }))
                .expect("the node's metadata")
            })
        }
    }
    #[async_trait::async_trait]
    impl Node for Waits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 600_000 } };
            let value = ctx.await_signal(timer).await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
        }
    }

    /// One live test's run, its broker and its line faked.
    struct Live {
        runner: LiveTestRunner,
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        line: Arc<crate::context::TestLine>,
        journal: Arc<MemJournal>,
        project_id: uuid::Uuid,
    }

    async fn live() -> Live {
        live_capped(None).await
    }

    /// [`live`] on a platform whose cap on one request is `run_cap`.
    async fn live_capped(run_cap: Option<std::time::Duration>) -> Live {
        let (tasks, answers, line) = (AwaitTasks::new(), Arc::new(Answers::default()), crate::context::TestLine::new());
        let journal = Arc::new(MemJournal::default());
        let clients = EngineClients {
            tasks: tasks.clone(),
            told: answers.told.clone(),
            runs: answers.clone(),
            line: line.clone(),
            ..clients(journal.clone())
        };
        // A catalog lives as long as the process, as the test binary's does.
        let catalog: &'static Arc<dyn weft_core::NodeCatalog> = Box::leak(Box::new(catalog(vec![("Waits", Box::new(Waits) as Box<dyn Node>)])));
        let project_id = uuid::Uuid::new_v4();
        let runner = LiveTestRunner::start(
            clients,
            catalog.as_ref(),
            Arc::default(),
            "replica".into(),
            "tenant".into(),
            project_id,
            weft_core::new_run_id(),
            "node-test:Waits::waits".into(),
            run_cap,
        )
        .await
        .expect("the test's run is born");
        Live { runner, tasks, answers, line, journal, project_id }
    }

    impl Live {
        /// The node waits, through a rig of the runner.
        fn waiting(&self) -> tokio::task::JoinHandle<weft_core::node_test::RigOutcome> {
            waiting_on(Arc::new(self.runner.rig("grant", "svc", Default::default())))
        }

        /// The wait's token, once the node registered it and its keeper
        /// asked for its answers.
        async fn asked(&self) -> String {
            let token = self.tasks.await_token().await;
            while self.answers.reads() == 0 {
                tokio::task::yield_now().await;
            }
            token
        }

        /// The token of a wait registered after `earlier`, once its keeper
        /// asked for its answers (a read past the `reads` there were).
        async fn asked_after(&self, earlier: &str, reads: usize) -> String {
            let token = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    match self.tasks.minted() {
                        Some(token) if token != earlier => return token,
                        _ => tokio::time::sleep(std::time::Duration::from_millis(2)).await,
                    }
                }
            })
            .await
            .expect("the second run registered a wait of its own");
            while self.answers.reads() <= reads {
                tokio::task::yield_now().await;
            }
            token
        }

        /// Answer the wait `token`: the answer is kept for the run, and
        /// its news comes on the line.
        fn answer(&self, token: String, value: serde_json::Value) {
            self.answers.keep(token, value);
            self.line
                .pushed
                .send(weft_task_store::pg_signal::Heard::Signal {
                    channel: weft_task_store::parked_fires::PARKED_FIRE_CHANNEL,
                    payload: weft_task_store::runs::run_payload(self.project_id, self.runner.run_id()).into(),
                })
                .unwrap();
        }
    }

    /// The node waits, through `rig`.
    fn waiting_on(rig: Arc<weft_core::LiveRig>) -> tokio::task::JoinHandle<weft_core::node_test::RigOutcome> {
        tokio::spawn(async move { rig.run(&Waits, json!({})).await })
    }

    async fn outcome(waiting: tokio::task::JoinHandle<weft_core::node_test::RigOutcome>) -> weft_core::node_test::RigOutcome {
        tokio::time::timeout(std::time::Duration::from_secs(10), waiting).await.expect("the node ended").unwrap()
    }

    // The node's wait takes the answer handed to its run once the line
    // brings the news, the node carries on, and what it emits is captured.
    weft_core::stress_test! {
        name: a_live_tests_wait_takes_the_answer_its_line_brings,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let live = live().await;
            let waiting = live.waiting();
            let token = live.asked().await;
            live.answer(token, json!("done"));
            let outcome = outcome(waiting).await;
            assert!(outcome.result.is_ok(), "{:?}", outcome.result);
            assert_eq!(outcome.output("value").unwrap(), &json!("done"));
            assert!(live.runner.settle().await.is_empty());
            assert!(live.tasks.withdrawn.lock().unwrap().is_empty(), "an answered wait is not withdrawn");
        }
    }

    // A live test that runs its node twice, each run parking on a wait,
    // gets a wait of its own for each run: the second run's wait carries
    // on from the first run's call numbering, so it is not the first
    // run's wait again, and it takes its own answer.
    weft_core::stress_test! {
        name: each_run_of_a_live_test_takes_its_own_wait,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let live = live().await;
            let rig = Arc::new(live.runner.rig("grant", "svc", Default::default()));
            let waiting = waiting_on(rig.clone());
            let first = live.asked().await;
            live.answer(first.clone(), json!("first"));
            let outcome_first = outcome(waiting).await;
            assert_eq!(outcome_first.output("value").ok(), Some(&json!("first")), "{:?}", outcome_first.result);

            let reads = live.answers.reads();
            let waiting = waiting_on(rig);
            let second = live.asked_after(&first, reads).await;
            live.answer(second, json!("second"));
            let outcome_second = outcome(waiting).await;
            assert_eq!(outcome_second.output("value").ok(), Some(&json!("second")), "{:?}", outcome_second.result);
            assert!(live.runner.settle().await.is_empty());
        }
    }

    /// A test that settles while its node still holds a wait gives the
    /// wait up: the node's call ends, naming why, rather than hanging.
    #[tokio::test]
    async fn settling_gives_up_a_wait_still_held() {
        let live = live().await;
        let waiting = live.waiting();
        live.asked().await;
        assert!(live.runner.settle().await.is_empty());
        let outcome = outcome(waiting).await;
        let error = outcome.result.unwrap_err().to_string();
        assert!(error.contains("the node test ended before its wait was answered"), "{error}");
    }

    /// On a platform with a cap on one request, a test whose node still
    /// waits is ended just before it: the wait gives up naming the cap,
    /// and the test settles with its run ended on record as stopped at a
    /// time limit, all before the platform would cut the request.
    #[tokio::test]
    async fn a_live_test_ends_before_the_platforms_cap() {
        let cap = crate::worker::RUN_CAP_MARGIN + std::time::Duration::from_millis(500);
        let live = live_capped(Some(cap)).await;
        let waiting = live.waiting();
        live.asked().await;
        let outcome = outcome(waiting).await;
        let error = outcome.result.unwrap_err().to_string();
        assert!(error.contains("minute limit this platform puts on one request"), "{error}");
        assert!(live.runner.settle().await.is_empty());
        let events = live.journal.events_of(live.runner.run_id());
        assert!(
            events.iter().any(|event| matches!(
                event,
                weft_journal::RunEvent::RunCancelled { cause: Some(weft_core::run::CancelCause::Limit { .. }), .. }
            )),
            "{events:?}"
        );
        assert!(!events.iter().any(|event| matches!(event, weft_journal::RunEvent::RunCompleted { .. })), "{events:?}");
    }
}
