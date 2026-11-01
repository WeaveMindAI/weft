    use super::*;
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::node::NodeMetadata;
    use weft_core::ProjectDefinition;
    use weft_journal::{RunEvent, JournalClient};
    use weft_task_store::InfraReader;
    use crate::context::InfraStateClient;

    pub(super) fn trivial_metadata(node_type: &str) -> NodeMetadata {
        serde_json::from_value(json!({
            "type": node_type, "label": node_type, "description": ""
        }))
        .expect("trivial metadata")
    }

    /// Inline test nodes have no metadata.json, so they can't use
    /// `#[derive(NodeManifest)]`; this hands them the same static
    /// manifest shape, built from `trivial_metadata`.
    macro_rules! test_manifest {
        ($node:ty, $ty:literal) => {
            impl weft_core::NodeManifest for $node {
                fn manifest(&self) -> &'static weft_core::node::NodeMetadata {
                    static M: std::sync::OnceLock<weft_core::node::NodeMetadata> =
                        std::sync::OnceLock::new();
                    M.get_or_init(|| crate::run_driver::engine_test_rig::trivial_metadata($ty))
                }
            }
        };
    }
    pub(super) use test_manifest;

    /// Lookup-by-type catalog over a plain list, the shape every suite
    /// that hands its own node set to `drive` needs.
    pub(super) struct VecCatalog {
        nodes: Vec<(&'static str, &'static dyn weft_core::Node)>,
    }
    impl weft_core::NodeCatalog for VecCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn weft_core::Node> {
            self.nodes.iter().find(|(t, _)| *t == node_type).map(|(_, n)| *n)
        }
        fn all(&self) -> Vec<&'static str> {
            self.nodes.iter().map(|(t, _)| *t).collect()
        }
    }

    pub(crate) fn catalog(
        nodes: Vec<(&'static str, Box<dyn weft_core::Node>)>,
    ) -> Arc<dyn weft_core::NodeCatalog> {
        // `Box::leak` is DELIBERATE: the catalog contract wants
        // `&'static dyn Node`, and each test (each stress-run) builds
        // its own node set, so the leak is bounded by the suite's test
        // count and lives only for the test process.
        Arc::new(VecCatalog {
            nodes: nodes
                .into_iter()
                .map(|(t, n)| (t, Box::leak(n) as &'static dyn weft_core::Node))
                .collect(),
        })
    }

    /// The record in memory, as a worker's writer lanes see it: every
    /// batch is taken whole, in order, and every event kept in one log
    /// across runs (a seed's runs and the run it seeds), each run's read
    /// back on its own.
    #[derive(Default)]
    pub(crate) struct MemJournal {
        pub(super) events: StdMutex<Vec<RunEvent>>,
        /// How many batches the writer sent.
        pub(crate) batches: std::sync::atomic::AtomicUsize,
        /// While set, a batch reaches nothing and gets no answer: the
        /// broker is out of reach.
        pub(crate) out_of_reach: std::sync::atomic::AtomicBool,
        /// Every secret on record, by id: the ones the batches carried
        /// and the ones kept straight onto it.
        pub(crate) secrets: StdMutex<std::collections::HashMap<weft_core::secret::SecretId, serde_json::Value>>,
    }
    impl MemJournal {
        /// Put `events` on record as they are: a run's record (or a seed's)
        /// before a test drives it.
        pub(crate) fn seed(&self, events: &[RunEvent]) {
            self.events.lock().unwrap().extend(events.iter().cloned());
        }

        /// Every event on record of `run_id`, in order.
        pub(crate) fn events_of(&self, run_id: RunId) -> Vec<RunEvent> {
            self.events.lock().unwrap().iter().filter(|event| event.run_id() == run_id).cloned().collect()
        }
    }
    /// Straight onto the record, as a test puts a run's record (or a
    /// seed's) there before it drives it.
    #[async_trait]
    impl JournalClient for MemJournal {
        async fn record_event(&self, event: &RunEvent, _replica: Option<&str>) -> anyhow::Result<()> {
            self.seed(std::slice::from_ref(event));
            Ok(())
        }
        async fn record_page(&self, run_id: RunId, after: Option<i32>) -> anyhow::Result<Vec<weft_journal::JournalRow>> {
            Ok(crate::test_record::rows_of(run_id, &self.events_of(run_id), after))
        }
        fn keep_secret(&self, value: serde_json::Value) -> weft_core::secret::SecretId {
            let id = weft_core::secret::SecretId::mint();
            self.secrets.lock().unwrap().insert(id, value);
            id
        }
        async fn reveal_secret(&self, id: weft_core::secret::SecretId) -> anyhow::Result<serde_json::Value> {
            self.secrets.lock().unwrap().get(&id).cloned().ok_or_else(|| weft_journal::secret_gone(id))
        }
    }
    #[async_trait]
    impl weft_journal::RecordClient for MemJournal {
        async fn record_batch(&self, batch: Vec<u8>) -> Result<weft_journal::frame::BatchAnswer, weft_journal::BatchError> {
            self.batches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.out_of_reach.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(weft_journal::BatchError::Unanswered(anyhow::anyhow!("the broker is out of reach")));
            }
            let (_, runs) = crate::test_record::decode_batch(&batch);
            let fates = runs.iter().map(|_| weft_journal::record::Fate::Accepted).collect();
            self.secrets.lock().unwrap().extend(runs.iter().flat_map(|(head, _)| &head.secrets).map(|secret| (secret.secret_id, secret.value.clone())));
            self.events.lock().unwrap().extend(runs.into_iter().flat_map(|(_, events)| events));
            Ok(weft_journal::frame::BatchAnswer { fates })
        }
        async fn record_of(&self, run_id: RunId, after: Option<i32>) -> anyhow::Result<weft_journal::record::RawRecord> {
            Ok(crate::test_record::page_of(&self.events_of(run_id), after))
        }
        async fn secret(&self, id: weft_core::secret::SecretId) -> anyhow::Result<Option<serde_json::Value>> {
            Ok(self.secrets.lock().unwrap().get(&id).cloned())
        }
        /// The record writes the ending of a run its worker gave up on.
        async fn give_up(&self, run_id: RunId, why: String) -> Result<(), weft_journal::BatchError> {
            self.events.lock().unwrap().push(RunEvent::RunFailed { run_id, error: why, at_unix: 0 });
            Ok(())
        }
    }

    pub(super) struct NoopTasks;
    #[async_trait]
    impl weft_task_store::TaskStoreClient for NoopTasks {
        async fn enqueue_dedup(&self, _s: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!("rig tests enqueue no tasks")
        }
        async fn wait_for_terminal(&self, _t: uuid::Uuid, _to: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!()
        }
    }
    /// Tasks fake for await_signal tests. `enqueue_dedup` of a
    /// RegisterSignal mints a deterministic token (recording it so the
    /// test can give the matching answer) and `wait_for_terminal` hands
    /// back a registered signal result; a WithdrawSignal is recorded.
    /// Every other task kind is unreachable in these tests.
    pub(crate) struct AwaitTasks {
        // (task_id -> token) so wait_for_terminal returns the same token
        // enqueue minted, and the test can read the token to resolve it.
        tokens: StdMutex<std::collections::HashMap<uuid::Uuid, String>>,
        // The most-recently-minted token, for the test to resolve.
        last_token: StdMutex<Option<String>>,
        /// The waits withdrawn, by token.
        pub(crate) withdrawn: StdMutex<Vec<String>>,
    }
    impl AwaitTasks {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self { tokens: Default::default(), last_token: Default::default(), withdrawn: Default::default() })
        }
        /// Block (test-side) until a token has been minted, then return
        /// it. Buses race the worker; the await may not have registered
        /// the instant the test wants to resolve it.
        pub(crate) async fn await_token(&self) -> String {
            for _ in 0..2000 {
                if let Some(t) = self.last_token.lock().unwrap().clone() {
                    return t;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            panic!("no register_signal token minted within timeout");
        }
        /// The token minted last, if any.
        pub(crate) fn minted(&self) -> Option<String> {
            self.last_token.lock().unwrap().clone()
        }
    }
    #[async_trait]
    impl weft_task_store::TaskStoreClient for AwaitTasks {
        async fn enqueue_dedup(&self, t: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            let id = uuid::Uuid::new_v4();
            if t.kind == weft_task_store::TaskKind::WithdrawSignal {
                let withdraw: weft_task_store::WithdrawSignalPayload = serde_json::from_value(t.payload)?;
                self.withdrawn.lock().unwrap().push(withdraw.token);
                return Ok(weft_task_store::tasks::DedupOutcome::Inserted(id));
            }
            assert_eq!(t.kind, weft_task_store::TaskKind::RegisterSignal, "await tests only register and withdraw waits");
            // The token derives from the wait's identity, as the
            // dispatcher's resume token does (`task_kinds::register_signal`):
            // two registrations of one (run, node, frames, call
            // index) get the same token.
            let identity = ["run_id", "node_id", "frames", "is_resume", "call_index"].map(|field| t.payload[field].to_string());
            let token = format!("tok-{}", uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.join("/").as_bytes()));
            self.tokens.lock().unwrap().insert(id, token.clone());
            *self.last_token.lock().unwrap() = Some(token);
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(id))
        }
        async fn wait_for_terminal(&self, t: uuid::Uuid, _to: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            let token = self.tokens.lock().unwrap().get(&t).cloned().expect("token for task id");
            Ok(weft_task_store::tasks::TaskOutcome {
                status: weft_task_store::tasks::TaskStatus::Complete,
                result: Some(serde_json::json!({ "kind": "registered", "token": token })),
                error: None,
            })
        }
    }
    pub(super) struct NoopSteering;
    #[async_trait]
    impl crate::context::RunSteeringClient for NoopSteering {
        async fn tag_run(&self, _c: RunId, _t: Vec<String>) -> anyhow::Result<()> {
            unreachable!("rig tests steer no firings")
        }
        async fn stop_tagged(&self, _c: RunId, _t: String, _s: weft_core::StopSelf) -> anyhow::Result<bool> {
            unreachable!("rig tests steer no firings")
        }
    }
    /// The broker's side of the runs a rig drives: the answers a test
    /// gives, handed to whoever reads next, each one told to the run (as
    /// its worker's line watch passes it on); every read counted; the runs
    /// let go of.
    pub(crate) struct Answers {
        waiting: StdMutex<Vec<weft_broker_client::protocol::RunAnswer>>,
        /// What the run is told, as its worker tells it: the clients' `told`.
        pub(crate) told: tokio::sync::broadcast::Sender<crate::context::RunTold>,
        /// The run that reads, as its first read names it.
        run: StdMutex<Option<RunId>>,
        /// How many times the answers were read.
        pub(crate) reads: std::sync::atomic::AtomicUsize,
        pub(crate) let_go: StdMutex<Vec<(RunId, weft_broker_client::protocol::LetGo)>>,
    }
    impl Default for Answers {
        fn default() -> Self {
            Self {
                waiting: StdMutex::default(),
                told: crate::context::run_told(),
                run: StdMutex::default(),
                reads: std::sync::atomic::AtomicUsize::new(0),
                let_go: StdMutex::default(),
            }
        }
    }
    impl Answers {
        /// An answer to the wait `token`, as the install hands one to the
        /// run's worker: kept for it, and told to the run that reads.
        pub(crate) fn answer(&self, token: impl Into<String>, value: Value) {
            self.keep(token, value);
            let run = self.run.lock().unwrap().expect("the run read its answers once as it started waiting");
            let _ = self.told.send(crate::context::RunTold::Answered(run));
        }

        /// An answer to the wait `token`, kept for the run's next read and
        /// told to nobody: the news of it travels on the line.
        pub(crate) fn keep(&self, token: impl Into<String>, value: Value) {
            self.waiting.lock().unwrap().push(weft_broker_client::protocol::RunAnswer {
                token: token.into(),
                answer: weft_core::primitive::WaitAnswer::Given { value },
            });
        }

        pub(crate) fn reads(&self) -> usize {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    #[async_trait]
    impl crate::context::RunClient for Answers {
        async fn claim(&self, _run_id: RunId) -> anyhow::Result<Option<weft_journal::record::Claimed>> {
            Ok(None)
        }
        async fn let_go(&self, run_id: RunId, why: weft_broker_client::protocol::LetGo) -> anyhow::Result<()> {
            self.let_go.lock().unwrap().push((run_id, why));
            Ok(())
        }
        async fn answers(&self, run_id: RunId, _taken: &[String]) -> anyhow::Result<Vec<weft_broker_client::protocol::RunAnswer>> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            *self.run.lock().unwrap() = Some(run_id);
            Ok(std::mem::take(&mut *self.waiting.lock().unwrap()))
        }
        async fn rechecked(&self) -> anyhow::Result<weft_broker_client::protocol::RunRecheckedResponse> {
            unreachable!("a rig has no line watch")
        }
    }
    pub(super) struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_address(&self, _c: weft_core::RunId, _r: Option<&weft_core::instance::InstanceId>, _i: &weft_core::infra::InfraHandle) -> anyhow::Result<Option<weft_core::infra::EndpointAddress>> { Ok(None) }
        async fn baked_outputs(&self, _c: weft_core::RunId, _r: Option<&weft_core::instance::InstanceId>, _p: &str, _m: Option<&weft_core::instance::InstanceId>) -> anyhow::Result<std::collections::BTreeMap<String, serde_json::Value>> { Ok(Default::default()) }
    }
    pub(super) struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(&self, _p: uuid::Uuid, _n: &str, _m: Option<&weft_core::instance::InstanceId>, _s: serde_json::Value, _r: weft_core::RunId) -> anyhow::Result<i64> { Ok(0) }
        async fn wait_apply(&self, _p: uuid::Uuid, _c: i64, _w: std::time::Duration) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
        async fn save_bake(&self, _x: weft_core::RunId, _p: &str, _m: Option<&weft_core::instance::InstanceId>, _v: std::collections::BTreeMap<String, serde_json::Value>) -> anyhow::Result<()> { Ok(()) }
    }
    pub(super) struct NoopProject;
    #[async_trait]
    impl crate::context::ProjectClient for NoopProject {
        async fn fetch_definition(
            &self,
            _project_id: uuid::Uuid,
            _expected_hash: &str,
        ) -> anyhow::Result<Option<ProjectDefinition>> {
            // These run_driver tests inject the project into
            // `run_one` directly, so the per-run
            // fetch path is never invoked here. Bail loud if it is.
            anyhow::bail!("NoopProject::fetch_definition not implemented in run_driver tests")
        }
    }

    struct ProjectHistory(HashMap<(String, String), ProjectDefinition>);

    #[async_trait]
    impl crate::context::ProjectClient for ProjectHistory {
        async fn fetch_definition(&self, project_id: uuid::Uuid, expected_hash: &str) -> anyhow::Result<Option<ProjectDefinition>> {
            Ok(self.0.get(&(project_id.to_string(), expected_hash.into())).cloned())
        }
    }

    /// Seed + drive one run: RunStarted(Fire) + a kick per
    /// entry node, then `run_one`. Returns the outcome and
    /// every journaled event.
    pub(super) async fn drive(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
    ) -> (RunOutcome, Vec<RunEvent>) {
        drive_with_cancel(project, catalog, kicks, CancellationFlag::new_arc()).await
    }

    /// `drive` with a caller-owned cancellation flag, for the tests
    /// that trip it mid-stream. Every run is bounded by a generous
    /// failsafe deadline, a rig-level safety net: a regression into a
    /// hang must FAIL the test by name, never wedge the whole
    /// `cargo test` process.
    pub(super) async fn drive_with_cancel(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        cancellation: Arc<CancellationFlag>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        drive_scoped(project, catalog, kicks, None, cancellation).await
    }

    /// `drive` over the cut of the program that keeps `subgraph` alone
    /// ([`cut_to`]), the shape a bounded run takes: only the named nodes
    /// are in the program it runs.
    pub(super) async fn drive_scoped(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        subgraph: Option<&[&str]>,
        cancellation: Arc<CancellationFlag>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        drive_kicked(project, catalog, kicks, None, subgraph, cancellation).await
    }

    /// `drive_scoped` for a TRIGGER FIRE: `firing` names the kick that
    /// is the fired trigger (journaled with `firing: true`, the way the
    /// worker's door writes a fire's birth), and `subgraph` is what the
    /// fire's cut keeps. The shape every two-programs test drives.
    pub(super) async fn drive_fire(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        firing: &str,
        kicks: &[&str],
        subgraph: Option<&[&str]>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        drive_kicked(project, catalog, kicks, Some(firing), subgraph, CancellationFlag::new_arc()).await
    }

    async fn drive_kicked(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        firing: Option<&str>,
        subgraph: Option<&[&str]>,
        cancellation: Arc<CancellationFlag>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        let run_id = weft_core::new_run_id();
        let (project, cut_from) = match subgraph {
            Some(nodes) => cut_to(&project, nodes, kicks, firing),
            None => (project, None),
        };
        let mut rows = vec![RunEvent::RunStarted { given: Default::default(),
            run_id,
            project_id: project.id,
            entry_node: kicks[0].to_string(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
            cut_from,
            binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            seed: None,
            instance: None, fired_trigger: None, stand_in: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }];
        for kick in kicks {
            rows.push(RunEvent::NodeKicked {
                run_id,
                node_id: kick.to_string(), frames: vec![],
                firing: firing == Some(*kick),
                payload: None,
                port_snapshot: None,
                at_unix: 0,
            });
        }
        let (drove, events) = drive_journal_observed(project, catalog, run_id, rows, cancellation).await;
        (drove.expect("run_one ok").outcome, events)
    }

    /// The cut of `project` that runs `nodes` alone (top-level places),
    /// started at `kicks` with `firing` the fired trigger, and the hash of
    /// the program it was cut from: the program a bounded run runs.
    pub(super) fn cut_to(
        project: &ProjectDefinition,
        nodes: &[&str],
        kicks: &[&str],
        firing: Option<&str>,
    ) -> (ProjectDefinition, Option<String>) {
        let places = |ids: &[&str]| ids.iter().map(|id| weft_core::frames::Located::top(*id)).collect::<std::collections::BTreeSet<_>>();
        let walk = weft_core::project::cut::Walk {
            runs: places(nodes),
            starts: places(kicks),
            fired: firing.map(weft_core::frames::Located::top),
            ..Default::default()
        };
        let program = weft_core::project::cut::cut(project, &walk, &mut Default::default()).expect("a cut");
        (program, Some(weft_core::project::hash::compute_definition_hash(project).unwrap()))
    }

    /// THE property this engine rests on: the rows the run wrote,
    /// folded over the program, rebuild the tables the worker held.
    /// Compares the pulses (id, status, closure and its error, frames,
    /// port, value) per node, the records (frames, status,
    /// error, suspension token, port warnings, absorbed pulses) per
    /// node in order, the loop instances (config, source, cap, gather
    /// ports, launched, fired, terminated, carries, gathers, outer
    /// input), and the kicks (dispatched, firing, payload, snapshot,
    /// the scope that skipped it). Record ids, timestamps, logs and
    /// costs are local to each side and are not compared.
    ///
    /// The scopes too: every copy decided, and how (when is each side's
    /// own clock, and is not compared).
    ///
    /// Cut and seeded runs obey the same equality, including which
    /// given values were used and their original runs.
    /// `events` as a record whose every event is a row of its own.
    pub(crate) fn record_rows(events: &[RunEvent]) -> Vec<weft_journal::JournalRow> {
        events.iter().enumerate().map(|(seq, event)| weft_journal::JournalRow { seq: seq as i32, index: 0, event: event.clone() }).collect()
    }

    pub(super) fn assert_fold_matches_live(
        project: &ProjectDefinition,
        events: &[RunEvent],
        sources: Arc<std::collections::BTreeMap<RunId, weft_journal::Fold>>,
        live: &Drove,
    ) {
        /// One pulse as compared: node, id, status (`absorbed` for a pulse
        /// taken off its wire), closed, close error, frames, port, value
        /// (as its JSON text, so the row orders totally).
        type PulseRow = (String, uuid::Uuid, String, bool, Option<weft_core::pulse::Failure>, Vec<String>, String, String, bool, Option<uuid::Uuid>);
        /// One record as compared: node, frames, status, error,
        /// suspension token, absorbed pulses.
        type RecordRow =
            (String, Vec<String>, NodeFiringStatus, Option<String>, Option<String>, Vec<uuid::Uuid>, String, Option<uuid::Uuid>, String, Vec<String>);
        /// One gather port as compared: per iteration index, the write
        /// (a value as JSON text, or the closed slot).
        type GatherRow = (String, Vec<(u32, String)>);
        /// One loop instance as compared: group, parent frames,
        /// config, source, cap, gather ports, launched, fired,
        /// terminated, carries, gathers, outer input (as JSON text).
        type LoopRow = (
            String,
            Vec<String>,
            String,
            String,
            Option<u32>,
            Vec<String>,
            Vec<u32>,
            Vec<u32>,
            Option<String>,
            Vec<(String, String)>,
            Vec<GatherRow>,
            Vec<(String, String)>,
        );
        /// One kick as compared: node, frames, dispatched, firing,
        /// payload, port snapshot, the scope that skipped it.
        type KickRow = (String, Vec<String>, bool, bool, Option<String>, Option<String>, Option<String>);
        // Frames as compared: each loop index and call site, in order.
        let frames_of = |frames: &[weft_core::frames::Frame]| frames.iter().map(|frame| format!("{frame:?}")).collect::<Vec<_>>();
        let birth = events.first().expect("journal has rows");
        let run_id = birth.run_id();
        let snap = {
            // The fold a worker drives a run on, which forgets what
            // ended as the drive does: once the record is read to its
            // end, both forgot the same.
            let mut live = weft_journal::LiveFold::from(weft_journal::Fold::new(run_id, Arc::new(project.clone())).driving(), sources);
            live.apply(&record_rows(events)).expect("journal folds");
            let mut fold = live.into_fold().expect("the record starts with its birth");
            fold.read_to_the_end();
            fold.into_snapshot()
        };
        assert!(snap.corruptions.is_empty(), "the fold rejected a row the run wrote: {:?}", snap.corruptions);
        let json_pairs = |m: &HashMap<String, Arc<serde_json::Value>>| -> Vec<(String, String)> {
            let mut v: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.to_string())).collect();
            v.sort();
            v
        };
        let pulses = |table: &PulseTable| -> Vec<PulseRow> {
            let mut v: Vec<_> = table
                .values()
                .flatten()
                .map(|p| (p, format!("{:?}", p.status)))
                .chain(table.all_absorbed().map(|p| (p, "absorbed".to_string())))
                .map(|(p, status)| {
                    (
                        p.target_node.clone(),
                        p.id,
                        status,
                        p.closed,
                        p.failure.clone(),
                        frames_of(&p.frames),
                        p.target_port.clone(),
                        p.value.to_string(),
                        p.provided,
                        p.inherited_from,
                    )
                })
                .collect();
            v.sort();
            v
        };
        {
            let live_pulses = pulses(&live.pulses);
            let fold_pulses = pulses(&snap.pulses);
            assert_eq!(
                live_pulses.len(),
                fold_pulses.len(),
                "the fold holds a different number of pulses than the run did\nlive: {live_pulses:#?}\nfold: {fold_pulses:#?}"
            );
            for (l, f) in live_pulses.iter().zip(&fold_pulses) {
                assert_eq!(l, f, "a pulse differs between the run and the fold");
            }
        }
        let records = |table: &NodeFiringTable| -> Vec<RecordRow> {
            table
                .iter()
                .flat_map(|(node, recs)| {
                    recs.values().map(move |r| {
                        let mut absorbed = r.pulses_absorbed.clone();
                        absorbed.sort();
                        let mut closed_outputs: Vec<_> = r.closed_output_ports.iter().cloned().collect();
                        closed_outputs.sort();
                        (
                            node.clone(),
                            frames_of(&r.frames),
                            r.status(),
                            r.error.clone(),
                            r.callback_id.clone(),
                            absorbed,
                            weft_core::project::hash::canonical_json(&serde_json::to_value(&r.received).unwrap()),
                            r.inherited_from,
                            serde_json::to_string(&r.skip_reason).unwrap(),
                            closed_outputs,
                        )
                    })
                })
                .collect()
        };
        assert_eq!(records(&live.firings), records(&snap.firings), "the records differ between the run and the fold");
        let loops = |rt: &LoopRuntime| -> Vec<LoopRow> {
            let mut v: Vec<LoopRow> = rt
                .iter()
                .map(|(key, inst)| {
                    let mut gathers: Vec<GatherRow> = inst
                        .gather_lists
                        .iter()
                        .map(|(port, slots)| (port.clone(), slots.iter().map(|(i, w)| (*i, format!("{w:?}"))).collect()))
                        .collect();
                    gathers.sort();
                    let mut gather_ports = inst.gather_ports.clone();
                    gather_ports.sort();
                    let launched: Vec<u32> = inst.launched.iter().collect();
                    let out_fired: Vec<u32> = inst.out_fired.iter().collect();
                    (
                        key.group_id.clone(),
                        frames_of(&key.parent_frames),
                        format!("{:?}", inst.config),
                        format!("{:?}", inst.source),
                        inst.iter_cap,
                        gather_ports,
                        launched,
                        out_fired,
                        inst.terminated.map(|r| format!("{r:?}")),
                        json_pairs(&inst.carry_values),
                        gathers,
                        json_pairs(&inst.outer_input),
                    )
                })
                .collect();
            v.sort();
            v
        };
        assert_eq!(loops(&live.loop_runtime), loops(&snap.loop_runtime), "the loop instances differ between the run and the fold");
        let kicks = |kicked: &weft_core::primitive::KickTable| -> Vec<KickRow> {
            let mut v: Vec<KickRow> = kicked
                .iter()
                .map(|(loc, k)| {
                    (
                        loc.node_id.clone(),
                        frames_of(&loc.frames),
                        k.dispatched(),
                        k.firing,
                        k.payload.as_ref().map(|v| v.to_string()),
                        k.port_snapshot.as_ref().map(|v| v.to_string()),
                        k.scope_skipped.clone(),
                    )
                })
                .collect();
            v.sort();
            v
        };
        assert_eq!(kicks(&live.kicked), kicks(&snap.kicked), "the kicks differ between the run and the fold");
        let scopes = |table: &weft_core::run::scope::ScopeTable| -> Vec<(String, Vec<String>, String)> {
            table
                .iter()
                .map(|(copy, record)| (copy.scope.clone(), copy.frames.iter().map(|f| format!("{f:?}")).collect(), format!("{:?}", record.state)))
                .collect()
        };
        assert_eq!(scopes(&live.scopes), scopes(&snap.scopes), "the scopes differ between the run and the fold");
    }

    /// Drive a run over a journal that already holds `rows` (the
    /// birth row included), the shape of a resume: returns the run's
    /// `Result` untouched, so a test can assert on a run that refuses
    /// to go on, plus every journaled event.
    pub(super) async fn drive_journal(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        run_id: RunId,
        rows: Vec<RunEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<RunOutcome>, Vec<RunEvent>) {
        let (drove, events) = drive_journal_observed(project, catalog, run_id, rows, cancellation).await;
        (drove.map(|d| d.outcome), events)
    }

    /// `drive_journal`, keeping the worker's tables.
    pub(super) async fn drive_journal_observed(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        run_id: RunId,
        rows: Vec<RunEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<Drove>, Vec<RunEvent>) {
        let journal = Arc::new(MemJournal::default());
        journal.seed(&rows);
        let project = Arc::new(project);
        let drove = drive_on(project, catalog, run_id, journal.clone(), clients(journal.clone()), cancellation, None).await;
        let events = journal.events.lock().unwrap().clone();
        (drove, events)
    }

    /// Drive a SEEDED run: the journal already holds `ancestor_rows`
    /// (the seed runs, under their own firings) and the child's birth
    /// rows are `rows`. Answers the outcome and the child's own rows.
    pub(super) async fn drive_seeded(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        run_id: RunId,
        ancestor_rows: Vec<RunEvent>,
        definitions: Vec<ProjectDefinition>,
        rows: Vec<RunEvent>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        let journal = Arc::new(MemJournal::default());
        journal.seed(&ancestor_rows);
        journal.seed(&rows);
        let project = Arc::new(project);
        let mut clients = clients(journal.clone());
        clients.project = Arc::new(ProjectHistory(definitions.into_iter().map(|definition| {
            let key = (definition.id.to_string(), weft_core::project::hash::compute_definition_hash(&definition).unwrap());
            (key, definition)
        }).collect()));
        let drove = drive_on(project, catalog, run_id, journal.clone(), clients, CancellationFlag::new_arc(), None)
            .await
            .expect("run_one ok");
        (drove.outcome, journal.events_of(run_id))
    }

    /// The rig's fake clients over `journal`.
    pub(crate) fn clients(journal: Arc<MemJournal>) -> EngineClients {
        clients_writing(journal, Default::default())
    }

    /// [`clients`], their writer batching as `settings` say, its lanes on
    /// the process's own io runtime as a worker's are (`EngineClients::io`).
    pub(crate) fn clients_writing(journal: Arc<MemJournal>, settings: crate::journal_writer::WriterSettings) -> EngineClients {
        let answers = Arc::new(Answers::default());
        let io = crate::context::io_runtime().expect("the io runtime starts");
        EngineClients {
            writer: crate::journal_writer::WorkerJournal::start(journal, settings, &io),
            io,
            told: answers.told.clone(),
            runs: answers,
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            access_broker: crate::context::FakeAccessBroker::new(),
            open_charges: crate::metering::OpenCharges::new(),
            line: crate::context::TestLine::new(),
            door_broker: crate::door::fake::FakeDoorBroker::new(Vec::new()),
            shared: weft_core::shared::Shared::new(std::time::Duration::MAX),
            steering: Arc::new(NoopSteering),
            needs_from: weft_core::needs::NeedsFrom::Machine,
        }
    }

    /// The handle `clients`' writer writes `run_id`'s record through,
    /// kept as its birth in `events` says; `next_seq` 0 for a run born now,
    /// past 0 for one whose record is already there.
    pub(crate) fn handle(
        clients: &EngineClients,
        run_id: RunId,
        events: &[RunEvent],
        next_seq: i32,
        secrets: Vec<weft_journal::frame::KeptSecret>,
    ) -> Arc<crate::journal_writer::DriveJournal> {
        let settings = events
            .iter()
            .find_map(|event| match event {
                RunEvent::RunStarted { settings, .. } => Some(*settings),
                _ => None,
            })
            .unwrap_or_default();
        clients.writer.run(crate::journal_writer::RunSpec {
            run_id,
            settings,
            keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
            epoch: 1,
            next_seq,
            secrets,
            program: None,
        })
    }

    /// A run born now, the way a worker's door bears one: its handle, its
    /// birth handed to it first.
    pub(crate) async fn born(clients: &EngineClients, run_id: RunId, birth: &[RunEvent]) -> Arc<crate::journal_writer::DriveJournal> {
        let run = handle(clients, run_id, birth, 0, Vec::new());
        run.record_events(birth, Some("instance-test")).await.expect("a birth is handed");
        run
    }

    /// A caller's exchange for a test's fake caller, its record kept
    /// nowhere.
    pub(crate) fn exchange(conn: Arc<dyn weft_core::caller::CallerConnection>) -> crate::run_driver::Exchange {
        crate::run_driver::Exchange { conn, live: None, sink: Arc::new(Unkept) }
    }

    /// A real caller's exchange, its conversation kept nowhere.
    pub(crate) fn live_exchange(conn: Arc<crate::caller_conn::LiveCallerConnection>) -> crate::run_driver::Exchange {
        crate::run_driver::Exchange { conn: conn.clone(), live: Some(conn), sink: Arc::new(Unkept) }
    }

    /// A caller's exchange kept nowhere.
    pub(crate) struct Unkept;
    impl crate::caller_conn::CallerJournalSink for Unkept {
        fn connected(&self, _: RunId, _: u64, _: weft_core::signal::Protocol) {}
        fn inbound(&self, _: RunId, _: u64, _: &weft_core::caller::InboundMessage) {}
        fn outbound(&self, _: RunId, _: u64, _: &weft_core::caller::OutboundChunk, _: bool, _: Option<u64>) {}
        fn errored(&self, _: RunId, _: u64, _: &str) {}
        fn disconnected(&self, _: RunId, _: u64, _: &str, _: &[u64]) {}
        fn close(&self) {}
    }

    /// Drive `run_id` from `first` through `journal`, the way a worker
    /// does, bounded by a failsafe deadline: a regression into a hang must
    /// FAIL the test by name, never wedge the whole test process.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_on(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        clients: &EngineClients,
        run_id: RunId,
        journal: Arc<crate::journal_writer::DriveJournal>,
        first: Vec<RunEvent>,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
        hand_back: Option<&HandBack>,
    ) -> anyhow::Result<Drove> {
        // Quiet runs counted the way a worker counts them, with no door to
        // tell.
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        run_on_holding(project, catalog, clients, run_id, journal, first, cancellation, caller, hand_back, &holds).await
    }

    /// What a worker reads after its claim of `run_id`, whose record is
    /// `first`: the record folded over `project`, a seed's ancestors read
    /// from `clients`' record.
    pub(crate) async fn claimed(clients: &EngineClients, run_id: RunId, project: &Arc<ProjectDefinition>, first: &[RunEvent]) -> crate::run_driver::ClaimedRecord {
        let record = clients.writer.record();
        crate::run_driver::ClaimedRecord::read(
            |run, after| -> futures::future::BoxFuture<'_, anyhow::Result<Vec<weft_journal::JournalRow>>> {
                if run == run_id {
                    let rows = crate::test_record::rows_of(run, first, after);
                    Box::pin(async move { Ok(rows) })
                } else {
                    Box::pin(weft_journal::decoded_page(&**record, run, after))
                }
            },
            &*clients.project,
            run_id,
            project.clone(),
        )
        .await
        .expect("the run's record reads")
    }

    /// [`run_on`], the run's quiet spells counted in `holds`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_on_holding(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        clients: &EngineClients,
        run_id: RunId,
        journal: Arc<crate::journal_writer::DriveJournal>,
        first: Vec<RunEvent>,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
        hand_back: Option<&HandBack>,
        holds: &Arc<crate::run_holds::Holds>,
    ) -> anyhow::Result<Drove> {
        // The tables a worker's claim builds for the run's program.
        let program = Arc::new(crate::plan::ProgramTables::new(project));
        let starts_from = crate::run_driver::StartsFrom::Record(Box::new(claimed(clients, run_id, &program.project, &first).await));
        let hold = Some(holds.of(run_id, None));
        let run = RunDrive { run_id, journal, program, starts_from, cancellation, exchange: caller.map(exchange), hand_back, hold };
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_one_observed(catalog, clients, run, "instance-test", "tenant-test"),
        )
        .await
        .expect("the drive hung: a loud-failure contract regressed into a hang")
    }

    /// Drive `run_id` over what `journal` already holds (`clients` is
    /// built over that same journal, by `clients` or by hand when a
    /// test swaps one fake), and check the rows the run wrote fold
    /// back into the tables it held (`assert_fold_matches_live`).
    /// Every engine test drives through here, so no run escapes that
    /// check.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn drive_on(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        run_id: RunId,
        journal: Arc<MemJournal>,
        clients: EngineClients,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> anyhow::Result<Drove> {
        let projects = clients.project.clone();
        // What a worker's claim would hand the drive: the run's record so far.
        let first = journal.events_of(run_id);
        let run = handle(&clients, run_id, &first, 1, Vec::new());
        let drove = run_on(project.clone(), catalog, &clients, run_id, run, first, cancellation, caller, None).await;
        // A fast run lets go of its record once its ending is handed over;
        // the checks below read the record whole, as the next reader does.
        clients.writer.written().await;
        let drove = drove?;
        // The run's own rows, folded over what it inherits (the journal
        // may hold the seed's rows under another run).
        let events = journal.events_of(run_id);
        let page = |c: RunId, after: Option<i32>| {
            let events = journal.events_of(c);
            async move { Ok(crate::test_record::rows_of(c, &events, after)) }
        };
        let chain = weft_journal::seed_chain(events.first(), &page, |id, hash| {
            let projects = projects.clone();
            async move {
                projects.fetch_definition(id, &hash).await?.map(Arc::new)
                    .ok_or_else(|| anyhow::anyhow!("test seed program {id}/{hash} was not registered"))
            }
        })
            .await
            .expect("the seed chain reads");
        let sources = chain.materialize(&page).await.expect("the seed chain folds");
        assert_fold_matches_live(&project, &events, Arc::new(sources), &drove);
        Ok(drove)
    }

    /// `drive_on`, answering the outcome alone.
    pub(super) async fn run_checked(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        run_id: RunId,
        journal: Arc<MemJournal>,
        clients: EngineClients,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> anyhow::Result<RunOutcome> {
        drive_on(project, catalog, run_id, journal, clients, cancellation, caller).await.map(|d| d.outcome)
    }
