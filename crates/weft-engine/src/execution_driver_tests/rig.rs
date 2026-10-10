    use super::*;
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::node::NodeMetadata;
    use weft_core::ProjectDefinition;
    use weft_journal::{ExecEvent, JournalClient};
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
                    M.get_or_init(|| crate::execution_driver::engine_test_rig::trivial_metadata($ty))
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

    pub(super) fn catalog(
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
    /// back on its own. Its selections are kept by digest, as the record
    /// keeps them.
    #[derive(Default)]
    pub(crate) struct MemJournal {
        pub(super) events: StdMutex<Vec<ExecEvent>>,
        selections: StdMutex<HashMap<String, weft_core::project::selection::RecordedSelection>>,
        /// How many batches the writer sent.
        pub(crate) batches: std::sync::atomic::AtomicUsize,
    }
    impl MemJournal {
        /// Put `events` on record as they are: a run's record (or a seed's)
        /// before a test drives it.
        pub(crate) fn seed(&self, events: &[ExecEvent]) {
            for event in events {
                if let ExecEvent::ExecutionStarted { selection: Some(selection), .. } = event {
                    self.selections.lock().unwrap().insert(selection.digest().to_string(), selection.clone());
                }
            }
            self.events.lock().unwrap().extend(events.iter().cloned());
        }

        /// Every event on record of `execution_id`, in order.
        pub(crate) fn events_of(&self, execution_id: ExecutionId) -> Vec<ExecEvent> {
            self.events.lock().unwrap().iter().filter(|event| event.execution_id() == execution_id).cloned().collect()
        }
    }
    /// Straight onto the record, as a test puts a run's record (or a
    /// seed's) there before it drives it.
    #[async_trait]
    impl JournalClient for MemJournal {
        async fn record_event(&self, event: &ExecEvent, _replica: Option<&str>) -> anyhow::Result<()> {
            self.seed(std::slice::from_ref(event));
            Ok(())
        }
        async fn events_for_execution_id(&self, execution_id: ExecutionId) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(self.events_of(execution_id))
        }
    }
    #[async_trait]
    impl weft_journal::RecordClient for MemJournal {
        async fn record_batch(&self, batch: Vec<u8>) -> Result<weft_journal::frame::BatchAnswer, weft_journal::BatchError> {
            self.batches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (head, runs) = crate::test_record::decode_batch(&batch);
            for stored in head.selections {
                let selection = weft_core::project::selection::RecordedSelection::read(stored.digest.clone(), stored.selection);
                self.selections.lock().unwrap().insert(stored.digest, selection);
            }
            let fates = runs.iter().map(|_| weft_journal::record::Fate::Accepted).collect();
            self.events.lock().unwrap().extend(runs.into_iter().flat_map(|(_, events)| events));
            Ok(weft_journal::frame::BatchAnswer { fates })
        }
        async fn record_of(&self, execution_id: ExecutionId) -> anyhow::Result<weft_journal::record::RawRecord> {
            let events = self.events_of(execution_id);
            let selection = events.iter().find_map(|event| match event {
                ExecEvent::ExecutionStarted { selection: Some(selection), .. } => Some(weft_journal::record::StoredSelection::of(selection)),
                _ => None,
            });
            Ok(weft_journal::record::RawRecord {
                selection,
                rows: vec![weft_journal::record::RunLogRow { seq: 0, events: weft_journal::stored::encode(&events) }],
            })
        }
        /// The record writes the ending of a run its worker gave up on.
        async fn give_up(&self, execution_id: ExecutionId, why: String) -> Result<(), weft_journal::BatchError> {
            self.events.lock().unwrap().push(ExecEvent::ExecutionFailed { execution_id, error: why, at_unix: 0 });
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
            if t.kind == weft_task_store::TaskKind::WithdrawSignal.as_str() {
                let withdraw: weft_task_store::WithdrawSignalPayload = serde_json::from_value(t.payload)?;
                self.withdrawn.lock().unwrap().push(withdraw.token);
                return Ok(weft_task_store::tasks::DedupOutcome::Inserted(id));
            }
            assert_eq!(t.kind, weft_task_store::TaskKind::RegisterSignal.as_str(), "await tests only register and withdraw waits");
            // Deterministic token derived from the task id.
            let token = format!("tok-{id}");
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
    impl crate::context::ExecutionSteeringClient for NoopSteering {
        async fn tag_execution(&self, _c: ExecutionId, _t: Vec<String>) -> anyhow::Result<()> {
            unreachable!("rig tests steer no executions")
        }
        async fn stop_tagged(&self, _c: ExecutionId, _t: String, _s: weft_core::StopSelf) -> anyhow::Result<bool> {
            unreachable!("rig tests steer no executions")
        }
    }
    /// The broker's side of the runs a rig drives: the answers a test
    /// gives, handed to whoever asks next (held until one comes, or the
    /// hold runs out), and the runs let go of.
    #[derive(Default)]
    pub(crate) struct Answers {
        waiting: StdMutex<Vec<weft_broker_client::protocol::RunAnswer>>,
        came: tokio::sync::Notify,
        pub(crate) let_go: StdMutex<Vec<(ExecutionId, weft_broker_client::protocol::LetGo)>>,
    }
    impl Answers {
        /// An answer to the wait `token`, as the install parks one for the
        /// run's worker to take.
        pub(crate) fn answer(&self, token: impl Into<String>, value: Value) {
            self.waiting.lock().unwrap().push(weft_broker_client::protocol::RunAnswer {
                token: token.into(),
                answer: weft_core::primitive::WaitAnswer::Given { value },
            });
            self.came.notify_waiters();
        }
    }
    #[async_trait]
    impl crate::context::RunClient for Answers {
        async fn claim(&self, _execution_id: ExecutionId) -> anyhow::Result<Option<weft_journal::record::Claimed>> {
            Ok(None)
        }
        async fn let_go(&self, execution_id: ExecutionId, why: weft_broker_client::protocol::LetGo) -> anyhow::Result<()> {
            self.let_go.lock().unwrap().push((execution_id, why));
            Ok(())
        }
        async fn answers(&self, _execution_id: ExecutionId, _taken: &[String], wait: std::time::Duration) -> anyhow::Result<Vec<weft_broker_client::protocol::RunAnswer>> {
            let deadline = tokio::time::Instant::now() + wait;
            loop {
                let came = self.came.notified();
                tokio::pin!(came);
                came.as_mut().enable();
                let answers = std::mem::take(&mut *self.waiting.lock().unwrap());
                if !answers.is_empty() || tokio::time::timeout_at(deadline, came).await.is_err() {
                    return Ok(answers);
                }
            }
        }
        async fn cancels(&self) -> anyhow::Result<Vec<weft_broker_client::protocol::RunCancel>> {
            Ok(Vec::new())
        }
    }
    pub(super) struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_address(&self, _c: weft_core::ExecutionId, _r: Option<&weft_core::instance::InstanceId>, _i: &weft_core::infra::InfraHandle) -> anyhow::Result<Option<weft_core::infra::EndpointAddress>> { Ok(None) }
        async fn baked_outputs(&self, _c: weft_core::ExecutionId, _r: Option<&weft_core::instance::InstanceId>, _p: &str, _m: Option<&weft_core::instance::InstanceId>) -> anyhow::Result<std::collections::BTreeMap<String, serde_json::Value>> { Ok(Default::default()) }
    }
    pub(super) struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(&self, _p: uuid::Uuid, _n: &str, _m: Option<&weft_core::instance::InstanceId>, _s: serde_json::Value) -> anyhow::Result<i64> { Ok(0) }
        async fn wait_apply(&self, _p: uuid::Uuid, _c: i64, _w: std::time::Duration) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
        async fn save_bake(&self, _x: weft_core::ExecutionId, _p: &str, _m: Option<&weft_core::instance::InstanceId>, _v: std::collections::BTreeMap<String, serde_json::Value>) -> anyhow::Result<()> { Ok(()) }
    }
    pub(super) struct NoopProject;
    #[async_trait]
    impl crate::context::ProjectClient for NoopProject {
        async fn fetch_definition(
            &self,
            _project_id: uuid::Uuid,
            _expected_hash: &str,
        ) -> anyhow::Result<Option<ProjectDefinition>> {
            // These execution_driver tests inject the project into
            // `run_one_execution` directly, so the per-execution
            // fetch path is never invoked here. Bail loud if it is.
            anyhow::bail!("NoopProject::fetch_definition not implemented in execution_driver tests")
        }
    }

    struct ProjectHistory(HashMap<(String, String), ProjectDefinition>);

    #[async_trait]
    impl crate::context::ProjectClient for ProjectHistory {
        async fn fetch_definition(&self, project_id: uuid::Uuid, expected_hash: &str) -> anyhow::Result<Option<ProjectDefinition>> {
            Ok(self.0.get(&(project_id.to_string(), expected_hash.into())).cloned())
        }
    }

    /// Seed + drive one execution: ExecutionStarted(Fire) + a kick per
    /// entry node, then `run_one_execution`. Returns the outcome and
    /// every journaled event.
    pub(super) async fn drive(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
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
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        drive_scoped(project, catalog, kicks, None, cancellation).await
    }

    /// `drive` with a journaled run subgraph, the shape a trigger fire
    /// produces: only the named nodes dispatch, and a pulse landing
    /// anywhere else is absorbed without a row.
    pub(super) async fn drive_scoped(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        subgraph: Option<&[&str]>,
        cancellation: Arc<CancellationFlag>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        drive_kicked(project, catalog, kicks, None, subgraph, cancellation).await
    }

    /// `drive_scoped` for a TRIGGER FIRE: `firing` names the kick that
    /// is the fired trigger (journaled with `firing: true`, the way the
    /// worker's door writes a fire's birth), and `subgraph` is the fire's
    /// computed set. The shape every two-programs test drives.
    pub(super) async fn drive_fire(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        firing: &str,
        kicks: &[&str],
        subgraph: Option<&[&str]>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        drive_kicked(project, catalog, kicks, Some(firing), subgraph, CancellationFlag::new_arc()).await
    }

    async fn drive_kicked(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        firing: Option<&str>,
        subgraph: Option<&[&str]>,
        cancellation: Arc<CancellationFlag>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let execution_id = weft_core::new_execution_id();
        let mut rows = vec![ExecEvent::ExecutionStarted {
            execution_id,
            project_id: project.id,
            entry_node: kicks[0].to_string(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
            binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
            selection: subgraph.map(|s| weft_core::project::selection::RecordedSelection::new(weft_core::project::selection::RunSelection::restricted(
                &project, s.iter().map(|n| weft_core::frames::Located::top(*n)).collect()).expect("valid test selection"))),
            seed: None,
            instance: None, fired_trigger: None, stand_in: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }];
        for kick in kicks {
            rows.push(ExecEvent::NodeKicked {
                execution_id,
                node_id: kick.to_string(), frames: vec![],
                firing: firing == Some(*kick),
                payload: None,
                port_snapshot: None,
                at_unix: 0,
            });
        }
        let (drove, events) = drive_journal_observed(project, catalog, execution_id, rows, cancellation).await;
        (drove.expect("run_one_execution ok").outcome, events)
    }

    /// THE property this engine rests on: the rows the run wrote,
    /// folded over the program, rebuild the tables the worker held.
    /// Compares the pulses (id, status, closure and its error, frames,
    /// port, value) per node, the records (frames, ordinal, status,
    /// error, suspension token, port warnings, absorbed pulses) per
    /// node in order, the loop instances (config, source, cap, gather
    /// ports, launched, fired, terminated, carries, gathers, outer
    /// input), and the kicks (dispatched, firing, payload, snapshot,
    /// the scope that skipped it). Record ids, timestamps, logs and
    /// costs are local to each side and are not compared.
    ///
    /// Selected and seeded runs obey the same equality, including
    /// which supplied values were used and their original runs.
    pub(super) fn assert_fold_matches_live(
        project: &ProjectDefinition,
        events: &[ExecEvent],
        chain: &weft_journal::SeedChain,
        live: &Drove,
    ) {
        /// One pulse as compared: node, id, status, closed, close
        /// error, frames, port, value (as its JSON text, so the row
        /// orders totally).
        type PulseRow = (String, uuid::Uuid, String, bool, Option<weft_core::pulse::Failure>, Vec<u32>, String, String, bool, bool, Option<uuid::Uuid>);
        /// One record as compared: node, frames, ordinal, status,
        /// error, suspension token, absorbed pulses.
        type RecordRow =
            (String, Vec<u32>, usize, NodeExecutionStatus, Option<String>, Option<String>, Vec<uuid::Uuid>, String, Option<uuid::Uuid>, String, Vec<String>);
        /// One gather port as compared: per iteration index, the write
        /// (a value as JSON text, or the closed slot).
        type GatherRow = (String, Vec<(u32, String)>);
        /// One loop instance as compared: group, parent frames,
        /// config, source, cap, gather ports, launched, fired,
        /// terminated, carries, gathers, outer input (as JSON text).
        type LoopRow = (
            String,
            Vec<u32>,
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
        type KickRow = (String, Vec<u32>, bool, bool, Option<String>, Option<String>, Option<String>);
        let birth = events.first().expect("journal has rows");
        let execution_id = birth.execution_id();
        let snap = weft_journal::fold_seeded(execution_id, Arc::new(project.clone()), chain, events).expect("journal folds");
        assert!(snap.corruptions.is_empty(), "the fold rejected a row the run wrote: {:?}", snap.corruptions);
        let json_pairs = |m: &HashMap<String, Arc<serde_json::Value>>| -> Vec<(String, String)> {
            let mut v: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.to_string())).collect();
            v.sort();
            v
        };
        let pulses = |table: &PulseTable| -> Vec<PulseRow> {
            let mut v: Vec<_> = table
                .iter()
                .flat_map(|(node, bucket)| {
                    bucket.iter().map(move |p| {
                        (
                            node.clone(),
                            p.id,
                            format!("{:?}", p.status),
                            p.closed,
                            p.failure.clone(),
                            p.frames.iter().map(|f| f.loop_index().expect("loop frame")).collect::<Vec<_>>(),
                            p.target_port.clone(),
                            p.value.to_string(),
                            p.provided,
                            p.backup,
                            p.inherited_from,
                        )
                    })
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
        let records = |table: &NodeExecutionTable| -> Vec<RecordRow> {
            table
                .iter()
                .flat_map(|(node, recs)| {
                    recs.iter().map(move |r| {
                        let mut absorbed = r.pulses_absorbed.clone();
                        absorbed.sort();
                        let mut closed_outputs: Vec<_> = r.closed_output_ports.iter().cloned().collect();
                        closed_outputs.sort();
                        (
                            node.clone(),
                            r.frames.iter().map(|f| f.loop_index().expect("loop frame")).collect::<Vec<_>>(),
                            r.ordinal,
                            r.status.clone(),
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
        assert_eq!(records(&live.executions), records(&snap.executions), "the records differ between the run and the fold");
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
                    let mut launched = inst.launched.clone();
                    launched.sort_unstable();
                    let mut out_fired = inst.out_fired.clone();
                    out_fired.sort_unstable();
                    (
                        key.group_id.clone(),
                        key.parent_frames.iter().map(|f| f.loop_index().expect("loop frame")).collect(),
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
        let kicks = |kicked: &HashMap<FiringLocation, weft_core::primitive::KickedNode>| -> Vec<KickRow> {
            let mut v: Vec<KickRow> = kicked
                .iter()
                .map(|(loc, k)| {
                    (
                        loc.node_id.clone(),
                        loc.frames.iter().map(|f| f.loop_index().expect("loop frame")).collect(),
                        k.dispatched,
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
    }

    /// Drive an execution over a journal that already holds `rows` (the
    /// birth row included), the shape of a resume: returns the run's
    /// `Result` untouched, so a test can assert on a run that refuses
    /// to go on, plus every journaled event.
    pub(super) async fn drive_journal(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        execution_id: ExecutionId,
        rows: Vec<ExecEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<ExecutionOutcome>, Vec<ExecEvent>) {
        let (drove, events) = drive_journal_observed(project, catalog, execution_id, rows, cancellation).await;
        (drove.map(|d| d.outcome), events)
    }

    /// `drive_journal`, keeping the worker's tables.
    pub(super) async fn drive_journal_observed(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        execution_id: ExecutionId,
        rows: Vec<ExecEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<Drove>, Vec<ExecEvent>) {
        drive_rows(project, catalog, execution_id, rows, cancellation).await
    }

    async fn drive_rows(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        execution_id: ExecutionId,
        rows: Vec<ExecEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<Drove>, Vec<ExecEvent>) {
        let journal = Arc::new(MemJournal::default());
        journal.seed(&rows);
        let project = Arc::new(project);
        let drove = drive_on(project, catalog, execution_id, journal.clone(), clients(journal.clone()), cancellation, None).await;
        let events = journal.events.lock().unwrap().clone();
        (drove, events)
    }

    /// Drive a SEEDED run: the journal already holds `ancestor_rows`
    /// (the seed runs, under their own executions) and the child's birth
    /// rows are `rows`. Answers the outcome and the child's own rows.
    pub(super) async fn drive_seeded(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        execution_id: ExecutionId,
        ancestor_rows: Vec<ExecEvent>,
        definitions: Vec<ProjectDefinition>,
        rows: Vec<ExecEvent>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let journal = Arc::new(MemJournal::default());
        journal.seed(&ancestor_rows);
        journal.seed(&rows);
        let project = Arc::new(project);
        let mut clients = clients(journal.clone());
        clients.project = Arc::new(ProjectHistory(definitions.into_iter().map(|definition| {
            let key = (definition.id.to_string(), weft_core::project::hash::compute_definition_hash(&definition).unwrap());
            (key, definition)
        }).collect()));
        let drove = drive_on(project, catalog, execution_id, journal.clone(), clients, CancellationFlag::new_arc(), None)
            .await
            .expect("run_one_execution ok");
        (drove.outcome, journal.events_of(execution_id))
    }

    /// The rig's fake clients over `journal`.
    pub(crate) fn clients(journal: Arc<MemJournal>) -> EngineClients {
        clients_writing(journal, Default::default())
    }

    /// [`clients`], their writer batching as `settings` say, its lanes on
    /// the process's own io runtime as a worker's are.
    pub(crate) fn clients_writing(journal: Arc<MemJournal>, settings: crate::journal_writer::WriterSettings) -> EngineClients {
        EngineClients {
            writer: crate::journal_writer::WorkerJournal::start(journal, settings, &crate::context::io_runtime().expect("the io runtime starts")),
            runs: Arc::new(Answers::default()),
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
        }
    }

    /// The handle `clients`' writer writes `execution_id`'s record through,
    /// kept as its birth in `events` says; `next_seq` 0 for a run born now,
    /// past 0 for one whose record is already there.
    pub(crate) fn handle(
        clients: &EngineClients,
        execution_id: ExecutionId,
        events: &[ExecEvent],
        next_seq: i32,
        redaction: weft_core::caller::Redaction,
    ) -> Arc<crate::journal_writer::DriveJournal> {
        let settings = events
            .iter()
            .find_map(|event| match event {
                ExecEvent::ExecutionStarted { settings, .. } => Some(*settings),
                _ => None,
            })
            .unwrap_or_default();
        clients.writer.run(crate::journal_writer::RunSpec {
            execution_id,
            settings,
            keep_for: weft_core::run_settings::KeepFor::WEFT_DEFAULT,
            epoch: 1,
            next_seq,
            redaction,
        })
    }

    /// A run born now, the way a worker's door bears one: its handle, its
    /// birth handed to it first.
    pub(crate) async fn born(clients: &EngineClients, execution_id: ExecutionId, birth: &[ExecEvent]) -> Arc<crate::journal_writer::DriveJournal> {
        let run = handle(clients, execution_id, birth, 0, Default::default());
        run.record_events(birth, Some("instance-test")).await.expect("a birth is handed");
        run
    }

    /// A caller's exchange for a test's fake caller, its record kept
    /// nowhere.
    pub(crate) fn exchange(conn: Arc<dyn weft_core::caller::CallerConnection>) -> crate::execution_driver::Exchange {
        struct Unkept;
        impl crate::caller_conn::CallerJournalSink for Unkept {
            fn connected(&self, _: ExecutionId, _: u64, _: weft_core::signal::Protocol) {}
            fn inbound(&self, _: ExecutionId, _: u64, _: &weft_core::caller::InboundMessage) {}
            fn outbound(&self, _: ExecutionId, _: u64, _: &weft_core::caller::OutboundChunk, _: bool) {}
            fn errored(&self, _: ExecutionId, _: u64, _: &str) {}
            fn disconnected(&self, _: ExecutionId, _: u64, _: &str) {}
            fn close(&self) {}
        }
        crate::execution_driver::Exchange { conn, live: None, sink: Arc::new(Unkept) }
    }

    /// Drive `execution_id` from `first` through `journal`, the way a worker
    /// does, bounded by a failsafe deadline: a regression into a hang must
    /// FAIL the test by name, never wedge the whole test process.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn run_on(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        clients: &EngineClients,
        execution_id: ExecutionId,
        journal: Arc<crate::journal_writer::DriveJournal>,
        first: Vec<ExecEvent>,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
        hand_back: Option<&HandBack>,
    ) -> anyhow::Result<Drove> {
        // The tables a worker's claim builds for the run: the program and
        // the part of it its birth names.
        let selection = first.iter().find_map(|event| match event {
            ExecEvent::ExecutionStarted { selection, .. } => selection.clone(),
            _ => None,
        });
        let program = Arc::new(crate::plan::ProgramTables::new(project, selection));
        let starts_from = crate::execution_driver::StartsFrom::Record(first);
        let run = RunDrive { execution_id, journal, program, starts_from, cancellation, exchange: caller.map(exchange), hand_back };
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_one_execution_observed(catalog, clients, run, "instance-test", "tenant-test"),
        )
        .await
        .expect("the drive hung: a loud-failure contract regressed into a hang")
    }

    /// Drive `execution_id` over what `journal` already holds (`clients` is
    /// built over that same journal, by `clients` or by hand when a
    /// test swaps one fake), and check the rows the run wrote fold
    /// back into the tables it held (`assert_fold_matches_live`).
    /// Every engine test drives through here, so no run escapes that
    /// check.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn drive_on(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        execution_id: ExecutionId,
        journal: Arc<MemJournal>,
        clients: EngineClients,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> anyhow::Result<Drove> {
        let projects = clients.project.clone();
        // What a worker's claim would hand the drive: the run's record so far.
        let first = journal.events_of(execution_id);
        let run = handle(&clients, execution_id, &first, 1, Default::default());
        let drove = run_on(project.clone(), catalog, &clients, execution_id, run, first, cancellation, caller, None).await;
        // A fast run lets go of its record once its ending is handed over;
        // the checks below read the record whole, as the next reader does.
        clients.writer.written().await;
        let drove = drove?;
        // The run's own rows, folded over what it inherits (the journal
        // may hold the seed's rows under another execution).
        let events = journal.events_of(execution_id);
        let chain = weft_journal::seed_chain(&events, |c| { let events = journal.events_of(c); async move { Ok(events) } }, |id, hash| {
            let projects = projects.clone();
            async move {
                projects.fetch_definition(id, &hash).await?.map(Arc::new)
                    .ok_or_else(|| anyhow::anyhow!("test seed program {id}/{hash} was not registered"))
            }
        })
            .await
            .expect("the seed chain reads");
        assert_fold_matches_live(&project, &events, &chain, &drove);
        Ok(drove)
    }

    /// `drive_on`, answering the outcome alone.
    pub(super) async fn run_checked(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        execution_id: ExecutionId,
        journal: Arc<MemJournal>,
        clients: EngineClients,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> anyhow::Result<ExecutionOutcome> {
        drive_on(project, catalog, execution_id, journal, clients, cancellation, caller).await.map(|d| d.outcome)
    }
