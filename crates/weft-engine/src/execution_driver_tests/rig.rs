    use super::*;
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::node::NodeMetadata;
    use weft_core::ProjectDefinition;
    use weft_journal::{ExecEvent, JournalClient};
    use weft_infra::InfraReader;
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

    /// In-memory recording journal: stores every event and replays them
    /// for the boot fold. Unlike the Noop journals in `replay_tests`,
    /// this actually drives a live execution.
    #[derive(Default)]
    pub(super) struct MemJournal {
        pub(super) events: StdMutex<Vec<ExecEvent>>,
    }
    #[async_trait]
    impl JournalClient for MemJournal {
        async fn record_event(&self, event: &ExecEvent, _pod: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_color(&self, color: Color) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.color() == color)
                .cloned()
                .collect())
        }
        async fn raw_events_for_color(&self, color: Color) -> anyhow::Result<Vec<String>> {
            Ok(self
                .events_for_color(color)
                .await?
                .iter()
                .map(|e| serde_json::to_string(e).expect("serialize ExecEvent"))
                .collect())
        }
        async fn has_terminal_event(&self, color: Color) -> anyhow::Result<bool> {
            Ok(self.events.lock().unwrap().iter().any(|e| matches!(
                e,
                ExecEvent::ExecutionCompleted { color: c, .. }
                    | ExecEvent::ExecutionFailed { color: c, .. }
                    | ExecEvent::ExecutionCancelled { color: c, .. } if *c == color
            )))
        }
    }

    pub(super) struct NoopTasks;
    #[async_trait]
    impl weft_task_store::TaskStoreClient for NoopTasks {
        async fn enqueue_dedup(&self, _s: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            unreachable!("rig tests enqueue no tasks")
        }
        async fn wait_for_terminal(&self, _t: uuid::Uuid, _to: std::time::Duration, _pi: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            unreachable!()
        }
        async fn claim_one(&self, _p: &str, _f: weft_task_store::tasks::ClaimFilter) -> anyhow::Result<Option<weft_task_store::tasks::Task>> { Ok(None) }
        async fn heartbeat(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> { Ok(true) }
        async fn requeue(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> { Ok(true) }
        async fn complete(&self, _t: uuid::Uuid, _p: &str, _r: Value) -> anyhow::Result<()> { Ok(()) }
        async fn fail(&self, _t: uuid::Uuid, _p: &str, _e: String) -> anyhow::Result<()> { Ok(()) }
    }
    pub(super) struct NoopSteering;
    #[async_trait]
    impl crate::context::ExecutionSteeringClient for NoopSteering {
        async fn tag_execution(&self, _c: Color, _t: Vec<String>, _p: &str) -> anyhow::Result<()> {
            unreachable!("rig tests steer no executions")
        }
        async fn stop_tagged(&self, _c: Color, _t: String, _s: weft_core::StopSelf, _p: &str) -> anyhow::Result<()> {
            unreachable!("rig tests steer no executions")
        }
    }
    pub(super) struct NoopInfra;
    #[async_trait]
    impl InfraReader for NoopInfra {
        async fn endpoint_url(&self, _p: &str, _n: &str, _e: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    }
    pub(super) struct NoopInfraState;
    #[async_trait]
    impl InfraStateClient for NoopInfraState {
        async fn enqueue_apply(&self, _p: &str, _n: &str, _s: serde_json::Value) -> anyhow::Result<i64> { Ok(0) }
        async fn wait_apply(&self, _p: &str, _c: i64) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
    }
    pub(super) struct NoopProject;
    #[async_trait]
    impl crate::context::ProjectClient for NoopProject {
        async fn fetch_definition(
            &self,
            _project_id: &str,
            _expected_hash: &str,
        ) -> anyhow::Result<Option<ProjectDefinition>> {
            // These execution_driver tests inject the project into
            // `run_one_execution` directly, so the per-execution
            // fetch path is never invoked here. Bail loud if it is.
            anyhow::bail!("NoopProject::fetch_definition not implemented in execution_driver tests")
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
    /// dispatcher's route_entry writes it), and `subgraph` is the fire's
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

    /// `drive` for a color whose journal ALREADY holds a terminal
    /// (cancelled before the worker claimed it): the shape of a cancel
    /// landing in the dispatcher's route window, or a late second
    /// execute task for a finished color.
    pub(super) async fn drive_settled(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        drive_kicked_settled(project, catalog, kicks, None, None, CancellationFlag::new_arc(), true).await
    }

    async fn drive_kicked(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        firing: Option<&str>,
        subgraph: Option<&[&str]>,
        cancellation: Arc<CancellationFlag>,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        drive_kicked_settled(project, catalog, kicks, firing, subgraph, cancellation, false).await
    }

    async fn drive_kicked_settled(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        kicks: &[&str],
        firing: Option<&str>,
        subgraph: Option<&[&str]>,
        cancellation: Arc<CancellationFlag>,
        already_cancelled: bool,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let color = uuid::Uuid::new_v4();
        let mut rows = vec![ExecEvent::ExecutionStarted {
            color,
            project_id: project.id.to_string(),
            entry_node: kicks[0].to_string(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some("test-hash".into()),
            node_test: false,
            subgraph: subgraph.map(|s| s.iter().map(|n| n.to_string()).collect()),
            at_unix: 0,
        }];
        for kick in kicks {
            rows.push(ExecEvent::NodeKicked {
                color,
                node_id: kick.to_string(),
                firing: firing == Some(*kick),
                payload: None,
                port_snapshot: None,
                at_unix: 0,
            });
        }
        if already_cancelled {
            rows.push(ExecEvent::ExecutionCancelled {
                color,
                reason: "cancelled in the route window".into(),
                cause: Some(weft_core::exec::CancelCause::User),
                at_unix: 0,
            });
        }
        let (drove, events) = drive_journal_observed(project, catalog, color, rows, cancellation).await;
        let drove = drove.expect("run_one_execution ok");
        // A color already settled before the worker claimed it drove
        // nothing: its tables are asserted empty (the fold of the same
        // rows holds the kicks, so the two are not compared).
        if already_cancelled {
            assert!(matches!(drove.outcome, ExecutionOutcome::AlreadySettled), "{:?}", drove.outcome);
            assert!(
                drove.pulses.is_empty() && drove.executions.is_empty() && drove.loop_runtime.iter().next().is_none(),
                "a settled color drives nothing"
            );
        }
        (drove.outcome, events)
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
    /// A run bounded to a subgraph is the one shape where the pulses
    /// legitimately differ (an out-of-scope node's pulses are absorbed
    /// live with no row, and refold Pending, to be absorbed the same
    /// silent way again), so for such a run (read off its birth row)
    /// the pulses are left out and everything else is still compared.
    pub(super) fn assert_fold_matches_live(
        project: &ProjectDefinition,
        events: &[ExecEvent],
        live: &Drove,
    ) {
        /// One pulse as compared: node, id, status, closed, close
        /// error, frames, port, value (as its JSON text, so the row
        /// orders totally).
        type PulseRow = (String, uuid::Uuid, String, bool, Option<String>, Vec<u32>, String, String);
        /// One record as compared: node, frames, ordinal, status,
        /// error, suspension token, port warnings, absorbed pulses.
        type RecordRow =
            (String, Vec<u32>, usize, NodeExecutionStatus, Option<String>, Option<String>, String, Vec<uuid::Uuid>);
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
        let color = birth.color();
        let subgraph_bounded = matches!(birth, ExecEvent::ExecutionStarted { subgraph: Some(_), .. });
        let snap = weft_journal::fold_to_snapshot(color, Arc::new(project.clone()), events);
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
                            p.close_error.clone(),
                            p.frames.iter().map(|f| f.index).collect::<Vec<_>>(),
                            p.target_port.clone(),
                            p.value.to_string(),
                        )
                    })
                })
                .collect();
            v.sort();
            v
        };
        if !subgraph_bounded {
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
                        (
                            node.clone(),
                            r.frames.iter().map(|f| f.index).collect::<Vec<_>>(),
                            r.ordinal,
                            r.status.clone(),
                            r.error.clone(),
                            r.callback_id.clone(),
                            format!("{:?}", r.port_warnings),
                            absorbed,
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
                        key.parent_frames.iter().map(|f| f.index).collect(),
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
                        loc.frames.iter().map(|f| f.index).collect(),
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

    /// Drive a color over a journal that already holds `rows` (the
    /// birth row included), the shape of a resume: returns the run's
    /// `Result` untouched, so a test can assert on a run that refuses
    /// to go on, plus every journaled event.
    pub(super) async fn drive_journal(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        color: Color,
        rows: Vec<ExecEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<ExecutionOutcome>, Vec<ExecEvent>) {
        let (drove, events) = drive_journal_observed(project, catalog, color, rows, cancellation).await;
        (drove.map(|d| d.outcome), events)
    }

    /// `drive_journal`, keeping the worker's tables.
    pub(super) async fn drive_journal_observed(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        color: Color,
        rows: Vec<ExecEvent>,
        cancellation: Arc<CancellationFlag>,
    ) -> (anyhow::Result<Drove>, Vec<ExecEvent>) {
        let journal = Arc::new(MemJournal::default());
        for row in &rows {
            journal.record_event(row, None).await.unwrap();
        }
        let project = Arc::new(project);
        let drove = drive_on(project, catalog, color, journal.clone(), clients(journal.clone()), cancellation, None).await;
        let events = journal.events.lock().unwrap().clone();
        (drove, events)
    }

    /// The rig's fake clients over `journal`.
    pub(super) fn clients(journal: Arc<MemJournal>) -> EngineClients {
        EngineClients {
            journal,
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            access_broker: crate::context::FakeAccessBroker::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
            steering: Arc::new(NoopSteering),
        }
    }

    /// Drive `color` over what `journal` already holds (`clients` is
    /// built over that same journal, by `clients` or by hand when a
    /// test swaps one fake), and check the rows the run wrote fold
    /// back into the tables it held (`assert_fold_matches_live`).
    /// Every engine test drives through here, so no run escapes that
    /// check. A run that found its color already settled drove
    /// nothing and holds nothing to compare.
    pub(super) async fn drive_on(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        color: Color,
        journal: Arc<MemJournal>,
        clients: EngineClients,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> anyhow::Result<Drove> {
        let drove = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_one_execution_observed(
                project.clone(),
                catalog,
                color,
                clients,
                "pod-test".into(),
                "tenant-test".into(),
                "ns-test".into(),
                cancellation,
                caller,
            ),
        )
        .await
        .expect("the drive hung: a loud-failure contract regressed into a hang")?;
        if !matches!(drove.outcome, ExecutionOutcome::AlreadySettled) {
            let events = journal.events.lock().unwrap().clone();
            assert_fold_matches_live(&project, &events, &drove);
        }
        Ok(drove)
    }

    /// `drive_on`, answering the outcome alone.
    pub(super) async fn run_checked(
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        color: Color,
        journal: Arc<MemJournal>,
        clients: EngineClients,
        cancellation: Arc<CancellationFlag>,
        caller: Option<Arc<dyn weft_core::caller::CallerConnection>>,
    ) -> anyhow::Result<ExecutionOutcome> {
        drive_on(project, catalog, color, journal, clients, cancellation, caller).await.map(|d| d.outcome)
    }
