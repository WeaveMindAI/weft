    //! The engine-side phase routing: nodes never read the phase; the
    //! engine picks the trait method from the manifest. Layer 1 pins the
    //! pure `node_body_for` rule; layer 3 drives real executions through
    //! `run_one_execution` and asserts which methods actually ran.

    use super::engine_test_rig::{test_manifest, MemJournal, NoopInfra, NoopInfraState, NoopProject, NoopTasks};
    use super::*;
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, NodeCatalog, ProjectDefinition};
    use weft_journal::{ExecEvent, JournalClient};
    use crate::context::EngineClients;

    #[test]
    fn node_body_for_routes_by_phase_and_manifest() {
        use weft_core::context::Phase;
        // A plain node runs its normal body in EVERY phase, wake or not.
        for phase in [Phase::InfraSetup, Phase::TriggerSetup, Phase::Fire] {
            for has_wake in [false, true] {
                assert_eq!(node_body_for(phase, false, has_wake), NodeBody::Run);
            }
        }
        // A trigger registers at setup; at Fire it runs ONLY as the
        // firing trigger (the dispatch carrying the wake payload) and
        // otherwise closes its ports; at infra setup it closes too.
        assert_eq!(node_body_for(Phase::TriggerSetup, true, false), NodeBody::SetupTrigger);
        assert_eq!(node_body_for(Phase::Fire, true, true), NodeBody::Run);
        assert_eq!(node_body_for(Phase::Fire, true, false), NodeBody::SkipTrigger);
        assert_eq!(node_body_for(Phase::InfraSetup, true, false), NodeBody::SkipTrigger);
    }

    /// Plain node: records the call, emits `value` so a downstream
    /// trigger's input is fed.
    struct Src {
        calls: Arc<StdMutex<Vec<&'static str>>>,
    }
    test_manifest!(Src, "Src");
    #[async_trait]
    impl Node for Src {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.calls.lock().unwrap().push("src:run");
            ctx.pulse_downstream(NodeOutput::new().set("value", "v")).await
        }
    }

    /// Trigger node implementing both bodies: each records which one the
    /// engine picked. `setup_trigger` deliberately registers nothing (the
    /// routing is what is under test, not the signal plumbing).
    struct Trig {
        calls: Arc<StdMutex<Vec<&'static str>>>,
    }
    test_manifest!(Trig, "Trig");
    #[async_trait]
    impl Node for Trig {
        async fn setup_trigger(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            self.calls.lock().unwrap().push("trig:setup_trigger");
            Ok(())
        }
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            self.calls.lock().unwrap().push("trig:run");
            Ok(())
        }
    }

    /// A node whose PROJECT declares it a trigger but whose impl never
    /// wrote `setup_trigger`: the trait default must fail loud.
    struct ForgotSetup;
    test_manifest!(ForgotSetup, "ForgotSetup");
    #[async_trait]
    impl Node for ForgotSetup {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Ok(())
        }
    }

    struct RoutingCatalog {
        src: &'static Src,
        trig: &'static Trig,
        forgot: &'static ForgotSetup,
    }
    impl NodeCatalog for RoutingCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            match node_type {
                "Src" => Some(self.src as &'static dyn Node),
                "Trig" => Some(self.trig as &'static dyn Node),
                "ForgotSetup" => Some(self.forgot as &'static dyn Node),
                _ => None,
            }
        }
        fn all(&self) -> Vec<&'static str> { vec!["Src", "Trig", "ForgotSetup"] }
    }

    /// Project: a lone trigger with no wiring, the plain fire shape.
    fn lone_trigger_project(trigger_type: &str) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "name": "phase-routing-lone-trigger",
            "description": null,
            "nodes": [
                {
                    "id": "trig", "nodeType": trigger_type, "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [],
                    "features": { "isTrigger": true }, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [],
            "groups": []
        }))
        .expect("lone trigger project")
    }

    /// Project: plain `src`.value -> trigger `trig`.value, mirroring a
    /// bridge-output feeding a trigger's setup-time input.
    fn src_feeds_trigger_project(trigger_type: &str) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "name": "phase-routing-test",
            "description": null,
            "nodes": [
                {
                    "id": "src", "nodeType": "Src", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "trig", "nodeType": trigger_type, "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "value", "portType": "String", "required": true }],
                    "outputs": [], "features": { "isTrigger": true }, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e", "source": "src", "target": "trig", "sourceHandle": "value", "targetHandle": "value" }
            ],
            "groups": []
        }))
        .expect("routing project")
    }

    fn clients(journal: Arc<MemJournal>) -> EngineClients {
        EngineClients {
            journal,
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            paid_calls: crate::context::FakePaidCallClient::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        }
    }

    async fn seed(journal: &MemJournal, project: &ProjectDefinition, color: Color, phase: weft_core::context::Phase, kicks: &[&str]) {
        journal
            .record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: project.id.to_string(),
                    entry_node: kicks[0].to_string(),
                    phase,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        for (i, node) in kicks.iter().enumerate() {
            // Mirror the dispatcher's fire kicks: at Fire the FIRST kick
            // is the firing trigger (explicit `firing` flag + payload);
            // every other kick (and every setup-phase kick) is neither.
            let firing = i == 0 && matches!(phase, weft_core::context::Phase::Fire);
            journal
                .record_event(
                    &ExecEvent::NodeKicked {
                        color,
                        node_id: node.to_string(),
                        firing,
                        payload: firing.then(|| json!({"fired": true})),
                        port_snapshot: None,
                        at_unix: 0,
                    },
                    None,
                )
                .await
                .unwrap();
        }
    }

    async fn run_routing_case(
        project: ProjectDefinition,
        phase: weft_core::context::Phase,
        kicks: &[&str],
    ) -> (Arc<StdMutex<Vec<&'static str>>>, ExecutionOutcome, Arc<MemJournal>) {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let catalog: Arc<dyn NodeCatalog> = Arc::new(RoutingCatalog {
            src: Box::leak(Box::new(Src { calls: calls.clone() })),
            trig: Box::leak(Box::new(Trig { calls: calls.clone() })),
            forgot: Box::leak(Box::new(ForgotSetup)),
        });
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, &project, color, phase, kicks).await;
        let outcome = run_one_execution(
            Arc::new(project),
            catalog,
            color,
            clients(journal.clone()),
            "pod-test".into(),
            "tenant-test".into(),
            "ns-test".into(),
            CancellationFlag::new_arc(),
            None,
        )
        .await
        .expect("run_one_execution ok");
        (calls, outcome, journal)
    }

    /// TriggerSetup: the plain upstream runs its normal body (feeding the
    /// trigger's setup-time input), the trigger gets `setup_trigger`, and
    /// `run` is never invoked on it.
    #[tokio::test]
    async fn trigger_setup_routes_upstream_to_run_and_trigger_to_setup() {
        let (calls, outcome, _journal) = run_routing_case(
            src_feeds_trigger_project("Trig"),
            weft_core::context::Phase::TriggerSetup,
            &["src"],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        assert_eq!(*calls.lock().unwrap(), vec!["src:run", "trig:setup_trigger"]);
    }

    /// Fire: the kicked trigger runs its normal body exactly once;
    /// `setup_trigger` is never invoked.
    #[tokio::test]
    async fn fire_routes_the_trigger_to_run() {
        let (calls, outcome, _journal) = run_routing_case(
            lone_trigger_project("Trig"),
            weft_core::context::Phase::Fire,
            &["trig"],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        assert_eq!(*calls.lock().unwrap(), vec!["trig:run"]);
    }

    /// A manifest-declared trigger with no `setup_trigger` impl fails
    /// loud at setup time via the trait default, naming the missing
    /// method.
    #[tokio::test]
    async fn a_trigger_without_setup_trigger_fails_loud() {
        let (_calls, _outcome, journal) = run_routing_case(
            src_feeds_trigger_project("ForgotSetup"),
            weft_core::context::Phase::TriggerSetup,
            &["src"],
        )
        .await;
        let events = format!("{:?}", journal.events.lock().unwrap());
        assert!(
            events.contains("did not implement Node::setup_trigger"),
            "expected the loud trait-default error in the journal; got: {events}"
        );
    }

    /// Trigger that reads its snapshot-seeded port AND a wake field,
    /// recording both, mirroring WhatsAppReceive's media path.
    struct SnapTrig {
        calls: Arc<StdMutex<Vec<String>>>,
    }
    test_manifest!(SnapTrig, "SnapTrig");
    #[async_trait]
    impl Node for SnapTrig {
        async fn setup_trigger(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Ok(())
        }
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let url: String = ctx.inputs.get("endpointUrl")?;
            let msg: String = ctx.wake.get("messageId")?;
            self.calls.lock().unwrap().push(format!("snap:{url}:{msg}"));
            Ok(())
        }
    }

    struct SnapCatalog {
        snap: &'static SnapTrig,
    }
    impl NodeCatalog for SnapCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            (node_type == "SnapTrig").then_some(self.snap as &'static dyn Node)
        }
        fn all(&self) -> Vec<&'static str> { vec!["SnapTrig"] }
    }

    /// A payload-less trigger kick at Fire (another trigger fired) does
    /// NOT run the body: it terminates cleanly with its ports closed, so
    /// the run completes without a "no wake payload" failure.
    #[tokio::test]
    async fn an_idle_trigger_at_fire_closes_instead_of_running() {
        let (calls, outcome, journal) = run_routing_case(
            src_feeds_trigger_project("Trig"),
            weft_core::context::Phase::Fire,
            // The first kick (the firing one) is a plain node here, so the
            // trigger's kick is payload-less: the idle-trigger shape.
            &["src", "trig"],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        assert!(
            !calls.lock().unwrap().contains(&"trig:run"),
            "an idle trigger's body must not run: {:?}",
            calls.lock().unwrap()
        );
        let events = format!("{:?}", journal.events.lock().unwrap());
        assert!(!events.contains("NodeFailed"), "idle trigger must close, not fail: {events}");
    }

    /// At Fire, a wire INTO a trigger is inert: the upstream node runs
    /// (it may feed the output path) but its pulse must not dispatch the
    /// trigger a second time. Exactly one `trig:run`.
    #[tokio::test]
    async fn a_wire_into_a_trigger_is_inert_at_fire() {
        let (calls, outcome, _journal) = run_routing_case(
            src_feeds_trigger_project("Trig"),
            weft_core::context::Phase::Fire,
            // The firing trigger first (carries the payload), plus the
            // upstream node kicked alive for the output path.
            &["trig", "src"],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        let calls = calls.lock().unwrap();
        let trig_runs = calls.iter().filter(|c| **c == "trig:run").count();
        assert_eq!(trig_runs, 1, "the firing trigger runs exactly once: {calls:?}");
        assert!(calls.contains(&"src:run"), "upstream still runs for the output path: {calls:?}");
    }

    /// The firing trigger's ports replay the setup-time snapshot: the
    /// kick's `port_snapshot` lands on `ctx.inputs`, the payload on
    /// `ctx.wake`, in the SAME single dispatch.
    #[tokio::test]
    async fn a_firing_trigger_reads_its_snapshot_and_wake_in_one_dispatch() {
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(SnapCatalog { snap: Box::leak(Box::new(SnapTrig { calls: calls.clone() })) });
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "name": "snapshot-fire",
            "description": null,
            "nodes": [
                {
                    "id": "trig", "nodeType": "SnapTrig", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "endpointUrl", "portType": "String", "required": true }],
                    "outputs": [], "features": { "isTrigger": true }, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [],
            "groups": []
        }))
        .unwrap();
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal
            .record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: project.id.to_string(),
                    entry_node: "trig".into(),
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::NodeKicked {
                    color,
                    node_id: "trig".into(),
                    firing: true,
                    payload: Some(json!({ "messageId": "m-7" })),
                    port_snapshot: Some(json!({ "endpointUrl": "http://bridge" })),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        let outcome = run_one_execution(
            Arc::new(project),
            catalog,
            color,
            clients(journal),
            "pod-test".into(),
            "tenant-test".into(),
            "ns-test".into(),
            CancellationFlag::new_arc(),
            None,
        )
        .await
        .unwrap();
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        assert_eq!(*calls.lock().unwrap(), vec!["snap:http://bridge:m-7".to_string()]);
    }
