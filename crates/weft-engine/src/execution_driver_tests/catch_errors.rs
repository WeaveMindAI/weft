    //! Layer 3: a node with `features.catchErrors` has its failure put on
    //! its `error` output by the runtime when that output is wired, and a
    //! step a dead worker left running is failed, never run again, with
    //! the same catch applying to that failure.

    use super::*;
    use super::engine_test_rig::{catalog, drive, drive_journal, test_manifest};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::{WeftError, WeftResult};
    use weft_core::node::Node;
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::ExecEvent;

    /// Fails as an outcome of the step (a provider refusing, say).
    struct Refused;
    test_manifest!(Refused, "Refused");
    #[async_trait]
    impl Node for Refused {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Err(WeftError::NodeExecution("the service refused".into()))
        }
    }

    /// Fails on the program's own shape: never caught.
    struct Misconfigured;
    test_manifest!(Misconfigured, "Misconfigured");
    #[async_trait]
    impl Node for Misconfigured {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Err(WeftError::Config("the setting is wrong".into()))
        }
    }

    /// Panics mid-body: the node failing, caught like any outcome.
    struct Panicking;
    test_manifest!(Panicking, "Panicking");
    #[async_trait]
    impl Node for Panicking {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            panic!("the body fell over")
        }
    }

    /// Must never run: the step a dead worker left running.
    struct NeverAgain;
    test_manifest!(NeverAgain, "NeverAgain");
    #[async_trait]
    impl Node for NeverAgain {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            panic!("a step the worker died in was run again")
        }
    }

    /// Emits `out`: a pure step, run again after a crash.
    struct Echo;
    test_manifest!(Echo, "Echo");
    #[async_trait]
    impl Node for Echo {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(weft_core::node::NodeOutput::new().set("out", "again")).await
        }
    }

    /// Reaches outside (it tags its run) while marked pure.
    struct Tagging;
    test_manifest!(Tagging, "Tagging");
    #[async_trait]
    impl Node for Tagging {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.tag_execution(["seen"]).await
        }
    }

    /// Reads its input and emits nothing.
    struct Sink;
    test_manifest!(Sink, "Sink");
    #[async_trait]
    impl Node for Sink {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let _: String = ctx.inputs.get("in")?;
            Ok(())
        }
    }

    fn nodes() -> Arc<dyn weft_core::NodeCatalog> {
        catalog(vec![
            ("Refused", Box::new(Refused)),
            ("Misconfigured", Box::new(Misconfigured)),
            ("Panicking", Box::new(Panicking)),
            ("NeverAgain", Box::new(NeverAgain)),
            ("Echo", Box::new(Echo)),
            ("Tagging", Box::new(Tagging)),
            ("Sink", Box::new(Sink)),
        ])
    }

    /// `step` (of `step_type`, catching its failures) feeds `after` on
    /// `out`, and, when `error_wired`, `handler` on `error`.
    fn project(step_type: &str, error_wired: bool) -> ProjectDefinition {
        let sink = |id: &str, x: f64| json!({
            "id": id, "nodeType": "Sink", "label": null, "config": null, "position": {"x": x, "y": 0.0},
            "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
            "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
        });
        let mut edges = vec![json!({"id": "a", "source": "step", "target": "after", "sourceHandle": "out", "targetHandle": "in"})];
        let mut nodes = vec![
            json!({
                "id": "step", "nodeType": step_type, "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                "inputs": [],
                "outputs": [
                    {"name": "out", "portType": "String", "required": false},
                    {"name": "error", "portType": "String", "required": false}
                ],
                "features": {"catchErrors": true}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }),
            sink("after", 1.0),
        ];
        if error_wired {
            nodes.push(sink("handler", 2.0));
            edges.push(json!({"id": "e", "source": "step", "target": "handler", "sourceHandle": "error", "targetHandle": "in"}));
        }
        serde_json::from_value(json!({ "id": uuid::Uuid::new_v4(), "nodes": nodes, "edges": edges, "groups": [] })).unwrap()
    }

    fn error_value(events: &[ExecEvent]) -> Option<String> {
        events.iter().find_map(|e| match e {
            ExecEvent::PortEmitted { node_id, port, value, .. } if node_id == "step" && port == "error" => {
                value.as_str().map(str::to_string)
            }
            _ => None,
        })
    }

    fn completed(events: &[ExecEvent], node: &str) -> bool {
        events.iter().any(|e| matches!(e, ExecEvent::NodeCompleted { node_id, .. } if node_id == node))
    }

    fn failed(events: &[ExecEvent], node: &str) -> Option<String> {
        events.iter().find_map(|e| match e {
            ExecEvent::NodeFailed { node_id, error, .. } if node_id == node => Some(error.clone()),
            _ => None,
        })
    }

    /// Wired, the failure is a value on `error`: the step completes,
    /// the branch reading `error` runs, the one reading `out` is
    /// skipped (the port closed), and the run completes.
    #[tokio::test]
    async fn a_failure_goes_to_a_wired_error_and_the_branch_carries_on() {
        let (outcome, events) = drive(project("Refused", true), nodes(), &["step"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(error_value(&events).as_deref(), Some("the service refused"));
        assert!(completed(&events, "step") && failed(&events, "step").is_none(), "{events:?}");
        assert!(completed(&events, "handler"), "{events:?}");
        assert!(events.iter().any(|e| matches!(e, ExecEvent::NodeSkipped { node_id, .. } if node_id == "after")), "{events:?}");
        // The node's log says where the failure went.
        assert!(
            events.iter().any(|e| matches!(e, ExecEvent::LogLine { node_id, message, .. }
                if node_id == "step" && message.contains("handed to the 'error' output") && message.contains("the service refused"))),
            "{events:?}"
        );
    }

    /// Unwired, a caught failure nobody reads would be silent: the run
    /// fails instead.
    #[tokio::test]
    async fn an_unwired_error_lets_the_failure_stop_the_run() {
        let (outcome, events) = drive(project("Refused", false), nodes(), &["step"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "step").is_some_and(|e| e.contains("the service refused")), "{events:?}");
        assert!(error_value(&events).is_none());
    }

    /// A mistake in the program is never routed around, wired or not.
    #[tokio::test]
    async fn a_config_error_is_never_caught() {
        let (outcome, events) = drive(project("Misconfigured", true), nodes(), &["step"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "step").is_some_and(|e| e.contains("the setting is wrong")), "{events:?}");
        assert!(error_value(&events).is_none());
    }

    /// A panic is the node failing: caught like any outcome.
    #[tokio::test]
    async fn a_panic_goes_to_a_wired_error() {
        let (outcome, events) = drive(project("Panicking", true), nodes(), &["step"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(error_value(&events).is_some_and(|e| e.contains("panicked")), "{events:?}");
    }

    /// The journal of a run whose worker died while `step` ran: born,
    /// kicked, started, nothing after.
    fn crashed_rows(project: &ProjectDefinition, execution_id: ExecutionId) -> Vec<ExecEvent> {
        vec![
            ExecEvent::ExecutionStarted {
                execution_id,
                project_id: project.id,
                entry_node: "step".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
                selection: None, seed: None, instance: None, stand_in: None, fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            ExecEvent::NodeKicked {
                execution_id, node_id: "step".into(), frames: vec![], firing: false,
                payload: None, port_snapshot: None, at_unix: 0,
            },
            ExecEvent::NodeStarted { execution_id, node_id: "step".into(), frames: vec![], at_unix: 0 },
        ]
    }

    /// The next worker fails the step it finds running (it may have
    /// partly happened), never starts it again, and the run stops.
    #[tokio::test]
    async fn a_step_the_worker_died_in_is_failed_never_run_again() {
        let project = project("NeverAgain", false);
        let execution_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, execution_id);
        let (outcome, events) = drive_journal(project, nodes(), execution_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let error = failed(&events, "step").expect("the step is failed");
        assert!(error.contains("went away while it was running") && error.contains("not run again"), "{error}");
        let starts = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeStarted { node_id, .. } | ExecEvent::NodeResumed { node_id, .. } if node_id == "step"))
            .count();
        assert_eq!(starts, 1, "only the dead worker's start: {events:?}");
    }

    /// A run's birth and its kick, with how it is kept: what the next
    /// worker reads when the step's own rows never landed.
    fn kicked_rows(project: &ProjectDefinition, execution_id: ExecutionId, settings: weft_core::run_settings::RunSettings) -> Vec<ExecEvent> {
        let mut rows = crashed_rows(project, execution_id);
        rows.pop();
        if let Some(ExecEvent::ExecutionStarted { settings: born, .. }) = rows.first_mut() {
            *born = settings;
        }
        rows
    }

    /// A durable run's record is whole: handed out again, it carries on
    /// from it and runs the step it never started.
    #[tokio::test]
    async fn a_durable_run_handed_out_again_carries_on() {
        let project = project("Refused", false);
        let execution_id = uuid::Uuid::new_v4();
        let durable = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Durable, true).unwrap();
        let rows = kicked_rows(&project, execution_id, durable);
        let (outcome, events) = drive_journal(project, nodes(), execution_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "step").is_some_and(|e| e.contains("the service refused")), "{events:?}");
    }

    /// The crash failure is an outcome of the step too: with `error`
    /// wired it goes there and the branch carries on.
    #[tokio::test]
    async fn a_step_the_worker_died_in_goes_to_a_wired_error() {
        let project = project("NeverAgain", true);
        let execution_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, execution_id);
        let (outcome, events) = drive_journal(project, nodes(), execution_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(error_value(&events).is_some_and(|e| e.contains("went away while it was running")), "{events:?}");
        assert!(completed(&events, "handler"), "{events:?}");
    }

    /// A worker that died between putting a caught failure on `error`
    /// and completing the step: the next one completes the step as
    /// caught, with no second value on `error`, and the branch reading
    /// it carries on.
    #[tokio::test]
    async fn a_catch_the_worker_died_in_completes_as_caught() {
        let project = project("NeverAgain", true);
        let execution_id = uuid::Uuid::new_v4();
        let mut rows = crashed_rows(&project, execution_id);
        rows.push(ExecEvent::PortEmitted {
            execution_id, emission_id: uuid::Uuid::new_v4(), node_id: "step".into(), frames: vec![],
            port: "error".into(), value: Arc::new(json!("the service refused")), provided: false, at_unix: 0,
        });
        let (outcome, events) = drive_journal(project, nodes(), execution_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(completed(&events, "step") && failed(&events, "step").is_none(), "{events:?}");
        let error_rows = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::PortEmitted { node_id, port, .. } if node_id == "step" && port == "error"))
            .count();
        assert_eq!(error_rows, 1, "only the dead worker's emission: {events:?}");
        assert_eq!(error_value(&events).as_deref(), Some("the service refused"));
        assert!(completed(&events, "handler"), "{events:?}");
    }

    /// [`project`] with `step` marked `features.pure`.
    fn pure_project(step_type: &str, error_wired: bool) -> ProjectDefinition {
        let mut marked = project(step_type, error_wired);
        let step = marked.nodes.iter_mut().find(|n| n.id == "step").unwrap();
        step.features.pure = true;
        marked
    }

    fn starts_of_step(events: &[ExecEvent]) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeStarted { node_id, .. } | ExecEvent::NodeResumed { node_id, .. } if node_id == "step"))
            .count()
    }

    /// A pure step the worker died in, before it emitted anything, did
    /// nothing anybody can see: the next worker runs it again and the run
    /// goes on.
    #[tokio::test]
    async fn a_pure_step_the_worker_died_in_runs_again() {
        let project = pure_project("Echo", false);
        let execution_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, execution_id);
        let (outcome, events) = drive_journal(project, nodes(), execution_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(completed(&events, "step") && failed(&events, "step").is_none(), "{events:?}");
        assert!(completed(&events, "after"), "{events:?}");
        assert_eq!(starts_of_step(&events), 2, "the dead worker's start, then the new one: {events:?}");
    }

    /// One that emitted already has a value downstream a second run would
    /// emit again: it is failed like any step.
    #[tokio::test]
    async fn a_pure_step_that_emitted_is_failed_not_run_again() {
        let project = pure_project("NeverAgain", false);
        let execution_id = uuid::Uuid::new_v4();
        let mut rows = crashed_rows(&project, execution_id);
        rows.push(ExecEvent::PortEmitted {
            execution_id, emission_id: uuid::Uuid::new_v4(), node_id: "step".into(), frames: vec![],
            port: "out".into(), value: Arc::new(json!("once")), provided: false, at_unix: 0,
        });
        let (outcome, events) = drive_journal(project, nodes(), execution_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "step").is_some_and(|e| e.contains("not run again")), "{events:?}");
        assert_eq!(starts_of_step(&events), 1, "{events:?}");
    }

    /// A node marked pure that reaches outside fails at that call, naming
    /// the flag, and the failure is the node's declaration, never caught.
    #[tokio::test]
    async fn a_pure_node_reaching_outside_fails_naming_the_flag() {
        let (outcome, events) = drive(pure_project("Tagging", true), nodes(), &["step"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let error = failed(&events, "step").expect("the step fails");
        assert!(error.contains("features.pure") && error.contains("tag its run"), "{error}");
        assert!(error_value(&events).is_none(), "a wrong declaration is never caught");
    }
