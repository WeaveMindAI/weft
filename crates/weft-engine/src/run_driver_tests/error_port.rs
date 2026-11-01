    //! Layer 3: a node's failure is put on its `_error` output by the
    //! runtime when that output is wired and the node has no stream open
    //! (a bus, a generator that yielded); a
    //! bus the node opened ends the way its body returned; and a firing a
    //! dead worker left running is failed, never run again, with the same
    //! catch applying to that failure.

    use super::*;
    use super::engine_test_rig::{catalog, drive, drive_journal, drive_journal_lost, test_manifest};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::{WeftError, WeftResult};
    use weft_core::node::Node;
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::RunEvent;
    use std::sync::Mutex as StdMutex;

    /// Fails as an outcome of the node (a provider refusing, say).
    struct Refused;
    test_manifest!(Refused, "Refused");
    #[async_trait]
    impl Node for Refused {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            Err(WeftError::NodeFailed("the service refused".into()))
        }
    }

    /// Fails on the program's own shape: never caught.
    struct Misconfigured;
    test_manifest!(Misconfigured, "Misconfigured");
    #[async_trait]
    impl Node for Misconfigured {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            Err(WeftError::Config("the setting is wrong".into()))
        }
    }

    /// Panics mid-body: the node failing, caught like any outcome.
    struct Panicking;
    test_manifest!(Panicking, "Panicking");
    #[async_trait]
    impl Node for Panicking {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            panic!("the body fell over")
        }
    }

    /// Must never run: the node a dead worker left running.
    struct NeverAgain;
    test_manifest!(NeverAgain, "NeverAgain");
    #[async_trait]
    impl Node for NeverAgain {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            panic!("a node the worker died in was run again")
        }
    }

    /// Emits `out`: a pure node, run again after a crash.
    struct Echo;
    test_manifest!(Echo, "Echo");
    #[async_trait]
    impl Node for Echo {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(weft_core::node::NodeOutput::new().set("out", "again")).await
        }
    }

    /// Reaches outside (it tags its run) while marked pure.
    struct Tagging;
    test_manifest!(Tagging, "Tagging");
    #[async_trait]
    impl Node for Tagging {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.tag_run(["seen"]).await
        }
    }

    /// Reads its input and emits nothing.
    struct Sink;
    test_manifest!(Sink, "Sink");
    #[async_trait]
    impl Node for Sink {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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

    /// `node` (of `node_type`) feeds `after` on `out`, and, when
    /// `error_wired`, `handler` on `_error`.
    fn project(node_type: &str, error_wired: bool) -> ProjectDefinition {
        let sink = |id: &str, x: f64| json!({
            "id": id, "nodeType": "Sink", "label": null, "config": null, "position": {"x": x, "y": 0.0},
            "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
            "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
        });
        let mut edges = vec![json!({"id": "a", "source": "node", "target": "after", "sourceHandle": "out", "targetHandle": "in"})];
        let mut nodes = vec![
            json!({
                "id": "node", "nodeType": node_type, "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                "inputs": [],
                "outputs": [
                    {"name": "out", "portType": "String", "required": false},
                    {"name": "_error", "portType": "String", "required": false}
                ],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }),
            sink("after", 1.0),
        ];
        if error_wired {
            nodes.push(sink("handler", 2.0));
            edges.push(json!({"id": "e", "source": "node", "target": "handler", "sourceHandle": "_error", "targetHandle": "in"}));
        }
        serde_json::from_value(json!({ "id": uuid::Uuid::new_v4(), "nodes": nodes, "edges": edges, "groups": [] })).unwrap()
    }

    fn error_value(events: &[RunEvent]) -> Option<String> {
        events.iter().find_map(|e| match e {
            RunEvent::PortEmitted { node_id, port, value, .. } if node_id == "node" && port == "_error" => {
                value.as_str().map(str::to_string)
            }
            _ => None,
        })
    }

    fn completed(events: &[RunEvent], node: &str) -> bool {
        events.iter().any(|e| matches!(e, RunEvent::NodeCompleted { node_id, .. } if node_id == node))
    }

    fn failed(events: &[RunEvent], node: &str) -> Option<String> {
        events.iter().find_map(|e| match e {
            RunEvent::NodeFailed { node_id, error, .. } if node_id == node => Some(error.clone()),
            _ => None,
        })
    }

    /// Wired, the failure is a value on `_error`: the node completes,
    /// the branch reading `_error` runs, the one reading `out` is
    /// skipped (the port closed), and the run completes.
    #[tokio::test]
    async fn a_failure_goes_to_a_wired_error_and_the_branch_carries_on() {
        let (outcome, events) = drive(project("Refused", true), nodes(), &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(error_value(&events).as_deref(), Some("the service refused"));
        assert!(completed(&events, "node") && failed(&events, "node").is_none(), "{events:?}");
        assert!(completed(&events, "handler"), "{events:?}");
        assert!(events.iter().any(|e| matches!(e, RunEvent::NodeSkipped { node_id, .. } if node_id == "after")), "{events:?}");
        // The node's log says where the failure went.
        assert!(
            events.iter().any(|e| matches!(e, RunEvent::LogLine { node_id, message, .. }
                if node_id == "node" && message.contains("handed to the '_error' output") && message.contains("the service refused"))),
            "{events:?}"
        );
    }

    /// Unwired, a caught failure nobody reads would be silent: the run
    /// fails instead.
    #[tokio::test]
    async fn an_unwired_error_lets_the_failure_stop_the_run() {
        let (outcome, events) = drive(project("Refused", false), nodes(), &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "node").is_some_and(|e| e.contains("the service refused")), "{events:?}");
        assert!(error_value(&events).is_none());
    }

    /// A mistake in the program is never routed around, wired or not.
    #[tokio::test]
    async fn a_config_error_is_never_caught() {
        let (outcome, events) = drive(project("Misconfigured", true), nodes(), &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "node").is_some_and(|e| e.contains("the setting is wrong")), "{events:?}");
        assert!(error_value(&events).is_none());
    }

    /// A panic is the node failing: caught like any outcome.
    #[tokio::test]
    async fn a_panic_goes_to_a_wired_error() {
        let (outcome, events) = drive(project("Panicking", true), nodes(), &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(error_value(&events).is_some_and(|e| e.contains("panicked")), "{events:?}");
    }

    /// The journal of a run whose worker died while `node` ran: born,
    /// kicked, started, nothing after.
    fn crashed_rows(project: &ProjectDefinition, run_id: RunId) -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "node".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
                cut_from: None, seed: None, instance: None, stand_in: None, fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            RunEvent::NodeKicked {
                run_id, node_id: "node".into(), frames: vec![], firing: false,
                payload: None, port_snapshot: None, at_unix: 0,
            },
            RunEvent::NodeStarted { run_id, node_id: "node".into(), frames: vec![], at_unix: 0 },
        ]
    }

    /// The next worker fails the firing it finds running (it may have
    /// partly happened), never starts it again, and the run stops.
    #[tokio::test]
    async fn a_node_the_worker_died_in_is_failed_never_run_again() {
        let project = project("NeverAgain", false);
        let run_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, run_id);
        let (outcome, events) = drive_journal(project, nodes(), run_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        let error = failed(&events, "node").expect("the node is failed");
        assert!(error.contains("went away while it was running") && error.contains("not run again"), "{error}");
        let starts = events
            .iter()
            .filter(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } | RunEvent::NodeResumed { node_id, .. } if node_id == "node"))
            .count();
        assert_eq!(starts, 1, "only the dead worker's start: {events:?}");
    }

    /// The worker that died in the node went away with its memory nearly
    /// full, and its claim says so: the failure says it ran out of memory
    /// and which lever raises it. A worker that left no note only went
    /// away.
    #[tokio::test]
    async fn a_node_the_worker_ran_out_of_memory_in_says_so() {
        let note = weft_broker_client::protocol::MemoryNote { used_bytes: 966 << 20, limit_bytes: 1 << 30, at_unix: 101 };
        let project = project("NeverAgain", false);
        let run_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, run_id);
        let (outcome, events) = drive_journal_lost(project.clone(), nodes(), run_id, rows.clone(), Some(note)).await;
        assert!(matches!(outcome.expect("the drive runs"), RunOutcome::Failed { .. }));
        let error = failed(&events, "node").expect("the node is failed");
        assert!(
            error.starts_with("the worker running 'node' ran out of memory while it was running (it was at 94% of its 1Gi); raise it with `weft workers set --memory`"),
            "{error}"
        );
        assert!(error.contains("not run again"), "{error}");

        let (_, events) = drive_journal_lost(project, nodes(), run_id, rows, None).await;
        assert!(failed(&events, "node").is_some_and(|e| e.contains("went away while it was running")), "{events:?}");
    }

    /// A run's birth and its kick, with how it is kept: what the next
    /// worker reads when the node's own rows never landed.
    fn kicked_rows(project: &ProjectDefinition, run_id: RunId, settings: weft_core::run_settings::RunSettings) -> Vec<RunEvent> {
        let mut rows = crashed_rows(project, run_id);
        rows.pop();
        if let Some(RunEvent::RunStarted { settings: born, .. }) = rows.first_mut() {
            *born = settings;
        }
        rows
    }

    /// A durable run's record is whole: handed out again, it carries on
    /// from it and runs the node it never started.
    #[tokio::test]
    async fn a_durable_run_handed_out_again_carries_on() {
        let project = project("Refused", false);
        let run_id = uuid::Uuid::new_v4();
        let durable = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Durable, true).unwrap();
        let rows = kicked_rows(&project, run_id, durable);
        let (outcome, events) = drive_journal(project, nodes(), run_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "node").is_some_and(|e| e.contains("the service refused")), "{events:?}");
    }

    /// The crash failure is an outcome of the node too: with `_error`
    /// wired it goes there and the branch carries on.
    #[tokio::test]
    async fn a_node_the_worker_died_in_goes_to_a_wired_error() {
        let project = project("NeverAgain", true);
        let run_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, run_id);
        let (outcome, events) = drive_journal(project, nodes(), run_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(error_value(&events).is_some_and(|e| e.contains("went away while it was running")), "{events:?}");
        assert!(completed(&events, "handler"), "{events:?}");
    }

    /// A worker that died between putting a caught failure on `_error`
    /// and completing the firing: the next one completes it as
    /// caught, with no second value on `_error`, and the branch reading
    /// it carries on.
    #[tokio::test]
    async fn a_catch_the_worker_died_in_completes_as_caught() {
        let project = project("NeverAgain", true);
        let run_id = uuid::Uuid::new_v4();
        let mut rows = crashed_rows(&project, run_id);
        rows.push(RunEvent::PortEmitted {
            run_id, emission_id: uuid::Uuid::new_v4(), node_id: "node".into(), frames: vec![],
            port: "_error".into(), value: Arc::new(json!("the service refused")), at_unix: 0,
        });
        let (outcome, events) = drive_journal(project, nodes(), run_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(completed(&events, "node") && failed(&events, "node").is_none(), "{events:?}");
        let error_rows = events
            .iter()
            .filter(|e| matches!(e, RunEvent::PortEmitted { node_id, port, .. } if node_id == "node" && port == "_error"))
            .count();
        assert_eq!(error_rows, 1, "only the dead worker's emission: {events:?}");
        assert_eq!(error_value(&events).as_deref(), Some("the service refused"));
        assert!(completed(&events, "handler"), "{events:?}");
    }

    /// [`project`] with `node` marked `features.pure`.
    fn pure_project(node_type: &str, error_wired: bool) -> ProjectDefinition {
        let mut marked = project(node_type, error_wired);
        let node = marked.nodes.iter_mut().find(|n| n.id == "node").unwrap();
        node.features.pure = true;
        marked
    }

    fn starts_of_node(events: &[RunEvent]) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } | RunEvent::NodeResumed { node_id, .. } if node_id == "node"))
            .count()
    }

    /// A pure node the worker died in, before it emitted anything, did
    /// nothing anybody can see: the next worker runs it again and the run
    /// goes on.
    #[tokio::test]
    async fn a_pure_node_the_worker_died_in_runs_again() {
        let project = pure_project("Echo", false);
        let run_id = uuid::Uuid::new_v4();
        let rows = crashed_rows(&project, run_id);
        let (outcome, events) = drive_journal(project, nodes(), run_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(completed(&events, "node") && failed(&events, "node").is_none(), "{events:?}");
        assert!(completed(&events, "after"), "{events:?}");
        assert_eq!(starts_of_node(&events), 2, "the dead worker's start, then the new one: {events:?}");
    }

    /// One that emitted already has a value downstream a second run would
    /// emit again: it is failed like any node.
    #[tokio::test]
    async fn a_pure_node_that_emitted_is_failed_not_run_again() {
        let project = pure_project("NeverAgain", false);
        let run_id = uuid::Uuid::new_v4();
        let mut rows = crashed_rows(&project, run_id);
        rows.push(RunEvent::PortEmitted {
            run_id, emission_id: uuid::Uuid::new_v4(), node_id: "node".into(), frames: vec![],
            port: "out".into(), value: Arc::new(json!("once")), at_unix: 0,
        });
        let (outcome, events) = drive_journal(project, nodes(), run_id, rows, CancellationFlag::new_arc()).await;
        let outcome = outcome.expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "node").is_some_and(|e| e.contains("not run again")), "{events:?}");
        assert_eq!(starts_of_node(&events), 1, "{events:?}");
    }

    /// A node marked pure that reaches outside fails at that call, naming
    /// the flag, and the failure is the node's declaration, never caught.
    #[tokio::test]
    async fn a_pure_node_reaching_outside_fails_naming_the_flag() {
        let (outcome, events) = drive(pure_project("Tagging", true), nodes(), &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        let error = failed(&events, "node").expect("the node fails");
        assert!(error.contains("features.pure") && error.contains("tag its run"), "{error}");
        assert!(error_value(&events).is_none(), "a wrong declaration is never caught");
    }

    /// Streams on `stream`: fails at `fail_at` pieces, before it opened
    /// its bus when that is 0 (a provider refusing the call), halfway
    /// otherwise, or finishes after `pieces` when it never fails.
    struct Streamer {
        pieces: usize,
        fail_at: Option<usize>,
    }
    test_manifest!(Streamer, "Streamer");
    #[async_trait]
    impl Node for Streamer {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            if self.fail_at == Some(0) {
                return Err(WeftError::NodeFailed("the model refused the call".into()));
            }
            let bus = ctx.open_bus("stream", Default::default(), "streamer").await?;
            for piece in 0..self.pieces {
                if self.fail_at == Some(piece) {
                    return Err(WeftError::NodeFailed("the model went away mid-answer".into()));
                }
                bus.send("delta", json!(piece)).map_err(|e| WeftError::NodeFailed(e.to_string()))?;
            }
            Ok(())
        }
    }

    /// What a [`Reader`] read off its bus, and how the bus ended.
    type ReadBack = Arc<StdMutex<Option<(Vec<serde_json::Value>, Option<weft_core::bus::StreamEnd>)>>>;

    /// Reads the bus on `bus` from the start, as a stream to a caller
    /// does, and keeps what it read and how the bus ended.
    struct Reader {
        read: ReadBack,
    }
    test_manifest!(Reader, "Reader");
    #[async_trait]
    impl Node for Reader {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let bus = ctx.bus_from_input("bus")?;
            let mut cursor = bus.cursor_from_start();
            let mut pieces = Vec::new();
            while let Some(entry) = cursor.next().await {
                if let weft_core::bus::BusEntryKind::Message { payload: Some(payload), .. } = entry.kind {
                    pieces.push(payload.into_json().expect("a json bus"));
                }
            }
            *self.read.lock().unwrap() = Some((pieces, bus.end()));
            Ok(())
        }
    }

    /// `node` (a [`Streamer`]) feeds `reader` on its bus and `handler` on
    /// `_error`.
    fn streaming(streamer: Streamer) -> (ProjectDefinition, Arc<dyn weft_core::NodeCatalog>, ReadBack) {
        let read: ReadBack = Default::default();
        let nodes = catalog(vec![
            ("Streamer", Box::new(streamer)),
            ("Reader", Box::new(Reader { read: read.clone() })),
            ("Sink", Box::new(Sink)),
        ]);
        let project = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "node", "nodeType": "Streamer", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                    "inputs": [],
                    "outputs": [
                        {"name": "stream", "portType": "Bus", "required": false},
                        {"name": "_error", "portType": "String", "required": false}
                    ],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "reader", "nodeType": "Reader", "label": null, "config": null, "position": {"x": 1.0, "y": 0.0},
                    "inputs": [{"name": "bus", "portType": "Bus", "required": true}], "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "handler", "nodeType": "Sink", "label": null, "config": null, "position": {"x": 2.0, "y": 0.0},
                    "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                {"id": "s", "source": "node", "target": "reader", "sourceHandle": "stream", "targetHandle": "bus"},
                {"id": "e", "source": "node", "target": "handler", "sourceHandle": "_error", "targetHandle": "in"}
            ],
            "groups": []
        }))
        .unwrap();
        (project, nodes, read)
    }

    fn skipped(events: &[RunEvent], node: &str) -> bool {
        events.iter().any(|e| matches!(e, RunEvent::NodeSkipped { node_id, .. } if node_id == node))
    }

    /// A failure before the bus opened is caught on the wired `_error`:
    /// the bus output closes as absence, so its reader never starts, and
    /// the branch gated on the failure answers instead.
    #[tokio::test]
    async fn a_failure_before_the_bus_opens_is_caught_and_its_readers_never_start() {
        let (project, nodes, read) = streaming(Streamer { pieces: 2, fail_at: Some(0) });
        let (outcome, events) = drive(project, nodes, &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(error_value(&events).as_deref(), Some("the model refused the call"));
        assert!(completed(&events, "handler"), "{events:?}");
        assert!(skipped(&events, "reader"), "{events:?}");
        assert!(read.lock().unwrap().is_none(), "the reader never ran");
    }

    /// Once the bus is open, a failure is not caught, even with `_error`
    /// wired: the node fails, the bus ends failed with its error, and the
    /// reader reads what was sent, then that end.
    #[tokio::test]
    async fn a_failure_after_the_first_piece_fails_the_node_and_ends_the_bus_failed() {
        let (project, nodes, read) = streaming(Streamer { pieces: 3, fail_at: Some(1) });
        let (outcome, events) = drive(project, nodes, &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "node").is_some_and(|e| e.contains("mid-answer")), "{events:?}");
        assert!(error_value(&events).is_none(), "nothing caught once the bus opened");
        assert!(skipped(&events, "handler"), "{events:?}");
        let (pieces, end) = read.lock().unwrap().clone().expect("the reader ran");
        assert_eq!(pieces, vec![json!(0)]);
        assert!(
            matches!(&end, Some(weft_core::bus::StreamEnd::Failed { error }) if error.contains("mid-answer")),
            "{end:?}"
        );
        assert!(
            events.iter().any(|e| matches!(e, RunEvent::BusClosed { end: weft_core::bus::StreamEnd::Failed { .. }, .. })),
            "the record says how the bus ended: {events:?}"
        );
    }

    /// A body that returns normally ends its bus finished.
    #[tokio::test]
    async fn a_body_that_returns_ends_its_bus_finished() {
        let (project, nodes, read) = streaming(Streamer { pieces: 2, fail_at: None });
        let (outcome, _events) = drive(project, nodes, &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let (pieces, end) = read.lock().unwrap().clone().expect("the reader ran");
        assert_eq!(pieces, vec![json!(0), json!(1)]);
        assert_eq!(end, Some(weft_core::bus::StreamEnd::Finished));
    }

    /// Yields one item on its `Generator` output `rows`, then fails.
    struct YieldThenFail;
    test_manifest!(YieldThenFail, "YieldThenFail");
    #[async_trait]
    impl Node for YieldThenFail {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(weft_core::node::NodeOutput::new().set("rows", 1)).await?;
            Err(WeftError::NodeFailed("the source went away mid-stream".into()))
        }
    }

    /// Pulls every item of `rows`; what it saw ends up in its own
    /// failure or completion.
    struct Puller;
    test_manifest!(Puller, "Puller");
    #[async_trait]
    impl Node for Puller {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let rows = ctx.inputs.get::<weft_core::Generator<i64>>("rows")?;
            while rows.next().await?.is_some() {}
            Ok(())
        }
    }

    /// A generator that already yielded is a stream its reader holds part
    /// of: a failure after it is not caught, even with `_error` wired, and
    /// the reader's pull sees the failure instead of a clean end.
    #[tokio::test]
    async fn a_failure_after_a_generator_yielded_is_not_caught() {
        let nodes = catalog(vec![
            ("YieldThenFail", Box::new(YieldThenFail)),
            ("Puller", Box::new(Puller)),
            ("Sink", Box::new(Sink)),
        ]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "node", "nodeType": "YieldThenFail", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                    "inputs": [],
                    "outputs": [
                        {"name": "rows", "portType": "Generator[Number]", "required": false},
                        {"name": "_error", "portType": "String", "required": false}
                    ],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "puller", "nodeType": "Puller", "label": null, "config": null, "position": {"x": 1.0, "y": 0.0},
                    "inputs": [{"name": "rows", "portType": "Generator[Number]", "required": true}], "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "handler", "nodeType": "Sink", "label": null, "config": null, "position": {"x": 2.0, "y": 0.0},
                    "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                {"id": "s", "source": "node", "target": "puller", "sourceHandle": "rows", "targetHandle": "rows"},
                {"id": "e", "source": "node", "target": "handler", "sourceHandle": "_error", "targetHandle": "in"}
            ],
            "groups": []
        }))
        .unwrap();
        let (outcome, events) = drive(project, nodes, &["node"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(failed(&events, "node").is_some_and(|e| e.contains("mid-stream")), "{events:?}");
        assert!(error_value(&events).is_none(), "nothing caught once the stream opened");
        assert!(failed(&events, "puller").is_some(), "the reader's pull saw the failure: {events:?}");
        assert!(skipped(&events, "handler"), "{events:?}");
    }
