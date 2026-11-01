    //! Layer 3: branching through the real run loop.
    //!
    //! `_should_flow` is the port that decides whether a node runs at all,
    //! and the rules that matter only exist once a whole run runs:
    //! the guarded node is skipped rather than failed, the skip carries the
    //! REASON that separates a decision from a consequence, and the cascade
    //! reaches everything behind it. Those are three facts about the driver,
    //! not about any node body, so they are pinned here.

    use super::*;
    use super::engine_test_rig::{drive, test_manifest};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::run::scope::ScopeState;
    use weft_core::run::skip::{SkipReason, SHOULD_FLOW_PORT, SHOULD_NOT_FLOW_PORT};
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, NodeCatalog, ProjectDefinition};
    use weft_journal::RunEvent;

    /// Which node bodies actually RAN. The journal records a
    /// `NodeStarted` for a skipped firing too (that row is what carries
    /// the firing's inputs to the inspector), so "did the body run" is a
    /// question only the body can answer.
    type Ran = Arc<StdMutex<Vec<&'static str>>>;

    /// A file input whose file is gone (expired, deleted) fails the firing
    /// that reads it, loudly and naming the file, never a silent skip: the
    /// failure is the one visible event, at the read.
    #[tokio::test]
    async fn a_missing_file_input_records_a_named_failure_instead_of_silently_skipping() {
        let file = weft_core::storage::StoredFile {
            key: format!("t1/asset/{}", "a".repeat(64)),
            mime_type: "image/png".into(),
            filename: "cat.png".into(),
            size_bytes: 3,
            version: weft_core::storage::FIRST_FILE_VERSION,
            link_secs: None,
        };
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{
                "id": "send_photo", "nodeType": "ReadsFile", "position": {"x": 0, "y": 0},
                "inputs": [{"name": "value", "portType": "Image", "required": true}],
                "portLiterals": {"value": file.to_value()}
            }]
        })).unwrap();
        let ran = Arc::new(StdMutex::new(Vec::new()));
        let nodes = catalog(vec![("ReadsFile", Box::new(ReadsFile { ran: ran.clone() }))]);
        // The rig's storage is empty, just as it is after the file expires.
        let (outcome, events) = drive(project, nodes, &["send_photo"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert_eq!(*ran.lock().unwrap(), vec!["send_photo"], "the body ran and met the missing file");
        let errors: Vec<_> = events.iter().filter_map(|event| match event {
            RunEvent::NodeFailed { node_id, error, .. } if node_id == "send_photo" => Some(error),
            _ => None,
        }).collect();
        assert_eq!(errors.len(), 1, "one visible failure: {events:?}");
        assert!(errors[0].contains(&file.key) && errors[0].contains("not found"), "{}", errors[0]);
        assert!(!events.iter().any(|event| matches!(event,
            RunEvent::NodeSkipped { node_id, .. } if node_id == "send_photo"
        )));
    }

    /// Reads the bytes of the file on `value`.
    struct ReadsFile {
        ran: Ran,
    }
    test_manifest!(ReadsFile, "ReadsFile");
    #[async_trait]
    impl Node for ReadsFile {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("send_photo");
            let file: weft_core::storage::FileHandle = ctx.inputs.get("value")?;
            ctx.storage(weft_core::storage::StorageScope::Run).get_bytes(&file).await?;
            ctx.pulse_downstream(NodeOutput::new()).await
        }
    }

    /// Emits a value and a permission, both fixed by the test.
    struct Source {
        /// `None` never mentions `allowed` at all, which is how a port
        /// genuinely CLOSES: the firing ends without it, so the engine
        /// tells everything downstream nothing is coming. Emitting a
        /// null on it would not close it, it would fail the node, since
        /// the port is declared `Boolean` and the declared type judges
        /// every value.
        allowed: Option<serde_json::Value>,
    }
    test_manifest!(Source, "Source");
    #[async_trait]
    impl Node for Source {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut out = NodeOutput::new().set("value", json!("payload"));
            if let Some(allowed) = &self.allowed {
                out = out.set("allowed", allowed.clone());
            }
            ctx.pulse_downstream(out).await
        }
    }

    /// The same source, broken: it never gets as far as emitting.
    struct FailingSource;
    test_manifest!(FailingSource, "Source");
    #[async_trait]
    impl Node for FailingSource {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            Err(weft_core::error::node_error("the database is down"))
        }
    }

    /// Passes its input straight through, and writes down that it ran.
    struct Echo {
        id: &'static str,
        ran: Ran,
    }
    test_manifest!(Echo, "Echo");
    #[async_trait]
    impl Node for Echo {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push(self.id);
            let value: serde_json::Value = ctx.inputs.get("value")?;
            ctx.pulse_downstream(NodeOutput::new().set("value", value)).await
        }
    }

    /// Writes down the value it received, so a test can assert WHICH
    /// value reached the node.
    struct Recorder {
        seen: Arc<StdMutex<Vec<String>>>,
        ran: Ran,
    }
    test_manifest!(Recorder, "Recorder");
    #[async_trait]
    impl Node for Recorder {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("recorder");
            let value: String = ctx.inputs.get("value")?;
            self.seen.lock().unwrap().push(value.clone());
            ctx.pulse_downstream(NodeOutput::new().set("value", json!(value))).await
        }
    }

    use super::engine_test_rig::catalog;

    /// A scope never journals a row: how each copy was decided is what
    /// the fold derives, so a test reads it off the snapshot.
    fn fold(project: ProjectDefinition, events: &[RunEvent]) -> weft_core::primitive::RunSnapshot {
        fold_with_scopes(project, events).0
    }

    /// The fold plus every scope copy it decided, as `(scope, state)` in
    /// the order it decided them: what the live bridge paints the group
    /// from.
    fn fold_with_scopes(
        project: ProjectDefinition,
        events: &[RunEvent],
    ) -> (weft_core::primitive::RunSnapshot, Vec<(String, ScopeState)>) {
        let run_id = events.first().expect("journal has rows").run_id();
        let mut fold = weft_journal::Fold::new(run_id, Arc::new(project));
        let mut decided = Vec::new();
        for e in events {
            decided.extend(fold.apply(e).scopes.into_iter().map(|(copy, state)| (copy.scope, state)));
        }
        let snap = fold.into_snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        (snap, decided)
    }

    /// A run that reached its end folds to the same end: every record
    /// terminal, nothing left in flight, and the fold applied every
    /// row. THE check that the journal carries enough to rebuild what
    /// the live engine held, now that the rows carry no derived state.
    fn assert_fold_settles(project: ProjectDefinition, events: &[RunEvent]) {
        let snap = fold(project, events);
        let pending: Vec<String> = snap
            .pulses
            .iter()
            .flat_map(|(n, b)| b.iter().map(move |p| format!("{n}.{}@{:?}", p.target_port, p.frames)))
            .collect();
        assert!(pending.is_empty(), "a completed run folds with pulses in flight: {pending:?}");
        let open: Vec<String> = snap
            .firings
            .values()
            .flat_map(|v| v.values())
            .filter(|e| !e.status().is_terminal())
            .map(|e| format!("{}@{:?}", e.node_id, e.frames))
            .collect();
        assert!(open.is_empty(), "a completed run folds with open records: {open:?}");
        assert_eq!(
            weft_core::run::check_completion(&snap.pulses, &snap.firings),
            Some(false),
            "the fold reaches the live engine's conclusion"
        );
    }

    /// How the one copy of `scope` was decided, off the fold. A scope
    /// decided twice, or never, is a failed assertion here.
    fn scope_state(decided: &[(String, ScopeState)], scope: &str) -> ScopeState {
        let mut states: Vec<ScopeState> = decided.iter().filter(|(s, _)| s == scope).map(|(_, state)| state.clone()).collect();
        assert_eq!(states.len(), 1, "one decision per copy, '{scope}' has {}", states.len());
        states.pop().expect("one decision")
    }

    /// How the top-level copy of `scope` stands in `snap`.
    fn snapshot_scope(snap: &weft_core::primitive::RunSnapshot, scope: &str) -> Option<ScopeState> {
        snap.scopes.get(&weft_core::project::scopes::ScopeCopy::new(scope, vec![])).map(|record| record.state.clone())
    }

    /// source.value -> guarded.value, source.allowed -> guarded._should_flow,
    /// guarded.value -> behind.value. The shape every test here drives.
    fn guarded_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "source", "nodeType": "Source", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [
                        { "name": "value", "portType": "String", "required": false },
                        { "name": "allowed", "portType": "Boolean", "required": false }
                    ],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "guarded", "nodeType": "Echo", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "behind", "nodeType": "Behind", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "source", "target": "guarded",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e2", "source": "source", "target": "guarded",
                  "sourceHandle": "allowed", "targetHandle": SHOULD_FLOW_PORT },
                { "id": "e3", "source": "guarded", "target": "behind",
                  "sourceHandle": "value", "targetHandle": "value" }
            ],
            "groups": []
        }))
        .expect("branch project")
    }

    fn skip_reason(events: &[RunEvent], node: &str) -> Option<SkipReason> {
        let mut reasons: Vec<SkipReason> = events
            .iter()
            .filter_map(|e| match e {
                RunEvent::NodeSkipped { node_id, reason, .. } if node_id == node => {
                    Some(reason.clone())
                }
                _ => None,
            })
            .collect();
        // Counting, not finding: a duplicated lifecycle pair for one
        // firing location is a bug this helper must surface, never
        // read past.
        assert!(
            reasons.len() <= 1,
            "one NodeSkipped per firing location, '{node}' has {}",
            reasons.len()
        );
        reasons.pop()
    }

    /// The catalog these tests drive: a source with a fixed permission,
    /// and two pass-through nodes that record whether they ran.
    fn branch_catalog(allowed: Option<serde_json::Value>, ran: &Ran) -> Arc<dyn NodeCatalog> {
        catalog(vec![
            ("Source", Box::new(Source { allowed })),
            ("Echo", Box::new(Echo { id: "guarded", ran: ran.clone() })),
            ("Behind", Box::new(Echo { id: "behind", ran: ran.clone() })),
        ])
    }

    /// The same graph with the gate wired the OTHER way round: the
    /// guarded node's permission is `_should_not_flow`, so every answer
    /// flips. Built by renaming the gate on the node and on the edge,
    /// which is exactly what the editor's right-click toggle does.
    fn inverted_gate_project() -> ProjectDefinition {
        let mut project = guarded_project();
        for node in &mut project.nodes {
            for port in &mut node.inputs {
                if port.name == SHOULD_FLOW_PORT {
                    port.name = SHOULD_NOT_FLOW_PORT.to_string();
                    port.port.name = SHOULD_NOT_FLOW_PORT.to_string();
                }
            }
        }
        for edge in &mut project.edges {
            if edge.target_handle.as_deref() == Some(SHOULD_FLOW_PORT) {
                edge.target_handle = Some(SHOULD_NOT_FLOW_PORT.to_string());
            }
        }
        project
    }

    /// A CLOSURE on the inverted gate is the yes. This is the whole
    /// point of the port: the thing it watches never happened, and that
    /// is what the node was written to act on. Nothing else in the
    /// language starts a node on a closure.
    #[tokio::test]
    async fn a_closed_inverted_gate_is_what_runs_the_node() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        // A null emission on `allowed` closes that port.
        let (outcome, events) =
            drive(inverted_gate_project(), branch_catalog(None, &ran), &["source"]).await;

        assert!(
            skip_reason(&events, "guarded").is_none(),
            "a closure is the yes here, got {:?}",
            skip_reason(&events, "guarded")
        );
        assert_eq!(*ran.lock().unwrap(), vec!["guarded", "behind"]);
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
    }

    /// The watched node FAILING is not the absence the inverted gate
    /// waits for. Its ports close either way, but a failure's closure
    /// carries the error, and the gate reads it: the "nothing there"
    /// branch stays off (a query that errored must not render as an
    /// empty result) and the run reports the failure instead.
    #[tokio::test]
    async fn a_failed_watched_node_keeps_the_inverted_gate_off() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let nodes = catalog(vec![
            ("Source", Box::new(FailingSource)),
            ("Echo", Box::new(Echo { id: "guarded", ran: ran.clone() })),
            ("Behind", Box::new(Echo { id: "behind", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(inverted_gate_project(), nodes, &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "a failure is not an absence, so the branch stays off: {:?}",
            ran.lock().unwrap()
        );
        match skip_reason(&events, "guarded") {
            Some(SkipReason::WatchedNodeFailed { failure }) => {
                assert!(failure.error.contains("the database is down"), "the reason names the failure: {failure}");
            }
            other => panic!("expected the watched-node-failed reason, got {other:?}"),
        }
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
    }

    /// And a value arriving is the no, with its own reason so the
    /// inspector can say which of the two gates decided.
    #[tokio::test]
    async fn a_value_on_the_inverted_gate_keeps_the_node_off() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) =
            drive(inverted_gate_project(), branch_catalog(Some(json!(true)), &ran), &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "the thing it watches DID happen, so it stays off: {:?}",
            ran.lock().unwrap()
        );
        assert_eq!(skip_reason(&events, "guarded"), Some(SkipReason::DidFlow));
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
    }

    /// `false` is the one value that means the same on both gates: the
    /// plain one reads it as no, and the inverted one reads it as yes,
    /// which is the same sentence read from either end.
    #[tokio::test]
    async fn false_on_the_inverted_gate_runs_the_node() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (_, events) =
            drive(inverted_gate_project(), branch_catalog(Some(json!(false)), &ran), &["source"]).await;

        assert_eq!(*ran.lock().unwrap(), vec!["guarded", "behind"]);
        assert!(skip_reason(&events, "guarded").is_none());
    }

    /// A guard that says no skips the node it guards, and the node
    /// behind it goes with it. The run still COMPLETES: a branch
    /// not taken is not a failure.
    #[tokio::test]
    async fn a_false_guard_skips_the_node_and_everything_behind_it() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) =
            drive(guarded_project(), branch_catalog(Some(json!(false)), &ran), &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "neither the guarded node nor the one behind it may run: {:?}",
            ran.lock().unwrap()
        );
        assert_eq!(skip_reason(&events, "guarded"), Some(SkipReason::DidNotFlow));
        assert_eq!(
            skip_reason(&events, "behind"),
            Some(SkipReason::RequiredInputClosed { port: "value".into(), failure: None }),
            "the node behind is a CONSEQUENCE (its input closed), not a decision"
        );
        assert!(
            matches!(outcome, RunOutcome::Completed),
            "a branch not taken is not a failure: {outcome:?}"
        );
    }

    /// The same graph with a guard that says yes runs the whole chain,
    /// so the skip above is the guard's doing and nothing else's.
    #[tokio::test]
    async fn a_true_guard_lets_the_chain_run() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) =
            drive(guarded_project(), branch_catalog(Some(json!(true)), &ran), &["source"]).await;

        assert_eq!(*ran.lock().unwrap(), vec!["guarded", "behind"]);
        assert!(skip_reason(&events, "guarded").is_none());
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
    }

    /// A guard that never spoke is its own reason: nothing decided, so
    /// the node does not run, and the inspector can say which of the two
    /// happened.
    #[tokio::test]
    async fn a_closed_guard_skips_with_its_own_reason() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        // The source never mentions `allowed`, so that port closes when
        // its firing ends: the structural "nothing is coming on this
        // one". (A null would not close it, it would fail the source:
        // the port is declared `Boolean` and its declared type judges
        // every value.)
        let (_, events) =
            drive(guarded_project(), branch_catalog(None, &ran), &["source"]).await;

        assert!(ran.lock().unwrap().is_empty());
        assert_eq!(skip_reason(&events, "guarded"), Some(SkipReason::FlowClosed { failure: None }));
    }

    /// An ENTRY node (no incoming edges, dispatched through the kick
    /// path) still answers to `_should_flow: false` written in its
    /// braces: the run skips it instead of running it anyway, as its
    /// author's decision, and the node reading it is skipped as the
    /// consequence (its input can never come), so the two read apart.
    #[tokio::test]
    async fn a_false_guard_literal_turns_off_an_entry_node() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [{
                "id": "solo", "nodeType": "Solo", "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [
                    { "name": "value", "portType": "String", "required": false },
                    { "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }
                ],
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": [],
                "portLiterals": { SHOULD_FLOW_PORT: false }
            }, {
                "id": "after", "nodeType": "Behind", "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [{ "name": "value", "portType": "String", "required": true }],
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            }],
            "edges": [{ "id": "e1", "source": "solo", "target": "after", "sourceHandle": "value", "targetHandle": "value" }],
            "groups": []
        }))
        .expect("solo project");
        let cat = catalog(vec![
            ("Solo", Box::new(Echo { id: "solo", ran: ran.clone() })),
            ("Behind", Box::new(Echo { id: "after", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(project, cat, &["solo"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "neither the turned-off entry node nor its reader may run: {:?}",
            ran.lock().unwrap()
        );
        assert_eq!(skip_reason(&events, "solo"), Some(SkipReason::DidNotFlow));
        assert_eq!(
            skip_reason(&events, "after"),
            Some(SkipReason::RequiredInputClosed { port: "value".into(), failure: None }),
            "the reader is a consequence, not a decision"
        );
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
    }

    /// The birth of `run_id` running all of `project`, kicked at `kick`.
    fn birth_of(project: &ProjectDefinition, run_id: RunId, kick: &str) -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id, project_id: project.id, entry_node: kick.into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                cut_from: None, binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
                seed: None, instance: None, fired_trigger: None, stand_in: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            RunEvent::NodeKicked { run_id, node_id: kick.into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ]
    }

    /// Emits `value`, then waits for a person; once answered it sends
    /// the answer on.
    struct Asks;
    test_manifest!(Asks, "Asks");
    #[async_trait]
    impl Node for Asks {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let answer = ctx
                .await_signal(weft_core::signal::Form {
                    form_type: "human_query".into(),
                    schema: weft_core::signal::FormSchema { fields: Vec::new() },
                    title: None,
                    description: None,
                    consumer_kind: None,
                })
                .await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!(answer.to_string()))).await
        }
    }

    /// A question a person skips skips the node that asked it, with that
    /// reason, and the node reading its answer is skipped in turn; the
    /// run completes. The run parks on the question, the skip lands on
    /// its record, and the next drive of the run carries it on from there.
    #[tokio::test]
    async fn a_skipped_question_skips_its_node_and_the_run_completes() {
        use super::engine_test_rig::{clients, drive_on, AwaitTasks, MemJournal};
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let mut project = guarded_project();
        project.nodes.iter_mut().find(|node| node.id == "guarded").expect("guarded").node_type = "Asks".into();
        project.edges.retain(|edge| edge.target_handle.as_deref() != Some(SHOULD_FLOW_PORT));
        let project = Arc::new(project);
        let nodes = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Asks", Box::new(Asks)),
            ("Behind", Box::new(Echo { id: "behind", ran: ran.clone() })),
        ]);
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth_of(&project, run_id, "source"));
        let tasks = AwaitTasks::new();
        let with_tasks = || EngineClients { tasks: tasks.clone(), ..clients(journal.clone()) };

        let parked = drive_on(project.clone(), nodes.clone(), run_id, journal.clone(), with_tasks(), CancellationFlag::new_arc(), None)
            .await
            .expect("the first drive runs")
            .outcome;
        assert!(matches!(parked, RunOutcome::Stalled), "the run parks on its question: {parked:?}");

        let token = tasks.minted().expect("the question was registered");
        journal.seed(&[RunEvent::wait_answered(run_id, token, weft_core::primitive::WaitAnswer::Skipped, 1)]);
        let ended = drive_on(project, nodes, run_id, journal.clone(), with_tasks(), CancellationFlag::new_arc(), None)
            .await
            .expect("the second drive runs")
            .outcome;
        assert!(matches!(ended, RunOutcome::Completed), "a skipped question is not a failure: {ended:?}");

        let events = journal.events_of(run_id);
        assert_eq!(skip_reason(&events, "guarded"), Some(SkipReason::WaitSkipped), "{events:?}");
        assert_eq!(
            skip_reason(&events, "behind"),
            Some(SkipReason::RequiredInputClosed { port: "value".into(), failure: None }),
            "the reader of the answer never gets one"
        );
        assert!(ran.lock().unwrap().is_empty(), "nothing behind the question ran: {:?}", ran.lock().unwrap());
    }

    /// Stops every run tagged `everyone`, its own run included, then
    /// sends on.
    struct StopsEveryone;
    test_manifest!(StopsEveryone, "StopsEveryone");
    #[async_trait]
    impl Node for StopsEveryone {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.stop_tagged("everyone", weft_core::StopSelf::Include).await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("went on"))).await
        }
    }

    /// The broker's side of a stop by tag that takes the asker too: it
    /// answers that the asker is among the runs stopped, and the stop
    /// reaches the asker's run the way the line brings it, as a cancel of
    /// its flag naming who asked and with which tag.
    struct StopsTheAsker {
        flag: Arc<CancellationFlag>,
        asked: StdMutex<Vec<(RunId, String)>>,
    }
    #[async_trait]
    impl crate::context::RunSteeringClient for StopsTheAsker {
        async fn tag_run(&self, _: RunId, _: Vec<String>) -> anyhow::Result<()> {
            unreachable!("the run tags nothing")
        }
        async fn stop_tagged(&self, run_id: RunId, tag: String, stop_self: weft_core::StopSelf) -> anyhow::Result<bool> {
            assert_eq!(stop_self, weft_core::StopSelf::Include);
            self.asked.lock().unwrap().push((run_id, tag.clone()));
            self.flag.cancel_because(weft_core::run::CancelCause::Run { by: run_id, tag: Some(tag) });
            Ok(true)
        }
    }

    /// A run that stops every run carrying a tag, itself included, stops
    /// where it asked: it ends cancelled by its own stop, the node that
    /// asked sends nothing on, and nothing after it starts.
    #[tokio::test]
    async fn a_run_that_stops_itself_by_tag_starts_nothing_after_the_stop() {
        use super::engine_test_rig::{clients, drive_on, MemJournal};
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let mut project = guarded_project();
        project.nodes.iter_mut().find(|node| node.id == "guarded").expect("guarded").node_type = "StopsEveryone".into();
        project.edges.retain(|edge| edge.target_handle.as_deref() != Some(SHOULD_FLOW_PORT));
        let nodes = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("StopsEveryone", Box::new(StopsEveryone)),
            ("Behind", Box::new(Echo { id: "behind", ran: ran.clone() })),
        ]);
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth_of(&project, run_id, "source"));
        let flag = CancellationFlag::new_arc();
        let steering = Arc::new(StopsTheAsker { flag: flag.clone(), asked: Default::default() });
        let with_steering = EngineClients { steering: steering.clone(), ..clients(journal.clone()) };

        let outcome = drive_on(Arc::new(project), nodes, run_id, journal.clone(), with_steering, flag, None)
            .await
            .expect("the drive runs")
            .outcome;
        assert!(
            matches!(&outcome, RunOutcome::Cancelled { cause: weft_core::run::CancelCause::Run { by, tag: Some(tag) } } if *by == run_id && tag == "everyone"),
            "the run is stopped by its own stop: {outcome:?}"
        );
        assert_eq!(*steering.asked.lock().unwrap(), vec![(run_id, "everyone".to_string())]);
        let events = journal.events_of(run_id);
        assert!(ran.lock().unwrap().is_empty(), "nothing after the stop ran: {:?}", ran.lock().unwrap());
        assert!(
            !events.iter().any(|event| matches!(event, RunEvent::NodeCompleted { node_id, .. } | RunEvent::NodeStarted { node_id, .. } if node_id == "behind")),
            "nothing after the stop starts: {events:?}"
        );
        assert!(
            !events.iter().any(|event| matches!(event, RunEvent::PortEmitted { node_id, .. } if node_id == "guarded")),
            "the asker sends nothing on: {events:?}"
        );
    }

    /// A run of a cut (a trigger fire's program) stays inside it: a node
    /// the cut left out gets no row, its body does not run, and the run
    /// completes.
    #[tokio::test]
    async fn a_cut_run_leaves_the_outside_untouched() {
        use super::engine_test_rig::drive_scoped;
        use weft_core::cancellation::CancellationFlag;
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_scoped(
            guarded_project(),
            branch_catalog(Some(json!(true)), &ran),
            &["source"],
            Some(&["source", "guarded"]),
            CancellationFlag::new_arc(),
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(
            ran.lock().unwrap().as_slice(),
            &["guarded"],
            "only the in-subgraph node runs"
        );
        assert!(!touched(&events, "behind"), "an out-of-scope node leaves no trace: {events:?}");
    }

    /// Two programs in one file sharing an upstream node, the shape a
    /// trigger fire runs a cut of.
    ///
    ///   my_trigger ──► mine ──► leaf          (the fired program; `leaf`
    ///   shared ───────┘                        feeds no output)
    ///   shared ──► theirs ◄── their_trigger   (the other program)
    ///
    /// `shared` feeds both programs and emits down every wire it has,
    /// so on a fire of `my_trigger` run whole a pulse lands on `theirs`
    /// too. Two things are pinned. Anything outside the cut leaves no
    /// row: it is not this run's business (here the driver is handed a
    /// cut that leaves `leaf` out; the walk a fire's cut comes from, the
    /// downstream of the fired trigger plus what it needs, is pinned in
    /// `weft_core::run_spec`). And run whole, the same fire ends Stuck,
    /// `theirs` parked on a partial input set, which is the failure
    /// every two-trigger project used to hit on every fire.
    fn two_programs_project() -> ProjectDefinition {
        fn source(id: &str, trigger: bool) -> serde_json::Value {
            json!({
                "id": id, "nodeType": "Source", "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [],
                "outputs": [
                    { "name": "value", "portType": "String", "required": false },
                    { "name": "allowed", "portType": "Boolean", "required": false }
                ],
                "features": { "isTrigger": trigger }, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            })
        }
        fn sink(id: &str, node_type: &str, inputs: &[&str]) -> serde_json::Value {
            let mut ins: Vec<serde_json::Value> = inputs
                .iter()
                .map(|n| json!({ "name": n, "portType": "String", "required": true }))
                .collect();
            ins.push(json!({ "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }));
            json!({
                "id": id, "nodeType": node_type, "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": ins,
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            })
        }
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                source("my_trigger", true),
                source("shared", false),
                source("their_trigger", true),
                sink("mine", "Echo", &["value", "extra"]),
                sink("leaf", "Behind", &["value"]),
                sink("theirs", "Behind", &["value", "go"])
            ],
            "edges": [
                { "id": "e1", "source": "my_trigger", "target": "mine",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e2", "source": "shared", "target": "mine",
                  "sourceHandle": "value", "targetHandle": "extra" },
                { "id": "e3", "source": "mine", "target": "leaf",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e4", "source": "shared", "target": "theirs",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e5", "source": "their_trigger", "target": "theirs",
                  "sourceHandle": "value", "targetHandle": "go" }
            ],
            "groups": []
        }))
        .expect("two-programs project")
    }

    fn touched(events: &[RunEvent], node: &str) -> bool {
        events.iter().any(|e| match e {
            RunEvent::NodeStarted { node_id, .. }
            | RunEvent::NodeSkipped { node_id, .. }
            | RunEvent::NodeCompleted { node_id, .. } => node_id == node,
            _ => false,
        })
    }

    #[tokio::test]
    async fn a_fire_keeps_a_shared_node_out_of_the_other_program() {
        use super::engine_test_rig::{drive_fire, drive_scoped};
        use weft_core::cancellation::CancellationFlag;

        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_fire(
            two_programs_project(),
            branch_catalog(Some(json!(true)), &ran),
            "my_trigger",
            &["my_trigger", "shared"],
            Some(&["my_trigger", "shared", "mine"]),
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["guarded"], "only the fired program's node runs");
        // The other program: nothing, not even a skip row.
        assert!(!touched(&events, "theirs"), "another program's node must leave no trace: {events:?}");
        // Outside the program the driver was handed: nothing either.
        assert!(!touched(&events, "leaf"), "{events:?}");

        // The fired program's own leaf (it feeds nothing) runs once the
        // fire's cut holds it, and the other program still leaves no trace.
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_fire(
            two_programs_project(),
            branch_catalog(Some(json!(true)), &ran),
            "my_trigger",
            &["my_trigger", "shared"],
            Some(&["my_trigger", "shared", "mine", "leaf"]),
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["guarded", "behind"], "`mine`, then its leaf");
        assert!(
            events.iter().any(|e| matches!(e, RunEvent::NodeCompleted { node_id, .. } if node_id == "leaf")),
            "the fired program's leaf completes: {events:?}"
        );
        assert!(!touched(&events, "theirs"), "{events:?}");

        // The same fire run whole: the shared pulse parks on
        // `theirs`, whose other input never comes, and the run is Stuck.
        // Pinned so "trigger fires run the whole graph" cannot come back
        // looking harmless.
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, _) = drive_scoped(
            two_programs_project(),
            branch_catalog(Some(json!(true)), &ran),
            &["my_trigger", "shared"],
            None,
            CancellationFlag::new_arc(),
        )
        .await;
        // The terminal names the firing that can never complete and the
        // wired port it never got, so the reader sees the leak without
        // opening the replay.
        let RunOutcome::Stuck { report } = outcome else { panic!("{outcome:?}") };
        let text = report.to_string();
        assert!(text.contains("theirs has value, still waiting on go"), "{text}");
    }

    /// A node outside the cut fed by only SOME of its parents (its other
    /// parent is an entry node outside the cut too) never parks a pulse
    /// waiting for an input that will never come: the cut holds neither
    /// it nor the wires into it.
    #[tokio::test]
    async fn a_partially_fed_node_outside_the_cut_never_parks() {
        use super::engine_test_rig::drive_scoped;
        use weft_core::cancellation::CancellationFlag;
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "source", "nodeType": "Source", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [
                        { "name": "value", "portType": "String", "required": false },
                        { "name": "allowed", "portType": "Boolean", "required": false }
                    ],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "a", "nodeType": "Echo", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "value", "portType": "String", "required": true }],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "orphan", "nodeType": "Orphan", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "b", "nodeType": "Behind", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": "extra", "portType": "String", "required": true }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "source", "target": "a",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e2", "source": "source", "target": "b",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e3", "source": "orphan", "target": "b",
                  "sourceHandle": "value", "targetHandle": "extra" }
            ],
            "groups": []
        }))
        .expect("diamond project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Echo", Box::new(Echo { id: "a", ran: ran.clone() })),
            ("Orphan", Box::new(Echo { id: "orphan", ran: ran.clone() })),
            ("Behind", Box::new(Echo { id: "b", ran: ran.clone() })),
        ]);
        // Aimed at `a`: only source is kicked, orphan never fires, and
        // b receives a pulse on `value` alone.
        let (outcome, events) = drive_scoped(
            project,
            cat,
            &["source"],
            Some(&["source", "a"]),
            CancellationFlag::new_arc(),
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["a"]);
        assert!(!touched(&events, "b"), "{events:?}");
    }

    /// A node outside the cut whose parents inside it send across
    /// SEPARATE ticks (one emits now, the other later) leaves no trace
    /// either time.
    #[tokio::test]
    async fn a_node_outside_the_cut_fed_across_ticks_never_appears() {
        use super::engine_test_rig::drive_scoped;
        use weft_core::cancellation::CancellationFlag;
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "source", "nodeType": "Source", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [
                        { "name": "value", "portType": "String", "required": false },
                        { "name": "allowed", "portType": "Boolean", "required": false }
                    ],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "a", "nodeType": "Echo", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "value", "portType": "String", "required": true }],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "b", "nodeType": "Behind", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": "extra", "portType": "String", "required": true }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "source", "target": "a",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e2", "source": "source", "target": "b",
                  "sourceHandle": "value", "targetHandle": "value" },
                { "id": "e3", "source": "a", "target": "b",
                  "sourceHandle": "value", "targetHandle": "extra" }
            ],
            "groups": []
        }))
        .expect("two-wave diamond");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Echo", Box::new(Echo { id: "a", ran: ran.clone() })),
            ("Behind", Box::new(Echo { id: "b", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive_scoped(
            project,
            cat,
            &["source"],
            Some(&["source", "a"]),
            CancellationFlag::new_arc(),
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["a"]);
        assert!(!touched(&events, "b"), "no row for the out-of-scope node: {events:?}");
    }

    /// A group of the program: its id, the group around it, its input
    /// and output ports (each `String`), the literals written on its
    /// ports (its gate's included), and its direct members.
    fn group(id: &str, parent: Option<&str>, ins: &[&str], outs: &[&str], literals: serde_json::Value, members: &[&str]) -> serde_json::Value {
        let ports = |names: &[&str]| names.iter().map(|name| json!({ "name": name, "portType": "String", "required": false })).collect::<Vec<_>>();
        json!({
            "id": id, "kind": "group", "parentGroupId": parent,
            "inPorts": ports(ins), "outPorts": ports(outs),
            "portLiterals": literals, "nodeIds": members
        })
    }

    /// One wire end to end, crossing the group ports `via` on the way,
    /// each `(group, port, side)`.
    fn through(id: &str, source: &str, source_port: &str, via: &[(&str, &str, &str)], target: &str, target_port: &str) -> serde_json::Value {
        json!({
            "id": id, "source": source, "target": target, "sourceHandle": source_port, "targetHandle": target_port,
            "via": via.iter().map(|(group, port, side)| json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>()
        })
    }

    /// A group nested two deep, guarded from the outer group's braces.
    /// source -> outer.value -> outer.inner.value -> deep ->
    /// outer.inner.value -> outer.value -> after, each one wire.
    fn nested_group_project() -> ProjectDefinition {
        let echo = |id: &str, scope: serde_json::Value| {
            json!({
                "id": id, "nodeType": id, "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [
                    { "name": "value", "portType": "String", "required": true },
                    { "name": SHOULD_FLOW_PORT, "portType": "T", "required": false }
                ],
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": scope, "groupBoundary": null,
                "requiresInfra": false, "images": []
            })
        };
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "source", "nodeType": "Source", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [
                        { "name": "value", "portType": "String", "required": false },
                        { "name": "allowed", "portType": "Boolean", "required": false }
                    ],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                echo("deep", json!(["outer", "outer.inner"])),
                echo("after", json!([]))
            ],
            "edges": [
                through("e1", "source", "value", &[("outer", "value", "in"), ("outer.inner", "value", "in")], "deep", "value"),
                through("e2", "deep", "value", &[("outer.inner", "value", "out"), ("outer", "value", "out")], "after", "value")
            ],
            "groups": [
                group("outer", None, &["value"], &["value"], json!({ SHOULD_FLOW_PORT: false }), &[]),
                group("outer.inner", Some("outer"), &["value"], &["value"], json!({}), &["deep"])
            ]
        }))
        .expect("nested project")
    }

    /// [`nested_group_project`] with its outer gate taken out of the
    /// braces: both groups run.
    fn open_nested_group_project() -> ProjectDefinition {
        let mut project = nested_group_project();
        let outer = project.groups.iter_mut().find(|g| g.id == "outer").expect("outer");
        outer.port_literals.remove(SHOULD_FLOW_PORT);
        project
    }

    /// `_should_flow: false` in a group's braces takes the whole scope
    /// down, nesting included: the outer scope does not run, with its
    /// reason, and the wire leaving it closes (so what comes after
    /// skips), and everything inside, the nested group and the node two
    /// levels in alike, is told its scope did not run.
    #[tokio::test]
    async fn a_guarded_group_skips_every_node_nested_inside_it() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Echo { id: "deep", ran: ran.clone() })),
            ("after", Box::new(Echo { id: "after", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(nested_group_project(), cat, &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "nothing inside the group, and nothing after it, may run: {:?}",
            ran.lock().unwrap()
        );
        let (_, decided) = fold_with_scopes(nested_group_project(), &events);
        assert_eq!(scope_state(&decided, "outer"), ScopeState::Skipped { reason: SkipReason::DidNotFlow });
        let scope_skipped = SkipReason::ScopeSkipped { scope: "outer".into() };
        assert_eq!(skip_reason(&events, "deep"), Some(scope_skipped.clone()), "the node two levels in");
        assert_eq!(scope_state(&decided, "outer.inner"), ScopeState::Skipped { reason: scope_skipped }, "the nested group");
        assert_eq!(
            skip_reason(&events, "after"),
            Some(SkipReason::RequiredInputClosed { port: "value".into(), failure: None }),
            "outside the scope, the closure cascade carries on"
        );
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_fold_settles(nested_group_project(), &events);
    }

    /// The same nesting with the gate open: the value crosses two
    /// groups in and two out on one wire each way, and the journal
    /// (which holds no row for either group) folds back to the settled
    /// run the engine held, both scopes open.
    #[tokio::test]
    async fn a_nested_group_run_folds_back_to_its_settled_state() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let project = open_nested_group_project();
        let project_again = project.clone();
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Echo { id: "deep", ran: ran.clone() })),
            ("after", Box::new(Echo { id: "after", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(project, cat, &["source"]).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["deep", "after"]);
        assert!(
            !events.iter().any(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id.starts_with("outer"))),
            "a group never journals a row: {events:?}"
        );
        let snap = fold(project_again.clone(), &events);
        for scope in ["outer", "outer.inner"] {
            assert_eq!(snapshot_scope(&snap, scope), Some(ScopeState::Open), "{scope}");
        }
        assert_fold_settles(project_again, &events);
    }

    /// A literal written on a group's port (`g.b = "fixed"`) reaches the
    /// members that read it, sent by the group as it opens, the same way
    /// a node's own braces literal reaches the node. Beside it a wire
    /// enters on the group's other port (`g.a = source.value`): the group
    /// opens on that wire's value, then sends the literal.
    #[tokio::test]
    async fn a_literal_on_a_group_port_reaches_the_nodes_inside() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "source", "nodeType": "Source", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [
                        { "name": "value", "portType": "String", "required": false },
                        { "name": "allowed", "portType": "Boolean", "required": false }
                    ],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "deep", "nodeType": "deep", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": "other", "portType": "String", "required": true }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": ["g"], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                through("e0", "source", "value", &[("g", "a", "in")], "deep", "other"),
                through("e1", "g", "b", &[("g", "b", "in")], "deep", "value")
            ],
            "groups": [group("g", None, &["a", "b"], &[], json!({ "b": "fixed" }), &["deep"])]
        }))
        .expect("literal-port project");

        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, _) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["fixed".to_string()]);
    }

    /// Emits a fixed list, for a loop to walk.
    struct Lister;
    test_manifest!(Lister, "Lister");
    #[async_trait]
    impl Node for Lister {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("items", json!(["a", "b"]))).await
        }
    }

    fn plain_node(id: &str, node_type: &str, scope: serde_json::Value, inputs: &[(&str, bool)]) -> serde_json::Value {
        let inputs: Vec<serde_json::Value> = inputs
            .iter()
            .map(|(name, required)| json!({ "name": name, "portType": "String", "required": required }))
            .collect();
        json!({
            "id": id, "nodeType": node_type, "label": null,
            "config": null, "position": { "x": 0.0, "y": 0.0 },
            "inputs": inputs,
            "outputs": [
                { "name": "value", "portType": "String", "required": false },
                { "name": "allowed", "portType": "Boolean", "required": false }
            ],
            "features": {}, "scope": scope, "groupBoundary": null,
            "requiresInfra": false, "images": []
        })
    }

    fn edge(id: &str, source: &str, source_port: &str, target: &str, target_port: &str) -> serde_json::Value {
        json!({ "id": id, "source": source, "target": target,
                "sourceHandle": source_port, "targetHandle": target_port })
    }

    /// `source.allowed` wired into `g`'s `_should_flow`, and inside `g` a
    /// ROOT (`seed`, no inputs at all) feeding `deep`. A body root has
    /// no wire to start it: the group kicks it when it opens.
    fn rooted_group_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("seed", "Seed", json!(["g"]), &[]),
                plain_node("deep", "deep", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e2", "source", "allowed", "g", SHOULD_FLOW_PORT),
                edge("e3", "seed", "value", "deep", "value")
            ],
            "groups": [group("g", None, &[], &[], json!({}), &["seed", "deep"])]
        }))
        .expect("rooted group project")
    }

    /// Inside `g` a root `boom` that FAILS, its value leaving `g` on `v`
    /// for `watcher`, which runs on it NOT arriving (`_should_not_flow`)
    /// with its own `value` from `source`.
    fn failing_group_watched_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("boom", "Boom", json!(["g"]), &[]),
                {
                    "id": "watcher", "nodeType": "Echo", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": SHOULD_NOT_FLOW_PORT, "portType": "T", "required": false }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                through("e2", "boom", "value", &[("g", "v", "out")], "watcher", SHOULD_NOT_FLOW_PORT),
                edge("e4", "source", "value", "watcher", "value")
            ],
            "groups": [group("g", None, &[], &["v"], json!({}), &["boom"])]
        }))
        .expect("failing group watched project")
    }

    /// A failure INSIDE a group reaches a watcher outside as a failure.
    /// The closure that leaves the group carries its error rather than a
    /// plain "nothing came", so the branch written for the absence stays
    /// off one scope up too.
    #[tokio::test]
    async fn a_failure_inside_a_group_keeps_an_outside_inverted_gate_off() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Boom", Box::new(FailingSource)),
            ("Echo", Box::new(Echo { id: "watcher", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(failing_group_watched_project(), cat, &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "the failure left the group as one, so the watcher stayed off: {:?}",
            ran.lock().unwrap()
        );
        match skip_reason(&events, "watcher") {
            Some(SkipReason::WatchedNodeFailed { failure }) => {
                assert!(failure.error.contains("the database is down"), "the wire kept the error: {failure}");
            }
            other => panic!("expected the watched-node-failed reason through the group, got {other:?}"),
        }
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
    }

    /// `boom -> middle.value` (required) and `middle.value ->
    /// watcher._should_not_flow`, with `source` feeding the watcher's
    /// own value. The failure has to cross a SKIPPED node to reach the
    /// watcher.
    fn failure_two_hops_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("boom", "Boom", json!([]), &[]),
                plain_node("middle", "Middle", json!([]), &[("value", true)]),
                {
                    "id": "watcher", "nodeType": "Echo", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": SHOULD_NOT_FLOW_PORT, "portType": "T", "required": false }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                edge("e1", "boom", "value", "middle", "value"),
                edge("e2", "middle", "value", "watcher", SHOULD_NOT_FLOW_PORT),
                edge("e3", "source", "value", "watcher", "value")
            ],
            "groups": []
        }))
        .expect("failure two hops project")
    }

    /// A node that skipped because its input closed on a failure did
    /// not decline either: its own closures carry the failure on, so a
    /// watcher two hops from the node that broke still reads a failure.
    /// Otherwise one skipped node between them would launder the
    /// failure back into the absence the watcher runs on.
    #[tokio::test]
    async fn a_failure_behind_a_skipped_node_keeps_the_inverted_gate_off() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Boom", Box::new(FailingSource)),
            ("Middle", Box::new(Echo { id: "middle", ran: ran.clone() })),
            ("Echo", Box::new(Echo { id: "watcher", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(failure_two_hops_project(), cat, &["source", "boom"]).await;

        assert!(ran.lock().unwrap().is_empty(), "nothing after the failure runs: {:?}", ran.lock().unwrap());
        match skip_reason(&events, "middle") {
            Some(SkipReason::RequiredInputClosed { port, failure: Some(failure) }) => {
                assert_eq!(port, "value");
                assert_eq!(failure.node, "boom", "the skip names the node that broke: {failure}");
                assert!(failure.error.contains("the database is down"), "the skip names the failure it inherited: {failure}");
            }
            other => panic!("expected the required input closed ON A FAILURE, got {other:?}"),
        }
        // Two hops out (boom failed, middle skipped, watcher reads
        // middle's closure), the failure still names `boom`, not `middle`.
        match skip_reason(&events, "watcher") {
            Some(SkipReason::WatchedNodeFailed { failure }) => {
                assert_eq!(failure.node, "boom", "the origin survives two hops: {failure}");
                assert!(failure.error.contains("the database is down"), "the skipped node passed the error on: {failure}");
            }
            other => panic!("expected the watched-node-failed reason two hops out, got {other:?}"),
        }
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
    }

    /// `boom.value -> g._should_flow`, a live root `inner` inside `g`
    /// whose value leaves `g` on `v` for `watcher._should_not_flow`. The
    /// group never runs because its gate closed on a failure.
    fn failure_gated_group_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("boom", "Boom", json!([]), &[]),
                plain_node("inner", "Inner", json!(["g"]), &[]),
                {
                    "id": "watcher", "nodeType": "Echo", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "value", "portType": "String", "required": true },
                        { "name": SHOULD_NOT_FLOW_PORT, "portType": "T", "required": false }
                    ],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                edge("e1", "boom", "value", "g", SHOULD_FLOW_PORT),
                through("e2", "inner", "value", &[("g", "v", "out")], "watcher", SHOULD_NOT_FLOW_PORT),
                edge("e4", "source", "value", "watcher", "value")
            ],
            "groups": [group("g", None, &[], &["v"], json!({}), &["inner"])]
        }))
        .expect("failure gated group project")
    }

    /// A scope whose gate closed on a failure is torn down as a failed
    /// scope: its outward closures carry the error, and a watcher on the
    /// group's output stays off. The gate's skip names the failure too.
    #[tokio::test]
    async fn a_group_gated_off_by_a_failure_keeps_an_outside_inverted_gate_off() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Boom", Box::new(FailingSource)),
            ("Inner", Box::new(Source { allowed: Some(json!(true)) })),
            ("Echo", Box::new(Echo { id: "watcher", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(failure_gated_group_project(), cat, &["source", "boom"]).await;

        assert!(ran.lock().unwrap().is_empty(), "the watcher stayed off: {:?}", ran.lock().unwrap());
        let (_, decided) = fold_with_scopes(failure_gated_group_project(), &events);
        match scope_state(&decided, "g") {
            ScopeState::Skipped { reason: SkipReason::FlowClosed { failure: Some(failure) } } => {
                assert_eq!(failure.node, "boom", "the gate's skip names the node that broke: {failure}");
                assert!(failure.error.contains("the database is down"), "and its error: {failure}");
            }
            other => panic!("expected the gate closed ON A FAILURE, got {other:?}"),
        }
        // Two hops on (the scope did not run, the wire leaving it
        // closed on to the watcher), the failure still names `boom`.
        match skip_reason(&events, "watcher") {
            Some(SkipReason::WatchedNodeFailed { failure }) => {
                assert_eq!(failure.node, "boom", "the origin survives the skips: {failure}");
                assert!(failure.error.contains("the database is down"), "the scope teardown kept the error: {failure}");
            }
            other => panic!("expected the watched-node-failed reason outside the gated scope, got {other:?}"),
        }
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
    }

    /// A node inside a group that nothing feeds starts when the group
    /// does: the opening kicks it at the group's frames, and what it
    /// emits reaches the member behind it.
    #[tokio::test]
    async fn a_group_body_root_starts_when_the_group_starts() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Seed", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(rooted_group_project(), cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string()], "the root's value reached deep");
        let snap = fold(rooted_group_project(), &events);
        let kick = snap.kicked.get(&FiringLocation::new("seed", vec![])).expect("the opening kicked the root");
        assert!(kick.dispatched() && kick.scope_skipped.is_none(), "{kick:?}");
        assert_eq!(snapshot_scope(&snap, "g"), Some(ScopeState::Open));
    }

    /// The same root when the group's `_should_flow` says no: nothing
    /// inside starts, and the root skips with the scope's reason like
    /// every other member, so the inspector never shows a hole.
    #[tokio::test]
    async fn a_group_body_root_is_skipped_with_its_gated_scope() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(false)) })),
            ("Seed", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(rooted_group_project(), cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "{:?}", ran.lock().unwrap());
        // The group itself journals nothing; the fold holds its
        // decision and the skip kicks of every member.
        let snap = fold(rooted_group_project(), &events);
        assert_eq!(snapshot_scope(&snap, "g"), Some(ScopeState::Skipped { reason: SkipReason::DidNotFlow }));
        assert_eq!(
            snap.kicked.get(&FiringLocation::new("seed", vec![])).map(|k| k.scope_skipped.as_deref()),
            Some(Some("g")),
            "a gated scope kicks its members into a skip"
        );
        let scope_skipped = Some(SkipReason::ScopeSkipped { scope: "g".into() });
        assert_eq!(skip_reason(&events, "seed"), scope_skipped, "the root, which no wire feeds");
        assert_eq!(skip_reason(&events, "deep"), scope_skipped);
    }

    /// The same group with its gate wired the OTHER way round, built by
    /// renaming the gate the wire lands on, which is what the editor's
    /// flip does.
    fn inverted_rooted_group_project() -> ProjectDefinition {
        let mut project = rooted_group_project();
        for edge in &mut project.edges {
            if edge.target_handle.as_deref() == Some(SHOULD_FLOW_PORT) {
                edge.target_handle = Some(SHOULD_NOT_FLOW_PORT.to_string());
            }
        }
        project
    }

    /// A whole SCOPE gated on an absence. The thing the group watches
    /// never arrived, so the group runs, and everything its opening
    /// starts runs too: the body root nothing feeds is kicked at the
    /// group's frames exactly as it is under the plain gate.
    #[tokio::test]
    async fn a_closed_inverted_group_gate_launches_the_whole_scope() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            // `allowed` is never mentioned, so the wire into the gate
            // closes: the absence this group was written to act on.
            ("Source", Box::new(Source { allowed: None })),
            ("Seed", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(inverted_rooted_group_project(), cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["payload".to_string()],
            "the body root ran and its value reached the member behind it"
        );
        let snap = fold(inverted_rooted_group_project(), &events);
        let kick = snap
            .kicked
            .get(&FiringLocation::new("seed", vec![]))
            .expect("the opening kicked the root of a scope an absence opened");
        assert!(kick.dispatched() && kick.scope_skipped.is_none(), "{kick:?}");
        assert_eq!(snapshot_scope(&snap, "g"), Some(ScopeState::Open));
    }

    /// And the mirror: the thing DID arrive, so the scope stays shut and
    /// every member carries the scope's reason, the loose root included.
    /// The cascade is the gate's, not the plain spelling's.
    #[tokio::test]
    async fn a_value_on_an_inverted_group_gate_skips_the_whole_scope() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Seed", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(inverted_rooted_group_project(), cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "{:?}", ran.lock().unwrap());
        let (snap, decided) = fold_with_scopes(inverted_rooted_group_project(), &events);
        assert_eq!(
            snap.kicked.get(&FiringLocation::new("seed", vec![])).map(|k| k.scope_skipped.as_deref()),
            Some(Some("g")),
            "an inverted gate kicks its members into a skip the same way"
        );
        let scope_skipped = Some(SkipReason::ScopeSkipped { scope: "g".into() });
        assert_eq!(skip_reason(&events, "seed"), scope_skipped, "the root, which no wire feeds");
        assert_eq!(skip_reason(&events, "deep"), scope_skipped);
        assert_eq!(
            scope_state(&decided, "g"),
            ScopeState::Skipped { reason: SkipReason::DidFlow },
            "the scope's OWN reason says which gate decided"
        );
    }

    /// `_should_flow` on a group from outside, when whatever decides it
    /// never spoke: the closure is a no for the scope (its own reason),
    /// and the members carry the scope's.
    #[tokio::test]
    async fn a_closed_group_guard_skips_the_scope_with_its_own_reason() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        // gate: guarded off by the source, so its `value` closes into g's flow port.
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("gate", "gate", json!([]), &[("value", true), (SHOULD_FLOW_PORT, false)]),
                plain_node("deep", "deep", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "gate", "value"),
                edge("e2", "source", "allowed", "gate", SHOULD_FLOW_PORT),
                through("e3", "source", "value", &[("g", "a", "in")], "deep", "value"),
                edge("e4", "gate", "value", "g", SHOULD_FLOW_PORT)
            ],
            "groups": [group("g", None, &["a"], &[], json!({}), &["deep"])]
        }))
        .expect("closed guard project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(false)) })),
            ("gate", Box::new(Echo { id: "gate", ran: ran.clone() })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "{:?}", ran.lock().unwrap());
        assert_eq!(skip_reason(&events, "gate"), Some(SkipReason::DidNotFlow));
        let (_, decided) = fold_with_scopes(project_again, &events);
        assert_eq!(scope_state(&decided, "g"), ScopeState::Skipped { reason: SkipReason::FlowClosed { failure: None } });
        assert_eq!(skip_reason(&events, "deep"), Some(SkipReason::ScopeSkipped { scope: "g".into() }));
    }

    /// A group input that arrives closed is not a reason to skip the
    /// group: it reaches the inside as a closure, the member that needs
    /// it skips there, and a sibling reading another port runs.
    #[tokio::test]
    async fn a_closed_group_input_spares_the_members_that_do_not_read_it() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("gate", "gate", json!([]), &[("value", true), (SHOULD_FLOW_PORT, false)]),
                plain_node("reads_a", "reads_a", json!(["g"]), &[("value", true)]),
                plain_node("reads_b", "reads_b", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "gate", "value"),
                edge("e2", "source", "allowed", "gate", SHOULD_FLOW_PORT),
                through("e3", "source", "value", &[("g", "a", "in")], "reads_a", "value"),
                through("e4", "gate", "value", &[("g", "b", "in")], "reads_b", "value")
            ],
            "groups": [group("g", None, &["a", "b"], &[], json!({}), &["reads_a", "reads_b"])]
        }))
        .expect("closed input project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(false)) })),
            ("gate", Box::new(Echo { id: "gate", ran: ran.clone() })),
            ("reads_a", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
            ("reads_b", Box::new(Echo { id: "reads_b", ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string()], "reads_a ran on the open port");
        // A group never journals: how it was decided is read off the
        // fold, and it ran (a closed input gates a member, never the
        // group, as long as another input brought a value).
        let (_, decided) = fold_with_scopes(project_again, &events);
        assert_eq!(scope_state(&decided, "g"), ScopeState::Open, "the group itself runs");
        assert_eq!(
            skip_reason(&events, "reads_b"),
            Some(SkipReason::RequiredInputClosed { port: "value".into(), failure: None }),
            "the closure lands on the member that needs it"
        );
    }

    /// A group with no gate whose ONLY input arrives closed does not run:
    /// nothing it was written to work on came, so its body root is
    /// kicked into the scope's skip with every other member, and the run
    /// completes instead of parking on a body nobody started.
    #[tokio::test]
    async fn a_group_whose_every_input_closes_does_not_run() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("gate", "gate", json!([]), &[("value", true), (SHOULD_FLOW_PORT, false)]),
                plain_node("seed", "Seed", json!(["g"]), &[]),
                plain_node("deep", "deep", json!(["g"]), &[("value", true)]),
                plain_node("reads_a", "reads_a", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "gate", "value"),
                edge("e2", "source", "allowed", "gate", SHOULD_FLOW_PORT),
                through("e3", "gate", "value", &[("g", "a", "in")], "reads_a", "value"),
                edge("e4", "seed", "value", "deep", "value")
            ],
            "groups": [group("g", None, &["a"], &[], json!({}), &["seed", "deep", "reads_a"])]
        }))
        .expect("closed-only-input project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(false)) })),
            ("gate", Box::new(Echo { id: "gate", ran: ran.clone() })),
            ("Seed", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
            ("reads_a", Box::new(Echo { id: "reads_a", ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let (snap, decided) = fold_with_scopes(project_again, &events);
        assert_eq!(scope_state(&decided, "g"), ScopeState::Skipped { reason: SkipReason::EveryInputClosed { failure: None } });
        assert!(seen.lock().unwrap().is_empty(), "nothing inside ran: {:?}", seen.lock().unwrap());
        let scope_skipped = Some(SkipReason::ScopeSkipped { scope: "g".into() });
        for member in ["seed", "deep", "reads_a"] {
            assert_eq!(skip_reason(&events, member), scope_skipped, "{member}");
        }
        assert_eq!(
            snap.kicked.get(&FiringLocation::new("seed", vec![])).map(|k| k.scope_skipped.as_deref()),
            Some(Some("g")),
            "the root is kicked into the scope's skip"
        );
    }

    /// A node inside a LOOP body that nothing feeds fires once per
    /// iteration, at the iteration's frames: each launch opens its
    /// iteration and kicks it, so every iteration sees its own copy.
    #[tokio::test]
    async fn a_loop_body_root_fires_once_per_iteration() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "lister", "nodeType": "Lister", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "items", "portType": "List[String]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "l__in", "nodeType": "LoopIn", "label": null,
                    "config": { "parentId": "l", "parallel": false, "over": ["items"], "carry": [] },
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "items", "portType": "List[String]", "required": true }],
                    "outputs": [
                        { "name": "items", "portType": "String", "required": false },
                        { "name": "index", "portType": "Number", "required": false }
                    ],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "l", "role": "In" }
                },
                plain_node("seed", "Seed", json!(["l"]), &[]),
                plain_node("tally", "tally", json!(["l"]), &[("value", true)]),
                {
                    "id": "l__out", "nodeType": "LoopOut", "label": null,
                    "config": { "parentId": "l" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "results", "portType": "String", "required": false, "optional": true },
                        { "name": "done", "portType": "Boolean", "required": false, "optional": true }
                    ],
                    "outputs": [{ "name": "results", "portType": "List[String | Null]", "required": false }],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "l", "role": "Out" }
                }
            ],
            "edges": [
                edge("e1", "lister", "items", "l__in", "items"),
                edge("e2", "seed", "value", "tally", "value"),
                edge("e3", "tally", "value", "l__out", "results")
            ],
            "groups": [{ "id": "l", "kind": "loop", "nodeIds": ["seed", "tally"],
                         "loopConfig": { "parallel": false, "over": ["items"], "carry": [] } }]
        }))
        .expect("loop root project");
        let cat = catalog(vec![
            ("Lister", Box::new(Lister)),
            ("Seed", Box::new(Source { allowed: Some(json!(true)) })),
            ("tally", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["lister"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string(), "payload".to_string()]);
        assert_fold_settles(project_again, &events);
        let mut seed_frames: Vec<Vec<u32>> = events
            .iter()
            .filter_map(|e| match e {
                RunEvent::NodeStarted { node_id, frames, .. } if node_id == "seed" => {
                    Some(weft_core::frames::loop_indices(frames))
                }
                _ => None,
            })
            .collect();
        seed_frames.sort();
        assert_eq!(seed_frames, vec![vec![0], vec![1]], "one firing per iteration, at its frames");
    }

    /// Ends its own run the way a `cancel_run` task does: flips the
    /// run's flag WITH a cause, then parks instead of returning.
    /// Returning in the same instant the flag flips would race the
    /// driver's idle `select!` (the terminal arm can win and journal
    /// NodeFailed, leaving the cancel walk nothing to catch up on);
    /// parked, only the cancellation arm is ever ready, so this record
    /// is provably non-terminal when the walk runs. Every production
    /// canceller flips the flag from a separate task, so the ordering
    /// here only needs to hold for the self-stop shape this node exists
    /// to exercise.
    struct SelfStopper {
        cause: weft_core::run::CancelCause,
    }
    test_manifest!(SelfStopper, "SelfStopper");
    #[async_trait]
    impl Node for SelfStopper {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let flag = ctx.cancellation();
            flag.cancel_because(self.cause.clone());
            // Park until the teardown aborts this body.
            let () = std::future::pending().await;
            unreachable!("the run's teardown aborts this body first")
        }
    }

    /// The cause a canceller records on the flag is what the worker's
    /// terminal names: `RunCancelled` carries it structured AND as
    /// text, and the catch-up `NodeCancelled` rows use the same text, so
    /// a run stopped by a sibling reads as exactly that whichever side
    /// wrote its terminal first.
    #[tokio::test]
    async fn worker_terminal_names_the_recorded_cancel_cause() {
        let by = weft_core::RunId::new_v4();
        let cause = weft_core::run::CancelCause::Run { by, tag: Some("user_7".into()) };
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [{
                "id": "stopper", "nodeType": "SelfStopper", "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [],
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            }],
            "edges": []
        }))
        .expect("one-node project");
        let cat = catalog(vec![("SelfStopper", Box::new(SelfStopper { cause: cause.clone() }))]);
        let (outcome, events) = drive(project, cat, &["stopper"]).await;

        assert!(matches!(outcome, RunOutcome::Cancelled { .. }), "{outcome:?}");
        let terminal = events
            .iter()
            .find_map(|e| match e {
                RunEvent::RunCancelled { reason, cause, .. } => Some((reason.clone(), cause.clone())),
                _ => None,
            })
            .expect("a cancelled run journals RunCancelled");
        assert_eq!(terminal, (cause.to_string(), Some(cause.clone())), "{events:?}");
        assert_eq!(
            terminal.0,
            format!("Stopped by run {by} (tag user_7)"),
            "the text a person reads names the run and the tag"
        );
        // The parked body is what the cancel walk exists to catch: its
        // record must be non-terminal when the walk runs, so exactly one
        // NodeCancelled lands, naming the same cause. A walk that wrote
        // nothing would otherwise pass this loop vacuously.
        let node_cancels: Vec<&String> = events
            .iter()
            .filter_map(|e| match e {
                RunEvent::NodeCancelled { node_id, reason, .. } if node_id == "stopper" => Some(reason),
                _ => None,
            })
            .collect();
        assert_eq!(node_cancels.len(), 1, "the parked node gets one cancel row: {events:?}");
        assert_eq!(node_cancels[0], &cause.to_string(), "per-node cancel rows carry the same cause");
    }

    /// The cancel walk's closures get the same pass a turn's would: a
    /// node parked two groups deep is cancelled with its output port
    /// never written, and the closure the walk puts on that port travels
    /// its one wire out of both groups on the worker exactly as the fold
    /// sends it from the `NodeCancelled` row (`drive` compares the two).
    #[tokio::test]
    async fn a_cancelled_body_closes_its_groups_outward() {
        let project = open_nested_group_project();
        let project_again = project.clone();
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("deep", Box::new(SelfStopper { cause: weft_core::run::CancelCause::User })),
        ]);
        let (outcome, events) = drive(project, cat, &["source"]).await;
        assert!(matches!(outcome, RunOutcome::Cancelled { .. }), "{outcome:?}");
        let snap = fold(project_again, &events);
        assert_eq!(snap.firings["deep"][0].status(), NodeFiringStatus::Cancelled);
        for scope in ["outer", "outer.inner"] {
            assert_eq!(snapshot_scope(&snap, scope), Some(ScopeState::Open), "{scope}");
        }
        let after: Vec<_> = snap.pulses["after"].iter().collect();
        assert_eq!(after.len(), 1, "the closure reached past both groups: {after:?}");
        assert!(after[0].closed);
    }

    /// A group whose gate closed on a FAILURE never runs, and every
    /// member is told. A member fed both from outside the group and by
    /// another member gets one skip and nothing left pending: the run
    /// ends Failed with every record terminal, never Stuck on a closure
    /// nobody takes.
    #[tokio::test]
    async fn a_group_gated_off_by_a_failure_settles_a_member_fed_from_inside_too() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("boom", "Boom", json!([]), &[]),
                plain_node("lone", "Source", json!(["g"]), &[]),
                plain_node("both", "Recorder", json!(["g"]), &[("value", true), ("other", true)]),
            ],
            "edges": [
                edge("e1", "boom", "value", "g", SHOULD_FLOW_PORT),
                through("e2", "source", "value", &[("g", "a", "in")], "both", "value"),
                edge("e3", "lone", "value", "both", "other"),
            ],
            "groups": [group("g", None, &["a"], &[], json!({}), &["lone", "both"])]
        }))
        .expect("failing-group project");
        let project_again = project.clone();
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("Boom", Box::new(FailingSource)),
            ("Recorder", Box::new(Recorder { seen: Arc::new(StdMutex::new(Vec::new())), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(project, cat, &["source", "boom"]).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "nothing inside a scope that never started runs");
        let snap = fold(project_again, &events);
        assert!(
            matches!(snapshot_scope(&snap, "g"), Some(ScopeState::Skipped { reason: SkipReason::FlowClosed { failure: Some(_) } })),
            "{:?}",
            snapshot_scope(&snap, "g")
        );
        for member in ["lone", "both"] {
            let records = &snap.firings[member];
            assert_eq!(records.len(), 1, "{member} fires once: {records:?}");
            assert_eq!(records[0].status(), NodeFiringStatus::Skipped, "{member}");
        }
        let in_flight: Vec<String> = snap
            .pulses
            .iter()
            .flat_map(|(n, b)| b.iter().map(move |p| format!("{n}.{}", p.target_port)))
            .collect();
        assert!(in_flight.is_empty(), "nothing is left for a member to take: {in_flight:?}");
        assert_eq!(
            weft_core::run::check_completion(&snap.pulses, &snap.firings),
            Some(true),
            "the fold reaches the live engine's conclusion: ended, failed"
        );
    }

    /// Holds its value back until `release` lets it go, then emits it,
    /// writing down that it was released. A failsafe lets it go on its
    /// own after a while, written down as such, so a run that never
    /// releases it fails its test by name instead of hanging.
    struct HeldBack {
        name: &'static str,
        release: Arc<tokio::sync::Notify>,
        log: Arc<StdMutex<Vec<String>>>,
    }
    test_manifest!(HeldBack, "HeldBack");
    #[async_trait]
    impl Node for HeldBack {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let how = match tokio::time::timeout(std::time::Duration::from_secs(10), self.release.notified()).await {
                Ok(()) => "released",
                Err(_) => "gave up waiting",
            };
            self.log.lock().unwrap().push(format!("{} {how}", self.name));
            ctx.pulse_downstream(NodeOutput::new().set("value", json!(self.name))).await
        }
    }

    /// Writes down that it ran, and lets `release` go when it has one.
    /// Emits nothing.
    struct Mark {
        name: &'static str,
        release: Option<Arc<tokio::sync::Notify>>,
        log: Arc<StdMutex<Vec<String>>>,
    }
    test_manifest!(Mark, "Mark");
    #[async_trait]
    impl Node for Mark {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            self.log.lock().unwrap().push(format!("{} ran", self.name));
            if let Some(release) = &self.release {
                release.notify_one();
            }
            Ok(())
        }
    }

    /// Where `entry` sits in `log`, failing when it is not there.
    fn logged_at(log: &[String], entry: &str) -> usize {
        log.iter().position(|e| e == entry).unwrap_or_else(|| panic!("'{entry}' is not in the log: {log:?}"))
    }

    /// A member with no input starts the moment its group opens, without
    /// waiting for the group's other inputs. `g` opens on `fast`'s value
    /// reaching `uses`; `late`, which feeds `g`'s other input, holds its
    /// value back until `m`, a member nothing feeds, has run. Were `m`
    /// held until every input of `g` had arrived, `late` would never be
    /// released.
    #[tokio::test]
    async fn a_member_with_no_input_starts_when_its_group_opens_before_the_other_inputs_arrive() {
        let log: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("fast", "Source", json!([]), &[]),
                plain_node("late", "HeldBack", json!([]), &[]),
                plain_node("m", "M", json!(["g"]), &[]),
                plain_node("uses", "Uses", json!(["g"]), &[("early", true), ("late", true)])
            ],
            "edges": [
                through("e1", "fast", "value", &[("g", "early", "in")], "uses", "early"),
                through("e2", "late", "value", &[("g", "late", "in")], "uses", "late")
            ],
            "groups": [group("g", None, &["early", "late"], &[], json!({}), &["m", "uses"])]
        }))
        .expect("late input project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("HeldBack", Box::new(HeldBack { name: "late", release: release.clone(), log: log.clone() })),
            ("M", Box::new(Mark { name: "m", release: Some(release), log: log.clone() })),
            ("Uses", Box::new(Mark { name: "uses", release: None, log: log.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["fast", "late"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let log = log.lock().unwrap().clone();
        assert!(logged_at(&log, "m ran") < logged_at(&log, "late released"), "m ran before g's other input existed: {log:?}");
        assert!(logged_at(&log, "late released") < logged_at(&log, "uses ran"), "uses waited for both its inputs: {log:?}");
        assert_eq!(snapshot_scope(&fold(project_again, &events), "g"), Some(ScopeState::Open));
    }

    /// A member's output reaches its consumer outside the group the
    /// moment it is made, before the group's other outputs exist. `g`
    /// holds `quick`, whose value leaves on `q` for `consumer`, and
    /// `slow`, which holds its value (leaving on `s` for `after`) back
    /// until `consumer` has run. Were a group's outputs held until all
    /// of them existed, `slow` would never be released.
    #[tokio::test]
    async fn a_member_output_reaches_its_consumer_before_the_groups_other_outputs_exist() {
        let log: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let release = Arc::new(tokio::sync::Notify::new());
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("start", "Start", json!([]), &[]),
                plain_node("quick", "Source", json!(["g"]), &[]),
                plain_node("slow", "HeldBack", json!(["g"]), &[]),
                plain_node("consumer", "Consumer", json!([]), &[("value", true)]),
                plain_node("after", "After", json!([]), &[("value", true)])
            ],
            "edges": [
                through("e1", "quick", "value", &[("g", "q", "out")], "consumer", "value"),
                through("e2", "slow", "value", &[("g", "s", "out")], "after", "value")
            ],
            "groups": [group("g", None, &[], &["q", "s"], json!({}), &["quick", "slow"])]
        }))
        .expect("early output project");
        let cat = catalog(vec![
            ("Start", Box::new(Mark { name: "start", release: None, log: log.clone() })),
            ("Source", Box::new(Source { allowed: Some(json!(true)) })),
            ("HeldBack", Box::new(HeldBack { name: "slow", release: release.clone(), log: log.clone() })),
            ("Consumer", Box::new(Mark { name: "consumer", release: Some(release), log: log.clone() })),
            ("After", Box::new(Mark { name: "after", release: None, log: log.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["start"]).await;

        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let log = log.lock().unwrap().clone();
        assert!(logged_at(&log, "consumer ran") < logged_at(&log, "slow released"), "quick's value left g before slow's existed: {log:?}");
        assert!(logged_at(&log, "slow released") < logged_at(&log, "after ran"), "{log:?}");
        assert_eq!(snapshot_scope(&fold(project_again, &events), "g"), Some(ScopeState::Open));
    }
