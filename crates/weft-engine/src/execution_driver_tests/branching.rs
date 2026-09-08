    //! Layer 3: branching through the real execution loop.
    //!
    //! `_should_flow` is the port that decides whether a node runs at all,
    //! and the rules that matter only exist once a whole execution runs:
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
    use weft_core::exec::skip::{SkipReason, SHOULD_FLOW_PORT};
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, NodeCatalog, ProjectDefinition};
    use weft_journal::ExecEvent;

    /// Which node bodies actually RAN. The journal records a
    /// `NodeStarted` for a skipped firing too (that row is what carries
    /// the firing's inputs to the inspector), so "did the body run" is a
    /// question only the body can answer.
    type Ran = Arc<StdMutex<Vec<&'static str>>>;

    #[tokio::test]
    async fn a_missing_file_input_records_a_named_failure_instead_of_silently_skipping() {
        let file = weft_core::storage::StoredFile {
            key: format!("t1/asset/p1/{}", "a".repeat(64)),
            mime_type: "image/png".into(),
            filename: "cat.png".into(),
            size_bytes: 3,
        };
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{
                "id": "send_photo", "nodeType": "Echo", "position": {"x": 0, "y": 0},
                "inputs": [{"name": "value", "portType": "Image", "required": true}],
                "portLiterals": {"value": file.to_value()}
            }]
        })).unwrap();
        let ran = Arc::new(StdMutex::new(Vec::new()));
        let nodes = catalog(vec![("Echo", Box::new(Echo { id: "send_photo", ran: ran.clone() }))]);
        // The rig's storage is empty, just as it is after the file expires.
        let (outcome, events) = drive(project, nodes, &["send_photo"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "do not run with an unavailable input");
        let errors: Vec<_> = events.iter().filter_map(|event| match event {
            ExecEvent::NodeFailed { node_id, error, .. } if node_id == "send_photo" => Some(error),
            _ => None,
        }).collect();
        assert_eq!(errors.len(), 1, "one visible failure: {events:?}");
        assert!(errors[0].contains("cat.png") && errors[0].contains("Input 'value'"), "{}", errors[0]);
        assert!(!events.iter().any(|event| matches!(event,
            ExecEvent::NodeSkipped { node_id, .. } if node_id == "send_photo"
        )));
    }

    /// Emits a value and a permission, both fixed by the test.
    struct Source {
        allowed: serde_json::Value,
    }
    test_manifest!(Source, "Source");
    #[async_trait]
    impl Node for Source {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(
                NodeOutput::new().set("value", json!("payload")).set("allowed", self.allowed.clone()),
            )
            .await
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
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
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
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("recorder");
            let value: String = ctx.inputs.get("value")?;
            self.seen.lock().unwrap().push(value.clone());
            ctx.pulse_downstream(NodeOutput::new().set("value", json!(value))).await
        }
    }

    use super::engine_test_rig::catalog;

    /// A group boundary never journals a row: what it did is what the
    /// fold derives, so a test reads it off the snapshot.
    fn fold(project: ProjectDefinition, events: &[ExecEvent]) -> weft_core::primitive::ExecutionSnapshot {
        fold_with_boundaries(project, events).0
    }

    /// The fold plus every boundary firing it derived, as `(node, skip
    /// reason)`: what the live bridge paints the boundary from.
    fn fold_with_boundaries(
        project: ProjectDefinition,
        events: &[ExecEvent],
    ) -> (weft_core::primitive::ExecutionSnapshot, Vec<(String, Option<SkipReason>)>) {
        let color = events.first().expect("journal has rows").color();
        let mut fold = weft_journal::Fold::new(color, Arc::new(project));
        let mut boundaries = Vec::new();
        for e in events {
            for b in fold.apply(e).boundaries {
                if let weft_core::exec::boundary::BoundaryOutcome::Fired { skip_reason, .. } = b.outcome {
                    boundaries.push((b.node_id, skip_reason));
                }
            }
        }
        let snap = fold.into_snapshot();
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        (snap, boundaries)
    }

    /// A run that reached its end folds to the same end: every record
    /// terminal, nothing left in flight, and the fold applied every
    /// row. THE check that the journal carries enough to rebuild what
    /// the live engine held, now that the rows carry no derived state.
    fn assert_fold_settles(project: ProjectDefinition, events: &[ExecEvent]) {
        let snap = fold(project, events);
        let pending: Vec<String> = snap
            .pulses
            .iter()
            .flat_map(|(n, b)| b.iter().filter(|p| p.status.in_flight()).map(move |p| format!("{n}.{}@{:?}", p.target_port, p.frames)))
            .collect();
        assert!(pending.is_empty(), "a completed run folds with pulses in flight: {pending:?}");
        let open: Vec<String> = snap
            .executions
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| !e.status.is_terminal())
            .map(|e| format!("{}@{:?}", e.node_id, e.frames))
            .collect();
        assert!(open.is_empty(), "a completed run folds with open records: {open:?}");
        assert_eq!(
            weft_core::exec::check_completion(&snap.pulses, &snap.executions),
            Some(false),
            "the fold reaches the live engine's conclusion"
        );
    }

    /// The skip reason a boundary fired with, off the fold: `None` is
    /// a boundary that fired and ran. A boundary that never fired is a
    /// failed assertion here, never a `None`.
    fn boundary_skip_reason(boundaries: &[(String, Option<SkipReason>)], node: &str) -> Option<SkipReason> {
        let mut reasons: Vec<Option<SkipReason>> =
            boundaries.iter().filter(|(n, _)| n == node).map(|(_, r)| r.clone()).collect();
        assert_eq!(reasons.len(), 1, "one boundary firing per location, '{node}' has {}", reasons.len());
        reasons.pop().flatten()
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

    fn skip_reason(events: &[ExecEvent], node: &str) -> Option<SkipReason> {
        let mut reasons: Vec<SkipReason> = events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::NodeSkipped { node_id, reason, .. } if node_id == node => {
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
    fn branch_catalog(allowed: serde_json::Value, ran: &Ran) -> Arc<dyn NodeCatalog> {
        catalog(vec![
            ("Source", Box::new(Source { allowed })),
            ("Echo", Box::new(Echo { id: "guarded", ran: ran.clone() })),
            ("Behind", Box::new(Echo { id: "behind", ran: ran.clone() })),
        ])
    }

    /// A guard that says no skips the node it guards, and the node
    /// behind it goes with it. The execution still COMPLETES: a branch
    /// not taken is not a failure.
    #[tokio::test]
    async fn a_false_guard_skips_the_node_and_everything_behind_it() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) =
            drive(guarded_project(), branch_catalog(json!(false), &ran), &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "neither the guarded node nor the one behind it may run: {:?}",
            ran.lock().unwrap()
        );
        assert_eq!(skip_reason(&events, "guarded"), Some(SkipReason::DidNotFlow));
        assert_eq!(
            skip_reason(&events, "behind"),
            Some(SkipReason::RequiredInputClosed { port: "value".into() }),
            "the node behind is a CONSEQUENCE (its input closed), not a decision"
        );
        assert!(
            matches!(outcome, ExecutionOutcome::Completed),
            "a branch not taken is not a failure: {outcome:?}"
        );
    }

    /// The same graph with a guard that says yes runs the whole chain,
    /// so the skip above is the guard's doing and nothing else's.
    #[tokio::test]
    async fn a_true_guard_lets_the_chain_run() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) =
            drive(guarded_project(), branch_catalog(json!(true), &ran), &["source"]).await;

        assert_eq!(*ran.lock().unwrap(), vec!["guarded", "behind"]);
        assert!(skip_reason(&events, "guarded").is_none());
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
    }

    /// A guard that never spoke is its own reason: nothing decided, so
    /// the node does not run, and the inspector can say which of the two
    /// happened.
    #[tokio::test]
    async fn a_closed_guard_skips_with_its_own_reason() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        // A null emission on `allowed` closes that port: the source says
        // "nothing is coming on this one".
        let (_, events) =
            drive(guarded_project(), branch_catalog(json!(null), &ran), &["source"]).await;

        assert!(ran.lock().unwrap().is_empty());
        assert_eq!(skip_reason(&events, "guarded"), Some(SkipReason::FlowClosed));
    }

    /// An ENTRY node (no incoming edges, dispatched through the kick
    /// path) still answers to `_should_flow: false` written in its
    /// braces: the run skips it instead of running it anyway.
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
            }],
            "edges": [],
            "groups": []
        }))
        .expect("solo project");
        let cat = catalog(vec![("Solo", Box::new(Echo { id: "solo", ran: ran.clone() }))]);
        let (outcome, events) = drive(project, cat, &["solo"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "the turned-off entry node must not run: {:?}",
            ran.lock().unwrap()
        );
        assert_eq!(skip_reason(&events, "solo"), Some(SkipReason::DidNotFlow));
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
    }

    /// A journaled run subgraph (a trigger fire's program) holds the
    /// execution to that boundary: a pulse into an out-of-scope node is
    /// absorbed silently, no row, its body does not run, and the run
    /// completes. The subgraph is read back from the journaled
    /// `ExecutionStarted`, the same row a resume folds, so this also
    /// pins the resume boundary.
    #[tokio::test]
    async fn a_run_subgraph_absorbs_the_outside_silently() {
        use super::engine_test_rig::drive_scoped;
        use weft_core::cancellation::CancellationFlag;
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_scoped(
            guarded_project(),
            branch_catalog(json!(true), &ran),
            &["source"],
            Some(&["source", "guarded"]),
            CancellationFlag::new_arc(),
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(
            ran.lock().unwrap().as_slice(),
            &["guarded"],
            "only the in-subgraph node runs"
        );
        assert!(!touched(&events, "behind"), "an out-of-scope node leaves no trace: {events:?}");
    }

    /// Two programs in one file sharing an upstream node, the shape a
    /// trigger fire journals its subgraph for.
    ///
    ///   my_trigger ──► mine ──► leaf          (the fired program; `leaf`
    ///   shared ───────┘                        feeds no output)
    ///   shared ──► theirs ◄── their_trigger   (the other program)
    ///
    /// `shared` feeds both programs and emits down every wire it has,
    /// so on a fire of `my_trigger` a pulse lands on `theirs` too. Two
    /// things are pinned. Anything outside the journaled program absorbs
    /// SILENTLY: no row, it is not this run's business (here the driver
    /// is handed a program that leaves `leaf` out; the dispatcher's own
    /// walk, the downstream of the fired trigger plus what it needs, is
    /// pinned in the dispatcher). And without the boundary the same fire
    /// ends Stuck, `theirs` parked on a partial input set, which is the
    /// failure every two-trigger project used to hit on every fire.
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

    fn touched(events: &[ExecEvent], node: &str) -> bool {
        events.iter().any(|e| match e {
            ExecEvent::NodeStarted { node_id, .. }
            | ExecEvent::NodeSkipped { node_id, .. }
            | ExecEvent::NodeCompleted { node_id, .. } => node_id == node,
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
            branch_catalog(json!(true), &ran),
            "my_trigger",
            &["my_trigger", "shared"],
            Some(&["my_trigger", "shared", "mine"]),
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["guarded"], "only the fired program's node runs");
        // The other program: nothing, not even a skip row.
        assert!(!touched(&events, "theirs"), "another program's node must leave no trace: {events:?}");
        // Outside the program the driver was handed: nothing either.
        assert!(!touched(&events, "leaf"), "{events:?}");

        // The same fire with no boundary: the shared pulse parks on
        // `theirs`, whose other input never comes, and the run is Stuck.
        // Pinned so "trigger fires run the whole graph" cannot come back
        // looking harmless.
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, _) = drive_scoped(
            two_programs_project(),
            branch_catalog(json!(true), &ran),
            &["my_trigger", "shared"],
            None,
            CancellationFlag::new_arc(),
        )
        .await;
        // The terminal names the firing that can never complete and the
        // wired port it never got, so the reader sees the leak without
        // opening the replay.
        let ExecutionOutcome::Stuck { report } = outcome else { panic!("{outcome:?}") };
        let text = report.to_string();
        assert!(text.contains("theirs has value, still waiting on go"), "{text}");
    }

    /// A color whose journal already holds a terminal when the worker
    /// boots (cancelled in the dispatcher's route window, or a late
    /// second execute task for a color that already ran) is not driven:
    /// no body runs, and nothing is journaled on top of the terminal.
    #[tokio::test]
    async fn a_color_with_a_terminal_is_not_driven_again() {
        use super::engine_test_rig::drive_settled;
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) =
            drive_settled(guarded_project(), branch_catalog(json!(true), &ran), &["source"]).await;
        assert!(matches!(outcome, ExecutionOutcome::AlreadySettled), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "no body may run: {:?}", ran.lock().unwrap());
        let terminals = events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    ExecEvent::ExecutionCompleted { .. }
                        | ExecEvent::ExecutionFailed { .. }
                        | ExecEvent::ExecutionCancelled { .. }
                )
            })
            .count();
        assert_eq!(terminals, 1, "the pre-existing terminal stays the only one: {events:?}");
        assert!(!touched(&events, "source") && !touched(&events, "guarded"));
    }

    /// An out-of-scope node fed by only SOME of its parents (its other
    /// parent is an unkicked entry node outside the subgraph) is
    /// absorbed immediately instead of parking its pulse forever waiting
    /// for an input that will never come.
    #[tokio::test]
    async fn a_partially_fed_out_of_scope_node_absorbs_instead_of_parking() {
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
            ("Source", Box::new(Source { allowed: json!(true) })),
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
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["a"]);
        assert!(!touched(&events, "b"), "{events:?}");
    }

    /// Pulses reaching an out-of-scope node across SEPARATE ticks (one
    /// parent emits now, the other closes later) leave no trace either
    /// time: every batch is absorbed silently.
    #[tokio::test]
    async fn an_out_of_scope_node_fed_across_ticks_never_appears() {
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
            ("Source", Box::new(Source { allowed: json!(true) })),
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
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["a"]);
        assert!(!touched(&events, "b"), "no row for the out-of-scope node: {events:?}");
    }

    /// A group nested two deep, guarded from the outer group's braces.
    /// source -> outer__in -> outer.inner__in -> deep -> inner__out ->
    /// outer__out -> after.
    fn nested_group_project() -> ProjectDefinition {
        // A boundary's `scope` is the chain OUTSIDE its container, the
        // way the compiler flattens it: a nested group's boundaries are
        // members of the enclosing body.
        let boundary = |id: &str, group: &str, role: &str, literal: bool, scope: serde_json::Value| {
            json!({
                "id": id, "nodeType": "Passthrough", "label": null,
                "config": { "parentId": group }, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [
                    { "name": "value", "portType": "String", "required": false },
                    { "name": SHOULD_FLOW_PORT, "portType": "T__should_flow", "required": false }
                ],
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": scope, "requiresInfra": false, "images": [],
                "groupBoundary": { "groupId": group, "role": role },
                "portLiterals": if literal { json!({ SHOULD_FLOW_PORT: false }) } else { json!({}) }
            })
        };
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
        let edge = |id: &str, source: &str, target: &str| {
            json!({ "id": id, "source": source, "target": target,
                    "sourceHandle": "value", "targetHandle": "value" })
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
                boundary("outer__in", "outer", "In", true, json!([])),
                boundary("outer.inner__in", "outer.inner", "In", false, json!(["outer"])),
                echo("deep", json!(["outer", "outer.inner"])),
                boundary("outer.inner__out", "outer.inner", "Out", false, json!(["outer"])),
                boundary("outer__out", "outer", "Out", false, json!([])),
                echo("after", json!([]))
            ],
            "edges": [
                edge("e1", "source", "outer__in"),
                edge("e2", "outer__in", "outer.inner__in"),
                edge("e3", "outer.inner__in", "deep"),
                edge("e4", "deep", "outer.inner__out"),
                edge("e5", "outer.inner__out", "outer__out"),
                edge("e6", "outer__out", "after")
            ],
            "groups": [
                { "id": "outer", "kind": "group", "parentId": null },
                { "id": "outer.inner", "kind": "group", "parentId": "outer" }
            ]
        }))
        .expect("nested project")
    }

    /// `_should_flow: false` in a group's braces takes the whole scope
    /// down, nesting included: the outer boundary skips with its reason
    /// and its outward ports close (so what comes after skips), and
    /// every member, the nested group's boundaries and the node two
    /// levels in alike, logs that its scope did not run.
    #[tokio::test]
    async fn a_guarded_group_skips_every_node_nested_inside_it() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(Echo { id: "deep", ran: ran.clone() })),
            ("after", Box::new(Echo { id: "after", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(nested_group_project(), cat, &["source"]).await;

        assert!(
            ran.lock().unwrap().is_empty(),
            "nothing inside the group, and nothing after it, may run: {:?}",
            ran.lock().unwrap()
        );
        let (_, boundaries) = fold_with_boundaries(nested_group_project(), &events);
        assert_eq!(boundary_skip_reason(&boundaries, "outer__in"), Some(SkipReason::DidNotFlow));
        let scope_skipped = Some(SkipReason::ScopeSkipped { scope: "outer".into() });
        assert_eq!(skip_reason(&events, "deep"), scope_skipped, "the node two levels in");
        assert_eq!(boundary_skip_reason(&boundaries, "outer.inner__in"), scope_skipped, "the nested boundary");
        assert_eq!(boundary_skip_reason(&boundaries, "outer.inner__out"), scope_skipped);
        assert_eq!(
            skip_reason(&events, "after"),
            Some(SkipReason::RequiredInputClosed { port: "value".into() }),
            "outside the scope, the closure cascade carries on"
        );
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_fold_settles(nested_group_project(), &events);
    }

    /// The same nesting with the gate open: the value crosses two
    /// boundaries in and two out, and the journal (which holds no row
    /// for any of the four) folds back to the settled run the engine
    /// held, boundaries included.
    #[tokio::test]
    async fn a_nested_group_run_folds_back_to_its_settled_state() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let mut project = nested_group_project();
        let outer_in = project.nodes.iter_mut().find(|n| n.id == "outer__in").expect("outer__in");
        outer_in.port_literals.remove(SHOULD_FLOW_PORT);
        let project_again = project.clone();
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(Echo { id: "deep", ran: ran.clone() })),
            ("after", Box::new(Echo { id: "after", ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(project, cat, &["source"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["deep", "after"]);
        assert!(
            !events.iter().any(|e| matches!(e, ExecEvent::NodeStarted { node_id, .. } if node_id.ends_with("__in") || node_id.ends_with("__out"))),
            "a boundary never journals a row: {events:?}"
        );
        let snap = fold(project_again.clone(), &events);
        for boundary in ["outer__in", "outer.inner__in", "outer.inner__out", "outer__out"] {
            assert_eq!(snap.executions[boundary][0].status, NodeExecutionStatus::Completed, "{boundary}");
        }
        assert_fold_settles(project_again, &events);
    }

    /// A literal written on a container's interface port (`g.b = "fixed"`)
    /// reaches the children through the In boundary, the same way a
    /// node's own braces literal reaches the node.
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
                    "id": "g__in", "nodeType": "Passthrough", "label": null,
                    "config": { "parentId": "g" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "a", "portType": "String", "required": false },
                        { "name": "b", "portType": "String", "required": false }
                    ],
                    "outputs": [
                        { "name": "a", "portType": "String", "required": false },
                        { "name": "b", "portType": "String", "required": false }
                    ],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "g", "role": "In" },
                    "portLiterals": { "b": "fixed" }
                },
                {
                    "id": "deep", "nodeType": "deep", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "value", "portType": "String", "required": true }],
                    "outputs": [{ "name": "value", "portType": "String", "required": false }],
                    "features": {}, "scope": ["g"], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "source", "target": "g__in",
                  "sourceHandle": "value", "targetHandle": "a" },
                { "id": "e2", "source": "g__in", "target": "deep",
                  "sourceHandle": "b", "targetHandle": "value" }
            ],
            "groups": [{ "id": "g", "kind": "group", "parentId": null }]
        }))
        .expect("literal-port project");

        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, _) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["fixed".to_string()]);
    }

    /// Emits a fixed list, for a loop to walk.
    struct Lister;
    test_manifest!(Lister, "Lister");
    #[async_trait]
    impl Node for Lister {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
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

    /// `source -> g__in.a` with `g`'s `_should_flow` wired from a gate,
    /// and inside `g` a ROOT (`seed`, no inputs at all) feeding `deep`.
    /// A body root has no wire to start it: the scope launcher kicks it
    /// when the group starts.
    fn rooted_group_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                {
                    "id": "g__in", "nodeType": "Passthrough", "label": null,
                    "config": { "parentId": "g" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "a", "portType": "String", "required": false },
                        { "name": SHOULD_FLOW_PORT, "portType": "T__should_flow", "required": false }
                    ],
                    "outputs": [{ "name": "a", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "g", "role": "In" }
                },
                plain_node("seed", "Seed", json!(["g"]), &[]),
                plain_node("deep", "deep", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "g__in", "a"),
                edge("e2", "source", "allowed", "g__in", SHOULD_FLOW_PORT),
                edge("e3", "seed", "value", "deep", "value")
            ],
            "groups": [{ "id": "g", "kind": "group", "parentId": null }]
        }))
        .expect("rooted group project")
    }

    /// A node inside a group that nothing feeds starts when the group
    /// does: the launcher kicks it at the group's frames, and what it
    /// emits reaches the member behind it.
    #[tokio::test]
    async fn a_group_body_root_starts_when_the_group_starts() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(true) })),
            ("Seed", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(rooted_group_project(), cat, &["source"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string()], "the root's value reached deep");
        let snap = fold(rooted_group_project(), &events);
        let kick = snap.kicked.get(&FiringLocation::new("seed", vec![])).expect("the launch kicked the root");
        assert!(kick.dispatched && kick.scope_skipped.is_none(), "{kick:?}");
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Completed);
    }

    /// The same root when the group's `_should_flow` says no: nothing
    /// inside starts, and the root skips with the scope's reason like
    /// every other member, so the inspector never shows a hole.
    #[tokio::test]
    async fn a_group_body_root_is_skipped_with_its_gated_scope() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(false) })),
            ("Seed", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let (outcome, events) = drive(rooted_group_project(), cat, &["source"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "{:?}", ran.lock().unwrap());
        // The boundary itself journals nothing; the fold holds its
        // skipped record and the skip kicks of every member.
        let snap = fold(rooted_group_project(), &events);
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Skipped);
        assert_eq!(
            snap.kicked.get(&FiringLocation::new("seed", vec![])).map(|k| k.scope_skipped.as_deref()),
            Some(Some("g")),
            "a gated scope kicks its members into a skip"
        );
        let scope_skipped = Some(SkipReason::ScopeSkipped { scope: "g".into() });
        assert_eq!(skip_reason(&events, "seed"), scope_skipped, "the root, which no wire feeds");
        assert_eq!(skip_reason(&events, "deep"), scope_skipped);
    }

    /// `_should_flow` on a group from outside, when whatever decides it
    /// never spoke: the closure is a no for the boundary (its own
    /// reason), and the members carry the scope's.
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
                {
                    "id": "g__in", "nodeType": "Passthrough", "label": null,
                    "config": { "parentId": "g" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "a", "portType": "String", "required": false },
                        { "name": SHOULD_FLOW_PORT, "portType": "T__should_flow", "required": false }
                    ],
                    "outputs": [{ "name": "a", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "g", "role": "In" }
                },
                plain_node("deep", "deep", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "gate", "value"),
                edge("e2", "source", "allowed", "gate", SHOULD_FLOW_PORT),
                edge("e3", "source", "value", "g__in", "a"),
                edge("e4", "gate", "value", "g__in", SHOULD_FLOW_PORT),
                edge("e5", "g__in", "a", "deep", "value")
            ],
            "groups": [{ "id": "g", "kind": "group", "parentId": null }]
        }))
        .expect("closed guard project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(false) })),
            ("gate", Box::new(Echo { id: "gate", ran: ran.clone() })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "{:?}", ran.lock().unwrap());
        assert_eq!(skip_reason(&events, "gate"), Some(SkipReason::DidNotFlow));
        let (_, boundaries) = fold_with_boundaries(project_again, &events);
        assert_eq!(boundary_skip_reason(&boundaries, "g__in"), Some(SkipReason::FlowClosed));
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
                {
                    "id": "g__in", "nodeType": "Passthrough", "label": null,
                    "config": { "parentId": "g" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "a", "portType": "String", "required": false },
                        { "name": "b", "portType": "String", "required": false },
                        { "name": SHOULD_FLOW_PORT, "portType": "T__should_flow", "required": false }
                    ],
                    "outputs": [
                        { "name": "a", "portType": "String", "required": false },
                        { "name": "b", "portType": "String", "required": false }
                    ],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "g", "role": "In" }
                },
                plain_node("reads_a", "reads_a", json!(["g"]), &[("value", true)]),
                plain_node("reads_b", "reads_b", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "gate", "value"),
                edge("e2", "source", "allowed", "gate", SHOULD_FLOW_PORT),
                edge("e3", "source", "value", "g__in", "a"),
                edge("e4", "gate", "value", "g__in", "b"),
                edge("e5", "g__in", "a", "reads_a", "value"),
                edge("e6", "g__in", "b", "reads_b", "value")
            ],
            "groups": [{ "id": "g", "kind": "group", "parentId": null }]
        }))
        .expect("closed input project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(false) })),
            ("gate", Box::new(Echo { id: "gate", ran: ran.clone() })),
            ("reads_a", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
            ("reads_b", Box::new(Echo { id: "reads_b", ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string()], "reads_a ran on the open port");
        // Boundaries never journal: the group's own firing is read off
        // the fold, and it ran (a closed input gates a member, never
        // the group).
        let (snap, boundaries) = fold_with_boundaries(project_again, &events);
        assert_eq!(boundary_skip_reason(&boundaries, "g__in"), None, "the group itself runs");
        assert_eq!(snap.executions["g__in"][0].status, NodeExecutionStatus::Completed);
        assert_eq!(
            skip_reason(&events, "reads_b"),
            Some(SkipReason::RequiredInputClosed { port: "value".into() }),
            "the closure lands on the member that needs it"
        );
    }

    /// A group whose ONLY input arrives closed still starts: the In
    /// boundary has no gate but `_should_flow`, so it forwards the
    /// closure, its body root is kicked and runs, the member reading the
    /// closed edge skips on its own rule, and the run completes instead
    /// of parking on a body nobody started (the collapsed-group
    /// regression: a boundary that rendered green with empty panels).
    #[tokio::test]
    async fn a_group_whose_only_input_closes_still_starts_its_body_roots() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let seen: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                plain_node("source", "Source", json!([]), &[]),
                plain_node("gate", "gate", json!([]), &[("value", true), (SHOULD_FLOW_PORT, false)]),
                {
                    "id": "g__in", "nodeType": "Passthrough", "label": null,
                    "config": { "parentId": "g" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "a", "portType": "String", "required": false }],
                    "outputs": [{ "name": "a", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "g", "role": "In" }
                },
                plain_node("seed", "Seed", json!(["g"]), &[]),
                plain_node("deep", "deep", json!(["g"]), &[("value", true)]),
                plain_node("reads_a", "reads_a", json!(["g"]), &[("value", true)])
            ],
            "edges": [
                edge("e1", "source", "value", "gate", "value"),
                edge("e2", "source", "allowed", "gate", SHOULD_FLOW_PORT),
                edge("e3", "gate", "value", "g__in", "a"),
                edge("e4", "seed", "value", "deep", "value"),
                edge("e5", "g__in", "a", "reads_a", "value")
            ],
            "groups": [{ "id": "g", "kind": "group", "parentId": null }]
        }))
        .expect("closed-only-input project");
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(false) })),
            ("gate", Box::new(Echo { id: "gate", ran: ran.clone() })),
            ("Seed", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
            ("reads_a", Box::new(Echo { id: "reads_a", ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["source"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        let snap = fold(project_again, &events);
        assert_eq!(
            snap.executions["g__in"][0].status,
            NodeExecutionStatus::Completed,
            "the boundary is not gated by its inputs"
        );
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string()], "the root ran and fed deep");
        assert_eq!(
            skip_reason(&events, "reads_a"),
            Some(SkipReason::RequiredInputClosed { port: "value".into() }),
            "the member on the closed edge skips on its own rule"
        );
        assert_eq!(skip_reason(&events, "seed"), None);
        assert!(
            snap.kicked.get(&FiringLocation::new("seed", vec![])).is_some_and(|k| k.scope_skipped.is_none()),
            "the scope launched for real"
        );
    }

    /// A node inside a LOOP body that nothing feeds fires once per
    /// iteration, at the iteration's frames: the launcher kicks it
    /// alongside the body pulses, so every iteration sees its own copy.
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
                        { "name": "results", "portType": "String", "required": false },
                        { "name": "done", "portType": "Boolean", "required": false }
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
            "groups": [{ "id": "l", "kind": "loop", "parentId": null,
                         "loopConfig": { "parallel": false, "over": ["items"], "carry": [] } }]
        }))
        .expect("loop root project");
        let cat = catalog(vec![
            ("Lister", Box::new(Lister)),
            ("Seed", Box::new(Source { allowed: json!(true) })),
            ("tally", Box::new(Recorder { seen: seen.clone(), ran: ran.clone() })),
        ]);
        let project_again = project.clone();
        let (outcome, events) = drive(project, cat, &["lister"]).await;

        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["payload".to_string(), "payload".to_string()]);
        assert_fold_settles(project_again, &events);
        let mut seed_frames: Vec<Vec<u32>> = events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::NodeStarted { node_id, frames, .. } if node_id == "seed" => {
                    Some(frames.iter().map(|f| f.index).collect())
                }
                _ => None,
            })
            .collect();
        seed_frames.sort();
        assert_eq!(seed_frames, vec![vec![0], vec![1]], "one firing per iteration, at its frames");
    }

    /// Ends its own run the way a `cancel_execution` task does: flips the
    /// execution's flag WITH a cause, then parks instead of returning.
    /// Returning in the same instant the flag flips would race the
    /// driver's idle `select!` (the terminal arm can win and journal
    /// NodeFailed, leaving the cancel walk nothing to catch up on);
    /// parked, only the cancellation arm is ever ready, so this record
    /// is provably non-terminal when the walk runs. Every production
    /// canceller flips the flag from a separate task, so the ordering
    /// here only needs to hold for the self-stop shape this node exists
    /// to exercise.
    struct SelfStopper {
        cause: weft_core::exec::CancelCause,
    }
    test_manifest!(SelfStopper, "SelfStopper");
    #[async_trait]
    impl Node for SelfStopper {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let flag = ctx.cancellation();
            flag.cancel_because(self.cause.clone());
            // Park until the teardown aborts this body.
            let () = std::future::pending().await;
            unreachable!("the run's teardown aborts this body first")
        }
    }

    /// The cause a canceller records on the flag is what the worker's
    /// terminal names: `ExecutionCancelled` carries it structured AND as
    /// text, and the catch-up `NodeCancelled` rows use the same text, so
    /// a run stopped by a sibling reads as exactly that whichever side
    /// wrote its terminal first.
    #[tokio::test]
    async fn worker_terminal_names_the_recorded_cancel_cause() {
        let by = weft_core::Color::new_v4();
        let cause = weft_core::exec::CancelCause::Execution { by, tag: "user_7".into() };
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

        assert!(matches!(outcome, ExecutionOutcome::Cancelled { .. }), "{outcome:?}");
        let terminal = events
            .iter()
            .find_map(|e| match e {
                ExecEvent::ExecutionCancelled { reason, cause, .. } => Some((reason.clone(), cause.clone())),
                _ => None,
            })
            .expect("a cancelled run journals ExecutionCancelled");
        assert_eq!(terminal, (cause.to_string(), Some(cause.clone())), "{events:?}");
        assert_eq!(
            terminal.0,
            format!("Stopped by execution {by} (tag user_7)"),
            "the text a person reads names the run and the tag"
        );
        // The parked body is what the cancel walk exists to catch: its
        // record must be non-terminal when the walk runs, so exactly one
        // NodeCancelled lands, naming the same cause. A walk that wrote
        // nothing would otherwise pass this loop vacuously.
        let node_cancels: Vec<&String> = events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::NodeCancelled { node_id, reason, .. } if node_id == "stopper" => Some(reason),
                _ => None,
            })
            .collect();
        assert_eq!(node_cancels.len(), 1, "the parked node gets one cancel row: {events:?}");
        assert_eq!(node_cancels[0], &cause.to_string(), "per-node cancel rows carry the same cause");
    }

    /// The cancel walk's closures get the same pass a turn's would: a
    /// node parked two groups deep is cancelled with its output port
    /// never written, so the closure the walk puts on that port makes
    /// both Out boundaries ready, and they fire (records, closures
    /// forwarded outward) on the worker exactly as the fold fires them
    /// from the `NodeCancelled` row (`drive` compares the two).
    #[tokio::test]
    async fn a_cancelled_body_closes_its_groups_outward() {
        let mut project = nested_group_project();
        let outer_in = project.nodes.iter_mut().find(|n| n.id == "outer__in").expect("outer__in");
        outer_in.port_literals.remove(SHOULD_FLOW_PORT);
        let project_again = project.clone();
        let cat = catalog(vec![
            ("Source", Box::new(Source { allowed: json!(true) })),
            ("deep", Box::new(SelfStopper { cause: weft_core::exec::CancelCause::User })),
        ]);
        let (outcome, events) = drive(project, cat, &["source"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Cancelled { .. }), "{outcome:?}");
        let snap = fold(project_again, &events);
        assert_eq!(snap.executions["deep"][0].status, NodeExecutionStatus::Cancelled);
        for boundary in ["outer.inner__out", "outer__out"] {
            assert_eq!(
                snap.executions.get(boundary).map(|r| r[0].status.clone()),
                Some(NodeExecutionStatus::Completed),
                "{boundary} fired on the cancelled body's closure"
            );
        }
        let after: Vec<_> = snap.pulses["after"].iter().collect();
        assert_eq!(after.len(), 1, "the closure reached past both groups: {after:?}");
        assert!(after[0].closed);
    }
