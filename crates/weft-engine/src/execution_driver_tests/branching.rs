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
                    Some(reason.clone().expect("every live writer journals a reason"))
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
            matches!(outcome, ExecutionOutcome::Completed { .. }),
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
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
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
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
    }

    /// A journaled run subgraph (a manual run aimed at targets) holds
    /// the execution to that boundary: the out-of-scope node journals a
    /// real `NodeSkipped { OutsideThisRun }`, never a blank, and its
    /// body does not run. The subgraph is read back from the journaled
    /// `ExecutionStarted`, the same row a resume folds, so this also
    /// pins the resume boundary.
    #[tokio::test]
    async fn a_run_subgraph_skips_the_outside_with_its_own_reason() {
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
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
        assert_eq!(
            ran.lock().unwrap().as_slice(),
            &["guarded"],
            "only the in-subgraph node runs"
        );
        assert_eq!(skip_reason(&events, "behind"), Some(SkipReason::OutsideThisRun));
    }

    /// Two programs in one file sharing an upstream node, the shape a
    /// trigger fire journals its subgraph for.
    ///
    ///   my_trigger ──► mine ──► leaf          (the fired program; `leaf`
    ///   shared ───────┘                        feeds no output)
    ///   shared ──► theirs ◄── their_trigger   (the other program)
    ///
    /// `shared` feeds both programs and emits down every wire it has,
    /// so on a fire of `my_trigger` a pulse lands on `theirs` too. Three
    /// things are pinned. The other program's node absorbs it SILENTLY:
    /// no row, it is someone else's program. The fired program's own
    /// dangling node (`leaf`, reachable from the trigger but wanted by
    /// no output) journals ONE skip saying it is outside this run: that
    /// row is the hint its author forgot to mark it as an output. And
    /// without the boundary the same fire ends Stuck, `theirs` parked on
    /// a partial input set, which is the failure every two-trigger
    /// project used to hit on every fire.
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
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["guarded"], "only the fired program's node runs");
        // The other program: nothing, not even a skip row.
        assert!(!touched(&events, "theirs"), "another program's node must leave no trace: {events:?}");
        // The fired program's own dangling node: one skip that says why.
        assert_eq!(skip_reason(&events, "leaf"), Some(SkipReason::OutsideThisRun));

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
        assert!(matches!(outcome, ExecutionOutcome::Stuck), "{outcome:?}");
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
    /// parent is an unkicked entry node outside the subgraph) still
    /// skips immediately instead of parking its pulse forever waiting
    /// for an input that will never come.
    #[tokio::test]
    async fn a_partially_fed_out_of_scope_node_skips_instead_of_parking() {
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
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["a"]);
        assert_eq!(skip_reason(&events, "b"), Some(SkipReason::OutsideThisRun));
    }

    /// Pulses reaching an out-of-scope node across SEPARATE ticks (one
    /// parent emits now, the other closes later) still produce exactly
    /// one lifecycle pair: the location terminally skipped once, every
    /// later batch is absorbed silently.
    #[tokio::test]
    async fn an_out_of_scope_node_fed_across_ticks_skips_exactly_once() {
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
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
        assert_eq!(ran.lock().unwrap().as_slice(), &["a"]);
        let started = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeStarted { node_id, .. } if node_id == "b"))
            .count();
        assert_eq!(started, 1, "one NodeStarted for the out-of-scope node: {events:?}");
        // skip_reason also asserts at most one NodeSkipped.
        assert_eq!(skip_reason(&events, "b"), Some(SkipReason::OutsideThisRun));
    }

    /// A group nested two deep, guarded from the outer group's braces.
    /// source -> outer__in -> outer.inner__in -> deep -> inner__out ->
    /// outer__out -> after.
    fn nested_group_project() -> ProjectDefinition {
        let boundary = |id: &str, group: &str, role: &str, literal: bool| {
            json!({
                "id": id, "nodeType": "Passthrough", "label": null,
                "config": { "parentId": group }, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [
                    { "name": "value", "portType": "String", "required": false },
                    { "name": SHOULD_FLOW_PORT, "portType": "T__should_flow", "required": false }
                ],
                "outputs": [{ "name": "value", "portType": "String", "required": false }],
                "features": {}, "scope": [], "requiresInfra": false, "images": [],
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
                boundary("outer__in", "outer", "In", true),
                boundary("outer.inner__in", "outer.inner", "In", false),
                echo("deep", json!(["outer", "outer.inner"])),
                boundary("outer.inner__out", "outer.inner", "Out", false),
                boundary("outer__out", "outer", "Out", false),
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

    /// `_should_flow: false` in a group's braces takes the whole subgraph
    /// down, nesting included: the outer boundary skips, so its outputs
    /// close, so the inner boundary skips, so the node inside it does.
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
        assert_eq!(skip_reason(&events, "outer__in"), Some(SkipReason::DidNotFlow));
        assert_eq!(
            skip_reason(&events, "deep"),
            Some(SkipReason::RequiredInputClosed { port: "value".into() }),
            "the node two levels in is skipped by the cascade"
        );
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
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

        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "{outcome:?}");
        assert_eq!(*seen.lock().unwrap(), vec!["fixed".to_string()]);
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
