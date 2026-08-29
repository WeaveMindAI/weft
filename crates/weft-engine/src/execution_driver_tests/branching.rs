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
