    //! Layer 3: seeded and scoped runs through the real execution loop.
    //!
    //! A seeded run's birth row names the run it inherits from and the
    //! nodes it reuses (`ExecutionStarted.seed`); the worker folds
    //! the seed's rows in (`weft_journal::seed`) and dispatches only the
    //! stale nodes. A from start records backup inputs on its selection
    //! and runs without the upstream source. Both
    //! are facts about the driver: what it re-dispatches, what reaches
    //! the nodes that run, and what it writes under its own color.

    use super::*;
    use super::engine_test_rig::{catalog, drive, drive_seeded, test_manifest};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::generator::Generator;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::{ExecEvent, Seed};

    type Ran = Arc<StdMutex<Vec<String>>>;

    /// Emits a fixed value, and writes down that it ran.
    struct Source {
        id: &'static str,
        value: serde_json::Value,
        ran: Ran,
    }
    test_manifest!(Source, "Source");
    #[async_trait]
    impl Node for Source {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push(self.id.to_string());
            ctx.pulse_downstream(NodeOutput::new().set("value", self.value.clone())).await
        }
    }

    /// Passes `value` through with a suffix, and writes down what it saw.
    struct Echo {
        id: &'static str,
        ran: Ran,
        seen: Arc<StdMutex<Vec<String>>>,
    }
    test_manifest!(Echo, "Echo");
    #[async_trait]
    impl Node for Echo {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push(self.id.to_string());
            let value: String = ctx.inputs.get("value")?;
            self.seen.lock().unwrap().push(value.clone());
            ctx.pulse_downstream(NodeOutput::new().set("value", json!(format!("{value}+{}", self.id)))).await
        }
    }

    /// Fails every time.
    struct Failer;
    test_manifest!(Failer, "Failer");
    #[async_trait]
    impl Node for Failer {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Err(weft_core::error::node_error("boom"))
        }
    }

    /// Streams `count` items, and writes down that it ran.
    struct Yielder {
        count: usize,
        ran: Ran,
    }
    test_manifest!(Yielder, "Yielder");
    #[async_trait]
    impl Node for Yielder {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("producer".into());
            for i in 0..self.count {
                ctx.pulse_downstream(NodeOutput::new().set("out", json!(i))).await?;
            }
            Ok(())
        }
    }

    /// Pulls the whole stream, writing down every item it took.
    struct Taker {
        took: Arc<StdMutex<Vec<i64>>>,
    }
    test_manifest!(Taker, "Taker");
    #[async_trait]
    impl Node for Taker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            while let Some(v) = stream.next().await? {
                self.took.lock().unwrap().push(v);
            }
            Ok(())
        }
    }

    /// `a -> b -> c` on `value: String`, with the node types given.
    fn chain(types: [&str; 3], wires: &[(&str, &str)]) -> ProjectDefinition {
        let node = |id: &str, ty: &str, input: bool| {
            json!({
                "id": id, "nodeType": ty, "label": null, "config": null,
                "position": {"x": 0.0, "y": 0.0},
                "inputs": if input { json!([{"name": "value", "portType": "String", "required": true}]) } else { json!([]) },
                "outputs": [{"name": "value", "portType": "String", "required": true}],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            })
        };
        let edges: Vec<serde_json::Value> = wires
            .iter()
            .map(|(s, t)| json!({"id": format!("{s}->{t}"), "source": s, "target": t, "sourceHandle": "value", "targetHandle": "value"}))
            .collect();
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node("a", types[0], false), node("b", types[1], true), node("c", types[2], true)],
            "edges": edges,
            "groups": []
        }))
        .expect("program")
    }

    fn birth(color: Color, project: &ProjectDefinition, subgraph: Option<&[&str]>, seed: Option<Seed>) -> ExecEvent {
        let mut selection = match subgraph {
            Some(nodes) => weft_core::project::selection::RunSelection::restricted(project, nodes.iter().map(|node| weft_core::frames::Located::top(*node)).collect()).unwrap(),
            None => weft_core::project::selection::RunSelection::whole(project),
        };
        if let Some(seed) = &seed {
            selection.nodes.retain(|node| !seed.origins.contains_key(node));
            selection.suppliers.extend(seed.origins.keys().cloned());
        }
        ExecEvent::ExecutionStarted {
            color,
            project_id: project.id.to_string(),
            entry_node: "a".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
            program: None, source_version: None, node_test: false,
            subgraph: Some(selection),
            seed,
            at_unix: 0,
        }
    }

    fn kick(color: Color, node: &str, payload: Option<serde_json::Value>) -> ExecEvent {
        ExecEvent::NodeKicked { color, node_id: node.into(), frames: vec![], firing: false, payload, port_snapshot: None, at_unix: 0 }
    }

    fn seed(parent: Color, kept: &[&str]) -> Seed {
        Seed { parent, origins: kept.iter().map(|node| (weft_core::frames::Located::top(*node), parent)).collect() }
    }

    fn started_nodes(events: &[ExecEvent]) -> Vec<String> {
        let mut out: Vec<String> = events
            .iter()
            .filter_map(|e| match e {
                ExecEvent::NodeStarted { node_id, .. } => Some(node_id.clone()),
                _ => None,
            })
            .collect();
        out.sort();
        out
    }

    fn ran_of(ran: &Ran) -> Vec<String> {
        let mut v = ran.lock().unwrap().clone();
        v.sort();
        v
    }

    fn cat(ran: &Ran, seen: &Arc<StdMutex<Vec<String>>>) -> Arc<dyn weft_core::NodeCatalog> {
        catalog(vec![
            ("Source", Box::new(Source { id: "a", value: json!("A"), ran: ran.clone() })),
            ("Echo", Box::new(Echo { id: "echo", ran: ran.clone(), seen: seen.clone() })),
            ("Failer", Box::new(Failer)),
        ])
    }

    /// The parent ran `a -> b -> c`; the child, seeded with `b` and `c`
    /// stale, never dispatches `a` (its body does not run, no row about
    /// it lands under the child's color), while `b` sees `a`'s old value
    /// and `c` sees `b`'s new one.
    #[tokio::test]
    async fn a_seeded_run_inherits_the_unchanged_upstream_and_reruns_the_stale() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, parent_rows) = drive(program.clone(), cat(&ran, &seen), &["a"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        let parent = parent_rows[0].color();
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        let child = uuid::Uuid::new_v4();
        let rows = vec![birth(child, &program, None, Some(seed(parent, &["a"])))];
        let (outcome, child_rows) = drive_seeded(program.clone(), cat(&ran, &seen), child, parent_rows, vec![program], rows).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo", "echo"], "a's body never ran again");
        assert_eq!(*seen.lock().unwrap(), vec!["A", "A+echo"], "b read the inherited value, c read b's new one");
        assert_eq!(started_nodes(&child_rows), vec!["b", "c"], "only the stale nodes have rows under the child");
        assert!(child_rows.iter().all(|e| e.color() == child));
    }

    /// A grandchild seeded from the child with only `c` stale takes `a`
    /// from the grandparent and `b` from the child.
    #[tokio::test]
    async fn a_grandchild_folds_through_both_edges() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (_, parent_rows) = drive(program.clone(), cat(&ran, &seen), &["a"]).await;
        let parent = parent_rows[0].color();
        let child = uuid::Uuid::new_v4();
        let (_, child_rows) = drive_seeded(
            program.clone(),
            cat(&ran, &seen),
            child,
            parent_rows.clone(),
            vec![program.clone()],
            vec![birth(child, &program, None, Some(seed(parent, &["a"])))],
        )
        .await;
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        let grandchild = uuid::Uuid::new_v4();
        let mut ancestors = parent_rows;
        ancestors.extend(child_rows);
        let (outcome, rows) = drive_seeded(
            program.clone(),
            cat(&ran, &seen),
            grandchild,
            ancestors,
            vec![program.clone()],
            vec![birth(grandchild, &program, None, Some(Seed { parent: child,
                origins: [(weft_core::frames::Located::top("a"), parent), (weft_core::frames::Located::top("b"), child)].into_iter().collect() }))],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo"], "only c ran");
        assert_eq!(*seen.lock().unwrap(), vec!["A+echo"], "c read b's value from the child");
        assert_eq!(started_nodes(&rows), vec!["c"]);
    }

    /// The child rewired `a` straight into `c` (dropping `b`): `a`'s
    /// inherited emission fans out over the CHILD's wires, so `c`
    /// receives it without `a` running again.
    #[tokio::test]
    async fn a_rewire_delivers_the_inherited_emission_to_the_new_input() {
        let parent_program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (_, parent_rows) = drive(parent_program.clone(), cat(&ran, &seen), &["a"]).await;
        let parent = parent_rows[0].color();
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        let mut child_program = chain(["Source", "Echo", "Echo"], &[("a", "c")]);
        child_program.nodes.retain(|n| n.id != "b");
        let child = uuid::Uuid::new_v4();
        let (outcome, rows) = drive_seeded(
            child_program.clone(),
            cat(&ran, &seen),
            child,
            parent_rows,
            vec![parent_program],
            vec![birth(child, &child_program, None, Some(seed(parent, &["a"])))],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo"]);
        assert_eq!(*seen.lock().unwrap(), vec!["A"], "c read a's inherited value over the new wire");
        assert_eq!(started_nodes(&rows), vec!["c"]);
    }

    /// An empty reuse set executes every body with the recorded starting
    /// payload; every new firing belongs to the child.
    #[tokio::test]
    async fn empty_reuse_runs_everything_with_its_starting_payload() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (_, parent_rows) = drive(program.clone(), cat(&ran, &seen), &["a"]).await;
        let parent = parent_rows[0].color();
        ran.lock().unwrap().clear();

        let child = uuid::Uuid::new_v4();
        let rows = vec![
            birth(child, &program, None, Some(seed(parent, &[]))),
            kick(child, "a", Some(json!({"copied": "from the seed"}))),
        ];
        let (outcome, child_rows) = drive_seeded(program.clone(), cat(&ran, &seen), child, parent_rows, vec![program], rows).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["a", "echo", "echo"]);
        assert_eq!(started_nodes(&child_rows), vec!["a", "b", "c"]);
    }

    /// The parent failed at `b`; the child (with `b` fixed) marks `b`
    /// and `c` stale, inherits `a`, and completes.
    #[tokio::test]
    async fn a_failed_node_reruns_after_the_fix() {
        let broken = chain(["Source", "Failer", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, parent_rows) = drive(broken.clone(), cat(&ran, &seen), &["a"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let parent = parent_rows[0].color();
        ran.lock().unwrap().clear();

        let fixed = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let child = uuid::Uuid::new_v4();
        let (outcome, rows) = drive_seeded(
            fixed.clone(),
            cat(&ran, &seen),
            child,
            parent_rows,
            vec![broken],
            vec![birth(child, &fixed, None, Some(seed(parent, &["a"])))],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo", "echo"], "a is inherited from the failed run");
        assert_eq!(started_nodes(&rows), vec!["b", "c"]);
    }

    /// A from start receives its backup, then its consumer runs.
    /// The source outside the cut never runs.
    #[tokio::test]
    async fn a_scoped_run_with_a_provided_input_fires_the_from_node_with_that_value() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let child = uuid::Uuid::new_v4();
        let mut start = birth(child, &program, Some(&["b", "c"]), None);
        let ExecEvent::ExecutionStarted { subgraph: Some(selection), .. } = &mut start else { unreachable!() };
        selection.input.insert(weft_core::frames::Located::top("b"), [("value".into(), json!("by hand"))].into());
        let rows = vec![start, kick(child, "b", None)];
        let (outcome, child_rows) = drive_seeded(program, cat(&ran, &seen), child, Vec::new(), Vec::new(), rows).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo", "echo"], "a never ran");
        assert_eq!(*seen.lock().unwrap(), vec!["by hand", "by hand+echo"]);
        assert_eq!(started_nodes(&child_rows), vec!["b", "c"]);
    }

    /// The parent streamed five items into the consumer; the child
    /// keeps the producer and marks the consumer stale: the items
    /// refold pending and the consumer takes all five again without
    /// the producer running.
    #[tokio::test]
    async fn a_kept_stream_producer_with_a_stale_consumer_redelivers_every_item() {
        let program: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "producer", "nodeType": "Yielder", "label": null, "config": null,
                    "position": {"x": 0.0, "y": 0.0}, "inputs": [],
                    "outputs": [{"name": "out", "portType": "Generator[Number]", "required": false}],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "consumer", "nodeType": "Taker", "label": null, "config": null,
                    "position": {"x": 1.0, "y": 0.0},
                    "inputs": [{"name": "in", "portType": "Generator[Number]", "required": true}],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                }
            ],
            "edges": [{"id": "e", "source": "producer", "target": "consumer", "sourceHandle": "out", "targetHandle": "in"}],
            "groups": []
        }))
        .unwrap();
        let ran: Ran = Default::default();
        let took = Arc::new(StdMutex::new(Vec::new()));
        let nodes = || {
            catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, ran: ran.clone() })),
                ("Taker", Box::new(Taker { took: took.clone() })),
            ])
        };
        let (outcome, parent_rows) = drive(program.clone(), nodes(), &["producer"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3, 4]);
        let parent = parent_rows[0].color();
        ran.lock().unwrap().clear();
        took.lock().unwrap().clear();

        let child = uuid::Uuid::new_v4();
        let (outcome, rows) = drive_seeded(
            program.clone(),
            nodes(),
            child,
            parent_rows,
            vec![program.clone()],
            vec![birth(child, &program, None, Some(seed(parent, &["producer"])))],
        )
        .await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "the producer never ran again");
        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3, 4], "every item was delivered again");
        assert_eq!(started_nodes(&rows), vec!["consumer"]);
    }
