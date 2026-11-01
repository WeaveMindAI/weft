    //! Layer 3: seeded and scoped runs through the real run loop.
    //!
    //! A seeded run runs a cut of its program where each reused node that
    //! feeds what runs stands in for itself with what it sent; its birth
    //! row names the run it inherits from and the nodes it reuses
    //! (`RunStarted.seed`), the worker folds the seed's rows in
    //! (`weft_journal::seed`) and dispatches only the stale nodes. A from
    //! start runs on a cut whose program hands its input a value, without
    //! the upstream source. Both
    //! are facts about the driver: what it re-dispatches, what reaches
    //! the nodes that run, and what it writes under its own run.

    use super::*;
    use super::engine_test_rig::{catalog, drive, drive_seeded, test_manifest};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::generator::Generator;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::{RunEvent, Seed};

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
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
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
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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

    /// The birth of `run_id` running `program`, cut from `whole` when it
    /// is a cut.
    fn birth(run_id: RunId, program: &ProjectDefinition, given: &weft_core::project::Given, whole: Option<&ProjectDefinition>, seed: Option<Seed>) -> RunEvent {
        RunEvent::RunStarted { given: Box::new(given.clone()),
            run_id,
            project_id: program.id,
            entry_node: "a".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some(weft_core::project::hash::compute_definition_hash(program).unwrap()),
            cut_from: whole.map(|whole| weft_core::project::hash::compute_definition_hash(whole).unwrap()),
            binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            seed,
            instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }
    }

    /// The cut of `program` a run of `runs` runs, `given` what it is
    /// handed and what stands in for itself, started at `starts`, and what
    /// a run of it is given.
    fn cut_of(program: &ProjectDefinition, runs: &[&str], starts: &[&str], mut given: weft_core::project::Given) -> (ProjectDefinition, weft_core::project::Given) {
        let top = |ids: &[&str]| ids.iter().map(|id| weft_core::frames::Located::top(*id)).collect();
        let walk = weft_core::project::cut::Walk {
            runs: top(runs),
            stand_ins: given.outputs.keys().cloned().collect(),
            starts: top(starts),
            ..Default::default()
        };
        let cut = weft_core::project::cut::cut(program, &walk, &mut given).expect("a cut");
        (cut, given)
    }

    /// The program a seeded run runs: `program` cut to `runs`, each of
    /// `stand_ins` (a node, its port, what it sent there, the run that
    /// sent it) sending again what it sent instead of running.
    fn seeded_cut(program: &ProjectDefinition, runs: &[&str], stand_ins: &[(&str, &str, serde_json::Value, RunId)]) -> (ProjectDefinition, weft_core::project::Given) {
        let outputs = stand_ins
            .iter()
            .map(|(node, port, value, run)| {
                let stand_in = weft_core::project::StandIn {
                    ports: [(port.to_string(), weft_core::project::GivenPort::Value { value: value.clone() })].into(),
                    origin: weft_core::project::GivenOrigin::Inherited { run: *run },
                };
                (weft_core::frames::Located::top(*node), stand_in)
            })
            .collect();
        cut_of(program, runs, &[], weft_core::project::Given { inputs: Default::default(), outputs })
    }

    fn kick(run_id: RunId, node: &str, payload: Option<serde_json::Value>) -> RunEvent {
        RunEvent::NodeKicked { run_id, node_id: node.into(), frames: vec![], firing: false, payload, port_snapshot: None, at_unix: 0 }
    }

    fn seed(parent: RunId, kept: &[&str]) -> Seed {
        Seed { parent, origins: kept.iter().map(|node| (weft_core::frames::Located::top(*node), parent)).collect(), answers: Default::default() }
    }

    fn started_nodes(events: &[RunEvent]) -> Vec<String> {
        let mut out: Vec<String> = events
            .iter()
            .filter_map(|e| match e {
                RunEvent::NodeStarted { node_id, .. } => Some(node_id.clone()),
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
    /// it lands under the child's run), while `b` sees `a`'s old value
    /// and `c` sees `b`'s new one.
    #[tokio::test]
    async fn a_seeded_run_inherits_the_unchanged_upstream_and_reruns_the_stale() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, parent_rows) = drive(program.clone(), cat(&ran, &seen), &["a"]).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let parent = parent_rows[0].run_id();
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        let child = uuid::Uuid::new_v4();
        let (cut, cut_given) = seeded_cut(&program, &["b", "c"], &[("a", "value", json!("A"), parent)]);
        let rows = vec![birth(child, &cut, &cut_given, Some(&program), Some(seed(parent, &["a"])))];
        let (outcome, child_rows) = drive_seeded(cut, cat(&ran, &seen), child, parent_rows, vec![program], rows).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo", "echo"], "a's body never ran again");
        assert_eq!(*seen.lock().unwrap(), vec!["A", "A+echo"], "b read the inherited value, c read b's new one");
        assert_eq!(started_nodes(&child_rows), vec!["b", "c"], "only the stale nodes have rows under the child");
        assert!(child_rows.iter().all(|e| e.run_id() == child));
    }

    /// A grandchild seeded from the child with only `c` stale takes `a`
    /// from the grandparent and `b` from the child.
    #[tokio::test]
    async fn a_grandchild_folds_through_both_edges() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (_, parent_rows) = drive(program.clone(), cat(&ran, &seen), &["a"]).await;
        let parent = parent_rows[0].run_id();
        let child = uuid::Uuid::new_v4();
        let (child_cut, child_cut_given) = seeded_cut(&program, &["b", "c"], &[("a", "value", json!("A"), parent)]);
        let (_, child_rows) = drive_seeded(
            child_cut.clone(),
            cat(&ran, &seen),
            child,
            parent_rows.clone(),
            vec![program.clone()],
            vec![birth(child, &child_cut, &child_cut_given, Some(&program), Some(seed(parent, &["a"])))],
        )
        .await;
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        let grandchild = uuid::Uuid::new_v4();
        let mut ancestors = parent_rows;
        ancestors.extend(child_rows);
        let (cut, cut_given) = seeded_cut(&program, &["c"], &[("b", "value", json!("A+echo"), child)]);
        let (outcome, rows) = drive_seeded(
            cut.clone(),
            cat(&ran, &seen),
            grandchild,
            ancestors,
            vec![program.clone(), child_cut],
            vec![birth(grandchild, &cut, &cut_given, Some(&program), Some(Seed { parent: child,
                origins: [(weft_core::frames::Located::top("a"), parent), (weft_core::frames::Located::top("b"), child)].into_iter().collect(),
                answers: Default::default() }))],
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
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
        let parent = parent_rows[0].run_id();
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        let mut child_program = chain(["Source", "Echo", "Echo"], &[("a", "c")]);
        child_program.nodes.retain(|n| n.id != "b");
        let child = uuid::Uuid::new_v4();
        let (cut, cut_given) = seeded_cut(&child_program, &["c"], &[("a", "value", json!("A"), parent)]);
        let (outcome, rows) = drive_seeded(
            cut.clone(),
            cat(&ran, &seen),
            child,
            parent_rows,
            vec![parent_program],
            vec![birth(child, &cut, &cut_given, Some(&child_program), Some(seed(parent, &["a"])))],
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
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
        let parent = parent_rows[0].run_id();
        ran.lock().unwrap().clear();

        let child = uuid::Uuid::new_v4();
        let rows = vec![
            birth(child, &program, &Default::default(), None, Some(seed(parent, &[]))),
            kick(child, "a", Some(json!({"copied": "from the seed"}))),
        ];
        let (outcome, child_rows) = drive_seeded(program.clone(), cat(&ran, &seen), child, parent_rows, vec![program], rows).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
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
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        let parent = parent_rows[0].run_id();
        ran.lock().unwrap().clear();

        let fixed = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let child = uuid::Uuid::new_v4();
        let (cut, cut_given) = seeded_cut(&fixed, &["b", "c"], &[("a", "value", json!("A"), parent)]);
        let (outcome, rows) = drive_seeded(
            cut.clone(),
            cat(&ran, &seen),
            child,
            parent_rows,
            vec![broken],
            vec![birth(child, &cut, &cut_given, Some(&fixed), Some(seed(parent, &["a"])))],
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(ran_of(&ran), vec!["echo", "echo"], "a is inherited from the failed run");
        assert_eq!(started_nodes(&rows), vec!["b", "c"]);
    }

    /// A from start fires with the value handed to its input, then its
    /// consumer runs. The source outside the cut never runs.
    #[tokio::test]
    async fn a_cut_run_with_a_handed_input_fires_the_from_node_with_that_value() {
        let program = chain(["Source", "Echo", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let child = uuid::Uuid::new_v4();
        let given = weft_core::project::Given {
            inputs: [(weft_core::frames::Located::top("b"), [("value".to_string(), json!("by hand"))].into())].into(),
            outputs: Default::default(),
        };
        let (cut, cut_given) = cut_of(&program, &["b", "c"], &["b"], given);
        let rows = vec![birth(child, &cut, &cut_given, Some(&program), None), kick(child, "b", None)];
        let (outcome, child_rows) = drive_seeded(cut, cat(&ran, &seen), child, Vec::new(), Vec::new(), rows).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
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
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3, 4]);
        let parent = parent_rows[0].run_id();
        ran.lock().unwrap().clear();
        took.lock().unwrap().clear();

        let child = uuid::Uuid::new_v4();
        let (cut, cut_given) = seeded_cut(&program, &["consumer"], &[("producer", "out", json!([0, 1, 2, 3, 4]), parent)]);
        let (outcome, rows) = drive_seeded(
            cut.clone(),
            nodes(),
            child,
            parent_rows,
            vec![program.clone()],
            vec![birth(child, &cut, &cut_given, Some(&program), Some(seed(parent, &["producer"])))],
        )
        .await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(ran.lock().unwrap().is_empty(), "the producer never ran again");
        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3, 4], "every item was delivered again");
        assert_eq!(started_nodes(&rows), vec!["consumer"]);
    }

    /// Asks a person, writes down that it ran, and sends the answer on.
    struct Asks {
        ran: Ran,
    }
    test_manifest!(Asks, "Asks");
    #[async_trait]
    impl Node for Asks {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("asks".into());
            let answer = ctx
                .await_signal(weft_core::signal::Form {
                    form_type: "human_query".into(),
                    schema: weft_core::signal::FormSchema { fields: Vec::new() },
                    title: None,
                    description: None,
                    consumer_kind: None,
                })
                .await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", answer)).await
        }
    }

    /// The parent ran `a -> b -> c` with `b` asking a person, who
    /// answered; the child, seeded with `b` and `c` stale and told to
    /// reuse the answers `b` was given, runs `b` again and `b` takes the
    /// answer from the parent's record: nothing new is asked (the child's
    /// clients register no wait at all), `a` is inherited, and `c` reads
    /// the answer the person gave.
    #[tokio::test]
    async fn a_seeded_run_reuses_the_answer_a_person_gave() {
        use super::engine_test_rig::{drive_on, AwaitTasks, MemJournal};
        let program = chain(["Source", "Asks", "Echo"], &[("a", "b"), ("b", "c")]);
        let ran: Ran = Default::default();
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let nodes = || {
            catalog(vec![
                ("Source", Box::new(Source { id: "a", value: json!("A"), ran: ran.clone() })),
                ("Asks", Box::new(Asks { ran: ran.clone() })),
                ("Echo", Box::new(Echo { id: "echo", ran: ran.clone(), seen: seen.clone() })),
            ])
        };

        // The parent: parks on the question, is answered, completes.
        let parent = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&[birth(parent, &program, &Default::default(), None, None), kick(parent, "a", None)]);
        let tasks = AwaitTasks::new();
        let with_tasks = || EngineClients { tasks: tasks.clone(), ..super::engine_test_rig::clients(journal.clone()) };
        let parked = drive_on(Arc::new(program.clone()), nodes(), parent, journal.clone(), with_tasks(), CancellationFlag::new_arc(), None)
            .await
            .expect("the parent's first drive")
            .outcome;
        assert!(matches!(parked, RunOutcome::Stalled), "{parked:?}");
        let token = tasks.minted().expect("the question was registered");
        journal.seed(&[RunEvent::wait_answered(parent, token, weft_core::primitive::WaitAnswer::Given { value: json!("approve") }, 1)]);
        let answered = drive_on(Arc::new(program.clone()), nodes(), parent, journal.clone(), with_tasks(), CancellationFlag::new_arc(), None)
            .await
            .expect("the parent's second drive")
            .outcome;
        assert!(matches!(answered, RunOutcome::Completed), "{answered:?}");
        ran.lock().unwrap().clear();
        seen.lock().unwrap().clear();

        // The child: `a` inherited, `b` and `c` run again, `b`'s answer
        // reused. Its clients register no wait: one asked would fail `b`.
        let child = uuid::Uuid::new_v4();
        let (cut, cut_given) = seeded_cut(&program, &["b", "c"], &[("a", "value", json!("A"), parent)]);
        let reuse = Seed {
            parent,
            origins: [(weft_core::frames::Located::top("a"), parent)].into_iter().collect(),
            answers: [weft_core::frames::Located::top("b")].into_iter().collect(),
        };
        let rows = vec![birth(child, &cut, &cut_given, Some(&program), Some(reuse))];
        let (outcome, child_rows) = drive_seeded(cut, nodes(), child, journal.events_of(parent), vec![program], rows).await;
        assert!(matches!(outcome, RunOutcome::Completed), "nobody answered the child, and it completed: {outcome:?}");
        assert_eq!(ran_of(&ran), vec!["asks", "echo"], "a is inherited, b runs again, then c");
        assert_eq!(*seen.lock().unwrap(), vec!["approve"], "c reads the answer the person gave the parent");
        assert_eq!(started_nodes(&child_rows), vec!["b", "c"]);
        assert!(
            !child_rows.iter().any(|row| matches!(row, RunEvent::NodeSuspended { .. })),
            "nothing parks in the child: {child_rows:?}"
        );
    }
