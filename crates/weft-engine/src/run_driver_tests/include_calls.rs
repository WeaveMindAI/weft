    //! Layer 3: a file included at several sites, through the real loop.
    //!
    //! An include compiles to one body (`kind: body`) and one call group
    //! per site (`kind: call`) naming it. Every firing of a node of the
    //! body carries the site it was called from as a call frame, so each
    //! call is its own copy: its own values, its own loops, its own
    //! connection picked for it, its own waits. These tests drive
    //! hand-built programs of that shape and read the frames off the rows.

    use super::*;
    use super::engine_test_rig::{catalog, clients, drive_journal_observed, drive_on, test_manifest, AwaitTasks, MemJournal};
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use weft_core::error::WeftResult;
    use weft_core::frames::Frame;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ProjectDefinition, RunContext};
    use weft_journal::RunEvent;

    /// A node of the program: id, type, inputs `(name, type)` (each
    /// required), outputs `(name, type)`, the groups around it.
    fn node(id: &str, node_type: &str, inputs: &[(&str, &str)], outputs: &[(&str, &str)], scope: &[&str]) -> Value {
        json!({
            "id": id, "nodeType": node_type, "position": { "x": 0, "y": 0 }, "scope": scope,
            "inputs": inputs.iter().map(|(name, ty)| json!({ "name": name, "portType": ty, "required": true })).collect::<Vec<_>>(),
            "outputs": outputs.iter().map(|(name, ty)| json!({ "name": name, "portType": ty, "required": false })).collect::<Vec<_>>(),
        })
    }

    /// The two boundary nodes of loop `group` over `over` (a list of
    /// `item` sent in), gathering `gather` (each an `item`), inside
    /// `scope`.
    fn loop_boundaries(group: &str, over: &str, item: &str, gather: &str, scope: &[&str]) -> [Value; 2] {
        let mut loop_in = node(&format!("{group}__in"), "LoopIn", &[(over, &format!("List[{item}]"))], &[(over, item), ("index", "Number")], scope);
        loop_in["config"] = json!({ "parentId": group, "parallel": false, "over": [over], "carry": [] });
        loop_in["groupBoundary"] = json!({ "groupId": group, "role": "In" });
        let mut loop_out = node(&format!("{group}__out"), "LoopOut", &[], &[(gather, &format!("List[{item} | Null]"))], scope);
        loop_out["inputs"] = json!([
            { "name": gather, "portType": item, "required": false, "optional": true },
            { "name": "done", "portType": "Boolean", "required": false, "optional": true }
        ]);
        loop_out["config"] = json!({ "parentId": group });
        loop_out["groupBoundary"] = json!({ "groupId": group, "role": "Out" });
        [loop_in, loop_out]
    }

    /// A wire, crossing the group ports `via`, each `(group, port, side)`.
    fn wire(source: &str, source_port: &str, via: &[(&str, &str, &str)], target: &str, target_port: &str) -> Value {
        json!({
            "id": format!("{source}.{source_port}->{target}.{target_port}"),
            "source": source, "sourceHandle": source_port, "target": target, "targetHandle": target_port,
            "via": via.iter().map(|(group, port, side)| json!({ "group": group, "port": port, "side": side })).collect::<Vec<_>>(),
        })
    }

    /// A group: its id, its kind and what goes with it, the group around
    /// it, its `x` in and `y` out ports (of type `ty`), its members.
    fn group(id: &str, mut kind: Value, parent: Option<&str>, ty: &str, members: &[&str]) -> Value {
        kind["id"] = json!(id);
        kind["parentGroupId"] = json!(parent);
        kind["inPorts"] = json!([{ "name": "x", "portType": ty, "required": false }]);
        kind["outPorts"] = json!([{ "name": "y", "portType": ty, "required": false }]);
        kind["nodeIds"] = json!(members);
        kind
    }

    fn program(nodes: Vec<Value>, edges: Vec<Value>, groups: Vec<Value>) -> ProjectDefinition {
        serde_json::from_value(json!({ "id": uuid::Uuid::new_v4(), "nodes": nodes, "edges": edges, "groups": groups })).expect("the program")
    }

    /// The birth of `run_id` running all of `project` (holding `picks`),
    /// kicked where a run of the whole program is.
    fn birth(project: &ProjectDefinition, run_id: RunId, picks: weft_core::picks::Picks) -> Vec<RunEvent> {
        let mut rows = vec![RunEvent::RunStarted { given: Default::default(),
            run_id, project_id: project.id, entry_node: "start".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
            cut_from: None, binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            seed: None, instance: None, fired_trigger: None, stand_in: None, instance_values: Default::default(), picks: Box::new(picks), at_unix: 0,
            settings: Default::default(),
        }];
        rows.extend(weft_core::run_spec::KickPlan::for_program(project, None, None).into_iter().map(|kick| RunEvent::NodeKicked {
            run_id, node_id: kick.node, frames: kick.frames, firing: false, payload: None, port_snapshot: None, at_unix: 0,
        }));
        rows
    }

    /// The frames of every firing of `node` that completed, in order.
    fn completed_at(events: &[RunEvent], node: &str) -> Vec<Vec<Frame>> {
        events
            .iter()
            .filter_map(|event| match event {
                RunEvent::NodeCompleted { node_id, frames, .. } if node_id == node => Some(frames.clone()),
                _ => None,
            })
            .collect()
    }

    fn site(name: &str) -> Frame {
        Frame::Call { site: name.into() }
    }

    fn iteration(index: u32) -> Frame {
        Frame::Loop { index }
    }

    /// Sends a fixed value on each of its outputs.
    struct Sends(Vec<(&'static str, Value)>);
    test_manifest!(Sends, "Sends");
    #[async_trait]
    impl Node for Sends {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let out = self.0.iter().fold(NodeOutput::new(), |out, (port, value)| out.set(*port, value.clone()));
            ctx.pulse_downstream(out).await
        }
    }

    /// `text` as the list of its characters.
    struct Split;
    test_manifest!(Split, "Split");
    #[async_trait]
    impl Node for Split {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let text: String = ctx.inputs.get("text")?;
            let chars: Vec<String> = text.chars().map(String::from).collect();
            ctx.pulse_downstream(NodeOutput::new().set("chars", json!(chars))).await
        }
    }

    /// One character, upper case.
    struct Upper;
    test_manifest!(Upper, "Upper");
    #[async_trait]
    impl Node for Upper {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let c: String = ctx.inputs.get("chars")?;
            ctx.pulse_downstream(NodeOutput::new().set("up", json!(c.to_uppercase()))).await
        }
    }

    /// The parts, joined back into one text.
    struct Join;
    test_manifest!(Join, "Join");
    #[async_trait]
    impl Node for Join {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let parts: Vec<Option<String>> = ctx.inputs.get("up")?;
            ctx.pulse_downstream(NodeOutput::new().set("text", json!(parts.into_iter().flatten().collect::<String>()))).await
        }
    }

    /// Writes down what reached its one input, under its own id.
    struct Keeps {
        port: &'static str,
        kept: Arc<std::sync::Mutex<Vec<(String, Value)>>>,
    }
    test_manifest!(Keeps, "Keeps");
    #[async_trait]
    impl Node for Keeps {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let value: Value = ctx.inputs.get(self.port)?;
            self.kept.lock().unwrap().push((ctx.node_id.clone(), value));
            Ok(())
        }
    }

    /// A file `B` that upper-cases its text a character at a time, in a
    /// loop of its own (`B.each`), included three times: at sites `one`
    /// and `two` at the top, and at `L.call` inside a loop `L` over two
    /// texts.
    ///
    ///   start.a -> one(B) -> sa        start.b -> two(B) -> sb
    ///   lister.items -> L { L.call(B) } -> total
    fn three_calls() -> ProjectDefinition {
        let [each_in, each_out] = loop_boundaries("B.each", "chars", "String", "up", &["B"]);
        let [l_in, l_out] = loop_boundaries("L", "items", "String", "text", &[]);
        program(
            vec![
                node("start", "Start", &[], &[("a", "String"), ("b", "String")], &[]),
                node("lister", "Lister", &[], &[("items", "List[String]")], &[]),
                node("B.split", "Split", &[("text", "String")], &[("chars", "List[String]")], &["B"]),
                each_in,
                node("B.each.up", "Upper", &[("chars", "String")], &[("up", "String")], &["B", "B.each"]),
                each_out,
                node("B.join", "Join", &[("up", "List[String | Null]")], &[("text", "String")], &["B"]),
                node("sa", "Keeps", &[("text", "String")], &[], &[]),
                node("sb", "Keeps", &[("text", "String")], &[], &[]),
                l_in,
                l_out,
                node("total", "KeepsList", &[("text", "List[String | Null]")], &[], &[]),
            ],
            vec![
                wire("start", "a", &[("one", "x", "in"), ("B", "x", "in")], "B.split", "text"),
                wire("start", "b", &[("two", "x", "in"), ("B", "x", "in")], "B.split", "text"),
                wire("lister", "items", &[], "L__in", "items"),
                wire("L__in", "items", &[("L.call", "x", "in"), ("B", "x", "in")], "B.split", "text"),
                wire("B.split", "chars", &[], "B.each__in", "chars"),
                wire("B.each__in", "chars", &[], "B.each.up", "chars"),
                wire("B.each.up", "up", &[], "B.each__out", "up"),
                wire("B.each__out", "up", &[], "B.join", "up"),
                wire("B.join", "text", &[("B", "y", "out"), ("one", "y", "out")], "sa", "text"),
                wire("B.join", "text", &[("B", "y", "out"), ("two", "y", "out")], "sb", "text"),
                wire("B.join", "text", &[("B", "y", "out"), ("L.call", "y", "out")], "L__out", "text"),
                wire("L__out", "text", &[], "total", "text"),
            ],
            vec![
                group("one", json!({ "kind": "call", "body": "B" }), None, "String", &[]),
                group("two", json!({ "kind": "call", "body": "B" }), None, "String", &[]),
                group("B", json!({ "kind": "body" }), None, "String", &["B.split", "B.join"]),
                group("B.each", json!({ "kind": "loop", "loopConfig": {} }), Some("B"), "String", &["B.each.up"]),
                group("L", json!({ "kind": "loop", "loopConfig": {} }), None, "String", &[]),
                group("L.call", json!({ "kind": "call", "body": "B" }), Some("L"), "String", &[]),
            ],
        )
    }

    /// Each call of a file runs it once, under its own site: the file's
    /// nodes run once per call, each firing carrying only that call's
    /// frame, and each call's answer reaches only what is wired to that
    /// call. A call inside a loop runs once per iteration, under the
    /// iteration and then the site, and the file's own loop runs inside
    /// each call: "hello", "hey", "a" and "b" make 5 + 3 + 1 + 1
    /// iterations of it.
    #[tokio::test]
    async fn a_file_included_three_times_runs_once_per_call() {
        let kept = Arc::new(std::sync::Mutex::new(Vec::new()));
        let nodes = catalog(vec![
            ("Start", Box::new(Sends(vec![("a", json!("hello")), ("b", json!("hey"))]))),
            ("Lister", Box::new(Sends(vec![("items", json!(["a", "b"]))]))),
            ("Split", Box::new(Split)),
            ("Upper", Box::new(Upper)),
            ("Join", Box::new(Join)),
            ("Keeps", Box::new(Keeps { port: "text", kept: kept.clone() })),
            ("KeepsList", Box::new(Keeps { port: "text", kept: kept.clone() })),
        ]);
        let project = three_calls();
        let run_id = weft_core::new_run_id();
        let (drove, events) = drive_journal_observed(project.clone(), nodes, run_id, birth(&project, run_id, Default::default()), CancellationFlag::new_arc()).await;
        let drove = drove.expect("the run drives");
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);

        let mut kept = kept.lock().unwrap().clone();
        kept.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            kept,
            vec![("sa".to_string(), json!("HELLO")), ("sb".to_string(), json!("HEY")), ("total".to_string(), json!(["A", "B"]))],
            "each call's answer reaches what is wired to that call alone"
        );
        let mut joined = completed_at(&events, "B.join");
        joined.sort_by_key(|frames| format!("{frames:?}"));
        let mut expected = vec![vec![site("one")], vec![site("two")], vec![iteration(0), site("L.call")], vec![iteration(1), site("L.call")]];
        expected.sort_by_key(|frames| format!("{frames:?}"));
        assert_eq!(joined, expected, "one run of the file per call, under that call's frames");
        let inner = events.iter().filter(|event| matches!(event, RunEvent::LoopIterationLaunched { group_id, .. } if group_id == "B.each")).count();
        assert_eq!(inner, 10, "the file's own loop ran inside each call");
        assert_eq!(completed_at(&events, "B.each.up").len(), 10);
    }

    /// One connection a `ReadsKey` was handed, with the frames of its site.
    type KeptKey = (Vec<Frame>, Value);

    /// Writes down the connection it was handed, under its site.
    struct ReadsKey {
        kept: Arc<std::sync::Mutex<Vec<KeptKey>>>,
    }
    test_manifest!(ReadsKey, "ReadsKey");
    #[async_trait]
    impl Node for ReadsKey {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let key: Value = ctx.inputs.get("key")?;
            self.kept.lock().unwrap().push((ctx.frames.clone(), key));
            Ok(())
        }
    }

    /// A file `P` whose one node takes a connection picked on the install,
    /// included at `one` and `two`.
    fn picked_twice() -> ProjectDefinition {
        let mut key = node("P.key", "ReadsKey", &[], &[], &["P"]);
        key["inputs"] = json!([{ "name": "key", "portType": "Access", "required": true }]);
        key["portLiterals"] = json!({ "key": weft_core::picks::install_picked_literal() });
        program(
            vec![key],
            vec![],
            vec![
                group("one", json!({ "kind": "call", "body": "P" }), None, "String", &[]),
                group("two", json!({ "kind": "call", "body": "P" }), None, "String", &[]),
                group("P", json!({ "kind": "body" }), None, "String", &["P.key"]),
            ],
        )
    }

    /// Each call of a file runs on the connection picked for it: a node
    /// whose connection is picked on the install takes, at each site, the
    /// pick made for that site, never the other's.
    #[tokio::test]
    async fn each_call_runs_on_the_connection_picked_for_it() {
        let kept = Arc::new(std::sync::Mutex::new(Vec::new()));
        let nodes = catalog(vec![("ReadsKey", Box::new(ReadsKey { kept: kept.clone() }) as Box<dyn Node>)]);
        let project = picked_twice();
        let place = |call: &str| weft_core::project::address_of(&project, "P.key", &[call.to_string()]);
        let handle = |id: u128| json!({ "id": uuid::Uuid::from_u128(id) });
        let picks: weft_core::picks::Picks =
            [(place("one"), [("key".to_string(), handle(1))].into()), (place("two"), [("key".to_string(), handle(2))].into())].into();
        let run_id = weft_core::new_run_id();
        let (drove, _) = drive_journal_observed(project.clone(), nodes, run_id, birth(&project, run_id, picks), CancellationFlag::new_arc()).await;
        let drove = drove.expect("the run drives");
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        let mut kept = kept.lock().unwrap().clone();
        kept.sort_by_key(|(frames, _)| format!("{frames:?}"));
        assert_eq!(kept, vec![(vec![site("one")], handle(1)), (vec![site("two")], handle(2))]);
    }

    /// Waits for a person, then sends what they answered.
    struct Holds;
    test_manifest!(Holds, "Holds");
    #[async_trait]
    impl Node for Holds {
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
            ctx.pulse_downstream(NodeOutput::new().set("y", answer)).await
        }
    }

    /// A file `W` whose one node waits for a person, included at `one`
    /// (answering `first`) and `two` (answering `second`).
    fn waits_twice() -> ProjectDefinition {
        program(
            vec![
                node("W.hold", "Holds", &[], &[("y", "String")], &["W"]),
                node("first", "Keeps", &[("text", "String")], &[], &[]),
                node("second", "Keeps", &[("text", "String")], &[], &[]),
            ],
            vec![
                wire("W.hold", "y", &[("W", "y", "out"), ("one", "y", "out")], "first", "text"),
                wire("W.hold", "y", &[("W", "y", "out"), ("two", "y", "out")], "second", "text"),
            ],
            vec![
                group("one", json!({ "kind": "call", "body": "W" }), None, "String", &[]),
                group("two", json!({ "kind": "call", "body": "W" }), None, "String", &[]),
                group("W", json!({ "kind": "body" }), None, "String", &["W.hold"]),
            ],
        )
    }

    /// The wait each parked firing of `node` registered, by its frames.
    fn parked(events: &[RunEvent], node: &str) -> Vec<(Vec<Frame>, String)> {
        let mut parked: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                RunEvent::NodeSuspended { node_id, frames, token, .. } if node_id == node => Some((frames.clone(), token.clone())),
                _ => None,
            })
            .collect();
        parked.sort_by_key(|(frames, _)| format!("{frames:?}"));
        parked.dedup();
        parked
    }

    /// Each call of a file holds its own wait: two waits, one per site,
    /// each its own token. Answering one call's wakes that call alone
    /// (its answer goes where that call is wired, and the run parks again
    /// on the other); answering the other then ends the run.
    #[tokio::test]
    async fn each_call_holds_its_own_wait_and_is_woken_on_its_own() {
        let kept = Arc::new(std::sync::Mutex::new(Vec::new()));
        let nodes = catalog(vec![
            ("Holds", Box::new(Holds) as Box<dyn Node>),
            ("Keeps", Box::new(Keeps { port: "text", kept: kept.clone() })),
        ]);
        let project = Arc::new(waits_twice());
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth(&project, run_id, Default::default()));
        let tasks = AwaitTasks::new();
        let drive = || {
            let clients = EngineClients { tasks: tasks.clone(), ..clients(journal.clone()) };
            drive_on(project.clone(), nodes.clone(), run_id, journal.clone(), clients, CancellationFlag::new_arc(), None)
        };

        let outcome = drive().await.expect("the first drive").outcome;
        assert!(matches!(outcome, RunOutcome::Stalled), "{outcome:?}");
        let waits = parked(&journal.events_of(run_id), "W.hold");
        assert_eq!(waits.iter().map(|(frames, _)| frames.clone()).collect::<Vec<_>>(), vec![vec![site("one")], vec![site("two")]], "one wait per call");
        assert_ne!(waits[0].1, waits[1].1, "each its own token");

        journal.seed(&[RunEvent::wait_answered(run_id, waits[0].1.clone(), weft_core::primitive::WaitAnswer::Given { value: json!("hello") }, 1)]);
        let outcome = drive().await.expect("the second drive").outcome;
        assert!(matches!(outcome, RunOutcome::Stalled), "the other call still waits: {outcome:?}");
        assert_eq!(*kept.lock().unwrap(), vec![("first".to_string(), json!("hello"))], "only the call woken answered");
        assert_eq!(completed_at(&journal.events_of(run_id), "W.hold"), vec![vec![site("one")]]);

        journal.seed(&[RunEvent::wait_answered(run_id, waits[1].1.clone(), weft_core::primitive::WaitAnswer::Given { value: json!("there") }, 2)]);
        let outcome = drive().await.expect("the third drive").outcome;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let mut kept = kept.lock().unwrap().clone();
        kept.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(kept, vec![("first".to_string(), json!("hello")), ("second".to_string(), json!("there"))]);
        let mut held = completed_at(&journal.events_of(run_id), "W.hold");
        held.sort_by_key(|frames| format!("{frames:?}"));
        assert_eq!(held, vec![vec![site("one")], vec![site("two")]]);
    }
