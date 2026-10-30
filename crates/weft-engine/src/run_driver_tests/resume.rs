    use super::*;
    use crate::run_driver::engine_test_rig::{catalog, drive_journal};
    use serde_json::json;
    use weft_core::signal::{to_spec, Form, FormSchema};
    use weft_journal::RunEvent;

    fn run_id() -> RunId {
        uuid::Uuid::nil()
    }

    fn spec() -> weft_core::primitive::SignalSpec {
        to_spec(Form {
            form_type: "human_query".into(),
            schema: FormSchema { fields: Vec::new() },
            title: None,
            description: None,
            consumer_kind: None,
        })
    }

    fn registered(token: &str, call_index: u32) -> RunEvent {
        RunEvent::SuspensionRegistered {
            run_id: run_id(),
            node_id: "n".into(),
            frames: vec![],
            token: token.into(),
            spec: spec(),
            call_index,
            at_unix: 0,
        }
    }

    fn suspended(token: &str) -> RunEvent {
        RunEvent::NodeSuspended {
            run_id: run_id(),
            node_id: "n".into(),
            frames: vec![],
            token: token.into(),
            at_unix: 0,
        }
    }

    /// Multi-await body where the FIRST await resolved (and the body
    /// resumed past it) and the body is now parked on the SECOND.
    /// `apply_snapshot` must NOT mark the node for re-dispatch: the
    /// suspension it is currently parked on is unresolved. The old
    /// "any resolved entry in the sequence" check re-dispatched here,
    /// which livelocked every worker boot of such a run (replay,
    /// re-suspend, two fresh journal rows, refetch sees new rows,
    /// repeat until the wall-clock deadline).
    fn two_await_events() -> (String, Vec<RunEvent>) {
        let emission = uuid::Uuid::new_v4();
        let pid = weft_core::run::emission::pulse_id(emission, "e", false).to_string();
        let events = vec![
            started("src"),
            RunEvent::PortEmitted {
                run_id: run_id(),
                emission_id: emission,
                node_id: "src".into(),
                frames: vec![],
                port: "out".into(),
                value: Arc::new(json!("x")),
                at_unix: 0,
            },
            RunEvent::NodeCompleted { run_id: run_id(), node_id: "src".into(), frames: vec![], at_unix: 0 },
            started("n"),
            registered("t0", 0),
            suspended("t0"),
            RunEvent::SuspensionResolved {
                run_id: run_id(),
                token: "t0".into(),
                value: json!("v0"),
                at_unix: 0,
            },
            RunEvent::NodeResumed {
                run_id: run_id(),
                node_id: "n".into(),
                frames: vec![],
                token: Some("t0".into()),
                at_unix: 0,
            },
            registered("t1", 1),
            suspended("t1"),
        ];
        (pid, events)
    }

    fn started(node: &str) -> RunEvent {
        RunEvent::NodeStarted { run_id: run_id(), node_id: node.into(), frames: vec![], at_unix: 0 }
    }

    /// `src.out` feeds `n.in`; neither consumes a stream, so no
    /// crashed firing is unresumable.
    fn await_project() -> Arc<ProjectDefinition> {
        Arc::new(
            serde_json::from_value(json!({
                "id": uuid::Uuid::nil(),
                "nodes": [
                    {
                        "id": "src", "nodeType": "Src", "label": null, "config": null,
                        "position": { "x": 0.0, "y": 0.0 }, "inputs": [],
                        "outputs": [{ "name": "out", "portType": "String", "required": false }],
                        "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                    },
                    {
                        "id": "n", "nodeType": "Awaiter", "label": null, "config": null,
                        "position": { "x": 1.0, "y": 0.0 },
                        "inputs": [{ "name": "in", "portType": "String", "required": true }],
                        "outputs": [],
                        "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                    }
                ],
                "edges": [
                    { "id": "e", "source": "src", "target": "n", "sourceHandle": "out", "targetHandle": "in" }
                ]
            }))
            .expect("await project json"),
        )
    }

    type Applied = (PulseTable, NodeFiringTable, weft_core::primitive::KickTable, Vec<FiringLocation>);

    /// The tables `apply_snapshot` leaves, and the firings it hands back
    /// to be failed.
    fn apply_with_crashed(events: &[RunEvent]) -> Applied {
        let project = await_project();
        let snap = weft_journal::fold_to_snapshot(run_id(), project.clone(), events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let mut pulses = PulseTable::default();
        let mut firings = NodeFiringTable::default();
        let mut kicked = weft_core::primitive::KickTable::new();
        let mut awaited = HashMap::new();
        let mut loops = LoopRuntime::new();
        let program = crate::plan::ProgramTables::new(project.clone());
        let crashed = apply_snapshot(
            &program, snap, &mut pulses, &mut firings, &mut kicked, &mut ScopeTable::default(), &mut awaited, &mut loops,
        )
        .expect("the snapshot resumes");
        (pulses, firings, kicked, crashed.into_iter().map(|firing| firing.loc).collect())
    }

    fn apply(events: &[RunEvent]) -> (PulseTable, NodeFiringTable, weft_core::primitive::KickTable) {
        let (pulses, firings, kicked, crashed) = apply_with_crashed(events);
        assert!(crashed.is_empty(), "no node is left running in these fixtures: {crashed:?}");
        (pulses, firings, kicked)
    }

    /// Whether pulse `pid` at `node` is taken (absorbed) rather than
    /// pending on its wire; it is one of the two.
    fn taken(pulses: &PulseTable, node: &str, pid: &str) -> bool {
        let id: uuid::Uuid = pid.parse().expect("a pulse id");
        let pending = pulses.get(node).is_some_and(|b| b.iter().any(|p| p.id == id && p.status.is_pending()));
        let absorbed = pulses.absorbed(id).is_some_and(|p| p.target_node == node);
        assert!(pending != absorbed, "pulse {pid} is either pending at '{node}' or taken there");
        absorbed
    }

    /// Project with a real stream edge plus a LoopIn boundary that
    /// also declares a generator input, for the crashed-firing routing
    /// tests below.
    fn stream_project() -> Arc<ProjectDefinition> {
        Arc::new(serde_json::from_value(json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                {
                    "id": "producer", "nodeType": "Yielder", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "consumer", "nodeType": "Taker", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "in", "portType": "Generator[Number]", "required": true }],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "work__in", "nodeType": "LoopIn", "label": null,
                    "config": null, "position": { "x": 2.0, "y": 0.0 },
                    "inputs": [{ "name": "rows", "portType": "Generator[Number]", "required": true }],
                    "outputs": [],
                    "features": {}, "scope": [],
                    "groupBoundary": { "groupId": "work", "role": "In" },
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e", "source": "producer", "target": "consumer", "sourceHandle": "out", "targetHandle": "in" }
            ],
            "groups": [{ "id": "work", "kind": "loop", "loopConfig": {}, "nodeIds": [] }]
        }))
        .expect("stream project"))
    }

    fn crashed_running(node: &str) -> Vec<RunEvent> {
        vec![started(node)]
    }

    /// The firings `apply_snapshot` hands back to be failed, as
    /// (location, read a stream).
    fn apply_stream(events: &[RunEvent]) -> Vec<(FiringLocation, bool)> {
        let project = stream_project();
        let snap = weft_journal::fold_to_snapshot(run_id(), project.clone(), events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let mut pulses = PulseTable::default();
        let mut firings = NodeFiringTable::default();
        apply_snapshot(
            &crate::plan::ProgramTables::new(project.clone()), snap, &mut pulses, &mut firings,
            &mut weft_core::primitive::KickTable::new(), &mut ScopeTable::default(), &mut HashMap::new(), &mut LoopRuntime::new(),
        )
        .expect("the snapshot resumes")
        .into_iter()
        .map(|firing| (firing.loc, firing.consumed_a_stream))
        .collect()
    }

    /// A journal the fold rejects does not resume: the run fails, and
    /// the failure is journaled as its terminal (naming the rejected
    /// row and `weft clean`) so the run reads Failed instead of
    /// running forever with no end row.
    #[tokio::test]
    async fn a_journal_that_will_not_fold_fails_the_run_with_a_terminal() {
        let rows = vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id: run_id(),
                project_id: uuid::Uuid::nil(),
                entry_node: "src".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some("test-hash".into()),
                binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
                cut_from: None,
                seed: None,
                instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            // A resume of a firing the journal never opened.
            RunEvent::NodeResumed { run_id: run_id(), node_id: "n".into(), frames: vec![], token: None, at_unix: 0 },
        ];
        let (outcome, events) = drive_journal(
            (*await_project()).clone(),
            catalog(vec![]),
            run_id(),
            rows,
            CancellationFlag::new_arc(),
        )
        .await;
        let err = format!("{:#}", outcome.expect_err("a rejected row refuses the resume"));
        assert!(err.contains("does not fit the program it runs now") && err.contains("weft run --seed"), "{err}");
        let Some(RunEvent::RunFailed { error, .. }) = events.last() else {
            panic!("the failure is the run's terminal, got {:?}", events.last());
        };
        assert!(error.contains("node_resumed node=n") && error.contains("weft clean"), "{error}");
        assert_eq!(events.iter().filter(|e| e.is_run_terminal()).count(), 1);
    }

    /// A firing a dead worker left running is handed back to be failed,
    /// never re-run; one reading a stream says so in its message.
    #[test]
    fn a_crashed_stream_consumer_is_failed() {
        assert_eq!(
            apply_stream(&crashed_running("consumer")),
            vec![(FiringLocation::new("consumer", vec![]), true)],
        );
    }

    /// A plain node is failed too: nothing says how far its body got.
    #[test]
    fn a_crashed_plain_node_is_failed() {
        assert_eq!(
            apply_stream(&crashed_running("producer")),
            vec![(FiringLocation::new("producer", vec![]), false)],
        );
    }

    /// A LOOP BOUNDARY is runtime machinery whose re-fire is
    /// journal-backed (launched / out_fired / stream_end), so it
    /// carries on instead of failing a fully recoverable loop.
    #[test]
    fn a_crashed_loop_boundary_carries_on() {
        let crashed = apply_stream(&crashed_running("work__in"));
        assert!(crashed.is_empty(), "a LoopIn resumes normally, got {crashed:?}");
    }

    #[test]
    fn parked_on_unresolved_second_await_does_not_redispatch() {
        let (pid, events) = two_await_events();
        let (pulses, _, _) = apply(&events);
        assert!(
            taken(&pulses, "n", &pid),
            "current suspension (t1) is unresolved; un-absorbing would livelock the boot"
        );
    }

    #[test]
    fn resolved_current_await_redispatches() {
        let (pid, mut events) = two_await_events();
        events.push(RunEvent::SuspensionResolved {
            run_id: run_id(),
            token: "t1".into(),
            value: json!("v1"),
            at_unix: 0,
        });
        let (pulses, _, _) = apply(&events);
        assert!(
            !taken(&pulses, "n", &pid),
            "current suspension (t1) resolved; the node must re-dispatch"
        );
    }

    fn kick_events() -> Vec<RunEvent> {
        vec![
            RunEvent::NodeKicked {
                run_id: run_id(),
                node_id: "n".into(), frames: vec![],
                firing: true,
                payload: Some(json!({"body": 1})),
                port_snapshot: None,
                at_unix: 0,
            },
            started("n"),
        ]
    }

    /// A kicked entry node whose worker died mid-Fire (Running firing,
    /// no terminal row) is handed back to be failed, never re-fired:
    /// its kick stays dispatched.
    #[test]
    fn a_crashed_kicked_node_is_failed_not_refired() {
        let (_, _, kicked, crashed) = apply_with_crashed(&kick_events());
        assert_eq!(crashed, vec![FiringLocation::new("n", vec![])]);
        assert!(kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched());
    }

    #[test]
    fn completed_kicked_node_stays_dispatched() {
        let mut events = kick_events();
        events.push(RunEvent::NodeCompleted {
            run_id: run_id(),
            node_id: "n".into(),
            frames: vec![],
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched());
    }

    /// A kicked node parked on a still-pending suspension must NOT
    /// re-dispatch on every worker boot (that is exactly the churn the
    /// resume-location scoping prevents); once its suspension
    /// resolves, it must.
    #[test]
    fn suspended_kicked_node_redispatches_only_after_resolve() {
        let mut events = kick_events();
        events.push(registered("tk", 0));
        events.push(suspended("tk"));
        let (_, _, kicked) = apply(&events);
        assert!(
            kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched(),
            "pending suspension: no re-dispatch churn"
        );
        events.push(RunEvent::SuspensionResolved {
            run_id: run_id(),
            token: "tk".into(),
            value: json!("answer"),
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(
            !kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched(),
            "resolved suspension: kick synthesis must re-fire the node"
        );
    }
