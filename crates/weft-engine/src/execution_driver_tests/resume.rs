    use super::*;
    use crate::execution_driver::engine_test_rig::{catalog, drive_journal};
    use serde_json::json;
    use weft_core::signal::{to_spec, Form, FormSchema};
    use weft_journal::ExecEvent;

    fn color() -> Color {
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

    fn registered(token: &str, call_index: u32) -> ExecEvent {
        ExecEvent::SuspensionRegistered {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            token: token.into(),
            spec: spec(),
            call_index,
            at_unix: 0,
        }
    }

    fn suspended(token: &str) -> ExecEvent {
        ExecEvent::NodeSuspended {
            color: color(),
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
    /// which livelocked every worker boot of such a color (replay,
    /// re-suspend, two fresh journal rows, refetch sees new rows,
    /// repeat until the wall-clock deadline).
    fn two_await_events() -> (String, Vec<ExecEvent>) {
        let emission = uuid::Uuid::new_v4();
        let pid = weft_core::exec::emission::pulse_id(emission, "out", "n", "in", false).to_string();
        let events = vec![
            started("src"),
            ExecEvent::PortEmitted {
                color: color(),
                emission_id: emission,
                node_id: "src".into(),
                frames: vec![],
                port: "out".into(),
                value: Arc::new(json!("x")),
                at_unix: 0,
            },
            started("n"),
            registered("t0", 0),
            suspended("t0"),
            ExecEvent::SuspensionResolved {
                color: color(),
                token: "t0".into(),
                value: json!("v0"),
                at_unix: 0,
            },
            ExecEvent::NodeResumed {
                color: color(),
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

    fn started(node: &str) -> ExecEvent {
        ExecEvent::NodeStarted { color: color(), node_id: node.into(), frames: vec![], at_unix: 0 }
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

    fn apply(events: &[ExecEvent]) -> (PulseTable, NodeExecutionTable, HashMap<FiringLocation, weft_core::primitive::KickedNode>) {
        let project = await_project();
        let snap = weft_journal::fold_to_snapshot(color(), project.clone(), events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        let mut awaited = HashMap::new();
        let mut loops = LoopRuntime::new();
        let doomed = apply_snapshot(
            &project, snap, &mut pulses, &mut executions, &mut kicked, &mut awaited, &mut loops,
        );
        assert!(doomed.is_empty(), "no stream consumers in these fixtures");
        (pulses, executions, kicked)
    }

    fn pulse_status(pulses: &PulseTable, node: &str, pid: &str) -> weft_core::pulse::PulseStatus {
        pulses
            .get(node)
            .and_then(|b| b.iter().find(|p| p.id.to_string() == pid))
            .map(|p| p.status)
            .expect("pulse present")
    }

    /// Project with a real stream edge plus a LoopIn boundary that
    /// also declares a generator input, for the doomed-consumer
    /// routing tests below.
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
            "groups": []
        }))
        .expect("stream project"))
    }

    fn crashed_running(node: &str) -> Vec<ExecEvent> {
        vec![started(node)]
    }

    fn apply_stream(events: &[ExecEvent]) -> Vec<FiringLocation> {
        let project = stream_project();
        let snap = weft_journal::fold_to_snapshot(color(), project.clone(), events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        apply_snapshot(
            &project, snap, &mut pulses, &mut executions,
            &mut HashMap::new(), &mut HashMap::new(), &mut LoopRuntime::new(),
        )
    }

    /// A crashed-Running STREAM CONSUMER is doomed (its already-pulled
    /// items were removed durably, so a re-run would silently compute
    /// over a truncated stream), and lands in the doomed set instead
    /// of being re-dispatched.
    /// A journal the fold rejects does not resume: the run fails, and
    /// the failure is journaled as its terminal (naming the rejected
    /// row and `weft clean`) so the run reads Failed instead of
    /// running forever with no end row.
    #[tokio::test]
    async fn a_journal_that_will_not_fold_fails_the_run_with_a_terminal() {
        let rows = vec![
            ExecEvent::ExecutionStarted {
                color: color(),
                project_id: uuid::Uuid::nil().to_string(),
                entry_node: "src".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some("test-hash".into()),
                node_test: false,
                subgraph: None,
                at_unix: 0,
            },
            // A resume of a firing the journal never opened.
            ExecEvent::NodeResumed { color: color(), node_id: "n".into(), frames: vec![], token: None, at_unix: 0 },
        ];
        let (outcome, events) = drive_journal(
            (*await_project()).clone(),
            catalog(vec![]),
            color(),
            rows,
            CancellationFlag::new_arc(),
        )
        .await;
        let err = format!("{:#}", outcome.expect_err("a rejected row refuses the resume"));
        assert!(err.contains("cannot be folded"), "{err}");
        let Some(ExecEvent::ExecutionFailed { error, .. }) = events.last() else {
            panic!("the failure is the run's terminal, got {:?}", events.last());
        };
        assert!(error.contains("node_resumed node=n") && error.contains("weft clean"), "{error}");
        assert_eq!(events.iter().filter(|e| e.is_execution_terminal()).count(), 1);
    }

    #[test]
    fn crashed_stream_consumer_is_doomed_not_redispatched() {
        let doomed = apply_stream(&crashed_running("consumer"));
        assert_eq!(
            doomed,
            vec![FiringLocation::new("consumer", vec![])],
            "the crashed stream consumer must be doomed"
        );
    }

    /// A crashed-Running node WITHOUT a generator input takes the
    /// normal re-dispatch route, never the doomed one.
    #[test]
    fn crashed_plain_node_is_not_doomed() {
        let doomed = apply_stream(&crashed_running("producer"));
        assert!(doomed.is_empty(), "a plain crashed node re-dispatches, got {doomed:?}");
    }

    /// A crashed-Running LOOP BOUNDARY also declares a generator input
    /// but is replay-safe by construction (journal-backed launched /
    /// out_fired / stream_end), so dooming it would kill a fully
    /// recoverable loop.
    #[test]
    fn crashed_loop_boundary_is_not_doomed() {
        let doomed = apply_stream(&crashed_running("work__in"));
        assert!(doomed.is_empty(), "a LoopIn resumes normally, got {doomed:?}");
    }

    #[test]
    fn parked_on_unresolved_second_await_does_not_redispatch() {
        let (pid, events) = two_await_events();
        let (pulses, _, _) = apply(&events);
        assert_eq!(
            pulse_status(&pulses, "n", &pid),
            weft_core::pulse::PulseStatus::Absorbed,
            "current suspension (t1) is unresolved; un-absorbing would livelock the boot"
        );
    }

    #[test]
    fn resolved_current_await_redispatches() {
        let (pid, mut events) = two_await_events();
        events.push(ExecEvent::SuspensionResolved {
            color: color(),
            token: "t1".into(),
            value: json!("v1"),
            at_unix: 0,
        });
        let (pulses, _, _) = apply(&events);
        assert_eq!(
            pulse_status(&pulses, "n", &pid),
            weft_core::pulse::PulseStatus::Pending,
            "current suspension (t1) resolved; the node must re-dispatch"
        );
    }

    fn kick_events() -> Vec<ExecEvent> {
        vec![
            ExecEvent::NodeKicked {
                color: color(),
                node_id: "n".into(),
                firing: true,
                payload: Some(json!({"body": 1})),
                port_snapshot: None,
                at_unix: 0,
            },
            started("n"),
        ]
    }

    /// Kicked entry node whose worker crashed mid-Fire (Running exec,
    /// no terminal row): `apply_snapshot` must reset `dispatched` so
    /// the kick synthesis re-fires it. Kicked nodes have no inbound
    /// pulses, so the pulse un-absorb path can never cover them; the
    /// old behavior left the exec Running forever and the execution
    /// landed Stuck with the wake payload silently dropped.
    #[test]
    fn crashed_kicked_node_redispatches() {
        let (_, _, kicked) = apply(&kick_events());
        assert!(!kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched);
    }

    #[test]
    fn completed_kicked_node_stays_dispatched() {
        let mut events = kick_events();
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched);
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
            kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched,
            "pending suspension: no re-dispatch churn"
        );
        events.push(ExecEvent::SuspensionResolved {
            color: color(),
            token: "tk".into(),
            value: json!("answer"),
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(
            !kicked.get(&FiringLocation::new("n", vec![])).expect("kick present").dispatched,
            "resolved suspension: kick synthesis must re-fire the node"
        );
    }
