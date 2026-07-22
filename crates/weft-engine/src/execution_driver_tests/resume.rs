    use super::*;
    use serde_json::json;
    use weft_core::signal::{to_spec, Form, FormSchema};
    use weft_journal::ExecEvent;

    fn color() -> Color {
        uuid::Uuid::nil()
    }

    fn spec() -> weft_core::primitive::SignalSpec {
        to_spec(Form {
            form_type: "human_query".into(),
            schema: FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
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
        let pid = uuid::Uuid::new_v4().to_string();
        let events = vec![
            ExecEvent::PulseEmitted {
                color: color(),
                pulse_id: pid.clone(),
                source_node: "src".into(),
                source_port: "out".into(),
                target_node: "n".into(),
                target_port: "in".into(),
                frames: vec![],
                value: json!("x"),
                closed: false,
                at_unix: 0,
            },
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "n".into(),
                frames: vec![],
                input: json!({"in": "x"}),
                pulses_absorbed: vec![pid.clone()],
                closed_ports: vec![],
                at_unix: 0,
            },
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
                value: Some(json!("v0")),
                pulses_absorbed: vec![],
                at_unix: 0,
            },
            registered("t1", 1),
            suspended("t1"),
        ];
        (pid, events)
    }

    fn apply(events: &[ExecEvent]) -> (PulseTable, NodeExecutionTable, HashMap<String, weft_core::primitive::KickedNode>) {
        let snap = weft_journal::fold_to_snapshot(color(), events);
        let mut pulses = PulseTable::default();
        let mut executions = NodeExecutionTable::default();
        let mut kicked = HashMap::new();
        let mut awaited = HashMap::new();
        apply_snapshot(snap, &mut pulses, &mut executions, &mut kicked, &mut awaited);
        (pulses, executions, kicked)
    }

    fn pulse_status(pulses: &PulseTable, node: &str, pid: &str) -> weft_core::pulse::PulseStatus {
        pulses
            .get(node)
            .and_then(|b| b.iter().find(|p| p.id.to_string() == pid))
            .map(|p| p.status)
            .expect("pulse present")
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
            ExecEvent::NodeStarted {
                color: color(),
                node_id: "n".into(),
                frames: vec![],
                input: json!({}),
                pulses_absorbed: vec![],
                closed_ports: vec![],
                at_unix: 0,
            },
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
        assert!(!kicked.get("n").expect("kick present").dispatched);
    }

    #[test]
    fn completed_kicked_node_stays_dispatched() {
        let mut events = kick_events();
        events.push(ExecEvent::NodeCompleted {
            color: color(),
            node_id: "n".into(),
            frames: vec![],
            output: json!({}),
            closure_emissions: vec![],
            at_unix: 0,
        });
        let (_, _, kicked) = apply(&events);
        assert!(kicked.get("n").expect("kick present").dispatched);
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
            kicked.get("n").expect("kick present").dispatched,
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
            !kicked.get("n").expect("kick present").dispatched,
            "resolved suspension: kick synthesis must re-fire the node"
        );
    }
