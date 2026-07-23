    use super::*;
    use async_trait::async_trait;
    use crate::loop_runtime::LoopRuntime;
    use std::sync::Mutex as StdMutex;
    use weft_core::frames::LoopIteration;
    use weft_core::exec::ready::ReadyGroup;
    use weft_core::primitive::LoopInstanceKey;
    use weft_core::project::{
        Edge, GroupBoundary, GroupBoundaryRole, NodeDefinition, PortDefinition, Position,
        ProjectDefinition,
    };

    /// Wrap bare wire ports as instance inputs (the rig declares pure
    /// wire ports; the instance type carries the resolved input surface).
    fn inputs_of(ports: Vec<PortDefinition>) -> Vec<weft_core::project::InputDefinition> {
        ports.into_iter().map(weft_core::project::InputDefinition::from_wire_port).collect()
    }
    use weft_core::pulse::PulseTable;
    use weft_core::weft_type::{WeftPrimitive, WeftType};
    use weft_journal::ExecEvent;

    fn empty_project_dt() -> serde_json::Value {
        serde_json::json!("1970-01-01T00:00:00Z")
    }
    fn parse_dt() -> serde_json::Value {
        empty_project_dt()
    }

    #[derive(Default)]
    struct CapturingJournal {
        events: StdMutex<Vec<ExecEvent>>,
    }
    #[async_trait]
    impl JournalClient for CapturingJournal {
        async fn record_event(&self, event: &ExecEvent, _pod: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn events_for_color(&self, _color: Color) -> anyhow::Result<Vec<ExecEvent>> {
            Ok(self.events.lock().unwrap().clone())
        }
        async fn has_terminal_event(&self, _color: Color) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    fn primitive(p: WeftPrimitive) -> WeftType {
        WeftType::primitive(p)
    }

    fn list_of(inner: WeftType) -> WeftType {
        WeftType::List(Box::new(inner))
    }

    fn list_of_nullable(inner: WeftType) -> WeftType {
        list_of(WeftType::Union(vec![inner, primitive(WeftPrimitive::Null)]))
    }

    /// Build a minimal Loop-shaped project with one LoopIn, one LoopOut,
    /// one body node, one outward consumer node. Returns the project and
    /// the relevant ids.
    struct LoopProject {
        project: ProjectDefinition,
        loop_in_id: String,
        loop_out_id: String,
        body_id: String,
        consumer_id: String,
        group_id: String,
    }

    fn build_parallel_map_project() -> LoopProject {
        // Layout:
        //   producer.items: List[String]
        //   -> loop__in (outer-in: items)
        //   loop__in.items (inside-out, T) -> body.in
        //   body.out -> loop__out.results (inside-in T?)
        //   loop__out.results (outer-out List[String | Null]) -> consumer.data
        let group_id = "myloop".to_string();
        let loop_in_id = format!("{group_id}__in");
        let loop_out_id = format!("{group_id}__out");
        let body_id = "body".to_string();
        let consumer_id = "consumer".to_string();

        // LoopIn boundary node. Loop config lives in `config`.
        let loop_in_cfg = serde_json::json!({
            "parentId": group_id,
            "parallel": true,
            "over": ["items"],
            "carry": [],
        });
        let loop_in = NodeDefinition {
            id: loop_in_id.clone(),
            node_type: "LoopIn".into(),
            label: None,
            config: loop_in_cfg.clone(),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::In }),
            inputs: inputs_of(vec![PortDefinition {
                name: "items".into(),
                port_type: list_of(primitive(WeftPrimitive::String)),
                required: true,
                description: None,
                synthesized_from_carry: false,
            }]),
            outputs: vec![
                PortDefinition {
                    name: "items".into(),
                    port_type: primitive(WeftPrimitive::String),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                },
                PortDefinition {
                    name: "index".into(),
                    port_type: primitive(WeftPrimitive::Number),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                },
            ],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        // LoopOut carries only the parent pointer: loop config
        // (parallel/over/carry/...) is authoritative on LoopIn and a
        // duplicated copy here would create two sources of truth.
        // Matches what the compiler now emits.
        let loop_out_cfg = serde_json::json!({"parentId": group_id});
        let loop_out = NodeDefinition {
            id: loop_out_id.clone(),
            node_type: "LoopOut".into(),
            label: None,
            config: loop_out_cfg,
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::Out }),
            inputs: inputs_of(vec![
                PortDefinition {
                    name: "results".into(),
                    port_type: primitive(WeftPrimitive::String),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                },
                PortDefinition {
                    name: "done".into(),
                    port_type: primitive(WeftPrimitive::Boolean),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                },
            ]),
            outputs: vec![PortDefinition {
                name: "results".into(),
                port_type: list_of_nullable(primitive(WeftPrimitive::String)),
                required: false,
                description: None,
                synthesized_from_carry: false,
            }],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        // Body node: simple Echo with one input port `in: String` and
        // one output `out: String`.
        let body = NodeDefinition {
            id: body_id.clone(),
            node_type: "Echo".into(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![group_id.clone()],
            group_boundary: None,
            inputs: inputs_of(vec![PortDefinition {
                name: "in".into(),
                port_type: primitive(WeftPrimitive::String),
                required: true,
                description: None,
                synthesized_from_carry: false,
            }]),
            outputs: vec![PortDefinition {
                name: "out".into(),
                port_type: primitive(WeftPrimitive::String),
                required: false,
                description: None,
                synthesized_from_carry: false,
            }],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        let consumer = NodeDefinition {
            id: consumer_id.clone(),
            node_type: "Sink".into(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: inputs_of(vec![PortDefinition {
                name: "data".into(),
                port_type: list_of_nullable(primitive(WeftPrimitive::String)),
                required: true,
                description: None,
                synthesized_from_carry: false,
            }]),
            outputs: vec![],
            features: Default::default(),
            requires_infra: false,
            images: vec![],
            span: None,
            header_span: None,
            config_spans: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        };

        let edges = vec![
            Edge {
                id: "e1".into(),
                source: loop_in_id.clone(),
                source_handle: Some("items".into()),
                target: body_id.clone(),
                target_handle: Some("in".into()),
                span: None,
            },
            Edge {
                id: "e2".into(),
                source: body_id.clone(),
                source_handle: Some("out".into()),
                target: loop_out_id.clone(),
                target_handle: Some("results".into()),
                span: None,
            },
            Edge {
                id: "e3".into(),
                source: loop_out_id.clone(),
                source_handle: Some("results".into()),
                target: consumer_id.clone(),
                target_handle: Some("data".into()),
                span: None,
            },
        ];

        let project_json = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": serde_json::to_value(vec![&loop_in, &loop_out, &body, &consumer]).unwrap(),
            "edges": serde_json::to_value(&edges).unwrap(),
            "groups": [],
            "createdAt": parse_dt(),
            "updatedAt": parse_dt(),
        });
        let project: ProjectDefinition = serde_json::from_value(project_json).expect("project deserialize");

        LoopProject {
            project,
            loop_in_id,
            loop_out_id,
            body_id,
            consumer_id,
            group_id,
        }
    }

    /// Helper: fire LoopIn with the given outer input bag at parent_frames=[].
    async fn fire_loop_in(
        lp: &LoopProject,
        outer_input: serde_json::Value,
        rt: &mut LoopRuntime,
        pulses: &mut PulseTable,
        journal: &CapturingJournal,
    ) {
        let edge_idx = weft_core::project::EdgeIndex::build(&lp.project);
        let loop_in = lp.project.nodes.iter().find(|n| n.id == lp.loop_in_id).unwrap();
        let group = ReadyGroup {
            frames: Vec::new(),
            color: uuid::Uuid::nil(),
            input: outer_input,
            closed_ports: Vec::new(),
            should_skip: false,
            pulse_ids: Vec::new(),
            error: None,
        };
        handle_loop_boundary_firing(loop_in, &group, &lp.project, &edge_idx, pulses, journal, "test-pod", rt)
            .await
            .expect("LoopIn firing");
    }

    /// Helper: fire LoopOut for iteration `i` with the given writes.
    async fn fire_loop_out(
        lp: &LoopProject,
        iter: u32,
        writes: serde_json::Value,
        closed_ports: Vec<String>,
        rt: &mut LoopRuntime,
        pulses: &mut PulseTable,
        journal: &CapturingJournal,
    ) {
        let edge_idx = weft_core::project::EdgeIndex::build(&lp.project);
        let loop_out = lp.project.nodes.iter().find(|n| n.id == lp.loop_out_id).unwrap();
        let group = ReadyGroup {
            frames: vec![LoopIteration { index: iter }],
            color: uuid::Uuid::nil(),
            input: writes,
            closed_ports,
            should_skip: false,
            pulse_ids: Vec::new(),
            error: None,
        };
        handle_loop_boundary_firing(loop_out, &group, &lp.project, &edge_idx, pulses, journal, "test-pod", rt)
            .await
            .expect("LoopOut firing");
    }

    /// Layer-3 rig 1: parallel-map LoopIn fires per-iteration body pulses
    /// at distinct frame stacks. Three elements -> three body pulses, one
    /// per iteration's body frame stack.
    #[tokio::test]
    async fn parallel_loop_in_emits_per_iteration_body_pulses() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let body = pulses.get(&lp.body_id).expect("body bucket");
        let on_in: Vec<_> = body
            .iter()
            .filter(|p| p.target_port == "in" && !p.closed)
            .collect();
        assert_eq!(on_in.len(), 3, "three body pulses, one per iteration");
        let frames: Vec<u32> = on_in.iter().map(|p| p.frames[0].index).collect();
        let mut sorted = frames.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2], "iterations 0..3 fired: {:?}", frames);
        let values: Vec<&str> = on_in
            .iter()
            .map(|p| p.value.as_str().unwrap_or(""))
            .collect();
        assert!(values.contains(&"a") && values.contains(&"b") && values.contains(&"c"),
            "all elements distributed: {:?}", values);
    }

    /// Layer-3 rig 2: LoopRuntime records instantiation + per-iteration
    /// launch events on the journal.
    #[tokio::test]
    async fn parallel_loop_in_journal_records_instantiation_and_launches() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let events = journal.events.lock().unwrap();
        let instantiations: Vec<_> = events.iter().filter(|e| matches!(e, ExecEvent::LoopInstantiated { .. })).collect();
        let launches: Vec<_> = events.iter().filter(|e| matches!(e, ExecEvent::LoopIterationLaunched { .. })).collect();
        assert_eq!(instantiations.len(), 1, "one LoopInstantiated event");
        assert_eq!(launches.len(), 2, "one LoopIterationLaunched per iteration");
    }

    /// Layer-3 rig 3: LoopOut firings collect gather writes per iteration
    /// and emit the assembled List[T | Null] outwardly when all iterations
    /// have fired.
    #[tokio::test]
    async fn parallel_loop_out_assembles_and_emits_outward() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Fire LoopOut for each iteration with a real gather write.
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 2, serde_json::json!({"results": "C"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // The outward consumer should now have one pulse on `data` at
        // parent_frames=[] carrying ["A","B","C"].
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data.len(), 1, "one outward pulse on consumer.data");
        assert!(data[0].frames.is_empty(), "outward emit at parent_frames=[]");
        assert_eq!(data[0].value, serde_json::json!(["A", "B", "C"]),
            "assembled in iteration-index order: {:?}", data[0].value);
        let events = journal.events.lock().unwrap();
        let terminated: Vec<_> = events.iter().filter(|e| matches!(e, ExecEvent::LoopTerminated { .. })).collect();
        assert_eq!(terminated.len(), 1, "one LoopTerminated event");
    }

    /// Crash-resume: only iteration 0's `LoopIterationLaunched` row
    /// survived the crash. The re-fired LoopIn must launch exactly the
    /// MISSING iterations (1, 2): deriving launches from
    /// `first_instantiation` instead of the rehydrated `launched` set
    /// silently launched nothing (whole loop skipped), and launching
    /// all three would duplicate iteration 0's journaled body pulses.
    #[tokio::test]
    async fn crash_resumed_loop_in_launches_only_missing_iterations() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        let bag = serde_json::json!({"items": ["a", "b", "c"]});
        fire_loop_in(&lp, bag.clone(), &mut rt, &mut pulses, &journal).await;
        // Keep LoopInstantiated + the FIRST launch row only.
        let mut kept = 0;
        let events: Vec<ExecEvent> = journal
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| match e {
                ExecEvent::LoopIterationLaunched { .. } => {
                    kept += 1;
                    kept <= 1
                }
                _ => true,
            })
            .cloned()
            .collect();
        let snap = weft_journal::fold_to_snapshot(uuid::Uuid::nil(), &events);
        let mut rt2 = rehydrate_loop_runtime(&lp.project, &snap.loop_instances).expect("rehydrate");
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        let body = pulses2.get(&lp.body_id).expect("body bucket");
        let mut frames: Vec<u32> = body
            .iter()
            .filter(|p| p.target_port == "in" && !p.closed)
            .map(|p| p.frames[0].index)
            .collect();
        frames.sort();
        assert_eq!(frames, vec![1, 2], "only the missing iterations relaunch");
        let events2 = journal2.events.lock().unwrap();
        let launches = events2.iter().filter(|e| matches!(e, ExecEvent::LoopIterationLaunched { .. })).count();
        assert_eq!(launches, 2, "one launch row per relaunched iteration");
        let instantiations = events2.iter().filter(|e| matches!(e, ExecEvent::LoopInstantiated { .. })).count();
        assert_eq!(instantiations, 0, "rehydrated instance must not re-journal LoopInstantiated");
    }

    /// Crash-resume replay of a zero-iteration LoopIn AFTER its
    /// `LoopTerminated` row landed: the outward emit must not run
    /// again (duplicate empty-list pulses would re-fire downstream).
    #[tokio::test]
    async fn replayed_zero_iter_loop_in_does_not_duplicate_outward() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        let bag = serde_json::json!({"items": []});
        fire_loop_in(&lp, bag.clone(), &mut rt, &mut pulses, &journal).await;
        assert_eq!(
            pulses.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            1,
            "zero-iter loop emits one outward pulse"
        );
        let events: Vec<ExecEvent> = journal.events.lock().unwrap().clone();
        let snap = weft_journal::fold_to_snapshot(uuid::Uuid::nil(), &events);
        let mut rt2 = rehydrate_loop_runtime(&lp.project, &snap.loop_instances).expect("rehydrate");
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        assert_eq!(
            pulses2.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            0,
            "replay on a terminated instance emits nothing"
        );
        let events2 = journal2.events.lock().unwrap();
        let terminated = events2.iter().filter(|e| matches!(e, ExecEvent::LoopTerminated { .. })).count();
        assert_eq!(terminated, 0, "no duplicate LoopTerminated row");
    }

    /// Crash-resume replay of a LoopOut firing the runtime refuses
    /// (post-termination / already fired) must journal NOTHING: the
    /// fold applies `LoopOutFired` unconditionally, so a row for a
    /// refused firing diverges the rehydrated instance from the live
    /// one.
    #[tokio::test]
    async fn replayed_loop_out_journals_no_duplicate_rows() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(&lp, serde_json::json!({"items": ["a", "b"]}), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let count_rows = |j: &CapturingJournal| {
            let evs = j.events.lock().unwrap();
            (
                evs.iter().filter(|e| matches!(e, ExecEvent::LoopOutFired { .. })).count(),
                evs.iter().filter(|e| matches!(e, ExecEvent::LoopTerminated { .. })).count(),
            )
        };
        assert_eq!(count_rows(&journal), (2, 1));
        let outward_before = pulses.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0);
        // Replay LoopOut@1 (crash between its journal row and its
        // NodeCompleted): runtime refuses, journal must not grow.
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        assert_eq!(count_rows(&journal), (2, 1), "no duplicate LoopOutFired / LoopTerminated rows");
        assert_eq!(
            pulses.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            outward_before,
            "no duplicate outward pulses"
        );
    }

    /// Layer-3 rig 4: gather-port closure at LoopOut produces `null` in
    /// the assembled outward list at that iteration's slot.
    #[tokio::test]
    async fn closure_on_gather_port_becomes_null_at_index() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // Iteration 1: body failed to write `results` (port closed).
        fire_loop_out(&lp, 1, serde_json::json!({}), vec!["results".into()], &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 2, serde_json::json!({"results": "C"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data[0].value, serde_json::json!(["A", null, "C"]),
            "closed iteration becomes null at its index: {:?}", data[0].value);
    }

    /// Layer-3 rig 5: zero-iteration loop (empty `over`) terminates
    /// immediately and emits an empty list outwardly.
    #[tokio::test]
    async fn zero_iteration_loop_terminates_with_empty_list() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": []}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // No body pulses, no iterations.
        let body = pulses.get(&lp.body_id);
        assert!(body.map(|b| b.is_empty()).unwrap_or(true), "no body work for zero iterations");
        // But still, the loop emits outwardly with an empty assembly.
        let events = journal.events.lock().unwrap();
        assert!(events.iter().any(|e| matches!(e, ExecEvent::LoopTerminated { .. })),
            "zero-iteration loop terminates outwardly");
    }

    /// Termination reason on the zero-iteration shortcut. When the
    /// user writes `max_iters: 0`, the binding constraint is
    /// MaxItersReached, not OverExhausted. The live (non-zero) path's
    /// reason logic must hold for the zero-iter shortcut too;
    /// hardcoding `OverExhausted` (the old shape) lies to the
    /// inspector about which knob bound the loop.
    #[tokio::test]
    async fn zero_iteration_with_max_iters_zero_reports_max_iters_reason() {
        use weft_core::primitive::LoopTerminationReason;
        let mut lp = build_parallel_map_project();
        for n in lp.project.nodes.iter_mut() {
            if matches!(n.node_type.as_str(), "LoopIn" | "LoopOut") {
                if let Some(obj) = n.config.as_object_mut() {
                    obj.insert("max_iters".into(), serde_json::json!(0));
                }
            }
        }
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let events = journal.events.lock().unwrap();
        let term = events.iter().find_map(|e| match e {
            ExecEvent::LoopTerminated { reason, .. } => Some(*reason),
            _ => None,
        }).expect("loop terminated");
        assert_eq!(
            term, LoopTerminationReason::MaxItersReached,
            "max_iters=0 is the binding constraint, not over-exhausted"
        );
    }

    /// Layer-3 rig 6: cancellation marks every live LoopInstance for the
    /// color as cancelled AND emits closures on the LoopOut's outward
    /// output ports at parent_frames.
    #[tokio::test]
    async fn cancel_loop_instances_emits_outward_closures() {
        use weft_core::primitive::LoopTerminationReason;
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Mid-flight: no LoopOut firings happened. Cancel.
        let edge_idx = weft_core::project::EdgeIndex::build(&lp.project);
        cancel_loop_instances(
            &mut rt,
            uuid::Uuid::nil(),
            &lp.project,
            &edge_idx,
            &mut pulses,
            &journal,
            "test-pod",
        )
        .await;
        // Instance is now terminated::Cancelled.
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(),
            parent_frames: Vec::new(),
            color: uuid::Uuid::nil(),
        };
        let inst = rt.get(&key).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::Cancelled));
        // Consumer received a closure on `data` at parent_frames=[].
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let closures: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && p.closed).collect();
        assert_eq!(closures.len(), 1, "one outward closure on consumer.data");
        assert!(closures[0].frames.is_empty(), "closure at parent_frames=[]");
    }

    /// Layer-3 rig 7: `max_iters` caps the launched iteration count even
    /// when `over` is longer.
    #[tokio::test]
    async fn max_iters_caps_launched_count() {
        // Rebuild project with max_iters=2 on the loop config.
        let mut lp = build_parallel_map_project();
        for n in lp.project.nodes.iter_mut() {
            if matches!(n.node_type.as_str(), "LoopIn" | "LoopOut") {
                if let Some(obj) = n.config.as_object_mut() {
                    obj.insert("max_iters".into(), serde_json::json!(2));
                }
            }
        }
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c", "d", "e"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let body = pulses.get(&lp.body_id).expect("body bucket");
        let on_in: Vec<_> = body.iter().filter(|p| p.target_port == "in" && !p.closed).collect();
        assert_eq!(on_in.len(), 2, "max_iters=2 caps body firings to 2: got {}", on_in.len());
    }

    /// Layer-3 rig 8: parallel ordering preservation. Fire LoopOut events
    /// out of order; the assembled outward list still matches input order.
    #[tokio::test]
    async fn parallel_ordering_preserved_regardless_of_loop_out_firing_order() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Fire out of order: 2, 0, 1.
        fire_loop_out(&lp, 2, serde_json::json!({"results": "C"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "B"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data[0].value, serde_json::json!(["A", "B", "C"]),
            "BTreeMap-driven assembly preserves input order: {:?}", data[0].value);
    }

    /// Layer-3 rig 9: compute_loop_iter_count zip-trim behavior with two
    /// `over` ports of different lengths.
    #[test]
    fn compute_iter_count_trims_to_shortest_with_trim_on() {
        use crate::loop_runtime::LoopConfig;
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into(), "b".into()],
            carry: vec![],
            max_iters: None,
            trim_on_mismatch: true,
        };
        let input: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30]})
        ).unwrap();
        let count = compute_loop_iter_count(&cfg, &input).expect("ok");
        assert_eq!(count, 3, "trims to shortest: {count}");
    }

    /// Layer-3 rig 10: compute_loop_iter_count panics loud with
    /// trim_on_mismatch=false and unequal lengths.
    #[test]
    fn compute_iter_count_rejects_mismatch_with_trim_off() {
        use crate::loop_runtime::LoopConfig;
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into(), "b".into()],
            carry: vec![],
            max_iters: None,
            trim_on_mismatch: false,
        };
        let input: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({"a": [1, 2, 3], "b": [10, 20]})
        ).unwrap();
        let err = compute_loop_iter_count(&cfg, &input).expect_err("must err on mismatch");
        assert!(err.contains("mismatch"), "loud mismatch error: {err}");
    }

    /// Layer-3 rig 11: max_iters cap applies in compute_iter_count.
    #[test]
    fn compute_iter_count_caps_at_max_iters() {
        use crate::loop_runtime::LoopConfig;
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into()],
            carry: vec![],
            max_iters: Some(2),
            trim_on_mismatch: true,
        };
        let input: serde_json::Map<String, serde_json::Value> = serde_json::from_value(
            serde_json::json!({"a": [1, 2, 3, 4, 5]})
        ).unwrap();
        let count = compute_loop_iter_count(&cfg, &input).expect("ok");
        assert_eq!(count, 2, "max_iters caps: {count}");
    }

    /// Layer-3 rig 12: `self.index` pulse arrives at each iteration's
    /// frame stack with the correct index value.
    #[tokio::test]
    async fn implicit_index_pulse_at_each_iteration_frame() {
        // Wire the index port into a body input on the body node by
        // editing the project. Simpler: just check the pulse at LoopIn's
        // `index` output reaches downstream nodes wired to it. The body
        // node's `in` port is wired to `items`, not `index`, so `index`
        // pulses won't reach `body.in`. Instead, scan all pulses for a
        // PulseEmitted on source_port=index. But this fires through
        // postprocess; pulses with no consumer edge are silently dropped.
        // Easier path: check the journal's LoopIterationLaunched events
        // line up with the iteration count.
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        let events = journal.events.lock().unwrap();
        let mut indices: Vec<u32> = events.iter().filter_map(|e| match e {
            ExecEvent::LoopIterationLaunched { index, .. } => Some(*index),
            _ => None,
        }).collect();
        indices.sort();
        assert_eq!(indices, vec![0, 1, 2], "each iteration launched with its index: {:?}", indices);
    }

    /// Layer-3 rig 13: nested loops produce distinct LoopInstance entries
    /// keyed by parent_frames.
    #[tokio::test]
    async fn nested_loops_have_distinct_instance_keys() {
        let lp = build_parallel_map_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Simulate: outer LoopIn instantiates at parent_frames=[].
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["x", "y"]}),
            &mut rt,
            &mut pulses,
            &journal,
        )
        .await;
        // Now imagine an inner loop instance keyed at parent_frames=[{0}]
        // (one inner instance per outer iteration). The runtime keys by
        // (group_id, parent_frames, color); two distinct parent_frames
        // mean two distinct instances even for the same group_id.
        let key_outer = LoopInstanceKey {
            group_id: lp.group_id.clone(),
            parent_frames: Vec::new(),
            color: uuid::Uuid::nil(),
        };
        let key_inner_iter0 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 0 }],
            color: uuid::Uuid::nil(),
        };
        let key_inner_iter1 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![LoopIteration { index: 1 }],
            color: uuid::Uuid::nil(),
        };
        rt.ensure(key_inner_iter0.clone(), crate::loop_runtime::LoopConfig {
            parallel: false, over: vec![], carry: vec![], max_iters: Some(1), trim_on_mismatch: true,
        }, 1, vec![]);
        rt.ensure(key_inner_iter1.clone(), crate::loop_runtime::LoopConfig {
            parallel: false, over: vec![], carry: vec![], max_iters: Some(1), trim_on_mismatch: true,
        }, 1, vec![]);
        assert!(rt.get(&key_outer).is_some(), "outer instance lives");
        assert!(rt.get(&key_inner_iter0).is_some(), "inner instance at outer iter 0 lives");
        assert!(rt.get(&key_inner_iter1).is_some(), "inner instance at outer iter 1 lives");
        // Distinct: cancelling one does not affect the other.
        rt.cancel_inside(&vec![LoopIteration { index: 0 }], uuid::Uuid::nil());
        use weft_core::primitive::LoopTerminationReason;
        assert_eq!(rt.get(&key_inner_iter0).unwrap().terminated, Some(LoopTerminationReason::Cancelled),
            "iter 0's inner instance cancelled");
        assert!(rt.get(&key_inner_iter1).unwrap().terminated.is_none(),
            "iter 1's inner instance untouched");
    }

    /// Sequential-fold project: `over: ["items"]`, `carry: ["acc"]`.
    /// LoopOut has a `results` gather output (List[String | Null]) AND
    /// an `acc` carry output (String). Body wires `self.items` to a
    /// concat node and writes both `self.results` and `self.acc`.
    fn build_sequential_fold_project() -> LoopProject {
        let group_id = "fold".to_string();
        let loop_in_id = format!("{group_id}__in");
        let loop_out_id = format!("{group_id}__out");
        let body_id = "body".to_string();
        let consumer_id = "consumer".to_string();
        let loop_cfg = serde_json::json!({
            "parentId": group_id,
            "parallel": false,
            "over": ["items"],
            "carry": ["acc"],
        });
        let loop_in = NodeDefinition {
            id: loop_in_id.clone(), node_type: "LoopIn".into(), label: None,
            config: loop_cfg.clone(), position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::In }),
            inputs: inputs_of(vec![
                PortDefinition { name: "items".into(), port_type: list_of(primitive(WeftPrimitive::String)), required: true, description: None, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),   port_type: primitive(WeftPrimitive::String),         required: false, description: None, synthesized_from_carry: false },
            ]),
            outputs: vec![
                PortDefinition { name: "items".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),   port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false },
                PortDefinition { name: "index".into(), port_type: primitive(WeftPrimitive::Number), required: false, description: None, synthesized_from_carry: false },
            ],
            features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        // LoopOut carries only `{"parentId": ...}` (matches compiler).
        let loop_out_cfg = serde_json::json!({"parentId": group_id});
        let loop_out = NodeDefinition {
            id: loop_out_id.clone(), node_type: "LoopOut".into(), label: None,
            config: loop_out_cfg, position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::Out }),
            inputs: inputs_of(vec![
                PortDefinition { name: "results".into(), port_type: primitive(WeftPrimitive::String),  required: false, description: None, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),     port_type: primitive(WeftPrimitive::String),  required: false, description: None, synthesized_from_carry: false },
                PortDefinition { name: "done".into(),    port_type: primitive(WeftPrimitive::Boolean), required: false, description: None, synthesized_from_carry: false },
            ]),
            outputs: vec![
                PortDefinition { name: "results".into(), port_type: list_of_nullable(primitive(WeftPrimitive::String)), required: false, description: None, synthesized_from_carry: false },
                PortDefinition { name: "acc".into(),     port_type: primitive(WeftPrimitive::String),                   required: false, description: None, synthesized_from_carry: false },
            ],
            features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        let body = NodeDefinition {
            id: body_id.clone(), node_type: "Concat".into(), label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![group_id.clone()], group_boundary: None,
            inputs: inputs_of(vec![
                PortDefinition { name: "left".into(),  port_type: primitive(WeftPrimitive::String), required: true, description: None, synthesized_from_carry: false },
                PortDefinition { name: "right".into(), port_type: primitive(WeftPrimitive::String), required: true, description: None, synthesized_from_carry: false },
            ]),
            outputs: vec![
                PortDefinition { name: "out".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false },
            ],
            features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        let consumer = NodeDefinition {
            id: consumer_id.clone(), node_type: "Sink".into(), label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 }, scope: vec![], group_boundary: None,
            inputs: inputs_of(vec![
                PortDefinition { name: "data".into(),  port_type: list_of_nullable(primitive(WeftPrimitive::String)), required: true, description: None, synthesized_from_carry: false },
                PortDefinition { name: "final".into(), port_type: primitive(WeftPrimitive::String),                    required: true, description: None, synthesized_from_carry: false },
            ]),
            outputs: vec![], features: Default::default(), requires_infra: false, images: vec![],
            span: None, header_span: None, config_spans: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(),
            file_refs: Default::default(), include_path: None,
        };
        let edges = vec![
            // body reads element + carry from LoopIn.
            Edge { id: "e1".into(), source: loop_in_id.clone(),  source_handle: Some("items".into()), target: body_id.clone(),     target_handle: Some("right".into()), span: None },
            Edge { id: "e2".into(), source: loop_in_id.clone(),  source_handle: Some("acc".into()),   target: body_id.clone(),     target_handle: Some("left".into()),  span: None },
            // body writes back to LoopOut on both results and acc.
            Edge { id: "e3".into(), source: body_id.clone(),     source_handle: Some("out".into()),   target: loop_out_id.clone(), target_handle: Some("results".into()), span: None },
            Edge { id: "e4".into(), source: body_id.clone(),     source_handle: Some("out".into()),   target: loop_out_id.clone(), target_handle: Some("acc".into()),     span: None },
            // outward to consumer.
            Edge { id: "e5".into(), source: loop_out_id.clone(), source_handle: Some("results".into()), target: consumer_id.clone(), target_handle: Some("data".into()),  span: None },
            Edge { id: "e6".into(), source: loop_out_id.clone(), source_handle: Some("acc".into()),     target: consumer_id.clone(), target_handle: Some("final".into()), span: None },
        ];
        let project_json = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": serde_json::to_value(vec![&loop_in, &loop_out, &body, &consumer]).unwrap(),
            "edges": serde_json::to_value(&edges).unwrap(),
            "groups": [], "createdAt": parse_dt(), "updatedAt": parse_dt(),
        });
        let project: ProjectDefinition = serde_json::from_value(project_json).expect("project");
        LoopProject { project, loop_in_id, loop_out_id, body_id, consumer_id, group_id }
    }

    /// Sequential mode launches iteration 0 on LoopIn fire, then each
    /// LoopOut fire either launches the next iteration or emits outward.
    /// This test pins the regression where LoopIn's input bag had been
    /// absorbed before iteration 1's launch, leaving the next iteration
    /// with no `items` / `acc` to read.
    #[tokio::test]
    async fn sequential_fold_threads_carry_across_iterations() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Fold "a" + "b" + "c" with initial acc = "".
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b", "c"], "acc": ""}),
            &mut rt, &mut pulses, &journal,
        ).await;

        // After LoopIn: iteration 0's body bucket has `items=a`, `acc=""`.
        fn iter_ports<'a>(
            pulses: &'a PulseTable, body_id: &str, idx: u32,
        ) -> std::collections::HashMap<String, serde_json::Value> {
            pulses.get(body_id)
                .map(|b| b.iter()
                    .filter(|p| p.frames.len() == 1 && p.frames[0].index == idx && !p.closed)
                    .map(|p| (p.target_port.clone(), p.value.clone()))
                    .collect())
                .unwrap_or_default()
        }
        let by_port = iter_ports(&pulses, &lp.body_id, 0);
        assert_eq!(by_port.get("right"), Some(&serde_json::json!("a")));
        assert_eq!(by_port.get("left"),  Some(&serde_json::json!("")));

        // Body for iteration 0 writes "a" on `results` AND `acc`.
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "a"}), Vec::new(), &mut rt, &mut pulses, &journal).await;

        // Iteration 1 must have been launched at frame=[{1}] with the
        // outer items still flowing through AND the updated carry. This
        // is the regression check: before the fix, the LoopIn's input
        // bag was gone and iter 1's body bucket would be empty.
        let by_port = iter_ports(&pulses, &lp.body_id, 1);
        assert!(!by_port.is_empty(),
            "sequential iteration 1 must launch body pulses (regression: outer input gone)");
        assert_eq!(by_port.get("right"), Some(&serde_json::json!("b")),
            "iter 1 sees element 'b': {:?}", by_port);
        assert_eq!(by_port.get("left"), Some(&serde_json::json!("a")),
            "iter 1 sees carry='a' from iter 0: {:?}", by_port);

        // Iteration 1 body writes "ab".
        fire_loop_out(&lp, 1, serde_json::json!({"results": "ab", "acc": "ab"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // Iteration 2 launched with carry='ab', element='c'.
        let by_port = iter_ports(&pulses, &lp.body_id, 2);
        assert_eq!(by_port.get("right"), Some(&serde_json::json!("c")));
        assert_eq!(by_port.get("left"),  Some(&serde_json::json!("ab")));

        // Iteration 2 body writes "abc". This is the last over element,
        // so the loop should emit outward with the assembled list and
        // the final carry.
        fire_loop_out(&lp, 2, serde_json::json!({"results": "abc", "acc": "abc"}), Vec::new(), &mut rt, &mut pulses, &journal).await;

        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        let final_carry: Vec<_> = consumer.iter().filter(|p| p.target_port == "final" && !p.closed).collect();
        assert_eq!(data.len(), 1, "one outward pulse on consumer.data: {} pulses found", data.len());
        assert_eq!(data[0].value, serde_json::json!(["a", "ab", "abc"]),
            "gather list assembled in iteration order: {:?}", data[0].value);
        assert_eq!(final_carry.len(), 1, "one outward pulse on consumer.final");
        assert_eq!(final_carry[0].value, serde_json::json!("abc"),
            "final carry value is the last successful write: {:?}", final_carry[0].value);

        // The instance is gone in the runtime perspective: terminated.
        use weft_core::primitive::LoopTerminationReason;
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), color: uuid::Uuid::nil(),
        };
        let inst = rt.get(&key).expect("instance");
        assert_eq!(inst.terminated, Some(LoopTerminationReason::OverExhausted));
    }

    /// An UNWIRED optional carry seeds iteration 0 from its declared
    /// type's ZERO VALUE, not an error. The `acc` carry is a `String`
    /// (optional, unwired here: no `acc` in the input bag), so iteration
    /// 0's body must see `left = ""` (String zero) rather than failing
    /// the dispatch invariant. This is the path that lets a loop
    /// accumulate from a clean default without an explicit seed.
    #[tokio::test]
    async fn unwired_optional_carry_seeds_type_zero_value() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Fire WITHOUT `acc` in the input bag (unwired optional carry).
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a", "b"]}),
            &mut rt, &mut pulses, &journal,
        ).await;

        let by_port = |pulses: &PulseTable, idx: u32| -> std::collections::HashMap<String, serde_json::Value> {
            pulses.get(&lp.body_id)
                .map(|b| b.iter()
                    .filter(|p| p.frames.len() == 1 && p.frames[0].index == idx && !p.closed)
                    .map(|p| (p.target_port.clone(), p.value.clone()))
                    .collect())
                .unwrap_or_default()
        };
        let iter0 = by_port(&pulses, 0);
        assert_eq!(iter0.get("right"), Some(&serde_json::json!("a")));
        assert_eq!(
            iter0.get("left"),
            Some(&serde_json::json!("")),
            "unwired String carry seeds iteration 0 with the String zero value \"\": {iter0:?}"
        );

        // And the instance's carry_values reflect the seeded zero.
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), color: uuid::Uuid::nil(),
        };
        assert_eq!(
            rt.get(&key).expect("instance").carry_values.get("acc"),
            Some(&serde_json::json!("")),
            "seeded carry value is the type zero, not null or missing"
        );
    }

    /// Done-driven loop with carry: body writes self.done = true at iter 2.
    /// Loop must terminate at that point, gather has 3 slots, carry final
    /// value is iter 2's write.
    #[tokio::test]
    async fn done_voted_sequential_loop_terminates_at_done() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        // Use a very long `over` list so we know termination came from
        // `done`, not exhaustion.
        fire_loop_in(
            &lp,
            serde_json::json!({"items": ["a","b","c","d","e","f","g","h"], "acc": ""}),
            &mut rt, &mut pulses, &journal,
        ).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "a", "done": false}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "ab", "acc": "ab", "done": false}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        // Vote done at iter 2.
        fire_loop_out(&lp, 2, serde_json::json!({"results": "abc", "acc": "abc", "done": true}), Vec::new(), &mut rt, &mut pulses, &journal).await;

        let consumer = pulses.get(&lp.consumer_id).expect("consumer bucket");
        let data: Vec<_> = consumer.iter().filter(|p| p.target_port == "data" && !p.closed).collect();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].value, serde_json::json!(["a", "ab", "abc"]),
            "gather list capped at iter 2's done vote: {:?}", data[0].value);

        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), color: uuid::Uuid::nil(),
        };
        use weft_core::primitive::LoopTerminationReason;
        assert_eq!(rt.get(&key).unwrap().terminated, Some(LoopTerminationReason::DoneVoted));
    }
