    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;
    use weft_core::run::loop_runtime::{compute_loop_iter_cap, LoopConfig, LoopItemSource};
    use weft_core::run::ready::InputBag;
    use weft_core::frames::Frame;
    use weft_core::run::ready::ReadyGroup;
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
    use weft_journal::RunEvent;

    fn empty_project_dt() -> serde_json::Value {
        serde_json::json!("1970-01-01T00:00:00Z")
    }
    fn parse_dt() -> serde_json::Value {
        empty_project_dt()
    }

    #[derive(Default)]
    struct CapturingJournal {
        events: StdMutex<Vec<RunEvent>>,
    }
    #[async_trait]
    impl JournalClient for CapturingJournal {
        async fn record_event(&self, event: &RunEvent, _instance: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn record_page(&self, _run_id: RunId, _after: Option<i32>) -> anyhow::Result<Vec<weft_journal::JournalRow>> {
            unreachable!("the loop rig reads no other run")
        }
        fn keep_secret(&self, _value: serde_json::Value) -> weft_core::secret::SecretId {
            unreachable!("the loop rig makes no secrets")
        }
        async fn reveal_secret(&self, _id: weft_core::secret::SecretId) -> anyhow::Result<serde_json::Value> {
            unreachable!("the loop rig makes no secrets")
        }
    }

    fn primitive(p: WeftPrimitive) -> WeftType {
        WeftType::primitive(p)
    }

    /// A firing's bag from a JSON object, the shape readiness would
    /// hand the boundary.
    fn bag(v: serde_json::Value) -> InputBag {
        v.as_object()
            .expect("bag fixture is an object")
            .iter()
            .map(|(k, v)| (k.clone(), Arc::new(v.clone())))
            .collect()
    }

    /// The journal rows a producer feeding the LoopIn writes, then the
    /// LoopIn's own start: what a real run holds before the loop rows,
    /// and what the fold rebuilds the instance from.
    fn rows_before_loop_in(lp: &LoopProject, bag_json: &serde_json::Value) -> Vec<RunEvent> {
        let run_id = uuid::Uuid::nil();
        let mut rows = vec![RunEvent::NodeStarted {
            run_id, node_id: "producer".into(), frames: vec![], at_unix: 0,
        }];
        for (port, value) in bag_json.as_object().expect("bag fixture is an object") {
            rows.push(RunEvent::PortEmitted {
                run_id,
                emission_id: uuid::Uuid::new_v4(),
                node_id: "producer".into(),
                frames: vec![],
                port: port.clone(),
                value: Arc::new(value.clone()),
                at_unix: 0,
            });
        }
        rows.push(RunEvent::NodeCompleted { run_id, node_id: "producer".into(), frames: vec![], at_unix: 0 });
        rows.push(RunEvent::NodeStarted { run_id, node_id: lp.loop_in_id.clone(), frames: vec![], at_unix: 0 });
        rows
    }

    /// Rebuild the loop runtime the way a resumed worker does: fold
    /// the producer's rows, the LoopIn's start, and the captured loop
    /// rows over the program.
    fn rehydrate(lp: &LoopProject, bag_json: &serde_json::Value, loop_rows: &[RunEvent]) -> LoopRuntime {
        let mut events = rows_before_loop_in(lp, bag_json);
        events.extend(loop_rows.iter().cloned());
        let snap = weft_journal::fold_to_snapshot(uuid::Uuid::nil(), Arc::new(lp.project.clone()), &events);
        assert!(snap.corruptions.is_empty(), "{:?}", snap.corruptions);
        snap.loop_runtime
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
        let loop_in_id = weft_core::project::loop_in_id(&group_id);
        let loop_out_id = weft_core::project::loop_out_id(&group_id);
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
            type_params: Vec::new(),
            place_types: Vec::new(),
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
                declared_type: None,
            }]),
            outputs: vec![
                PortDefinition {
                    name: "items".into(),
                    port_type: primitive(WeftPrimitive::String),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                    declared_type: None,
                },
                PortDefinition {
                    name: "index".into(),
                    port_type: primitive(WeftPrimitive::Number),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                    declared_type: None,
                },
            ],
            features: Default::default(),
            requires_infra: false, per_instance: None, per_instance_paths: Default::default(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            images: vec![],
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };

        // LoopOut carries only the parent pointer: loop config
        // (parallel/over/carry/...) is authoritative on LoopIn and a
        // duplicated copy here would create two sources of truth.
        // Matches what the compiler now emits.
        let loop_out_cfg = serde_json::json!({"parentId": group_id});
        let loop_out = NodeDefinition {
            id: loop_out_id.clone(),
            node_type: "LoopOut".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
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
                    declared_type: None,
                },
                PortDefinition {
                    name: "done".into(),
                    port_type: primitive(WeftPrimitive::Boolean),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                    declared_type: None,
                },
            ]),
            outputs: vec![PortDefinition {
                name: "results".into(),
                port_type: list_of_nullable(primitive(WeftPrimitive::String)),
                required: false,
                description: None,
                synthesized_from_carry: false,
                declared_type: None,
            }],
            features: Default::default(),
            requires_infra: false, per_instance: None, per_instance_paths: Default::default(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            images: vec![],
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };

        // Body node: simple Echo with one input port `in: String` and
        // one output `out: String`.
        let body = NodeDefinition {
            id: body_id.clone(),
            node_type: "Echo".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
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
                declared_type: None,
            }]),
            outputs: vec![PortDefinition {
                name: "out".into(),
                port_type: primitive(WeftPrimitive::String),
                required: false,
                description: None,
                synthesized_from_carry: false,
                declared_type: None,
            }],
            features: Default::default(),
            requires_infra: false, per_instance: None, per_instance_paths: Default::default(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            images: vec![],
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };

        let consumer = NodeDefinition {
            id: consumer_id.clone(),
            node_type: "Sink".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
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
                declared_type: None,
            }]),
            outputs: vec![],
            features: Default::default(),
            requires_infra: false, per_instance: None, per_instance_paths: Default::default(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            images: vec![],
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };

        let edges = vec![
            Edge {
                id: "e1".into(),
                source: loop_in_id.clone(),
                source_handle: Some("items".into()),
                target: body_id.clone(),
                target_handle: Some("in".into()),
                path: Vec::new(),
                via: Vec::new(),
                span: None,
                source_file: None,
            },
            Edge {
                id: "e2".into(),
                source: body_id.clone(),
                source_handle: Some("out".into()),
                target: loop_out_id.clone(),
                target_handle: Some("results".into()),
                path: Vec::new(),
                via: Vec::new(),
                span: None,
                source_file: None,
            },
            Edge {
                id: "e3".into(),
                source: loop_out_id.clone(),
                source_handle: Some("results".into()),
                target: consumer_id.clone(),
                target_handle: Some("data".into()),
                path: Vec::new(),
                via: Vec::new(),
                span: None,
                source_file: None,
            },
        ];

        let (producer, edges) = producer_feeding(&loop_in, edges);
        let project_json = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": serde_json::to_value(vec![&producer, &loop_in, &loop_out, &body, &consumer]).unwrap(),
            "edges": serde_json::to_value(&edges).unwrap(),
            "groups": [{ "id": group_id, "kind": "loop", "loopConfig": {}, "nodeIds": [body_id] }],
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
        let program_idx = weft_core::project::ProgramIndex::build(&lp.project);
        let loop_in = lp.project.nodes.iter().find(|n| n.id == lp.loop_in_id).unwrap();
        let group = ReadyGroup {
            frames: Vec::new(),
            run_id: uuid::Uuid::nil(),
            received: weft_core::run::ready::FiringInput { input: bag(outer_input), ..Default::default() },
            skip: None,
            pulse_ids: Vec::new(),
            error: None,
        };
        // The rig asserts on pulses + runtime state; the stream runtime
        // is throwaway plumbing here (no stream loops in these
        // fixtures).
        let mut stream_rt =
            crate::stream_runtime::StreamRuntime::new(crate::wait_tracker::WaitTracker::new());
        handle_loop_boundary_firing(
            loop_in, &group, &lp.project, &program_idx, pulses, journal,
            "test-instance", rt, &mut stream_rt, &mut weft_core::primitive::KickTable::new(), &mut ScopeTable::default(),
        )
        .await
        .expect("LoopIn firing");
    }

    /// Helper: fire LoopOut for iteration `i` with the given writes.
    /// Journals what the real driver journals around the handler: the
    /// body's writes as the body's emissions, then the LoopOut's own
    /// start and completion, so the captured rows fold the way a real
    /// run's do (the fold reads the LoopOut's writes off its firing's
    /// absorbed pulses).
    async fn fire_loop_out(
        lp: &LoopProject,
        iter: u32,
        writes: serde_json::Value,
        closed_ports: Vec<String>,
        rt: &mut LoopRuntime,
        pulses: &mut PulseTable,
        journal: &CapturingJournal,
    ) {
        let run_id = uuid::Uuid::nil();
        let frames = vec![Frame::Loop { index: iter }];
        let program_idx = weft_core::project::ProgramIndex::build(&lp.project);
        let loop_out = lp.project.nodes.iter().find(|n| n.id == lp.loop_out_id).unwrap();
        // The body firings that produced these writes: `writes` is
        // keyed by LoopOut input port, so each write is journaled as
        // the emission of the body port wired into it, then each body
        // node's completion (which closes the ports it did not write,
        // the `closed_ports` the LoopOut sees).
        let mut by_source: std::collections::BTreeMap<String, Vec<(String, serde_json::Value)>> =
            Default::default();
        for (port, value) in writes.as_object().expect("writes fixture is an object") {
            // Every write reaches the LoopOut down a wire, as in a real
            // run: a fixture writing an unwired port is a fixture bug.
            let edge = lp
                .project
                .edges
                .iter()
                .find(|e| e.target == lp.loop_out_id && e.target_handle.as_deref() == Some(port))
                .unwrap_or_else(|| panic!("the rig project wires no body port into LoopOut '{port}'"));
            by_source
                .entry(edge.source.clone())
                .or_default()
                .push((edge.source_handle.clone().expect("edge names its source port"), value.clone()));
        }
        for (source, emitted) in by_source {
            journal.record_event(&RunEvent::NodeStarted { run_id, node_id: source.clone(), frames: frames.clone(), at_unix: 0 }, None).await.unwrap();
            let emission = uuid::Uuid::new_v4();
            for (port, value) in emitted {
                journal
                    .record_event(
                        &RunEvent::PortEmitted {
                            run_id,
                            emission_id: emission,
                            node_id: source.clone(),
                            frames: frames.clone(),
                            port,
                            value: Arc::new(value),
                            at_unix: 0,
                        },
                        None,
                    )
                    .await
                    .unwrap();
            }
            journal.record_event(&RunEvent::NodeCompleted { run_id, node_id: source, frames: frames.clone(), at_unix: 0 }, None).await.unwrap();
        }
        journal.record_event(&RunEvent::NodeStarted { run_id, node_id: lp.loop_out_id.clone(), frames: frames.clone(), at_unix: 0 }, None).await.unwrap();
        let group = ReadyGroup {
            frames,
            run_id,
            received: weft_core::run::ready::FiringInput { input: bag(writes), closed_ports, ..Default::default() },
            skip: None,
            pulse_ids: Vec::new(),
            error: None,
        };
        let mut stream_rt =
            crate::stream_runtime::StreamRuntime::new(crate::wait_tracker::WaitTracker::new());
        handle_loop_boundary_firing(
            loop_out, &group, &lp.project, &program_idx, pulses, journal,
            "test-instance", rt, &mut stream_rt, &mut weft_core::primitive::KickTable::new(), &mut ScopeTable::default(),
        )
        .await
        .expect("LoopOut firing");
        journal
            .record_event(&RunEvent::NodeCompleted { run_id, node_id: lp.loop_out_id.clone(), frames: vec![Frame::Loop { index: iter }], at_unix: 0 }, None)
            .await
            .unwrap();
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
        let frames: Vec<u32> = on_in.iter().map(|p| p.frames[0].loop_index().expect("loop frame")).collect();
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
        let instantiations: Vec<_> = events.iter().filter(|e| matches!(e, RunEvent::LoopInstantiated { .. })).collect();
        let launches: Vec<_> = events.iter().filter(|e| matches!(e, RunEvent::LoopIterationLaunched { .. })).collect();
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
        assert_eq!(*data[0].value, serde_json::json!(["A", "B", "C"]),
            "assembled in iteration-index order: {:?}", data[0].value);
        let events = journal.events.lock().unwrap();
        let terminated: Vec<_> = events.iter().filter(|e| matches!(e, RunEvent::LoopTerminated { .. })).collect();
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
        let events: Vec<RunEvent> = journal
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| match e {
                RunEvent::LoopIterationLaunched { .. } => {
                    kept += 1;
                    kept <= 1
                }
                _ => true,
            })
            .cloned()
            .collect();
        let mut rt2 = rehydrate(&lp, &bag, &events);
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        let body = pulses2.get(&lp.body_id).expect("body bucket");
        let mut frames: Vec<u32> = body
            .iter()
            .filter(|p| p.target_port == "in" && !p.closed)
            .map(|p| p.frames[0].loop_index().expect("loop frame"))
            .collect();
        frames.sort();
        assert_eq!(frames, vec![1, 2], "only the missing iterations relaunch");
        let events2 = journal2.events.lock().unwrap();
        let launches = events2.iter().filter(|e| matches!(e, RunEvent::LoopIterationLaunched { .. })).count();
        assert_eq!(launches, 2, "one launch row per relaunched iteration");
        let instantiations = events2.iter().filter(|e| matches!(e, RunEvent::LoopInstantiated { .. })).count();
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
        let events: Vec<RunEvent> = journal.events.lock().unwrap().clone();
        let mut rt2 = rehydrate(&lp, &bag, &events);
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        assert_eq!(
            pulses2.get(&lp.consumer_id).map(|b| b.len()).unwrap_or(0),
            0,
            "replay on a terminated instance emits nothing"
        );
        let events2 = journal2.events.lock().unwrap();
        let terminated = events2.iter().filter(|e| matches!(e, RunEvent::LoopTerminated { .. })).count();
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
                evs.iter().filter(|e| matches!(e, RunEvent::LoopOutFired { .. })).count(),
                evs.iter().filter(|e| matches!(e, RunEvent::LoopTerminated { .. })).count(),
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
        assert_eq!(*data[0].value, serde_json::json!(["A", null, "C"]),
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
        assert!(events.iter().any(|e| matches!(e, RunEvent::LoopTerminated { .. })),
            "zero-iteration loop terminates outwardly");
    }

    /// Termination reason on the zero-iteration shortcut. When the
    /// user writes `maxIters: 0`, the binding constraint is
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
                    obj.insert("maxIters".into(), serde_json::json!(0));
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
            RunEvent::LoopTerminated { reason, .. } => Some(*reason),
            _ => None,
        }).expect("loop terminated");
        assert_eq!(
            term, LoopTerminationReason::MaxItersReached,
            "maxIters=0 is the binding constraint, not over-exhausted"
        );
    }

    /// Layer-3 rig 6: cancellation marks every live LoopInstance for the
    /// run as cancelled AND emits closures on the LoopOut's outward
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
        let program_idx = weft_core::project::ProgramIndex::build(&lp.project);
        cancel_loop_instances(
            &mut rt,
            uuid::Uuid::nil(),
            &lp.project,
            &program_idx,
            &mut pulses,
            &journal,
            "test-instance",
        )
        .await;
        // Instance is now terminated::Cancelled.
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(),
            parent_frames: Vec::new(),
            run_id: uuid::Uuid::nil(),
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
        // Rebuild project with maxIters=2 on the loop config.
        let mut lp = build_parallel_map_project();
        for n in lp.project.nodes.iter_mut() {
            if matches!(n.node_type.as_str(), "LoopIn" | "LoopOut") {
                if let Some(obj) = n.config.as_object_mut() {
                    obj.insert("maxIters".into(), serde_json::json!(2));
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
        assert_eq!(on_in.len(), 2, "maxIters=2 caps body firings to 2: got {}", on_in.len());
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
        assert_eq!(*data[0].value, serde_json::json!(["A", "B", "C"]),
            "BTreeMap-driven assembly preserves input order: {:?}", data[0].value);
    }

    /// Layer-3 rig 9: compute_loop_iter_cap zip-trim behavior with two
    /// `over` ports of different lengths.
    #[test]
    fn compute_iter_cap_trims_to_shortest_with_trim_on() {
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into(), "b".into()],
            carry: vec![],
            max_iters: None,
            trim_on_mismatch: true,
        };
        let input = bag(serde_json::json!({"a": [1, 2, 3, 4, 5], "b": [10, 20, 30]}));
        let count = compute_loop_iter_cap(&cfg, &input).expect("ok");
        assert_eq!(count, 3, "trims to shortest: {count}");
    }

    /// Layer-3 rig 10: compute_loop_iter_cap refuses a length mismatch
    /// with trim_on_mismatch=false (a loud Err, no silent trim).
    #[test]
    fn compute_iter_cap_rejects_mismatch_with_trim_off() {
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into(), "b".into()],
            carry: vec![],
            max_iters: None,
            trim_on_mismatch: false,
        };
        let input = bag(serde_json::json!({"a": [1, 2, 3], "b": [10, 20]}));
        let err = compute_loop_iter_cap(&cfg, &input).expect_err("must err on mismatch");
        assert!(err.contains("mismatch"), "loud mismatch error: {err}");
    }

    /// Layer-3 rig 11: max_iters cap applies in compute_iter_cap.
    #[test]
    fn compute_iter_cap_caps_at_max_iters() {
        let cfg = LoopConfig {
            parallel: true,
            over: vec!["a".into()],
            carry: vec![],
            max_iters: Some(2),
            trim_on_mismatch: true,
        };
        let input = bag(serde_json::json!({"a": [1, 2, 3, 4, 5]}));
        let count = compute_loop_iter_cap(&cfg, &input).expect("ok");
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
        // a pulse from source_port=index. But this fires through
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
            RunEvent::LoopIterationLaunched { index, .. } => Some(*index),
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
        // (group_id, parent_frames, run); two distinct parent_frames
        // mean two distinct instances even for the same group_id.
        let key_outer = LoopInstanceKey {
            group_id: lp.group_id.clone(),
            parent_frames: Vec::new(),
            run_id: uuid::Uuid::nil(),
        };
        let key_inner_iter0 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![Frame::Loop { index: 0 }],
            run_id: uuid::Uuid::nil(),
        };
        let key_inner_iter1 = LoopInstanceKey {
            group_id: "inner".into(),
            parent_frames: vec![Frame::Loop { index: 1 }],
            run_id: uuid::Uuid::nil(),
        };
        rt.ensure(key_inner_iter0.clone(), LoopConfig {
            parallel: false, over: vec![], carry: vec![], max_iters: Some(1), trim_on_mismatch: true,
        }, LoopItemSource::DoneDriven, Some(1), vec![]);
        rt.ensure(key_inner_iter1.clone(), LoopConfig {
            parallel: false, over: vec![], carry: vec![], max_iters: Some(1), trim_on_mismatch: true,
        }, LoopItemSource::DoneDriven, Some(1), vec![]);
        assert!(rt.get(&key_outer).is_some(), "outer instance lives");
        assert!(rt.get(&key_inner_iter0).is_some(), "inner instance at outer iter 0 lives");
        assert!(rt.get(&key_inner_iter1).is_some(), "inner instance at outer iter 1 lives");
        // Distinct: cancelling one does not affect the other.
        rt.cancel_inside(&vec![Frame::Loop { index: 0 }], uuid::Uuid::nil());
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
    /// The producer that feeds a loop's outer inputs (one output per
    /// LoopIn input, wired across), so a journal of the loop's rows
    /// folds the way a real run's does: the LoopIn's bag is its
    /// absorbed pulses. Returns the node and `edges` with the wires
    /// added.
    fn producer_feeding(loop_in: &NodeDefinition, mut edges: Vec<Edge>) -> (NodeDefinition, Vec<Edge>) {
        let producer = NodeDefinition {
            id: "producer".into(),
            node_type: "Producer".into(),
            type_params: Vec::new(),
            place_types: Vec::new(),
            label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: None,
            inputs: inputs_of(vec![]),
            outputs: loop_in
                .inputs
                .iter()
                .map(|i| PortDefinition {
                    name: i.name.clone(),
                    port_type: i.port_type.clone(),
                    required: false,
                    description: None,
                    synthesized_from_carry: false,
                    declared_type: None,
                })
                .collect(),
            features: Default::default(),
            requires_infra: false, per_instance: None, per_instance_paths: Default::default(),
            fires_with: Default::default(), baked_outputs: Default::default(),
            images: vec![],
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None,
            header_span: None,
            config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            port_literal_types: Default::default(),
            metadata_ports: Vec::new(),
            file_refs: Default::default(),
            include_path: None,
            include_contents: None,
            source_file: None,
        };
        for port in &producer.outputs {
            edges.push(Edge {
                id: format!("producer-{}", port.name),
                source: "producer".into(),
                source_handle: Some(port.name.clone()),
                target: loop_in.id.clone(),
                target_handle: Some(port.name.clone()),
                path: Vec::new(),
                via: Vec::new(),
                span: None,
                source_file: None,
            });
        }
        (producer, edges)
    }

    fn build_sequential_fold_project() -> LoopProject {
        let group_id = "fold".to_string();
        let loop_in_id = weft_core::project::loop_in_id(&group_id);
        let loop_out_id = weft_core::project::loop_out_id(&group_id);
        let body_id = "body".to_string();
        let consumer_id = "consumer".to_string();
        let loop_cfg = serde_json::json!({
            "parentId": group_id,
            "parallel": false,
            "over": ["items"],
            "carry": ["acc"],
        });
        let loop_in = NodeDefinition {
            id: loop_in_id.clone(), node_type: "LoopIn".into(), type_params: Vec::new(), place_types: Vec::new(), label: None,
            config: loop_cfg.clone(), position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::In }),
            inputs: inputs_of(vec![
                PortDefinition { name: "items".into(), port_type: list_of(primitive(WeftPrimitive::String)), required: true, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "acc".into(),   port_type: primitive(WeftPrimitive::String),         required: false, description: None, synthesized_from_carry: false, declared_type: None },
            ]),
            outputs: vec![
                PortDefinition { name: "items".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "acc".into(),   port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "index".into(), port_type: primitive(WeftPrimitive::Number), required: false, description: None, synthesized_from_carry: false, declared_type: None },
            ],
            features: Default::default(), requires_infra: false, per_instance: None, per_instance_paths: Default::default(), images: vec![], fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None, header_span: None, config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(), port_literal_types: Default::default(), metadata_ports: Vec::new(),
            file_refs: Default::default(), include_path: None, include_contents: None, source_file: None,
        };
        // LoopOut carries only `{"parentId": ...}` (matches compiler).
        let loop_out_cfg = serde_json::json!({"parentId": group_id});
        let loop_out = NodeDefinition {
            id: loop_out_id.clone(), node_type: "LoopOut".into(), type_params: Vec::new(), place_types: Vec::new(), label: None,
            config: loop_out_cfg, position: Position { x: 0.0, y: 0.0 },
            scope: vec![],
            group_boundary: Some(GroupBoundary { group_id: group_id.clone(), role: GroupBoundaryRole::Out }),
            inputs: inputs_of(vec![
                PortDefinition { name: "results".into(), port_type: primitive(WeftPrimitive::String),  required: false, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "acc".into(),     port_type: primitive(WeftPrimitive::String),  required: false, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "done".into(),    port_type: primitive(WeftPrimitive::Boolean), required: false, description: None, synthesized_from_carry: false, declared_type: None },
            ]),
            outputs: vec![
                PortDefinition { name: "results".into(), port_type: list_of_nullable(primitive(WeftPrimitive::String)), required: false, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "acc".into(),     port_type: primitive(WeftPrimitive::String),                   required: false, description: None, synthesized_from_carry: false, declared_type: None },
            ],
            features: Default::default(), requires_infra: false, per_instance: None, per_instance_paths: Default::default(), images: vec![], fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None, header_span: None, config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(), port_literal_types: Default::default(), metadata_ports: Vec::new(),
            file_refs: Default::default(), include_path: None, include_contents: None, source_file: None,
        };
        let body = NodeDefinition {
            id: body_id.clone(), node_type: "Concat".into(), type_params: Vec::new(), place_types: Vec::new(), label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 },
            scope: vec![group_id.clone()], group_boundary: None,
            inputs: inputs_of(vec![
                PortDefinition { name: "left".into(),  port_type: primitive(WeftPrimitive::String), required: true, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "right".into(), port_type: primitive(WeftPrimitive::String), required: true, description: None, synthesized_from_carry: false, declared_type: None },
            ]),
            outputs: vec![
                PortDefinition { name: "out".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false, declared_type: None },
                // The carry write leaves on its own port, so a test can
                // hand the gather and the carry different values and
                // tell the two wires apart on the way back.
                PortDefinition { name: "acc".into(), port_type: primitive(WeftPrimitive::String), required: false, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "done".into(), port_type: primitive(WeftPrimitive::Boolean), required: false, description: None, synthesized_from_carry: false, declared_type: None },
            ],
            features: Default::default(), requires_infra: false, per_instance: None, per_instance_paths: Default::default(), images: vec![], fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None, header_span: None, config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(), port_literal_types: Default::default(), metadata_ports: Vec::new(),
            file_refs: Default::default(), include_path: None, include_contents: None, source_file: None,
        };
        let consumer = NodeDefinition {
            id: consumer_id.clone(), node_type: "Sink".into(), type_params: Vec::new(), place_types: Vec::new(), label: None,
            config: serde_json::Value::Object(Default::default()),
            position: Position { x: 0.0, y: 0.0 }, scope: vec![], group_boundary: None,
            inputs: inputs_of(vec![
                PortDefinition { name: "data".into(),  port_type: list_of_nullable(primitive(WeftPrimitive::String)), required: true, description: None, synthesized_from_carry: false, declared_type: None },
                PortDefinition { name: "final".into(), port_type: primitive(WeftPrimitive::String),                    required: true, description: None, synthesized_from_carry: false, declared_type: None },
            ]),
            outputs: vec![], features: Default::default(), requires_infra: false, per_instance: None, per_instance_paths: Default::default(), images: vec![], fires_with: Default::default(), baked_outputs: Default::default(),
            published_service: None,
            instance_service: None,
            instance_rules: None,
            span: None, header_span: None, config_spans: Default::default(),
            optional_ports: Default::default(),
            port_literals: Default::default(), port_literal_spans: Default::default(), port_literal_types: Default::default(), metadata_ports: Vec::new(),
            file_refs: Default::default(), include_path: None, include_contents: None, source_file: None,
        };
        let edges = vec![
            // body reads element + carry from LoopIn.
            Edge { id: "e1".into(), source: loop_in_id.clone(),  source_handle: Some("items".into()), target: body_id.clone(),     target_handle: Some("right".into()), path: Vec::new(), via: Vec::new(), span: None, source_file: None },
            Edge { id: "e2".into(), source: loop_in_id.clone(),  source_handle: Some("acc".into()),   target: body_id.clone(),     target_handle: Some("left".into()),  path: Vec::new(), via: Vec::new(), span: None, source_file: None },
            // body writes back to LoopOut on both results and acc.
            Edge { id: "e3".into(), source: body_id.clone(),     source_handle: Some("out".into()),   target: loop_out_id.clone(), target_handle: Some("results".into()), path: Vec::new(), via: Vec::new(), span: None, source_file: None },
            Edge { id: "e4".into(), source: body_id.clone(),     source_handle: Some("acc".into()),   target: loop_out_id.clone(), target_handle: Some("acc".into()),     path: Vec::new(), via: Vec::new(), span: None, source_file: None },
            // and its done vote.
            Edge { id: "e7".into(), source: body_id.clone(),     source_handle: Some("done".into()),  target: loop_out_id.clone(), target_handle: Some("done".into()),    path: Vec::new(), via: Vec::new(), span: None, source_file: None },
            // outward to consumer.
            Edge { id: "e5".into(), source: loop_out_id.clone(), source_handle: Some("results".into()), target: consumer_id.clone(), target_handle: Some("data".into()),  path: Vec::new(), via: Vec::new(), span: None, source_file: None },
            Edge { id: "e6".into(), source: loop_out_id.clone(), source_handle: Some("acc".into()),     target: consumer_id.clone(), target_handle: Some("final".into()), path: Vec::new(), via: Vec::new(), span: None, source_file: None },
        ];
        let (producer, edges) = producer_feeding(&loop_in, edges);
        let project_json = serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000000",
            "nodes": serde_json::to_value(vec![&producer, &loop_in, &loop_out, &body, &consumer]).unwrap(),
            "edges": serde_json::to_value(&edges).unwrap(),
            "groups": [{ "id": group_id, "kind": "loop", "loopConfig": {}, "nodeIds": [body_id] }],
            "createdAt": parse_dt(), "updatedAt": parse_dt(),
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
        fn iter_ports(
            pulses: &PulseTable, body_id: &str, idx: u32,
        ) -> std::collections::HashMap<String, serde_json::Value> {
            pulses.get(body_id)
                .map(|b| b.iter()
                    .filter(|p| p.frames.len() == 1 && p.frames[0].loop_index().expect("loop frame") == idx && !p.closed)
                    .map(|p| (p.target_port.clone(), (*p.value).clone()))
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
        assert_eq!(*data[0].value, serde_json::json!(["a", "ab", "abc"]),
            "gather list assembled in iteration order: {:?}", data[0].value);
        assert_eq!(final_carry.len(), 1, "one outward pulse on consumer.final");
        assert_eq!(*final_carry[0].value, serde_json::json!("abc"),
            "final carry value is the last successful write: {:?}", final_carry[0].value);

        // The instance is gone in the runtime perspective: terminated.
        use weft_core::primitive::LoopTerminationReason;
        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), run_id: uuid::Uuid::nil(),
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
                    .filter(|p| p.frames.len() == 1 && p.frames[0].loop_index().expect("loop frame") == idx && !p.closed)
                    .map(|p| (p.target_port.clone(), (*p.value).clone()))
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
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), run_id: uuid::Uuid::nil(),
        };
        assert_eq!(
            rt.get(&key).expect("instance").carry_values.get("acc").map(|v| &**v),
            Some(&serde_json::json!("")),
            "seeded carry value is the type zero, not null or missing"
        );
    }

    /// The worker dies mid-loop, two iterations gathered and the third
    /// launched: the fold rebuilds the writes and the carry off the
    /// LoopOut firings' pulses, and the re-fired LoopIn launches nothing
    /// twice.
    #[tokio::test]
    async fn a_loop_resumed_mid_gather_rebuilds_its_writes_and_relaunches_nothing() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        let bag = serde_json::json!({"items": ["a", "b", "c"], "acc": ""});
        fire_loop_in(&lp, bag.clone(), &mut rt, &mut pulses, &journal).await;
        // The gather and the carry get DIFFERENT values, so the rebuilt
        // instance proves each wire was read, not a coincidence.
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "A"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 1, serde_json::json!({"results": "ab", "acc": "AB"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let events: Vec<RunEvent> = journal.events.lock().unwrap().clone();
        // The worker died here: two iterations gathered, the third
        // launched and not yet fired.
        let key = LoopInstanceKey { group_id: lp.group_id.clone(), parent_frames: Vec::new(), run_id: uuid::Uuid::nil() };
        let rt2 = rehydrate(&lp, &bag, &events);
        let inst = rt2.get(&key).expect("the instance is rebuilt");
        assert_eq!(inst.out_fired, weft_core::run::loop_runtime::Indices::from([0, 1]));
        assert_eq!(inst.launched, weft_core::run::loop_runtime::Indices::from([0, 1, 2]));
        assert_eq!(*inst.carry_values["acc"], serde_json::json!("AB"), "the carry is the last write");
        assert_eq!(inst.gather_lists["results"].len(), 2, "both gathered writes are back");
        assert_eq!(inst.gather_lists["results"][&0], weft_core::run::loop_runtime::LoopWrite::Value(Arc::new(serde_json::json!("a"))));
        assert_eq!(inst.gather_lists["results"][&1], weft_core::run::loop_runtime::LoopWrite::Value(Arc::new(serde_json::json!("ab"))));
        assert!(inst.terminated.is_none());
        // The re-fired LoopIn finds every iteration launched and
        // launches nothing more.
        let mut rt2 = rt2;
        let mut pulses2 = PulseTable::default();
        let journal2 = CapturingJournal::default();
        fire_loop_in(&lp, bag, &mut rt2, &mut pulses2, &journal2).await;
        assert!(pulses2.get(&lp.body_id).map(|b| b.is_empty()).unwrap_or(true), "nothing relaunched: {pulses2:?}");
        assert_eq!(rt2.get(&key).unwrap().launched, weft_core::run::loop_runtime::Indices::from([0, 1, 2]));
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
        assert_eq!(*data[0].value, serde_json::json!(["a", "ab", "abc"]),
            "gather list capped at iter 2's done vote: {:?}", data[0].value);

        let key = LoopInstanceKey {
            group_id: lp.group_id.clone(), parent_frames: Vec::new(), run_id: uuid::Uuid::nil(),
        };
        use weft_core::primitive::LoopTerminationReason;
        assert_eq!(rt.get(&key).unwrap().terminated, Some(LoopTerminationReason::DoneVoted));
    }

    /// Fire LoopOut for iteration `iter` with `acc` closed (with
    /// `failure` as the closure's error when set) and a gathered value,
    /// returning the handler's outcome.
    async fn fire_loop_out_with_closed_carry(
        lp: &LoopProject,
        iter: u32,
        failure: Option<&str>,
        rt: &mut LoopRuntime,
        pulses: &mut PulseTable,
        journal: &CapturingJournal,
    ) -> Result<(), String> {
        let program_idx = weft_core::project::ProgramIndex::build(&lp.project);
        let loop_out = lp.project.nodes.iter().find(|n| n.id == lp.loop_out_id).unwrap();
        let mut received = weft_core::run::ready::FiringInput {
            input: bag(serde_json::json!({"results": "r"})),
            closed_ports: vec!["acc".to_string()],
            ..Default::default()
        };
        if let Some(error) = failure {
            received.closed_failures.insert(
                "acc".to_string(),
                weft_core::pulse::Failure { node: "ask".into(), error: error.to_string() },
            );
        }
        let group = ReadyGroup {
            frames: vec![Frame::Loop { index: iter }],
            run_id: uuid::Uuid::nil(),
            received,
            skip: None,
            pulse_ids: Vec::new(),
            error: None,
        };
        let mut stream_rt =
            crate::stream_runtime::StreamRuntime::new(crate::wait_tracker::WaitTracker::new());
        handle_loop_boundary_firing(
            loop_out, &group, &lp.project, &program_idx, pulses, journal,
            "test-instance", rt, &mut stream_rt, &mut weft_core::primitive::KickTable::new(), &mut ScopeTable::default(),
        )
        .await
    }

    /// A carried value closed because a node inside the loop failed
    /// stops the loop: the LoopOut firing fails (the driver then tears
    /// the loop down as Failed) with the iteration, the port and the
    /// error, and no next iteration launches from the stale carry.
    #[tokio::test]
    async fn carry_closed_by_a_failure_fails_the_loop() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(&lp, serde_json::json!({"items": ["a", "b", "c"], "acc": ""}), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "a"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        let err = fire_loop_out_with_closed_carry(&lp, 1, Some("the model answered 500"), &mut rt, &mut pulses, &journal)
            .await
            .expect_err("a failed carry fails the loop");
        assert!(err.contains("iteration 1") && err.contains("'acc'"), "{err}");
        assert!(err.contains("because 'ask' failed: the model answered 500"), "names the node that broke: {err}");
        let key = LoopInstanceKey { group_id: lp.group_id.clone(), parent_frames: Vec::new(), run_id: uuid::Uuid::nil() };
        assert_eq!(rt.get(&key).unwrap().launched, weft_core::run::loop_runtime::Indices::from([0, 1]), "iteration 2 never launches");
    }

    /// A carried value closed with no failure (a branch not taken, a
    /// gate) keeps its previous value and the loop goes on.
    #[tokio::test]
    async fn carry_closed_by_a_gate_keeps_its_value() {
        let lp = build_sequential_fold_project();
        let mut rt = LoopRuntime::new();
        let mut pulses = PulseTable::default();
        let journal = CapturingJournal::default();
        fire_loop_in(&lp, serde_json::json!({"items": ["a", "b", "c"], "acc": ""}), &mut rt, &mut pulses, &journal).await;
        fire_loop_out(&lp, 0, serde_json::json!({"results": "a", "acc": "a"}), Vec::new(), &mut rt, &mut pulses, &journal).await;
        fire_loop_out_with_closed_carry(&lp, 1, None, &mut rt, &mut pulses, &journal).await.expect("a plain closure is fine");
        let carry_at_2: Vec<_> = pulses[&lp.body_id]
            .iter()
            .filter(|p| p.frames == vec![Frame::Loop { index: 2 }] && p.target_port == "left" && !p.closed)
            .map(|p| (*p.value).clone())
            .collect();
        assert_eq!(carry_at_2, vec![serde_json::json!("a")], "iteration 2 starts from the kept carry");
    }

    // A loop run through the real run loop, from the kicks a run's start
    // makes (`KickPlan::for_program`) to its terminal: what a loop with no
    // output port needs, an iteration that ends with nothing written into
    // its `LoopOut`, only exists once a whole run runs.

    /// Emits the list the test gives it, for the loop to walk.
    struct Lister {
        items: serde_json::Value,
    }
    super::engine_test_rig::test_manifest!(Lister, "Lister");
    #[async_trait]
    impl weft_core::Node for Lister {
        async fn run(&self, ctx: weft_core::RunContext) -> weft_core::error::WeftResult<()> {
            ctx.pulse_downstream(weft_core::node::NodeOutput::new().set("items", self.items.clone())).await
        }
    }

    /// The loop's body: writes down each value it is handed and passes it on.
    struct Tally {
        seen: Arc<StdMutex<Vec<serde_json::Value>>>,
    }
    super::engine_test_rig::test_manifest!(Tally, "Tally");
    #[async_trait]
    impl weft_core::Node for Tally {
        async fn run(&self, ctx: weft_core::RunContext) -> weft_core::error::WeftResult<()> {
            let value: serde_json::Value = ctx.inputs.get("value")?;
            self.seen.lock().unwrap().push(value.clone());
            ctx.pulse_downstream(weft_core::node::NodeOutput::new().set("value", value)).await
        }
    }

    /// `lister.items -> l (Loop over items) -> ...`, the body `tally` fed
    /// each item. With `gathers`, `tally.value` is written into the loop's
    /// `results` and the gathered list reaches `sink`; without, the loop
    /// has no output port and nothing writes into its `LoopOut`.
    fn walked_loop_project(parallel: bool, gathers: bool) -> ProjectDefinition {
        let loop_config = serde_json::json!({ "parallel": parallel, "over": ["items"], "carry": [] });
        let mut loop_out_inputs = vec![serde_json::json!({ "name": "done", "portType": "Boolean", "required": false, "optional": true })];
        let mut loop_out_outputs = Vec::new();
        let mut nodes = vec![
            serde_json::json!({
                "id": "lister", "nodeType": "Lister", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [], "outputs": [{ "name": "items", "portType": "List[Number]", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }),
            serde_json::json!({
                "id": "l__in", "nodeType": "LoopIn", "label": null,
                "config": { "parentId": "l", "parallel": parallel, "over": ["items"], "carry": [] },
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": [{ "name": "items", "portType": "List[Number]", "required": true }],
                "outputs": [
                    { "name": "items", "portType": "Number", "required": false },
                    { "name": "index", "portType": "Number", "required": false }
                ],
                "features": {}, "scope": [], "requiresInfra": false, "images": [],
                "groupBoundary": { "groupId": "l", "role": "In" }
            }),
            serde_json::json!({
                "id": "tally", "nodeType": "Tally", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [{ "name": "value", "portType": "Number", "required": true }],
                "outputs": [{ "name": "value", "portType": "Number", "required": false }],
                "features": {}, "scope": ["l"], "groupBoundary": null, "requiresInfra": false, "images": []
            }),
        ];
        let mut edges = vec![
            serde_json::json!({ "id": "e1", "source": "lister", "sourceHandle": "items", "target": "l__in", "targetHandle": "items" }),
            serde_json::json!({ "id": "e2", "source": "l__in", "sourceHandle": "items", "target": "tally", "targetHandle": "value" }),
        ];
        if gathers {
            loop_out_inputs.insert(0, serde_json::json!({ "name": "results", "portType": "Number", "required": false, "optional": true }));
            loop_out_outputs.push(serde_json::json!({ "name": "results", "portType": "List[Number | Null]", "required": true }));
            nodes.push(serde_json::json!({
                "id": "sink", "nodeType": "Tally", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [{ "name": "value", "portType": "List[Number | Null]", "required": true }],
                "outputs": [{ "name": "value", "portType": "List[Number | Null]", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }));
            edges.push(serde_json::json!({ "id": "e3", "source": "tally", "sourceHandle": "value", "target": "l__out", "targetHandle": "results" }));
            edges.push(serde_json::json!({ "id": "e4", "source": "l__out", "sourceHandle": "results", "target": "sink", "targetHandle": "value" }));
        }
        nodes.push(serde_json::json!({
            "id": "l__out", "nodeType": "LoopOut", "label": null,
            "config": { "parentId": "l" }, "position": { "x": 0.0, "y": 0.0 },
            "inputs": loop_out_inputs, "outputs": loop_out_outputs,
            "features": {}, "scope": [], "requiresInfra": false, "images": [],
            "groupBoundary": { "groupId": "l", "role": "Out" }
        }));
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": nodes,
            "edges": edges,
            "groups": [{ "id": "l", "kind": "loop", "nodeIds": ["tally"], "loopConfig": loop_config }],
            "createdAt": parse_dt(),
            "updatedAt": parse_dt(),
        }))
        .expect("walked loop project")
    }

    /// A run's birth rows: its start, and a kick for every root its start
    /// kicks, as the dispatcher writes them.
    fn born(project: &ProjectDefinition, run_id: RunId) -> Vec<RunEvent> {
        let mut rows = vec![RunEvent::RunStarted { given: Default::default(),
            run_id, project_id: project.id, entry_node: "lister".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: Some("test-hash".into()), cut_from: None, binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            seed: None, instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }];
        rows.extend(weft_core::run_spec::KickPlan::for_program(project, None, None).into_iter().map(|kick| RunEvent::NodeKicked {
            run_id, node_id: kick.node, frames: kick.frames, firing: false, payload: None, port_snapshot: None, at_unix: 0,
        }));
        rows
    }

    /// Drive a run of `project` over `rows` (its birth rows, or a resume's
    /// whole journal so far) with a lister handing `items`: the worker's
    /// tables, every row the journal holds after, and what `tally` saw.
    async fn walk(
        project: &ProjectDefinition,
        items: serde_json::Value,
        run_id: RunId,
        rows: Vec<RunEvent>,
    ) -> (Drove, Vec<RunEvent>, Vec<serde_json::Value>) {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let cat = super::engine_test_rig::catalog(vec![
            ("Lister", Box::new(Lister { items })),
            ("Tally", Box::new(Tally { seen: seen.clone() })),
        ]);
        let (drove, events) = super::engine_test_rig::drive_journal_observed(project.clone(), cat, run_id, rows, CancellationFlag::new_arc()).await;
        let drove = drove.expect("the loop run drives");
        super::engine_test_rig::assert_fold_matches_live(project, &events, Default::default(), &drove);
        let seen = seen.lock().unwrap().clone();
        (drove, events, seen)
    }

    /// The frames each of `node`'s firings completed at, sorted.
    fn completed_at(events: &[RunEvent], node: &str) -> Vec<Vec<u32>> {
        let mut at: Vec<Vec<u32>> = events.iter().filter_map(|event| match event {
            RunEvent::NodeCompleted { node_id, frames, .. } if node_id == node => Some(weft_core::frames::loop_indices(frames)),
            _ => None,
        }).collect();
        at.sort();
        at
    }

    weft_core::stress_test! {
        name: a_loop_with_no_output_ends_over_an_empty_list_and_over_items,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            for parallel in [false, true] {
                let project = walked_loop_project(parallel, false);
                assert!(
                    !born(&project, uuid::Uuid::nil()).iter().any(|row| matches!(row, RunEvent::NodeKicked { node_id, .. } if node_id == "l__out")),
                    "a run's start never kicks a LoopOut (parallel {parallel})",
                );

                // Zero iterations: the LoopIn ends the loop on its own.
                let run_id = uuid::Uuid::new_v4();
                let (drove, events, seen) = walk(&project, serde_json::json!([]), run_id, born(&project, run_id)).await;
                assert!(matches!(drove.outcome, RunOutcome::Completed), "parallel {parallel}, []: {:?}", drove.outcome);
                assert!(seen.is_empty(), "parallel {parallel}: no iteration, no body: {seen:?}");
                assert!(completed_at(&events, "l__out").is_empty(), "parallel {parallel}: no iteration, no LoopOut");

                // Two iterations: each fires its LoopOut, and the body ran in each.
                let run_id = uuid::Uuid::new_v4();
                let (drove, events, mut seen) = walk(&project, serde_json::json!([1, 2]), run_id, born(&project, run_id)).await;
                assert!(matches!(drove.outcome, RunOutcome::Completed), "parallel {parallel}, [1, 2]: {:?}", drove.outcome);
                seen.sort_by_key(|value| value.as_i64());
                assert_eq!(seen, vec![serde_json::json!(1), serde_json::json!(2)], "parallel {parallel}: the body ran once per item");
                assert_eq!(completed_at(&events, "tally"), vec![vec![0], vec![1]], "parallel {parallel}");
                assert_eq!(completed_at(&events, "l__out"), vec![vec![0], vec![1]], "parallel {parallel}: one LoopOut per iteration, none at the loop's frames");
            }
        }
    }

    weft_core::stress_test! {
        name: a_loop_with_no_output_resumed_after_its_first_launch_ends_without_running_an_iteration_twice,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let project = walked_loop_project(false, false);
            let run_id = uuid::Uuid::new_v4();
            let (_, whole, _) = walk(&project, serde_json::json!([1, 2]), run_id, born(&project, run_id)).await;
            // The worker went away right after launching iteration 0,
            // before anything inside it fired: a new one folds the journal
            // so far, which kicks that iteration's LoopOut again, and
            // carries on from there. (A later cut would catch `tally`
            // mid-run, which a resume fails on purpose.)
            let cut = whole.iter().position(|row| matches!(row, RunEvent::LoopIterationLaunched { index: 0, .. }))
                .expect("iteration 0 launched");
            assert!(!whole[..=cut].iter().any(|row| matches!(row, RunEvent::NodeStarted { node_id, .. } if node_id == "l__out" || node_id == "tally")),
                "the cut is before anything inside the loop fired");
            let (drove, events, _) = walk(&project, serde_json::json!([1, 2]), run_id, whole[..=cut].to_vec()).await;
            assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
            assert_eq!(completed_at(&events, "tally"), vec![vec![0], vec![1]], "each iteration's body once, across both workers");
            assert_eq!(completed_at(&events, "l__out"), vec![vec![0], vec![1]], "each iteration's LoopOut once, across both workers");
        }
    }

    weft_core::stress_test! {
        name: a_loop_with_an_output_still_gathers_what_its_body_writes,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            for parallel in [false, true] {
                let project = walked_loop_project(parallel, true);
                for (items, gathered) in [(serde_json::json!([]), serde_json::json!([])), (serde_json::json!([1, 2]), serde_json::json!([1, 2]))] {
                    let run_id = uuid::Uuid::new_v4();
                    let (drove, events, seen) = walk(&project, items.clone(), run_id, born(&project, run_id)).await;
                    assert!(matches!(drove.outcome, RunOutcome::Completed), "parallel {parallel}, {items}: {:?}", drove.outcome);
                    // The body records each item it ran on, the sink the one
                    // list it was given: exactly those, nothing twice.
                    let (lists, mut ran): (Vec<serde_json::Value>, Vec<serde_json::Value>) = seen.into_iter().partition(|value| value.is_array());
                    ran.sort_by_key(|value| value.as_i64());
                    assert_eq!(lists, vec![gathered], "parallel {parallel}, {items}: the sink got the gathered list once");
                    assert_eq!(serde_json::Value::Array(ran), items, "parallel {parallel}, {items}: the body ran once per item");
                    assert_eq!(completed_at(&events, "sink"), vec![Vec::<u32>::new()], "parallel {parallel}, {items}");
                }
            }
        }
    }

    // A loop launches its iterations and does not own them: an iteration
    // that keeps working after its `LoopOut` fired outlives the loop, and
    // the run, and its memory of the iteration, wait for it.

    /// A loop's body that outlives its loop: opens a bus on `bus`, which
    /// is what its `LoopOut` gathers, then answers the first `ask` it gets
    /// with the item it was handed, the ask, and who asked, and returns.
    struct Server;
    super::engine_test_rig::test_manifest!(Server, "Server");
    #[async_trait]
    impl weft_core::Node for Server {
        async fn run(&self, ctx: weft_core::RunContext) -> weft_core::error::WeftResult<()> {
            let item: serde_json::Value = ctx.inputs.get("value")?;
            let bus = ctx.open_bus("bus", Default::default(), "server").await?;
            let mut asks = bus.cursor();
            let asked = asks.next_json("ask").await.map_err(|e| weft_core::error::WeftError::NodeFailed(format!("read the ask: {e:?}")))?;
            let Some((from, ask)) = asked else {
                return Err(weft_core::error::WeftError::NodeFailed("the bus closed before anyone asked".into()));
            };
            bus.send("answer", serde_json::json!({ "item": item, "ask": ask, "from": from }))
                .map_err(|e| weft_core::error::WeftError::NodeFailed(format!("send the answer: {e:?}")))
        }
    }

    /// After the loop: asks every gathered bus, in gather order, with its
    /// index, and writes down `(index, who answered, the answer)`.
    struct Asker {
        answers: Arc<StdMutex<Vec<(usize, String, serde_json::Value)>>>,
    }
    super::engine_test_rig::test_manifest!(Asker, "Asker");
    #[async_trait]
    impl weft_core::Node for Asker {
        async fn run(&self, ctx: weft_core::RunContext) -> weft_core::error::WeftResult<()> {
            let failed = |what: &str, e: &dyn std::fmt::Debug| weft_core::error::WeftError::NodeFailed(format!("{what}: {e:?}"));
            let buses: Vec<serde_json::Value> = ctx.inputs.get("buses")?;
            for (index, marker) in buses.iter().enumerate() {
                let mut bus = ctx.bus(marker)?;
                bus.register("asker").map_err(|e| failed("register", &e))?;
                let mut answers = bus.cursor();
                bus.send("ask", serde_json::json!(index)).map_err(|e| failed("send the ask", &e))?;
                let answered = answers.next_json("answer").await.map_err(|e| failed("read the answer", &e))?;
                let (from, answer) = answered.ok_or_else(|| failed("the bus closed unanswered", &index))?;
                self.answers.lock().unwrap().push((index, from, answer));
            }
            Ok(())
        }
    }

    /// `lister.items -> l (parallel Loop over items)`, the body `server`
    /// opening a bus the loop gathers into `buses`, which reach `asker`.
    fn outliving_loop_project() -> ProjectDefinition {
        let loop_config = serde_json::json!({ "parallel": true, "over": ["items"], "carry": [] });
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "lister", "nodeType": "Lister", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "items", "portType": "List[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "l__in", "nodeType": "LoopIn", "label": null,
                    "config": { "parentId": "l", "parallel": true, "over": ["items"], "carry": [] },
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "items", "portType": "List[Number]", "required": true }],
                    "outputs": [
                        { "name": "items", "portType": "Number", "required": false },
                        { "name": "index", "portType": "Number", "required": false }
                    ],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "l", "role": "In" }
                },
                {
                    "id": "server", "nodeType": "Server", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "value", "portType": "Number", "required": true }],
                    "outputs": [{ "name": "bus", "portType": "Bus", "required": false }],
                    "features": {}, "scope": ["l"], "groupBoundary": null, "requiresInfra": false, "images": []
                },
                {
                    "id": "l__out", "nodeType": "LoopOut", "label": null,
                    "config": { "parentId": "l" }, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [
                        { "name": "buses", "portType": "Bus", "required": false, "optional": true },
                        { "name": "done", "portType": "Boolean", "required": false, "optional": true }
                    ],
                    "outputs": [{ "name": "buses", "portType": "List[Bus | Null]", "required": true }],
                    "features": {}, "scope": [], "requiresInfra": false, "images": [],
                    "groupBoundary": { "groupId": "l", "role": "Out" }
                },
                {
                    "id": "asker", "nodeType": "Asker", "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "buses", "portType": "List[Bus | Null]", "required": true }],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "lister", "sourceHandle": "items", "target": "l__in", "targetHandle": "items" },
                { "id": "e2", "source": "l__in", "sourceHandle": "items", "target": "server", "targetHandle": "value" },
                { "id": "e3", "source": "server", "sourceHandle": "bus", "target": "l__out", "targetHandle": "buses" },
                { "id": "e4", "source": "l__out", "sourceHandle": "buses", "target": "asker", "targetHandle": "buses" }
            ],
            "groups": [{ "id": "l", "kind": "loop", "nodeIds": ["server"], "loopConfig": loop_config }],
            "createdAt": parse_dt(),
            "updatedAt": parse_dt(),
        }))
        .expect("outliving loop project")
    }

    /// What a worker driving the run on `rows` (its record so far) has
    /// forgotten, read the way it folds a record it carries a run on from.
    fn forgetting_after(project: &ProjectDefinition, rows: &[RunEvent]) -> weft_core::run::forget::Forgetting {
        let run_id = rows.first().expect("the record starts with its birth").run_id();
        let mut live = weft_journal::LiveFold::from(weft_journal::Fold::new(run_id, Arc::new(project.clone())).driving(), Default::default());
        live.apply(&super::engine_test_rig::record_rows(rows)).expect("journal folds");
        let mut fold = live.into_fold().expect("the record starts with its birth");
        fold.read_to_the_end();
        fold.into_snapshot().forgetting.expect("a driving fold forgets")
    }

    weft_core::stress_test! {
        name: a_parallel_loop_ends_while_its_iterations_still_serve_their_buses,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            const ITEMS: u32 = 20;
            let project = outliving_loop_project();
            let run_id = uuid::Uuid::new_v4();
            let answers = Arc::new(StdMutex::new(Vec::new()));
            let cat = super::engine_test_rig::catalog(vec![
                ("Lister", Box::new(Lister { items: serde_json::json!((0..ITEMS).map(|i| 100 + i).collect::<Vec<_>>()) })),
                ("Server", Box::new(Server)),
                ("Asker", Box::new(Asker { answers: answers.clone() })),
            ]);
            let driving = super::engine_test_rig::drive_journal_observed(project.clone(), cat, run_id, born(&project, run_id), CancellationFlag::new_arc());
            let (drove, events) = tokio::time::timeout(std::time::Duration::from_secs(30), driving).await.expect("the run ends");
            let drove = drove.expect("the run drives");
            assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
            super::engine_test_rig::assert_fold_matches_live(&project, &events, Default::default(), &drove);

            // Each gathered bus reaches the iteration that opened it, still
            // there to answer.
            let mut answers = answers.lock().unwrap().clone();
            answers.sort_by_key(|(index, _, _)| *index);
            let expected: Vec<(usize, String, serde_json::Value)> = (0..ITEMS as usize)
                .map(|i| (i, "server".to_string(), serde_json::json!({ "item": 100 + i, "ask": i, "from": "asker" })))
                .collect();
            assert_eq!(answers, expected, "every gathered bus answered by its own iteration");

            // The loop ended before any body returned, and the run after the
            // last one did.
            let position = |pick: &dyn Fn(&RunEvent) -> bool| -> Vec<usize> {
                events.iter().enumerate().filter(|(_, row)| pick(row)).map(|(at, _)| at).collect()
            };
            let loop_ended = position(&|row| matches!(row, RunEvent::LoopTerminated { group_id, .. } if group_id == "l"));
            assert_eq!(loop_ended.len(), 1, "the loop ends once");
            let loop_ended = loop_ended[0];
            assert_eq!(position(&|row| matches!(row, RunEvent::LoopOutFired { .. })).len(), ITEMS as usize);
            assert!(position(&|row| matches!(row, RunEvent::LoopOutFired { .. })).iter().all(|at| *at < loop_ended));
            let returned: Vec<(u32, usize)> = events.iter().enumerate().filter_map(|(at, row)| match row {
                RunEvent::NodeCompleted { node_id, frames, .. } if node_id == "server" => Some((weft_core::frames::loop_indices(frames)[0], at)),
                _ => None,
            }).collect();
            let mut returned_indices: Vec<u32> = returned.iter().map(|(index, _)| *index).collect();
            returned_indices.sort();
            assert_eq!(returned_indices, (0..ITEMS).collect::<Vec<_>>(), "every body returned once");
            let first_return = returned.iter().map(|(_, at)| *at).min().unwrap();
            let last_return = returned.iter().map(|(_, at)| *at).max().unwrap();
            assert!(loop_ended < first_return, "the loop ended (row {loop_ended}) before any body returned (row {first_return})");
            let asked_from = position(&|row| matches!(row, RunEvent::NodeStarted { node_id, .. } if node_id == "asker"));
            assert!(asked_from.len() == 1 && asked_from[0] > loop_ended, "the asker starts once, after the loop ended: {asked_from:?}");
            let run_completed = position(&|row| matches!(row, RunEvent::RunCompleted { .. }));
            assert!(run_completed.len() == 1 && run_completed[0] > last_return, "the run completes once, after the last body returned (row {last_return}): {run_completed:?}");

            // Each iteration is held until its body returns and forgotten
            // once it did, the loop long ended either way.
            let key = LoopInstanceKey { group_id: "l".into(), parent_frames: vec![], run_id };
            for (index, at) in &returned {
                let copy = weft_core::project::scopes::ScopeCopy::new("l", vec![Frame::Loop { index: *index }]);
                let before = forgetting_after(&project, &events[..*at]);
                assert!(before.holds_iteration(&copy) && !before.forgot_iteration(&key, *index), "iteration {index} is held while its body runs");
                let after = forgetting_after(&project, &events[..=*at]);
                assert!(!after.holds_iteration(&copy) && after.forgot_iteration(&key, *index), "iteration {index} is forgotten once its body returned");
            }
        }
    }

    /// `lister.items -> l (Loop over items)`: in the body, `a` reads the
    /// item and `l` gathers it; `spread` passes the list it is handed into
    /// the inner loop `l.m`, whose body `l.m.c` reads each item, and `after`
    /// reads what `l.m` gathers. Two loops deep for a cut inside `l.m`'s
    /// body.
    fn nested_loop_project() -> ProjectDefinition {
        let node = |id: &str, node_type: &str, scope: &[&str], inputs: serde_json::Value, outputs: serde_json::Value| serde_json::json!({
            "id": id, "nodeType": node_type, "label": null, "config": null, "position": { "x": 0.0, "y": 0.0 },
            "inputs": inputs, "outputs": outputs, "features": {}, "scope": scope, "groupBoundary": null, "requiresInfra": false, "images": []
        });
        let boundary = |group: &str, role: &str, scope: &[&str], inputs: serde_json::Value, outputs: serde_json::Value| {
            let (suffix, node_type, config) = if role == "In" {
                ("__in", "LoopIn", serde_json::json!({ "parentId": group, "parallel": false, "over": ["items"], "carry": [] }))
            } else {
                ("__out", "LoopOut", serde_json::json!({ "parentId": group }))
            };
            serde_json::json!({
                "id": format!("{group}{suffix}"), "nodeType": node_type, "label": null, "config": config,
                "position": { "x": 0.0, "y": 0.0 }, "inputs": inputs, "outputs": outputs, "features": {}, "scope": scope,
                "requiresInfra": false, "images": [], "groupBoundary": { "groupId": group, "role": role }
            })
        };
        let port = |name: &str, ty: &str| serde_json::json!({ "name": name, "portType": ty, "required": true });
        let gather = |name: &str, ty: &str| serde_json::json!({ "name": name, "portType": ty, "required": false, "optional": true });
        let done = gather("done", "Boolean");
        let loop_in = |group: &str, scope: &[&str]| boundary(group, "In", scope,
            serde_json::json!([port("items", "List[Number]")]),
            serde_json::json!([{ "name": "items", "portType": "Number", "required": false }, { "name": "index", "portType": "Number", "required": false }]));
        let loop_out = |group: &str, scope: &[&str]| boundary(group, "Out", scope,
            serde_json::json!([gather("results", "Number"), done.clone()]),
            serde_json::json!([port("results", "List[Number | Null]")]));
        let wire = |id: &str, source: &str, source_port: &str, target: &str, target_port: &str| serde_json::json!({
            "id": id, "source": source, "sourceHandle": source_port, "target": target, "targetHandle": target_port
        });
        let lists = serde_json::json!([{ "name": "items", "portType": "List[Number]", "required": false }]);
        let tally = |ty: &str| (serde_json::json!([port("value", ty)]), serde_json::json!([{ "name": "value", "portType": ty, "required": false }]));
        let (number_in, number_out) = tally("Number");
        let (list_in, list_out) = tally("List[Number | Null]");
        serde_json::from_value(serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                node("lister", "Lister", &[], serde_json::json!([]), lists),
                loop_in("l", &[]),
                node("l.a", "Tally", &["l"], number_in.clone(), number_out.clone()),
                node("l.spread", "Tally", &["l"], serde_json::json!([port("value", "List[Number]")]), serde_json::json!([{ "name": "value", "portType": "List[Number]", "required": false }])),
                loop_in("l.m", &["l"]),
                node("l.m.c", "Tally", &["l", "l.m"], number_in, number_out),
                loop_out("l.m", &["l"]),
                node("l.after", "Tally", &["l"], list_in.clone(), list_out.clone()),
                loop_out("l", &[]),
                node("sink", "Tally", &[], list_in, list_out),
            ],
            "edges": [
                wire("e1", "lister", "items", "l__in", "items"),
                wire("e2", "l__in", "items", "l.a", "value"),
                wire("e3", "l.a", "value", "l__out", "results"),
                wire("e4", "l__out", "results", "sink", "value"),
                wire("e5", "l.spread", "value", "l.m__in", "items"),
                wire("e6", "l.m__in", "items", "l.m.c", "value"),
                wire("e7", "l.m.c", "value", "l.m__out", "results"),
                wire("e8", "l.m__out", "results", "l.after", "value"),
            ],
            "groups": [
                { "id": "l", "kind": "loop", "nodeIds": ["l.a", "l.spread", "l.after"], "loopConfig": { "parallel": false, "over": ["items"], "carry": [] } },
                { "id": "l.m", "kind": "loop", "nodeIds": ["l.m.c"], "parentGroupId": "l", "loopConfig": { "parallel": false, "over": ["items"], "carry": [] } },
            ],
            "createdAt": parse_dt(),
            "updatedAt": parse_dt(),
        }))
        .expect("nested loop project")
    }

    /// The run of the cut `bounds` of `whole`, handed `given` at its
    /// starts: its birth (the cut it runs, what it is given, its kicks)
    /// driven through, the fold checked against the worker's tables.
    async fn run_cut(whole: &ProjectDefinition, bounds: weft_core::project::cut::SelectionBounds, handed: Vec<(&str, serde_json::Value)>) -> (Drove, Vec<RunEvent>, Vec<serde_json::Value>) {
        let walked = weft_core::project::cut::walk(&weft_core::project::graph::ProjectGraph::new(whole), &bounds).expect("the cut walks");
        let mut given = weft_core::project::Given {
            inputs: handed.into_iter().map(|(at, value)| (weft_core::frames::Located::top(at), [("value".to_string(), value)].into())).collect(),
            outputs: Default::default(),
        };
        let project = weft_core::project::cut::cut(whole, &walked, &mut given).expect("a cut");
        let run_id = uuid::Uuid::new_v4();
        let mut rows = vec![RunEvent::RunStarted { given: Box::new(given),
            run_id, project_id: project.id, entry_node: "l".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: Some("test-hash".into()), cut_from: Some("whole-hash".into()), binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
            seed: None, instance: None, stand_in: None, fired_trigger: None, instance_values: Default::default(), picks: Default::default(), at_unix: 0,
            settings: Default::default(),
        }];
        rows.extend(weft_core::run_spec::KickPlan::for_program(&project, None, None).into_iter().map(|kick| RunEvent::NodeKicked {
            run_id, node_id: kick.node, frames: kick.frames, firing: false, payload: None, port_snapshot: None, at_unix: 0,
        }));
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let cat = super::engine_test_rig::catalog(vec![
            ("Lister", Box::new(Lister { items: serde_json::json!([1, 2]) })),
            ("Tally", Box::new(Tally { seen: seen.clone() })),
        ]);
        let (drove, events) = super::engine_test_rig::drive_journal_observed(project.clone(), cat, run_id, rows, CancellationFlag::new_arc()).await;
        let drove = drove.expect("the cut's run drives");
        super::engine_test_rig::assert_fold_matches_live(&project, &events, Default::default(), &drove);
        let seen = seen.lock().unwrap().clone();
        (drove, events, seen)
    }

    weft_core::stress_test! {
        name: a_cut_inside_a_loop_body_runs_in_its_iteration_zero,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            use weft_core::project::cut::SelectionBounds;
            let whole = nested_loop_project();
            let spelled = |names: &[&str]| names.iter().map(|name| name.to_string()).collect::<Vec<_>>();

            // A start handed its list, and a stretch holding the inner loop
            // whole: everything in it runs once, inside `l`'s iteration 0,
            // the inner loop launching its own iterations there; nothing of
            // `l` itself runs, nor the rest of its body.
            let bounds = SelectionBounds { from: spelled(&["l.spread"]), target: spelled(&["l.after"]), ..Default::default() };
            let (drove, events, mut seen) = run_cut(&whole, bounds, vec![("l.spread", serde_json::json!([1, 2]))]).await;
            assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
            assert_eq!(completed_at(&events, "l.spread"), vec![vec![0]]);
            assert_eq!(completed_at(&events, "l.m.c"), vec![vec![0, 0], vec![0, 1]], "the inner loop ran its iterations inside iteration 0");
            assert_eq!(completed_at(&events, "l.m__out"), vec![vec![0, 0], vec![0, 1]]);
            assert_eq!(completed_at(&events, "l.after"), vec![vec![0]]);
            for outside in ["lister", "l__in", "l.a", "l__out", "sink"] {
                assert!(!events.iter().any(|row| matches!(row, RunEvent::NodeStarted { node_id, .. } | RunEvent::NodeKicked { node_id, .. } if node_id == outside)), "{outside} never runs");
            }
            assert!(!events.iter().any(|row| matches!(row, RunEvent::LoopInstantiated { group_id, .. } if group_id == "l")), "the loop around the cut is never launched");
            seen.sort_by_key(|value| value.to_string());
            assert_eq!(seen, vec![serde_json::json!(1), serde_json::json!(2), serde_json::json!([1, 2]), serde_json::json!([1, 2])]);

            // Wholly inside the inner loop's body: one firing, inside
            // iteration 0 of both loops.
            let bounds = SelectionBounds { from: spelled(&["l.m.c"]), target: spelled(&["l.m.c"]), ..Default::default() };
            let (drove, events, seen) = run_cut(&whole, bounds, vec![("l.m.c", serde_json::json!(5))]).await;
            assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
            assert_eq!(completed_at(&events, "l.m.c"), vec![vec![0, 0]]);
            assert_eq!(seen, vec![serde_json::json!(5)]);
            assert!(!events.iter().any(|row| matches!(row, RunEvent::LoopInstantiated { .. })), "no loop is launched");
        }
    }
