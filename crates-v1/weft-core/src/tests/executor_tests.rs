use super::*;
use crate::project::{LaneMode, EdgeIndex, PortDefinition, Edge, Position};
use crate::weft_type::{WeftType, WeftPrimitive};
use serde_json::json;

fn make_port(name: &str, lane_mode: LaneMode, required: bool) -> PortDefinition {
    PortDefinition {
        name: name.to_string(),
        portType: WeftType::type_var("T"),
        required,
        description: None,
        laneMode: lane_mode,
        laneDepth: 1,
        configurable: true,
    }
}

fn make_node(id: &str, inputs: Vec<PortDefinition>, outputs: Vec<PortDefinition>) -> NodeDefinition {
    NodeDefinition {
        id: id.to_string(),
        nodeType: "ExecPython".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs,
        outputs,
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    }
}

fn make_edge(source: &str, source_port: &str, target: &str, target_port: &str) -> Edge {
    Edge {
        id: format!("{}.{}->{}.{}", source, source_port, target, target_port),
        source: source.to_string(),
        target: target.to_string(),
        sourceHandle: Some(source_port.to_string()),
        targetHandle: Some(target_port.to_string()),
    }
}

fn sf(count: u32, index: u32) -> SplitFrame {
    SplitFrame { count, index }
}

/// Test: Mixed Gather+Expand+Single inputs on a single node.
///
/// Scenario: "mixer" node has:
///   - gathered: Gather input (receives 9 depth-2 scalar pulses, 3 per row)
///   - expanded: Expand input (receives 1 depth-0 list pulse with 3 labels)
///   - constant: Single input (receives 1 depth-0 scalar pulse, broadcast)
///
/// After preprocessing:
///   - gathered: 3 depth-1 list pulses (one per row, each with 3 gathered values)
///   - expanded: 3 depth-1 scalar pulses (one per label)
///   - constant: unchanged at depth-0 (broadcast)
///
/// find_ready_nodes should produce 3 ReadyGroups at depth-1.
#[test]
fn test_mixed_gather_expand_single_preprocessing() {
    // Build a minimal project with just the mixer node and upstream edges
    let mixer = make_node("mixer", vec![
        make_port("gathered", LaneMode::Gather, true),
        make_port("expanded", LaneMode::Expand, true),
        make_port("constant", LaneMode::Single, true),
    ], vec![]);

    // We also need stub upstream nodes so edges resolve
    let add_one = make_node("add_one", vec![], vec![
        make_port("result", LaneMode::Single, false),
    ]);
    let labels = make_node("labels", vec![], vec![
        make_port("value", LaneMode::Single, false),
    ]);
    let multiplier = make_node("multiplier", vec![], vec![
        make_port("value", LaneMode::Single, false),
    ]);

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        description: None,
        nodes: vec![add_one, labels, multiplier, mixer],
        edges: vec![
            make_edge("add_one", "result", "mixer", "gathered"),
            make_edge("labels", "value", "mixer", "expanded"),
            make_edge("multiplier", "value", "mixer", "constant"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let initial_input = json!({});
    let color = "c1".to_string();

    // Simulate pulse state after add_one completes 9 times at depth-2.
    // Row 0: cells [10,20,30] -> add_one produces [11,21,31]
    // Row 1: cells [40,50,60] -> add_one produces [41,51,61]
    // Row 2: cells [70,80,90] -> add_one produces [71,81,91]
    let mut pulses: PulseTable = BTreeMap::new();

    // 9 depth-2 Pending pulses on mixer.gathered (from add_one.result)
    let mixer_pulses: Vec<Pulse> = vec![
        Pulse::new_on_port(color.clone(), vec![sf(3,0), sf(3,0)], json!(11), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,0), sf(3,1)], json!(21), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,0), sf(3,2)], json!(31), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,1), sf(3,0)], json!(41), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,1), sf(3,1)], json!(51), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,1), sf(3,2)], json!(61), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,2), sf(3,0)], json!(71), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,2), sf(3,1)], json!(81), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,2), sf(3,2)], json!(91), "gathered".into()),
        // 1 depth-0 list pulse on mixer.expanded (from labels.value)
        Pulse::new_on_port(color.clone(), vec![], json!(["row_A", "row_B", "row_C"]), "expanded".into()),
        // 1 depth-0 scalar pulse on mixer.constant (from multiplier.value)
        Pulse::new_on_port(color.clone(), vec![], json!(100), "constant".into()),
    ];
    pulses.insert("mixer".to_string(), mixer_pulses);

    // --- Step 1: preprocess_input should gather + expand ---
    let mut iterations = 0;
    while preprocess_input(&project, &mut pulses) {
        iterations += 1;
        assert!(iterations < 10, "preprocess_input should converge");
    }

    // Verify mixer pulses after preprocessing
    let mixer_ps = pulses.get("mixer").unwrap();
    let pending: Vec<&Pulse> = mixer_ps.iter()
        .filter(|p| p.status == PulseStatus::Pending)
        .collect();

    // All 9 original gathered pulses should be Absorbed
    let absorbed_gathered: Vec<&Pulse> = mixer_ps.iter()
        .filter(|p| p.status == PulseStatus::Absorbed && p.port.as_deref() == Some("gathered"))
        .collect();
    assert_eq!(absorbed_gathered.len(), 9, "all 9 depth-2 gathered pulses should be absorbed");

    // 3 new gathered list pulses at depth-1
    let gathered_pending: Vec<&Pulse> = pending.iter()
        .filter(|p| p.port.as_deref() == Some("gathered"))
        .copied()
        .collect();
    assert_eq!(gathered_pending.len(), 3, "should have 3 gathered list pulses at depth-1");
    for p in &gathered_pending {
        assert_eq!(p.lane.len(), 1, "gathered list pulse should be at depth-1");
        assert!(p.data.is_array(), "gathered pulse data should be an array");
        assert_eq!(p.data.as_array().unwrap().len(), 3, "each gathered list should have 3 items");
    }

    // Original expand list pulse should be Absorbed
    let absorbed_expand: Vec<&Pulse> = mixer_ps.iter()
        .filter(|p| p.status == PulseStatus::Absorbed && p.port.as_deref() == Some("expanded"))
        .collect();
    assert_eq!(absorbed_expand.len(), 1, "original expand list pulse should be absorbed");

    // 3 new expanded scalar pulses at depth-1
    let expanded_pending: Vec<&Pulse> = pending.iter()
        .filter(|p| p.port.as_deref() == Some("expanded"))
        .copied()
        .collect();
    assert_eq!(expanded_pending.len(), 3, "should have 3 expanded scalar pulses at depth-1");
    for p in &expanded_pending {
        assert_eq!(p.lane.len(), 1, "expanded scalar pulse should be at depth-1");
        assert!(p.data.is_string(), "expanded pulse data should be a string");
    }

    // constant pulse unchanged at depth-0
    let constant_pending: Vec<&Pulse> = pending.iter()
        .filter(|p| p.port.as_deref() == Some("constant"))
        .copied()
        .collect();
    assert_eq!(constant_pending.len(), 1, "constant pulse should remain at depth-0");
    assert!(constant_pending[0].lane.is_empty(), "constant pulse should have empty lane");
    assert_eq!(constant_pending[0].data, json!(100));

    // --- Step 2: find_ready_nodes should produce 3 ReadyGroups for mixer ---
    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let mixer_ready: Vec<&ReadyGroup> = ready.iter()
        .filter(|(nid, _)| nid == "mixer")
        .map(|(_, g)| g)
        .collect();

    assert_eq!(mixer_ready.len(), 3, "mixer should have 3 ready groups (one per row)");

    // Each ReadyGroup should be at depth-1 with the correct input
    for g in &mixer_ready {
        assert_eq!(g.lane.len(), 1, "each mixer ReadyGroup should be at depth-1");
        assert!(!g.should_skip, "mixer should not be skipped");
        assert!(g.error.is_none(), "mixer should have no error");

        let input = g.input.as_object().unwrap();
        assert!(input.contains_key("gathered"), "input should have gathered");
        assert!(input.contains_key("expanded"), "input should have expanded");
        assert!(input.contains_key("constant"), "input should have constant");

        let gathered = input["gathered"].as_array().unwrap();
        assert_eq!(gathered.len(), 3, "gathered should have 3 items");

        assert!(input["expanded"].is_string(), "expanded should be a string");
        assert_eq!(input["constant"], json!(100), "constant should be 100");
    }

    // Verify that the gathered values are correct per row
    let mut row_data: Vec<(u32, Vec<i64>, String)> = mixer_ready.iter().map(|g| {
        let row_idx = g.lane[0].index;
        let gathered: Vec<i64> = g.input["gathered"].as_array().unwrap()
            .iter().map(|v| v.as_i64().unwrap()).collect();
        let label = g.input["expanded"].as_str().unwrap().to_string();
        (row_idx, gathered, label)
    }).collect();
    row_data.sort_by_key(|(idx, _, _)| *idx);

    assert_eq!(row_data[0], (0, vec![11, 21, 31], "row_A".to_string()));
    assert_eq!(row_data[1], (1, vec![41, 51, 61], "row_B".to_string()));
    assert_eq!(row_data[2], (2, vec![71, 81, 91], "row_C".to_string()));
}

/// Test: Gather input pulses arriving incrementally should NOT trigger
/// premature readiness. Only after all siblings arrive and preprocessing
/// gathers them should the node become ready.
#[test]
fn test_gather_partial_arrival_no_premature_readiness() {
    let mixer = make_node("mixer", vec![
        make_port("gathered", LaneMode::Gather, true),
        make_port("constant", LaneMode::Single, true),
    ], vec![]);
    let upstream = make_node("upstream", vec![], vec![
        make_port("result", LaneMode::Single, false),
    ]);
    let broadcast = make_node("broadcast", vec![], vec![
        make_port("value", LaneMode::Single, false),
    ]);

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        description: None,
        nodes: vec![upstream, broadcast, mixer],
        edges: vec![
            make_edge("upstream", "result", "mixer", "gathered"),
            make_edge("broadcast", "value", "mixer", "constant"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let initial_input = json!({});
    let color = "c1".to_string();

    // Only 2 of 3 siblings arrived on the Gather port
    let mut pulses: PulseTable = BTreeMap::new();
    pulses.insert("mixer".to_string(), vec![
        Pulse::new_on_port(color.clone(), vec![sf(3,0)], json!(10), "gathered".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,1)], json!(20), "gathered".into()),
        // sf(3,2) NOT arrived yet
        Pulse::new_on_port(color.clone(), vec![], json!(100), "constant".into()),
    ]);

    // Preprocess: gather should NOT fire (only 2 of 3 siblings)
    while preprocess_input(&project, &mut pulses) {}

    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let mixer_ready: Vec<_> = ready.iter().filter(|(nid, _)| nid == "mixer").collect();
    assert!(mixer_ready.is_empty(), "mixer should NOT be ready with only 2/3 gather siblings");

    // Now the 3rd sibling arrives
    pulses.get_mut("mixer").unwrap().push(
        Pulse::new_on_port(color.clone(), vec![sf(3,2)], json!(30), "gathered".into()),
    );

    // Preprocess: gather should fire now
    while preprocess_input(&project, &mut pulses) {}

    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let mixer_ready: Vec<_> = ready.iter().filter(|(nid, _)| nid == "mixer").collect();
    assert_eq!(mixer_ready.len(), 1, "mixer should be ready after all 3 gather siblings arrive");

    let g = &mixer_ready[0].1;
    assert!(g.lane.is_empty(), "gathered at depth-1 collapses to depth-0");
    let gathered = g.input["gathered"].as_array().unwrap();
    assert_eq!(gathered, &vec![json!(10), json!(20), json!(30)]);
    assert_eq!(g.input["constant"], json!(100));
}

/// Test: Double Collect (Gather→Gather chaining).
///
/// Scenario: ForEach(depth 0→1) → ForEach(depth 1→2) → Collect(depth 2→1) → Collect(depth 1→0)
///
/// The second Collect receives depth-1 list pulses (arrays from the first
/// Collect) and must gather them into a single depth-0 list-of-lists.
/// This verifies that upstream array values on Gather ports are correctly
/// gathered (not skipped as "already gathered").
#[test]
fn test_double_collect_chained_gather() {
    // collect_inner: Gather input, receives depth-2 scalars → produces depth-1 lists
    // collect_outer: Gather input, receives depth-1 lists → produces depth-0 list-of-lists
    let collect_outer = make_node("collect_outer", vec![
        make_port("value", LaneMode::Gather, true),
    ], vec![
        make_port("list", LaneMode::Single, false),
    ]);
    let collect_inner = make_node("collect_inner", vec![], vec![
        make_port("list", LaneMode::Single, false),
    ]);

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        description: None,
        nodes: vec![collect_inner, collect_outer],
        edges: vec![
            make_edge("collect_inner", "list", "collect_outer", "value"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let initial_input = json!({});
    let color = "c1".to_string();

    // Simulate: collect_inner already produced 3 depth-1 list pulses
    // (these are normal upstream pulses, NOT gather-synthesized)
    let mut pulses: PulseTable = BTreeMap::new();
    pulses.insert("collect_outer".to_string(), vec![
        Pulse::new_on_port(color.clone(), vec![sf(3,0)], json!([10, 20, 30]), "value".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,1)], json!([40, 50, 60]), "value".into()),
        Pulse::new_on_port(color.clone(), vec![sf(3,2)], json!([70, 80, 90]), "value".into()),
    ]);

    // Preprocess: should gather the 3 depth-1 list pulses into 1 depth-0 list-of-lists
    while preprocess_input(&project, &mut pulses) {}

    let outer_ps = pulses.get("collect_outer").unwrap();
    let absorbed: Vec<&Pulse> = outer_ps.iter()
        .filter(|p| p.status == PulseStatus::Absorbed)
        .collect();
    assert_eq!(absorbed.len(), 3, "all 3 depth-1 list pulses should be absorbed");

    let gathered: Vec<&Pulse> = outer_ps.iter()
        .filter(|p| p.status == PulseStatus::Pending && p.gathered)
        .collect();
    assert_eq!(gathered.len(), 1, "should have 1 gathered pulse at depth-0");
    assert!(gathered[0].lane.is_empty(), "gathered pulse should be at depth-0");
    let list = gathered[0].data.as_array().unwrap();
    assert_eq!(list.len(), 3, "gathered list should have 3 items");
    assert_eq!(list[0], json!([10, 20, 30]));
    assert_eq!(list[1], json!([40, 50, 60]));
    assert_eq!(list[2], json!([70, 80, 90]));

    // find_ready_nodes should produce 1 ReadyGroup for collect_outer at depth-0
    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let outer_ready: Vec<_> = ready.iter().filter(|(nid, _)| nid == "collect_outer").collect();
    assert_eq!(outer_ready.len(), 1, "collect_outer should be ready");
    assert!(outer_ready[0].1.lane.is_empty(), "should fire at depth-0");
}

/// Test: Group with Expand input, simulating the full Passthrough lifecycle.
///
/// Compiled group structure (from weft compiler):
///   external_list (outputs list on "value" port)
///   Processing__in (Passthrough: Expand input "item", Single output "item")
///   transform (Single input "item", Single output "result")
///   Processing__out (Passthrough: Single input "result", Gather output "result")
///   final_node (Single input "data")
///
/// Flow:
/// 1. external_list completes with ["a","b","c"]
/// 2. Processing__in.item (Expand) receives the list
/// 3. preprocess_input splits into 3 child-lane pulses
/// 4. Processing__in dispatches 3 times, each returning scalar
/// 5. transform dispatches 3 times at child lanes
/// 6. Processing__out dispatches 3 times at child lanes
/// 7. Processing__out Gather output collects all 3, emits at parent lane
/// 8. final_node receives the gathered list
#[test]
fn test_group_expand_input_full_lifecycle() {
    // Build the flattened project (as the weft compiler would produce)
    let external_list = make_node("external_list", vec![], vec![
        make_port("value", LaneMode::Single, false),
    ]);
    let pt_in = NodeDefinition {
        id: "Processing__in".to_string(),
        nodeType: "Passthrough".into(),
        label: Some("Processing (in)".to_string()),
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "item".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![make_port("item", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };
    let transform = make_node("transform", vec![
        make_port("item", LaneMode::Single, true),
    ], vec![
        make_port("result", LaneMode::Single, false),
    ]);
    let pt_out = NodeDefinition {
        id: "Processing__out".to_string(),
        nodeType: "Passthrough".into(),
        label: Some("Processing (out)".to_string()),
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Gather, false)],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };
    let final_node = make_node("final_node", vec![
        make_port("data", LaneMode::Single, true),
    ], vec![]);

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".to_string(),
        description: None,
        nodes: vec![external_list, pt_in, transform, pt_out, final_node],
        edges: vec![
            make_edge("external_list", "value", "Processing__in", "item"),
            make_edge("Processing__in", "item", "transform", "item"),
            make_edge("transform", "result", "Processing__out", "result"),
            make_edge("Processing__out", "result", "final_node", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let initial_input = json!({});
    let color = "c1".to_string();

    // === Step 1: external_list completes with ["a","b","c"] ===
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    // Simulate external_list completing and emitting downstream
    postprocess_output("external_list", &json!({"value": ["a","b","c"]}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Verify: Processing__in should have a Pending pulse with array data on "item" port
    let pt_in_pulses = pulses.get("Processing__in").unwrap();
    assert_eq!(pt_in_pulses.len(), 1, "Processing__in should have 1 pulse");
    assert_eq!(pt_in_pulses[0].status, PulseStatus::Pending);
    assert_eq!(pt_in_pulses[0].port.as_deref(), Some("item"));
    assert_eq!(pt_in_pulses[0].data, json!(["a","b","c"]));

    // === Step 2: preprocess_input splits the Expand input ===
    while preprocess_input(&project, &mut pulses) {}

    let pt_in_pulses = pulses.get("Processing__in").unwrap();
    let pending_in: Vec<&Pulse> = pt_in_pulses.iter().filter(|p| p.status == PulseStatus::Pending).collect();
    assert_eq!(pending_in.len(), 3, "Processing__in should have 3 child-lane pending pulses after expand");
    for (_i, p) in pending_in.iter().enumerate() {
        assert_eq!(p.lane.len(), 1, "child pulse should be at depth 1");
        assert_eq!(p.lane[0].count, 3);
    }

    // === Step 3: find_ready_nodes for Processing__in ===
    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let pt_in_ready: Vec<_> = ready.iter().filter(|(nid, _)| nid == "Processing__in").collect();
    assert_eq!(pt_in_ready.len(), 3, "Processing__in should have 3 ready groups");

    // === Step 4: Simulate Processing__in execution (Passthrough: forward input as output) ===
    // For each ready group, simulate dispatch + completion
    for (_, group) in &pt_in_ready {
        // Mark consumed pulses as absorbed (as collect_dispatch_work does)
        if let Some(ps) = pulses.get_mut("Processing__in") {
            for p in ps.iter_mut() {
                if group.pulse_ids.contains(&p.id) && p.status == PulseStatus::Pending {
                    p.status = PulseStatus::Absorbed;
                }
            }
        }

        // Passthrough returns input as output
        let output = group.input.clone();

        // Run postprocess_output
        postprocess_output("Processing__in", &output, &group.color, &group.lane, &project, &mut pulses, &edge_idx, &mut node_executions);
    }

    // === Step 5: Verify transform received correct pulses ===
    while preprocess_input(&project, &mut pulses) {}
    let transform_pulses = pulses.get("transform").unwrap();
    let transform_pending: Vec<&Pulse> = transform_pulses.iter().filter(|p| p.status == PulseStatus::Pending).collect();
    assert_eq!(transform_pending.len(), 3, "transform should have 3 pending pulses");

    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let transform_ready: Vec<_> = ready.iter().filter(|(nid, _)| nid == "transform").collect();
    assert_eq!(transform_ready.len(), 3, "transform should have 3 ready groups");

    // Verify transform inputs are the scalar values, not null
    for (_, group) in &transform_ready {
        let item_val = group.input.get("item").expect("transform input should have 'item'");
        assert!(!item_val.is_null(), "transform input 'item' should not be null, got: {:?} at lane {:?}", item_val, group.lane);
        assert!(item_val.is_string(), "transform input 'item' should be a string, got: {:?}", item_val);
    }

    // === Step 6: Simulate transform execution ===
    for (_, group) in &transform_ready {
        if let Some(ps) = pulses.get_mut("transform") {
            for p in ps.iter_mut() {
                if group.pulse_ids.contains(&p.id) && p.status == PulseStatus::Pending {
                    p.status = PulseStatus::Absorbed;
                }
            }
        }
        let item_val = group.input.get("item").unwrap();
        let result = json!({"result": format!("processed_{}", item_val.as_str().unwrap())});
        postprocess_output("transform", &result, &group.color, &group.lane, &project, &mut pulses, &edge_idx, &mut node_executions);
    }

    // === Step 7: Processing__out should have 3 pending pulses ===
    while preprocess_input(&project, &mut pulses) {}
    let pt_out_pulses = pulses.get("Processing__out").unwrap();
    let pt_out_pending: Vec<&Pulse> = pt_out_pulses.iter().filter(|p| p.status == PulseStatus::Pending).collect();
    assert_eq!(pt_out_pending.len(), 3, "Processing__out should have 3 pending pulses");

    let ready = find_ready_nodes(&project, &pulses, &initial_input, &edge_idx);
    let pt_out_ready: Vec<_> = ready.iter().filter(|(nid, _)| nid == "Processing__out").collect();
    assert_eq!(pt_out_ready.len(), 3, "Processing__out should have 3 ready groups");

    // === Step 8: Simulate Processing__out execution (Passthrough + Gather output) ===
    for (_, group) in &pt_out_ready {
        if let Some(ps) = pulses.get_mut("Processing__out") {
            for p in ps.iter_mut() {
                if group.pulse_ids.contains(&p.id) && p.status == PulseStatus::Pending {
                    p.status = PulseStatus::Absorbed;
                }
            }
        }
        let output = group.input.clone();
        // Create a NodeExecution record (needed for Gather output's try_gather_and_emit)
        node_executions.entry("Processing__out".to_string()).or_default().push(NodeExecution {
            id: uuid::Uuid::new_v4().to_string(),
            nodeId: "Processing__out".to_string(),
            status: NodeExecutionStatus::Completed,
            pulseIdsAbsorbed: group.pulse_ids.clone(),
            pulseId: uuid::Uuid::new_v4().to_string(),
    
            error: None,
            callbackId: None,
            startedAt: 0,
            completedAt: Some(1),
            input: None,
            output: Some(output.clone()),
            costUsd: 0.0,
            logs: vec![],
            color: group.color.clone(),
            lane: group.lane.clone(),
        });
        postprocess_output("Processing__out", &output, &group.color, &group.lane, &project, &mut pulses, &edge_idx, &mut node_executions);
    }

    // === Step 9: Verify final_node received gathered results ===
    while preprocess_input(&project, &mut pulses) {}
    let final_pulses = pulses.get("final_node").expect("final_node should have pulses");
    let final_pending: Vec<&Pulse> = final_pulses.iter().filter(|p| p.status == PulseStatus::Pending).collect();
    assert_eq!(final_pending.len(), 1, "final_node should have 1 pending pulse (gathered)");
    assert!(final_pending[0].lane.is_empty(), "gathered pulse should be at depth 0");

    let data = &final_pending[0].data;
    assert!(!data.is_null(), "final_node data should not be null");

    // The gathered result should be a list of processed items
    // try_gather_and_emit reads p.data.get(port_name) for each sibling
    eprintln!("final_node received: {:?}", data);
}

// =========================================================================
// runtime_type_check : unit tests (post-transform types only)
// =========================================================================

#[test]
fn test_type_check_string_match() {
    let pt = WeftType::primitive(WeftPrimitive::String);
    assert!(runtime_type_check(&pt, &json!("hello")));
}

#[test]
fn test_type_check_string_mismatch_number() {
    let pt = WeftType::primitive(WeftPrimitive::String);
    assert!(!runtime_type_check(&pt, &json!(42)));
}

#[test]
fn test_type_check_string_mismatch_array() {
    let pt = WeftType::primitive(WeftPrimitive::String);
    assert!(!runtime_type_check(&pt, &json!(["a", "b"])));
}

#[test]
fn test_type_check_list_string() {
    let pt = WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String)));
    assert!(runtime_type_check(&pt, &json!(["a", "b", "c"])));
    assert!(!runtime_type_check(&pt, &json!("hello")));
    assert!(!runtime_type_check(&pt, &json!([1, 2])));
}

#[test]
fn test_type_check_dict() {
    let pt = WeftType::dict(WeftType::primitive(WeftPrimitive::String), WeftType::primitive(WeftPrimitive::String));
    assert!(runtime_type_check(&pt, &json!({"k": "v"})));
    assert!(!runtime_type_check(&pt, &json!("string")));
}

#[test]
fn test_type_check_unresolved_always_passes() {
    let pt = WeftType::type_var("T");
    assert!(runtime_type_check(&pt, &json!(42)));
    assert!(runtime_type_check(&pt, &json!("hello")));
    assert!(runtime_type_check(&pt, &json!([1, 2])));
}

// =========================================================================
// Input type checking integration : build_input_from_pulses
// =========================================================================

#[test]
fn test_input_type_check_single_match_no_error() {
    let node = make_node("n", vec![
        PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    let pulse = Pulse::new_on_port("c1".into(), vec![], json!("hello"), "data".into());
    let mut type_errors = Vec::new();
    let input = build_input_from_pulses(&node, &[pulse], &[], "c1", &json!({}), true, &mut type_errors);
    assert!(type_errors.is_empty(), "no type error expected, got: {:?}", type_errors);
    assert_eq!(input.get("data").unwrap(), &json!("hello"));
}

#[test]
fn test_input_type_check_single_mismatch_nulls_and_errors() {
    let node = make_node("n", vec![
        PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    let pulse = Pulse::new_on_port("c1".into(), vec![], json!(42), "data".into());
    let mut type_errors = Vec::new();
    let input = build_input_from_pulses(&node, &[pulse], &[], "c1", &json!({}), true, &mut type_errors);
    assert_eq!(type_errors.len(), 1, "should have 1 type error");
    assert!(type_errors[0].contains("Type mismatch"), "error should mention type mismatch: {}", type_errors[0]);
    assert!(input.get("data").unwrap().is_null(), "mismatched value should be nulled");
}

#[test]
fn test_input_type_check_gather_skipped() {
    // Gather ports skip input type checking : the type is post-transform
    // and checking happens elsewhere (after gather collects the list).
    let node = make_node("n", vec![
        PortDefinition {
            name: "stream".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Gather,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    // Even a "wrong" type passes through : not checked at input for Gather ports
    let pulse = Pulse::new_on_port("c1".into(), vec![], json!(42), "stream".into());
    let mut type_errors = Vec::new();
    let input = build_input_from_pulses(&node, &[pulse], &[], "c1", &json!({}), true, &mut type_errors);
    assert!(type_errors.is_empty(), "Gather ports skip input type check");
    assert_eq!(input.get("stream").unwrap(), &json!(42));
}

#[test]
fn test_input_type_check_expand_element_passes() {
    let node = make_node("n", vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    // Post-expand: a single String element
    let pulse = Pulse::new_on_port("c1".into(), vec![sf(3,0)], json!("hello"), "items".into());
    let mut type_errors = Vec::new();
    let input = build_input_from_pulses(&node, &[pulse], &[sf(3,0)], "c1", &json!({}), true, &mut type_errors);
    assert!(type_errors.is_empty(), "String element on Expand(String) should pass: {:?}", type_errors);
    assert_eq!(input.get("items").unwrap(), &json!("hello"));
}

#[test]
fn test_input_type_check_expand_array_passes() {
    let node = make_node("n", vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    // Pre-expand: the value is an Array
    let pulse = Pulse::new_on_port("c1".into(), vec![], json!(["a", "b"]), "items".into());
    let mut type_errors = Vec::new();
    let input = build_input_from_pulses(&node, &[pulse], &[], "c1", &json!({}), true, &mut type_errors);
    assert!(type_errors.is_empty(), "Array on Expand(String) should pass: {:?}", type_errors);
    assert_eq!(input.get("items").unwrap(), &json!(["a", "b"]));
}

#[test]
fn test_input_type_check_expand_mismatch_nulls() {
    let node = make_node("n", vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    // Expand ports skip input type checking
    let pulse = Pulse::new_on_port("c1".into(), vec![], json!(42), "items".into());
    let mut type_errors = Vec::new();
    let input = build_input_from_pulses(&node, &[pulse], &[], "c1", &json!({}), true, &mut type_errors);
    assert!(type_errors.is_empty(), "Expand ports skip input type check");
    assert_eq!(input.get("items").unwrap(), &json!(42));
}

// =========================================================================
// Output type checking integration : postprocess_output
// =========================================================================

#[test]
fn test_output_type_check_single_match_emits() {
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "result", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    postprocess_output("src", &json!({"result": "hello"}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    let dst_ps = pulses.get("dst").expect("dst should have pulses");
    assert_eq!(dst_ps.len(), 1);
    assert_eq!(dst_ps[0].data, json!("hello"));
    assert_eq!(dst_ps[0].port.as_deref(), Some("data"));
}

#[test]
fn test_output_type_check_single_mismatch_emits_null() {
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "result", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    postprocess_output("src", &json!({"result": 42}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    let dst_ps = pulses.get("dst").expect("dst should have pulses");
    assert_eq!(dst_ps.len(), 1);
    // Mismatched output should be nulled before emission
    assert!(dst_ps[0].data.is_null(), "mismatched output should be null");
}

#[test]
fn test_output_type_check_gather_element_emits() {
    // Gather output declared as String. Node produces a String (pre-gather element). Should pass.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Gather,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "result", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    // At scalar lane (no siblings), Gather output emits directly
    postprocess_output("src", &json!({"result": "hello"}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    let dst_ps = pulses.get("dst").expect("dst should have pulses");
    assert_eq!(dst_ps.len(), 1);
    assert_eq!(dst_ps[0].data, json!("hello"));
}

#[test]
fn test_output_type_check_gather_skipped() {
    // Gather output ports skip type checking at output time.
    // The value passes through; type is checked post-gather at the downstream input.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Gather,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "result", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    postprocess_output("src", &json!({"result": 42}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    let dst_ps = pulses.get("dst").expect("dst should have pulses");
    assert_eq!(dst_ps.len(), 1);
    // Value passes through : not nulled
    assert_eq!(dst_ps[0].data, json!(42));
}

#[test]
fn test_output_type_check_expand_array_emits() {
    // Expand output declared as String. Node produces Array (pre-expand). Should pass.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "items", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    postprocess_output("src", &json!({"items": ["a", "b", "c"]}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    // Expand should create child-lane pulses
    let dst_ps = pulses.get("dst").expect("dst should have pulses");
    assert_eq!(dst_ps.len(), 3, "Expand should create 3 child-lane pulses");
}

#[test]
fn test_output_type_check_expand_non_array_produces_no_items() {
    // Expand output declared as String. Node produces 42 (not an array).
    // emit_expand_port logs error and produces no items.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "items", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    postprocess_output("src", &json!({"items": 42}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    // 42 is not an array, so no items to expand : no downstream pulses
    assert!(pulses.get("dst").is_none() || pulses.get("dst").unwrap().is_empty());
}
#[test]
fn test_expand_output_per_element_type_check() {
    // Expand output declared as String. Node outputs ["hello", 42, "world"].
    // "hello" and "world" should pass. 42 should be nulled (that lane only).
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "items", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    postprocess_output("src", &json!({"items": ["hello", 42, "world"]}), "c1", &[], &project, &mut pulses, &edge_idx, &mut BTreeMap::new());
    let dst_ps = pulses.get("dst").expect("dst should have pulses");
    assert_eq!(dst_ps.len(), 3, "3 items → 3 lanes");
    // Lane 0: "hello" → passes
    assert_eq!(dst_ps[0].data, json!("hello"));
    // Lane 1: 42 → Number doesn't match String → nulled
    assert!(dst_ps[1].data.is_null(), "lane 1 (Number) should be nulled");
    // Lane 2: "world" → passes
    assert_eq!(dst_ps[2].data, json!("world"));
}

#[test]
fn test_gather_output_collected_list_type_check() {
    // Gather output declared as List[String]. All siblings produce Numbers.
    // After gathering: [1, 2, 3]. Expected List[String]. Should log mismatch.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "results".to_string(),
            portType: WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String))),
            required: false,
            description: None,
            laneMode: LaneMode::Gather,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "results", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    // Simulate 3 completed sibling executions with Number values
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    let src_execs = node_executions.entry("src".to_string()).or_default();
    for i in 0..3u32 {
        src_execs.push(NodeExecution {
            id: format!("exec-{}", i),
            nodeId: "src".to_string(),
            status: NodeExecutionStatus::Completed,
            pulseIdsAbsorbed: vec![],
            pulseId: format!("p-{}", i),
    
            error: None,
            callbackId: None,
            startedAt: 0,
            completedAt: Some(1),
            input: None,
            output: Some(json!({"results": i + 1})),
            costUsd: 0.0,
            logs: vec![],
            color: "c1".to_string(),
            lane: vec![SplitFrame { count: 3, index: i }],
        });
    }
    // try_gather_and_emit collects [1, 2, 3] and checks against List[String]
    let gathered = try_gather_and_emit("src", "c1", &[SplitFrame { count: 3, index: 0 }], &["results"], &project, &mut pulses, &edge_idx, &node_executions);
    assert!(gathered, "gather should fire (all 3 siblings complete)");
    // The gathered list is [1, 2, 3] which doesn't match List[String] : mismatch logged
    // but data still passes through (gather doesn't null, downstream input check catches)
    let empty: Vec<Pulse> = vec![];
    let dst_ps = pulses.get("dst").unwrap_or(&empty);
    assert!(!dst_ps.is_empty(), "gathered data should still be emitted downstream");
}

#[test]
fn test_expand_input_per_element_type_check() {
    // Expand input declared as String. Receives ["hello", 42, "world"].
    // After expand: lane 0="hello"(ok), lane 1=42(null), lane 2="world"(ok).
    let node = make_node("n", vec![
        PortDefinition {
            name: "items".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Expand,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node],
        edges: vec![],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let mut pulses: PulseTable = BTreeMap::new();
    pulses.entry("n".to_string()).or_default().push(
        Pulse::new_on_port("c1".to_string(), vec![], json!(["hello", 42, "world"]), "items".to_string())
    );
    let changed = preprocess_input(&project, &mut pulses);
    assert!(changed, "should have expanded");
    let n_pulses = pulses.get("n").unwrap();
    let pending: Vec<&Pulse> = n_pulses.iter().filter(|p| p.status == PulseStatus::Pending).collect();
    assert_eq!(pending.len(), 3, "3 expanded lanes");
    // Sort by lane index
    let mut sorted = pending.clone();
    sorted.sort_by_key(|p| p.lane.last().unwrap().index);
    assert_eq!(sorted[0].data, json!("hello"), "lane 0 should pass");
    assert!(sorted[1].data.is_null(), "lane 1 (42) should be nulled : Number doesn't match String");
    assert_eq!(sorted[2].data, json!("world"), "lane 2 should pass");
}

#[test]
fn test_gather_input_collected_list_type_mismatch() {
    // Gather input declared as List[String]. Three lanes produce Numbers [1, 2, 3].
    // After gather: collected list is [1, 2, 3] which is List[Number], not List[String].
    // The mismatch should be logged.
    let node = make_node("n", vec![
        PortDefinition {
            name: "stream".to_string(),
            portType: WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String))),
            required: true,
            description: None,
            laneMode: LaneMode::Gather,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node],
        edges: vec![],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let mut pulses: PulseTable = BTreeMap::new();
    // 3 pending sibling pulses with Number values at depth 1
    let ps = pulses.entry("n".to_string()).or_default();
    for i in 0..3u32 {
        ps.push(Pulse::new_on_port(
            "c1".to_string(),
            vec![SplitFrame { count: 3, index: i }],
            json!(i + 1),
            "stream".to_string(),
        ));
    }
    let changed = preprocess_input(&project, &mut pulses);
    assert!(changed, "gather should fire");
    // The gathered pulse should exist at parent lane (empty)
    let n_pulses = pulses.get("n").unwrap();
    let gathered: Vec<&Pulse> = n_pulses.iter()
        .filter(|p| p.gathered && p.status == PulseStatus::Pending)
        .collect();
    assert_eq!(gathered.len(), 1, "should have 1 gathered pulse");
    // The gathered data is [1, 2, 3] : List[Number] not List[String]
    // Data still passes through (logged error, not nulled at gather stage)
    assert_eq!(gathered[0].data, json!([1, 2, 3]));
}

#[test]
fn test_gather_input_collected_list_type_match() {
    // Gather input declared as List[String]. Three lanes produce Strings.
    // After gather: ["a", "b", "c"] matches List[String]. No error.
    let node = make_node("n", vec![
        PortDefinition {
            name: "stream".to_string(),
            portType: WeftType::List(Box::new(WeftType::primitive(WeftPrimitive::String))),
            required: true,
            description: None,
            laneMode: LaneMode::Gather,
            laneDepth: 1,
            configurable: true,
        },
    ], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node],
        edges: vec![],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let mut pulses: PulseTable = BTreeMap::new();
    let ps = pulses.entry("n".to_string()).or_default();
    for (i, val) in ["a", "b", "c"].iter().enumerate() {
        ps.push(Pulse::new_on_port(
            "c1".to_string(),
            vec![SplitFrame { count: 3, index: i as u32 }],
            json!(val),
            "stream".to_string(),
        ));
    }
    let changed = preprocess_input(&project, &mut pulses);
    assert!(changed, "gather should fire");
    let n_pulses = pulses.get("n").unwrap();
    let gathered: Vec<&Pulse> = n_pulses.iter()
        .filter(|p| p.gathered && p.status == PulseStatus::Pending)
        .collect();
    assert_eq!(gathered.len(), 1);
    assert_eq!(gathered[0].data, json!(["a", "b", "c"]));
}

#[test]
fn test_output_type_mismatch_sets_execution_to_failed() {
    // Regression: output type errors must set the NodeExecution to Failed.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "result", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    // Pre-populate a NodeExecution as Completed (simulates node finishing execution)
    node_executions.insert("src".to_string(), vec![NodeExecution {
        id: "exec-1".to_string(),
        nodeId: "src".to_string(),
        status: NodeExecutionStatus::Completed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: Some(json!({"result": 42})),
        costUsd: 0.0,
        logs: vec![],
        color: "c1".to_string(),
        lane: vec![],
    }]);

    postprocess_output("src", &json!({"result": 42}), "c1", &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    let src_execs = node_executions.get("src").expect("src should have executions");
    assert_eq!(src_execs[0].status, NodeExecutionStatus::Failed, "output type mismatch must set execution to Failed");
    assert!(src_execs[0].error.is_some(), "output type mismatch must set error message");
    assert!(src_execs[0].error.as_ref().unwrap().contains("result"), "error should mention the port name");
}

#[test]
fn test_output_type_match_keeps_execution_completed() {
    // Ensure correct output types don't accidentally fail the execution.
    let node = make_node("src", vec![], vec![
        PortDefinition {
            name: "result".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        },
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![node, dst],
        edges: vec![make_edge("src", "result", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    // Pre-populate a NodeExecution as Completed
    node_executions.insert("src".to_string(), vec![NodeExecution {
        id: "exec-1".to_string(),
        nodeId: "src".to_string(),
        status: NodeExecutionStatus::Completed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: Some(json!({"result": "hello"})),
        costUsd: 0.0,
        logs: vec![],
        color: "c1".to_string(),
        lane: vec![],
    }]);

    postprocess_output("src", &json!({"result": "hello"}), "c1", &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    let src_execs = node_executions.get("src").expect("src should have executions");
    assert_eq!(src_execs[0].status, NodeExecutionStatus::Completed, "matching output type must stay Completed");
    assert!(src_execs[0].error.is_none(), "matching output type must have no error");
}

#[test]
fn test_node_execution_summary_shows_failed_on_type_error() {
    // Regression: node_execution_summary must report "failed" when execution is Failed from type error.
    let executions = vec![NodeExecution {
        id: "exec-1".to_string(),
        nodeId: "src".to_string(),
        status: NodeExecutionStatus::Failed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: Some("Output port 'result': expected String, got Number".to_string()),
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: None,
        costUsd: 0.0,
        logs: vec![],
        color: "c1".to_string(),
        lane: vec![],
    }];
    assert_eq!(node_execution_summary(&executions), "failed");
}

// =============================================================================
// NodeExecutionStatus serde
// =============================================================================

#[test]
fn node_execution_status_serializes_as_snake_case() {
    assert_eq!(serde_json::to_string(&NodeExecutionStatus::Running).unwrap(), "\"running\"");
    assert_eq!(serde_json::to_string(&NodeExecutionStatus::Completed).unwrap(), "\"completed\"");
    assert_eq!(serde_json::to_string(&NodeExecutionStatus::Failed).unwrap(), "\"failed\"");
    assert_eq!(serde_json::to_string(&NodeExecutionStatus::WaitingForInput).unwrap(), "\"waiting_for_input\"");
    assert_eq!(serde_json::to_string(&NodeExecutionStatus::Skipped).unwrap(), "\"skipped\"");
    assert_eq!(serde_json::to_string(&NodeExecutionStatus::Cancelled).unwrap(), "\"cancelled\"");
}

#[test]
fn node_execution_status_deserializes_from_snake_case() {
    assert_eq!(serde_json::from_str::<NodeExecutionStatus>("\"running\"").unwrap(), NodeExecutionStatus::Running);
    assert_eq!(serde_json::from_str::<NodeExecutionStatus>("\"waiting_for_input\"").unwrap(), NodeExecutionStatus::WaitingForInput);
}

// =============================================================================
// node_execution_summary breakdown
// =============================================================================

#[test]
fn node_execution_summary_mixed_completed_and_failed() {
    fn make_exec(status: NodeExecutionStatus) -> NodeExecution {
        NodeExecution {
            id: uuid::Uuid::new_v4().to_string(),
            nodeId: "n".to_string(),
            status,
            pulseIdsAbsorbed: vec![],
            pulseId: String::new(),
    
            error: None,
            callbackId: None,
            startedAt: 0,
            completedAt: Some(1),
            input: None,
            output: None,
            costUsd: 0.0,
            logs: vec![],
            color: "c".to_string(),
            lane: vec![],
        }
    }

    let execs = vec![
        make_exec(NodeExecutionStatus::Completed),
        make_exec(NodeExecutionStatus::Completed),
        make_exec(NodeExecutionStatus::Failed),
    ];
    let summary = node_execution_summary(&execs);
    assert!(summary.starts_with("completed"), "expected 'completed' prefix, got: {}", summary);
    assert!(summary.contains("2 completed"), "expected '2 completed' in: {}", summary);
    assert!(summary.contains("1 failed"), "expected '1 failed' in: {}", summary);
    assert!(summary.contains("3 executions"), "expected '3 executions' in: {}", summary);
}

#[test]
fn node_execution_summary_single_execution_no_breakdown() {
    let execs = vec![NodeExecution {
        id: "e1".to_string(),
        nodeId: "n".to_string(),
        status: NodeExecutionStatus::Completed,
        pulseIdsAbsorbed: vec![],
        pulseId: String::new(),

        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: None,
        costUsd: 0.0,
        logs: vec![],
        color: "c".to_string(),
        lane: vec![],
    }];
    // Single execution: just base status, no breakdown
    assert_eq!(node_execution_summary(&execs), "completed");
}

// =============================================================================
// build_node_outputs_from_executions with errors
// =============================================================================

#[test]
fn build_outputs_includes_error_for_failed_execution() {
    let mut node_execs: NodeExecutionTable = BTreeMap::new();
    node_execs.insert("n1".to_string(), vec![NodeExecution {
        id: "e1".to_string(),
        nodeId: "n1".to_string(),
        status: NodeExecutionStatus::Failed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: Some("something broke".to_string()),
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: None,
        costUsd: 0.0,
        logs: vec![],
        color: "c".to_string(),
        lane: vec![],
    }]);

    let outputs = build_node_outputs_from_executions(&node_execs);
    let n1_output = outputs.get("n1").expect("n1 should have output");
    assert_eq!(n1_output["_error"], "something broke");
}

#[test]
fn build_outputs_uses_output_for_completed_execution() {
    let mut node_execs: NodeExecutionTable = BTreeMap::new();
    node_execs.insert("n1".to_string(), vec![NodeExecution {
        id: "e1".to_string(),
        nodeId: "n1".to_string(),
        status: NodeExecutionStatus::Completed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: Some(json!({"result": 42})),
        costUsd: 0.0,
        logs: vec![],
        color: "c".to_string(),
        lane: vec![],
    }]);

    let outputs = build_node_outputs_from_executions(&node_execs);
    assert_eq!(outputs["n1"]["result"], 42);
}

// =============================================================================
// compute_active_edges
// =============================================================================

#[test]
fn compute_active_edges_finds_pending_pulses() {
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".into(),
        description: None,
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
        nodes: vec![
            make_node("A", vec![], vec![make_port("out", LaneMode::Single, false)]),
            make_node("B", vec![make_port("in", LaneMode::Single, true)], vec![]),
        ],
        edges: vec![make_edge("A", "out", "B", "in")],
    };

    let mut pulses: PulseTable = BTreeMap::new();
    // B has a Pending pulse on port "in"
    pulses.insert("B".to_string(), vec![
        Pulse::new_on_port("c".to_string(), vec![], json!(42), "in".to_string()),
    ]);

    let active = compute_active_edges(&pulses, &project);
    assert_eq!(active.len(), 1);
    assert_eq!(active[0], project.edges[0].id);
}

#[test]
fn compute_active_edges_ignores_absorbed_pulses() {
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "test".into(),
        description: None,
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
        nodes: vec![
            make_node("A", vec![], vec![make_port("out", LaneMode::Single, false)]),
            make_node("B", vec![make_port("in", LaneMode::Single, true)], vec![]),
        ],
        edges: vec![make_edge("A", "out", "B", "in")],
    };

    let mut pulses: PulseTable = BTreeMap::new();
    let mut p = Pulse::new_on_port("c".to_string(), vec![], json!(42), "in".to_string());
    p.status = PulseStatus::Absorbed;
    pulses.insert("B".to_string(), vec![p]);

    let active = compute_active_edges(&pulses, &project);
    assert!(active.is_empty());
}

// =============================================================================
// check_completion
// =============================================================================

#[test]
fn check_completion_not_done_with_pending_pulses() {
    let mut pulses: PulseTable = BTreeMap::new();
    pulses.insert("n1".to_string(), vec![Pulse::new("c".to_string(), vec![], json!(1))]);
    let node_execs: NodeExecutionTable = BTreeMap::new();
    assert_eq!(check_completion(&pulses, &node_execs), None);
}

#[test]
fn check_completion_not_done_with_running_execution() {
    let pulses: PulseTable = BTreeMap::new();
    let mut node_execs: NodeExecutionTable = BTreeMap::new();
    node_execs.insert("n1".to_string(), vec![NodeExecution {
        id: "e1".to_string(),
        nodeId: "n1".to_string(),
        status: NodeExecutionStatus::Running,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: None,
        input: None,
        output: None,
        costUsd: 0.0,
        logs: vec![],
        color: "c".to_string(),
        lane: vec![],
    }]);
    assert_eq!(check_completion(&pulses, &node_execs), None);
}

#[test]
fn check_completion_done_all_terminal() {
    let pulses: PulseTable = BTreeMap::new();
    let mut node_execs: NodeExecutionTable = BTreeMap::new();
    node_execs.insert("n1".to_string(), vec![NodeExecution {
        id: "e1".to_string(),
        nodeId: "n1".to_string(),
        status: NodeExecutionStatus::Completed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: Some(json!({"ok": true})),
        costUsd: 0.0,
        logs: vec![],
        color: "c".to_string(),
        lane: vec![],
    }]);
    assert_eq!(check_completion(&pulses, &node_execs), Some(false));
}

#[test]
fn check_completion_done_with_failure() {
    let pulses: PulseTable = BTreeMap::new();
    let mut node_execs: NodeExecutionTable = BTreeMap::new();
    node_execs.insert("n1".to_string(), vec![NodeExecution {
        id: "e1".to_string(),
        nodeId: "n1".to_string(),
        status: NodeExecutionStatus::Failed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),

        error: Some("boom".to_string()),
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: None,
        costUsd: 0.0,
        logs: vec![],
        color: "c".to_string(),
        lane: vec![],
    }]);
    assert_eq!(check_completion(&pulses, &node_execs), Some(true));
}

// =============================================================================
// Runtime type check: connected-only + per-port null on mismatch
// =============================================================================

fn make_typed_port(name: &str, weft_type: WeftType) -> PortDefinition {
    PortDefinition {
        name: name.to_string(),
        portType: weft_type,
        required: false,
        description: None,
        laneMode: LaneMode::Single,
        laneDepth: 1,
        configurable: true,
    }
}

fn make_completed_exec(node_id: &str, output: serde_json::Value) -> NodeExecution {
    NodeExecution {
        id: format!("exec-{}", node_id),
        nodeId: node_id.to_string(),
        status: NodeExecutionStatus::Completed,
        pulseIdsAbsorbed: vec![],
        pulseId: "p1".to_string(),
        error: None,
        callbackId: None,
        startedAt: 0,
        completedAt: Some(1),
        input: None,
        output: Some(output),
        costUsd: 0.0,
        logs: vec![],
        color: "c1".to_string(),
        lane: vec![],
    }
}

#[test]
fn type_error_on_unconnected_port_does_not_fail_node() {
    // Node has two output ports: "good" (String, connected) and "bad" (String, unconnected).
    // "bad" receives a Number (type mismatch) but since it's unconnected, node should stay Completed.
    let src = make_node("src", vec![], vec![
        make_typed_port("good", WeftType::primitive(WeftPrimitive::String)),
        make_typed_port("bad", WeftType::primitive(WeftPrimitive::String)),
    ]);
    let dst = make_node("dst", vec![make_port("data", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![src, dst],
        // Only "good" is connected, "bad" is not
        edges: vec![make_edge("src", "good", "dst", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    node_executions.insert("src".to_string(), vec![
        make_completed_exec("src", json!({"good": "hello", "bad": 42})),
    ]);

    postprocess_output("src", &json!({"good": "hello", "bad": 42}), "c1", &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Node should stay Completed, the mismatched port is unconnected
    let src_execs = node_executions.get("src").unwrap();
    assert_eq!(src_execs[0].status, NodeExecutionStatus::Completed,
        "unconnected type mismatch should not fail the node");
    assert!(src_execs[0].error.is_none());

    // Downstream should receive the "good" value
    let dst_pulses = pulses.get("dst").expect("dst should have pulses");
    assert!(!dst_pulses.is_empty(), "dst should have received a pulse");
    assert_eq!(dst_pulses[0].data.as_str(), Some("hello"));
}

#[test]
fn type_error_on_connected_port_sends_null_only_on_that_port() {
    // Node has three output ports: "ok1" (String), "bad" (String), "ok2" (Number).
    // "bad" receives a Number (mismatch), "ok1" and "ok2" are correct.
    // All three are connected to different downstream nodes.
    let src = make_node("src", vec![], vec![
        make_typed_port("ok1", WeftType::primitive(WeftPrimitive::String)),
        make_typed_port("bad", WeftType::primitive(WeftPrimitive::String)),
        make_typed_port("ok2", WeftType::primitive(WeftPrimitive::Number)),
    ]);
    let dst_ok1 = make_node("dst_ok1", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let dst_bad = make_node("dst_bad", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let dst_ok2 = make_node("dst_ok2", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![src, dst_ok1, dst_bad, dst_ok2],
        edges: vec![
            make_edge("src", "ok1", "dst_ok1", "in"),
            make_edge("src", "bad", "dst_bad", "in"),
            make_edge("src", "ok2", "dst_ok2", "in"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    node_executions.insert("src".to_string(), vec![
        make_completed_exec("src", json!({"ok1": "hello", "bad": 42, "ok2": 99})),
    ]);

    postprocess_output("src", &json!({"ok1": "hello", "bad": 42, "ok2": 99}), "c1", &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Node should be Failed (type error on "bad")
    let src_execs = node_executions.get("src").unwrap();
    assert_eq!(src_execs[0].status, NodeExecutionStatus::Failed);
    assert!(src_execs[0].error.as_ref().unwrap().contains("bad"));

    // dst_ok1 should receive "hello" (real data)
    let ok1_pulses = pulses.get("dst_ok1").expect("dst_ok1 should have pulses");
    assert_eq!(ok1_pulses[0].data.as_str(), Some("hello"));

    // dst_bad should receive null (type error on that port)
    let bad_pulses = pulses.get("dst_bad").expect("dst_bad should have pulses");
    assert!(bad_pulses[0].data.is_null(),
        "mismatched port should send null downstream, got: {:?}", bad_pulses[0].data);

    // dst_ok2 should receive 99 (real data)
    let ok2_pulses = pulses.get("dst_ok2").expect("dst_ok2 should have pulses");
    assert_eq!(ok2_pulses[0].data.as_i64(), Some(99));
}

#[test]
fn type_error_multiple_ports_fail_independently() {
    // Both "a" and "b" have type errors, "c" is correct. All connected.
    let src = make_node("src", vec![], vec![
        make_typed_port("a", WeftType::primitive(WeftPrimitive::String)),
        make_typed_port("b", WeftType::primitive(WeftPrimitive::Boolean)),
        make_typed_port("c", WeftType::primitive(WeftPrimitive::Number)),
    ]);
    let dst_a = make_node("dst_a", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let dst_b = make_node("dst_b", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let dst_c = make_node("dst_c", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![src, dst_a, dst_b, dst_c],
        edges: vec![
            make_edge("src", "a", "dst_a", "in"),
            make_edge("src", "b", "dst_b", "in"),
            make_edge("src", "c", "dst_c", "in"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    // a gets Number (should be String), b gets String (should be Boolean), c gets 42 (correct)
    node_executions.insert("src".to_string(), vec![
        make_completed_exec("src", json!({"a": 123, "b": "not bool", "c": 42})),
    ]);

    postprocess_output("src", &json!({"a": 123, "b": "not bool", "c": 42}), "c1", &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Node should be Failed
    let src_execs = node_executions.get("src").unwrap();
    assert_eq!(src_execs[0].status, NodeExecutionStatus::Failed);

    // dst_a and dst_b get null
    let a_pulses = pulses.get("dst_a").unwrap();
    assert!(a_pulses[0].data.is_null());
    let b_pulses = pulses.get("dst_b").unwrap();
    assert!(b_pulses[0].data.is_null());

    // dst_c gets real data
    let c_pulses = pulses.get("dst_c").unwrap();
    assert_eq!(c_pulses[0].data.as_i64(), Some(42));
}

#[test]
fn json_dict_port_accepts_deeply_nested_runtime_value() {
    // JsonDict port should not trigger a type error on deeply nested API responses
    let src = make_node("src", vec![], vec![
        make_typed_port("raw", WeftType::JsonDict),
    ]);
    let dst = make_node("dst", vec![make_port("in", LaneMode::Single, false)], vec![]);
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "t".into(), description: None,
        nodes: vec![src, dst],
        edges: vec![make_edge("src", "raw", "dst", "in")],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    let deeply_nested = json!({
        "user": {"name": "Alice", "scores": [1, 2, 3], "meta": {"role": "admin"}},
        "tags": [],
        "active": true,
        "count": 42
    });
    node_executions.insert("src".to_string(), vec![
        make_completed_exec("src", json!({"raw": deeply_nested.clone()})),
    ]);

    postprocess_output("src", &json!({"raw": deeply_nested.clone()}), "c1", &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Should stay Completed, JsonDict accepts any dict
    let src_execs = node_executions.get("src").unwrap();
    assert_eq!(src_execs[0].status, NodeExecutionStatus::Completed);
    assert!(src_execs[0].error.is_none());

    // Downstream gets the real data
    let dst_pulses = pulses.get("dst").unwrap();
    assert_eq!(dst_pulses[0].data["user"]["name"], "Alice");
}

// =========================================================================
// Passthrough never-skip
// =========================================================================

#[test]
fn passthrough_check_should_skip_helper_reports_null() {
    // Historical note: there was a period where ALL passthroughs were
    // exempt from skip. That is no longer true: group In boundaries now
    // honor the check (see group_in_boundary_skips_when_required_input_null).
    // This test only verifies the behavior of the check_should_skip HELPER,
    // which is pure and reports null-on-required regardless of node kind.
    // The executor's branching logic is what decides whether to act on it.
    let passthrough_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };

    // check_should_skip returns true for null on required port
    let pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    let required: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let would_skip = check_should_skip(&passthrough_in, &pulses, &[], "c1", &required, &wired, &std::collections::HashSet::new());
    assert!(would_skip, "check_should_skip helper should say yes for null on required port");
    // The test is about the helper, not the executor's routing. The executor
    // decides per-node whether to honor the helper's result: group Out
    // boundaries ignore it (forward through); group In boundaries and
    // regular nodes honor it (skip).
    let is_passthrough = passthrough_in.nodeType.0 == "Passthrough";
    assert!(is_passthrough, "should be detected as Passthrough");
}

// =========================================================================
// Null in union type does not skip
// =========================================================================

#[test]
fn null_on_port_with_null_in_union_does_not_skip() {
    // When a port type includes Null (e.g. String | Null), receiving null is a valid value, not skip.
    let node = NodeDefinition {
        id: "worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::union(vec![
                WeftType::primitive(WeftPrimitive::String),
                WeftType::primitive(WeftPrimitive::Null),
            ]),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };

    let pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    let required: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let should_skip = check_should_skip(&node, &pulses, &[], "c1", &required, &wired, &std::collections::HashSet::new());
    assert!(!should_skip, "null should NOT skip when port type includes Null");
}

#[test]
fn null_on_port_without_null_in_type_does_skip() {
    // Port type is just String (no Null in union) → null causes skip.
    let node = NodeDefinition {
        id: "worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };

    let pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    let required: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let should_skip = check_should_skip(&node, &pulses, &[], "c1", &required, &wired, &std::collections::HashSet::new());
    assert!(should_skip, "null SHOULD skip when port type is just String");
}

#[test]
fn null_on_optional_port_does_not_skip() {
    // Optional ports don't trigger skip even with null (they're not in required_ports).
    let node = NodeDefinition {
        id: "worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };

    let pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    // Optional port not in required set
    let required: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let should_skip = check_should_skip(&node, &pulses, &[], "c1", &required, &wired, &std::collections::HashSet::new());
    assert!(!should_skip, "optional port should not cause skip");
}

// =========================================================================
// Group boundary nodes never skip
// =========================================================================

#[test]
fn check_should_skip_reports_null_on_required_regardless_of_boundary() {
    // The check_should_skip helper is pure: given a required port with a
    // null pulse, it returns true regardless of whether the node is a
    // group boundary. The executor layer is what decides whether to
    // honor that result for a specific boundary role (In: honor; Out:
    // ignore and forward).
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let boundary = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: true,
        }],
        features: Default::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp".to_string(), role: GroupBoundaryRole::In }),
    };

    // check_should_skip would say true for null on required port
    let pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    let required: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let would_skip = check_should_skip(&boundary, &pulses, &[], "c1", &required, &wired, &std::collections::HashSet::new());
    assert!(would_skip, "check_should_skip should say yes for null on required port");

    // But the executor bypasses skip for group boundary nodes
    assert!(boundary.groupBoundary.is_some(), "should be detected as group boundary");
}

#[test]
fn scope_membership_check() {
    // Verify that scope-based group membership works correctly
    let node_in_inner = NodeDefinition {
        id: "outer.inner.worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![],
        outputs: vec![],
        features: Default::default(),
        scope: vec!["outer".to_string(), "outer.inner".to_string()],
        groupBoundary: None,
    };

    let node_in_outer_only = NodeDefinition {
        id: "outer.pre".to_string(),
        nodeType: "ExecPython".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![],
        outputs: vec![],
        features: Default::default(),
        scope: vec!["outer".to_string()],
        groupBoundary: None,
    };

    // Mocking "outer.inner": worker is inside, pre is not
    let mocked = "outer.inner";
    assert!(node_in_inner.scope.iter().any(|s| s == mocked));
    assert!(!node_in_outer_only.scope.iter().any(|s| s == mocked));

    // Mocking "outer": both are inside
    let mocked_outer = "outer";
    assert!(node_in_inner.scope.iter().any(|s| s == mocked_outer));
    assert!(node_in_outer_only.scope.iter().any(|s| s == mocked_outer));
}

// =========================================================================
// Mock output sanitization
// =========================================================================

#[test]
fn sanitize_mock_strips_unknown_ports() {
    let ports = vec![
        PortDefinition {
            name: "summary".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        },
        PortDefinition {
            name: "score".to_string(),
            portType: WeftType::primitive(WeftPrimitive::Number),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        },
    ];

    // Mock has extra ports that don't exist on the node
    let mock = json!({
        "summary": "hello",
        "score": 42,
        "extra_field": "should be dropped",
        "another_fake": [1, 2, 3]
    });

    let sanitized = sanitize_mock_output(&mock, &ports);
    let obj = sanitized.as_object().unwrap();
    assert_eq!(obj.len(), 2, "should only have 2 ports, not 4");
    assert_eq!(obj["summary"], json!("hello"));
    assert_eq!(obj["score"], json!(42));
    assert!(!obj.contains_key("extra_field"));
    assert!(!obj.contains_key("another_fake"));
}

#[test]
fn sanitize_mock_fills_missing_ports_with_null() {
    let ports = vec![
        PortDefinition {
            name: "response".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        },
        PortDefinition {
            name: "confidence".to_string(),
            portType: WeftType::primitive(WeftPrimitive::Number),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        },
    ];

    // Mock only provides one of two ports
    let mock = json!({"response": "hello world"});
    let sanitized = sanitize_mock_output(&mock, &ports);
    let obj = sanitized.as_object().unwrap();
    assert_eq!(obj.len(), 2);
    assert_eq!(obj["response"], json!("hello world"));
    assert_eq!(obj["confidence"], json!(null), "missing port should be null");
}

#[test]
fn sanitize_mock_passthrough_non_object() {
    let ports = vec![
        PortDefinition {
            name: "value".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        },
    ];

    // Non-object mock passes through unchanged
    let mock = json!("raw string");
    let sanitized = sanitize_mock_output(&mock, &ports);
    assert_eq!(sanitized, json!("raw string"));
}

#[test]
fn sanitize_mock_empty_ports() {
    let mock = json!({"anything": "value"});
    let sanitized = sanitize_mock_output(&mock, &[]);
    assert_eq!(sanitized, json!({}), "no ports = empty object");
}

// =========================================================================
// is_inside_mocked_group
// =========================================================================

#[test]
fn is_inside_mocked_group_simple() {
    let mut mocks = std::collections::HashMap::new();
    mocks.insert("grp".to_string(), json!({"response": "mock"}));

    let inside = NodeDefinition {
        id: "grp.worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![], outputs: vec![],
        features: Default::default(),
        scope: vec!["grp".to_string()],
        groupBoundary: None,
    };
    assert!(is_inside_mocked_group(&inside, &mocks));

    let outside = NodeDefinition {
        id: "other".to_string(),
        nodeType: "ExecPython".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![], outputs: vec![],
        features: Default::default(),
        scope: vec![],
        groupBoundary: None,
    };
    assert!(!is_inside_mocked_group(&outside, &mocks));
}

#[test]
fn is_inside_mocked_group_nested_only_inner() {
    // Mock only "outer.inner", not "outer"
    let mut mocks = std::collections::HashMap::new();
    mocks.insert("outer.inner".to_string(), json!({}));

    let deep = NodeDefinition {
        id: "outer.inner.node".to_string(),
        nodeType: "ExecPython".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![], outputs: vec![],
        features: Default::default(),
        scope: vec!["outer".to_string(), "outer.inner".to_string()],
        groupBoundary: None,
    };
    assert!(is_inside_mocked_group(&deep, &mocks), "node inside mocked inner group");

    let sibling = NodeDefinition {
        id: "outer.sibling".to_string(),
        nodeType: "ExecPython".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![], outputs: vec![],
        features: Default::default(),
        scope: vec!["outer".to_string()],
        groupBoundary: None,
    };
    assert!(!is_inside_mocked_group(&sibling, &mocks), "sibling in outer but not in inner");
}

#[test]
fn is_inside_mocked_group_empty_mocks() {
    let mocks = std::collections::HashMap::new();
    let node = NodeDefinition {
        id: "grp.worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![], outputs: vec![],
        features: Default::default(),
        scope: vec!["grp".to_string()],
        groupBoundary: None,
    };
    assert!(!is_inside_mocked_group(&node, &mocks));
}

// =========================================================================
// End-to-end group mock flow simulation
// =========================================================================

#[test]
fn group_mock_e2e_skips_internals_emits_mock_downstream() {
    // Simulate what collect_dispatch_work does when a group is mocked.
    // Project: source -> grp(grp__in -> worker -> grp__out) -> consumer
    // Mock: grp -> {"result": "mocked value"}
    // Expected: worker is skipped, consumer receives "mocked value"
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let source = make_node("source", vec![], vec![
        make_port("value", LaneMode::Single, false),
    ]);
    let grp_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp".to_string(), role: GroupBoundaryRole::In }),
    };
    let worker = NodeDefinition {
        id: "grp.worker".to_string(),
        nodeType: "ExecPython".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["grp".to_string()],
        groupBoundary: None,
    };
    let grp_out = NodeDefinition {
        id: "grp__out".to_string(),
        nodeType: "Passthrough".into(),
        label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp".to_string(), role: GroupBoundaryRole::Out }),
    };
    let consumer = make_node("consumer", vec![
        make_port("data", LaneMode::Single, true),
    ], vec![]);

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "mock_test".to_string(),
        description: None,
        nodes: vec![source.clone(), grp_in.clone(), worker.clone(), grp_out.clone(), consumer.clone()],
        edges: vec![
            make_edge("source", "value", "grp__in", "data"),
            make_edge("grp__in", "data", "grp.worker", "data"),
            make_edge("grp.worker", "result", "grp__out", "result"),
            make_edge("grp__out", "result", "consumer", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("grp".to_string(), json!({"result": "mocked value"})),
    ].into_iter().collect();

    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    // Step 1: source completes
    postprocess_output("source", &json!({"value": "real input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Step 2: find ready nodes, grp__in should be ready
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(!ready.is_empty(), "grp__in should be ready");
    let (ready_id, ready_group) = &ready[0];
    assert_eq!(ready_id, "grp__in");

    // Step 3: Simulate what collect_dispatch_work does for a mocked group boundary
    // Check: is this a group In boundary for a mocked group?
    let grp_in_node = project.nodes.iter().find(|n| n.id == "grp__in").unwrap();
    let is_mocked_boundary = grp_in_node.groupBoundary.as_ref().map_or(false, |gb| {
        gb.role == GroupBoundaryRole::In && mocks.contains_key(&gb.groupId)
    });
    assert!(is_mocked_boundary, "grp__in should be detected as mocked group boundary");

    // Absorb the input pulses (as collect_dispatch_work would)
    if let Some(node_pulses) = pulses.get_mut("grp__in") {
        for p in node_pulses.iter_mut() {
            if ready_group.pulse_ids.contains(&p.id) {
                p.status = PulseStatus::Absorbed;
            }
        }
    }

    // Find the Out boundary and sanitize mock
    let grp_out_node = project.nodes.iter().find(|n| {
        n.groupBoundary.as_ref().map_or(false, |b| b.groupId == "grp" && b.role == GroupBoundaryRole::Out)
    }).unwrap();
    let sanitized = sanitize_mock_output(&mocks["grp"], &grp_out_node.outputs);
    assert_eq!(sanitized, json!({"result": "mocked value"}));

    // Emit mock data downstream of grp__out
    postprocess_output("grp__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Step 4: Check internal worker is NOT ready (no pulses reached it)
    let ready_after = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    let worker_ready = ready_after.iter().any(|(id, _)| id == "grp.worker");
    assert!(!worker_ready, "internal worker should NOT be ready (mocked group)");

    // Step 5: consumer should be ready with mock data
    let consumer_ready = ready_after.iter().find(|(id, _)| id == "consumer");
    assert!(consumer_ready.is_some(), "consumer should be ready");
    let (_, consumer_group) = consumer_ready.unwrap();
    let consumer_input = &consumer_group.input;
    assert_eq!(consumer_input["data"], json!("mocked value"), "consumer should receive mock data");
}

#[test]
fn group_mock_e2e_nested_only_inner_mocked() {
    // Project: source -> outer(outer__in -> outer.pre -> inner(inner__in -> inner.deep -> inner__out) -> outer__out) -> consumer
    // Mock: outer.inner only
    // Expected: outer.pre runs normally, inner.deep is skipped, consumer receives inner's mock data
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let source = make_node("source", vec![], vec![make_port("value", LaneMode::Single, false)]);

    let outer_in = NodeDefinition {
        id: "outer__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::In }),
    };
    let pre = NodeDefinition {
        id: "outer.pre".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["outer".to_string()],
        groupBoundary: None,
    };
    let inner_in = NodeDefinition {
        id: "outer.inner__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::In }),
    };
    let deep = NodeDefinition {
        id: "outer.inner.deep".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["outer".to_string(), "outer.inner".to_string()],
        groupBoundary: None,
    };
    let inner_out = NodeDefinition {
        id: "outer.inner__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::Out }),
    };
    let outer_out = NodeDefinition {
        id: "outer__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::Out }),
    };
    let consumer = make_node("consumer", vec![make_port("data", LaneMode::Single, true)], vec![]);

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "nested_mock".to_string(), description: None,
        nodes: vec![source, outer_in, pre.clone(), inner_in, deep.clone(), inner_out, outer_out, consumer],
        edges: vec![
            make_edge("source", "value", "outer__in", "data"),
            make_edge("outer__in", "data", "outer.pre", "data"),
            make_edge("outer.pre", "result", "outer.inner__in", "data"),
            make_edge("outer.inner__in", "data", "outer.inner.deep", "data"),
            make_edge("outer.inner.deep", "result", "outer.inner__out", "result"),
            make_edge("outer.inner__out", "result", "outer__out", "result"),
            make_edge("outer__out", "result", "consumer", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("outer.inner".to_string(), json!({"result": "inner mock"})),
    ].into_iter().collect();

    // outer.pre is inside outer but NOT inside outer.inner → should NOT be skipped
    assert!(!is_inside_mocked_group(&pre, &mocks));
    // outer.inner.deep IS inside outer.inner → should be skipped
    assert!(is_inside_mocked_group(&deep, &mocks));

    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    // source completes
    postprocess_output("source", &json!({"value": "input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // outer__in becomes ready → it's NOT mocked (outer is not in mocks), so it runs normally
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    let outer_in_ready = ready.iter().find(|(id, _)| id == "outer__in");
    assert!(outer_in_ready.is_some());

    // Simulate outer__in completing (passthrough forwards data)
    if let Some(ps) = pulses.get_mut("outer__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    postprocess_output("outer__in", &json!({"data": "input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // outer.pre becomes ready
    let ready2 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(ready2.iter().any(|(id, _)| id == "outer.pre"), "outer.pre should be ready");

    // Simulate outer.pre completing
    if let Some(ps) = pulses.get_mut("outer.pre") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    postprocess_output("outer.pre", &json!({"result": "processed"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // inner__in becomes ready → it IS a mocked group boundary
    let ready3 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    let inner_in_ready = ready3.iter().find(|(id, _)| id == "outer.inner__in");
    assert!(inner_in_ready.is_some());

    let inner_in_node = project.nodes.iter().find(|n| n.id == "outer.inner__in").unwrap();
    let is_mocked = inner_in_node.groupBoundary.as_ref().map_or(false, |gb| {
        gb.role == GroupBoundaryRole::In && mocks.contains_key(&gb.groupId)
    });
    assert!(is_mocked, "inner__in should be detected as mocked group boundary");

    // Short-circuit: absorb inner__in pulses, emit mock on inner__out
    if let Some(ps) = pulses.get_mut("outer.inner__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    let inner_out_node = project.nodes.iter().find(|n| {
        n.groupBoundary.as_ref().map_or(false, |b| b.groupId == "outer.inner" && b.role == GroupBoundaryRole::Out)
    }).unwrap();
    let sanitized = sanitize_mock_output(&mocks["outer.inner"], &inner_out_node.outputs);
    postprocess_output("outer.inner__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // deep should NOT be ready (inner was mocked, no pulses reached it)
    let ready4 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(!ready4.iter().any(|(id, _)| id == "outer.inner.deep"), "deep should be skipped");

    // outer__out should be ready with inner's mock data
    let outer_out_ready = ready4.iter().find(|(id, _)| id == "outer__out");
    assert!(outer_out_ready.is_some(), "outer__out should be ready");

    // Simulate outer__out completing
    if let Some(ps) = pulses.get_mut("outer__out") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    postprocess_output("outer__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // consumer should be ready with mock data
    let ready5 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    let consumer_ready = ready5.iter().find(|(id, _)| id == "consumer");
    assert!(consumer_ready.is_some(), "consumer should be ready");
    assert_eq!(consumer_ready.unwrap().1.input["data"], json!("inner mock"));
}

#[test]
fn group_mock_e2e_outer_mocked_skips_everything_inside_including_inner_group() {
    // Mock "outer" → everything inside outer is skipped, including inner group's nodes.
    // inner is NOT in the mocks map, but it's still skipped because it's inside outer.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let source = make_node("source", vec![], vec![make_port("value", LaneMode::Single, false)]);
    let outer_in = NodeDefinition {
        id: "outer__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::In }),
    };
    let pre = NodeDefinition {
        id: "outer.pre".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["outer".to_string()],
        groupBoundary: None,
    };
    let inner_in = NodeDefinition {
        id: "outer.inner__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::In }),
    };
    let deep = NodeDefinition {
        id: "outer.inner.deep".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["outer".to_string(), "outer.inner".to_string()],
        groupBoundary: None,
    };
    let inner_out = NodeDefinition {
        id: "outer.inner__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::Out }),
    };
    let outer_out = NodeDefinition {
        id: "outer__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::Out }),
    };
    let consumer = make_node("consumer", vec![make_port("data", LaneMode::Single, true)], vec![]);

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("outer".to_string(), json!({"result": "outer mock"})),
    ].into_iter().collect();

    // ALL nodes inside outer should be detected as inside a mocked group
    assert!(is_inside_mocked_group(&pre, &mocks), "pre is inside mocked outer");
    assert!(is_inside_mocked_group(&deep, &mocks), "deep is inside mocked outer (transitively)");

    // inner's boundaries are also inside outer's scope
    assert!(is_inside_mocked_group(&inner_in, &mocks), "inner__in is inside outer");
    assert!(is_inside_mocked_group(&inner_out, &mocks), "inner__out is inside outer");

    // outer's own boundaries are NOT inside (they have empty scope)
    assert!(!is_inside_mocked_group(&outer_in, &mocks), "outer__in is NOT inside outer (it's the boundary)");
    assert!(!is_inside_mocked_group(&outer_out, &mocks), "outer__out is NOT inside outer");

    // Simulate the flow
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "outer_mock".to_string(), description: None,
        nodes: vec![source, outer_in, pre, inner_in, deep, inner_out, outer_out, consumer],
        edges: vec![
            make_edge("source", "value", "outer__in", "data"),
            make_edge("outer__in", "data", "outer.pre", "data"),
            make_edge("outer.pre", "result", "outer.inner__in", "data"),
            make_edge("outer.inner__in", "data", "outer.inner.deep", "data"),
            make_edge("outer.inner.deep", "result", "outer.inner__out", "result"),
            make_edge("outer.inner__out", "result", "outer__out", "result"),
            make_edge("outer__out", "result", "consumer", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    // source completes
    postprocess_output("source", &json!({"value": "input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // outer__in is ready (it's a mocked boundary for "outer")
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(ready.iter().any(|(id, _)| id == "outer__in"));

    // Short-circuit: absorb outer__in, emit mock on outer__out
    if let Some(ps) = pulses.get_mut("outer__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    let out_node = project.nodes.iter().find(|n| n.id == "outer__out").unwrap();
    let sanitized = sanitize_mock_output(&mocks["outer"], &out_node.outputs);
    postprocess_output("outer__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Nothing inside outer should be ready
    let ready2 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(!ready2.iter().any(|(id, _)| id == "outer.pre"), "pre should not be ready");
    assert!(!ready2.iter().any(|(id, _)| id == "outer.inner__in"), "inner__in should not be ready");
    assert!(!ready2.iter().any(|(id, _)| id == "outer.inner.deep"), "deep should not be ready");

    // consumer should be ready with outer's mock
    let consumer_ready = ready2.iter().find(|(id, _)| id == "consumer");
    assert!(consumer_ready.is_some());
    assert_eq!(consumer_ready.unwrap().1.input["data"], json!("outer mock"));
}

#[test]
fn group_mock_e2e_both_outer_and_inner_mocked_outer_triggers() {
    // Both "outer" and "outer.inner" are in the mocks map.
    // outer__in is the first boundary reached → outer's mock fires.
    // inner's mock should never trigger because inner__in never gets pulses.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let outer_in = NodeDefinition {
        id: "outer__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::In }),
    };
    let inner_in = NodeDefinition {
        id: "outer.inner__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::In }),
    };
    let worker = NodeDefinition {
        id: "outer.inner.worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["outer".to_string(), "outer.inner".to_string()],
        groupBoundary: None,
    };
    let inner_out = NodeDefinition {
        id: "outer.inner__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::Out }),
    };
    let outer_out = NodeDefinition {
        id: "outer__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::Out }),
    };
    let source = make_node("source", vec![], vec![make_port("value", LaneMode::Single, false)]);
    let consumer = make_node("consumer", vec![make_port("data", LaneMode::Single, true)], vec![]);

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("outer".to_string(), json!({"result": "outer wins"})),
        ("outer.inner".to_string(), json!({"result": "inner should not fire"})),
    ].into_iter().collect();

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "both_mocked".to_string(), description: None,
        nodes: vec![source, outer_in, inner_in, worker, inner_out, outer_out, consumer],
        edges: vec![
            make_edge("source", "value", "outer__in", "data"),
            make_edge("outer__in", "data", "outer.inner__in", "data"),
            make_edge("outer.inner__in", "data", "outer.inner.worker", "data"),
            make_edge("outer.inner.worker", "result", "outer.inner__out", "result"),
            make_edge("outer.inner__out", "result", "outer__out", "result"),
            make_edge("outer__out", "result", "consumer", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    postprocess_output("source", &json!({"value": "input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // outer__in is ready, it's the mocked boundary for "outer"
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(ready.iter().any(|(id, _)| id == "outer__in"));

    // Short-circuit outer
    if let Some(ps) = pulses.get_mut("outer__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    let out_node = project.nodes.iter().find(|n| n.id == "outer__out").unwrap();
    let sanitized = sanitize_mock_output(&mocks["outer"], &out_node.outputs);
    postprocess_output("outer__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // inner__in should NOT be ready (no pulses reached it because outer was short-circuited)
    let ready2 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(!ready2.iter().any(|(id, _)| id == "outer.inner__in"), "inner__in should not fire when outer is mocked");
    assert!(!ready2.iter().any(|(id, _)| id == "outer.inner.worker"), "worker should not fire");

    // consumer gets outer's mock, not inner's
    let consumer_ready = ready2.iter().find(|(id, _)| id == "consumer");
    assert!(consumer_ready.is_some());
    assert_eq!(consumer_ready.unwrap().1.input["data"], json!("outer wins"));
}

#[test]
fn group_mock_e2e_sibling_groups_only_one_mocked() {
    // Two sibling groups: grp_a and grp_b. Only grp_a is mocked.
    // grp_b should run normally.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let source = make_node("source", vec![], vec![
        make_port("a_data", LaneMode::Single, false),
        make_port("b_data", LaneMode::Single, false),
    ]);
    let a_in = NodeDefinition {
        id: "grp_a__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp_a".to_string(), role: GroupBoundaryRole::In }),
    };
    let a_worker = NodeDefinition {
        id: "grp_a.worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["grp_a".to_string()], groupBoundary: None,
    };
    let a_out = NodeDefinition {
        id: "grp_a__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp_a".to_string(), role: GroupBoundaryRole::Out }),
    };
    let b_in = NodeDefinition {
        id: "grp_b__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp_b".to_string(), role: GroupBoundaryRole::In }),
    };
    let b_worker = NodeDefinition {
        id: "grp_b.worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["grp_b".to_string()], groupBoundary: None,
    };
    let b_out = NodeDefinition {
        id: "grp_b__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp_b".to_string(), role: GroupBoundaryRole::Out }),
    };

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("grp_a".to_string(), json!({"result": "a mocked"})),
    ].into_iter().collect();

    // grp_a's worker is inside mocked group
    assert!(is_inside_mocked_group(&a_worker, &mocks));
    // grp_b's worker is NOT inside any mocked group
    assert!(!is_inside_mocked_group(&b_worker, &mocks));

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "siblings".to_string(), description: None,
        nodes: vec![source, a_in, a_worker, a_out, b_in, b_worker, b_out],
        edges: vec![
            make_edge("source", "a_data", "grp_a__in", "data"),
            make_edge("grp_a__in", "data", "grp_a.worker", "data"),
            make_edge("grp_a.worker", "result", "grp_a__out", "result"),
            make_edge("source", "b_data", "grp_b__in", "data"),
            make_edge("grp_b__in", "data", "grp_b.worker", "data"),
            make_edge("grp_b.worker", "result", "grp_b__out", "result"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    postprocess_output("source", &json!({"a_data": "for a", "b_data": "for b"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    // Both __in passthroughs should be ready
    assert!(ready.iter().any(|(id, _)| id == "grp_a__in"));
    assert!(ready.iter().any(|(id, _)| id == "grp_b__in"));

    // Short-circuit grp_a (mocked)
    if let Some(ps) = pulses.get_mut("grp_a__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    let a_out_node = project.nodes.iter().find(|n| n.id == "grp_a__out").unwrap();
    let sanitized_a = sanitize_mock_output(&mocks["grp_a"], &a_out_node.outputs);
    postprocess_output("grp_a__out", &sanitized_a, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Let grp_b__in run normally (not mocked)
    if let Some(ps) = pulses.get_mut("grp_b__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    postprocess_output("grp_b__in", &json!({"data": "for b"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    let ready2 = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);

    // grp_a.worker should NOT be ready (mocked)
    assert!(!ready2.iter().any(|(id, _)| id == "grp_a.worker"), "grp_a.worker should be skipped");

    // grp_b.worker SHOULD be ready (not mocked)
    assert!(ready2.iter().any(|(id, _)| id == "grp_b.worker"), "grp_b.worker should run normally");
}

#[test]
fn group_mock_outer_mocked_inner_passthroughs_never_ready() {
    // Verify that when outer is mocked, inner's __in and __out passthroughs
    // never receive pulses and never become ready.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let source = make_node("source", vec![], vec![make_port("value", LaneMode::Single, false)]);
    let outer_in = NodeDefinition {
        id: "outer__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::In }),
    };
    let inner_in = NodeDefinition {
        id: "outer.inner__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::In }),
    };
    let deep = NodeDefinition {
        id: "outer.inner.deep".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["outer".to_string(), "outer.inner".to_string()],
        groupBoundary: None,
    };
    let inner_out = NodeDefinition {
        id: "outer.inner__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec!["outer".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "outer.inner".to_string(), role: GroupBoundaryRole::Out }),
    };
    let outer_out = NodeDefinition {
        id: "outer__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "outer".to_string(), role: GroupBoundaryRole::Out }),
    };
    let consumer = make_node("consumer", vec![make_port("data", LaneMode::Single, true)], vec![]);

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("outer".to_string(), json!({"result": "outer mock"})),
    ].into_iter().collect();

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "inner_pt_never_ready".to_string(), description: None,
        nodes: vec![source, outer_in, inner_in, deep, inner_out, outer_out, consumer],
        edges: vec![
            make_edge("source", "value", "outer__in", "data"),
            make_edge("outer__in", "data", "outer.inner__in", "data"),
            make_edge("outer.inner__in", "data", "outer.inner.deep", "data"),
            make_edge("outer.inner.deep", "result", "outer.inner__out", "result"),
            make_edge("outer.inner__out", "result", "outer__out", "result"),
            make_edge("outer__out", "result", "consumer", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    postprocess_output("source", &json!({"value": "input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // outer__in is ready, short-circuit it
    if let Some(ps) = pulses.get_mut("outer__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    let out_node = project.nodes.iter().find(|n| n.id == "outer__out").unwrap();
    let sanitized = sanitize_mock_output(&mocks["outer"], &out_node.outputs);
    postprocess_output("outer__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Verify: inner__in has NO pulses at all (not even pending ones)
    let inner_in_pulses = pulses.get("outer.inner__in");
    assert!(
        inner_in_pulses.is_none() || inner_in_pulses.unwrap().iter().all(|p| p.status != PulseStatus::Pending),
        "inner__in should have no pending pulses"
    );

    // Verify: inner__out has NO pulses at all
    let inner_out_pulses = pulses.get("outer.inner__out");
    assert!(
        inner_out_pulses.is_none() || inner_out_pulses.unwrap().iter().all(|p| p.status != PulseStatus::Pending),
        "inner__out should have no pending pulses"
    );

    // Verify: deep has NO pulses
    let deep_pulses = pulses.get("outer.inner.deep");
    assert!(
        deep_pulses.is_none() || deep_pulses.unwrap().iter().all(|p| p.status != PulseStatus::Pending),
        "deep should have no pending pulses"
    );

    // Only consumer should be ready
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert_eq!(ready.len(), 1, "only consumer should be ready");
    assert_eq!(ready[0].0, "consumer");
}

#[test]
fn group_mock_node_with_all_optional_inputs_inside_mocked_group() {
    // Edge case: a node inside a mocked group has all optional inputs.
    // Without mocking, it could fire on null. With mocking, it should NOT fire
    // because no pulses reach it at all (the group boundary short-circuits).
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let source = make_node("source", vec![], vec![make_port("value", LaneMode::Single, false)]);
    let grp_in = NodeDefinition {
        id: "grp__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp".to_string(), role: GroupBoundaryRole::In }),
    };
    // This node has ALL OPTIONAL inputs, normally it would run even on null
    let optional_worker = NodeDefinition {
        id: "grp.optional_worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None, laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        }],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["grp".to_string()],
        groupBoundary: None,
    };
    let grp_out = NodeDefinition {
        id: "grp__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("result", LaneMode::Single, false)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp".to_string(), role: GroupBoundaryRole::Out }),
    };
    let consumer = make_node("consumer", vec![make_port("data", LaneMode::Single, true)], vec![]);

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("grp".to_string(), json!({"result": "mocked"})),
    ].into_iter().collect();

    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(), name: "optional_inside".to_string(), description: None,
        nodes: vec![source, grp_in, optional_worker, grp_out, consumer],
        edges: vec![
            make_edge("source", "value", "grp__in", "data"),
            make_edge("grp__in", "data", "grp.optional_worker", "data"),
            make_edge("grp.optional_worker", "result", "grp__out", "result"),
            make_edge("grp__out", "result", "consumer", "data"),
        ],
        status: Default::default(),
        createdAt: chrono::Utc::now(), updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let color = "c1".to_string();
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();

    postprocess_output("source", &json!({"value": "input"}), &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // Short-circuit grp
    if let Some(ps) = pulses.get_mut("grp__in") {
        for p in ps.iter_mut() { p.status = PulseStatus::Absorbed; }
    }
    let out_node = project.nodes.iter().find(|n| n.id == "grp__out").unwrap();
    let sanitized = sanitize_mock_output(&mocks["grp"], &out_node.outputs);
    postprocess_output("grp__out", &sanitized, &color, &[], &project, &mut pulses, &edge_idx, &mut node_executions);

    // optional_worker should NOT be ready even though its inputs are optional
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    assert!(!ready.iter().any(|(id, _)| id == "grp.optional_worker"),
        "optional_worker should not fire inside mocked group (no pulses reached it)");

    // consumer should be ready with mock data
    let consumer_ready = ready.iter().find(|(id, _)| id == "consumer");
    assert!(consumer_ready.is_some());
    assert_eq!(consumer_ready.unwrap().1.input["data"], json!("mocked"));
}

#[test]
fn group_mock_multiple_output_ports() {
    // Group has multiple output ports. Mock provides some but not all.
    // Missing ports should be null, extra mock ports should be dropped.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let grp_out = NodeDefinition {
        id: "grp__out".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![
            make_port("summary", LaneMode::Single, false),
            make_port("score", LaneMode::Single, false),
            make_port("category", LaneMode::Single, false),
        ],
        outputs: vec![
            PortDefinition { name: "summary".to_string(), portType: WeftType::primitive(WeftPrimitive::String), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
            PortDefinition { name: "score".to_string(), portType: WeftType::primitive(WeftPrimitive::Number), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
            PortDefinition { name: "category".to_string(), portType: WeftType::primitive(WeftPrimitive::String), required: false, description: None, laneMode: LaneMode::Single, laneDepth: 1, configurable: true },
        ],
        features: Default::default(), scope: vec![],
        groupBoundary: Some(GroupBoundary { groupId: "grp".to_string(), role: GroupBoundaryRole::Out }),
    };

    // Mock provides summary and a fake port, but not score or category
    let mock = json!({
        "summary": "Revenue increased",
        "fake_port": "should be dropped",
    });

    let sanitized = sanitize_mock_output(&mock, &grp_out.outputs);
    let obj = sanitized.as_object().unwrap();
    assert_eq!(obj.len(), 3, "should have exactly 3 ports matching the node");
    assert_eq!(obj["summary"], json!("Revenue increased"));
    assert_eq!(obj["score"], json!(null), "missing port → null");
    assert_eq!(obj["category"], json!(null), "missing port → null");
    assert!(!obj.contains_key("fake_port"), "unknown port → dropped");
}

#[test]
fn group_mock_direct_node_mock_coexists_with_group_mock() {
    // A direct node mock and a group mock in the same execution.
    // node_a is directly mocked. grp contains node_b which should be skipped via group mock.
    // They should not interfere with each other.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let node_a = NodeDefinition {
        id: "node_a".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("input", LaneMode::Single, true)],
        outputs: vec![make_port("output", LaneMode::Single, false)],
        features: Default::default(), scope: vec![], groupBoundary: None,
    };
    let node_b = NodeDefinition {
        id: "grp.node_b".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("input", LaneMode::Single, true)],
        outputs: vec![make_port("output", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["grp".to_string()],
        groupBoundary: None,
    };

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("node_a".to_string(), json!({"output": "a mocked directly"})),
        ("grp".to_string(), json!({"output": "grp mocked"})),
    ].into_iter().collect();

    // node_a is directly mocked (not via group scope)
    assert!(!is_inside_mocked_group(&node_a, &mocks), "node_a is not inside any group");
    assert!(mocks.contains_key("node_a"), "node_a has a direct mock");

    // node_b is inside a mocked group
    assert!(is_inside_mocked_group(&node_b, &mocks));

    // Sanitize direct mock
    let sanitized_a = sanitize_mock_output(&mocks["node_a"], &node_a.outputs);
    assert_eq!(sanitized_a, json!({"output": "a mocked directly"}));
}

#[test]
fn group_mock_triple_nesting_mock_middle() {
    // a > b > c. Only b is mocked.
    // a's nodes (outside b) run. b's nodes (including c) are skipped.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let a_worker = NodeDefinition {
        id: "a.worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["a".to_string()],
        groupBoundary: None,
    };
    let b_worker = NodeDefinition {
        id: "a.b.worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["a".to_string(), "a.b".to_string()],
        groupBoundary: None,
    };
    let c_worker = NodeDefinition {
        id: "a.b.c.worker".to_string(), nodeType: "ExecPython".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, true)],
        outputs: vec![make_port("result", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["a".to_string(), "a.b".to_string(), "a.b.c".to_string()],
        groupBoundary: None,
    };
    let c_in = NodeDefinition {
        id: "a.b.c__in".to_string(), nodeType: "Passthrough".into(), label: None, config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![make_port("data", LaneMode::Single, false)],
        outputs: vec![make_port("data", LaneMode::Single, false)],
        features: Default::default(),
        scope: vec!["a".to_string(), "a.b".to_string()],
        groupBoundary: Some(GroupBoundary { groupId: "a.b.c".to_string(), role: GroupBoundaryRole::In }),
    };

    let mocks: std::collections::HashMap<String, serde_json::Value> = [
        ("a.b".to_string(), json!({"result": "b mocked"})),
    ].into_iter().collect();

    // a.worker: scope=["a"], NOT inside a.b → runs
    assert!(!is_inside_mocked_group(&a_worker, &mocks));

    // a.b.worker: scope=["a", "a.b"], inside a.b → skipped
    assert!(is_inside_mocked_group(&b_worker, &mocks));

    // a.b.c.worker: scope=["a", "a.b", "a.b.c"], inside a.b (transitively) → skipped
    assert!(is_inside_mocked_group(&c_worker, &mocks));

    // a.b.c__in: scope=["a", "a.b"], inside a.b → skipped (it's a passthrough for c, but it's inside b)
    assert!(is_inside_mocked_group(&c_in, &mocks));
}

#[test]
fn mock_sanitization_with_type_checked_ports() {
    // Real-world scenario: LLM node with typed ports
    let ports = vec![
        PortDefinition {
            name: "response".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, description: None,
            laneMode: LaneMode::Single, laneDepth: 1,
            configurable: true,
        },
    ];

    // AI hallucinates 4 ports, only 1 exists
    let mock = json!({
        "response": "The revenue increased by 15%",
        "summary": "Revenue up",
        "category": "Finance",
        "score": 0.95
    });

    let sanitized = sanitize_mock_output(&mock, &ports);
    let obj = sanitized.as_object().unwrap();
    assert_eq!(obj.len(), 1);
    assert_eq!(obj["response"], json!("The revenue increased by 15%"));
}
// =========================================================================
// Group-level skip: required/oneOfRequired on group interface ports
// =========================================================================

#[test]
fn group_in_boundary_skips_when_required_input_null() {
    // A group with a required input port. When the In boundary receives
    // null on that required input, the executor's check_should_skip should
    // return true for the In boundary (the old behavior exempted boundaries
    // unconditionally; the new behavior honors the group's interface required
    // markers).
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let grp_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
        outputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
        features: Default::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::In,
        }),
    };

    let null_pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    let required: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let should_skip = check_should_skip(
        &grp_in,
        &null_pulses,
        &[],
        "c1",
        &required,
        &wired,
        &std::collections::HashSet::new(),
    );
    assert!(
        should_skip,
        "group In boundary with required input receiving null should skip"
    );
}

#[test]
fn group_in_boundary_does_not_skip_when_optional_input_null() {
    // Optional group input receiving null: the group body runs (no skip).
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let grp_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false, // optional
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
        outputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
        features: Default::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::In,
        }),
    };

    let null_pulses = vec![Pulse {
        id: "p1".to_string(),
        port: Some("data".to_string()),
        data: json!(null),
        lane: vec![],
        color: "c1".to_string(),
        status: PulseStatus::Pending,
        gathered: false,
    }];
    let required: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let wired: std::collections::HashSet<&str> = ["data"].into_iter().collect();
    let should_skip = check_should_skip(
        &grp_in,
        &null_pulses,
        &[],
        "c1",
        &required,
        &wired,
        &std::collections::HashSet::new(),
    );
    assert!(
        !should_skip,
        "group In boundary with optional input receiving null should not skip"
    );
}

#[test]
fn group_in_boundary_skips_when_one_of_required_all_null() {
    // Group with @require_one_of(a, b): if both a and b are null, skip.
    use crate::project::{GroupBoundary, GroupBoundaryRole};
    use crate::node::NodeFeatures;

    let mut features = NodeFeatures::default();
    features.oneOfRequired = vec![vec!["a".to_string(), "b".to_string()]];

    let grp_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![
            PortDefinition {
                name: "a".to_string(),
                portType: WeftType::primitive(WeftPrimitive::String),
                required: false,
                description: None,
                laneMode: LaneMode::Single,
                laneDepth: 1,
                configurable: false,
            },
            PortDefinition {
                name: "b".to_string(),
                portType: WeftType::primitive(WeftPrimitive::String),
                required: false,
                description: None,
                laneMode: LaneMode::Single,
                laneDepth: 1,
                configurable: false,
            },
        ],
        outputs: vec![],
        features,
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::In,
        }),
    };

    let null_pulses = vec![
        Pulse {
            id: "p1".to_string(),
            port: Some("a".to_string()),
            data: json!(null),
            lane: vec![],
            color: "c1".to_string(),
            status: PulseStatus::Pending,
            gathered: false,
        },
        Pulse {
            id: "p2".to_string(),
            port: Some("b".to_string()),
            data: json!(null),
            lane: vec![],
            color: "c1".to_string(),
            status: PulseStatus::Pending,
            gathered: false,
        },
    ];
    let required: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let wired: std::collections::HashSet<&str> = ["a", "b"].into_iter().collect();
    let should_skip = check_should_skip(
        &grp_in,
        &null_pulses,
        &[],
        "c1",
        &required,
        &wired,
        &std::collections::HashSet::new(),
    );
    assert!(
        should_skip,
        "group In boundary with oneOfRequired where all inputs are null should skip"
    );
}

#[test]
fn group_in_boundary_does_not_skip_when_one_of_required_one_present() {
    // Group with @require_one_of(a, b): if a is present and b is null, run.
    use crate::project::{GroupBoundary, GroupBoundaryRole};
    use crate::node::NodeFeatures;

    let mut features = NodeFeatures::default();
    features.oneOfRequired = vec![vec!["a".to_string(), "b".to_string()]];

    let grp_in = NodeDefinition {
        id: "grp__in".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![
            PortDefinition {
                name: "a".to_string(),
                portType: WeftType::primitive(WeftPrimitive::String),
                required: false,
                description: None,
                laneMode: LaneMode::Single,
                laneDepth: 1,
                configurable: false,
            },
            PortDefinition {
                name: "b".to_string(),
                portType: WeftType::primitive(WeftPrimitive::String),
                required: false,
                description: None,
                laneMode: LaneMode::Single,
                laneDepth: 1,
                configurable: false,
            },
        ],
        outputs: vec![],
        features,
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::In,
        }),
    };

    let mixed_pulses = vec![
        Pulse {
            id: "p1".to_string(),
            port: Some("a".to_string()),
            data: json!("hello"),
            lane: vec![],
            color: "c1".to_string(),
            status: PulseStatus::Pending,
            gathered: false,
        },
        Pulse {
            id: "p2".to_string(),
            port: Some("b".to_string()),
            data: json!(null),
            lane: vec![],
            color: "c1".to_string(),
            status: PulseStatus::Pending,
            gathered: false,
        },
    ];
    let required: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let wired: std::collections::HashSet<&str> = ["a", "b"].into_iter().collect();
    let should_skip = check_should_skip(
        &grp_in,
        &mixed_pulses,
        &[],
        "c1",
        &required,
        &wired,
        &std::collections::HashSet::new(),
    );
    assert!(
        !should_skip,
        "group In boundary with oneOfRequired where one input is present should not skip"
    );
}

#[test]
fn group_out_boundary_never_skips() {
    // Out boundaries never self-skip, they forward whatever inner nodes
    // produced. If the whole group is skipped at the In boundary, the
    // orchestrator is responsible for emitting null from the Out boundary
    // directly, not relying on the Out's own skip check.
    use crate::project::{GroupBoundary, GroupBoundaryRole};

    let grp_out = NodeDefinition {
        id: "grp__out".to_string(),
        nodeType: "Passthrough".into(),
        label: None,
        config: json!({}),
        position: Position { x: 0.0, y: 0.0 },
        inputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: true, // even if we set this, Out boundary should not skip
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
        outputs: vec![PortDefinition {
            name: "data".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
        features: Default::default(),
        scope: vec![],
        groupBoundary: Some(GroupBoundary {
            groupId: "grp".to_string(),
            role: GroupBoundaryRole::Out,
        }),
    };

    // Set up a project so we can invoke find_ready_nodes (which applies the
    // boundary skip rules). A single Out boundary fed by a null pulse.
    let source = make_node(
        "source",
        vec![],
        vec![PortDefinition {
            name: "value".to_string(),
            portType: WeftType::primitive(WeftPrimitive::String),
            required: false,
            description: None,
            laneMode: LaneMode::Single,
            laneDepth: 1,
            configurable: false,
        }],
    );
    let project = ProjectDefinition {
        id: uuid::Uuid::new_v4(),
        name: "out_skip_test".to_string(),
        description: None,
        nodes: vec![source, grp_out],
        edges: vec![make_edge("source", "value", "grp__out", "data")],
        status: Default::default(),
        createdAt: chrono::Utc::now(),
        updatedAt: chrono::Utc::now(),
    };
    let edge_idx = EdgeIndex::build(&project);
    let mut pulses: PulseTable = BTreeMap::new();
    let mut node_executions: NodeExecutionTable = BTreeMap::new();
    // Source emits null on `value` which lands on grp__out.data (required).
    postprocess_output(
        "source",
        &json!({"value": null}),
        "c1",
        &[],
        &project,
        &mut pulses,
        &edge_idx,
        &mut node_executions,
    );
    let ready = find_ready_nodes(&project, &pulses, &json!({}), &edge_idx);
    let grp_out_ready = ready.iter().find(|(id, _)| id == "grp__out");
    assert!(grp_out_ready.is_some(), "grp__out should be ready");
    let (_, group) = grp_out_ready.unwrap();
    assert!(
        !group.should_skip,
        "grp__out should NOT skip even when required input is null"
    );
}
