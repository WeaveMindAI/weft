    use super::*;
    use weft_core::project::{Edge, NodeDefinition, Position, ProjectDefinition};

    fn mk_node(id: &str, is_trigger: bool, requires_infra: bool) -> NodeDefinition {
        let mut features = weft_core::node::NodeFeatures::default();
        features.is_trigger = is_trigger;
        NodeDefinition {
            id: id.to_string(),
            node_type: "Test".to_string(),
            label: None,
            config: serde_json::Value::Null,
            position: Position { x: 0.0, y: 0.0 },
            inputs: Vec::new(),
            outputs: Vec::new(),
            features,
            scope: Vec::new(),
            group_boundary: None,
            requires_infra,
            images: Vec::new(),
            span: None,
            header_span: None,
            config_spans: Default::default(),
            port_literals: Default::default(),
            port_literal_spans: Default::default(),
            file_refs: Default::default(),
            include_path: None,
        }
    }

    fn mk_edge(src: &str, dst: &str) -> Edge {
        Edge {
            id: format!("e-{src}-{dst}"),
            source: src.to_string(),
            target: dst.to_string(),
            source_handle: None,
            target_handle: None,
            span: None,
        }
    }

    fn mk_project(nodes: Vec<NodeDefinition>, edges: Vec<Edge>) -> ProjectDefinition {
        let v = serde_json::json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": nodes,
            "edges": edges,
            "groups": []
        });
        serde_json::from_value(v).expect("test project definition")
    }

    #[test]
    fn infra_setup_scope_includes_infra_and_upstream() {
        // text -> compute -> infra
        let project = mk_project(
            vec![
                mk_node("text", false, false),
                mk_node("compute", false, false),
                mk_node("infra", false, true),
            ],
            vec![mk_edge("text", "compute"), mk_edge("compute", "infra")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.contains("infra"));
        assert!(scope.contains("compute"));
        assert!(scope.contains("text"));
    }

    #[test]
    fn infra_setup_scope_excludes_downstream_of_infra() {
        // infra -> trigger -> reply (a fire-time-only path)
        let project = mk_project(
            vec![
                mk_node("infra", false, true),
                mk_node("trigger", true, false),
                mk_node("reply", false, false),
            ],
            vec![mk_edge("infra", "trigger"), mk_edge("trigger", "reply")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.contains("infra"));
        // The trigger node is downstream of infra; not part of the
        // InfraSetup scope.
        assert!(!scope.contains("trigger"));
        assert!(!scope.contains("reply"));
    }

    #[test]
    fn infra_setup_scope_handles_multiple_infra_nodes() {
        // text -> infraA ; cfg -> infraB
        let project = mk_project(
            vec![
                mk_node("text", false, false),
                mk_node("cfg", false, false),
                mk_node("infraA", false, true),
                mk_node("infraB", false, true),
            ],
            vec![mk_edge("text", "infraA"), mk_edge("cfg", "infraB")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.contains("text"));
        assert!(scope.contains("cfg"));
        assert!(scope.contains("infraA"));
        assert!(scope.contains("infraB"));
    }

    #[test]
    fn trigger_setup_scope_unchanged() {
        // text -> trigger ; trigger -> reply (downstream not in scope)
        let project = mk_project(
            vec![
                mk_node("text", false, false),
                mk_node("trigger", true, false),
                mk_node("reply", false, false),
            ],
            vec![mk_edge("text", "trigger"), mk_edge("trigger", "reply")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_trigger_setup_scope(&project, &idx);
        assert!(scope.contains("trigger"));
        assert!(scope.contains("text"));
        assert!(!scope.contains("reply"));
    }

    #[test]
    fn empty_infra_set_yields_empty_scope() {
        let project = mk_project(
            vec![mk_node("a", false, false), mk_node("b", false, false)],
            vec![mk_edge("a", "b")],
        );
        let idx = EdgeIndex::build(&project);
        let scope = compute_infra_setup_scope(&project, &idx);
        assert!(scope.is_empty());
    }
