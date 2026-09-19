    //! Layer 1: the run's failure reason names the failed node the way
    //! the source spells it. A node inside an included file has a
    //! compiled id (`Api.answer`) nobody wrote; what the person wrote
    //! is the site and the name (`cards.answer`), and that is what the
    //! failure line carries.

    use super::*;
    use serde_json::json;
    use weft_core::exec::execution::NodeExecution;
    use weft_core::frames::Frame;
    use weft_core::ProjectDefinition;

    fn project_with_an_include() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {"id": "Api.answer", "nodeType": "Debug", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                 "inputs": [], "outputs": [], "features": {}, "scope": ["Api"], "groupBoundary": null,
                 "requiresInfra": false, "images": []}
            ],
            "edges": [],
            "groups": [
                {"id": "cards", "kind": "call", "body": "Api", "nodeIds": []},
                {"id": "Api", "kind": "body", "nodeIds": ["Api.answer"]}
            ]
        }))
        .unwrap()
    }

    fn failed(node_id: &str, frames: Vec<Frame>, error: &str) -> NodeExecution {
        NodeExecution {
            id: uuid::Uuid::new_v4(),
            received: Default::default(),
            skip_reason: None,
            node_id: node_id.into(),
            status: NodeExecutionStatus::Failed,
            pulses_absorbed: Vec::new(),
            ordinal: 0,
            error: Some(error.into()),
            callback_id: None,
            started_at: 1,
            completed_at: Some(2),
            cost_usd: 0.0,
            logs: Vec::new(),
            mentioned_ports: Default::default(),
            closed_output_ports: Default::default(),
            color: uuid::Uuid::new_v4(),
            frames,
            inherited_from: None,
        }
    }

    #[test]
    fn a_failed_node_in_an_included_file_is_named_by_its_address() {
        let project = project_with_an_include();
        let mut executions = NodeExecutionTable::new();
        executions.entry("Api.answer".into()).or_default().push(failed(
            "Api.answer",
            vec![Frame::Call { site: "cards".into() }],
            "storage denied",
        ));
        assert_eq!(
            first_failure(&project, &executions).as_deref(),
            Some("cards.answer: storage denied")
        );
    }
