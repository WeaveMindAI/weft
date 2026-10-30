    //! Layer 1: the run's failure reason names the failed node the way
    //! the source spells it. A node inside an included file has a
    //! compiled id (`Api.answer`) nobody wrote; what the person wrote
    //! is the site and the name (`cards.answer`), and that is what the
    //! failure line carries.

    use super::*;
    use serde_json::json;
    use weft_core::run::firing::NodeFiring;
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

    /// The table of one firing of `node_id` at `frames` that failed with
    /// `error`.
    fn failed(node_id: &str, frames: Vec<Frame>, error: &str) -> NodeFiringTable {
        let mut firings = NodeFiringTable::new();
        let mut record = NodeFiring::started(node_id, frames.clone(), uuid::Uuid::new_v4(), Default::default(), Vec::new(), 1);
        record.error = Some(error.into());
        record.completed_at = Some(2);
        firings.insert(record);
        firings.set_status(node_id, &frames, NodeFiringStatus::Failed);
        firings
    }

    #[test]
    fn a_failed_node_in_an_included_file_is_named_by_its_address() {
        let project = project_with_an_include();
        let firings = failed("Api.answer", vec![Frame::Call { site: "cards".into() }], "storage denied");
        assert_eq!(
            first_failed(&firings).map(|record| failure_of(&project, record)).as_deref(),
            Some("cards.answer: storage denied")
        );
    }
