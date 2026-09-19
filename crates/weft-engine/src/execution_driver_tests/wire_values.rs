    //! Layer 3: what a wire carries. A port value is small by rule
    //! (`weft_core::storage::MAX_WIRE_VALUE_BYTES`); a node that emits
    //! more fails at that node, with the refusal in the journal, and the
    //! run ends failed instead of dying on a journal write downstream.

    use super::*;
    use super::engine_test_rig::{catalog, drive, test_manifest};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::ExecEvent;

    /// Emits a string of `bytes` characters on `out`.
    struct Heavy {
        bytes: usize,
    }
    test_manifest!(Heavy, "Heavy");
    #[async_trait]
    impl Node for Heavy {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("out", json!("x".repeat(self.bytes)))).await
        }
    }

    /// Reads its input and emits nothing.
    struct Sink;
    test_manifest!(Sink, "Sink");
    #[async_trait]
    impl Node for Sink {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let _: String = ctx.inputs.get("in")?;
            Ok(())
        }
    }

    fn nodes(heavy: Heavy) -> Arc<dyn weft_core::NodeCatalog> {
        catalog(vec![("Heavy", Box::new(heavy)), ("Sink", Box::new(Sink))])
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {"id": "heavy", "nodeType": "Heavy", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                 "inputs": [], "outputs": [{"name": "out", "portType": "String", "required": false}],
                 "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []},
                {"id": "sink", "nodeType": "Sink", "label": null, "config": null, "position": {"x": 1.0, "y": 0.0},
                 "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
                 "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []}
            ],
            "edges": [{"id": "e", "source": "heavy", "target": "sink", "sourceHandle": "out", "targetHandle": "in"}],
            "groups": []
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn an_oversize_value_fails_the_node_that_emitted_it() {
        let (outcome, events) = drive(project(), nodes(Heavy { bytes: weft_core::storage::MAX_WIRE_VALUE_BYTES }), &["heavy"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let error = events.iter().find_map(|e| match e {
            ExecEvent::NodeFailed { node_id, error, .. } if node_id == "heavy" => Some(error.clone()),
            _ => None,
        }).expect("the emitting node failed");
        assert!(error.contains("port 'out' of 'heavy' carries 101 KB"), "{error}");
        assert!(error.contains("stored file"), "{error}");
        // Nothing was emitted: the refusal is a clean no-op on the wire.
        assert!(!events.iter().any(|e| matches!(e, ExecEvent::PortEmitted { node_id, .. } if node_id == "heavy")), "{events:?}");
        assert!(events.iter().any(|e| matches!(e, ExecEvent::NodeSkipped { node_id, .. } if node_id == "sink")), "{events:?}");
    }

    /// Emits `first` (a String, as declared), then a NUMBER on `out`,
    /// which is declared String: the second call fails, and the node
    /// returns that error.
    struct Mistyped;
    test_manifest!(Mistyped, "Mistyped");
    #[async_trait]
    impl Node for Mistyped {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("first", json!("kept"))).await?;
            ctx.pulse_downstream(NodeOutput::new().set("out", json!(42))).await
        }
    }

    fn two_port_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {"id": "heavy", "nodeType": "Mistyped", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                 "inputs": [],
                 "outputs": [{"name": "first", "portType": "String", "required": false}, {"name": "out", "portType": "String", "required": false}],
                 "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []},
                {"id": "sink", "nodeType": "Sink", "label": null, "config": null, "position": {"x": 1.0, "y": 0.0},
                 "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
                 "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []},
                {"id": "reader", "nodeType": "Sink", "label": null, "config": null, "position": {"x": 1.0, "y": 1.0},
                 "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
                 "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []}
            ],
            "edges": [
                {"id": "e", "source": "heavy", "target": "sink", "sourceHandle": "out", "targetHandle": "in"},
                {"id": "f", "source": "heavy", "target": "reader", "sourceHandle": "first", "targetHandle": "in"}
            ],
            "groups": []
        }))
        .unwrap()
    }

    /// A value the port's declared type does not accept fails the
    /// send, and the node with it: what the firing sent before stays
    /// sent (`reader` runs on `first`), the refused port and every
    /// other unmentioned one close (`sink` skips), and the run ends
    /// failed naming the port. Closing the port quietly and marking
    /// the node completed used to leave a caller with no answer and
    /// no error.
    #[tokio::test]
    async fn a_value_the_declared_type_refuses_fails_the_node_and_keeps_what_was_sent() {
        let cat = catalog(vec![("Mistyped", Box::new(Mistyped)), ("Sink", Box::new(Sink))]);
        let (outcome, events) = drive(two_port_project(), cat, &["heavy"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let error = events.iter().find_map(|e| match e {
            ExecEvent::NodeFailed { node_id, error, .. } if node_id == "heavy" => Some(error.clone()),
            _ => None,
        }).expect("the emitting node failed");
        assert!(error.contains("port 'out'") && error.contains("does not accept") && error.contains("got Number"), "{error}");
        assert!(events.iter().any(|e| matches!(e, ExecEvent::PortEmitted { node_id, port, .. } if node_id == "heavy" && port == "first")), "{events:?}");
        assert!(!events.iter().any(|e| matches!(e, ExecEvent::PortEmitted { node_id, port, .. } if node_id == "heavy" && port == "out")), "{events:?}");
        assert!(events.iter().any(|e| matches!(e, ExecEvent::NodeCompleted { node_id, .. } if node_id == "reader")), "what was sent stays sent: {events:?}");
        assert!(events.iter().any(|e| matches!(e, ExecEvent::NodeSkipped { node_id, .. } if node_id == "sink")), "{events:?}");
    }

    /// Emits a Number on `out` (declared String), catches the refusal,
    /// and emits a String on the SAME port.
    struct Recovers;
    test_manifest!(Recovers, "Recovers");
    #[async_trait]
    impl Node for Recovers {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let refused = ctx
                .pulse_downstream(NodeOutput::new().set("out", json!(42)))
                .await
                .expect_err("a Number on a String port is refused");
            assert!(refused.to_string().contains("does not accept"), "{refused}");
            ctx.pulse_downstream(NodeOutput::new().set("out", json!("second try"))).await
        }
    }

    /// The refused send leaves the port OPEN, so a node that catches
    /// the error can try again and be believed. This is why the type
    /// check runs before the port is claimed: claim it first and the
    /// retry comes back "touched twice", which is a lie about what the
    /// node did and takes away the only way it had to recover.
    ///
    /// The run completes. A value its declared type refuses does not
    /// travel, and that is the whole of the consequence when the node
    /// handles it.
    #[tokio::test]
    async fn a_node_that_catches_a_refused_send_can_use_the_port_again() {
        let cat = catalog(vec![("Recovers", Box::new(Recovers)), ("Sink", Box::new(Sink))]);
        let mut project = two_port_project();
        project.nodes[0].node_type = "Recovers".to_string();
        let (outcome, events) = drive(project, cat, &["heavy"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        let emitted: Vec<&ExecEvent> = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::PortEmitted { node_id, port, .. } if node_id == "heavy" && port == "out"))
            .collect();
        assert_eq!(emitted.len(), 1, "only the value that fit was ever sent: {events:?}");
        assert!(
            matches!(emitted[0], ExecEvent::PortEmitted { value, .. } if value.as_ref() == &json!("second try")),
            "{:?}",
            emitted[0]
        );
        assert!(events.iter().any(|e| matches!(e, ExecEvent::NodeCompleted { node_id, .. } if node_id == "heavy")), "{events:?}");
        assert!(events.iter().any(|e| matches!(e, ExecEvent::NodeCompleted { node_id, .. } if node_id == "sink")), "the retry reached the consumer: {events:?}");
        assert!(!events.iter().any(|e| matches!(e, ExecEvent::NodeFailed { node_id, .. } if node_id == "heavy")), "{events:?}");
    }

    #[tokio::test]
    async fn a_value_at_the_cap_travels() {
        let (outcome, _) = drive(project(), nodes(Heavy { bytes: weft_core::storage::MAX_WIRE_VALUE_BYTES - 2 }), &["heavy"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
    }
