    //! Layer 3: a field the program lets each instance leave empty
    //! (`field: @instance_filled(null)`) that is empty for the run's
    //! instance skips its node before the body runs when the node needs
    //! it, and the nodes behind it decide on its closed output the way
    //! they decide on any closure. The same for a connection
    //! (`account: @instance_filled(null)` on an access node) as for any
    //! other field. A node that takes the field as optional runs and reads
    //! the null.

    use super::*;
    use super::engine_test_rig::{birth_kicking, catalog, drive_journal_observed, test_manifest};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::instance::{instance_filled_literal, InstanceId, InstanceValues};
    use weft_core::run::skip::SkipReason;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::RunEvent;

    type Ran = Arc<StdMutex<Vec<&'static str>>>;

    /// An access node's pass-through body: the connection out on `access`.
    struct Access {
        ran: Ran,
    }
    test_manifest!(Access, "FakeAccess");
    #[async_trait]
    impl Node for Access {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("slack");
            let access = ctx.inputs.access("account")?;
            ctx.pulse_downstream(NodeOutput::new().set("access", json!(access.map(|a| a.to_value())))).await
        }
    }

    /// Writes down that it ran and what it read on its one input, and
    /// sends on `out`.
    struct Reader {
        ran: Ran,
        read: Arc<StdMutex<Vec<serde_json::Value>>>,
    }
    test_manifest!(Reader, "Reader");
    #[async_trait]
    impl Node for Reader {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.ran.lock().unwrap().push("reader");
            if let Some(value) = ctx.inputs.raw("channel") {
                self.read.lock().unwrap().push(value.clone());
            }
            ctx.pulse_downstream(NodeOutput::new().set("out", json!(true))).await
        }
    }

    fn reader(id: &str, input: &str, port_type: &str, required: bool) -> serde_json::Value {
        json!({
            "id": id, "nodeType": "Reader", "position": { "x": 0, "y": 0 },
            "inputs": [{ "name": input, "portType": port_type, "required": required, "optional": !required }],
            "outputs": [{ "name": "out", "portType": "Boolean", "required": false }]
        })
    }

    fn edge(id: &str, source: &str, out: &str, target: &str, input: &str) -> serde_json::Value {
        json!({ "id": id, "source": source, "target": target, "sourceHandle": out, "targetHandle": input })
    }

    /// `node` with `field` each instance fills, written
    /// `@instance_filled(null)` when `marked`.
    fn filled_by_each_instance(mut node: serde_json::Value, field: &str, marked: bool) -> serde_json::Value {
        node["portLiterals"] = json!({ field: instance_filled_literal(marked.then_some(serde_json::Value::Null)) });
        node
    }

    /// slack.access -> needs.access (required), slack.access -> maybe.access
    /// (optional); each instance brings `slack`'s connection, written
    /// `@instance_filled(null)` when `marked`.
    fn connection_project(marked: bool) -> ProjectDefinition {
        let slack = json!({
            "id": "slack", "nodeType": "FakeAccess", "position": { "x": 0, "y": 0 },
            "inputs": [{
                "name": "account", "portType": "Access", "required": true,
                "widget": { "kind": "access", "service": "slack", "optional": false }
            }],
            "outputs": [{ "name": "access", "portType": "Access", "required": false }]
        });
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                filled_by_each_instance(slack, "account", marked),
                reader("needs", "access", "Access", true),
                reader("maybe", "access", "Access", false)
            ],
            "edges": [edge("e1", "slack", "access", "needs", "access"), edge("e2", "slack", "access", "maybe", "access")]
        }))
        .expect("project")
    }

    /// send (its `channel`, required when `required`, filled by each
    /// instance and allowed to stay empty) -> after.in (required).
    fn field_project(required: bool) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                filled_by_each_instance(reader("send", "channel", "String", required), "channel", true),
                reader("after", "in", "Boolean", true)
            ],
            "edges": [edge("e1", "send", "out", "after", "in")]
        }))
        .expect("project")
    }

    /// Drives `project` from `kick` for instance `ada`, whose values are
    /// `values`.
    async fn drive_for_ada(project: ProjectDefinition, catalog: Arc<dyn weft_core::NodeCatalog>, kick: &str, values: InstanceValues) -> (RunOutcome, Vec<RunEvent>) {
        let run_id = weft_core::new_run_id();
        let mut rows = birth_kicking(&project, run_id, &[kick], Default::default());
        let RunEvent::RunStarted { instance, instance_values, .. } = &mut rows[0] else { unreachable!("a birth starts the run") };
        *instance = Some(InstanceId::new("ada").unwrap());
        **instance_values = values;
        let (drove, events) = drive_journal_observed(project, catalog, run_id, rows, CancellationFlag::new_arc()).await;
        (drove.expect("run_one ok").outcome, events)
    }

    fn skip_reason(events: &[RunEvent], node: &str) -> Option<SkipReason> {
        events.iter().find_map(|e| match e {
            RunEvent::NodeSkipped { node_id, reason, .. } if node_id == node => Some(reason.clone()),
            _ => None,
        })
    }

    fn nodes(ran: &Ran) -> Arc<dyn weft_core::NodeCatalog> {
        nodes_reading(ran, &Arc::new(StdMutex::new(Vec::new())))
    }

    fn nodes_reading(ran: &Ran, read: &Arc<StdMutex<Vec<serde_json::Value>>>) -> Arc<dyn weft_core::NodeCatalog> {
        catalog(vec![
            ("FakeAccess", Box::new(Access { ran: ran.clone() })),
            ("Reader", Box::new(Reader { ran: ran.clone(), read: read.clone() })),
        ])
    }

    #[tokio::test]
    async fn a_marked_connection_left_empty_skips_its_node() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_for_ada(connection_project(true), nodes(&ran), "slack", InstanceValues::new()).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(
            skip_reason(&events, "slack"),
            Some(SkipReason::Unfilled { field: "account".into(), service: Some("slack".into()) })
        );
        assert_eq!(
            skip_reason(&events, "needs"),
            Some(SkipReason::RequiredInputClosed { port: "access".into(), failure: None }),
            "a required input behind it skips as on any closure"
        );
        assert_eq!(skip_reason(&events, "maybe"), None, "an optional input behind it runs");
        assert_eq!(*ran.lock().unwrap(), vec!["reader"], "the access node's body never ran; only `maybe` did");
    }

    /// Any field, not only a connection: left empty, a node that needs it
    /// skips; filled, it runs.
    #[tokio::test]
    async fn a_marked_field_skips_its_node_only_while_it_is_empty() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_for_ada(field_project(true), nodes(&ran), "send", InstanceValues::new()).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(skip_reason(&events, "send"), Some(SkipReason::Unfilled { field: "channel".into(), service: None }));
        assert_eq!(skip_reason(&events, "after"), Some(SkipReason::RequiredInputClosed { port: "in".into(), failure: None }));
        assert!(ran.lock().unwrap().is_empty(), "nothing ran");

        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let values = InstanceValues::from([("send".into(), std::collections::BTreeMap::from([("channel".into(), json!("#support"))]))]);
        let (_, events) = drive_for_ada(field_project(true), nodes(&ran), "send", values).await;
        assert_eq!(skip_reason(&events, "send"), None);
        assert_eq!(ran.lock().unwrap().len(), 2, "both ran");
    }

    /// A node that takes the field as optional runs with it left empty,
    /// and reads a null there: what it does about that is its own code's.
    #[tokio::test]
    async fn an_optional_field_left_empty_runs_its_node_with_a_null() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let read = Arc::new(StdMutex::new(Vec::new()));
        let (outcome, events) = drive_for_ada(field_project(false), nodes_reading(&ran, &read), "send", InstanceValues::new()).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(skip_reason(&events, "send"), None);
        assert_eq!(*read.lock().unwrap(), vec![json!(null)]);
        assert_eq!(ran.lock().unwrap().len(), 2, "both ran");
    }

    /// Unmarked, the engine leaves the decision to the node: its body runs
    /// and reads no connection (a run for an instance missing a value it
    /// needs was refused before it was born, so this only happens in a
    /// rig).
    #[tokio::test]
    async fn an_unmarked_field_is_never_skipped_by_the_engine() {
        let ran: Ran = Arc::new(StdMutex::new(Vec::new()));
        let (_, events) = drive_for_ada(connection_project(false), nodes(&ran), "slack", InstanceValues::new()).await;
        assert_eq!(skip_reason(&events, "slack"), None);
        assert!(ran.lock().unwrap().contains(&"slack"), "the body ran");
    }
