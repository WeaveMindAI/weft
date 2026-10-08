    //! Layer 3: an infra node with baked outputs runs like any node. A
    //! baked output its body emits nothing on carries what its copy
    //! saved; one it emits on carries the body's value. An infra setup
    //! saves what went out on them before its step completes.

    use super::*;
    use super::engine_test_rig::{catalog, clients, drive_on, test_manifest, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::{ExecEvent, JournalClient};

    /// Emits `addr` only, leaving `access` to its saved value.
    struct Db;
    test_manifest!(Db, "Db");
    #[async_trait]
    impl Node for Db {
        async fn provision_infra(&self, _ctx: weft_core::InfraProvisionContext, _input: weft_core::ValueBag) -> WeftResult<weft_core::infra::InfraSpec> {
            Ok(weft_core::infra::InfraSpec::default())
        }
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("addr", json!("from-the-body"))).await
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

    /// What the copy saved, and every save an infra setup made.
    #[derive(Default)]
    struct Copy {
        saved: StdMutex<BTreeMap<String, serde_json::Value>>,
        saves: StdMutex<Vec<(String, BTreeMap<String, serde_json::Value>)>>,
    }

    #[async_trait]
    impl weft_task_store::InfraReader for Copy {
        async fn endpoint_address(
            &self,
            _: ExecutionId,
            _: Option<&weft_core::instance::InstanceId>,
            _: &weft_core::infra::InfraHandle,
        ) -> anyhow::Result<Option<weft_core::infra::EndpointAddress>> {
            Ok(None)
        }
        async fn baked_outputs(
            &self,
            _: ExecutionId,
            _: Option<&weft_core::instance::InstanceId>,
            place: &str,
            copy: Option<&weft_core::instance::InstanceId>,
        ) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
            assert_eq!((place, copy), ("db", None), "the node's own shared copy");
            Ok(self.saved.lock().unwrap().clone())
        }
    }

    #[async_trait]
    impl crate::context::InfraStateClient for Copy {
        async fn enqueue_apply(&self, _: uuid::Uuid, _: &str, _: Option<&weft_core::instance::InstanceId>, _: serde_json::Value) -> anyhow::Result<i64> {
            Ok(1)
        }
        async fn wait_apply(&self, _: uuid::Uuid, _: i64, _: std::time::Duration) -> anyhow::Result<weft_broker_client::protocol::InfraWaitApplyResponse> {
            Ok(weft_broker_client::protocol::InfraWaitApplyResponse {
                completed: true,
                outcome: Some(weft_broker_client::protocol::LifecycleOutcome::Succeeded),
                outcome_message: None,
            })
        }
        async fn save_bake(
            &self,
            _: ExecutionId,
            place: &str,
            _: Option<&weft_core::instance::InstanceId>,
            values: BTreeMap<String, serde_json::Value>,
        ) -> anyhow::Result<()> {
            self.saves.lock().unwrap().push((place.to_string(), values));
            Ok(())
        }
    }

    /// `db` (baking `access` and `addr`, emitting a third, `status`, never)
    /// feeds `a` on `access`, `b` on `addr` and `c` on `status`.
    fn project() -> ProjectDefinition {
        let sink = |id: &str| json!({
            "id": id, "nodeType": "Sink", "label": null, "config": null, "position": {"x": 1.0, "y": 0.0},
            "inputs": [{"name": "in", "portType": "String", "required": true}], "outputs": [],
            "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
        });
        let wire = |port: &str, to: &str| json!({"id": port, "source": "db", "target": to, "sourceHandle": port, "targetHandle": "in"});
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "db", "nodeType": "Db", "label": null, "config": null, "position": {"x": 0.0, "y": 0.0},
                    "inputs": [],
                    "outputs": [
                        {"name": "access", "portType": "String", "required": false},
                        {"name": "addr", "portType": "String", "required": false},
                        {"name": "status", "portType": "String", "required": false}
                    ],
                    "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": true, "images": [],
                    "bakedOutputs": ["access", "addr"]
                },
                sink("a"), sink("b"), sink("c")
            ],
            "edges": [wire("access", "a"), wire("addr", "b"), wire("status", "c")],
            "groups": []
        }))
        .unwrap()
    }

    fn nodes() -> Arc<dyn weft_core::NodeCatalog> {
        catalog(vec![("Db", Box::new(Db)), ("Sink", Box::new(Sink))])
    }

    async fn drive_phase(phase: weft_core::context::Phase, copy: Arc<Copy>) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let project = project();
        let execution_id = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        let rows = [
            ExecEvent::ExecutionStarted {
                execution_id,
                project_id: project.id,
                entry_node: "db".into(),
                phase,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
                binary_hash: None, source_version: None, run_kind: weft_core::exec::RunKind::Execution,
                // A setup names the nodes it brings up; any other run here runs the whole program.
                selection: (phase == weft_core::context::Phase::InfraSetup).then(|| {
                    weft_core::project::selection::RecordedSelection::new(
                        weft_core::project::selection::RunSelection::restricted(&project, ["db", "a", "b", "c"].iter().map(|n| weft_core::frames::Located::top(*n)).collect())
                            .expect("valid test selection"),
                    )
                }),
                seed: None, instance: None, stand_in: None, fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            ExecEvent::NodeKicked { execution_id, node_id: "db".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ];
        for row in &rows {
            journal.record_event(row, None).await.unwrap();
        }
        let mut clients = clients(journal.clone());
        clients.infra = copy.clone();
        clients.infra_state = copy;
        let drove = drive_on(Arc::new(project), nodes(), execution_id, journal.clone(), clients, CancellationFlag::new_arc(), None)
            .await
            .expect("run_one_execution ok");
        (drove.outcome, journal.events_for_execution_id(execution_id).await.unwrap())
    }

    fn emitted(events: &[ExecEvent], port: &str) -> Option<serde_json::Value> {
        events.iter().find_map(|e| match e {
            ExecEvent::PortEmitted { node_id, port: p, value, .. } if node_id == "db" && p == port => Some((**value).clone()),
            _ => None,
        })
    }

    fn ran(events: &[ExecEvent], node: &str) -> bool {
        events.iter().any(|e| matches!(e, ExecEvent::NodeCompleted { node_id, .. } if node_id == node))
    }

    #[tokio::test]
    async fn a_baked_output_the_body_leaves_carries_what_its_copy_saved() {
        let copy = Arc::new(Copy::default());
        *copy.saved.lock().unwrap() = BTreeMap::from([("access".to_string(), json!("saved")), ("addr".to_string(), json!("old"))]);
        let (outcome, events) = drive_phase(weft_core::context::Phase::Fire, copy.clone()).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(emitted(&events, "access"), Some(json!("saved")), "the saved value, where the body sent nothing");
        assert_eq!(emitted(&events, "addr"), Some(json!("from-the-body")), "the body's value wins");
        assert_eq!(emitted(&events, "status"), None, "an output that is not baked closes");
        assert!(ran(&events, "a") && ran(&events, "b") && !ran(&events, "c"), "{events:?}");
        assert!(copy.saves.lock().unwrap().is_empty(), "a run that is no setup saves nothing");
    }

    #[tokio::test]
    async fn with_nothing_saved_a_baked_output_the_body_leaves_closes() {
        let (outcome, events) = drive_phase(weft_core::context::Phase::Fire, Arc::new(Copy::default())).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(emitted(&events, "access"), None);
        assert!(!ran(&events, "a") && ran(&events, "b"), "{events:?}");
    }

    #[tokio::test]
    async fn an_infra_setup_saves_what_went_out_on_its_baked_outputs() {
        let copy = Arc::new(Copy::default());
        *copy.saved.lock().unwrap() = BTreeMap::from([("access".to_string(), json!("saved")), ("addr".to_string(), json!("old"))]);
        let (outcome, _) = drive_phase(weft_core::context::Phase::InfraSetup, copy.clone()).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert_eq!(
            *copy.saves.lock().unwrap(),
            vec![("db".to_string(), BTreeMap::from([("addr".to_string(), json!("from-the-body"))]))],
            "only what went out: the broker merges it over what the copy holds, so a value pushed meanwhile stays"
        );
    }
