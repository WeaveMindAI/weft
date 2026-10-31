    //! Layer 3: an infra node with baked outputs runs like any node. A
    //! baked output its body emits nothing on carries what its copy
    //! saved; one it emits on carries the body's value. An infra setup
    //! saves what went out on them before its firing completes.

    use super::*;
    use super::engine_test_rig::{catalog, clients, drive_on, test_manifest, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Mutex as StdMutex;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::{RunEvent, JournalClient};

    /// Emits `addr` only, leaving `access` to its saved value.
    struct Db;
    test_manifest!(Db, "Db");
    #[async_trait]
    impl Node for Db {
        async fn provision_infra(&self, _ctx: weft_core::InfraProvisionContext, _input: weft_core::ValueBag) -> WeftResult<weft_core::infra::InfraSpec> {
            Ok(weft_core::infra::InfraSpec::default())
        }
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("addr", json!("from-the-body"))).await
        }
    }

    /// Reads its input and emits nothing.
    struct Sink;
    test_manifest!(Sink, "Sink");
    #[async_trait]
    impl Node for Sink {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
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
            _: RunId,
            _: Option<&weft_core::instance::InstanceId>,
            _: &weft_core::infra::InfraHandle,
        ) -> anyhow::Result<Option<weft_core::infra::EndpointAddress>> {
            Ok(None)
        }
        async fn baked_outputs(
            &self,
            _: RunId,
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
        async fn enqueue_apply(&self, _: uuid::Uuid, _: &str, _: Option<&weft_core::instance::InstanceId>, _: serde_json::Value, _: weft_core::RunId) -> anyhow::Result<i64> {
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
            _: RunId,
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

    /// The run's birth in `phase`, `db` kicked.
    fn birth(project: &ProjectDefinition, run_id: RunId, phase: weft_core::context::Phase) -> [RunEvent; 2] {
        [
            RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "db".into(),
                phase,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None, source_version: None, run_kind: weft_core::run::RunKind::Project,
                // Every phase here runs the whole program: `db` and what it feeds.
                cut_from: None,
                seed: None, instance: None, stand_in: None, fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(), at_unix: 0,
                settings: Default::default(),
            },
            RunEvent::NodeKicked { run_id, node_id: "db".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ]
    }

    async fn drive_phase(phase: weft_core::context::Phase, copy: Arc<Copy>) -> (RunOutcome, Vec<RunEvent>) {
        let project = project();
        let run_id = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        for row in &birth(&project, run_id, phase) {
            journal.record_event(row, None).await.unwrap();
        }
        let mut clients = clients(journal.clone());
        clients.infra = copy.clone();
        clients.infra_state = copy;
        let drove = drive_on(Arc::new(project), nodes(), run_id, journal.clone(), clients, CancellationFlag::new_arc(), None)
            .await
            .expect("run_one ok");
        (drove.outcome, journal.events_of(run_id))
    }

    fn emitted(events: &[RunEvent], port: &str) -> Option<serde_json::Value> {
        events.iter().find_map(|e| match e {
            RunEvent::PortEmitted { node_id, port: p, value, .. } if node_id == "db" && p == port => Some((**value).clone()),
            _ => None,
        })
    }

    fn ran(events: &[RunEvent], node: &str) -> bool {
        events.iter().any(|e| matches!(e, RunEvent::NodeCompleted { node_id, .. } if node_id == node))
    }

    #[tokio::test]
    async fn a_baked_output_the_body_leaves_carries_what_its_copy_saved() {
        let copy = Arc::new(Copy::default());
        *copy.saved.lock().unwrap() = BTreeMap::from([("access".to_string(), json!("saved")), ("addr".to_string(), json!("old"))]);
        let (outcome, events) = drive_phase(weft_core::context::Phase::Fire, copy.clone()).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(emitted(&events, "access"), Some(json!("saved")), "the saved value, where the body sent nothing");
        assert_eq!(emitted(&events, "addr"), Some(json!("from-the-body")), "the body's value wins");
        assert_eq!(emitted(&events, "status"), None, "an output that is not baked closes");
        assert!(ran(&events, "a") && ran(&events, "b") && !ran(&events, "c"), "{events:?}");
        assert!(copy.saves.lock().unwrap().is_empty(), "a run that is no setup saves nothing");
    }

    #[tokio::test]
    async fn with_nothing_saved_a_baked_output_the_body_leaves_closes() {
        let (outcome, events) = drive_phase(weft_core::context::Phase::Fire, Arc::new(Copy::default())).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(emitted(&events, "access"), None);
        assert!(!ran(&events, "a") && ran(&events, "b"), "{events:?}");
    }

    #[tokio::test]
    async fn an_infra_setup_saves_what_went_out_on_its_baked_outputs() {
        let copy = Arc::new(Copy::default());
        *copy.saved.lock().unwrap() = BTreeMap::from([("access".to_string(), json!("saved")), ("addr".to_string(), json!("old"))]);
        let (outcome, _) = drive_phase(weft_core::context::Phase::InfraSetup, copy.clone()).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(
            *copy.saves.lock().unwrap(),
            vec![("db".to_string(), BTreeMap::from([("addr".to_string(), json!("from-the-body"))]))],
            "only what went out: the broker merges it over what the copy holds, so a value pushed meanwhile stays"
        );
    }

    /// Emits `addr` (what it sends tells its first body from the replay),
    /// then waits for an answer and returns.
    struct PausingDb {
        bodies: std::sync::atomic::AtomicU32,
    }
    test_manifest!(PausingDb, "Db");
    #[async_trait]
    impl Node for PausingDb {
        async fn provision_infra(&self, _ctx: weft_core::InfraProvisionContext, _input: weft_core::ValueBag) -> WeftResult<weft_core::infra::InfraSpec> {
            Ok(weft_core::infra::InfraSpec::default())
        }
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let life = match self.bodies.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 => "first-life",
                _ => "replayed",
            };
            ctx.pulse_downstream(NodeOutput::new().set("addr", json!(life))).await?;
            ctx.await_signal(weft_core::signal::Form {
                form_type: "human_query".into(),
                schema: weft_core::signal::FormSchema { fields: Vec::new() },
                title: None,
                description: None,
                consumer_kind: None,
            })
            .await?;
            Ok(())
        }
    }

    /// An infra setup sends a baked output, pauses, and resumes on a
    /// later worker: the replay sends nothing on that output, and the
    /// bake saves the value downstream received, the first body's.
    #[tokio::test]
    async fn an_infra_setup_that_paused_bakes_what_went_out_before_the_pause() {
        let project = Arc::new(project());
        let run_id = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth(&project, run_id, weft_core::context::Phase::InfraSetup));
        let nodes = catalog(vec![
            ("Db", Box::new(PausingDb { bodies: Default::default() })),
            ("Sink", Box::new(Sink)),
        ]);
        let copy = Arc::new(Copy::default());
        let tasks = super::engine_test_rig::AwaitTasks::new();
        let with_fakes = || {
            let mut c = clients(journal.clone());
            c.infra = copy.clone();
            c.infra_state = copy.clone();
            c.tasks = tasks.clone();
            c
        };

        let paused = drive_on(project.clone(), nodes.clone(), run_id, journal.clone(), with_fakes(), CancellationFlag::new_arc(), None)
            .await
            .expect("the first drive runs")
            .outcome;
        assert!(matches!(paused, RunOutcome::Stalled), "{paused:?}");
        assert!(copy.saves.lock().unwrap().is_empty(), "a paused setup has saved nothing yet");

        let token = tasks.minted().expect("the wait registered");
        journal.seed(&[RunEvent::wait_answered(run_id, token, weft_core::primitive::WaitAnswer::Given { value: json!("go") }, 1)]);
        let resumed = drive_on(project, nodes, run_id, journal.clone(), with_fakes(), CancellationFlag::new_arc(), None)
            .await
            .expect("the second drive runs")
            .outcome;
        assert!(matches!(resumed, RunOutcome::Completed), "{resumed:?}");

        let events = journal.events_of(run_id);
        assert_eq!(emitted(&events, "addr"), Some(json!("first-life")), "{events:?}");
        assert_eq!(
            events.iter().filter(|e| matches!(e, RunEvent::PortEmitted { node_id, port, .. } if node_id == "db" && port == "addr")).count(),
            1,
            "the replay sent nothing on `addr`"
        );
        assert_eq!(
            *copy.saves.lock().unwrap(),
            vec![("db".to_string(), BTreeMap::from([("addr".to_string(), json!("first-life"))]))],
            "the bake holds what downstream received"
        );
    }
