    //! Layer 3: an unrecorded run through the real loop. Its journal is the
    //! worker's memory: a run that completes leaves nothing in the real
    //! journal, one that fails is written there whole as a recorded run,
    //! and a wait is refused at the call because there is nowhere to park.

    use super::*;
    use super::engine_test_rig::{catalog, clients, test_manifest, MemJournal};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::exec::RunKind;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::{ExecEvent, JournalClient, RawJournalRow, UnrecordedJournal};

    /// The database side as an unrecorded run sees it: rows written as
    /// they happen, the record written afterwards, and whether the run
    /// was forgotten.
    #[derive(Default)]
    struct Durable {
        rows: StdMutex<Vec<ExecEvent>>,
        recorded: StdMutex<Option<Vec<ExecEvent>>>,
        forgotten: StdMutex<bool>,
    }
    #[async_trait]
    impl JournalClient for Durable {
        async fn record_event(&self, event: &ExecEvent, _: Option<&str>) -> anyhow::Result<()> {
            self.rows.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn raw_rows_after(&self, _: ExecutionId, _: i64, _: std::time::Duration) -> anyhow::Result<Vec<RawJournalRow>> {
            Ok(Vec::new())
        }
        async fn has_terminal_event(&self, _: ExecutionId) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn record_retroactively(&self, events: &[ExecEvent], _: Option<&str>) -> anyhow::Result<()> {
            *self.recorded.lock().unwrap() = Some(events.to_vec());
            Ok(())
        }
        async fn forget_unrecorded(&self, _: ExecutionId, _: Option<&str>) -> anyhow::Result<()> {
            *self.forgotten.lock().unwrap() = true;
            Ok(())
        }
    }

    struct Answer;
    test_manifest!(Answer, "Answer");
    #[async_trait]
    impl Node for Answer {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("ok"))).await
        }
    }

    struct Broken;
    test_manifest!(Broken, "Answer");
    #[async_trait]
    impl Node for Broken {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            Err(weft_core::error::node_error("the status table is gone"))
        }
    }

    struct Waits;
    test_manifest!(Waits, "Answer");
    #[async_trait]
    impl Node for Waits {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 5_000 } };
            ctx.await_signal(timer).await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("late"))).await
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{
                "id": "answer", "nodeType": "Answer", "position": {"x": 0, "y": 0},
                "outputs": [{"name": "value", "portType": "String", "required": false}]
            }]
        }))
        .unwrap()
    }

    /// Drive one unrecorded run of `node` and settle it the way the worker
    /// does. Answers the outcome, the rows the run held, and the durable side.
    async fn drive_unrecorded(node: Box<dyn Node>) -> (ExecutionOutcome, Vec<ExecEvent>, Arc<Durable>) {
        let project = project();
        let execution_id = uuid::Uuid::new_v4();
        let birth = vec![
            ExecEvent::ExecutionStarted {
                execution_id,
                project_id: project.id,
                entry_node: "answer".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
                program: None,
                source_version: None,
                run_kind: RunKind::Unrecorded,
                subgraph: None,
                seed: None,
                instance: None,
                fired_trigger: None,
                instance_values: Default::default(), picks: Default::default(),
                at_unix: 0,
                run_class: weft_core::run_class::RunClass::Short,
            },
            ExecEvent::NodeKicked { execution_id, node_id: "answer".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ];
        let durable = Arc::new(Durable::default());
        let journal = UnrecordedJournal::seeded(execution_id, birth, durable.clone()).unwrap();
        let mut run_clients = clients(Arc::new(MemJournal::default()));
        run_clients.journal = journal.clone();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run_one_execution(
                Arc::new(project),
                catalog(vec![("Answer", node)]),
                execution_id,
                run_clients,
                "instance-test".into(),
                "tenant-test".into(),
                CancellationFlag::new_arc(),
                None,
            ),
        )
        .await
        .expect("the drive hung")
        .expect("the drive ends");
        journal.settle(Some("instance-test")).await.expect("settle");
        (outcome, journal.events(), durable)
    }

    #[tokio::test]
    async fn a_completed_run_leaves_nothing_durable() {
        let (outcome, held, durable) = drive_unrecorded(Box::new(Answer)).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed), "{outcome:?}");
        assert!(held.iter().any(|e| matches!(e, ExecEvent::NodeCompleted { .. })), "the run read its own rows");
        assert!(durable.rows.lock().unwrap().is_empty(), "nothing was written as it ran");
        assert!(durable.recorded.lock().unwrap().is_none(), "nothing was written afterwards");
        assert!(*durable.forgotten.lock().unwrap());
    }

    #[tokio::test]
    async fn a_failed_run_is_written_whole() {
        let (outcome, held, durable) = drive_unrecorded(Box::new(Broken)).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let recorded = durable.recorded.lock().unwrap().clone().expect("the failure is recorded");
        assert_eq!(recorded.len(), held.len(), "every row it held, and no more");
        assert!(matches!(recorded[0], ExecEvent::ExecutionStarted { run_kind: RunKind::Execution, .. }), "it lists as a run");
        assert!(recorded.iter().any(|e| matches!(e, ExecEvent::NodeFailed { error, .. } if error.contains("status table"))));
        assert!(matches!(recorded.last(), Some(ExecEvent::ExecutionFailed { .. })));
        assert!(!*durable.forgotten.lock().unwrap());
    }

    #[tokio::test]
    async fn a_wait_is_refused_at_the_call() {
        let (outcome, _, durable) = drive_unrecorded(Box::new(Waits)).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "{outcome:?}");
        let recorded = durable.recorded.lock().unwrap().clone().expect("the refusal is a failure, so recorded");
        let error = recorded
            .iter()
            .find_map(|e| match e {
                ExecEvent::NodeFailed { error, .. } => Some(error.clone()),
                _ => None,
            })
            .expect("the waiting node failed");
        assert!(error.contains("`recorded`"), "{error}");
        assert!(!recorded.iter().any(|e| matches!(e, ExecEvent::NodeSuspended { .. })), "nothing parked");
    }
