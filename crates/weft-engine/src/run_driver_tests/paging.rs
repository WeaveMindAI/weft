    //! Layer 3: a worker reads a run's record a page at a time. The run
    //! it carries on is folded as the pages come; once it took answers,
    //! its fold reads only the rows that landed since it last looked.

    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex as StdMutex;
    use weft_journal::RunEvent;

    /// A record in memory that keeps what each read asked for.
    struct Pages {
        events: StdMutex<Vec<RunEvent>>,
        asked: StdMutex<Vec<Option<i32>>>,
    }

    #[async_trait]
    impl JournalClient for Pages {
        async fn record_event(&self, event: &RunEvent, _replica: Option<&str>) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        async fn record_page(&self, run_id: RunId, after: Option<i32>) -> anyhow::Result<Vec<weft_journal::JournalRow>> {
            self.asked.lock().unwrap().push(after);
            Ok(crate::test_record::rows_of(run_id, &self.events.lock().unwrap(), after))
        }
        fn keep_secret(&self, _value: serde_json::Value) -> weft_core::secret::SecretId {
            unreachable!("the paging rig makes no secrets")
        }
        async fn reveal_secret(&self, _id: weft_core::secret::SecretId) -> anyhow::Result<serde_json::Value> {
            unreachable!("the paging rig makes no secrets")
        }
    }

    /// `src -> sink`, `sink` reading `in`.
    fn project() -> Arc<ProjectDefinition> {
        Arc::new(serde_json::from_value(json!({
            "id": uuid::Uuid::nil(),
            "nodes": [
                { "id": "src", "nodeType": "T", "position": {"x": 0, "y": 0}, "outputs": [{ "name": "out", "portType": "Number", "required": false }] },
                { "id": "sink", "nodeType": "T", "position": {"x": 0, "y": 0}, "inputs": [{ "name": "in", "portType": "Number", "required": true }] }
            ],
            "edges": [{ "id": "e", "source": "src", "sourceHandle": "out", "target": "sink", "targetHandle": "in" }]
        })).unwrap())
    }

    fn birth(run_id: RunId, project: &ProjectDefinition) -> RunEvent {
        RunEvent::RunStarted {
            given: Default::default(), run_id, project_id: uuid::Uuid::nil(), entry_node: "src".into(),
            phase: weft_core::context::Phase::Fire,
            definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
            binary_hash: None, cut_from: None, source_version: None, run_kind: weft_core::run::RunKind::Project, seed: None,
            instance: None, stand_in: None, fired_trigger: None, settings: Default::default(), instance_values: Default::default(),
            picks: Default::default(), at_unix: 0,
        }
    }

    /// A run carried on from a record longer than a page folds every page
    /// of it; once answers landed, its fold reads past the last row it
    /// took, never the record again, and holds what a fold of the whole
    /// record holds.
    #[tokio::test]
    async fn a_run_s_fold_reads_its_record_a_page_at_a_time_and_then_only_what_landed() {
        let run_id = weft_core::new_run_id();
        let project = project();
        let first = vec![
            birth(run_id, &project),
            RunEvent::NodeStarted { run_id, node_id: "src".into(), frames: vec![], at_unix: 1 },
            RunEvent::PortEmitted { run_id, emission_id: uuid::Uuid::new_v4(), node_id: "src".into(), frames: vec![], port: "out".into(), value: Arc::new(json!(5)), at_unix: 1 },
            RunEvent::NodeCompleted { run_id, node_id: "src".into(), frames: vec![], at_unix: 1 },
            RunEvent::NodeStarted { run_id, node_id: "sink".into(), frames: vec![], at_unix: 2 },
        ];
        let pages = Pages { events: StdMutex::new(first.clone()), asked: StdMutex::default() };
        let claimed = ClaimedRecord::read(
            |run, after| pages.record_page(run, after),
            &engine_test_rig::NoopProject,
            run_id,
            project.clone(),
        )
        .await
        .unwrap();
        assert_eq!(claimed.last_seq, 4);
        assert_eq!(*pages.asked.lock().unwrap(), vec![None, Some(1), Some(3), Some(4)], "a page at a time, to the end");
        let (mut fold, snap) = RunFold::of_live(run_id, project.clone(), claimed.chain, claimed.fold).await.unwrap();
        assert!(snap.firings["sink"][0].status() == weft_core::run::NodeFiringStatus::Running);

        pages.asked.lock().unwrap().clear();
        pages.events.lock().unwrap().push(RunEvent::NodeCompleted { run_id, node_id: "sink".into(), frames: vec![], at_unix: 3 });
        let caught = fold.caught_up(&pages).await.unwrap();
        assert_eq!(*pages.asked.lock().unwrap(), vec![Some(4), Some(5)], "only past the last row it took");
        // The record folded at once, the way the worker folds it (forgetting
        // what ended): the fold caught up lands where it does.
        let mut whole = weft_journal::Fold::new(run_id, project).driving();
        for event in pages.events.lock().unwrap().iter() {
            whole.apply(event);
        }
        let whole = whole.into_snapshot();
        assert!(!caught.firings.contains_key("src"), "the firing that ended is forgotten");
        assert_eq!(
            caught.firings.iter().map(|(node, records)| (node.clone(), records.values().map(|r| r.status()).collect::<Vec<_>>())).collect::<Vec<_>>(),
            whole.firings.iter().map(|(node, records)| (node.clone(), records.values().map(|r| r.status()).collect::<Vec<_>>())).collect::<Vec<_>>(),
        );
    }
