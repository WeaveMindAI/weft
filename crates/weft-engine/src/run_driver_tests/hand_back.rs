    //! Layer 3: a run asked to let go of its worker (`hand_back`) starts
    //! no new node, lets the running ones end, and ends `HandedBack` with
    //! its whole record written, which the next drive carries on from.

    use super::*;
    use super::engine_test_rig::{born, catalog, clients, handle, run_on, test_manifest, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::RunEvent;

    /// Says it started, then holds until it is let through.
    struct Gate {
        started: Arc<tokio::sync::Notify>,
        through: Arc<tokio::sync::Notify>,
    }
    test_manifest!(Gate, "Gate");
    #[async_trait]
    impl Node for Gate {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.started.notify_one();
            self.through.notified().await;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("through"))).await
        }
    }

    struct Sink;
    test_manifest!(Sink, "Sink");
    #[async_trait]
    impl Node for Sink {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            Ok(())
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "gate", "nodeType": "Gate", "position": {"x": 0, "y": 0},
                    "outputs": [{"name": "value", "portType": "String", "required": false}]
                },
                {
                    "id": "sink", "nodeType": "Sink", "position": {"x": 1, "y": 0},
                    "inputs": [{"name": "in", "portType": "String", "required": true}]
                }
            ],
            "edges": [{"id": "e", "source": "gate", "target": "sink", "sourceHandle": "value", "targetHandle": "in"}],
            "groups": []
        }))
        .unwrap()
    }

    fn birth(project: &ProjectDefinition, run_id: RunId) -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "gate".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None,
                source_version: None,
                run_kind: weft_core::run::RunKind::Project,
                cut_from: None,
                seed: None,
                instance: None,
                fired_trigger: None,
                stand_in: None,
                instance_values: Default::default(),
                picks: Default::default(),
                settings: Default::default(),
                at_unix: 0,
            },
            RunEvent::NodeKicked {
                run_id,
                node_id: "gate".into(),
                frames: vec![],
                firing: false,
                payload: None,
                port_snapshot: None,
                at_unix: 0,
            },
        ]
    }

    fn started(events: &[RunEvent], node: &str) -> bool {
        events.iter().any(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id == node))
    }

    weft_core::stress_test! {
        name: a_run_handed_back_ends_its_running_node_starts_no_other_and_carries_on_from_its_record,
        runs: 20,
        worker_threads: 4,
        async fn body() {
            let project = project();
            let run_id = weft_core::new_run_id();
            let birth = birth(&project, run_id);
            let journal = Arc::new(MemJournal::default());
            let (started_gate, through) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
            let nodes = || {
                catalog(vec![
                    ("Gate", Box::new(Gate { started: started_gate.clone(), through: through.clone() })),
                    ("Sink", Box::new(Sink)),
                ])
            };
            let first_clients = clients(journal.clone());
            let run = born(&first_clients, run_id, &birth).await;
            let hand_back = HandBack::default();
            let drive = {
                let (project, nodes, hand_back) = (Arc::new(project.clone()), nodes(), hand_back.clone());
                tokio::spawn(async move {
                    run_on(
                        project, nodes, &first_clients, run_id, run, birth,
                        CancellationFlag::new_arc(), None, Some(&hand_back),
                    )
                    .await
                    .map(|drove| drove.outcome)
                })
            };
            // Asked to let go while the gate runs: the gate ends, the sink it
            // feeds never starts here.
            started_gate.notified().await;
            hand_back.asked.cancel();
            through.notify_one();
            let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), drive).await.expect("the drive hung").unwrap().unwrap();
            assert!(matches!(outcome, RunOutcome::HandedBack), "{outcome:?}");
            let events = journal.events_of(run_id);
            assert!(events.iter().any(|e| matches!(e, RunEvent::NodeCompleted { node_id, .. } if node_id == "gate")), "the running node ended on record");
            assert!(!started(&events, "sink"), "no node started once asked to let go");
            assert!(!events.iter().any(RunEvent::is_run_terminal), "a run handed back has not ended");

            // The next worker carries it on from that record: the sink runs,
            // the gate does not run again.
            let next_clients = clients(journal.clone());
            let rows = journal.events_of(run_id);
            let run = handle(&next_clients, run_id, &rows, 1, Default::default());
            let outcome = run_on(Arc::new(project), nodes(), &next_clients, run_id, run, rows, CancellationFlag::new_arc(), None, None)
                .await
                .unwrap()
                .outcome;
            // A fast run lets go of its record once its ending is handed over.
            next_clients.writer.written().await;
            assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
            let events = journal.events_of(run_id);
            assert!(started(&events, "sink"));
            let gate_starts = events.iter().filter(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id == "gate")).count();
            assert_eq!(gate_starts, 1, "the node that ended before the hand-back is not run again");
        }
    }

    /// A run with nothing left to start when it is asked to let go ends
    /// the way it ended: nothing is handed back.
    #[tokio::test]
    async fn a_run_that_finishes_its_last_node_while_letting_go_completes() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [{
                "id": "gate", "nodeType": "Gate", "position": {"x": 0, "y": 0},
                "outputs": [{"name": "value", "portType": "String", "required": false}]
            }],
            "edges": [],
            "groups": []
        }))
        .unwrap();
        let run_id = weft_core::new_run_id();
        let birth = birth(&project, run_id);
        let journal = Arc::new(MemJournal::default());
        let (started_gate, through) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        let nodes = catalog(vec![("Gate", Box::new(Gate { started: started_gate.clone(), through: through.clone() }))]);
        let run_clients = clients(journal.clone());
        let run = born(&run_clients, run_id, &birth).await;
        let hand_back = HandBack::default();
        let drive = {
            let hand_back = hand_back.clone();
            tokio::spawn(async move {
                run_on(
                    Arc::new(project), nodes, &run_clients, run_id, run, birth,
                    CancellationFlag::new_arc(), None, Some(&hand_back),
                )
                .await
                .map(|drove| drove.outcome)
            })
        };
        started_gate.notified().await;
        hand_back.asked.cancel();
        through.notify_one();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), drive).await.expect("the drive hung").unwrap().unwrap();
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
    }

    /// A node still running when the hand-back is overdue is stopped where
    /// it is, the run is handed back, and the next worker fails its firing
    /// as cut short rather than running it again.
    #[tokio::test]
    async fn a_node_still_running_when_the_hand_back_is_overdue_is_failed_by_the_next_worker() {
        let project = project();
        let run_id = weft_core::new_run_id();
        let birth = birth(&project, run_id);
        let journal = Arc::new(MemJournal::default());
        // The gate is never let through.
        let started_gate = Arc::new(tokio::sync::Notify::new());
        let nodes = || {
            catalog(vec![
                ("Gate", Box::new(Gate { started: started_gate.clone(), through: Arc::new(tokio::sync::Notify::new()) })),
                ("Sink", Box::new(Sink)),
            ])
        };
        let run_clients = clients(journal.clone());
        let run = born(&run_clients, run_id, &birth).await;
        let hand_back = HandBack::default();
        let drive = {
            let (project, nodes, hand_back) = (Arc::new(project.clone()), nodes(), hand_back.clone());
            tokio::spawn(async move {
                run_on(
                    project, nodes, &run_clients, run_id, run, birth,
                    CancellationFlag::new_arc(), None, Some(&hand_back),
                )
                .await
                .map(|drove| drove.outcome)
            })
        };
        started_gate.notified().await;
        hand_back.asked.cancel();
        hand_back.overdue.cancel();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), drive).await.expect("the drive hung").unwrap().unwrap();
        assert!(matches!(outcome, RunOutcome::HandedBack), "{outcome:?}");
        let events = journal.events_of(run_id);
        assert!(started(&events, "gate"));
        assert!(!events.iter().any(RunEvent::is_run_terminal));

        let next_clients = clients(journal.clone());
        let rows = journal.events_of(run_id);
        let run = handle(&next_clients, run_id, &rows, 1, Default::default());
        run_on(Arc::new(project), nodes(), &next_clients, run_id, run, rows, CancellationFlag::new_arc(), None, None)
            .await
            .unwrap();
        // A fast run lets go of its record once its ending is handed over.
        next_clients.writer.written().await;
        let events = journal.events_of(run_id);
        assert!(events.iter().any(|e| matches!(e, RunEvent::NodeFailed { node_id, .. } if node_id == "gate")), "the cut node is failed");
        let gate_starts = events.iter().filter(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id == "gate")).count();
        assert_eq!(gate_starts, 1, "a node cut short is not run again");
    }

    /// Says it started, sends once it is let through, then holds for good.
    struct Talker {
        started: Arc<tokio::sync::Notify>,
        through: Arc<tokio::sync::Notify>,
    }
    test_manifest!(Talker, "Talker");
    #[async_trait]
    impl Node for Talker {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.started.notify_one();
            self.through.notified().await;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("said"))).await?;
            std::future::pending().await
        }
    }

    /// `project` with its gate a [`Talker`].
    fn talking(project: &ProjectDefinition) -> ProjectDefinition {
        let mut project = project.clone();
        project.nodes[0].node_type = "Talker".into();
        project
    }

    fn talker_nodes(started: Arc<tokio::sync::Notify>, through: Arc<tokio::sync::Notify>) -> Arc<dyn weft_core::NodeCatalog> {
        catalog(vec![("Talker", Box::new(Talker { started, through })), ("Sink", Box::new(Sink))])
    }

    /// A run of [`talking`] kept `keeping`, whose record's broker goes out
    /// of reach for good once the gate started and before it sends (its
    /// batches are sent again for a tenth of a second, then given up):
    /// what its drive ended with, and the record that landed.
    async fn drive_until_the_record_is_out_of_reach(
        project: &ProjectDefinition,
        keeping: weft_core::run_settings::Keeping,
    ) -> (RunId, anyhow::Result<RunOutcome>, Arc<MemJournal>) {
        let run_id = weft_core::new_run_id();
        let mut birth = birth(project, run_id);
        if let RunEvent::RunStarted { settings, .. } = &mut birth[0] {
            *settings = weft_core::run_settings::RunSettings::new(keeping, true).unwrap();
        }
        let journal = Arc::new(MemJournal::default());
        let (started_gate, through) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        let nodes = talker_nodes(started_gate.clone(), through.clone());
        let run_clients = super::engine_test_rig::clients_writing(
            journal.clone(),
            crate::journal_writer::WriterSettings { resend_for: std::time::Duration::from_millis(100), ..Default::default() },
        );
        let run = born(&run_clients, run_id, &birth).await;
        let drive = {
            let project = Arc::new(project.clone());
            tokio::spawn(async move {
                run_on(project, nodes, &run_clients, run_id, run, birth, CancellationFlag::new_arc(), None, None)
                    .await
                    .map(|drove| drove.outcome)
            })
        };
        started_gate.notified().await;
        journal.out_of_reach.store(true, std::sync::atomic::Ordering::SeqCst);
        through.notify_one();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), drive).await.expect("the drive hung").unwrap();
        journal.out_of_reach.store(false, std::sync::atomic::Ordering::SeqCst);
        (run_id, outcome, journal)
    }

    /// A durable run whose record can no longer be written (its broker out
    /// of reach past the resend budget) is handed back, not failed: its
    /// running node is stopped, nothing ends the run, and the next worker
    /// carries it on from what landed, failing the node whose end never
    /// landed rather than running it again.
    #[tokio::test]
    async fn a_durable_run_whose_record_cannot_be_written_is_handed_back() {
        let project = talking(&project());
        let (run_id, outcome, journal) = drive_until_the_record_is_out_of_reach(&project, weft_core::run_settings::Keeping::Durable).await;
        assert!(matches!(outcome, Ok(RunOutcome::HandedBack)), "{outcome:?}");
        let events = journal.events_of(run_id);
        assert!(started(&events, "gate"), "the gate's start landed before the broker went");
        assert!(!events.iter().any(|e| matches!(e, RunEvent::PortEmitted { .. })), "what it sent after never landed");
        assert!(!events.iter().any(RunEvent::is_run_terminal), "nothing ended the run: {events:?}");

        let next_clients = clients(journal.clone());
        let rows = journal.events_of(run_id);
        let run = handle(&next_clients, run_id, &rows, 1, Default::default());
        let nodes = talker_nodes(Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        run_on(Arc::new(project), nodes, &next_clients, run_id, run, rows, CancellationFlag::new_arc(), None, None)
            .await
            .unwrap();
        next_clients.writer.written().await;
        let events = journal.events_of(run_id);
        assert!(events.iter().any(|e| matches!(e, RunEvent::NodeFailed { node_id, .. } if node_id == "gate")), "the cut node is failed");
        let gate_starts = events.iter().filter(|e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id == "gate")).count();
        assert_eq!(gate_starts, 1, "a node cut short is not run again");
        assert!(events.iter().any(RunEvent::is_run_terminal), "the next worker ended it");
    }

    /// A fast run whose record can no longer be written is not built to
    /// survive it: its drive fails, and the broker is asked to end it.
    #[tokio::test]
    async fn a_fast_run_whose_record_cannot_be_written_fails() {
        let (run_id, outcome, journal) = drive_until_the_record_is_out_of_reach(&talking(&project()), weft_core::run_settings::Keeping::Fast).await;
        assert!(outcome.is_err(), "{outcome:?}");
        let events = journal.events_of(run_id);
        assert!(events.iter().any(|e| matches!(e, RunEvent::RunFailed { .. })), "the broker ended it: {events:?}");
    }

    /// A run asked to leave that cannot be suspended carries on as if
    /// nothing was asked, past the hand-back's deadline: it gets as far as
    /// it can before the process goes.
    #[tokio::test]
    async fn a_run_that_cannot_be_suspended_carries_on_when_asked_to_leave() {
        let project = project();
        let run_id = weft_core::new_run_id();
        let mut birth = birth(&project, run_id);
        if let RunEvent::RunStarted { settings, .. } = &mut birth[0] {
            *settings = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Fast, false).unwrap();
        }
        let journal = Arc::new(MemJournal::default());
        let (started_gate, through) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        let nodes = catalog(vec![
            ("Gate", Box::new(Gate { started: started_gate.clone(), through: through.clone() })),
            ("Sink", Box::new(Sink)),
        ]);
        let run_clients = clients(journal.clone());
        let run = born(&run_clients, run_id, &birth).await;
        let hand_back = HandBack::default();
        let drive = {
            let hand_back = hand_back.clone();
            tokio::spawn(async move {
                run_on(
                    Arc::new(project), nodes, &run_clients, run_id, run, birth,
                    CancellationFlag::new_arc(), None, Some(&hand_back),
                )
                .await
                .map(|drove| drove.outcome)
            })
        };
        started_gate.notified().await;
        hand_back.asked.cancel();
        hand_back.overdue.cancel();
        through.notify_one();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), drive).await.expect("the drive hung").unwrap().unwrap();
        assert!(matches!(outcome, RunOutcome::Completed), "the gate and the sink after it ran: {outcome:?}");
    }
