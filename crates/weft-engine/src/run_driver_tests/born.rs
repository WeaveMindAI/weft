    //! Layer 3: a run born at the worker's door through the real loop. Its
    //! birth was never read back from a journal: the drive writes it first,
    //! through the same journal as everything after it.

    use super::*;
    use super::engine_test_rig::{catalog, clients, clients_writing, handle, run_on, test_manifest, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::JournalClient;

    struct Answer;
    test_manifest!(Answer, "Answer");
    #[async_trait]
    impl Node for Answer {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("ok"))).await
        }
    }

    fn project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{
                "id": "a", "nodeType": "Answer", "position": {"x": 0, "y": 0},
                "outputs": [{"name": "value", "portType": "String", "required": false}]
            }]
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn a_run_born_here_writes_its_birth_first_and_runs_to_its_end() {
        let project = project();
        let run_id = weft_core::new_run_id();
        let birth = vec![
            weft_journal::RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "a".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
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
            weft_journal::RunEvent::NodeKicked {
                run_id,
                node_id: "a".into(),
                frames: vec![],
                firing: false,
                payload: None,
                port_snapshot: None,
                at_unix: 0,
            },
        ];
        let journal = std::sync::Arc::new(MemJournal::default());
        let clients = clients(journal.clone());
        // What the door does before the drive: hand the birth to the run's
        // record.
        let run = handle(&clients, run_id, &birth, 0, Default::default());
        run.record_events(&birth, Some("instance-test")).await.unwrap();
        let drove = run_on(
            std::sync::Arc::new(project),
            catalog(vec![("Answer", Box::new(Answer))]),
            &clients,
            run_id,
            run,
            birth.clone(),
            weft_core::cancellation::CancellationFlag::new_arc(),
            None,
            None,
        )
        .await
        .expect("the drive ends");
        // A fast run lets go of its record once its ending is handed over.
        clients.writer.written().await;
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        let events = journal.events_of(run_id);
        let json = |events: &[weft_journal::RunEvent]| serde_json::to_value(events).unwrap();
        assert_eq!(json(&events[..2]), json(&birth), "the birth is first in the log");
        assert!(events.iter().any(|e| matches!(e, weft_journal::RunEvent::RunCompleted { .. })));
    }

    /// What `Probe` found on record when its body ran.
    static PROBED: std::sync::Mutex<Option<bool>> = std::sync::Mutex::new(None);
    static PROBE_JOURNAL: std::sync::OnceLock<std::sync::Arc<MemJournal>> = std::sync::OnceLock::new();

    struct Probe;
    test_manifest!(Probe, "Answer");
    #[async_trait]
    impl Node for Probe {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let journal = PROBE_JOURNAL.get().expect("the test set its journal");
            let events = journal.events.lock().unwrap().clone();
            let started = events.iter().any(|e| matches!(e, weft_journal::RunEvent::NodeStarted { node_id, .. } if node_id == "a"));
            *PROBED.lock().unwrap() = Some(started);
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("ok"))).await
        }
    }

    /// A durable run's firing is on record before its body runs: the node
    /// finds its own start already written.
    #[tokio::test]
    async fn a_durable_runs_firing_is_on_record_before_its_body_runs() {
        let project = project();
        let run_id = weft_core::new_run_id();
        let durable = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Durable,
            true,
        )
        .unwrap();
        let birth = vec![
            weft_journal::RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "a".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
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
                settings: durable,
                at_unix: 0,
            },
            weft_journal::RunEvent::NodeKicked {
                run_id,
                node_id: "a".into(),
                frames: vec![],
                firing: false,
                payload: None,
                port_snapshot: None,
                at_unix: 0,
            },
        ];
        let journal = std::sync::Arc::new(MemJournal::default());
        // A gathering pause far longer than the test: only a run that
        // waits for its rows sees them written.
        let clients = clients_writing(journal.clone(), crate::journal_writer::WriterSettings {
            gather: std::time::Duration::from_secs(3600),
            ..Default::default()
        });
        PROBE_JOURNAL.set(journal.clone()).ok().expect("one test sets it");
        journal.seed(&birth);
        let run = handle(&clients, run_id, &birth, 1, Default::default());
        let drove = run_on(
            std::sync::Arc::new(project),
            catalog(vec![("Answer", Box::new(Probe))]),
            &clients,
            run_id,
            run,
            birth,
            weft_core::cancellation::CancellationFlag::new_arc(),
            None,
            None,
        )
        .await
        .expect("the drive ends");
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        assert_eq!(*PROBED.lock().unwrap(), Some(true), "the start was on record when the body ran");
    }

    struct Pass;
    test_manifest!(Pass, "Pass");
    #[async_trait]
    impl Node for Pass {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("ok"))).await
        }
    }

    /// A run born from its trigger's plan, the way the door bears one,
    /// runs through the real loop to its end: its birth is handed to its
    /// record and never folded, its first state is the plan's, and every
    /// node of the part of the program it runs ran.
    #[tokio::test]
    async fn a_run_born_from_a_plan_runs_to_its_end_without_a_fold() {
        let project = crate::plan::tests::chain();
        let plan = crate::plan::tests::plan_of(&project, "trig", &Default::default()).await;
        let Ok(startable) = plan.start.as_ref() else { panic!("a plan that starts") };
        let run_id = weft_core::new_run_id();
        let payload = json!({ "tick": 1 });
        let birth = startable.birth(&plan, run_id, &payload, 0);
        let journal = std::sync::Arc::new(MemJournal::default());
        let clients = clients(journal.clone());
        let run = handle(&clients, run_id, &birth, 0, Default::default());
        run.record_events(&birth, Some("instance-test")).await.unwrap();
        let born_with = crate::run_driver::BornWith {
            phase: weft_core::context::Phase::Fire,
            instance: None,
            instance_values: Default::default(),
            picks: Default::default(),
            settings: Default::default(),
            given: startable.given.clone(),
        };
        let drive = crate::run_driver::RunDrive {
            run_id,
            journal: run,
            program: startable.program.clone(),
            starts_from: crate::run_driver::StartsFrom::Plan { born_with, kicked: startable.opening(&payload) },
            cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
            exchange: None,
            hand_back: None,
            hold: None,
        };
        let catalog = catalog(vec![("Trig", Box::new(Answer)), ("Pass", Box::new(Pass))]);
        let drove = crate::run_driver::run_one_observed(catalog, &clients, drive, "instance-test", "tenant-test")
            .await
            .expect("the drive ends");
        clients.writer.written().await;
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        let events = journal.events_of(run_id);
        for node in ["trig", "a", "b"] {
            assert!(
                events.iter().any(|e| matches!(e, weft_journal::RunEvent::NodeCompleted { node_id, .. } if node_id == node)),
                "{node} ran: {events:?}"
            );
        }
    }

    struct Replies;
    test_manifest!(Replies, "Replies");
    #[async_trait]
    impl Node for Replies {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let http = ctx.http_caller().await?;
            http.respond(weft_core::caller::OutboundChunk::Text("pong".into())).await
        }
    }

    /// A durable run whose answer is its last act writes once after its
    /// node started: the answer, the node's end and the run's ending go
    /// on record together, and the answer leaves only once they are there.
    #[tokio::test]
    async fn a_durable_answer_that_ends_its_run_shares_the_endings_write() {
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "edges": [],
            "nodes": [{ "id": "a", "nodeType": "Replies", "position": {"x": 0, "y": 0}, "outputs": [] }]
        }))
        .unwrap();
        let run_id = weft_core::new_run_id();
        let durable = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Durable, true).unwrap();
        let birth = vec![
            weft_journal::RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "a".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(&project).unwrap()),
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
                settings: durable,
                at_unix: 0,
            },
            weft_journal::RunEvent::NodeKicked { run_id, node_id: "a".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ];
        let journal = std::sync::Arc::new(MemJournal::default());
        // Nothing goes out unless somebody waits for it.
        let clients = clients_writing(journal.clone(), crate::journal_writer::WriterSettings {
            gather: std::time::Duration::from_secs(3600),
            ..Default::default()
        });
        journal.seed(&birth);
        let run = handle(&clients, run_id, &birth, 1, Default::default());
        let config = weft_core::caller::CallerRuntimeConfig {
            protocol: weft_core::signal::Protocol::Http,
            data_type: weft_core::signal::DataType::Json,
            backpressure: weft_core::signal::Backpressure::Block,
            error_mode: weft_core::signal::ErrorMode::Surface,
            max_inbound_bytes: 1024,
            caller_silence_secs: weft_core::signal::DEFAULT_CALLER_SILENCE_SECS,
            max_session_secs: 0,
            outlives_caller: false,
            inbound_window: weft_core::caller::DEFAULT_INBOUND_WINDOW,
            journal: weft_core::stream_journal::JournalPolicy::default(),
        };
        let sink = crate::worker::ExchangeJournal::new(run.clone(), "instance-test".into(), config.journal);
        let (conn, outbound, _) = crate::caller_conn::new_connection(
            config,
            run_id,
            std::sync::Arc::new(Default::default()),
            None,
            sink.clone(),
        );
        // The caller's side: what it hears, and what was on record then.
        let heard = {
            let (journal, conn) = (journal.clone(), conn.clone());
            tokio::spawn(async move {
                let first = outbound.recv().await;
                let ended = journal.events_of(run_id).iter().any(|e| matches!(e, weft_journal::RunEvent::RunCompleted { .. }));
                // What the call's task does once the answer left.
                conn.mark_disconnected("response complete");
                (first, ended)
            })
        };
        let program_project = std::sync::Arc::new(project);
        let drive = crate::run_driver::RunDrive {
            run_id,
            journal: run,
            program: std::sync::Arc::new(crate::plan::ProgramTables::new(program_project.clone())),
            starts_from: crate::run_driver::StartsFrom::Record(Box::new(super::engine_test_rig::claimed(&clients, run_id, &program_project, &birth).await)),
            cancellation: weft_core::cancellation::CancellationFlag::new_arc(),
            exchange: Some(crate::run_driver::Exchange { conn: conn.clone(), live: Some(conn.clone()), sink }),
            hand_back: None,
            hold: None,
        };
        let drove = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            crate::run_driver::run_one_observed(catalog(vec![("Replies", Box::new(Replies))]), &clients, drive, "instance-test", "tenant-test"),
        )
        .await
        .expect("the drive ends")
        .expect("the drive runs");
        assert!(matches!(drove.outcome, RunOutcome::Completed), "{:?}", drove.outcome);
        let (first, ended) = heard.await.unwrap();
        assert!(matches!(first, Some(crate::caller_conn::Outbound::Terminate(Some(_), _))), "the answer: {first:?}");
        assert!(ended, "the answer left only once the run's ending was on record");
        // The node's start (its body waits for it), then one write for the
        // rest.
        assert_eq!(journal.batches.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    /// A live conversation keeps the journal window its trigger was set
    /// up with: two messages a few short windows apart land in rows of
    /// their own, where the default one-second window would have put them
    /// in one row.
    #[tokio::test(start_paused = true)]
    async fn a_conversation_keeps_the_journal_window_it_was_set_up_with() {
        use crate::caller_conn::CallerJournalSink;
        use weft_core::stream_journal::{JournalPolicy, DEFAULT_JOURNAL_WINDOW, MIN_JOURNAL_WINDOW};
        let rows_of = |window: std::time::Duration| async move {
            let run_id = weft_core::new_run_id();
            let journal = std::sync::Arc::new(MemJournal::default());
            let clients = clients_writing(journal.clone(), Default::default());
            let run = handle(&clients, run_id, &[], 0, Default::default());
            let sink = crate::worker::ExchangeJournal::new(run, "instance-test".into(), JournalPolicy { window, ..Default::default() });
            let said = |n: u64| weft_core::caller::InboundMessage::Json(json!(n));
            sink.inbound(run_id, 1, &said(1));
            tokio::time::sleep(MIN_JOURNAL_WINDOW * 3).await;
            sink.inbound(run_id, 2, &said(2));
            sink.disconnected(run_id, 3, "done", &[]);
            clients.writer.written().await;
            let rows: Vec<usize> = journal
                .events_of(run_id)
                .iter()
                .filter_map(|e| match e {
                    weft_journal::RunEvent::CallerWindow { messages, .. } => Some(messages.len()),
                    _ => None,
                })
                .collect();
            rows
        };
        assert_eq!(rows_of(MIN_JOURNAL_WINDOW).await, [1, 1], "a short window, a row each");
        assert_eq!(rows_of(DEFAULT_JOURNAL_WINDOW).await, [2], "the default, one row for both");
    }
