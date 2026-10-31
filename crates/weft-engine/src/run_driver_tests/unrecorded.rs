    //! Layer 3: an unrecorded run through the real loop. Its worker keeps
    //! no history of it: a run that completes writes nothing, one that
    //! fails writes its birth and its failure (marked as not recorded on
    //! its row), and a wait holds in its node's call, since there is
    //! nowhere to park it.

    use super::*;
    use super::engine_test_rig::{born, catalog, clients, run_on_holding, test_manifest, Answers, AwaitTasks, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::run::RunKind;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::RunEvent;

    struct Answer;
    test_manifest!(Answer, "Answer");
    #[async_trait]
    impl Node for Answer {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("ok"))).await
        }
    }

    struct Broken;
    test_manifest!(Broken, "Answer");
    #[async_trait]
    impl Node for Broken {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            Err(weft_core::error::node_error("the status table is gone"))
        }
    }

    struct Waits;
    test_manifest!(Waits, "Answer");
    #[async_trait]
    impl Node for Waits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 5_000 } };
            ctx.await_signal(timer).await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("late"))).await
        }
    }

    /// Starts waiting, then goes on without the wait: its call is dropped
    /// once the wait is registered, and the body returns.
    struct Abandons;
    test_manifest!(Abandons, "Answer");
    #[async_trait]
    impl Node for Abandons {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 5_000 } };
            let waiting = ctx.await_signal(timer);
            tokio::pin!(waiting);
            // Polled until it holds (registered, and waiting in its call),
            // then dropped.
            for _ in 0..50 {
                if futures::poll!(waiting.as_mut()).is_ready() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            ctx.pulse_downstream(NodeOutput::new().set("value", json!("without it"))).await
        }
    }

    /// Panics while a wait it started is open: registered, and waiting in
    /// its call.
    struct PanicsWaiting;
    test_manifest!(PanicsWaiting, "Answer");
    #[async_trait]
    impl Node for PanicsWaiting {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 5_000 } };
            let waiting = ctx.await_signal(timer);
            tokio::pin!(waiting);
            for _ in 0..50 {
                if futures::poll!(waiting.as_mut()).is_ready() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            panic!("the node broke while it waited");
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

    /// Drive one unrecorded run of `node` the way the worker does. Answers
    /// the outcome and what the record holds of it once its writer is done.
    async fn drive_unrecorded(node: Box<dyn Node>) -> (RunOutcome, Vec<RunEvent>) {
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        drive_holding(node, weft_core::run_settings::DEFAULT_HOLD_SECS, AwaitTasks::new(), Arc::new(Answers::default()), &holds).await
    }

    /// [`drive_unrecorded`], the run holding its waits `hold_secs`, its
    /// waits registered with `tasks` and answered through `answers`, its
    /// quiet spells counted in `holds`.
    async fn drive_holding(
        node: Box<dyn Node>,
        hold_secs: u32,
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        holds: &Arc<crate::run_holds::Holds>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        drive_run(weft_core::new_run_id(), node, hold_secs, tasks, answers, holds).await
    }

    /// [`drive_holding`], of run `run_id`.
    async fn drive_run(
        run_id: weft_core::RunId,
        node: Box<dyn Node>,
        hold_secs: u32,
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        holds: &Arc<crate::run_holds::Holds>,
    ) -> (RunOutcome, Vec<RunEvent>) {
        let project = project();
        let birth = birth(run_id, &project, hold_secs);
        let journal = Arc::new(MemJournal::default());
        let run_clients = EngineClients { tasks, told: answers.told.clone(), runs: answers, ..clients(journal.clone()) };
        let run = born(&run_clients, run_id, &birth).await;
        let drove = run_on_holding(Arc::new(project), catalog(vec![("Answer", node)]), &run_clients, run_id, run, birth, CancellationFlag::new_arc(), None, None, holds)
            .await
            .expect("the drive ends");
        run_clients.writer.written().await;
        (drove.outcome, journal.events_of(run_id))
    }

    /// The birth of unrecorded run `run_id` of `project`, holding its
    /// waits `hold_secs`.
    fn birth(run_id: weft_core::RunId, project: &ProjectDefinition, hold_secs: u32) -> Vec<RunEvent> {
        vec![
            RunEvent::RunStarted { given: Default::default(),
                run_id,
                project_id: project.id,
                entry_node: "answer".into(),
                phase: weft_core::context::Phase::Fire,
                definition_hash: Some(weft_core::project::hash::compute_definition_hash(project).unwrap()),
                binary_hash: None,
                source_version: None,
                run_kind: RunKind::Project,
                cut_from: None,
                seed: None,
                instance: None,
                fired_trigger: None,
                stand_in: None,
                instance_values: Default::default(), picks: Default::default(),
                at_unix: 0,
                settings: weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Fast, false)
                    .and_then(|settings| settings.holding_for(hold_secs))
                    .unwrap(),
            },
            RunEvent::NodeKicked { run_id, node_id: "answer".into(), frames: vec![], firing: false, payload: None, port_snapshot: None, at_unix: 0 },
        ]
    }

    struct Listens;
    test_manifest!(Listens, "Answer");
    #[async_trait]
    impl Node for Listens {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let Some(weft_core::caller::CallerHandle::Websocket(ws)) = ctx.caller() else { panic!("a socket caller") };
            let heard = ws.recv_next().await?;
            ctx.pulse_downstream(NodeOutput::new().set("value", json!(format!("heard: {}", heard.is_some())))).await
        }
    }

    /// A run whose one node reads its caller's next message, its own cap
    /// ending at `cap_ends` and its caller's session capped at
    /// `max_session_secs` (0 for none), and a caller silent for two
    /// minutes, then speaking: how the run's worker saw it at two minutes
    /// (quiet runs, and until when), whether it was still going then, and
    /// how it ended.
    async fn a_silent_caller(cap_ends: Option<i64>, max_session_secs: u64) -> ((usize, Option<i64>), bool, RunOutcome) {
        let run_id = weft_core::new_run_id();
        let project = project();
        let birth = birth(run_id, &project, weft_core::run_settings::DEFAULT_HOLD_SECS);
        let run_clients = clients(Arc::new(MemJournal::default()));
        let run = born(&run_clients, run_id, &birth).await;
        let cfg = weft_core::caller::CallerRuntimeConfig {
            protocol: weft_core::signal::Protocol::Websocket,
            data_type: weft_core::signal::DataType::Json,
            backpressure: weft_core::signal::Backpressure::Block,
            error_mode: weft_core::signal::ErrorMode::Surface,
            max_inbound_bytes: 1024,
            caller_silence_secs: weft_core::signal::DEFAULT_CALLER_SILENCE_SECS,
            max_session_secs,
            outlives_caller: false,
            inbound_window: 4,
            journal: weft_core::stream_journal::JournalPolicy::default(),
        };
        let (conn, outbound, inbound) =
            crate::caller_conn::new_connection(cfg, run_id, Arc::new(weft_core::caller::LiveRequest::default()), None, Arc::new(super::engine_test_rig::Unkept));
        // The socket's side: what the run says is taken, and once the run
        // hangs up, the caller is gone.
        tokio::spawn({
            let conn = conn.clone();
            async move {
                while outbound.recv().await.is_some() {}
                conn.mark_disconnected("the socket closed");
            }
        });
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        let project = Arc::new(project);
        let claimed = super::engine_test_rig::claimed(&run_clients, run_id, &project, &birth).await;
        let drive = RunDrive {
            run_id,
            journal: run,
            program: Arc::new(crate::plan::ProgramTables::new(project)),
            starts_from: StartsFrom::Record(Box::new(claimed)),
            cancellation: CancellationFlag::new_arc(),
            exchange: Some(super::engine_test_rig::live_exchange(conn)),
            hand_back: None,
            hold: Some(holds.of(run_id, cap_ends)),
        };
        let catalog = catalog(vec![("Answer", Box::new(Listens) as Box<dyn Node>)]);
        let driving = tokio::spawn(async move { run_one_observed(catalog, &run_clients, drive, "instance-test", "tenant-test").await });
        tokio::time::sleep(std::time::Duration::from_secs(120)).await;
        let (quiet, going) = (holds.quiet(), !driving.is_finished());
        inbound.expect("a socket has an inbound log").push(weft_core::caller::InboundMessage::Json(json!({ "said": "hi" })));
        let drove = driving.await.unwrap().expect("the drive ends");
        (quiet, going, drove.outcome)
    }

    /// A caller silent for two minutes is never cut nor its run unwound:
    /// on a platform with caps, the run is quiet until the first of its own
    /// cap and its caller's session cap, so its worker stops ticking; with
    /// neither, it is not quiet, and its worker keeps ticking.
    #[tokio::test(start_paused = true)]
    async fn a_silent_caller_is_waited_on_and_quiets_the_door_only_under_a_cap() {
        let now = crate::now_unix() as i64;
        let ((quiet, until), going, outcome) = a_silent_caller(Some(now + 3_540), 600).await;
        assert!(going, "two minutes of silence end nothing");
        assert_eq!(quiet, 1, "the run only waits on its caller");
        let until = until.expect("quiet until a cap");
        assert!((now + 600..now + 610).contains(&until), "the session cap comes first: {until}");
        assert!(matches!(outcome, RunOutcome::Completed), "its caller spoke and it carried on: {outcome:?}");

        let ((quiet, until), going, outcome) = a_silent_caller(None, 0).await;
        assert!(going, "two minutes of silence end nothing");
        assert_eq!((quiet, until), (0, None), "no cap: not quiet, its worker keeps ticking");
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
    }

    #[tokio::test]
    async fn a_completed_run_writes_nothing() {
        let (outcome, written) = drive_unrecorded(Box::new(Answer)).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(written.is_empty(), "nothing was written: {written:?}");
    }

    #[tokio::test]
    async fn a_failed_run_leaves_its_failure_and_nothing_else() {
        let (outcome, written) = drive_unrecorded(Box::new(Broken)).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert_eq!(written.len(), 2, "its birth and its failure: {written:?}");
        assert!(matches!(&written[0], RunEvent::RunStarted { settings, .. } if !settings.recorded()), "its row says it was not recorded: {written:?}");
        assert!(
            matches!(&written[1], RunEvent::RunFailed { error, .. } if error.contains("status table")),
            "the failure names what failed: {written:?}"
        );
    }

    /// A wait holds in its node's call, and its answer carries the run on
    /// in the same worker: nothing is written of it. While it only waited,
    /// the run counted as quiet (so its worker may stop ticking) until its
    /// hold would give up, and it moved again once answered.
    #[tokio::test]
    async fn a_wait_holds_and_takes_its_answer() {
        let (tasks, answers) = (AwaitTasks::new(), Arc::new(Answers::default()));
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        // Answered once the run is quiet, as a person answers a run that
        // waited, after reading how long it waits.
        let answering = tokio::spawn({
            let (tasks, answers, holds) = (tasks.clone(), answers.clone(), holds.clone());
            async move {
                let token = tasks.await_token().await;
                let latest = loop {
                    if let (1, Some(latest)) = holds.quiet() {
                        break latest;
                    }
                    tokio::task::yield_now().await;
                };
                answers.answer(token, json!("now"));
                latest
            }
        });
        let asked_at = crate::now_unix() as i64;
        let (outcome, written) = drive_holding(Box::new(Waits), 60, tasks.clone(), answers, &holds).await;
        let quiet_until = answering.await.unwrap();
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert!(written.is_empty(), "nothing was written: {written:?}");
        assert!(tasks.withdrawn.lock().unwrap().is_empty(), "an answered wait is not withdrawn");
        assert!((asked_at + 60..=crate::now_unix() as i64 + 61).contains(&quiet_until), "quiet until its hold gives up: {quiet_until}");
        assert_eq!(holds.quiet(), (0, None), "it moved again");
    }

    /// A waiting run reads its answers once as it starts waiting, and then
    /// only when its worker tells it one was handed to it: another run's
    /// answer, and a read for every run that found none for it, read
    /// nothing; an answer the worker's one read found for it is taken from
    /// that read, with no read of its own.
    #[tokio::test]
    async fn a_waiting_run_reads_its_answers_only_when_told() {
        use crate::context::RunTold;
        let (tasks, answers) = (AwaitTasks::new(), Arc::new(Answers::default()));
        let holds = crate::run_holds::Holds::new(Box::new(|| {}));
        let run = weft_core::new_run_id();
        let telling = tokio::spawn({
            let (tasks, answers, holds) = (tasks.clone(), answers.clone(), holds.clone());
            async move {
                let token = tasks.await_token().await;
                while holds.quiet().0 == 0 {
                    tokio::task::yield_now().await;
                }
                let settled = || async {
                    for _ in 0..50 {
                        tokio::task::yield_now().await;
                    }
                };
                settled().await;
                assert_eq!(answers.reads(), 1, "read once as it started waiting");
                let answer_of = |run, token: &str| weft_broker_client::protocol::RunAnswerOf {
                    run_id: run,
                    token: token.into(),
                    answer: weft_core::primitive::WaitAnswer::Given { value: json!("now") },
                };
                answers.told.send(RunTold::Answered(weft_core::new_run_id())).unwrap();
                answers.told.send(RunTold::Rechecked(Arc::new(vec![answer_of(weft_core::new_run_id(), "other")]))).unwrap();
                settled().await;
                assert_eq!(answers.reads(), 1, "nothing for this run: nothing read");
                answers.told.send(RunTold::Answered(run)).unwrap();
                settled().await;
                assert_eq!(answers.reads(), 2, "told one was handed to it: one read");
                answers.told.send(RunTold::Rechecked(Arc::new(vec![answer_of(run, &token)]))).unwrap();
            }
        });
        let (outcome, _) = drive_run(run, Box::new(Waits), 60, tasks, answers.clone(), &holds).await;
        telling.await.unwrap();
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(answers.reads(), 2, "its answer came with the worker's one read: no read of its own");
    }

    /// A wait nothing answers is given up once the run was quiet for its
    /// hold: the waiting call fails, naming why, and the wait is withdrawn.
    #[tokio::test]
    async fn a_wait_nothing_answers_is_given_up_after_its_hold() {
        let tasks = AwaitTasks::new();
        let (outcome, written) = drive_holding(Box::new(Waits), 1, tasks.clone(), Arc::new(Answers::default()), &crate::run_holds::Holds::new(Box::new(|| {}))).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        let error = written
            .iter()
            .find_map(|e| match e {
                RunEvent::RunFailed { error, .. } => Some(error.clone()),
                _ => None,
            })
            .expect("the waiting node failed, so its run leaves its failure");
        assert!(error.contains("gave up its wait") && error.contains("`holdSecs`") && error.contains("`recorded: false`"), "{error}");
        assert_eq!(*tasks.withdrawn.lock().unwrap(), vec![tasks.minted().expect("the wait was registered")]);
        assert!(!written.iter().any(|e| matches!(e, RunEvent::NodeSuspended { .. })), "nothing parked");
    }

    /// A firing that went on without a wait it started (its call dropped)
    /// leaves nothing behind once it ends: the wait is withdrawn, so
    /// nobody is asked to answer it, and the run completes.
    #[tokio::test]
    async fn a_wait_its_firing_went_on_without_is_withdrawn_when_the_firing_ends() {
        let tasks = AwaitTasks::new();
        let (outcome, _) = drive_holding(Box::new(Abandons), 60, tasks.clone(), Arc::new(Answers::default()), &crate::run_holds::Holds::new(Box::new(|| {}))).await;
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        assert_eq!(*tasks.withdrawn.lock().unwrap(), vec![tasks.minted().expect("the wait was registered")]);
    }

    /// A firing that panics while a wait it started is open leaves nothing
    /// behind either: the panic stops the node's code, and the drive, as
    /// it fails the firing, withdraws the wait.
    #[tokio::test]
    async fn a_wait_open_when_its_firing_panics_is_withdrawn() {
        let tasks = AwaitTasks::new();
        let (outcome, _) = drive_holding(Box::new(PanicsWaiting), 60, tasks.clone(), Arc::new(Answers::default()), &crate::run_holds::Holds::new(Box::new(|| {}))).await;
        assert!(matches!(&outcome, RunOutcome::Failed { error, .. } if error.contains("panicked")), "{outcome:?}");
        assert_eq!(*tasks.withdrawn.lock().unwrap(), vec![tasks.minted().expect("the wait was registered")]);
    }

    /// A hold of zero gives the wait up at once, through the one path a
    /// wait given up takes: registered, then given up and withdrawn.
    #[tokio::test]
    async fn a_hold_of_zero_fails_the_wait_at_once() {
        let tasks = AwaitTasks::new();
        let (outcome, _) = drive_holding(Box::new(Waits), 0, tasks.clone(), Arc::new(Answers::default()), &crate::run_holds::Holds::new(Box::new(|| {}))).await;
        assert!(matches!(&outcome, RunOutcome::Failed { error, held_too_long: true } if error.contains("is 0")), "{outcome:?}");
        assert_eq!(*tasks.withdrawn.lock().unwrap(), vec![tasks.minted().expect("the wait was registered")]);
    }
