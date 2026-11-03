    //! Layer 3: a run pauses in one way, once every node it runs waits and
    //! it stayed that quiet for its `pauseAfterSecs`. A lone node looping
    //! on `state.wait` holds each wait in its call meanwhile: an answer
    //! inside the window goes straight into the waiting call, with no pause
    //! and no restart of the body; with nothing coming, the run pauses
    //! once the window runs out, the firing on record as suspended on its
    //! wait. A firing an earlier pause parked, its answer coming while the
    //! run goes on, resumes in place. On tokio's paused clock, so the
    //! seconds pass as the test says.

    use super::*;
    use super::engine_test_rig::{birth_kicking, catalog, clients_on_paused_clock, human_form, node_json, run_checked, test_manifest, Answers, AwaitTasks, MemJournal};
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{NodeCatalog, ProjectDefinition, RunContext};
    use weft_journal::RunEvent;

    /// What the asker keeps: the answers so far and the one it waits on.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Asked {
        answers: Vec<Value>,
        next: Option<Value>,
    }

    /// Asks twice in a loop, counting how many times its body starts.
    struct Asker {
        starts: Arc<AtomicU32>,
    }
    test_manifest!(Asker, "Asker");
    #[async_trait]
    impl Node for Asker {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            let mut state = ctx.keep(Asked::default()).await?;
            while state.answers.len() < 2 {
                let answer = state.wait(|s| &mut s.next, human_form()).await?;
                state.answers.push(answer);
                state.next = None;
                state.save().await?;
            }
            ctx.pulse_downstream(NodeOutput::new().set("out", json!({ "answers": state.answers }))).await
        }
    }

    fn lone_asker(starts: Arc<AtomicU32>) -> (ProjectDefinition, Arc<dyn NodeCatalog>) {
        let project = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node_json("asker", "Asker", &[], &[("out", "JsonDict")])],
            "edges": [],
            "groups": []
        }))
        .expect("project");
        (project, catalog(vec![("Asker", Box::new(Asker { starts }))]))
    }

    /// A run of the lone asker, staying `pause_after_secs` quiet before it
    /// pauses, driven by one worker on the test's runtime.
    struct Quiet {
        run_id: RunId,
        journal: Arc<MemJournal>,
        tasks: Arc<AwaitTasks>,
        answers: Arc<Answers>,
        starts: Arc<AtomicU32>,
        drive: tokio::task::JoinHandle<anyhow::Result<RunOutcome>>,
    }

    impl Quiet {
        fn start(pause_after_secs: u32) -> Self {
            let starts = Arc::new(AtomicU32::new(0));
            let (project, catalog) = lone_asker(starts.clone());
            let run_id = weft_core::new_run_id();
            let journal = Arc::new(MemJournal::default());
            let settings = weft_core::run_settings::RunSettings::default().pausing_after(pause_after_secs).expect("a stay within the cap");
            journal.seed(&birth_kicking(&project, run_id, &["asker"], settings));
            let (tasks, answers) = (AwaitTasks::new(), Arc::new(Answers::default()));
            let clients = EngineClients {
                tasks: tasks.clone(),
                told: answers.told.clone(),
                runs: answers.clone(),
                ..clients_on_paused_clock(journal.clone())
            };
            let drive = tokio::spawn(run_checked(Arc::new(project), catalog, run_id, journal.clone(), clients, CancellationFlag::new_arc(), None));
            Self { run_id, journal, tasks, answers, starts, drive }
        }

        /// Once the asker holds its wait and the run asks for its answers
        /// for the `reads`-th time: the run is quiet.
        async fn quiet(&self, after: Option<&str>, reads: usize) -> String {
            let token = match after {
                None => self.tasks.await_token().await,
                Some(earlier) => self.tasks.await_token_after(earlier).await,
            };
            self.answers.read_past(reads).await;
            token
        }

        async fn outcome(self) -> (RunOutcome, Vec<RunEvent>, u32) {
            let outcome = self.drive.await.expect("the drive joins").expect("the drive runs");
            (outcome, self.journal.events_of(self.run_id), self.starts.load(Ordering::SeqCst))
        }
    }

    fn count(events: &[RunEvent], matches: impl Fn(&RunEvent) -> bool) -> usize {
        events.iter().filter(|e| matches(e)).count()
    }

    /// Answers inside the window go straight into the waiting call: the
    /// body runs once, never pauses, and the run completes on its worker.
    #[tokio::test(start_paused = true)]
    async fn an_answer_inside_the_window_carries_a_lone_wait_on_with_no_pause() {
        let run = Quiet::start(10);
        let first = run.quiet(None, 0).await;
        tokio::time::sleep(Duration::from_secs(6)).await;
        assert!(!run.drive.is_finished(), "6 seconds quiet, the run is still on its worker");
        run.answers.answer(first.clone(), json!("a"));
        let second = run.quiet(Some(&first), 1).await;
        tokio::time::sleep(Duration::from_secs(9)).await;
        assert!(!run.drive.is_finished(), "the window started over when the run went quiet again");
        run.answers.answer(second, json!("b"));
        let (outcome, events, starts) = run.outcome().await;
        assert!(matches!(outcome, RunOutcome::Completed), "the run carries on to its end: {outcome:?}");
        assert_eq!(starts, 1, "the body never started again");
        assert_eq!(count(&events, |e| matches!(e, RunEvent::NodeSuspended { .. })), 0, "nothing paused: {events:?}");
        assert_eq!(count(&events, |e| matches!(e, RunEvent::NodeResumed { .. })), 0, "nothing resumed: {events:?}");
    }

    /// With nothing coming, the run pauses once the window runs out, not
    /// before, and the firing goes on record as suspended on its wait, so
    /// a later answer resumes it. A window of `0` pauses at once.
    #[tokio::test(start_paused = true)]
    async fn a_lone_wait_with_nothing_coming_pauses_once_the_window_runs_out() {
        let run = Quiet::start(10);
        let token = run.quiet(None, 0).await;
        let quiet_at = tokio::time::Instant::now();
        let (outcome, events, starts) = run.outcome().await;
        assert!(matches!(outcome, RunOutcome::Stalled), "the run pauses: {outcome:?}");
        let stayed = quiet_at.elapsed();
        assert!(stayed >= Duration::from_secs(10), "it stayed its 10 seconds: {stayed:?}");
        assert!(stayed < Duration::from_secs(20), "and paused then, well before its hold would end: {stayed:?}");
        assert_eq!(starts, 1);
        let suspended = |e: &RunEvent| matches!(e, RunEvent::NodeSuspended { node_id, token: Some(on), .. } if node_id == "asker" && *on == token);
        assert_eq!(count(&events, suspended), 1, "suspended on its wait: {events:?}");

        let started = tokio::time::Instant::now();
        let (outcome, _, _) = Quiet::start(0).outcome().await;
        assert!(matches!(outcome, RunOutcome::Stalled), "the run pauses: {outcome:?}");
        assert!(started.elapsed() < Duration::from_secs(10), "0 pauses at once: {:?}", started.elapsed());
    }

    /// Waits on a person once, then rings `go`.
    struct RingsWhenAnswered {
        go: Arc<tokio::sync::Notify>,
    }
    test_manifest!(RingsWhenAnswered, "RingsWhenAnswered");
    #[async_trait]
    impl Node for RingsWhenAnswered {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            super::engine_test_rig::wait_once(&ctx, human_form()).await?;
            self.go.notify_one();
            Ok(())
        }
    }

    /// Waits on a person once, then works until `go` rings (a wait the
    /// run does not know of, so the run keeps its worker meanwhile).
    struct WorksUntilRung {
        go: Arc<tokio::sync::Notify>,
    }
    test_manifest!(WorksUntilRung, "WorksUntilRung");
    #[async_trait]
    impl Node for WorksUntilRung {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            super::engine_test_rig::wait_once(&ctx, human_form()).await?;
            self.go.notified().await;
            Ok(())
        }
    }

    /// Both nodes pause together. The next worker carries on the one whose
    /// wait was answered, while the other stays parked on its wait; the
    /// answer to that one, coming while the first still works, resumes it
    /// in place on the same worker.
    #[tokio::test(start_paused = true)]
    async fn a_firing_parked_by_an_earlier_pause_resumes_in_place_while_the_run_goes_on() {
        let go = Arc::new(tokio::sync::Notify::new());
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node_json("ringer", "RingsWhenAnswered", &[], &[]), node_json("worker", "WorksUntilRung", &[], &[])],
            "edges": [],
            "groups": []
        }))
        .expect("project");
        let catalog = catalog(vec![
            ("RingsWhenAnswered", Box::new(RingsWhenAnswered { go: go.clone() })),
            ("WorksUntilRung", Box::new(WorksUntilRung { go: go.clone() })),
        ]);
        let project = Arc::new(project);
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth_kicking(&project, run_id, &["ringer", "worker"], super::engine_test_rig::pausing_at_once()));
        let drive = |answers: Arc<Answers>| {
            let clients = EngineClients { tasks: AwaitTasks::new(), told: answers.told.clone(), runs: answers, ..clients_on_paused_clock(journal.clone()) };
            tokio::spawn(run_checked(project.clone(), catalog.clone(), run_id, journal.clone(), clients, CancellationFlag::new_arc(), None))
        };

        let paused = drive(Arc::new(Answers::default())).await.expect("joins").expect("the first drive runs");
        assert!(matches!(paused, RunOutcome::Stalled), "both nodes paused together: {paused:?}");
        let events = journal.events_of(run_id);
        let token_of = |node: &str| {
            events
                .iter()
                .find_map(|e| match e {
                    RunEvent::NodeSuspended { node_id, token: Some(token), .. } if node_id == node => Some(token.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{node} paused on its wait: {events:?}"))
        };
        let (ringer, worker) = (token_of("ringer"), token_of("worker"));
        journal.seed(&[RunEvent::wait_answered(run_id, worker, weft_core::primitive::WaitAnswer::Given { value: json!("work") }, 1)]);

        let answers = Arc::new(Answers::default());
        let carried_on = drive(answers.clone());
        answers.read_past(0).await;
        answers.answer(ringer, json!("ring"));
        let outcome = carried_on.await.expect("joins").expect("the second drive runs");
        assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
        let events = journal.events_of(run_id);
        let resumed = |node: &str| count(&events, |e| matches!(e, RunEvent::NodeResumed { node_id, .. } if node_id == node));
        assert_eq!((resumed("worker"), resumed("ringer")), (1, 1), "each carried on once, the ringer in place: {events:?}");
    }
