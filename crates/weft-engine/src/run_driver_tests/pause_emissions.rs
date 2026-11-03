    //! Layer 3: a node that emits, pauses on a wait, and emits more. On
    //! resume its body runs again from the top, and a port the firing sent
    //! before the pause sends nothing the second time: the node on that
    //! port fires once, the node on the later port fires when the body
    //! gets there, and the record folds back the same. An answer that
    //! comes while the run goes on reaches the waiting call itself, and the
    //! body never runs again. A firing that is
    //! no longer running owns nothing done through its ctx, and one the
    //! engine already failed waits on nothing: a wait it reaches, or one
    //! it holds, ends at once with its failure.

    use super::*;
    use super::engine_test_rig::{catalog, clients, human_form as form, node_json as node, run_checked, test_manifest, Answers, AwaitTasks, MemJournal};
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex as StdMutex;
    use weft_core::error::WeftResult;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{RunContext, ProjectDefinition};
    use weft_journal::RunEvent;

    /// Emits `stats` at once, waits for the picture, then emits `image`
    /// with the answer. Counts how often its body starts.
    struct Imager {
        bodies: Arc<AtomicU32>,
    }
    test_manifest!(Imager, "Imager");
    #[async_trait]
    impl Node for Imager {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            self.bodies.fetch_add(1, Ordering::SeqCst);
            ctx.pulse_downstream(NodeOutput::new().set("stats", json!({"queued": true}))).await?;
            let image = crate::run_driver::engine_test_rig::wait_once(&ctx, form()).await?;
            ctx.pulse_downstream(NodeOutput::new().set("image", image)).await
        }
    }

    /// Keeps what reached its `value` input.
    struct Seen {
        values: Arc<StdMutex<Vec<serde_json::Value>>>,
    }
    test_manifest!(Seen, "Seen");
    #[async_trait]
    impl Node for Seen {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let value: serde_json::Value = ctx.inputs.get("value")?;
            self.values.lock().unwrap().push(value);
            Ok(())
        }
    }

    /// Runs until told to stop: keeps the drive going while another
    /// node pauses, so an answer resumes that node in place.
    struct Holder {
        stop: Arc<tokio::sync::Notify>,
    }
    test_manifest!(Holder, "Holder");
    #[async_trait]
    impl Node for Holder {
        async fn run(&self, _ctx: RunContext) -> WeftResult<()> {
            self.stop.notified().await;
            Ok(())
        }
    }

    /// `imager.stats -> on_stats.value`, `imager.image -> on_image.value`,
    /// and a `holder` beside them. `stats_path` reads a key off `stats`
    /// on its wire.
    fn project(stats_path: &[&str]) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                node("imager", "Imager", &[], &[("stats", "JsonDict"), ("image", "String")]),
                node("on_stats", "SeenStats", &[("value", "JsonDict")], &[]),
                node("on_image", "SeenImage", &[("value", "String")], &[]),
                node("holder", "Holder", &[], &[]),
            ],
            "edges": [
                { "id": "e1", "source": "imager", "sourceHandle": "stats", "target": "on_stats", "targetHandle": "value", "path": stats_path },
                { "id": "e2", "source": "imager", "sourceHandle": "image", "target": "on_image", "targetHandle": "value" }
            ],
            "groups": []
        }))
        .expect("pause project")
    }

    struct Rig {
        bodies: Arc<AtomicU32>,
        stats: Arc<StdMutex<Vec<serde_json::Value>>>,
        images: Arc<StdMutex<Vec<serde_json::Value>>>,
        stop: Arc<tokio::sync::Notify>,
        catalog: Arc<dyn weft_core::NodeCatalog>,
    }

    fn rig() -> Rig {
        let bodies = Arc::new(AtomicU32::new(0));
        let stats = Arc::new(StdMutex::new(Vec::new()));
        let images = Arc::new(StdMutex::new(Vec::new()));
        let stop = Arc::new(tokio::sync::Notify::new());
        let catalog = catalog(vec![
            ("Imager", Box::new(Imager { bodies: bodies.clone() })),
            ("Holder", Box::new(Holder { stop: stop.clone() })),
        ]);
        let catalog = sinks(catalog, stats.clone(), images.clone());
        Rig { bodies, stats, images, stop, catalog }
    }

    /// `base` plus one `Seen` per sink, told apart by node type.
    fn sinks(
        base: Arc<dyn weft_core::NodeCatalog>,
        stats: Arc<StdMutex<Vec<serde_json::Value>>>,
        images: Arc<StdMutex<Vec<serde_json::Value>>>,
    ) -> Arc<dyn weft_core::NodeCatalog> {
        struct Split {
            base: Arc<dyn weft_core::NodeCatalog>,
            stats: &'static dyn Node,
            images: &'static dyn Node,
        }
        impl weft_core::NodeCatalog for Split {
            fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
                match node_type {
                    "SeenStats" => Some(self.stats),
                    "SeenImage" => Some(self.images),
                    other => self.base.lookup(other),
                }
            }
            fn all(&self) -> Vec<&'static str> {
                let mut all = self.base.all();
                all.extend(["SeenStats", "SeenImage"]);
                all
            }
        }
        Arc::new(Split {
            base,
            stats: Box::leak(Box::new(Seen { values: stats })),
            images: Box::leak(Box::new(Seen { values: images })),
        })
    }

    /// The run's birth, its listed nodes kicked.
    fn birth(project: &ProjectDefinition, run_id: RunId, kicks: &[&str]) -> Vec<RunEvent> {
        super::engine_test_rig::birth_kicking(project, run_id, kicks, super::engine_test_rig::pausing_at_once())
    }

    fn rows_of<'a>(events: &'a [RunEvent], matches: impl Fn(&RunEvent) -> bool + 'a) -> usize {
        events.iter().filter(|e| matches(e)).count()
    }

    fn started(events: &[RunEvent], node: &str) -> usize {
        rows_of(events, |e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id == node))
    }

    fn emitted(events: &[RunEvent], node: &str, on: &str) -> usize {
        rows_of(events, |e| matches!(e, RunEvent::PortEmitted { node_id, port, .. } if node_id == node && port == on))
    }

    /// The worker that drove the pause is gone; the next one folds the
    /// record, the answer among it, and runs the body again: `stats` sends
    /// nothing again, `image` goes out, and each sink fired once.
    #[tokio::test]
    async fn a_port_sent_before_a_pause_is_not_sent_again_after_a_refold() {
        let rig = rig();
        let project = Arc::new(project(&[]));
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth(&project, run_id, &["imager"]));
        let tasks = AwaitTasks::new();
        let with_tasks = |journal: Arc<MemJournal>| {
            let mut c = clients(journal);
            c.tasks = tasks.clone();
            c
        };

        let paused = run_checked(project.clone(), rig.catalog.clone(), run_id, journal.clone(), with_tasks(journal.clone()), CancellationFlag::new_arc(), None)
            .await
            .expect("the first drive runs");
        assert!(matches!(paused, RunOutcome::Stalled), "{paused:?}");
        assert_eq!(*rig.stats.lock().unwrap(), vec![json!({"queued": true})], "the node on `stats` fired before the wait");
        assert!(rig.images.lock().unwrap().is_empty());

        let token = tasks.minted().expect("the wait registered");
        journal.seed(&[RunEvent::wait_answered(
            run_id,
            token,
            weft_core::primitive::WaitAnswer::Given { value: json!("cat.png") },
            1,
        )]);
        let resumed = run_checked(project.clone(), rig.catalog.clone(), run_id, journal.clone(), with_tasks(journal.clone()), CancellationFlag::new_arc(), None)
            .await
            .expect("the second drive runs");
        assert!(matches!(resumed, RunOutcome::Completed), "{resumed:?}");

        assert_eq!(rig.bodies.load(Ordering::SeqCst), 2, "the body ran, paused, and ran again");
        assert_eq!(*rig.stats.lock().unwrap(), vec![json!({"queued": true})], "`stats` reached its node once");
        assert_eq!(*rig.images.lock().unwrap(), vec![json!("cat.png")]);
        let events = journal.events_of(run_id);
        assert_eq!(emitted(&events, "imager", "stats"), 1, "the body run again sent nothing on `stats`: {events:?}");
        assert_eq!(emitted(&events, "imager", "image"), 1);
        assert_eq!(started(&events, "on_stats"), 1);
        assert_eq!(started(&events, "on_image"), 1);
    }

    weft_core::stress_test! {
        name: an_answer_while_the_run_goes_on_reaches_the_waiting_call,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let rig = rig();
            let project = Arc::new(project(&[]));
            let run_id = weft_core::new_run_id();
            let journal = Arc::new(MemJournal::default());
            journal.seed(&birth(&project, run_id, &["imager", "holder"]));
            let tasks = AwaitTasks::new();
            let answers = Arc::new(Answers::default());
            let mut c = clients(journal.clone());
            c.tasks = tasks.clone();
            c.told = answers.told.clone();
            c.runs = answers.clone();
            let drive = tokio::spawn({
                let (project, catalog, journal) = (project.clone(), rig.catalog.clone(), journal.clone());
                async move { run_checked(project, catalog, run_id, journal, c, CancellationFlag::new_arc(), None).await }
            });

            // The node holds its wait (registered) while `holder` keeps
            // the drive going, and the run read its answers once.
            let token = tasks.await_token().await;
            for _ in 0..2000 {
                if answers.reads() > 0 { break; }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            answers.answer(token, json!("cat.png"));
            for _ in 0..2000 {
                if !rig.images.lock().unwrap().is_empty() { break; }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            rig.stop.notify_one();

            let outcome = drive.await.expect("join").expect("the drive runs");
            assert!(matches!(outcome, RunOutcome::Completed), "{outcome:?}");
            assert_eq!(rig.bodies.load(Ordering::SeqCst), 1, "the answer went into the waiting call: the body never ran again");
            assert_eq!(*rig.stats.lock().unwrap(), vec![json!({"queued": true})], "`stats` reached its node once");
            assert_eq!(*rig.images.lock().unwrap(), vec![json!("cat.png")]);
            let events = journal.events_of(run_id);
            assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::NodeSuspended { .. } | RunEvent::NodeResumed { .. })), 0, "nothing paused: {events:?}");
            assert_eq!(emitted(&events, "imager", "stats"), 1);
            assert_eq!(started(&events, "on_stats"), 1);
            assert_eq!(started(&events, "on_image"), 1);
        }
    }

    /// Spawns a task with a clone of its ctx that emits once told, then
    /// pauses: the firing is no longer running when the task emits.
    struct LeavesATask {
        go: Arc<tokio::sync::Notify>,
        result: Arc<StdMutex<Option<Result<(), String>>>>,
        done: Arc<tokio::sync::Notify>,
    }
    test_manifest!(LeavesATask, "LeavesATask");
    #[async_trait]
    impl Node for LeavesATask {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let (late, go, result, done) = (ctx.clone(), self.go.clone(), self.result.clone(), self.done.clone());
            tokio::spawn(async move {
                go.notified().await;
                let sent = late.pulse_downstream(NodeOutput::new().set("image", json!("late"))).await;
                *result.lock().unwrap() = Some(sent.map_err(|e| e.to_string()));
                done.notify_one();
            });
            crate::run_driver::engine_test_rig::wait_once(&ctx, form()).await?;
            Ok(())
        }
    }

    /// A task the body left running emits after its firing paused: the
    /// emission is refused to the task, and nothing reaches downstream.
    #[tokio::test]
    async fn a_task_left_running_cannot_emit_after_its_firing_paused() {
        let go = Arc::new(tokio::sync::Notify::new());
        let done = Arc::new(tokio::sync::Notify::new());
        let result = Arc::new(StdMutex::new(None));
        let images = Arc::new(StdMutex::new(Vec::new()));
        let base = catalog(vec![(
            "LeavesATask",
            Box::new(LeavesATask { go: go.clone(), result: result.clone(), done: done.clone() }),
        )]);
        let cat = sinks(base, Arc::new(StdMutex::new(Vec::new())), images.clone());
        let mut project = project(&[]);
        project.nodes.iter_mut().find(|n| n.id == "imager").expect("imager").node_type = "LeavesATask".into();
        project.nodes.retain(|n| n.id != "holder");
        let project = Arc::new(project);
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&birth(&project, run_id, &["imager"]));
        let mut c = clients(journal.clone());
        c.tasks = AwaitTasks::new();
        let outcome = run_checked(project, cat, run_id, journal.clone(), c, CancellationFlag::new_arc(), None)
            .await
            .expect("the drive runs");
        assert!(matches!(outcome, RunOutcome::Stalled), "{outcome:?}");

        go.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(10), done.notified()).await.expect("the task emitted");
        let refused = result.lock().unwrap().clone().expect("the task's emission answered");
        let err = refused.expect_err("an emission after the firing paused is refused");
        assert!(err.contains("after its body returned or paused"), "{err}");
        assert!(images.lock().unwrap().is_empty());
        let events = journal.events_of(run_id);
        assert_eq!(emitted(&events, "imager", "image"), 0, "{events:?}");
        assert_eq!(started(&events, "on_image"), 0);
    }

    /// Sends `stats` and waits for it to be taken (refused: its wire reads
    /// a key off a value that has none, which fails the firing), carries on
    /// past the refusal, and waits on a person.
    struct FailsThenWaits;
    test_manifest!(FailsThenWaits, "FailsThenWaits");
    #[async_trait]
    impl Node for FailsThenWaits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Option::<serde_json::Value>::None).await?;
            let _refused = ctx.yield_downstream(NodeOutput::new().set("stats", json!({"queued": true})), &mut state).await;
            state.wait(|s| s, form()).await?;
            Ok(())
        }
    }

    /// Waits on a person while a task it spawned sends `stats` a moment
    /// later (refused the same way, failing the firing while its wait is
    /// held in the quiet window).
    struct WaitsThenFails;
    test_manifest!(WaitsThenFails, "WaitsThenFails");
    #[async_trait]
    impl Node for WaitsThenFails {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let late = ctx.clone();
            let task = tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                let _refused = late.pulse_downstream(NodeOutput::new().set("stats", json!({"queued": true}))).await;
            });
            let mut state = ctx.keep(Option::<serde_json::Value>::None).await?;
            let waited = state.wait(|s| s, form()).await;
            let _ = task.await;
            waited.map(|_| ())
        }
    }

    /// The firing of node type `imager` (its `stats` wire reading `speed`,
    /// which the value has not) driven under the default quiet window, on
    /// the paused clock: how it ended, the run's record, and how long it
    /// took.
    async fn failing_firing(imager: Box<dyn Node>) -> (RunOutcome, Vec<RunEvent>, std::time::Duration) {
        let cat = sinks(catalog(vec![("Imager", imager)]), Arc::new(StdMutex::new(Vec::new())), Arc::new(StdMutex::new(Vec::new())));
        let mut project = project(&["speed"]);
        project.nodes.retain(|n| n.id != "holder");
        let project = Arc::new(project);
        let run_id = weft_core::new_run_id();
        let journal = Arc::new(MemJournal::default());
        journal.seed(&super::engine_test_rig::birth_kicking(&project, run_id, &["imager"], Default::default()));
        let mut c = super::engine_test_rig::clients_on_paused_clock(journal.clone());
        c.tasks = AwaitTasks::new();
        let started_at = tokio::time::Instant::now();
        let outcome = run_checked(project, cat, run_id, journal.clone(), c, CancellationFlag::new_arc(), None)
            .await
            .expect("the drive runs");
        (outcome, journal.events_of(run_id), started_at.elapsed())
    }

    /// A firing the engine failed before its body reaches a wait waits on
    /// nothing: the wait ends at once with the failure, inside no window.
    #[tokio::test(start_paused = true)]
    async fn a_wait_reached_after_its_firing_failed_ends_at_once() {
        let (outcome, events, took) = failing_firing(Box::new(FailsThenWaits)).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(took < std::time::Duration::from_secs(1), "no window waited out, only the record's writes: {took:?}");
        assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::NodeFailed { node_id, .. } if node_id == "imager")), 1, "{events:?}");
        assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::NodeSuspended { node_id, .. } if node_id == "imager")), 0, "{events:?}");
        assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::SuspensionRegistered { .. })), 0, "no wait was registered: {events:?}");
    }

    /// A wait held open in the quiet window ends the moment its firing
    /// fails, with the failure: the run ends failed without waiting out
    /// the window.
    #[tokio::test(start_paused = true)]
    async fn a_wait_held_in_the_window_ends_when_its_firing_fails() {
        let (outcome, events, took) = failing_firing(Box::new(WaitsThenFails)).await;
        assert!(matches!(outcome, RunOutcome::Failed { .. }), "{outcome:?}");
        assert!(took < std::time::Duration::from_secs(1), "no window waited out, only the task's moment and the record's writes: {took:?}");
        assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::SuspensionRegistered { .. })), 1, "the wait was held: {events:?}");
        assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::NodeSuspended { node_id, .. } if node_id == "imager")), 0, "{events:?}");
        assert_eq!(rows_of(&events, |e| matches!(e, RunEvent::NodeFailed { node_id, .. } if node_id == "imager")), 1, "{events:?}");
    }
