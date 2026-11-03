    //! Layer 3: a firing that keeps its state (`ctx.keep`), through the
    //! real loop and across drives. Each drive is one worker: the run
    //! pauses (Stalled), its answer goes on its record, and the next
    //! drive folds the record and runs every paused firing again from
    //! the top, its state handed back. Waits around a `once`, a wait in
    //! a loop, a stream paused from either end, a consumer crashing
    //! between saves, a drain paused with its run, a bus conversation
    //! paused and picked up again, the rules a node reading a bus is held
    //! to and the patterns that resume right under them
    //! (ephemeral and journaled), a firing holding its run instead of
    //! keeping a state, and a run paused before saved states existed.

    use super::*;
    use super::engine_test_rig::{birth_kicking, catalog, clients_on_paused_clock, human_form, node_json, run_checked, test_manifest, AwaitTasks, MemJournal};
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex as StdMutex;
    use weft_core::bus::BusEntryKind;
    use weft_core::error::WeftResult;
    use weft_core::generator::Generator;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{NodeCatalog, ProjectDefinition, RunContext};
    use weft_journal::RunEvent;

    /// One run driven by one worker after another over the same record.
    struct Drives {
        project: Arc<ProjectDefinition>,
        catalog: Arc<dyn NodeCatalog>,
        run_id: RunId,
        journal: Arc<MemJournal>,
        tasks: Arc<AwaitTasks>,
    }

    impl Drives {
        /// A run of `project`, its `kicks` started, under `settings`, which
        /// pause at once when the run goes quiet (`pauseAfterSecs` 0): these
        /// tests are about what a pause carries, not the window before it.
        fn new(project: ProjectDefinition, catalog: Arc<dyn NodeCatalog>, kicks: &[&str], settings: weft_core::run_settings::RunSettings) -> Self {
            let settings = settings.pausing_after(0).expect("0 is a valid window");
            let run_id = weft_core::new_run_id();
            let journal = Arc::new(MemJournal::default());
            journal.seed(&birth_kicking(&project, run_id, kicks, settings));
            Self { project: Arc::new(project), catalog, run_id, journal, tasks: AwaitTasks::new() }
        }

        /// The same run on a record holding only `rows`, as a worker finds
        /// it after the one before died having written just those.
        fn over(&self, rows: Vec<RunEvent>) -> Self {
            let journal = Arc::new(MemJournal::default());
            journal.seed(&rows);
            Self { project: self.project.clone(), catalog: self.catalog.clone(), run_id: self.run_id, journal, tasks: AwaitTasks::new() }
        }

        /// One worker's drive of the run, to its end or its pause. On
        /// tokio's paused clock (`start_paused`), so what the run waits for
        /// in time (a bus's journal window flushing as it pauses) passes at
        /// once.
        async fn drive(&self) -> RunOutcome {
            let mut clients = clients_on_paused_clock(self.journal.clone());
            clients.tasks = self.tasks.clone();
            run_checked(self.project.clone(), self.catalog.clone(), self.run_id, self.journal.clone(), clients, CancellationFlag::new_arc(), None)
                .await
                .expect("the drive runs")
        }

        /// Answer the wait registered last, the way the install puts an
        /// answer on the run's record.
        fn answer(&self, value: Value) {
            let token = self.tasks.minted().expect("a wait registered");
            self.journal.seed(&[RunEvent::wait_answered(self.run_id, token, weft_core::primitive::WaitAnswer::Given { value }, 1)]);
        }

        fn events(&self) -> Vec<RunEvent> {
            self.journal.events_of(self.run_id)
        }

        /// Answer every wait on record not answered yet with `value`.
        fn answer_open(&self, value: Value) {
            let events = self.events();
            let answered: std::collections::HashSet<&String> = events
                .iter()
                .filter_map(|e| match e {
                    RunEvent::SuspensionResolved { token, .. } | RunEvent::SuspensionSkipped { token, .. } => Some(token),
                    _ => None,
                })
                .collect();
            let open: Vec<String> = events
                .iter()
                .filter_map(|e| match e {
                    RunEvent::SuspensionRegistered { token, .. } if !answered.contains(token) => Some(token.clone()),
                    _ => None,
                })
                .collect();
            for token in open {
                self.journal.seed(&[RunEvent::wait_answered(self.run_id, token, weft_core::primitive::WaitAnswer::Given { value: value.clone() }, 1)]);
            }
        }

        /// Drive the run to its end, answering every wait it pauses on;
        /// how it ended and how many times it paused.
        async fn drive_to_end(&self) -> (RunOutcome, usize) {
            let mut pauses = 0;
            loop {
                let outcome = self.drive().await;
                if !matches!(outcome, RunOutcome::Stalled) || pauses == 10 {
                    return (outcome, pauses);
                }
                pauses += 1;
                self.answer_open(json!("go on"));
            }
        }
    }

    fn count(events: &[RunEvent], matches: impl Fn(&RunEvent) -> bool) -> usize {
        events.iter().filter(|e| matches(e)).count()
    }

    fn emitted(events: &[RunEvent], node: &str, on: &str) -> usize {
        count(events, |e| matches!(e, RunEvent::PortEmitted { node_id, port, .. } if node_id == node && port == on))
    }

    fn saves(events: &[RunEvent], node: &str) -> usize {
        count(events, |e| matches!(e, RunEvent::NodeStateSaved { node_id, .. } if node_id == node))
    }

    fn assert_stalled(outcome: RunOutcome) {
        assert!(matches!(outcome, RunOutcome::Stalled), "the run pauses: {outcome:?}");
    }

    fn assert_completed(outcome: RunOutcome) {
        assert!(matches!(outcome, RunOutcome::Completed), "the run completes: {outcome:?}");
    }

    /// Keeps what reached its `value` input.
    struct Seen {
        values: Arc<StdMutex<Vec<Value>>>,
    }
    test_manifest!(Seen, "Seen");
    #[async_trait]
    impl Node for Seen {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let value: Value = ctx.inputs.get("value")?;
            self.values.lock().unwrap().push(value);
            Ok(())
        }
    }

    // ----- Waits and work done once -------------------------------------

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Interview {
        name: Option<Value>,
        ticket: Option<u32>,
        verdict: Option<Value>,
    }

    /// Waits for a name, opens a ticket once, waits for a verdict.
    struct Interviewer {
        tickets: Arc<AtomicU32>,
    }
    test_manifest!(Interviewer, "Interviewer");
    #[async_trait]
    impl Node for Interviewer {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Interview::default()).await?;
            let name = state.wait(|s| &mut s.name, human_form()).await?;
            let tickets = self.tickets.clone();
            let ticket = state.once(|s| &mut s.ticket, async move |_| Ok(tickets.fetch_add(1, Ordering::SeqCst) + 1)).await?;
            let verdict = state.wait(|s| &mut s.verdict, human_form()).await?;
            ctx.pulse_downstream(NodeOutput::new().set("out", json!({ "name": name, "ticket": ticket, "verdict": verdict }))).await
        }
    }

    fn one_node_to_seen(id: &str, ty: &str) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node_json(id, ty, &[], &[("out", "JsonDict")]), node_json("seen", "Seen", &[("value", "JsonDict")], &[])],
            "edges": [{ "id": "e", "source": id, "sourceHandle": "out", "target": "seen", "targetHandle": "value" }],
            "groups": []
        }))
        .expect("project")
    }

    /// Two waits with a `once` between: each wait pauses the run, and the
    /// work between them runs once across both resumes.
    #[tokio::test(start_paused = true)]
    async fn two_waits_with_work_between_run_the_work_once() {
        let tickets = Arc::new(AtomicU32::new(0));
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![
            ("Interviewer", Box::new(Interviewer { tickets: tickets.clone() })),
            ("Seen", Box::new(Seen { values: seen.clone() })),
        ]);
        let run = Drives::new(one_node_to_seen("interviewer", "Interviewer"), catalog, &["interviewer"], Default::default());

        assert_stalled(run.drive().await);
        run.answer(json!("Ada"));
        assert_stalled(run.drive().await);
        assert_eq!(tickets.load(Ordering::SeqCst), 1, "the ticket opened between the waits");
        run.answer(json!("hired"));
        assert_completed(run.drive().await);

        assert_eq!(tickets.load(Ordering::SeqCst), 1, "the ticket opened once over three runs of the body");
        assert_eq!(*seen.lock().unwrap(), vec![json!({ "name": "Ada", "ticket": 1, "verdict": "hired" })]);
        let events = run.events();
        assert_eq!(count(&events, |e| matches!(e, RunEvent::NodeStarted { node_id, .. } if node_id == "interviewer")), 1, "{events:?}");
        assert_eq!(count(&events, |e| matches!(e, RunEvent::NodeResumed { node_id, .. } if node_id == "interviewer")), 2, "{events:?}");
    }

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Collected {
        answers: Vec<Value>,
        next: Option<Value>,
    }

    /// Asks three times in a loop, keeping each answer.
    struct AsksThrice;
    test_manifest!(AsksThrice, "AsksThrice");
    #[async_trait]
    impl Node for AsksThrice {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Collected::default()).await?;
            while state.answers.len() < 3 {
                let answer = state.wait(|s| &mut s.next, human_form()).await?;
                state.answers.push(answer);
                state.next = None;
                state.save().await?;
            }
            ctx.pulse_downstream(NodeOutput::new().set("out", json!({ "answers": state.answers }))).await
        }
    }

    /// A wait in a loop: every turn pauses the run on a wait of its own
    /// number, and the answers come back in order.
    #[tokio::test(start_paused = true)]
    async fn a_wait_in_a_loop_pauses_once_per_turn() {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![("AsksThrice", Box::new(AsksThrice)), ("Seen", Box::new(Seen { values: seen.clone() }))]);
        let run = Drives::new(one_node_to_seen("asker", "AsksThrice"), catalog, &["asker"], Default::default());

        let mut tokens = Vec::new();
        for answer in ["a", "b", "c"] {
            assert_stalled(run.drive().await);
            tokens.push(run.tasks.minted().expect("a wait"));
            run.answer(json!(answer));
        }
        assert_completed(run.drive().await);

        assert_eq!(*seen.lock().unwrap(), vec![json!({ "answers": ["a", "b", "c"] })]);
        let distinct: std::collections::HashSet<&String> = tokens.iter().collect();
        assert_eq!(distinct.len(), 3, "each turn waited under a number of its own");
        let registered: Vec<u32> = run
            .events()
            .iter()
            .filter_map(|e| match e {
                RunEvent::SuspensionRegistered { wait_index, .. } => Some(*wait_index),
                _ => None,
            })
            .collect();
        assert_eq!(registered, vec![0, 1, 2]);
    }

    // ----- Streams ------------------------------------------------------

    /// `producer.out -> consumer.in`, a stream of numbers.
    fn stream_project(producer: &str, consumer: &str) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                node_json("producer", producer, &[], &[("out", "Generator[Number]")]),
                node_json("consumer", consumer, &[("in", "Generator[Number]")], &[]),
            ],
            "edges": [{ "id": "e", "source": "producer", "sourceHandle": "out", "target": "consumer", "targetHandle": "in" }],
            "groups": []
        }))
        .expect("stream project")
    }

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Paging {
        next: u32,
        asked: Option<Value>,
    }

    /// Yields 0 to 3, each one waiting for its take; with `ask_at`, waits
    /// before yielding that item, on a person or (`on_a_timer`) a timer.
    struct Pager {
        ask_at: Option<u32>,
        on_a_timer: bool,
    }
    test_manifest!(Pager, "Pager");
    #[async_trait]
    impl Node for Pager {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Paging::default()).await?;
            while state.next < 4 {
                if Some(state.next) == self.ask_at {
                    if self.on_a_timer {
                        let timer = weft_core::signal::Timer { spec: weft_core::signal::TimerSpec::After { duration_ms: 60_000 } };
                        state.wait(|s| &mut s.asked, timer).await?;
                    } else {
                        state.wait(|s| &mut s.asked, human_form()).await?;
                    }
                }
                let item = state.next;
                ctx.yield_downstream(NodeOutput::new().set("out", json!(item)), &mut state).await?;
                state.next = item + 1;
            }
            Ok(())
        }
    }

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Reading {
        first: Option<i64>,
        took: Vec<i64>,
        asked: Option<Value>,
    }

    /// Takes every item; with `ask_after_first`, reads the first one
    /// through its state and waits on a person after it. What it took when
    /// the stream ended goes to `took`.
    struct Reader {
        ask_after_first: bool,
        took: Arc<StdMutex<Vec<i64>>>,
    }
    test_manifest!(Reader, "Reader");
    #[async_trait]
    impl Node for Reader {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let mut state = ctx.keep(Reading::default()).await?;
            if self.ask_after_first {
                let first = state.read(|s| &mut s.first, &mut stream).await?.expect("a first item");
                state.wait(|s| &mut s.asked, human_form()).await?;
                if state.took.is_empty() {
                    state.took.push(first);
                }
            }
            while let Some(item) = stream.next(&mut state).await? {
                state.took.push(item);
            }
            *self.took.lock().unwrap() = state.took.clone();
            Ok(())
        }
    }

    /// A consumer pauses on a wait after its first item while its producer
    /// waits for the second item's take: both are picked up again, the
    /// second item is the producer's yield on record (sent once), and the
    /// consumer takes it and the rest.
    #[tokio::test(start_paused = true)]
    async fn a_consumer_pausing_mid_stream_takes_the_rest_after_the_resume() {
        let took = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![
            ("Pager", Box::new(Pager { ask_at: None, on_a_timer: false })),
            ("Reader", Box::new(Reader { ask_after_first: true, took: took.clone() })),
        ]);
        let run = Drives::new(stream_project("Pager", "Reader"), catalog, &["producer"], Default::default());

        assert_stalled(run.drive().await);
        assert!(saves(&run.events(), "producer") >= 1, "the producer paused in its yield, saving where it was");
        run.answer(json!("go on"));
        assert_completed(run.drive().await);

        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(emitted(&run.events(), "producer", "out"), 4, "every item went out once");
    }

    /// A producer pauses on a wait of its own halfway: the consumer
    /// waiting for its next item pauses with it, and after the resume
    /// the producer goes on from its third item and the consumer from
    /// its third take.
    async fn producer_pausing_mid_stream(on_a_timer: bool) {
        let took = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![
            ("Pager", Box::new(Pager { ask_at: Some(2), on_a_timer })),
            ("Reader", Box::new(Reader { ask_after_first: false, took: took.clone() })),
        ]);
        let run = Drives::new(stream_project("Pager", "Reader"), catalog, &["producer"], Default::default());

        assert_stalled(run.drive().await);
        assert!(saves(&run.events(), "consumer") >= 1, "the consumer paused in its pull, saving what it took");
        run.answer(json!(if on_a_timer { json!({ "firedAt": "now" }) } else { json!("go on") }));
        assert_completed(run.drive().await);

        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3]);
        let events = run.events();
        assert_eq!(emitted(&events, "producer", "out"), 4, "every item went out once");
        let kind = if on_a_timer { "timer" } else { "form" };
        assert_eq!(
            count(&events, |e| matches!(e, RunEvent::SuspensionRegistered { node_id, spec, .. } if node_id == "producer" && spec.kind == kind)),
            1,
            "the producer waited once, on its {kind}: {events:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_producer_pausing_on_a_wait_mid_stream_carries_on_after_the_resume() {
        producer_pausing_mid_stream(false).await;
    }

    /// The same between pages on a timer: the pause in the producer's own
    /// timer wait, numbered and kinded as a timer, carries it on.
    #[tokio::test(start_paused = true)]
    async fn a_producer_pausing_on_a_timer_between_pages_carries_on_after_the_resume() {
        producer_pausing_mid_stream(true).await;
    }

    /// Takes the whole stream with one `drain`; what it got goes to
    /// `took`.
    struct Drainer {
        took: Arc<StdMutex<Vec<i64>>>,
    }
    test_manifest!(Drainer, "Drainer");
    #[async_trait]
    impl Node for Drainer {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let mut state = ctx.keep(()).await?;
            *self.took.lock().unwrap() = stream.drain(&mut state).await?;
            Ok(())
        }
    }

    /// A drain whose lock-step producer pauses on a person halfway pauses
    /// with the run (the two items it took saved with it), and after the
    /// resume hands the whole stream, each item once.
    #[tokio::test(start_paused = true)]
    async fn a_drain_pauses_with_its_run_and_hands_the_whole_stream_after_the_resume() {
        let took = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![
            ("Pager", Box::new(Pager { ask_at: Some(2), on_a_timer: false })),
            ("Drainer", Box::new(Drainer { took: took.clone() })),
        ]);
        let run = Drives::new(stream_project("Pager", "Drainer"), catalog, &["producer"], Default::default());

        assert_stalled(run.drive().await);
        let held = run
            .events()
            .iter()
            .rev()
            .find_map(|e| match e {
                RunEvent::NodeStateSaved { node_id, saved, .. } if node_id == "consumer" => Some(saved.positions.drained.clone()),
                _ => None,
            })
            .expect("the drain paused, saving what it held");
        assert_eq!(held.get("in"), Some(&vec![json!(0), json!(1)]));
        run.answer(json!("go on"));
        assert_completed(run.drive().await);

        assert_eq!(*took.lock().unwrap(), vec![0, 1, 2, 3]);
        assert_eq!(emitted(&run.events(), "producer", "out"), 4, "every item went out once");
    }

    /// Sends 0 to 3 without waiting for their takes.
    struct Spills;
    test_manifest!(Spills, "Spills");
    #[async_trait]
    impl Node for Spills {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            for item in 0..4 {
                ctx.pulse_downstream(NodeOutput::new().set("out", json!(item))).await?;
            }
            Ok(())
        }
    }

    /// Takes every item, saving after each; every take is logged.
    struct SavesPerItem {
        log: Arc<StdMutex<Vec<i64>>>,
    }
    test_manifest!(SavesPerItem, "SavesPerItem");
    #[async_trait]
    impl Node for SavesPerItem {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let mut state = ctx.keep(Vec::<i64>::new()).await?;
            while let Some(item) = stream.next(&mut state).await? {
                self.log.lock().unwrap().push(item);
                state.push(item);
                state.save().await?;
            }
            Ok(())
        }
    }

    // A consumer saving after each item whose worker dies between two
    // items is run again from its last save: it gets back what it kept,
    // and takes again only the items after that save. Stress-looped: the
    // stream's end reaches the consumer's feed from the driver's task,
    // racing its saves.
    weft_core::stress_test!(
        name: a_consumer_crashing_between_saves_takes_again_only_what_followed_its_last_save,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let log = Arc::new(StdMutex::new(Vec::new()));
            let catalog = catalog(vec![("Spills", Box::new(Spills)), ("SavesPerItem", Box::new(SavesPerItem { log: log.clone() }))]);
            let durable = weft_core::run_settings::RunSettings::new(weft_core::run_settings::Keeping::Durable, true).unwrap();
            let first = Drives::new(stream_project("Spills", "SavesPerItem"), catalog, &["producer"], durable);
            assert_completed(first.drive().await);
            assert_eq!(*log.lock().unwrap(), vec![0, 1, 2, 3]);

            // What the record held when the worker died just after the
            // consumer's second save: the consumer's later rows and the run's
            // ending never landed.
            let events = first.events();
            let cut = events
                .iter()
                .enumerate()
                .filter(|(_, e)| matches!(e, RunEvent::NodeStateSaved { node_id, .. } if node_id == "consumer"))
                .nth(1)
                .map(|(at, _)| at)
                .expect("a second save");
            let rows: Vec<RunEvent> = events
                .into_iter()
                .enumerate()
                .filter(|(at, e)| *at <= cut || !written_by_the_consumer_or_the_ending(e))
                .map(|(_, e)| e)
                .collect();
            log.lock().unwrap().clear();

            let after_the_crash = first.over(rows);
            assert_completed(after_the_crash.drive().await);
            assert_eq!(*log.lock().unwrap(), vec![2, 3], "only the items after its last save are taken again: {:?}", after_the_crash.events());
            let saved = after_the_crash
                .events()
                .into_iter()
                .filter_map(|e| match e {
                    RunEvent::NodeStateSaved { node_id, saved, .. } if node_id == "consumer" => Some(saved.state),
                    _ => None,
                })
                .last()
                .expect("the consumer saved");
            assert_eq!(saved, json!([0, 1, 2, 3]), "it carried on from what it kept");
        }
    );

    /// What the consumer of the crash test writes, and the run's ending:
    /// the rows a worker dying mid-consumer never wrote.
    fn written_by_the_consumer_or_the_ending(event: &RunEvent) -> bool {
        match event {
            RunEvent::NodeStateSaved { node_id, .. }
            | RunEvent::PulsesConsumed { node_id, .. }
            | RunEvent::NodeCompleted { node_id, .. }
            | RunEvent::NodeFailed { node_id, .. } => node_id == "consumer",
            RunEvent::RunCompleted { .. } => true,
            _ => false,
        }
    }

    // ----- A firing that holds its run ----------------------------------

    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Halfway {
        sent_first: bool,
        answer: Option<Value>,
    }

    /// Sends 0, waits on a person, then sends 1.
    struct AsksHalfway;
    test_manifest!(AsksHalfway, "AsksHalfway");
    #[async_trait]
    impl Node for AsksHalfway {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Halfway::default()).await?;
            if !state.sent_first {
                ctx.pulse_downstream(NodeOutput::new().set("out", json!(0))).await?;
                state.sent_first = true;
                state.save().await?;
            }
            state.wait(|s| &mut s.answer, human_form()).await?;
            ctx.pulse_downstream(NodeOutput::new().set("out", json!(1))).await
        }
    }

    /// Takes every item while it holds its run, saving nothing.
    struct HoldsWhileReading {
        took: Arc<StdMutex<Vec<i64>>>,
    }
    test_manifest!(HoldsWhileReading, "HoldsWhileReading");
    #[async_trait]
    impl Node for HoldsWhileReading {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let mut holding = ctx.hold();
            while let Some(item) = stream.next(&mut holding).await? {
                self.took.lock().unwrap().push(item);
            }
            Ok(())
        }
    }

    /// A consumer holding its run keeps it from pausing while it waits on
    /// its stream, though every node waits and its producer is parked on
    /// a person: the run stays on its worker, the answer resumes the
    /// producer in place, and the consumer takes the rest without ever
    /// stopping.
    #[tokio::test(start_paused = true)]
    async fn a_firing_holding_its_run_keeps_it_on_its_worker() {
        let took = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![
            ("AsksHalfway", Box::new(AsksHalfway)),
            ("HoldsWhileReading", Box::new(HoldsWhileReading { took: took.clone() })),
        ]);
        let run = Drives::new(stream_project("AsksHalfway", "HoldsWhileReading"), catalog, &["producer"], Default::default());
        let answers = Arc::new(super::engine_test_rig::Answers::default());
        let mut clients = clients_on_paused_clock(run.journal.clone());
        clients.tasks = run.tasks.clone();
        clients.told = answers.told.clone();
        clients.runs = answers.clone();
        let driving = tokio::spawn({
            let (project, catalog, journal, run_id) = (run.project.clone(), run.catalog.clone(), run.journal.clone(), run.run_id);
            async move { run_checked(project, catalog, run_id, journal, clients, CancellationFlag::new_arc(), None).await }
        });

        // The producer parked and the run, holding, read its answers.
        let token = run.tasks.await_token().await;
        for _ in 0..2000 {
            if answers.reads() > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!driving.is_finished(), "a holding firing keeps the run from pausing");
        assert_eq!(*took.lock().unwrap(), vec![0]);
        answers.answer(token, json!("go on"));

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), driving).await.expect("the run ends").expect("join").expect("drove");
        assert_completed(outcome);
        assert_eq!(*took.lock().unwrap(), vec![0, 1]);
        let events = run.events();
        assert_eq!(saves(&events, "consumer"), 0, "the holding firing saved nothing: {events:?}");
        assert_eq!(count(&events, |e| matches!(e, RunEvent::NodeResumed { node_id, .. } if node_id == "consumer")), 0, "it never stopped");
        assert_eq!(
            count(&events, |e| matches!(e, RunEvent::NodeSuspended { node_id, .. } | RunEvent::NodeResumed { node_id, .. } if node_id == "producer")),
            0,
            "the producer's answer went into its waiting call: it never paused"
        );
    }

    // ----- A bus conversation across a pause ---------------------------

    /// What the host keeps: whether it asked, the first answer (read
    /// through the state before its wait, acted on after), the wait, and
    /// what it heard.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Hosting {
        asked_first: bool,
        first: Option<weft_core::bus::BusEntry>,
        asked: Option<Value>,
        heard: Vec<Value>,
    }

    /// Opens a bus on `ch`, asks the guest a question, keeps the answer
    /// while it waits on a person, then asks a second one and closes.
    struct Host {
        opts: fn() -> weft_core::bus::BusOptions,
        heard: Arc<StdMutex<Vec<Value>>>,
        bus_ids: Arc<StdMutex<Vec<uuid::Uuid>>>,
    }
    test_manifest!(Host, "Host");
    #[async_trait]
    impl Node for Host {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Hosting::default()).await?;
            let (mut bus, marker) = ctx.create_bus("ch", (self.opts)())?;
            self.bus_ids.lock().unwrap().push(bus.id());
            bus.register("host").map_err(|e| weft_core::error::node_error(e.to_string()))?;
            ctx.pulse_downstream(NodeOutput::new().set("ch", marker)).await?;
            let mut answers = bus.cursor()?.with_filter(|entry| matches!(&entry.kind, BusEntryKind::Message { msg_kind, .. } if msg_kind == "answer"));
            bus.wait_for("guest", &mut state).await?;
            if !state.asked_first {
                bus.send("question", json!(1)).map_err(|e| weft_core::error::node_error(e.to_string()))?;
                state.asked_first = true;
            }
            let first = state.read(|s| &mut s.first, &mut answers).await?.expect("the guest answers");
            state.wait(|s| &mut s.asked, human_form()).await?;
            if state.heard.is_empty() {
                state.heard.push(said(&first).1);
            }
            bus.send("question", json!(2)).map_err(|e| weft_core::error::node_error(e.to_string()))?;
            let (_, answer) = answers.next_json("answer", &mut state).await?.expect("the guest answers again");
            state.heard.push(answer);
            *self.heard.lock().unwrap() = state.heard.clone();
            bus.close();
            Ok(())
        }
    }

    /// The kind and the JSON value of a message.
    fn said(entry: &weft_core::bus::BusEntry) -> (String, Value) {
        let (kind, value) = entry.as_json().unwrap_or_else(|| panic!("not a JSON message: {entry:?}"));
        (kind.to_string(), value.clone())
    }

    /// Joins the bus as `guest` and answers each question with ten times
    /// it until the bus closes.
    struct Guest {
        asked: Arc<StdMutex<Vec<Value>>>,
    }
    test_manifest!(Guest, "Guest");
    #[async_trait]
    impl Node for Guest {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(()).await?;
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("guest").map_err(|e| weft_core::error::node_error(e.to_string()))?;
            let mut questions = bus.cursor()?;
            while let Some((_, question)) = questions.next_json("question", &mut state).await? {
                self.asked.lock().unwrap().push(question.clone());
                let answer = json!(question.as_i64().expect("a number") * 10);
                bus.send("answer", answer).map_err(|e| weft_core::error::node_error(e.to_string()))?;
            }
            Ok(())
        }
    }

    struct Chat {
        heard: Arc<StdMutex<Vec<Value>>>,
        asked: Arc<StdMutex<Vec<Value>>>,
        bus_ids: Arc<StdMutex<Vec<uuid::Uuid>>>,
        run: Drives,
    }

    /// The host and its guest over a bus opened with `opts`, driven to
    /// the host's pause, answered, and driven to the end.
    async fn chat_across_a_pause(opts: fn() -> weft_core::bus::BusOptions) -> Chat {
        let heard = Arc::new(StdMutex::new(Vec::new()));
        let asked = Arc::new(StdMutex::new(Vec::new()));
        let bus_ids = Arc::new(StdMutex::new(Vec::new()));
        let catalog = catalog(vec![
            ("Host", Box::new(Host { opts, heard: heard.clone(), bus_ids: bus_ids.clone() })),
            ("Guest", Box::new(Guest { asked: asked.clone() })),
        ]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node_json("host", "Host", &[], &[("ch", "Bus")]), node_json("guest", "Guest", &[("ch", "Bus")], &[])],
            "edges": [{ "id": "e", "source": "host", "sourceHandle": "ch", "target": "guest", "targetHandle": "ch" }],
            "groups": []
        }))
        .expect("chat project");
        let run = Drives::new(project, catalog, &["host"], Default::default());

        assert_stalled(run.drive().await);
        assert_eq!(*asked.lock().unwrap(), vec![json!(1)], "the first exchange happened before the pause");
        run.answer(json!("carry on"));
        assert_completed(run.drive().await);
        Chat { heard, asked, bus_ids, run }
    }

    /// The conversation goes on where it stopped, on the same bus, with
    /// nobody leaving or joining again, and the host's answer read before
    /// its wait handled after.
    fn assert_the_chat_carried_on(chat: &Chat) {
        assert_eq!(*chat.heard.lock().unwrap(), vec![json!(10), json!(20)], "the host kept the answer it read before its wait");
        assert_eq!(*chat.asked.lock().unwrap(), vec![json!(1), json!(2)], "the guest read each question once");
        let ids = chat.bus_ids.lock().unwrap().clone();
        assert_eq!(ids.len(), 2, "the host ran twice");
        assert_eq!(ids[0], ids[1], "the resumed host got the bus it created");
        let events = chat.run.events();
        assert_eq!(count(&events, |e| matches!(e, RunEvent::BusCreated { .. })), 1, "{events:?}");
        assert_eq!(count(&events, |e| matches!(e, RunEvent::BusJoined { .. })), 2, "each member joined once: {events:?}");
        assert_eq!(count(&events, |e| matches!(e, RunEvent::BusLeft { .. })), 0, "nobody left over the pause: {events:?}");
        assert_eq!(emitted(&events, "host", "ch"), 1, "the marker went out once");
        let mut last = None;
        for event in &events {
            if let RunEvent::BusWindow { first_offset, last_offset, .. } = event {
                assert!(last.is_none_or(|last| *first_offset > last), "offsets go on after the pause: {events:?}");
                last = Some(*last_offset);
            }
        }
    }

    /// An ephemeral bus paused mid-conversation: reopened with its id at
    /// its next offset, and the conversation carries on.
    #[tokio::test(start_paused = true)]
    async fn an_ephemeral_bus_chat_carries_on_across_a_pause() {
        let chat = chat_across_a_pause(|| weft_core::bus::BusOptions {
            journal: weft_core::stream_journal::JournalPolicy::ephemeral(),
            ..Default::default()
        })
        .await;
        assert_the_chat_carried_on(&chat);
    }

    /// A journaled bus paused mid-conversation: reopened with its id, its
    /// offsets and its members, and the conversation carries on.
    #[tokio::test(start_paused = true)]
    async fn a_journaled_bus_chat_carries_on_across_a_pause() {
        let chat = chat_across_a_pause(Default::default).await;
        assert_the_chat_carried_on(&chat);
    }

    // ----- Reading a bus across a pause: the rules and their patterns ----

    /// One step of a [`Talker`]'s script.
    #[derive(Clone)]
    enum Say {
        /// Send a message of this kind and value on the bus of this port.
        On(&'static str, &'static str, Value),
        /// Wait on a person (pausing the run when every node waits).
        Ask,
        /// Read the reader's messages until one of this kind.
        Await(&'static str),
    }

    fn on(port: &'static str, kind: &'static str, value: Value) -> Say {
        Say::On(port, kind, value)
    }

    /// What a talker keeps: its step, its wait, and the reply it is
    /// reading.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Talking {
        step: usize,
        asked: Option<Value>,
        reply: Option<weft_core::bus::BusEntry>,
    }

    /// Opens a bus on each of its `ports`, waits for `reader` to join them
    /// all, plays its script, then waits for the reader's `bye` and closes.
    struct Talker {
        ports: &'static [&'static str],
        script: Vec<Say>,
    }
    test_manifest!(Talker, "Talker");
    #[async_trait]
    impl Node for Talker {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let fail = |e: &dyn std::fmt::Display| weft_core::error::node_error(e.to_string());
            let mut state = ctx.keep(Talking::default()).await?;
            let mut buses = Vec::new();
            let mut markers = NodeOutput::new();
            for port in self.ports {
                let (mut bus, marker) = ctx.create_bus(port, Default::default())?;
                bus.register("talker").map_err(|e| fail(&e))?;
                markers = markers.set(*port, marker);
                buses.push(bus);
            }
            ctx.pulse_downstream(markers).await?;
            let mut replies = buses[0].cursor()?.with_filter(|entry| matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "reader"));
            for bus in &buses {
                bus.wait_for("reader", &mut state).await?;
            }
            let script: Vec<Say> = self.script.iter().cloned().chain(std::iter::once(Say::Await("bye"))).collect();
            while let Some(say) = script.get(state.step) {
                match say {
                    Say::On(port, kind, value) => {
                        let at = self.ports.iter().position(|p| p == port).expect("a port of the talker");
                        buses[at].send(*kind, value.clone()).map_err(|e| fail(&e))?;
                    }
                    Say::Ask => {
                        state.wait(|s| &mut s.asked, human_form()).await?;
                        state.asked = None;
                    }
                    Say::Await(kind) => loop {
                        let entry = state.read(|s| &mut s.reply, &mut replies).await?.ok_or_else(|| fail(&"the bus closed before the reader answered"))?;
                        state.reply = None;
                        if said(&entry).0 == *kind {
                            break;
                        }
                    },
                }
                state.step += 1;
            }
            for bus in &buses {
                bus.close();
            }
            Ok(())
        }
    }

    /// The bus on input `port` joined as `reader`, and its one reader of
    /// the talker's messages.
    fn talker_reader(ctx: &RunContext, port: &str) -> WeftResult<(weft_core::bus::BusHandle, weft_core::bus::BusCursor)> {
        let mut bus = ctx.bus_from_input(port)?;
        bus.register("reader").map_err(|e| weft_core::error::node_error(e.to_string()))?;
        let reader = bus.cursor()?.with_filter(|entry| matches!(&entry.kind, BusEntryKind::Message { from, .. } if from == "talker"));
        Ok((bus, reader))
    }

    /// Says `bye` to the talker on `bus`, once.
    fn bye(bus: &weft_core::bus::BusHandle) -> WeftResult<()> {
        bus.send("bye", json!(null)).map_err(|e| weft_core::error::node_error(e.to_string()))
    }

    /// A talker with `ports` playing `script` to a node of type `reader`.
    fn talk(ports: &'static [&'static str], script: Vec<Say>, reader: (&'static str, Box<dyn Node>)) -> Drives {
        talk_under(ports, script, reader, Default::default())
    }

    /// [`talk`], the run under `settings`.
    fn talk_under(
        ports: &'static [&'static str],
        script: Vec<Say>,
        reader: (&'static str, Box<dyn Node>),
        settings: weft_core::run_settings::RunSettings,
    ) -> Drives {
        let (reader_type, reader_node) = reader;
        let catalog = catalog(vec![("Talker", Box::new(Talker { ports, script })), (reader_type, reader_node)]);
        let wires: Vec<(&str, &str)> = ports.iter().map(|port| (*port, "Bus")).collect();
        let edges: Vec<Value> = ports
            .iter()
            .map(|port| json!({ "id": format!("e-{port}"), "source": "talker", "sourceHandle": port, "target": "reader", "targetHandle": port }))
            .collect();
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [node_json("talker", "Talker", &[], &wires), node_json("reader", reader_type, &wires, &[])],
            "edges": edges,
            "groups": []
        }))
        .expect("talk project");
        Drives::new(project, catalog, &["talker"], settings)
    }

    fn assert_failed_with(outcome: &RunOutcome, text: &str) -> String {
        match outcome {
            RunOutcome::Failed { error, .. } if error.contains(text) => error.clone(),
            other => panic!("the run fails with '{text}': {other:?}"),
        }
    }

    /// How the reader failed: refused for a direct read before a place
    /// its run can pause, `did` that place, both lines of this file named.
    fn refused_naming_both_lines(run: &Drives, did: &str) {
        let error = run
            .events()
            .into_iter()
            .find_map(|e| match e {
                RunEvent::NodeFailed { node_id, error, .. } if node_id == "reader" => Some(error),
                _ => None,
            })
            .expect("the reader failed");
        assert!(error.contains("read directly (not through its state) at") && error.contains(did), "{error}");
        assert_eq!(error.matches("saved_state.rs:").count(), 3, "both lines named, the earlier one twice: {error}");
    }

    /// Opens two readers on its bus.
    struct TwoReaders;
    test_manifest!(TwoReaders, "TwoReaders");
    #[async_trait]
    impl Node for TwoReaders {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let _state = ctx.keep(()).await?;
            let (bus, _first) = talker_reader(&ctx, "ch")?;
            let _second = bus.cursor()?;
            Ok(())
        }
    }

    /// A node opening a second reader on a bus it already reads fails
    /// there.
    #[tokio::test(start_paused = true)]
    async fn a_second_reader_on_one_bus_fails_the_node() {
        let run = talk(&["ch"], Vec::new(), ("TwoReaders", Box::new(TwoReaders)));
        let (outcome, _) = run.drive_to_end().await;
        let error = assert_failed_with(&outcome, "opened a second reader on a bus at");
        assert_eq!(error.matches("saved_state.rs:").count(), 2, "both lines named: {error}");
    }

    /// What a loop reading directly keeps: what it heard.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Looped {
        heard: Vec<Value>,
    }

    /// Reads every message directly in one loop until `done`.
    struct Loops {
        heard: Arc<StdMutex<Vec<Value>>>,
    }
    test_manifest!(Loops, "Loops");
    #[async_trait]
    impl Node for Loops {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Looped::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            loop {
                let (kind, value) = said(&reader.next(&mut state).await?.expect("the talker says done"));
                if kind == "done" {
                    break;
                }
                state.heard.push(value);
            }
            *self.heard.lock().unwrap() = state.heard.clone();
            bye(&bus)
        }
    }

    /// A loop reading directly, paused in its read, comes back where its
    /// reader stopped.
    #[tokio::test(start_paused = true)]
    async fn a_loop_reading_directly_resumes_in_its_read() {
        let heard = Arc::new(StdMutex::new(Vec::new()));
        let script = vec![on("ch", "msg", json!(1)), Say::Ask, on("ch", "msg", json!(2)), on("ch", "done", json!(null))];
        let run = talk(&["ch"], script, ("Loops", Box::new(Loops { heard: heard.clone() })));
        let (outcome, pauses) = run.drive_to_end().await;
        assert_completed(outcome);
        assert_eq!(pauses, 1);
        assert_eq!(*heard.lock().unwrap(), vec![json!(1), json!(2)]);
    }

    /// What a greeter keeps.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Greeted {
        greeting: Option<weft_core::bus::BusEntry>,
        heard: Vec<Value>,
    }

    /// Reads a greeting, then every message until `done`, then says bye.
    /// `through_state`: the greeting is read through the state; otherwise
    /// directly, at a second line of direct reads.
    struct Greeter {
        through_state: bool,
        heard: Arc<StdMutex<Vec<Value>>>,
    }
    test_manifest!(Greeter, "Greeter");
    #[async_trait]
    impl Node for Greeter {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Greeted::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            let greeting = if self.through_state {
                state.read(|s| &mut s.greeting, &mut reader).await?
            } else {
                reader.next(&mut state).await?
            };
            let greeting = said(&greeting.expect("a greeting")).1;
            loop {
                let (kind, value) = said(&reader.next(&mut state).await?.expect("the talker says done"));
                if kind == "done" {
                    break;
                }
                state.heard.push(value);
            }
            *self.heard.lock().unwrap() = std::iter::once(greeting).chain(state.heard.iter().cloned()).collect();
            bye(&bus)
        }
    }

    fn greeting_script() -> Vec<Say> {
        vec![on("ch", "greeting", json!("hi")), on("ch", "msg", json!(1)), Say::Ask, on("ch", "msg", json!(2)), on("ch", "done", json!(null))]
    }

    /// A greeting read directly before a loop reading directly is two
    /// lines of direct reads in one part of the body: the node fails at
    /// the loop's first read on its first run, nothing paused, naming
    /// both lines.
    #[tokio::test(start_paused = true)]
    async fn a_greeting_read_directly_before_a_loop_fails_on_its_first_run() {
        let heard = Arc::new(StdMutex::new(Vec::new()));
        let run = talk(&["ch"], greeting_script(), ("Greeter", Box::new(Greeter { through_state: false, heard })));
        run.drive().await;
        refused_naming_both_lines(&run, "before it read a bus at");
        assert_eq!(saves(&run.events(), "reader"), 0, "it never paused");
    }

    /// The same greeting read through the state: paused inside the loop,
    /// the node comes back with its greeting kept and its reader where it
    /// stopped.
    #[tokio::test(start_paused = true)]
    async fn a_greeting_read_through_the_state_resumes_inside_the_loop() {
        let heard = Arc::new(StdMutex::new(Vec::new()));
        let run = talk(&["ch"], greeting_script(), ("Greeter", Box::new(Greeter { through_state: true, heard: heard.clone() })));
        let (outcome, pauses) = run.drive_to_end().await;
        assert_completed(outcome);
        assert_eq!(pauses, 1);
        assert_eq!(*heard.lock().unwrap(), vec![json!("hi"), json!(1), json!(2)]);
    }

    /// What a node reading two buses keeps.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Paired {
        a: Option<weft_core::bus::BusEntry>,
        pairs: Vec<(Value, Value)>,
    }

    /// Reads one message of bus `a` and one of bus `b` per turn until `a`
    /// says done. `through_state`: `a` is read through the state (the
    /// pattern); otherwise directly, at a second line of direct reads.
    struct Pairs {
        through_state: bool,
        pairs: Arc<StdMutex<Vec<(Value, Value)>>>,
    }
    test_manifest!(Pairs, "Pairs");
    #[async_trait]
    impl Node for Pairs {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Paired::default()).await?;
            let (bus, mut ra) = talker_reader(&ctx, "a")?;
            let (_bus_b, mut rb) = talker_reader(&ctx, "b")?;
            loop {
                let a = if self.through_state { state.read(|s| &mut s.a, &mut ra).await? } else { ra.next(&mut state).await? };
                let (kind, a) = said(&a.expect("bus a says done"));
                if kind == "done" {
                    break;
                }
                let (_, b) = said(&rb.next(&mut state).await?.expect("bus b answers"));
                state.pairs.push((a, b));
                state.a = None;
            }
            *self.pairs.lock().unwrap() = state.pairs.clone();
            bye(&bus)
        }
    }

    /// Two buses read directly in one loop fail on the first run.
    #[tokio::test(start_paused = true)]
    async fn two_buses_read_directly_in_one_loop_fail_on_the_first_run() {
        let pairs = Arc::new(StdMutex::new(Vec::new()));
        let script = vec![on("a", "msg", json!(1)), on("b", "msg", json!(1)), on("a", "done", json!(null))];
        let run = talk(&["a", "b"], script, ("Pairs", Box::new(Pairs { through_state: false, pairs })));
        run.drive().await;
        refused_naming_both_lines(&run, "before it read a bus at");
    }

    /// Reads its two buses directly in turn, at one line of a loop.
    struct ReadsEachBus;
    test_manifest!(ReadsEachBus, "ReadsEachBus");
    #[async_trait]
    impl Node for ReadsEachBus {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(()).await?;
            let (bus, ra) = talker_reader(&ctx, "a")?;
            let (_bus_b, rb) = talker_reader(&ctx, "b")?;
            let mut readers = [ra, rb];
            loop {
                for reader in &mut readers {
                    reader.next(&mut state).await?;
                }
                bye(&bus)?;
            }
        }
    }

    /// Several buses read at one line of a loop are a place each: the
    /// node fails at the second bus's read on its first run.
    #[tokio::test(start_paused = true)]
    async fn buses_read_in_turn_at_one_line_fail_at_the_second_on_the_first_run() {
        let script = vec![on("a", "msg", json!(1)), on("b", "msg", json!(1))];
        let run = talk(&["a", "b"], script, ("ReadsEachBus", Box::new(ReadsEachBus)));
        run.drive().await;
        let error = run
            .events()
            .into_iter()
            .find_map(|e| match e {
                RunEvent::NodeFailed { node_id, error, .. } if node_id == "reader" => Some(error),
                _ => None,
            })
            .expect("the reader failed");
        assert!(error.contains("read directly (not through its state) at") && error.contains("before it read a bus at"), "{error}");
        assert_eq!(saves(&run.events(), "reader"), 0, "it never paused");
    }

    /// Two buses, one read through the state: paused in either read, the
    /// node comes back with the pair it was making.
    #[tokio::test(start_paused = true)]
    async fn two_buses_one_read_through_the_state_resume_paused_in_either_read() {
        let in_the_direct_read = vec![on("a", "msg", json!(1)), on("b", "msg", json!(1)), on("a", "msg", json!(2)), Say::Ask, on("b", "msg", json!(2))];
        let in_the_state_read = vec![on("a", "msg", json!(1)), on("b", "msg", json!(1)), Say::Ask, on("a", "msg", json!(2)), on("b", "msg", json!(2))];
        for mut script in [in_the_direct_read, in_the_state_read] {
            script.push(on("a", "done", json!(null)));
            let pairs = Arc::new(StdMutex::new(Vec::new()));
            let run = talk(&["a", "b"], script, ("Pairs", Box::new(Pairs { through_state: true, pairs: pairs.clone() })));
            let (outcome, pauses) = run.drive_to_end().await;
            assert_completed(outcome);
            assert_eq!(pauses, 1);
            assert_eq!(*pairs.lock().unwrap(), vec![(json!(1), json!(1)), (json!(2), json!(2))]);
        }
    }

    /// What a node of two loops keeps.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct TwoPhases {
        greeting: Option<weft_core::bus::BusEntry>,
        first: Vec<Value>,
        ended: Option<Value>,
        approval: Option<Value>,
        acked: bool,
        second: Vec<Value>,
    }

    /// The two-loops pattern: a greeting through the state, a first loop
    /// inside a `once` returning the message that ended it (adding up
    /// what it reads in the state), a person's approval, then a second
    /// loop reading directly at top level.
    struct TwoLoops {
        heard: Arc<StdMutex<Value>>,
    }
    test_manifest!(TwoLoops, "TwoLoops");
    #[async_trait]
    impl Node for TwoLoops {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(TwoPhases::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            let greeting = said(&state.read(|s| &mut s.greeting, &mut reader).await?.expect("a greeting")).1;
            let ended = state
                .once(|s| &mut s.ended, async |st| loop {
                    let (kind, value) = said(&reader.next(st).await?.expect("the first phase ends"));
                    if kind == "done" {
                        return Ok(value);
                    }
                    st.first.push(value);
                })
                .await?;
            state.wait(|s| &mut s.approval, human_form()).await?;
            if !state.acked {
                bus.send("ack", json!(null)).map_err(|e| weft_core::error::node_error(e.to_string()))?;
                state.acked = true;
            }
            loop {
                let (kind, value) = said(&reader.next(&mut state).await?.expect("the second phase ends"));
                if kind == "done" {
                    break;
                }
                state.second.push(value);
            }
            *self.heard.lock().unwrap() = json!({ "greeting": greeting, "first": state.first, "ended": ended, "second": state.second });
            bye(&bus)
        }
    }

    /// The two-loops pattern comes back right paused in each of its four
    /// steps: the greeting's read, the first loop, the approval, and the
    /// second loop.
    #[tokio::test(start_paused = true)]
    async fn two_loops_with_a_wait_between_resume_paused_in_each_step() {
        let script = |ask_at: Option<usize>, await_ack: bool| {
            let mut script = vec![
                on("ch", "greeting", json!("hi")),
                on("ch", "msg", json!(1)),
                on("ch", "msg", json!(2)),
                on("ch", "done", json!("end1")),
                on("ch", "msg", json!(3)),
                on("ch", "msg", json!(4)),
                on("ch", "done", json!("end2")),
            ];
            if await_ack {
                script.insert(4, Say::Await("ack"));
            }
            if let Some(at) = ask_at {
                script.insert(at, Say::Ask);
            }
            script
        };
        let steps = [
            ("the greeting's read", script(Some(0), false), 2),
            ("the first loop", script(Some(2), false), 2),
            ("the approval", script(None, false), 1),
            ("the second loop", script(Some(6), true), 2),
        ];
        for (step, script, paused) in steps {
            let heard = Arc::new(StdMutex::new(Value::Null));
            let run = talk(&["ch"], script, ("TwoLoops", Box::new(TwoLoops { heard: heard.clone() })));
            let (outcome, pauses) = run.drive_to_end().await;
            assert!(matches!(outcome, RunOutcome::Completed), "paused in {step}: {outcome:?}");
            assert_eq!(pauses, paused, "paused in {step}");
            assert_eq!(
                *heard.lock().unwrap(),
                json!({ "greeting": "hi", "first": [1, 2], "ended": "end1", "second": [3, 4] }),
                "paused in {step}"
            );
        }
    }

    /// What a moderator keeps.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Moderating {
        handling: Option<weft_core::bus::BusEntry>,
        approval: Option<Value>,
        heard: Vec<Value>,
    }

    /// Opens its reader once, before its loop: reads each message through
    /// the state and asks a person about the one valued 2 before passing
    /// it on.
    struct Moderator {
        heard: Arc<StdMutex<Vec<Value>>>,
    }
    test_manifest!(Moderator, "Moderator");
    #[async_trait]
    impl Node for Moderator {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Moderating::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            loop {
                let (kind, value) = said(&state.read(|s| &mut s.handling, &mut reader).await?.expect("the talker says done"));
                if kind == "done" {
                    break;
                }
                if value == json!(2) {
                    state.wait(|s| &mut s.approval, human_form()).await?;
                }
                state.heard.push(value);
                state.handling = None;
                state.approval = None;
            }
            *self.heard.lock().unwrap() = state.heard.clone();
            bye(&bus)
        }
    }

    /// A reader opened once before a loop comes back at the right message
    /// after a pause in the loop's read and after a pause in a wait inside
    /// the loop.
    #[tokio::test(start_paused = true)]
    async fn a_reader_opened_before_a_loop_resumes_at_the_right_message() {
        let heard = Arc::new(StdMutex::new(Vec::new()));
        let script = vec![on("ch", "msg", json!(1)), Say::Ask, on("ch", "msg", json!(2)), on("ch", "msg", json!(3)), on("ch", "done", json!(null))];
        let run = talk(&["ch"], script, ("Moderator", Box::new(Moderator { heard: heard.clone() })));
        let (outcome, pauses) = run.drive_to_end().await;
        assert_completed(outcome);
        assert_eq!(pauses, 2, "once in its read, once in its wait");
        assert_eq!(*heard.lock().unwrap(), vec![json!(1), json!(2), json!(3)]);
    }

    /// What a node reading through its state and then waiting keeps.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct ReadThenAsk {
        message: Option<weft_core::bus::BusEntry>,
        heard: Vec<Value>,
        approval: Option<Value>,
    }

    /// Reads every message through its state until `done`, then waits on a
    /// person.
    struct ReadsThenAsks;
    test_manifest!(ReadsThenAsks, "ReadsThenAsks");
    #[async_trait]
    impl Node for ReadsThenAsks {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(ReadThenAsk::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            loop {
                let (kind, value) = said(&state.read(|s| &mut s.message, &mut reader).await?.expect("the talker says done"));
                if kind == "done" {
                    break;
                }
                state.heard.push(value);
                state.message = None;
            }
            state.wait(|s| &mut s.approval, human_form()).await?;
            bye(&bus)
        }
    }

    /// `state.read` writes nothing by itself: three messages read through
    /// the state leave no save on record, and the state goes on record
    /// once, with all three, when the run pauses.
    #[tokio::test(start_paused = true)]
    async fn a_read_through_the_state_writes_nothing_by_itself() {
        let script = vec![on("ch", "msg", json!(1)), on("ch", "msg", json!(2)), on("ch", "msg", json!(3)), on("ch", "done", json!(null))];
        let run = talk(&["ch"], script, ("ReadsThenAsks", Box::new(ReadsThenAsks)));
        assert_stalled(run.drive().await);
        let saved: Vec<Value> = run
            .events()
            .into_iter()
            .filter_map(|e| match e {
                RunEvent::NodeStateSaved { node_id, saved, .. } if node_id == "reader" => Some(saved.state),
                _ => None,
            })
            .collect();
        assert_eq!(saved.len(), 1, "written once, at the pause: {saved:?}");
        assert_eq!(saved[0]["heard"], json!([1, 2, 3]));
        let (outcome, _) = run.drive_to_end().await;
        assert_completed(outcome);
    }

    /// What a node reading once and then waiting keeps.
    #[derive(Default, serde::Serialize, serde::Deserialize)]
    struct Asked {
        approval: Option<Value>,
    }

    /// Reads a message directly, then waits on a person about it without
    /// keeping it.
    struct ReadsThenWaits;
    test_manifest!(ReadsThenWaits, "ReadsThenWaits");
    #[async_trait]
    impl Node for ReadsThenWaits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Asked::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            reader.next(&mut state).await?;
            state.wait(|s| &mut s.approval, human_form()).await?;
            bye(&bus)
        }
    }

    /// A direct read before a wait fails at the wait on the first run,
    /// nothing paused: run again after a pause, the body would read again
    /// and lose the message.
    #[tokio::test(start_paused = true)]
    async fn a_direct_read_before_a_wait_fails_at_the_wait_on_the_first_run() {
        let run = talk(&["ch"], vec![on("ch", "msg", json!(1))], ("ReadsThenWaits", Box::new(ReadsThenWaits)));
        run.drive().await;
        refused_naming_both_lines(&run, "before it waited at");
        assert_eq!(saves(&run.events(), "reader"), 0, "it never paused");
    }

    /// Reads a message holding its run, then waits on a person; `drops`:
    /// the hold ends before the wait, else the wait is inside it.
    struct HoldsThenWaits {
        drops: bool,
    }
    test_manifest!(HoldsThenWaits, "HoldsThenWaits");
    #[async_trait]
    impl Node for HoldsThenWaits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Asked::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            if self.drops {
                {
                    let mut holding = ctx.hold();
                    reader.next(&mut holding).await?;
                }
                state.wait(|s| &mut s.approval, human_form()).await?;
            } else {
                let mut holding = ctx.hold();
                reader.next(&mut holding).await?;
                state.wait(|s| &mut s.approval, human_form()).await?;
            }
            bye(&bus)
        }
    }

    /// A read and a wait inside one hold: the wait does not pause, the
    /// firing holding its run. Nothing answers it, so it holds for the
    /// trigger's `holdSecs` and then gives up, loudly.
    #[tokio::test(start_paused = true)]
    async fn a_wait_inside_a_hold_holds_and_gives_up_instead_of_pausing() {
        let hold = 10;
        let settings = weft_core::run_settings::RunSettings::default().holding_for(hold).expect("a hold");
        let run = talk_under(&["ch"], vec![on("ch", "msg", json!(1))], ("HoldsThenWaits", Box::new(HoldsThenWaits { drops: false })), settings);
        let started = tokio::time::Instant::now();
        run.drive().await;
        let events = run.events();
        assert_eq!(count(&events, |e| matches!(e, RunEvent::NodeSuspended { node_id, .. } if node_id == "reader")), 0, "it never paused: {events:?}");
        let error = events
            .iter()
            .find_map(|e| match e {
                RunEvent::NodeFailed { node_id, error, .. } if node_id == "reader" => Some(error.clone()),
                _ => None,
            })
            .expect("the reader's wait gave up");
        assert!(error.contains("gave up its wait") && error.contains("holds its run"), "{error}");
        assert!(started.elapsed() >= std::time::Duration::from_secs(hold.into()), "it held for its holdSecs: {:?}", started.elapsed());
    }

    /// A read made holding the run, the hold then dropped: that read counts
    /// as a direct read, so the wait after fails on the first run, naming
    /// both lines.
    #[tokio::test(start_paused = true)]
    async fn a_held_read_counts_once_the_hold_is_dropped() {
        let run = talk(&["ch"], vec![on("ch", "msg", json!(1))], ("HoldsThenWaits", Box::new(HoldsThenWaits { drops: true })));
        run.drive().await;
        refused_naming_both_lines(&run, "before it waited at");
        assert_eq!(saves(&run.events(), "reader"), 0, "it never paused");
    }

    /// Waits on a person, and from its second run on reads a message first,
    /// on the strength of a count outside its state.
    struct Wanders {
        runs: Arc<AtomicU32>,
    }
    test_manifest!(Wanders, "Wanders");
    #[async_trait]
    impl Node for Wanders {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Asked::default()).await?;
            let (bus, mut reader) = talker_reader(&ctx, "ch")?;
            if self.runs.fetch_add(1, Ordering::SeqCst) > 0 {
                reader.next(&mut state).await?;
            }
            state.wait(|s| &mut s.approval, human_form()).await?;
            bye(&bus)
        }
    }

    /// A body whose path before its wait depends on something outside its
    /// state takes a direct read only on its resume: it fails there,
    /// where the paths split, naming both lines.
    #[tokio::test(start_paused = true)]
    async fn a_body_taking_another_path_on_its_resume_fails_where_the_paths_split() {
        let run = talk(&["ch"], vec![on("ch", "msg", json!(1))], ("Wanders", Box::new(Wanders { runs: Arc::new(AtomicU32::new(0)) })));
        assert_stalled(run.drive().await);
        run.answer(json!("go on"));
        run.drive().await;
        refused_naming_both_lines(&run, "before it waited at");
    }

    /// Joins the bus as `a` and leaves at once.
    struct Visits;
    test_manifest!(Visits, "Visits");
    #[async_trait]
    impl Node for Visits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("a").map_err(|e| weft_core::error::node_error(e.to_string()))?;
            Ok(())
        }
    }

    /// Waits for `a` to have joined, then waits on a person; each time it
    /// gets past the membership wait goes to `passed`.
    struct AwaitsTheVisitor {
        passed: Arc<AtomicU32>,
    }
    test_manifest!(AwaitsTheVisitor, "AwaitsTheVisitor");
    #[async_trait]
    impl Node for AwaitsTheVisitor {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            let mut state = ctx.keep(Asked::default()).await?;
            let (bus, _reader) = talker_reader(&ctx, "ch")?;
            bus.wait_for("a", &mut state).await?;
            self.passed.fetch_add(1, Ordering::SeqCst);
            state.wait(|s| &mut s.approval, human_form()).await?;
            bye(&bus)
        }
    }

    /// A member that joined and left still counts as joined over a pause:
    /// the resumed body gets past its membership wait at once, and carries
    /// on.
    #[tokio::test(start_paused = true)]
    async fn a_membership_wait_is_passed_at_once_after_a_pause() {
        let passed = Arc::new(AtomicU32::new(0));
        let catalog = catalog(vec![
            ("Talker", Box::new(Talker { ports: &["ch"], script: Vec::new() })),
            ("Visits", Box::new(Visits)),
            ("AwaitsTheVisitor", Box::new(AwaitsTheVisitor { passed: passed.clone() })),
        ]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                node_json("talker", "Talker", &[], &[("ch", "Bus")]),
                node_json("visitor", "Visits", &[("ch", "Bus")], &[]),
                node_json("reader", "AwaitsTheVisitor", &[("ch", "Bus")], &[]),
            ],
            "edges": [
                { "id": "e1", "source": "talker", "sourceHandle": "ch", "target": "visitor", "targetHandle": "ch" },
                { "id": "e2", "source": "talker", "sourceHandle": "ch", "target": "reader", "targetHandle": "ch" }
            ],
            "groups": []
        }))
        .expect("visit project");
        let run = Drives::new(project, catalog, &["talker"], Default::default());
        let (outcome, pauses) = run.drive_to_end().await;
        assert_completed(outcome);
        assert_eq!(pauses, 1);
        assert_eq!(passed.load(Ordering::SeqCst), 2, "past the membership wait on its first run and at once on its resume");
        let events = run.events();
        assert_eq!(count(&events, |e| matches!(e, RunEvent::BusLeft { name, .. } if name == "a")), 1, "the visitor left before the pause: {events:?}");
    }

    // ----- A run paused before saved states ----------------------------

    /// Waits once, the way a body of any version would.
    struct Waits;
    test_manifest!(Waits, "Waits");
    #[async_trait]
    impl Node for Waits {
        async fn run(&self, ctx: RunContext) -> WeftResult<()> {
            crate::run_driver::engine_test_rig::wait_once(&ctx, human_form()).await?;
            Ok(())
        }
    }

    /// A node parked on a wait by a weft before saved states left no
    /// state: answered, it fails loudly saying how to start the run
    /// again, instead of running from the top as if nothing had
    /// happened.
    #[tokio::test(start_paused = true)]
    async fn a_node_parked_before_saved_states_fails_when_answered() {
        let catalog = catalog(vec![("Waits", Box::new(Waits))]);
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "nodes": [node_json("waiter", "Waits", &[], &[])], "edges": [], "groups": []
        }))
        .expect("project");
        let run = Drives::new(project, catalog, &["waiter"], Default::default());
        let spec = weft_core::signal::to_spec(human_form());
        let id = run.run_id;
        run.journal.seed(&[
            RunEvent::NodeStarted { run_id: id, node_id: "waiter".into(), frames: vec![], at_unix: 0 },
            RunEvent::SuspensionRegistered { run_id: id, node_id: "waiter".into(), frames: vec![], token: "old".into(), spec, wait_index: 0, at_unix: 0 },
            RunEvent::NodeSuspended { run_id: id, node_id: "waiter".into(), frames: vec![], token: Some("old".into()), at_unix: 0 },
            RunEvent::wait_answered(id, "old".into(), weft_core::primitive::WaitAnswer::Given { value: json!("late") }, 0),
        ]);

        let outcome = run.drive().await;
        assert!(
            matches!(&outcome, RunOutcome::Failed { error, .. } if error.contains("paused before this version of weft")),
            "{outcome:?}"
        );
    }
