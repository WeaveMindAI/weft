    //! Layer-3 tests for `Generator[T]` streams and delivery-waiting
    //! emissions, driven through the REAL loop (`run_one_execution`
    //! with inline nodes over the in-memory journal). These are the
    //! tests that pin the engine-level contracts:
    //!
    //! - items flow producer -> consumer in order, the consumer is
    //!   dispatched exactly ONCE, and the stream ends cleanly;
    //! - a delivered yield runs in lock-step (each item's send returns
    //!   only after its pull);
    //! - a delivered emission on a NORMAL port returns only after the
    //!   downstream consumer actually dispatched;
    //! - an empty stream (a close with no prior item on a Generator
    //!   port) RUNS its consumer, whose first pull reads a clean end;
    //!   a close after items never retroactively skips anything;
    //! - a failed producer poisons the stream (the pull errors, drains
    //!   never hand back a truncated list);
    //! - deliveries that can never happen fail LOUDLY (consumer
    //!   skipped, consumer finished early, proven deadlock, buffer
    //!   overrun) instead of hanging;
    //! - `over` on a Generator drives sequential and parallel loops,
    //!   with clean gathers, loud failures, and early done votes.
    //!
    //! Everything here coordinates two+ tasks over tokio sync
    //! primitives, which is exactly the profile the project's testing
    //! rules say to stress-loop BY CONSTRUCTION, so the timing-
    //! sensitive tests run through `stress_test!`.

    use super::*;
    use super::engine_test_rig::{drive, drive_with_cancel, test_manifest};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::error::WeftResult;
    use weft_core::generator::Generator;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::{ExecutionContext, ProjectDefinition};
    use weft_journal::ExecEvent;

    type Log = Arc<StdMutex<Vec<String>>>;

    fn log(l: &Log, entry: impl Into<String>) {
        l.lock().unwrap().push(entry.into());
    }

    fn log_index(entries: &[String], needle: &str) -> Option<usize> {
        entries.iter().position(|e| e == needle)
    }

    // ----- Inline nodes ------------------------------------------------

    /// Yield `count` numbers on the stream port `out`, plainly or with
    /// delivery-waiting, optionally failing after `fail_after` yields.
    /// Every outcome of a yield is logged so the tests can assert
    /// ordering against the consumer's log.
    struct Yielder {
        count: usize,
        delivered: bool,
        fail_after: Option<usize>,
        log: Log,
    }
    test_manifest!(Yielder, "Yielder");
    #[async_trait]
    impl Node for Yielder {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            for i in 0..self.count {
                if self.fail_after == Some(i) {
                    return Err(weft_core::error::node_error("yielder exploded"));
                }
                let out = NodeOutput::new().set("out", json!(i));
                let result = if self.delivered {
                    ctx.yield_downstream(out).await
                } else {
                    ctx.pulse_downstream(out).await
                };
                match result {
                    Ok(()) => log(&self.log, format!("sent {i}")),
                    Err(e) => {
                        log(&self.log, format!("send error {i}: {e}"));
                        return Err(e);
                    }
                }
            }
            Ok(())
        }
    }

    /// Pull the whole stream on input `in`, logging each take, then
    /// (optionally) stop after `take_only` items and return.
    struct Taker {
        take_only: Option<usize>,
        log: Log,
    }
    test_manifest!(Taker, "Taker");
    #[async_trait]
    impl Node for Taker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            log(&self.log, "taker dispatched");
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let mut taken = 0usize;
            loop {
                if self.take_only == Some(taken) {
                    log(&self.log, "taker quit early");
                    return Ok(());
                }
                // Logged BEFORE the await, on the consumer's own task:
                // the race-free observation point the lock-step test
                // orders producer sends against.
                log(&self.log, format!("pulling {taken}"));
                match stream.next().await {
                    Ok(Some(v)) => {
                        log(&self.log, format!("took {v}"));
                        taken += 1;
                    }
                    Ok(None) => {
                        log(&self.log, "stream finished");
                        return Ok(());
                    }
                    Err(e) => {
                        log(&self.log, format!("pull error: {e}"));
                        return Err(e);
                    }
                }
            }
        }
    }

    use super::engine_test_rig::catalog;

    /// producer.out (Generator[Number]) -> consumer.in (Generator[Number]).
    fn stream_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "producer", "nodeType": "Yielder", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "consumer", "nodeType": "Taker", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "in", "portType": "Generator[Number]", "required": true }],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e", "source": "producer", "target": "consumer", "sourceHandle": "out", "targetHandle": "in" }
            ],
            "groups": []
        }))
        .expect("stream project")
    }

    /// How many times a node was DISPATCHED fresh (NodeStarted rows).
    fn started_count(events: &[ExecEvent], node: &str) -> usize {
        events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeStarted { node_id, .. } if node_id == node))
            .count()
    }

    fn node_failed_error(events: &[ExecEvent], node: &str) -> Option<String> {
        events.iter().find_map(|e| match e {
            ExecEvent::NodeFailed { node_id, error, .. } if node_id == node => {
                Some(error.clone())
            }
            _ => None,
        })
    }

    fn node_skipped(events: &[ExecEvent], node: &str) -> bool {
        events
            .iter()
            .any(|e| matches!(e, ExecEvent::NodeSkipped { node_id, .. } if node_id == node))
    }

    // ----- Plain stream flow -------------------------------------------

    weft_core::stress_test!(
        name: stream_items_flow_in_order_and_dispatch_the_consumer_once,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: false, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(
                matches!(outcome, ExecutionOutcome::Completed { .. }),
                "stream run completes, got {outcome:?}"
            );
            let entries = log.lock().unwrap().clone();
            let takes: Vec<&str> = entries
                .iter()
                .filter(|e| e.starts_with("took "))
                .map(|e| e.as_str())
                .collect();
            assert_eq!(
                takes,
                ["took 0", "took 1", "took 2", "took 3", "took 4"],
                "every item arrives, in emission order"
            );
            assert!(
                entries.contains(&"stream finished".to_string()),
                "the producer's return closes the stream cleanly"
            );
            // THE dispatch contract: five items, ONE consumer dispatch.
            // (And no retroactive skip from the close after real items.)
            assert_eq!(
                started_count(&events, "consumer"),
                1,
                "later items feed the running consumer instead of re-dispatching it"
            );
            assert!(!node_skipped(&events, "consumer"), "a close after items never skips");
        }
    );

    weft_core::stress_test!(
        name: delivered_yields_run_in_lock_step_with_the_pulls,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 4, delivered: true, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, _) = drive(stream_project(), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            // Lock-step is pinned as "pulling i" BEFORE "sent i":
            // `yield i` may only return once pull i is underway, and
            // "pulling i" is written on the consumer's own task BEFORE
            // it awaits, so the ordering is race-free by causality.
            // ("took i" before "sent i" is NOT assertable: the feed
            // pops the item and releases the producer inside the pull,
            // before the consumer's post-await log line runs.) A
            // non-delivered producer fails immediately (every send
            // logged before the first pull), and a one-slot-buffer
            // regression fails deterministically (sent 0 lands before
            // the consumer is even dispatched).
            for i in 0..4 {
                let sent = log_index(&entries, &format!("sent {i}"))
                    .unwrap_or_else(|| panic!("'sent {i}' missing from {entries:?}"));
                let pulling = log_index(&entries, &format!("pulling {i}"))
                    .unwrap_or_else(|| panic!("'pulling {i}' missing from {entries:?}"));
                assert!(
                    pulling < sent,
                    "a delivered yield returns only after its pull: item {i} \
                     (log: {entries:?})"
                );
            }
        }
    );

    // ----- Empty-stream semantics --------------------------------------

    weft_core::stress_test!(
        name: an_empty_stream_runs_its_consumer_which_reads_a_clean_end,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            // A closure on a Generator port is the EMPTY STREAM, a
            // value the body consumes (the documented pull contract
            // answers Ok(None) right away), never a skip: "zero items"
            // must behave like "one item minus the item", so the
            // body's post-loop code (a summary over zero rows, say)
            // still runs.
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 0, delivered: false, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
            assert!(
                !node_skipped(&events, "consumer"),
                "an empty stream must RUN its consumer, not skip it"
            );
            let entries = log.lock().unwrap().clone();
            assert!(entries.iter().any(|e| e == "taker dispatched"), "{entries:?}");
            assert!(
                entries.iter().any(|e| e == "stream finished"),
                "the body's pull must read the empty stream's clean end: {entries:?}"
            );
            assert!(
                !entries.iter().any(|e| e.starts_with("took ")),
                "zero items were yielded: {entries:?}"
            );
        }
    );

    // ----- Failure semantics -------------------------------------------

    weft_core::stress_test!(
        name: a_failed_producer_poisons_the_stream_for_the_pulling_consumer,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: false, fail_after: Some(2), log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let consumer_err = node_failed_error(&events, "consumer")
                .expect("the consumer fails on the poisoned end");
            assert!(
                consumer_err.contains("yielder exploded"),
                "the producer's error reaches the consumer's pull: {consumer_err}"
            );
            // The two items yielded before the failure still arrived.
            let entries = log.lock().unwrap().clone();
            assert!(
                entries.contains(&"took 0".to_string())
                    && entries.contains(&"took 1".to_string()),
                "items yielded before the failure are delivered: {entries:?}"
            );
        }
    );

    weft_core::stress_test!(
        name: a_consumer_finishing_early_fails_a_delivered_yield_loudly,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: true, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: Some(1), log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let producer_err = node_failed_error(&events, "producer")
                .expect("the delivery-waiting producer fails");
            assert!(
                producer_err
                    .contains("the consumer 'consumer' finished without taking this stream item"),
                "the error names the abandoned delivery: {producer_err}"
            );
        }
    );

    weft_core::stress_test!(
        name: a_partial_plain_take_completes_and_drops_the_leftovers,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            // Fire-and-forget items a consumer never took are dropped
            // like any value a finished node never read: the run
            // COMPLETES, nothing hangs, and the drop is durable (no
            // pulse left in flight to wedge the completion check).
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: false, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: Some(1), log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(
                matches!(outcome, ExecutionOutcome::Completed { .. }),
                "a take-1-of-5 consumer is a legal pattern for plain yields, got {outcome:?}"
            );
            let entries = log.lock().unwrap().clone();
            assert_eq!(
                entries.iter().filter(|e| e.starts_with("took ")).count(),
                1,
                "exactly the one requested item was taken: {entries:?}"
            );
            assert!(node_failed_error(&events, "producer").is_none(), "nobody failed");
            assert!(node_failed_error(&events, "consumer").is_none(), "nobody failed");
        }
    );

    // ----- Delivery on a NORMAL port -----------------------------------

    /// A delivered emission on a plain port: `first.x -> joint.x`, and
    /// `joint` also requires `y` from `second`, so `first`'s delivered
    /// emission can only resolve once `second` emitted and `joint`
    /// dispatched.
    fn join_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "first", "nodeType": "First", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "x", "portType": "Number", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "second", "nodeType": "Second", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 1.0 },
                    "inputs": [],
                    "outputs": [{ "name": "y", "portType": "Number", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "joint", "nodeType": "Joint", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [
                        { "name": "x", "portType": "Number", "required": true },
                        { "name": "y", "portType": "Number", "required": true }
                    ],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "first", "target": "joint", "sourceHandle": "x", "targetHandle": "x" },
                { "id": "e2", "source": "second", "target": "joint", "sourceHandle": "y", "targetHandle": "y" }
            ],
            "groups": []
        }))
        .expect("join project")
    }

    /// Emits `x` with delivery-waiting, logging around the wait and
    /// releasing `gate` right before it (a permit-carrying signal, so
    /// the peer needs no clock and no polling).
    struct First {
        log: Log,
        first_started: Arc<tokio::sync::Notify>,
    }
    test_manifest!(First, "First");
    #[async_trait]
    impl Node for First {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            log(&self.log, "first before");
            // `notify_one` stores a permit, so the peer's later
            // `notified()` resolves whichever side gets there first.
            self.first_started.notify_one();
            ctx.yield_downstream(NodeOutput::new().set("x", json!(1))).await?;
            log(&self.log, "first after");
            Ok(())
        }
    }

    /// Waits until `first` has started (its signal fires immediately
    /// before its delivery wait, sparing a pointless spin-up race),
    /// then emits `y`. The ORDERING the tests assert is enforced by
    /// `joint` requiring both `x` and `y`, not by this signal.
    struct Second {
        log: Log,
        first_started: Arc<tokio::sync::Notify>,
    }
    test_manifest!(Second, "Second");
    #[async_trait]
    impl Node for Second {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.first_started.notified().await;
            log(&self.log, "second emitted");
            ctx.pulse_downstream(NodeOutput::new().set("y", json!(2))).await
        }
    }

    /// A stream consumer whose body calls `await_signal`: the engine
    /// must refuse it (a durable suspension cannot replay the items
    /// its pulls already consumed).
    struct AwaitingTaker;
    test_manifest!(AwaitingTaker, "AwaitingTaker");
    #[async_trait]
    impl Node for AwaitingTaker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.await_signal(weft_core::signal::timer::Timer {
                spec: weft_core::signal::timer::TimerSpec::After { duration_ms: 1 },
            })
            .await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_stream_consumers_await_signal_fails_the_firing() {
        let log: Log = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Yielder", Box::new(Yielder { count: 1, delivered: false, fail_after: None, log: log.clone() })),
            ("Taker", Box::new(AwaitingTaker)),
        ]);
        let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
        let err = node_failed_error(&events, "consumer").expect("the consumer fails");
        assert!(
            err.contains(&weft_core::context::stream_consumer_await_signal_error("consumer")),
            "{err}"
        );
    }

    struct Joint {
        log: Log,
        fail: bool,
    }
    test_manifest!(Joint, "Joint");
    #[async_trait]
    impl Node for Joint {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let _x: f64 = ctx.inputs.get("x")?;
            log(&self.log, "joint ran");
            if self.fail {
                return Err(weft_core::error::node_error("joint failed"));
            }
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: a_delivered_emission_on_a_normal_port_waits_for_the_consumers_dispatch,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let first_started = Arc::new(tokio::sync::Notify::new());
            let cat = catalog(vec![
                ("First", Box::new(First { log: log.clone(), first_started: first_started.clone() })),
                ("Second", Box::new(Second { log: log.clone(), first_started })),
                ("Joint", Box::new(Joint { log: log.clone(), fail: false })),
            ]);
            let (outcome, events) = drive(join_project(), cat, &["first", "second"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            let before = log_index(&entries, "first before").expect("first ran");
            let second = log_index(&entries, "second emitted").expect("second ran");
            let after = log_index(&entries, "first after").expect("first resumed");
            assert!(
                before < second && second < after,
                "the delivered emission resolves only once the OTHER input arrived and \
                 the consumer dispatched: {entries:?}"
            );
            // The delivery gate resolves on the consumer's dispatch
            // (its absorb): the consumer must actually have fired,
            // exactly once, or the ordering above proves nothing.
            assert_eq!(started_count(&events, "joint"), 1, "the consumer dispatched once");
        }
    );

    weft_core::stress_test!(
        name: a_delivered_emission_resolves_on_dispatch_even_when_the_consumer_then_fails,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            // The delivery gate's contract is "the value was TAKEN"
            // (the consumer dispatched), not "the consumer succeeded": a
            // failing consumer resumes the producer normally, and the
            // run's failure is the consumer's own.
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let first_started = Arc::new(tokio::sync::Notify::new());
            let cat = catalog(vec![
                ("First", Box::new(First { log: log.clone(), first_started: first_started.clone() })),
                ("Second", Box::new(Second { log: log.clone(), first_started })),
                ("Joint", Box::new(Joint { log: log.clone(), fail: true })),
            ]);
            let (outcome, events) = drive(join_project(), cat, &["first", "second"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            assert!(
                log_index(&entries, "first after").is_some(),
                "the producer resumed on the dispatch, not on the consumer's outcome: {entries:?}"
            );
            assert!(
                node_failed_error(&events, "first").is_none(),
                "the producer's own firing succeeded"
            );
            let joint_err = node_failed_error(&events, "joint").expect("joint failed");
            assert!(joint_err.contains("joint failed"), "{joint_err}");
        }
    );

    /// `second` CLOSES `y` instead of emitting: `joint` (y required)
    /// skips, and `first`'s delivered emission fails loudly instead of
    /// waiting forever for a dispatch that can never happen.
    struct SecondCloses;
    test_manifest!(SecondCloses, "SecondCloses");
    #[async_trait]
    impl Node for SecondCloses {
        async fn run(&self, _ctx: ExecutionContext) -> WeftResult<()> {
            // Emit nothing: the termination sweep closes `y`.
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: a_delivered_emission_to_a_skipping_consumer_fails_loudly,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("First", Box::new(First { log: log.clone(), first_started: Arc::new(tokio::sync::Notify::new()) })),
                ("Second", Box::new(SecondCloses)),
                ("Joint", Box::new(Joint { log: log.clone(), fail: false })),
            ]);
            let (outcome, events) = drive(join_project(), cat, &["first", "second"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let first_err =
                node_failed_error(&events, "first").expect("the waiting producer fails");
            assert!(
                first_err
                    .contains("the consumer 'joint' skipped (the required input 'y' closed)"),
                "the error names the skip that killed the delivery: {first_err}"
            );
            assert!(node_skipped(&events, "joint"), "joint skipped on the closed required y");
            assert!(
                !log.lock().unwrap().iter().any(|e| e == "joint ran"),
                "the skipped consumer never ran"
            );
        }
    );

    // ----- Deadlock resolution -----------------------------------------

    /// `ping.out -> pong.in` and `pong.out -> ping.in`, both streams.
    /// Each yields one item then pulls a SECOND item from the other,
    /// which never comes: every in-flight task is parked on a pull, no
    /// bus exists, and the engine must resolve the deadlock loudly
    /// (both pulls error) instead of hanging forever.
    fn cycle_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "ping", "nodeType": "PingPong", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [{ "name": "in", "portType": "Generator[Number]", "required": true }],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "pong", "nodeType": "PingPong", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "in", "portType": "Generator[Number]", "required": true }],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "ping", "target": "pong", "sourceHandle": "out", "targetHandle": "in" },
                { "id": "e2", "source": "pong", "target": "ping", "sourceHandle": "out", "targetHandle": "in" }
            ],
            "groups": []
        }))
        .expect("cycle project")
    }

    /// Yield one item, take one, then pull again (which can never
    /// resolve once both sides are here).
    struct PingPong {
        log: Log,
    }
    test_manifest!(PingPong, "PingPong");
    #[async_trait]
    impl Node for PingPong {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("out", json!(1))).await?;
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let first = stream.next().await?;
            log(&self.log, format!("got {first:?}"));
            // The second pull can never resolve: the peer is doing the
            // same thing. The engine must fail this loudly.
            let second = stream.next().await;
            log(&self.log, format!("second: {second:?}"));
            second?;
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: a_mutual_stream_pull_deadlock_is_resolved_loudly,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![("PingPong", Box::new(PingPong { log: log.clone() }))]);
            let (outcome, events) = drive(cycle_project(), cat, &["ping"]).await;
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "a deadlocked run fails loudly, got {outcome:?}"
            );
            let err = node_failed_error(&events, "ping")
                .or_else(|| node_failed_error(&events, "pong"))
                .expect("at least one side fails with the deadlock error");
            assert!(err.contains("deadlock"), "the error names the deadlock: {err}");
        }
    );

    // ----- Buffer bound ------------------------------------------------

    /// Yield past the buffer cap without ever waiting for delivery.
    /// The consumer is GATED (it does not start pulling until the
    /// producer finished flooding and released it), so the un-taken
    /// count deterministically reaches the cap and the emission past
    /// it fails the producer's firing engine-side while the body sails
    /// on; the body's own stale `Ok` terminal must NOT resurrect the
    /// failed record. `declare` picks the cap: `Some(n)` pins that a
    /// declared cap overrides the default; `None` pins
    /// [`weft_core::generator::DEFAULT_MAX_BUFFERED_ITEMS`] itself.
    struct Flooder {
        declare: Option<usize>,
        count: usize,
        release: Arc<tokio::sync::Notify>,
    }
    test_manifest!(Flooder, "Flooder");
    #[async_trait]
    impl Node for Flooder {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            if let Some(cap) = self.declare {
                ctx.set_max_buffered_items("out", cap)?;
            }
            for i in 0..self.count {
                ctx.pulse_downstream(NodeOutput::new().set("out", json!(i))).await?;
            }
            // Only now may the consumer start pulling (a permit is
            // stored if it has not armed yet).
            self.release.notify_one();
            Ok(())
        }
    }

    /// A `Taker` that pulls nothing until `release` fires. Its wait is
    /// invisible to the engine's wait tracker on purpose: the test
    /// wants a consumer that is provably NOT pulling while the flood
    /// lands, and the flood always ends in the producer releasing it.
    struct GatedTaker {
        release: Arc<tokio::sync::Notify>,
        log: Log,
    }
    test_manifest!(GatedTaker, "GatedTaker");
    #[async_trait]
    impl Node for GatedTaker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            self.release.notified().await;
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            loop {
                match stream.next().await {
                    Ok(Some(v)) => log(&self.log, format!("took {v}")),
                    Ok(None) => return Ok(()),
                    Err(e) => {
                        log(&self.log, format!("pull error: {e}"));
                        return Err(e);
                    }
                }
            }
        }
    }

    async fn overrun_run(
        flooder: Flooder,
        release: Arc<tokio::sync::Notify>,
        log: &Log,
    ) -> (ExecutionOutcome, Vec<ExecEvent>) {
        let cat = catalog(vec![
            ("Yielder", Box::new(flooder)),
            ("Taker", Box::new(GatedTaker { release, log: log.clone() })),
        ]);
        drive(stream_project(), cat, &["producer"]).await
    }

    fn assert_overrun_failed(
        outcome: &ExecutionOutcome,
        events: &[ExecEvent],
        cap: usize,
    ) -> String {
        assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
        let err = node_failed_error(events, "producer").expect("producer failed");
        assert!(
            err.contains("un-taken items buffered")
                && err.contains(&format!("cap is {cap}"))
                && err.contains("yield_downstream"),
            "the overrun error names the cap and the fix: {err}"
        );
        // The engine-side failure STANDS: no NodeCompleted for the
        // producer resurrects the record (the body returned Ok after
        // the overrun, which is stale).
        assert!(
            !events.iter().any(|e| matches!(
                e,
                ExecEvent::NodeCompleted { node_id, .. } if node_id == "producer"
            )),
            "the stale body terminal must not overwrite the engine-side failure"
        );
        err
    }

    weft_core::stress_test!(
        name: overrunning_a_declared_stream_buffer_fails_the_producer_loudly,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let release = Arc::new(tokio::sync::Notify::new());
            let (outcome, events) = overrun_run(
                Flooder { declare: Some(8), count: 9, release: release.clone() },
                release,
                &log,
            )
            .await;
            assert_overrun_failed(&outcome, &events, 8);
        }
    );

    weft_core::stress_test!(
        name: overrunning_the_default_stream_buffer_fails_the_producer_loudly,
        runs: 2,
        worker_threads: 4,
        async fn body() {
            use weft_core::generator::DEFAULT_MAX_BUFFERED_ITEMS;
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let release = Arc::new(tokio::sync::Notify::new());
            let (outcome, events) = overrun_run(
                Flooder {
                    declare: None,
                    count: DEFAULT_MAX_BUFFERED_ITEMS + 1,
                    release: release.clone(),
                },
                release,
                &log,
            )
            .await;
            assert_overrun_failed(&outcome, &events, DEFAULT_MAX_BUFFERED_ITEMS);
        }
    );

    /// A cap declared MID-STREAM governs exactly the emissions after
    /// it: three uncapped items buffer fine, then `cap 2` makes the
    /// very next emission the overrun (3 already buffered >= 2).
    struct MidStreamCapper {
        release: Arc<tokio::sync::Notify>,
        log: Log,
    }
    test_manifest!(MidStreamCapper, "MidStreamCapper");
    #[async_trait]
    impl Node for MidStreamCapper {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            for i in 0..3 {
                ctx.pulse_downstream(NodeOutput::new().set("out", json!(i))).await?;
                log(&self.log, format!("sent {i}"));
            }
            ctx.set_max_buffered_items("out", 2)?;
            ctx.pulse_downstream(NodeOutput::new().set("out", json!(3))).await?;
            log(&self.log, "sent 3");
            self.release.notify_one();
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: a_cap_declared_mid_stream_governs_only_the_emissions_after_it,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let release = Arc::new(tokio::sync::Notify::new());
            let cat = catalog(vec![
                (
                    "Yielder",
                    Box::new(MidStreamCapper { release: release.clone(), log: log.clone() })
                        as Box<dyn Node>,
                ),
                ("Taker", Box::new(GatedTaker { release, log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            let err = assert_overrun_failed(&outcome, &events, 2);
            assert!(err.contains("3 un-taken"), "the 3 pre-cap items buffered fine: {err}");
            let entries = log.lock().unwrap().clone();
            assert!(
                entries.contains(&"sent 2".to_string()),
                "the emissions BEFORE the declaration were not governed by it: {entries:?}"
            );
        }
    );

    // ----- over: Generator on a Loop -----------------------------------

    /// producer.out (stream) -> lp__in.values; body doubles each item;
    /// lp__out.results gathers; sink.data receives the list.
    fn stream_loop_project(parallel: bool) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "producer", "nodeType": "Yielder", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "lp__in", "nodeType": "LoopIn", "label": null,
                    "config": { "parentId": "lp", "parallel": parallel, "over": ["values"], "carry": [] },
                    "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "values", "portType": "Generator[Number]", "required": true }],
                    "outputs": [
                        { "name": "values", "portType": "Number", "required": false },
                        { "name": "index", "portType": "Number", "required": false }
                    ],
                    "features": {}, "scope": [],
                    "groupBoundary": { "groupId": "lp", "role": "In" },
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "doubler", "nodeType": "Doubler", "label": null,
                    "config": null, "position": { "x": 2.0, "y": 0.0 },
                    "inputs": [{ "name": "n", "portType": "Number", "required": true }],
                    "outputs": [
                        { "name": "d", "portType": "Number", "required": false },
                        { "name": "stop", "portType": "Boolean", "required": false }
                    ],
                    "features": {}, "scope": ["lp"], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "lp__out", "nodeType": "LoopOut", "label": null,
                    "config": { "parentId": "lp" },
                    "position": { "x": 3.0, "y": 0.0 },
                    "inputs": [
                        { "name": "results", "portType": "Number", "required": false },
                        { "name": "done", "portType": "Boolean", "required": false }
                    ],
                    "outputs": [{ "name": "results", "portType": "List[Number | Null]", "required": false }],
                    "features": {}, "scope": [],
                    "groupBoundary": { "groupId": "lp", "role": "Out" },
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "sink", "nodeType": "Sink", "label": null,
                    "config": null, "position": { "x": 4.0, "y": 0.0 },
                    "inputs": [{ "name": "data", "portType": "List[Number | Null]", "required": true }],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e1", "source": "producer", "target": "lp__in", "sourceHandle": "out", "targetHandle": "values" },
                { "id": "e2", "source": "lp__in", "target": "doubler", "sourceHandle": "values", "targetHandle": "n" },
                { "id": "e3", "source": "doubler", "target": "lp__out", "sourceHandle": "d", "targetHandle": "results" },
                { "id": "e4", "source": "lp__out", "target": "sink", "sourceHandle": "results", "targetHandle": "data" }
            ],
            "groups": []
        }))
        .expect("stream loop project")
    }

    /// Doubles the item.
    struct Doubler;
    test_manifest!(Doubler, "Doubler");
    #[async_trait]
    impl Node for Doubler {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let n: f64 = ctx.inputs.get("n")?;
            ctx.pulse_downstream(NodeOutput::new().set("d", json!(n * 2.0))).await
        }
    }

    /// Votes done on every iteration (the done-vote test's body).
    struct DoublerDone;
    test_manifest!(DoublerDone, "DoublerDone");
    #[async_trait]
    impl Node for DoublerDone {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let n: f64 = ctx.inputs.get("n")?;
            ctx.pulse_downstream(
                NodeOutput::new().set("d", json!(n * 2.0)).set("stop", json!(true)),
            )
            .await
        }
    }

    /// Records the gathered list.
    struct Sink {
        log: Log,
    }
    test_manifest!(Sink, "Sink");
    #[async_trait]
    impl Node for Sink {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let data: Vec<serde_json::Value> = ctx.inputs.get("data")?;
            log(&self.log, format!("sink {data:?}"));
            Ok(())
        }
    }

    /// One body for every (parallel, delivered) combination: five
    /// streamed items each run a lane/iteration, the gather is
    /// index-aligned with the stream, and every launch row carries its
    /// item's pulse (the take's durability).
    async fn loop_over_stream_gathers(parallel: bool, delivered: bool) {
        let log: Log = Arc::new(StdMutex::new(Vec::new()));
        let cat = catalog(vec![
            ("Yielder", Box::new(Yielder { count: 5, delivered, fail_after: None, log: log.clone() })),
            ("Doubler", Box::new(Doubler)),
            ("Sink", Box::new(Sink { log: log.clone() })),
        ]);
        let (outcome, events) = drive(stream_loop_project(parallel), cat, &["producer"]).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        let entries = log.lock().unwrap().clone();
        let sink = entries.iter().find(|e| e.starts_with("sink")).expect("sink ran");
        assert_eq!(
            sink,
            &format!("sink {:?}", vec![json!(0.0), json!(2.0), json!(4.0), json!(6.0), json!(8.0)]),
            "the gather is exactly the doubled stream, in order \
             (parallel={parallel}, delivered={delivered})"
        );
        let launches = events
            .iter()
            .filter(|e| matches!(
                e,
                ExecEvent::LoopIterationLaunched { stream_pulse: Some(_), .. }
            ))
            .count();
        assert_eq!(launches, 5, "one stream-consuming launch per item");
    }

    weft_core::stress_test!(
        name: a_sequential_loop_over_a_delivered_stream_gathers_every_item,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            loop_over_stream_gathers(false, true).await;
        }
    );

    weft_core::stress_test!(
        name: a_sequential_loop_over_a_plain_stream_gathers_every_item,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            loop_over_stream_gathers(false, false).await;
        }
    );

    weft_core::stress_test!(
        name: a_parallel_loop_over_a_plain_stream_gathers_every_item,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            loop_over_stream_gathers(true, false).await;
        }
    );

    weft_core::stress_test!(
        name: a_parallel_loop_over_a_delivered_stream_gathers_every_item,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            loop_over_stream_gathers(true, true).await;
        }
    );

    weft_core::stress_test!(
        name: a_failed_stream_fails_the_loop_instead_of_gathering_a_truncated_list,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: true, fail_after: Some(2), log: log.clone() })),
                ("Doubler", Box::new(Doubler)),
                ("Sink", Box::new(Sink { log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_loop_project(false), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            assert!(
                !entries.iter().any(|e| e.starts_with("sink")),
                "no gather reaches the sink from a failed stream: {entries:?}"
            );
            let loop_err = node_failed_error(&events, "lp__in")
                .expect("the loop's boundary fails on the failed stream");
            assert!(
                loop_err.contains("yielder exploded"),
                "the loop failure carries the producer's error: {loop_err}"
            );
        }
    );

    weft_core::stress_test!(
        name: a_done_vote_ends_a_stream_loop_early_and_fails_the_waiting_producer,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: true, fail_after: None, log: log.clone() })),
                ("Doubler", Box::new(DoublerDone)),
                ("Sink", Box::new(Sink { log: log.clone() })),
            ]);
            let mut project = stream_loop_project(false);
            // Wire the body's done vote: doubler.stop -> lp__out.done.
            project.edges.push(
                serde_json::from_value(json!({
                    "id": "e5", "source": "doubler", "target": "lp__out",
                    "sourceHandle": "stop", "targetHandle": "done"
                }))
                .unwrap(),
            );
            let (outcome, events) = drive(project, cat, &["producer"]).await;
            // The loop terminated cleanly after one iteration; the
            // producer parked on a delivery the dead loop can never
            // take, which is a loud failure, so the run overall fails.
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            let sink = entries.iter().find(|e| e.starts_with("sink")).expect("sink ran");
            assert_eq!(
                sink,
                &format!("sink {:?}", vec![json!(0.0)]),
                "the done vote gathered exactly the one completed iteration"
            );
            let producer_err = node_failed_error(&events, "producer")
                .expect("the delivery-waiting producer fails once the loop is done");
            assert!(
                producer_err.contains("terminated before taking this stream item")
                    || producer_err
                        .contains("the consumer 'lp__in' finished without taking this stream item"),
                "the error names the dropped delivery, on the loop-terminated path or the \
                 boundary-retire path depending on which the vote raced into: {producer_err}"
            );
        }
    );

    // ----- Unwired, mistyped, and multi-stream edges --------------------

    weft_core::stress_test!(
        name: a_delivered_yield_on_an_unwired_stream_port_resolves_immediately,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            // No consumer edge: the emission creates no pulses, so the
            // gate arms empty and resolves as trivially delivered; the
            // producer must complete, never hang on a port nobody
            // listens to.
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 3, delivered: true, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let mut project = stream_project();
            project.edges.clear();
            let (outcome, _) = drive(project, cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            for i in 0..3 {
                assert!(entries.contains(&format!("sent {i}")), "{entries:?}");
            }
        }
    );

    /// Emits a STRING on the Generator[Number] port: the call itself
    /// must fail (closing the port instead would end the stream
    /// mid-flight and read downstream as a clean finish, the masked
    /// truncation the typed stream exists to prevent).
    struct WrongTyped {
        log: Log,
    }
    test_manifest!(WrongTyped, "WrongTyped");
    #[async_trait]
    impl Node for WrongTyped {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let r = ctx.pulse_downstream(NodeOutput::new().set("out", json!("not a number"))).await;
            log(&self.log, format!("emit: {r:?}"));
            r
        }
    }

    weft_core::stress_test!(
        name: a_mistyped_stream_item_fails_the_producers_call_never_a_clean_close,
        runs: 4,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(WrongTyped { log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let producer_err = node_failed_error(&events, "producer").expect("producer failed");
            assert!(producer_err.contains("does not accept"), "{producer_err}");
            // The consumer must NOT read a clean end over the refused
            // item: the producer's failure rides the closure as a
            // FAILED end, surfacing through the pull.
            let entries = log.lock().unwrap().clone();
            assert!(
                !entries.contains(&"stream finished".to_string()),
                "a mistyped item must never read downstream as a clean finish: {entries:?}"
            );
        }
    );

    /// Pulls the Number stream as `Generator<String>`: every take
    /// fails deserialization. The take is reported BEFORE the parse,
    /// so a delivered producer's gate still resolves (the handoff
    /// happened; the consumer's inability to parse is its own
    /// failure).
    struct StringTaker {
        log: Log,
    }
    test_manifest!(StringTaker, "StringTaker");
    #[async_trait]
    impl Node for StringTaker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let stream = ctx.inputs.get::<Generator<String>>("in")?;
            match stream.next().await {
                Ok(v) => log(&self.log, format!("took {v:?}")),
                Err(e) => {
                    log(&self.log, format!("pull error: {e}"));
                    return Err(e);
                }
            }
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: a_take_is_reported_before_deserialization_so_the_yield_resolves,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 1, delivered: true, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(StringTaker { log: log.clone() })),
            ]);
            let (outcome, events) = drive(stream_project(), cat, &["producer"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Failed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            assert!(
                entries.contains(&"sent 0".to_string()),
                "the producer's delivered yield resolved on the TAKE, not on the parse: \
                 {entries:?}"
            );
            let consumer_err = node_failed_error(&events, "consumer").expect("consumer failed");
            assert!(consumer_err.contains("does not deserialize"), "{consumer_err}");
            assert!(
                node_failed_error(&events, "producer").is_none(),
                "the producer's own firing succeeded (its item was taken)"
            );
        }
    );

    /// Joins two pull loops over two independent streams feeding one
    /// firing: the wait tracker must see the task as parked only when
    /// BOTH pulls are parked, and both streams must drain fully.
    struct TwoTaker {
        log: Log,
    }
    test_manifest!(TwoTaker, "TwoTaker");
    #[async_trait]
    impl Node for TwoTaker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let a = ctx.inputs.get::<Generator<i64>>("in_a")?;
            let b = ctx.inputs.get::<Generator<i64>>("in_b")?;
            let (ra, rb) = tokio::join!(a.drain(), b.drain());
            log(&self.log, format!("a {:?} b {:?}", ra?, rb?));
            Ok(())
        }
    }

    fn two_stream_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "pa", "nodeType": "YielderA", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "pb", "nodeType": "YielderB", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 1.0 },
                    "inputs": [],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "consumer", "nodeType": "TwoTaker", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [
                        { "name": "in_a", "portType": "Generator[Number]", "required": true },
                        { "name": "in_b", "portType": "Generator[Number]", "required": true }
                    ],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "ea", "source": "pa", "target": "consumer", "sourceHandle": "out", "targetHandle": "in_a" },
                { "id": "eb", "source": "pb", "target": "consumer", "sourceHandle": "out", "targetHandle": "in_b" }
            ],
            "groups": []
        }))
        .expect("two stream project")
    }

    weft_core::stress_test!(
        name: one_firing_drains_two_independent_streams_concurrently,
        runs: 16,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("YielderA", Box::new(Yielder { count: 3, delivered: false, fail_after: None, log: log.clone() })),
                ("YielderB", Box::new(Yielder { count: 2, delivered: true, fail_after: None, log: log.clone() })),
                ("TwoTaker", Box::new(TwoTaker { log: log.clone() })),
            ]);
            let (outcome, events) = drive(two_stream_project(), cat, &["pa", "pb"]).await;
            assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
            let entries = log.lock().unwrap().clone();
            assert!(
                entries.contains(&"a [0, 1, 2] b [0, 1]".to_string()),
                "both streams drain fully into the one firing: {entries:?}"
            );
            assert_eq!(started_count(&events, "consumer"), 1, "one dispatch for both streams");
        }
    );

    /// The false-positive half of deadlock detection: a producer that
    /// COMPUTES between yields leaves its consumer parked-and-caught-up
    /// for most of the run, exactly the window where a sloppy detector
    /// would close live work. The run must complete.
    struct BusyYielder {
        log: Log,
    }
    test_manifest!(BusyYielder, "BusyYielder");
    #[async_trait]
    impl Node for BusyYielder {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            for i in 0..5 {
                // Real scheduler-visible work between yields (no clock).
                for _ in 0..200 {
                    tokio::task::yield_now().await;
                }
                ctx.pulse_downstream(NodeOutput::new().set("out", json!(i))).await?;
                log(&self.log, format!("sent {i}"));
            }
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: a_computing_producer_is_never_declared_deadlocked_under_its_parked_consumer,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("Yielder", Box::new(BusyYielder { log: log.clone() })),
                ("Taker", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, _) = drive(stream_project(), cat, &["producer"]).await;
            assert!(
                matches!(outcome, ExecutionOutcome::Completed { .. }),
                "a live producer must never be torn down by the stuck-check, got {outcome:?}"
            );
            let entries = log.lock().unwrap().clone();
            assert_eq!(
                entries.iter().filter(|e| e.starts_with("took ")).count(),
                5,
                "every item arrived: {entries:?}"
            );
        }
    );

    /// A consumer that trips the execution's cancellation flag after
    /// its first take, while the producer is parked on its next
    /// delivered yield: the run must come back Cancelled (the cancel
    /// walk fails the pending gate; nothing hangs, nothing leaks).
    struct CancellingTaker {
        flag: Arc<CancellationFlag>,
        log: Log,
    }
    test_manifest!(CancellingTaker, "CancellingTaker");
    #[async_trait]
    impl Node for CancellingTaker {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let stream = ctx.inputs.get::<Generator<i64>>("in")?;
            let first = stream.next().await?;
            log(&self.log, format!("took {first:?}"));
            self.flag.cancel();
            // Keep pulling: the poison/teardown resolves this pull, or
            // the task is aborted by the cancel walk; either is fine.
            let _ = stream.next().await;
            Ok(())
        }
    }

    weft_core::stress_test!(
        name: cancelling_mid_stream_tears_down_cleanly,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let flag = CancellationFlag::new_arc();
            let cat = catalog(vec![
                ("Yielder", Box::new(Yielder { count: 5, delivered: true, fail_after: None, log: log.clone() })),
                ("Taker", Box::new(CancellingTaker { flag: flag.clone(), log: log.clone() })),
            ]);
            let (outcome, _) =
                drive_with_cancel(stream_project(), cat, &["producer"], flag).await;
            assert!(
                matches!(outcome, ExecutionOutcome::Cancelled),
                "a mid-stream cancel resolves the run as Cancelled, got {outcome:?}"
            );
        }
    );

    /// Mixed-kind deadlock: one node parked on a BUS wait that can
    /// never resolve, its peer parked on a STREAM pull from it. The
    /// shared tracker must see both wait kinds in one picture and
    /// resolve loudly (bus close first, unwinding everything).
    struct BusWaiter;
    test_manifest!(BusWaiter, "BusWaiter");
    #[async_trait]
    impl Node for BusWaiter {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            // One item first, so the puller dispatches with a live
            // feed and parks on its SECOND pull (a genuine stream
            // wait), while this body parks on a bus wait.
            ctx.pulse_downstream(NodeOutput::new().set("out", json!(0))).await?;
            let (bus, _marker) = ctx.create_bus(weft_core::bus::BusOptions::default())?;
            // Nobody will ever join as "ghost": a bus wait that only
            // deadlock resolution can end.
            bus.wait_for("ghost")
                .await
                .map_err(|e| weft_core::error::node_error(format!("bus wait ended: {e}")))?;
            Ok(())
        }
    }

    fn bus_stream_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "nodes": [
                {
                    "id": "waiter", "nodeType": "BusWaiter", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [],
                    "outputs": [{ "name": "out", "portType": "Generator[Number]", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "puller", "nodeType": "PullerFromWaiter", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "in", "portType": "Generator[Number]", "required": true }],
                    "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e", "source": "waiter", "target": "puller", "sourceHandle": "out", "targetHandle": "in" }
            ],
            "groups": []
        }))
        .expect("bus stream project")
    }

    weft_core::stress_test!(
        name: a_deadlock_across_a_bus_wait_and_a_stream_pull_resolves_loudly,
        runs: 8,
        worker_threads: 4,
        async fn body() {
            let log: Log = Arc::new(StdMutex::new(Vec::new()));
            let cat = catalog(vec![
                ("BusWaiter", Box::new(BusWaiter)),
                ("PullerFromWaiter", Box::new(Taker { take_only: None, log: log.clone() })),
            ]);
            let (outcome, events) = drive(bus_stream_project(), cat, &["waiter"]).await;
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "the mixed bus+stream deadlock must resolve loudly, got {outcome:?}"
            );
            // The proof that ONE picture saw both wait kinds: the
            // resolution only fires when EVERY in-flight firing is
            // provably parked, so if the tracker missed the stream
            // pull the drive would hang (the harness failsafe would
            // panic). Stage one of the resolution closes the buses,
            // so the waiter's error names the CLOSED bus wait, and the
            // waiter's failure poisons its stream so the puller's next
            // pull fails upstream too.
            let waiter_err = node_failed_error(&events, "waiter").expect("the bus waiter fails");
            assert!(
                waiter_err.contains("bus wait ended") && waiter_err.contains("closed"),
                "the waiter fails on the resolution's bus close, got: {waiter_err}"
            );
            let puller_err = node_failed_error(&events, "puller").expect("the puller fails");
            assert!(
                puller_err.contains("failed upstream"),
                "the puller's pull reads the poisoned stream, got: {puller_err}"
            );
            let entries = log.lock().unwrap().clone();
            assert!(
                entries.contains(&"took 0".to_string()),
                "the puller took the one item before parking: {entries:?}"
            );
        }
    );
