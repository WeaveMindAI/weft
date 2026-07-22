    //! Layer-3 test: two co-alive nodes exchanging messages over a bus,
    //! driven through the real loop. A `Producer` creates a bus, registers
    //! as "producer", emits the handle (which fires the `Consumer`), then
    //! waits via the handshake for the consumer to register as "llm"
    //! before streaming. The `Consumer` receives the live bus, registers
    //! as "llm", and drains the messages into a shared collector. The test
    //! asserts the consumer saw exactly the producer's messages, each
    //! stamped with the sender's REGISTERED name, proving create_bus ->
    //! register -> pulse_downstream -> input_bus -> wait_for -> send/recv
    //! works end to end through the engine.

    use super::*;
    use super::engine_test_rig::{test_manifest, MemJournal, NoopInfra, NoopInfraState, NoopProject, NoopTasks};
    use std::sync::Mutex as StdMutex;
    use async_trait::async_trait;
    use serde_json::json;
    use weft_core::node::{Node, NodeOutput};
    use weft_core::error::WeftResult;
    use weft_core::{ExecutionContext, NodeCatalog, ProjectDefinition};
    use weft_journal::ExecEvent;
    use crate::context::EngineClients;

    /// A minimal wait-for-input signal for the bus + await_signal tests.
    fn human_form() -> weft_core::signal::Form {
        weft_core::signal::Form {
            form_type: "human_query".into(),
            schema: weft_core::signal::FormSchema {
                title: String::new(),
                description: None,
                fields: Vec::new(),
            },
            title: None,
            description: None,
            consumer_kind: None,
        }
    }

    /// Producer: create a bus on output port "channel", emit it, then
    /// send three messages and close. Stays alive (its execute does not
    /// return) until it has sent + closed.
    struct Producer;
    test_manifest!(Producer, "Producer");
    #[async_trait]
    impl Node for Producer {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("producer").expect("register producer");
            // Put the marker on the bus output port; downstream resolves
            // it through the per-execution `BusRegistry`.
            ctx.pulse_downstream(NodeOutput::new().set("channel", marker))
                .await?;
            // Wait (via the language-level handshake) for the consumer to
            // register under "llm" before streaming: the bus only delivers
            // messages sent AFTER a participant is live. `wait_for` returns
            // an error (never hangs) if "llm" can never register.
            bus.wait_for("llm").await.expect("consumer 'llm' should register");
            for i in 0..3 {
                bus.send("tick", json!({ "i": i })).expect("send to live consumer");
            }
            // close() means "no more messages": the consumer drains the
            // three buffered ticks, THEN recv returns None.
            bus.close();
            Ok(())
        }
    }

    /// Consumer: register under "llm", then drain every "tick" message
    /// into the shared collector until the bus closes. Holds an Arc to the
    /// collector so the test can read it.
    struct Consumer {
        seen: Arc<StdMutex<Vec<(String, i64)>>>,
    }
    test_manifest!(Consumer, "Consumer");
    #[async_trait]
    impl Node for Consumer {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let mut bus = ctx.bus_from_input("channel")?;
            // Claim our identity: this is what releases the producer's
            // `wait_for("llm")` and stamps our sends.
            bus.register("llm").expect("register llm");
            let mut cursor = bus.cursor().with_filter(|entry| {
                matches!(
                    &entry.kind,
                    weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "tick"
                )
            });
            while let Some(entry) = cursor.next().await.expect("no FellBehind on journaled bus") {
                if let weft_core::bus::BusEntryKind::Message { from, payload, .. } = entry.kind {
                    let payload = payload.expect("journaled payload");
                    let i = payload["i"].as_i64().unwrap_or(-1);
                    self.seen.lock().unwrap().push((from, i));
                }
            }
            Ok(())
        }
    }

    struct TestCatalog {
        producer: &'static Producer,
        consumer: &'static Consumer,
    }
    impl NodeCatalog for TestCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            match node_type {
                "Producer" => Some(self.producer as &'static dyn Node),
                "Consumer" => Some(self.consumer as &'static dyn Node),
                _ => None,
            }
        }
        fn all(&self) -> Vec<&'static str> { vec!["Producer", "Consumer"] }
    }

    /// Project: producer.channel -> consumer.channel, both Bus ports.
    fn bus_project() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "name": "bus-test",
            "description": null,
            "nodes": [
                {
                    "id": "producer", "nodeType": "Producer", "label": null,
                    "config": null, "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "channel", "portType": "Bus", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "consumer", "nodeType": "Consumer", "label": null,
                    "config": null, "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "channel", "portType": "Bus", "required": true }],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e", "source": "producer", "target": "consumer", "sourceHandle": "channel", "targetHandle": "channel" }
            ],
            "groups": []
        }))
        .expect("bus project")
    }

    #[tokio::test]
    async fn two_nodes_exchange_messages_over_a_bus() {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let catalog: Arc<dyn NodeCatalog> = Arc::new(TestCatalog {
            producer: Box::leak(Box::new(Producer)),
            consumer: Box::leak(Box::new(Consumer { seen: seen.clone() })),
        });

        let project = bus_project();
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());

        // Seed: ExecutionStarted(Fire) + a NodeKicked on the producer
        // so it becomes ready (it has no real inputs).
        journal
            .record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: project.id.to_string(),
                    entry_node: "producer".into(),
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::NodeKicked {
                    color,
                    node_id: "producer".into(),
                    firing: false,
                    payload: None,
                    port_snapshot: None,
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let clients = EngineClients {
            journal: journal.clone(),
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            paid_calls: crate::context::FakePaidCallClient::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };

        let outcome = run_one_execution(
            Arc::new(project),
            catalog,
            color,
            clients,
            "pod-test".into(),
            "tenant-test".into(),
            "ns-test".into(),
            CancellationFlag::new_arc(),
            None,
        )
        .await
        .expect("run_one_execution ok");

        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "execution should complete, got {outcome:?}"
        );

        let mut got = seen.lock().unwrap().clone();
        got.sort_by_key(|(_, i)| *i);
        assert_eq!(
            got,
            vec![
                ("producer".to_string(), 0),
                ("producer".to_string(), 1),
                ("producer".to_string(), 2),
            ],
            "consumer received all three producer messages, each stamped with the producer's registered name"
        );

        // Bus events MUST be journaled so the inspector replays the
        // conversation. After the node_id reshape the bus events
        // carry only (bus_id, name) / (bus_id, from, payload): node
        // attribution is derived from PulseEmitted at the dispatcher
        // bridge, not stamped on the bus stream. Here we assert the
        // raw protocol shape: both names registered, three messages
        // arrived in order with the producer's name stamped on each.
        let journal_events = journal.events.lock().unwrap().clone();
        let mut join_names: Vec<String> = Vec::new();
        let mut messages: Vec<(String, String)> = Vec::new();
        for ev in &journal_events {
            match ev {
                ExecEvent::BusJoined { name, .. } => {
                    join_names.push(name.clone());
                }
                ExecEvent::BusMessage { from, payload, .. } => {
                    let p = payload
                        .value()
                        .and_then(|v| v.as_object())
                        .and_then(|o| o.get("i"))
                        .and_then(|v| v.as_i64())
                        .map(|i| i.to_string())
                        .unwrap_or_default();
                    messages.push((from.clone(), p));
                }
                _ => {}
            }
        }
        assert!(
            join_names.contains(&"producer".to_string()),
            "producer should have joined; got {join_names:?}"
        );
        assert!(
            join_names.contains(&"llm".to_string()),
            "consumer should have joined as 'llm'; got {join_names:?}"
        );
        assert_eq!(
            messages,
            vec![
                ("producer".to_string(), "0".to_string()),
                ("producer".to_string(), "1".to_string()),
                ("producer".to_string(), "2".to_string()),
            ],
            "every message must be journaled in order, stamped with the sender's registered name"
        );
    }

    /// A node waiting forever on a bus cursor (a warm co-alive node
    /// whose task never ends on its own) must not prevent cancellation:
    /// when the execution is cancelled, the loop wakes, returns
    /// Failed(cancelled), and the waiting task is aborted (its bus
    /// handle drops). This is what makes a user "Stop" tear down a
    /// live-bus execution.
    struct Waiter;
    test_manifest!(Waiter, "Waiter");
    #[async_trait]
    impl Node for Waiter {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let (mut bus, _marker) = ctx.create_bus(Default::default())?;
            bus.register("waiter").expect("register");
            // Wait forever: no peer ever sends or closes. Only
            // cancellation (task abort) can end this.
            let mut cursor = bus.cursor();
            while cursor.next().await.expect("no FellBehind on journaled bus").is_some() {}
            Ok(())
        }
    }

    struct WaiterCatalog(&'static Waiter);
    impl NodeCatalog for WaiterCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            (node_type == "Waiter").then_some(self.0 as &'static dyn Node)
        }
        fn all(&self) -> Vec<&'static str> { vec!["Waiter"] }
    }

    #[tokio::test]
    async fn cancellation_unblocks_a_node_waiting_on_cursor() {
        let catalog: Arc<dyn NodeCatalog> = Arc::new(WaiterCatalog(Box::leak(Box::new(Waiter))));
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "waiter", "description": null,
            "nodes": [{
                "id": "waiter", "nodeType": "Waiter", "label": null, "config": null,
                "position": { "x": 0.0, "y": 0.0 },
                "inputs": [], "outputs": [{ "name": "channel", "portType": "Bus", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": []
            }],
            "edges": [], "groups": []
        }))
        .expect("waiter project");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal.record_event(&ExecEvent::ExecutionStarted {
            color, project_id: project.id.to_string(), entry_node: "waiter".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: "test-hash".into(), at_unix: 0,
        }, None).await.unwrap();
        journal.record_event(&ExecEvent::NodeKicked {
            color, node_id: "waiter".into(), firing: false, payload: None, port_snapshot: None, at_unix: 0,
        }, None).await.unwrap();

        let clients = EngineClients {
            journal: journal.clone(),
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            paid_calls: crate::context::FakePaidCallClient::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        let cancel = CancellationFlag::new_arc();

        let run = tokio::spawn(run_one_execution(
            Arc::new(project), catalog, color, clients,
            "pod".into(), "tenant".into(), "ns".into(),
            cancel.clone(),
            None,
        ));
        // Let the waiter reach its cursor wait, then cancel.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();

        // Must finish (not hang). Bound it so a regression fails the
        // test instead of hanging the suite. The Waiter is a lone node
        // waiting on a cursor with no peer; cancellation and the dead-
        // end detector (every in-flight task waiting, nothing can feed
        // them) RACE to unblock it, and either is a correct teardown.
        // The safety property the test pins is "the execution
        // terminates", not the specific terminal outcome.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("run_one_execution must return, not hang")
            .expect("join ok")
            .expect("run ok");
        match outcome {
            ExecutionOutcome::Cancelled => {} // cancel won the race
            ExecutionOutcome::Completed { .. } => {} // dead-end closed the bus, Waiter exited cleanly
            other => panic!("expected Cancelled or Completed via dead-end, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Dead-end coverage: the four holes the bus model has to handle.
    //
    // The bus itself never decides "this wait can never be satisfied":
    // its `wait_for` only releases on register-or-close. The ENGINE
    // closes the bus when its loop concludes nothing can possibly feed
    // the waiting tasks. Each test below pins one such case.
    // -----------------------------------------------------------------

    /// A single configurable test node whose body is looked up from a
    /// global registry by `(project_id, node_id)`. Keying by project_id
    /// scopes bodies per-test (each test creates a fresh project uuid),
    /// so parallel `cargo test` runs of two tests that both use node_id
    /// "a" don't collide on a shared global key. The body is stored as
    /// `Arc<dyn Fn>` and cloned on lookup so a node can be dispatched
    /// more than once in one execution (fan-out, resume) without
    /// the first dispatch consuming the only copy.
    type NodeBody = std::sync::Arc<
        dyn Fn(ExecutionContext) -> std::pin::Pin<Box<dyn std::future::Future<Output = WeftResult<()>> + Send>>
            + Send
            + Sync,
    >;
    type BodyKey = (String, String); // (project_id, node_id)
    static NODE_BODIES: std::sync::OnceLock<StdMutex<std::collections::HashMap<BodyKey, NodeBody>>> =
        std::sync::OnceLock::new();
    fn bodies() -> &'static StdMutex<std::collections::HashMap<BodyKey, NodeBody>> {
        NODE_BODIES.get_or_init(|| StdMutex::new(std::collections::HashMap::new()))
    }
    fn install_body(project_id: &str, node_id: &str, body: NodeBody) {
        bodies()
            .lock()
            .unwrap()
            .insert((project_id.to_string(), node_id.to_string()), body);
    }

    struct Configurable;
    test_manifest!(Configurable, "Configurable");
    #[async_trait]
    impl Node for Configurable {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let key = (ctx.project_id.clone(), ctx.node_id.clone());
            let body = bodies().lock().unwrap().get(&key).cloned();
            match body {
                Some(b) => b(ctx).await,
                None => Err(weft_core::error::WeftError::Runtime(anyhow::anyhow!(
                    "no body installed for project '{}' node '{}'",
                    key.0, key.1
                ))),
            }
        }
    }
    struct ConfigurableCatalog(&'static Configurable);
    impl NodeCatalog for ConfigurableCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            (node_type == "Configurable").then_some(self.0 as &'static dyn Node)
        }
        fn all(&self) -> Vec<&'static str> { vec!["Configurable"] }
    }
    fn configurable_catalog() -> Arc<dyn NodeCatalog> {
        Arc::new(ConfigurableCatalog(Box::leak(Box::new(Configurable))))
    }

    /// Build a project where one entry node creates a bus on `port`, the
    /// rest of `extra_node_ids` are wired to it (downstream Bus consumers).
    fn bus_topology(creator: &str, extra_node_ids: &[&str], port: &str) -> ProjectDefinition {
        let mut nodes = vec![json!({
            "id": creator, "nodeType": "Configurable", "label": null, "config": null,
            "position": { "x": 0.0, "y": 0.0 },
            "inputs": [],
            "outputs": [{ "name": port, "portType": "Bus", "required": false }],
            "features": {}, "scope": [], "groupBoundary": null,
            "requiresInfra": false, "images": []
        })];
        let mut edges = Vec::new();
        for (i, peer) in extra_node_ids.iter().enumerate() {
            nodes.push(json!({
                "id": peer, "nodeType": "Configurable", "label": null, "config": null,
                "position": { "x": (i as f64) + 1.0, "y": 0.0 },
                "inputs": [{ "name": port, "portType": "Bus", "required": true }],
                "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            }));
            edges.push(json!({
                "id": format!("e{i}"),
                "source": creator, "target": peer,
                "sourceHandle": port, "targetHandle": port
            }));
        }
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "dead-end", "description": null,
            "nodes": nodes, "edges": edges, "groups": []
        }))
        .expect("project")
    }

    /// Topology for the bus + await_signal matrix: a bus `creator` wired
    /// to a `peer` (the pair holds the worker alive), PLUS an independent
    /// `waiter` entry node that does the `await_signal`. The waiter does
    /// NOT touch the bus and does NOT emit before awaiting (the engine
    /// forbids emit-then-await: replay would re-emit). All three are
    /// kicked as entry roots. This mirrors reality: a HumanQuery-style
    /// node waits for input while OTHER nodes keep a bus conversation
    /// alive; the bus only prevents the worker dying.
    fn bus_plus_waiter_topology() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "bus-plus-waiter", "description": null,
            "nodes": [
                {
                    "id": "creator", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "ch", "portType": "Bus", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "peer", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "ch", "portType": "Bus", "required": true }],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "waiter", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 1.0 },
                    "inputs": [], "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e0", "source": "creator", "target": "peer", "sourceHandle": "ch", "targetHandle": "ch" }
            ],
            "groups": []
        }))
        .expect("bus_plus_waiter project")
    }

    /// Like `bus_plus_waiter_topology` but the `waiter` is PULSE-FED: two
    /// upstream feeders (`feeder1`, `feeder2`) each emit a plain data pulse
    /// into a distinct required input port (`in1`, `in2`). Because the
    /// waiter has inbound edges, its resume goes through the
    /// `pulses_absorbed` un-absorb path (not the kicked `dispatched=false`
    /// reset). Two required ports means a re-dispatch only forms when BOTH
    /// ports carry a pending pulse, which is the lever the regression test
    /// uses: if a resume-absorbed pulse on `in2` is not recorded into
    /// `pulses_absorbed`, the next resume cannot re-satisfy `in2` and the
    /// waiter never re-fires. The creator+peer bus keeps the worker alive
    /// across the awaits exactly as in the kicked variant.
    fn bus_plus_pulse_fed_waiter_topology() -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "bus-plus-pulse-fed-waiter", "description": null,
            "nodes": [
                {
                    "id": "creator", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [{ "name": "ch", "portType": "Bus", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "peer", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 1.0, "y": 0.0 },
                    "inputs": [{ "name": "ch", "portType": "Bus", "required": true }],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "feeder1", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 1.0 },
                    "inputs": [], "outputs": [{ "name": "out", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "feeder2", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 1.0, "y": 1.0 },
                    "inputs": [], "outputs": [{ "name": "out", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "feeder3", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 2.0, "y": 1.0 },
                    "inputs": [], "outputs": [{ "name": "out", "portType": "String", "required": false }],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                },
                {
                    "id": "waiter", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 2.0 },
                    "inputs": [
                        { "name": "in1", "portType": "String", "required": true },
                        { "name": "in2", "portType": "String", "required": true }
                    ],
                    "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }
            ],
            "edges": [
                { "id": "e0", "source": "creator", "target": "peer", "sourceHandle": "ch", "targetHandle": "ch" },
                { "id": "e1", "source": "feeder1", "target": "waiter", "sourceHandle": "out", "targetHandle": "in1" },
                { "id": "e2", "source": "feeder2", "target": "waiter", "sourceHandle": "out", "targetHandle": "in2" },
                { "id": "e3", "source": "feeder3", "target": "waiter", "sourceHandle": "out", "targetHandle": "in2" }
            ],
            "groups": []
        }))
        .expect("bus_plus_pulse_fed_waiter project")
    }

    /// Standard test harness: seed ExecutionStarted + NodeKicked on the
    /// creator, run the execution with a bounded timeout (so a hang fails
    /// the test instead of the whole suite).
    async fn run_test(project: ProjectDefinition, creator: &str) -> ExecutionOutcome {
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        journal.record_event(&ExecEvent::ExecutionStarted {
            color, project_id: project.id.to_string(), entry_node: creator.into(),
            phase: weft_core::context::Phase::Fire, definition_hash: "test-hash".into(), at_unix: 0,
        }, None).await.unwrap();
        journal.record_event(&ExecEvent::NodeKicked {
            color, node_id: creator.into(), firing: false, payload: None, port_snapshot: None, at_unix: 0,
        }, None).await.unwrap();
        let clients = EngineClients {
            journal: journal.clone(),
            tasks: Arc::new(NoopTasks),
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            paid_calls: crate::context::FakePaidCallClient::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_one_execution(
                Arc::new(project), configurable_catalog(), color, clients,
                "pod".into(), "tenant".into(), "ns".into(),
                CancellationFlag::new_arc(),
                None,
            ),
        )
        .await
        .expect("execution must not hang (the dead-end detector failed)")
        .expect("run_one_execution ok")
    }

    /// A node body that PANICS must NOT re-run forever. The panicked
    /// task never sends a NodeTaskResult, so before the task-id fix its
    /// exec record stayed Running, the crashed-Running refold path
    /// re-dispatched it on every respawn, and the node panicked in a
    /// tight loop until the refetch wall-clock deadline (the execution
    /// effectively hung). Now the loop maps the JoinError's task id back
    /// to (node, frames) and journals a terminal NodeFailed, so the
    /// execution unwinds promptly. The 10s timeout in `run_test` is the
    /// hang tripwire.
    #[tokio::test]
    async fn panicking_node_body_fails_instead_of_re_running_forever() {
        let project = bus_topology("a", &[], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "a", std::sync::Arc::new(|_ctx| Box::pin(async move {
            panic!("node body boom");
        })));
        let outcome = run_test(project, "a").await;
        // The panicked node is the only firing; the execution must reach
        // a terminal outcome (Failed via the cascade, or Completed once
        // the failed node closed its outputs and nothing else remained),
        // never hang. The tripwire is the timeout inside run_test.
        assert!(
            matches!(
                outcome,
                ExecutionOutcome::Failed { .. } | ExecutionOutcome::Completed { .. }
            ),
            "panicking node must terminate the execution, got {outcome:?}"
        );
    }

    /// Announce a previously-created bus by emitting on its output port.
    /// Emit a bus marker on `port`. The producer owns the marker value
    /// (returned by `create_bus()` alongside the handle); putting it on
    /// the output port is what makes the bus reachable downstream.
    async fn emit_bus_marker(
        ctx: &ExecutionContext,
        port: &str,
        marker: serde_json::Value,
    ) -> WeftResult<()> {
        ctx.pulse_downstream(NodeOutput::new().set(port, marker)).await
    }

    /// HOLE 1: creator registers + waits for a peer whose dispatch happens
    /// LATER (the peer's pulse only fires once the creator emits). The
    /// engine must not declare dead-end while the peer is still scheduled
    /// to dispatch (its pulse is pending, in_flight is about to grow).
    /// The existing happy-path `two_nodes_exchange_messages_over_a_bus`
    /// test exercises this baseline; here we stress it by making the
    /// creator emit AND wait, while the peer takes its time to dispatch.
    #[tokio::test]
    async fn hole1_waits_while_peer_is_still_scheduled() {
        let project = bus_topology("creator", &["peer"], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "creator", std::sync::Arc::new(|ctx| Box::pin(async move {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("creator").expect("register");
            emit_bus_marker(&ctx, "ch", marker).await?;
            // Wait for the peer to register. The peer is dispatched on the
            // next loop iteration after the emit lands; the engine must
            // not panic / dead-end while it gets there.
            bus.wait_for("peer").await.expect("peer should register");
            bus.send("ping", json!(null)).expect("send to live peer");
            bus.close();
            Ok(())
        })));
        install_body(&pid, "peer", std::sync::Arc::new(|ctx| Box::pin(async move {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("peer").expect("register");
            // Drain until close.
            let mut cursor = bus.cursor();
            while cursor.next().await.expect("no FellBehind on journaled bus").is_some() {}
            Ok(())
        })));
        let outcome = run_test(project, "creator").await;
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "hole 1: peer registers in time, execution completes; got {outcome:?}"
        );
    }

    /// HOLE 2: A → B → C cascade. A waits for "b", B waits for "c", and
    /// C fails before registering. The engine has to: see C terminate,
    /// notice B is waiting with nothing to feed it, close the bus → B
    /// fails → notice A is waiting with nothing to feed it, close again
    /// → A fails. Execution unwinds to Failed without hanging.
    #[tokio::test]
    async fn hole2_cascading_failure_when_peer_crashes_before_registering() {
        let project = bus_topology("a", &["b", "c"], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "a", std::sync::Arc::new(|ctx| Box::pin(async move {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("a").expect("register a");
            emit_bus_marker(&ctx, "ch", marker).await?;
            // A waits for B. When the engine closes the bus, this errors.
            bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                anyhow::anyhow!("a: wait_for(b) failed: {e}"),
            ))?;
            Ok(())
        })));
        install_body(&pid, "b", std::sync::Arc::new(|ctx| Box::pin(async move {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("b").expect("register b");
            // B waits for C. When the engine closes the bus, this errors.
            bus.wait_for("c").await.map_err(|e| weft_core::error::WeftError::Runtime(
                anyhow::anyhow!("b: wait_for(c) failed: {e}"),
            ))?;
            Ok(())
        })));
        install_body(&pid, "c", std::sync::Arc::new(|_ctx| Box::pin(async move {
            // C crashes before registering: its handle drops without ever
            // claiming a name. B's `wait_for("c")` would hang forever
            // without the engine's dead-end detector.
            Err(weft_core::error::WeftError::Runtime(anyhow::anyhow!("c: simulated crash")))
        })));
        let outcome = run_test(project, "a").await;
        assert!(
            matches!(outcome, ExecutionOutcome::Failed { .. }),
            "hole 2: the cascade unwinds to Failed (not hang); got {outcome:?}"
        );
    }

    /// HOLE 3: a node receives the bus and keeps the handle alive without
    /// ever registering on it (a "passes through" / "holds without
    /// participating" pattern). The producer waits for the real consumer;
    /// the holder must not be miscounted as a participant. With the new
    /// model the holder is invisible to membership (it never registers),
    /// so the producer waits cleanly for the real consumer and unblocks
    /// when it registers.
    #[tokio::test]
    async fn hole3_node_holds_bus_without_registering_does_not_block_waits() {
        let project = bus_topology("producer", &["inspector", "consumer"], "ch");
        let pid = project.id.to_string();
        install_body(&pid, "producer", std::sync::Arc::new(|ctx| Box::pin(async move {
            let (mut bus, marker) = ctx.create_bus(Default::default())?;
            bus.register("producer").expect("register");
            emit_bus_marker(&ctx, "ch", marker).await?;
            // The producer waits for the real consumer. The holder
            // (`inspector`) also has the bus but never registered; if the
            // engine miscounted holders as participants, this would
            // dead-end with the inspector still alive.
            bus.wait_for("consumer").await.expect("real consumer should register");
            bus.send("ping", json!(null)).expect("send");
            bus.close();
            Ok(())
        })));
        install_body(&pid, "inspector", std::sync::Arc::new(|ctx| Box::pin(async move {
            // Holds the bus, never registers, never recvs. Just stays
            // alive briefly then drops it. Simulates "a node that touches
            // the bus but does not participate" (the case where a holder
            // would have falsely counted as a participant under the old
            // drop-based liveness model).
            let _bus = ctx.bus_from_input("ch")?;
            Ok(())
        })));
        install_body(&pid, "consumer", std::sync::Arc::new(|ctx| Box::pin(async move {
            let mut bus = ctx.bus_from_input("ch")?;
            bus.register("consumer").expect("register");
            let mut cursor = bus.cursor();
            while cursor.next().await.expect("no FellBehind on journaled bus").is_some() {}
            Ok(())
        })));
        let outcome = run_test(project, "producer").await;
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "hole 3: holder does not affect the wait; got {outcome:?}"
        );
    }

    // HOLE 4: mutual deadlock. A registers as "a" and waits for "b". B
    // registers as "b" and waits for "a"... wait, that releases. The
    // real deadlock is: A registers as "a" and waits for "x", B
    // registers as "b" and waits for "y", neither x nor y ever come.
    // Every in-flight task is waiting, nothing can feed them, the engine
    // closes the buses and both error out.
    //
    // MULTI-THREADED to exercise the engine loop's
    // `bus_coordinator.wait_notified()` arm-then-check pattern under
    // contention: a wait-start from the LLM worker thread can race the
    // loop thread's dead-end check. Before the `notify_one` fix this
    // test deadlocked to the 10s harness in ~1/8 of parallel-load
    // runs. Stress-looped at 64 concurrent iterations per invocation
    // so any future regression of the arm-then-check window
    // reproduces on the first failing CI run rather than waiting
    // for the flake to find us.
    weft_core::stress_test!(
        name: hole4_mutual_deadlock_when_both_wait_for_names_that_never_come,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            install_body(&pid, "a", std::sync::Arc::new(|ctx| Box::pin(async move {
                let (mut bus, marker) = ctx.create_bus(Default::default())?;
                bus.register("a").expect("register");
                emit_bus_marker(&ctx, "ch", marker).await?;
                bus.wait_for("x").await.map_err(|e| weft_core::error::WeftError::Runtime(
                    anyhow::anyhow!("a: {e}"),
                ))?;
                Ok(())
            })));
            install_body(&pid, "b", std::sync::Arc::new(|ctx| Box::pin(async move {
                let mut bus = ctx.bus_from_input("ch")?;
                bus.register("b").expect("register");
                bus.wait_for("y").await.map_err(|e| weft_core::error::WeftError::Runtime(
                    anyhow::anyhow!("b: {e}"),
                ))?;
                Ok(())
            })));
            let outcome = run_test(project, "a").await;
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "hole 4: mutual deadlock unwinds to Failed; got {outcome:?}"
            );
        }
    );

    weft_core::stress_test!(
        // The mirror of hole4: a GENUINE live exchange (A sends, B
        // replies) that ends in a deadlock. The send-then-park window is
        // exactly where a naive stuck-check could close the bus under a
        // woken-but-unpolled receiver. With the per-node observed-
        // generation accounting (`deadlock_provable`), the close must
        // wait until B has consumed A's message and A has consumed B's
        // reply; only the final mutual wait-for-never deadlocks. Stress-
        // looped under a 4-thread runtime so the cross-thread "B woken
        // but parked in another worker's queue" interleaving surfaces.
        //
        // Each iteration gets its OWN `Arc<AtomicU32>` exchange counter
        // captured into the node bodies (the 64 stress iterations run
        // concurrently, so a shared/global counter would race across
        // iterations). The counter reaches 2 only if BOTH directions of
        // the exchange complete before the deadlock close.
        name: live_exchange_then_deadlock_is_not_torn_down_early,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let exchanges = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            let a_ex = exchanges.clone();
            install_body(&pid, "a", std::sync::Arc::new(move |ctx| {
                let a_ex = a_ex.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("a").expect("register");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    // Wait for B to be live, then send it a message and
                    // immediately park reading for B's reply. The park
                    // right after the send is the false-positive window.
                    bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: wait_for(b): {e}"),
                    ))?;
                    // Position the pong-cursor BEFORE sending ping, so B's
                    // reply (which may land before A is re-scheduled)
                    // cannot slip behind the cursor's start offset.
                    let mut cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "pong"
                    ));
                    bus.send("ping", serde_json::json!({"v": 1})).expect("a sends ping");
                    // The close may legitimately race the deadlock tail;
                    // a closed bus here surfaces as Ok(None), not a panic.
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        a_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    // Now deadlock: wait for a name that never comes.
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let b_ex = exchanges.clone();
            install_body(&pid, "b", std::sync::Arc::new(move |ctx| {
                let b_ex = b_ex.clone();
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("b").expect("register");
                    // Drain A's ping, reply with pong, then deadlock.
                    let mut cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "ping"
                    ));
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        b_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // The reply can race the close; ignore a closed bus.
                        let _ = bus.send("pong", serde_json::json!({"v": 2}));
                    }
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("b: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let outcome = run_test(project, "a").await;
            // Both directions of the exchange must have completed before
            // the deadlock close. A premature stuck-close would cut the
            // reply, leaving the counter below 2.
            assert_eq!(
                exchanges.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "live ping/pong must complete before the deadlock close \
                 (bus torn down under a woken-but-unpolled peer)"
            );
            // And the final mutual wait-for-never still unwinds to Failed.
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "post-exchange mutual deadlock unwinds to Failed; got {outcome:?}"
            );
        }
    );

    weft_core::stress_test!(
        // Targets the observed-to-return window: a cursor's `next()`
        // records its node's observed generation at the TOP of its loop
        // (clearing its parked flag), BEFORE the search runs. Without the
        // per-node parked flag, the stuck-check could run inside that
        // window and read the searching node as "caught up" while its
        // evaluation was about to SUCCEED (find A's ping): A is parked
        // caught-up on wait_for("never"), B is mid-search, so both nodes
        // could read as parked-and-caught-up, the parked count reaches
        // in_flight, close_all() fires, and B's pong reply hits
        // SendError::Closed: a live exchange torn down.
        //
        // The window is widened deliberately: B's filter (which runs
        // synchronously inside the search, after the `observed` call and
        // before the message is returned) sleeps ~1ms on the matching
        // entry, holding B mid-evaluation long enough for the driver
        // thread's stuck-check to interleave on the multi-thread
        // runtime. With the parked flag, B's node reads as not-parked
        // for the whole search, so the parked count stays below
        // in_flight, the close is suppressed, the pong send succeeds, and
        // the exchange counter reaches 2. The final mutual
        // wait-for-never still closes (both nodes parked, both caught
        // up): no false negative.
        name: stuck_check_during_succeeding_evaluation_does_not_close,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let exchanges = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            let a_ex = exchanges.clone();
            install_body(&pid, "a", std::sync::Arc::new(move |ctx| {
                let a_ex = a_ex.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("a").expect("register");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: wait_for(b): {e}"),
                    ))?;
                    // Cursor positioned BEFORE the ping send so B's pong
                    // cannot slip behind the start offset.
                    let mut cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "pong"
                    ));
                    // Send ping, then park immediately: A becomes the
                    // parked caught-up half of the false-positive pair
                    // while B is mid-search on the ping.
                    bus.send("ping", serde_json::json!({"v": 1})).expect("a sends ping");
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        a_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let b_ex = exchanges.clone();
            install_body(&pid, "b", std::sync::Arc::new(move |ctx| {
                let b_ex = b_ex.clone();
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("b").expect("register");
                    // The sleep runs inside the search, between B's
                    // record_observed and the message being returned:
                    // exactly the window the parked flag must cover.
                    let mut cursor = bus.cursor().with_filter(|e| {
                        let hit = matches!(
                            &e.kind,
                            weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "ping"
                        );
                        if hit {
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                        hit
                    });
                    if matches!(cursor.next().await, Ok(Some(_))) {
                        b_ex.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // The reply MUST succeed: a Closed here is the
                        // false positive this test exists to catch. The
                        // exchange counter (asserted below) carries the
                        // failure; `expect` would also abort the run
                        // loudly at the exact broken send.
                        bus.send("pong", serde_json::json!({"v": 2}))
                            .expect("pong send failed: bus closed under a succeeding evaluation");
                    }
                    bus.wait_for("never").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("b: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            let outcome = run_test(project, "a").await;
            assert_eq!(
                exchanges.load(std::sync::atomic::Ordering::SeqCst),
                2,
                "ping/pong must complete: the stuck-check must not close \
                 the bus while B's evaluation is mid-search on the ping"
            );
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "post-exchange mutual deadlock unwinds to Failed; got {outcome:?}"
            );
        }
    );

    weft_core::stress_test!(
        // A cancelled node's runtime-granted provider access is given
        // back by the RUNTIME even though the node's future is aborted
        // mid-body (the node never runs any wrap-up code of its own; the
        // AccessCloseGuard's drop path is what must fire). The body opens
        // an access, signals readiness, and parks on the cancellation flag
        // forever; the test cancels after the open is in, then asserts the
        // close landed. Stress-looped: the drop-spawned close races the
        // execution's teardown by construction.
        name: a_cancelled_nodes_provider_access_is_given_back,
        runs: 32,
        worker_threads: 4,
        async fn body() {
            let project: ProjectDefinition = serde_json::from_value(json!({
                "id": uuid::Uuid::new_v4(), "name": "grant", "description": null,
                "nodes": [{
                    "id": "payer", "nodeType": "Configurable", "label": null, "config": null,
                    "position": { "x": 0.0, "y": 0.0 },
                    "inputs": [], "outputs": [],
                    "features": {}, "scope": [], "groupBoundary": null,
                    "requiresInfra": false, "images": []
                }],
                "edges": [], "groups": []
            }))
            .expect("grant project");
            let pid = project.id.to_string();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
            let ready_tx = std::sync::Arc::new(StdMutex::new(Some(ready_tx)));
            install_body(&pid, "payer", std::sync::Arc::new(move |ctx| {
                let ready_tx = ready_tx.clone();
                Box::pin(async move {
                    let _access = ctx.provider_access("openrouter", None).await?;
                    if let Some(tx) = ready_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                    // Park until the abort: the node itself never gives the
                    // access back; the runtime must.
                    ctx.cancellation().cancelled().await;
                    std::future::pending::<()>().await;
                    Ok(())
                })
            }));

            let color = uuid::Uuid::new_v4();
            let journal = Arc::new(MemJournal::default());
            journal.record_event(&ExecEvent::ExecutionStarted {
                color, project_id: pid.clone(), entry_node: "payer".into(),
                phase: weft_core::context::Phase::Fire, definition_hash: "test-hash".into(), at_unix: 0,
            }, None).await.unwrap();
            journal.record_event(&ExecEvent::NodeKicked {
                color, node_id: "payer".into(), firing: false, payload: None, port_snapshot: None, at_unix: 0,
            }, None).await.unwrap();
            let fake_paid_calls = crate::context::FakePaidCallClient::new();
            fake_paid_calls.set_key("openrouter", "sk-platform");
            let clients = EngineClients {
                journal: journal.clone(),
                tasks: Arc::new(NoopTasks),
                infra: Arc::new(NoopInfra),
                infra_state: Arc::new(NoopInfraState),
                project: Arc::new(NoopProject),
                clock: Arc::new(weft_platform_traits::clock::SystemClock),
                storage: crate::storage::FakeWorkerStorage::new(),
                paid_calls: fake_paid_calls.clone(),
                pending_costs: crate::metering::PendingCostRecords::new(),
            };
            let cancel = CancellationFlag::new_arc();
            let run = tokio::spawn(run_one_execution(
                Arc::new(project), configurable_catalog(), color, clients,
                "pod".into(), "tenant".into(), "ns".into(),
                cancel.clone(),
                None,
            ));
            ready_rx.await.expect("payer must reach its opened-access state");
            cancel.cancel();
            let outcome = tokio::time::timeout(std::time::Duration::from_secs(8), run)
                .await
                .expect("cancel must terminate promptly")
                .expect("join ok")
                .expect("run ok");
            assert!(
                matches!(outcome, ExecutionOutcome::Cancelled),
                "cancelled execution reports Cancelled; got {outcome:?}"
            );
            // The close is drop-spawned on abort, so it may land a beat
            // after the run returns: poll briefly rather than sleep blind.
            for _ in 0..100 {
                if !fake_paid_calls.closed_accesses.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(
                *fake_paid_calls.closed_accesses.lock().unwrap(),
                vec!["sk-platform".to_string()],
                "the aborted body's access must be given back by the runtime"
            );
        }
    );

    weft_core::stress_test!(
        // END-TO-END concurrent waits in ONE node task, through the real
        // WaitGuard/cursor wiring (not the coordinator hooks directly).
        // Node A holds TWO concurrent bus waits at once: a `tokio::select!`
        // over two cursors on its bus (one filtering "ping", one a
        // membership wait). This is the exact shape the per-node single-
        // wait slot got wrong: two WaitGuards under one (node, frames),
        // where the old code clobbered the first wait's state and panicked
        // the worker when the second guard dropped. Here B sends "ping",
        // resolving A's select; A then deadlocks waiting for a name that
        // never comes while B also deadlocks. The run must reach Failed
        // (clean unwind) and NOT panic: if the select's two guards
        // corrupted the liveness map, the worker would abort instead.
        name: concurrent_waits_in_one_task_unwind_cleanly,
        runs: 64,
        worker_threads: 4,
        async fn body() {
            let got_ping = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
            let project = bus_topology("a", &["b"], "ch");
            let pid = project.id.to_string();
            let a_got = got_ping.clone();
            install_body(&pid, "a", std::sync::Arc::new(move |ctx| {
                let a_got = a_got.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("a").expect("register");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("b").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: wait_for(b): {e}"),
                    ))?;
                    // TWO concurrent waits in one task: a cursor for "ping"
                    // and a membership wait for a name that never comes.
                    // select! polls BOTH futures, so both hold a live
                    // WaitGuard under (a, root frames) at once.
                    let mut ping_cursor = bus.cursor().with_filter(|e| matches!(
                        &e.kind,
                        weft_core::bus::BusEntryKind::Message { msg_kind, .. } if msg_kind == "ping"
                    ));
                    let never_handle = bus.new_handle();
                    tokio::select! {
                        r = ping_cursor.next() => {
                            if matches!(r, Ok(Some(_))) {
                                a_got.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                        r = never_handle.wait_for("never-x") => {
                            // The bus close can race this; a Closed here is
                            // fine (the select's other branch or the
                            // deadlock won).
                            let _ = r;
                        }
                    }
                    // Now deadlock on a name that never arrives.
                    bus.wait_for("never-y").await.map_err(|e| weft_core::error::WeftError::Runtime(
                        anyhow::anyhow!("a: {e}"),
                    ))?;
                    Ok(())
                })
            }));
            install_body(&pid, "b", std::sync::Arc::new(|ctx| Box::pin(async move {
                let mut bus = ctx.bus_from_input("ch")?;
                bus.register("b").expect("register");
                let _ = bus.send("ping", serde_json::json!({"v": 1}));
                bus.wait_for("never-z").await.map_err(|e| weft_core::error::WeftError::Runtime(
                    anyhow::anyhow!("b: {e}"),
                ))?;
                Ok(())
            })));
            let outcome = run_test(project, "a").await;
            assert!(
                matches!(outcome, ExecutionOutcome::Failed { .. }),
                "concurrent waits then mutual deadlock unwinds to Failed (no worker abort); \
                 got {outcome:?}"
            );
        }
    );

    // ─────────────────────────────────────────────────────────────────
    // Bus + wait-for-input (await_signal). A live bus keeps the worker
    // alive but must be TRANSPARENT to the signal machinery: a node can
    // await_signal while a bus is open; the worker stays alive (bus
    // holds it) and the resume is delivered IN PROCESS the moment the
    // fire's `SuspensionResolved` lands. When no bus holds the worker, a
    // parked await falls through to the normal stall -> die -> respawn
    // path. These tests pin the whole matrix.
    // ─────────────────────────────────────────────────────────────────

    /// Tasks fake for await_signal tests. `enqueue_dedup` of a
    /// RegisterSignal mints a deterministic token (recording it so the
    /// test can inject the matching `SuspensionResolved`) and
    /// `wait_for_terminal` hands back a `RegisterSignalReply { token }`.
    /// Every other task kind is unreachable in these tests.
    struct AwaitTasks {
        // (task_id -> token) so wait_for_terminal returns the same token
        // enqueue minted, and the test can read the token to resolve it.
        tokens: StdMutex<std::collections::HashMap<uuid::Uuid, String>>,
        // The most-recently-minted token, for the test to resolve.
        last_token: StdMutex<Option<String>>,
    }
    impl AwaitTasks {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                tokens: StdMutex::new(std::collections::HashMap::new()),
                last_token: StdMutex::new(None),
            })
        }
        /// Block (test-side) until a token has been minted, then return
        /// it. Buses race the worker; the await may not have registered
        /// the instant the test wants to resolve it.
        async fn await_token(&self) -> String {
            for _ in 0..2000 {
                if let Some(t) = self.last_token.lock().unwrap().clone() {
                    return t;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            panic!("no register_signal token minted within timeout");
        }
    }
    #[async_trait]
    impl weft_task_store::TaskStoreClient for AwaitTasks {
        async fn enqueue_dedup(
            &self,
            t: weft_task_store::tasks::NewTask,
        ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
            assert_eq!(
                t.kind,
                weft_task_store::TaskKind::RegisterSignal.as_str(),
                "await tests only enqueue RegisterSignal"
            );
            let id = uuid::Uuid::new_v4();
            // Deterministic token derived from the task id.
            let token = format!("tok-{id}");
            self.tokens.lock().unwrap().insert(id, token.clone());
            *self.last_token.lock().unwrap() = Some(token);
            Ok(weft_task_store::tasks::DedupOutcome::Inserted(id))
        }
        async fn wait_for_terminal(
            &self,
            t: uuid::Uuid,
            _to: std::time::Duration,
            _pi: std::time::Duration,
        ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
            let token = self
                .tokens
                .lock()
                .unwrap()
                .get(&t)
                .cloned()
                .expect("token for task id");
            Ok(weft_task_store::tasks::TaskOutcome {
                status: weft_task_store::tasks::TaskStatus::Complete,
                result: Some(serde_json::json!({ "token": token })),
                error: None,
            })
        }
        async fn claim_one(
            &self,
            _p: &str,
            _f: weft_task_store::tasks::ClaimFilter,
        ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
            Ok(None)
        }
        async fn heartbeat(&self, _t: uuid::Uuid, _p: &str) -> anyhow::Result<bool> {
            Ok(true)
        }
        async fn complete(&self, _t: uuid::Uuid, _p: &str, _r: Value) -> anyhow::Result<()> {
            Ok(())
        }
        async fn fail(&self, _t: uuid::Uuid, _p: &str, _e: String) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// Spawn `run_one_execution` with the given tasks fake + journal so a
    /// test can inject a `SuspensionResolved` into the journal while the
    /// worker runs. Every node in `kicked` is seeded as an entry root
    /// (so a bus-holder and an independent await-node can both start).
    /// Returns the join handle and the color.
    fn spawn_run(
        project: ProjectDefinition,
        kicked: &[&str],
        journal: Arc<MemJournal>,
        tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    ) -> (tokio::task::JoinHandle<anyhow::Result<ExecutionOutcome>>, Color) {
        let color = uuid::Uuid::new_v4();
        let entry = kicked[0].to_string();
        let kicked: Vec<String> = kicked.iter().map(|s| s.to_string()).collect();
        let pid = project.id.to_string();
        let j = journal.clone();
        let handle = tokio::spawn(async move {
            j.record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: pid,
                    entry_node: entry,
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
            for node_id in kicked {
                j.record_event(
                    &ExecEvent::NodeKicked {
                        color,
                        node_id,
                        firing: false,
                        payload: None,
                        port_snapshot: None,
                        at_unix: 0,
                    },
                    None,
                )
                .await
                .unwrap();
            }
            let clients = EngineClients {
                journal: j,
                tasks,
                infra: Arc::new(NoopInfra),
                infra_state: Arc::new(NoopInfraState),
                project: Arc::new(NoopProject),
                clock: Arc::new(weft_platform_traits::clock::SystemClock),
                storage: crate::storage::FakeWorkerStorage::new(),
                paid_calls: crate::context::FakePaidCallClient::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
            };
            run_one_execution(
                Arc::new(project),
                configurable_catalog(),
                color,
                clients,
                "pod".into(),
                "tenant".into(),
                "ns".into(),
                CancellationFlag::new_arc(),
                None,
            )
            .await
        });
        (handle, color)
    }

    /// BASELINE (no bus): a lone node awaits. With no bus holding it, the
    /// worker stalls and EXITS (Stalled) so the dispatcher can respawn it
    /// on the fire. This is the unchanged normal path; the bus work must
    /// not have altered it.
    #[tokio::test]
    async fn await_without_bus_stalls_and_exits() {
        let project = bus_topology("waiter", &[], "ch");
        // (no bus consumer wired; the creator just awaits, never touches a bus)
        let pid = project.id.to_string();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let _ = ctx.await_signal(human_form()).await?;
                    Ok(())
                })
            }),
        );
        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, _color) =
            spawn_run(project, &["waiter"], journal.clone(), tasks.clone());
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Stalled),
            "a bus-less await must stall and exit so a fresh worker resumes on the fire; got {outcome:?}"
        );
    }

    /// IN-FLIGHT RESUME (bus alive): a `creator`+`peer` keep a bus
    /// conversation open (holding the worker alive), while an independent
    /// `waiter` node parks on `await_signal`. The test injects
    /// `SuspensionResolved` mid-flight; the waiter resumes IN PROCESS on
    /// the live worker (no respawn), and once it resumes it tells the
    /// creator to wrap up so the bus closes and the execution completes.
    /// Proves the bus is transparent to the signal: the resume happens on
    /// the running worker exactly as it would without a bus.
    #[tokio::test]
    async fn await_with_live_bus_resumes_in_process() {
        let project = bus_plus_waiter_topology();
        let pid = project.id.to_string();
        // Shared flag: the waiter flips it on resume; the creator polls it
        // on the bus and closes once set. (A plain Arc<AtomicBool> is the
        // cross-node signal; the bus just keeps the worker warm.)
        let resumed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let r_creator = resumed.clone();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(move |ctx| {
                let resumed = r_creator.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    // Hold the bus open until the waiter has resumed, then
                    // close so the execution can complete. This keeps the
                    // worker alive across the await + resume.
                    while !resumed.load(std::sync::atomic::Ordering::Acquire) {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    let mut cursor = bus.cursor();
                    // Stay co-alive until the creator closes the bus.
                    while cursor.next().await.expect("no FellBehind").is_some() {}
                    Ok(())
                })
            }),
        );
        let r_waiter = resumed.clone();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(move |ctx| {
                let resumed = r_waiter.clone();
                Box::pin(async move {
                    // Park on a signal WHILE the bus holds the worker.
                    let _ = ctx.await_signal(human_form()).await?;
                    // Resumed in-process: tell the creator to wrap up.
                    resumed.store(true, std::sync::atomic::Ordering::Release);
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, color) =
            spawn_run(project, &["creator", "waiter"], journal.clone(), tasks.clone());

        // Wait until the waiter's await registered (token minted), give it
        // a beat to reach the suspended state, then write the journal
        // rows the dispatcher would on a real fire: SuspensionRegistered
        // (the fold builds the awaited sequence from THIS) followed by
        // SuspensionResolved carrying the value. Both land while the
        // worker is alive (bus open); the in-loop resume poll picks them
        // up and resumes the waiter in process.
        let token = tasks.await_token().await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color,
                    node_id: "waiter".into(),
                    frames: vec![],
                    token: token.clone(),
                    spec: weft_core::signal::to_spec(human_form()),
                    call_index: 0,
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color,
                    token,
                    value: serde_json::json!({ "answer": 42 }),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: in-flight resume should complete on the live worker")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "the resume must happen in process on the bus-held worker and complete; got {outcome:?}"
        );
        // The journal must show the WAITER resumed (NodeResumed), proving
        // the resume happened on this live worker, not via a respawn.
        let events = journal.events.lock().unwrap().clone();
        assert!(
            events.iter().any(|e| matches!(e, ExecEvent::NodeResumed { node_id, .. } if node_id == "waiter")),
            "waiter must have a NodeResumed event (in-process resume); got {events:?}"
        );
    }

    /// TWO awaits on a live bus where the waiter is a KICKED entry node
    /// (no inbound pulses): it parks, resumes, parks again, resumes again,
    /// all in process on the bus-held worker. A kicked node re-fires via
    /// the `dispatched=false` reset (not the pulse un-absorb path), so
    /// this pins the multi-await-on-bus path for entry nodes. The pulse-
    /// fed variant that exercises the `pulses_absorbed` un-absorb across
    /// two resumes is `two_awaits_pulse_fed_waiter_both_resume`.
    #[tokio::test]
    async fn two_awaits_on_live_bus_both_resume_in_process() {
        let project = bus_plus_waiter_topology();
        let pid = project.id.to_string();
        // Counts how many times the waiter has resumed (0 -> 1 -> 2). The
        // creator holds the bus open until BOTH resumes have landed.
        let resumes = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let c_resumes = resumes.clone();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(move |ctx| {
                let resumes = c_resumes.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    // Hold the bus open until BOTH awaits have resumed.
                    while resumes.load(std::sync::atomic::Ordering::Acquire) < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    let mut cursor = bus.cursor();
                    while cursor.next().await.expect("no FellBehind").is_some() {}
                    Ok(())
                })
            }),
        );
        let w_resumes = resumes.clone();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(move |ctx| {
                let resumes = w_resumes.clone();
                Box::pin(async move {
                    // First park + resume.
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    // Second park + resume on the same live worker.
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, color) =
            spawn_run(project, &["creator", "waiter"], journal.clone(), tasks.clone());

        // Resolve the FIRST await (call_index 0).
        let token0 = tasks.await_token().await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token0.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 0, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token0.clone(),
                    value: serde_json::json!({ "answer": 1 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        // Wait for the SECOND await to register a NEW token (distinct from
        // the first), then resolve it (call_index 1). `await_token` returns
        // the most-recently-minted token, so poll until it changes.
        let token1 = loop {
            let t = tasks.await_token().await;
            if t != token0 {
                break t;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token1.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 1, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token1,
                    value: serde_json::json!({ "answer": 2 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: the SECOND in-place resume must re-fire the waiter")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "both resumes must land in process and the execution complete; got {outcome:?}"
        );
        // Exactly TWO NodeResumed for the waiter: one per await. A single
        // one would mean the second resume never fired (the hang).
        let events = journal.events.lock().unwrap().clone();
        let resumed_count = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeResumed { node_id, .. } if node_id == "waiter"))
            .count();
        assert_eq!(
            resumed_count, 2,
            "waiter must resume TWICE in process (one NodeResumed per await); got {resumed_count} in {events:?}"
        );
    }

    /// PULSE-FED waiter, two in-place resumes. The waiter has two required
    /// inbound ports (`in1`, `in2`). `feeder1` emits `in1` once, `feeder2`
    /// emits `in2` once before the first await, and `feeder3` emits a second
    /// `in2` pulse after the first resume. Unlike the kicked variant, this
    /// waiter re-fires through the `pulses_absorbed` UN-ABSORB path (it has
    /// inbound edges, so its resume cannot use the `dispatched=false` kick
    /// reset). It pins that the pulse-fed double-resume path completes and
    /// emits exactly two `NodeResumed` events.
    ///
    /// NOTE: this test does NOT pin the `is_resume` `pulses_absorbed`
    /// extension fix (it passes whether that loop is present or reverted).
    /// That extension keeps the live RAM record equal to a journal refold,
    /// but reverting it has no reachable behavioral effect: a re-fire
    /// un-absorbs the ORIGINAL `pulses_absorbed` recorded at `NodeStarted`,
    /// and the first fire already proved every wired port had a pulse there,
    /// so every wired port is always re-satisfied on every resume. A pulse a
    /// resume absorbs on top of that is never the sole satisfier of any
    /// port, so dropping it never starves a re-fire. The full engine suite
    /// passes with that loop reverted; the fix is a defensive RAM==refold
    /// consistency guard, not a fix for a reachable hang.
    #[tokio::test]
    async fn two_awaits_pulse_fed_waiter_both_resume() {
        let project = bus_plus_pulse_fed_waiter_topology();
        let pid = project.id.to_string();
        // 0 -> first await parked, 1 -> first resume done, 2 -> second
        // resume done. feeder2 uses it to time its second emit.
        let resumes = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let c_resumes = resumes.clone();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(move |ctx| {
                let resumes = c_resumes.clone();
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    while resumes.load(std::sync::atomic::Ordering::Acquire) < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    let mut cursor = bus.cursor();
                    while cursor.next().await.expect("no FellBehind").is_some() {}
                    Ok(())
                })
            }),
        );
        // feeder1: one pulse on in1, then done.
        install_body(
            &pid,
            "feeder1",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    ctx.pulse_downstream(NodeOutput::new().set("out", json!("a"))).await?;
                    Ok(())
                })
            }),
        );
        // feeder2: the ORIGINAL in2 pulse (before the first await).
        install_body(
            &pid,
            "feeder2",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    ctx.pulse_downstream(NodeOutput::new().set("out", json!("b1"))).await?;
                    Ok(())
                })
            }),
        );
        // feeder3: a SECOND in2 pulse, emitted only AFTER the first resume
        // (resumes >= 1). A port may be emitted at most once per firing, so
        // this must be a distinct node, not a re-emit by feeder2. This pulse
        // is the one the first resume dispatch absorbs; if the fix does not
        // record it into the waiter's live `pulses_absorbed`, the second
        // resume cannot re-satisfy `in2`.
        let f3_resumes = resumes.clone();
        install_body(
            &pid,
            "feeder3",
            std::sync::Arc::new(move |ctx| {
                let resumes = f3_resumes.clone();
                Box::pin(async move {
                    while resumes.load(std::sync::atomic::Ordering::Acquire) < 1 {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    ctx.pulse_downstream(NodeOutput::new().set("out", json!("b2"))).await?;
                    Ok(())
                })
            }),
        );
        let w_resumes = resumes.clone();
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(move |ctx| {
                let resumes = w_resumes.clone();
                Box::pin(async move {
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    let _ = ctx.await_signal(human_form()).await?;
                    resumes.fetch_add(1, std::sync::atomic::Ordering::Release);
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        // The waiter is pulse-fed, so it is NOT kicked: the feeders are the
        // kicked roots that produce its input pulses.
        let (handle, color) = spawn_run(
            project,
            &["creator", "feeder1", "feeder2", "feeder3"],
            journal.clone(),
            tasks.clone(),
        );

        // Resolve the FIRST await (call_index 0).
        let token0 = tasks.await_token().await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token0.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 0, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token0.clone(),
                    value: serde_json::json!({ "answer": 1 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        // Resolve the SECOND await (call_index 1) once its token appears.
        let token1 = loop {
            let t = tasks.await_token().await;
            if t != token0 {
                break t;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        };
        journal
            .record_event(
                &ExecEvent::SuspensionRegistered {
                    color, node_id: "waiter".into(), frames: vec![],
                    token: token1.clone(), spec: weft_core::signal::to_spec(human_form()),
                    call_index: 1, at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::SuspensionResolved {
                    color, token: token1,
                    value: serde_json::json!({ "answer": 2 }), at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: the SECOND in-place resume must re-fire the pulse-fed waiter")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "both resumes must land in process and the execution complete; got {outcome:?}"
        );
        let events = journal.events.lock().unwrap().clone();
        let resumed_count = events
            .iter()
            .filter(|e| matches!(e, ExecEvent::NodeResumed { node_id, .. } if node_id == "waiter"))
            .count();
        assert_eq!(
            resumed_count, 2,
            "pulse-fed waiter must resume TWICE in process; got {resumed_count} in {events:?}"
        );
    }

    /// BUS CLOSES BEFORE THE FIRE: the `creator`+`peer` bus conversation
    /// ends (bus closes) while the independent `waiter` is parked on
    /// `await_signal` and the fire has NOT arrived. Once the bus is gone
    /// nothing holds the worker, so the await must fall through to the
    /// normal stall -> exit path (the dispatcher respawns on the eventual
    /// fire). No `SuspensionResolved` is injected, proving the worker
    /// exits rather than waiting forever on the dead bus.
    #[tokio::test]
    async fn bus_closes_before_fire_then_worker_exits_normally() {
        let project = bus_plus_waiter_topology();
        let pid = project.id.to_string();
        install_body(
            &pid,
            "creator",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let (mut bus, marker) = ctx.create_bus(Default::default())?;
                    bus.register("creator").expect("register creator");
                    emit_bus_marker(&ctx, "ch", marker).await?;
                    bus.wait_for("peer").await.expect("peer joins");
                    // The conversation is over: close the bus. After this,
                    // nothing holds the worker, so the parked waiter must
                    // stall+exit (not hang on the dead bus).
                    bus.close();
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "peer",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let mut bus = ctx.bus_from_input("ch")?;
                    bus.register("peer").expect("register peer");
                    // Joined; exit immediately. The creator closes the bus.
                    Ok(())
                })
            }),
        );
        install_body(
            &pid,
            "waiter",
            std::sync::Arc::new(|ctx| {
                Box::pin(async move {
                    let _ = ctx.await_signal(human_form()).await?;
                    Ok(())
                })
            }),
        );

        let journal = Arc::new(MemJournal::default());
        let tasks = AwaitTasks::new();
        let (handle, _color) =
            spawn_run(project, &["creator", "waiter"], journal.clone(), tasks.clone());

        // No SuspensionResolved is injected. The worker must exit Stalled
        // (bus closed, nothing holds it, await unresolved) rather than
        // hang.
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), handle)
            .await
            .expect("must not hang: a closed bus must not hold the worker for an unresolved await")
            .expect("join")
            .expect("run ok");
        assert!(
            matches!(outcome, ExecutionOutcome::Stalled),
            "once the bus closed, the unresolved await must stall+exit for a respawn; got {outcome:?}"
        );
    }

    /// Two nodes: `producer` emits a value on `out` then RETURNS
    /// IMMEDIATELY (no work between the emit and the return); `consumer`
    /// has a REQUIRED input wired to it and emits nothing. The producer's
    /// emission and its terminal are sent back-to-back, so this is the
    /// exact shape that, with two separate task channels, raced: the
    /// terminal could be observed before the emission, the `out` port
    /// closed as "unmentioned", and the consumer SKIPPED (then re-
    /// dispatched). With one ordered task channel the emission always
    /// precedes the terminal, so the consumer must NEVER be skipped and
    /// must receive the value. Looped many times to surface any residual
    /// scheduling race.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn emit_then_immediately_return_never_skips_the_consumer() {
        fn producer_consumer_project() -> ProjectDefinition {
            serde_json::from_value(json!({
                "id": uuid::Uuid::new_v4(), "name": "emit-return", "description": null,
                "nodes": [
                    {
                        "id": "producer", "nodeType": "Configurable", "label": null, "config": null,
                        "position": { "x": 0.0, "y": 0.0 },
                        "inputs": [],
                        "outputs": [{ "name": "out", "portType": "String", "required": false }],
                        "features": {}, "scope": [], "groupBoundary": null,
                        "requiresInfra": false, "images": []
                    },
                    {
                        "id": "consumer", "nodeType": "Configurable", "label": null, "config": null,
                        "position": { "x": 1.0, "y": 0.0 },
                        "inputs": [{ "name": "in", "portType": "String", "required": true }],
                        "outputs": [], "features": {}, "scope": [], "groupBoundary": null,
                        "requiresInfra": false, "images": []
                    }
                ],
                "edges": [{
                    "id": "e0", "source": "producer", "target": "consumer",
                    "sourceHandle": "out", "targetHandle": "in"
                }],
                "groups": []
            }))
            .expect("project")
        }

        for i in 0..100 {
            let project = producer_consumer_project();
            let pid = project.id.to_string();
            install_body(
                &pid,
                "producer",
                std::sync::Arc::new(|ctx| {
                    Box::pin(async move {
                        // Emit then return with nothing in between: the
                        // emission and the terminal are sent back-to-back.
                        ctx.pulse_downstream(NodeOutput::new().set("out", json!("payload"))).await?;
                        Ok(())
                    })
                }),
            );
            install_body(
                &pid,
                "consumer",
                std::sync::Arc::new(|_ctx| Box::pin(async move { Ok(()) })),
            );

            let journal = Arc::new(MemJournal::default());
            let tasks = AwaitTasks::new();
            let (handle, color) = spawn_run(project, &["producer"], journal.clone(), tasks);
            tokio::time::timeout(std::time::Duration::from_secs(10), handle)
                .await
                .expect("must not hang")
                .expect("join")
                .expect("run ok");

            let events = journal.events_for_color(color).await.unwrap();
            // The consumer must NEVER be skipped: its required `in` arrived
            // as a real value, not a closure.
            let consumer_skipped = events.iter().any(|e| {
                matches!(e, ExecEvent::NodeSkipped { node_id, .. } if node_id == "consumer")
            });
            assert!(!consumer_skipped, "iter {i}: consumer was skipped (its emitted input was wrongly closed)");
            // And the consumer's firing must have seen the value on `in`.
            let consumer_got_value = events.iter().any(|e| match e {
                ExecEvent::NodeStarted { node_id, input, .. } if node_id == "consumer" => {
                    input.get("in").and_then(|v| v.as_str()) == Some("payload")
                }
                _ => false,
            });
            assert!(consumer_got_value, "iter {i}: consumer never received the producer's value on `in`");
        }
    }

    // ─── Live caller × other features (layer 3) ──────────────────────────
    //
    // These reuse the in-process `run_one_execution` harness above but wire
    // a `FakeCallerConnection` into the run (the last arg the bus test left
    // `None`), so a node's `ctx.caller()` resolves. They prove the live
    // caller coexists with the bus, with downstream pulses, with multiple
    // readers (broadcast), and that the disconnect lifetime axis behaves.

    use weft_core::caller::{
        CallerConnection, CallerHandle, CallerRuntimeConfig, FakeCallerConnection,
        InboundMessage, OutboundChunk,
    };
    use weft_core::signal::{Backpressure, DataType, ErrorMode, Protocol};
    use weft_core::wait::SuspendPolicy;

    /// Runtime config for a fake caller. `can_suspend = false` is the
    /// caller-tied default (disconnect cancels); `true` is the survives case.
    fn caller_cfg(protocol: Protocol, can_suspend: bool) -> CallerRuntimeConfig {
        CallerRuntimeConfig {
            protocol,
            data_type: DataType::Json,
            backpressure: Backpressure::Block,
            error_mode: ErrorMode::Surface,
            connect_timeout_secs: 5,
            max_inbound_bytes: 1_048_576,
            max_session_secs: 0,
            suspend: SuspendPolicy { can_suspend, default_hold_secs: 60 },
            inbound_window: weft_core::caller::DEFAULT_INBOUND_WINDOW,
        }
    }

    /// Seed a journal with ExecutionStarted(Fire) + a NodeKicked on
    /// `entry`, the minimal state to make a no-input node ready.
    async fn seed(journal: &MemJournal, color: Color, project: &ProjectDefinition, entry: &str) {
        journal
            .record_event(
                &ExecEvent::ExecutionStarted {
                    color,
                    project_id: project.id.to_string(),
                    entry_node: entry.into(),
                    phase: weft_core::context::Phase::Fire,
                    definition_hash: "test-hash".into(),
                    at_unix: 0,
                },
                None,
            )
            .await
            .unwrap();
        journal
            .record_event(
                &ExecEvent::NodeKicked { color, node_id: entry.into(), firing: false, payload: None, port_snapshot: None, at_unix: 0 },
                None,
            )
            .await
            .unwrap();
    }

    /// Run `project` to completion with `catalog` and an optional live
    /// caller wired in. Returns the outcome; the caller's call log is read
    /// off the `Arc<FakeCallerConnection>` the test still holds.
    async fn run_with_caller(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        journal: Arc<MemJournal>,
        color: Color,
        caller: Option<Arc<dyn CallerConnection>>,
    ) -> ExecutionOutcome {
        run_with_caller_tasks(project, catalog, journal, color, caller, Arc::new(NoopTasks)).await
    }

    /// As `run_with_caller` but with an explicit tasks client (await_signal
    /// nodes enqueue a RegisterSignal task, which `NoopTasks` rejects; those
    /// tests pass `AwaitTasks`).
    async fn run_with_caller_tasks(
        project: ProjectDefinition,
        catalog: Arc<dyn NodeCatalog>,
        journal: Arc<MemJournal>,
        color: Color,
        caller: Option<Arc<dyn CallerConnection>>,
        tasks: Arc<dyn weft_task_store::TaskStoreClient>,
    ) -> ExecutionOutcome {
        let clients = EngineClients {
            journal,
            tasks,
            infra: Arc::new(NoopInfra),
            infra_state: Arc::new(NoopInfraState),
            project: Arc::new(NoopProject),
            clock: Arc::new(weft_platform_traits::clock::SystemClock),
            storage: crate::storage::FakeWorkerStorage::new(),
            paid_calls: crate::context::FakePaidCallClient::new(),
            pending_costs: crate::metering::PendingCostRecords::new(),
        };
        run_one_execution(
            Arc::new(project),
            catalog,
            color,
            clients,
            "pod-test".into(),
            "tenant-test".into(),
            "ns-test".into(),
            CancellationFlag::new_arc(),
            caller,
        )
        .await
        .expect("run_one_execution ok")
    }

    /// A single-node project for `node_type` with no inputs and a `done`
    /// boolean output, the shape every caller test node uses as the entry.
    fn single_node_project(node_type: &str) -> ProjectDefinition {
        serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(),
            "name": "caller-test",
            "description": null,
            "nodes": [{
                "id": "entry", "nodeType": node_type, "label": null,
                "config": null, "position": { "x": 0.0, "y": 0.0 },
                "inputs": [],
                "outputs": [{ "name": "done", "portType": "Boolean", "required": false }],
                "features": {}, "scope": [], "groupBoundary": null,
                "requiresInfra": false, "images": []
            }],
            "edges": [],
            "groups": []
        }))
        .expect("single-node project")
    }

    // ----- Test nodes -------------------------------------------------------

    /// Reads every inbound WS message from the live caller and forwards each
    /// onto a bus it creates, then closes the bus. Proves caller + bus
    /// coexist in one firing. Exposes the bus on `channel` for a consumer.
    struct CallerToBus;
    test_manifest!(CallerToBus, "CallerToBus");
    #[async_trait]
    impl Node for CallerToBus {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
                return Err(weft_core::error::WeftError::NodeExecution("no ws caller".into()));
            };
            let (mut bus, marker) = ctx.create_bus(weft_core::bus::BusOptions::default())?;
            bus.register("caller_to_bus").expect("register");
            // Pulse the bus marker downstream FIRST so the consumer node
            // fires and registers, THEN wait for it (mirrors Producer).
            ctx.pulse_downstream(NodeOutput::new().set("channel", marker)).await?;
            bus.wait_for("drain").await.expect("consumer registers");
            // The test scripts inbound before the run, so read history from
            // the retained window start (a forward `ws.receive()` would only
            // see messages arriving after attach).
            let cursor = ws.cursor_from_start();
            loop {
                match cursor.receive().await {
                    Ok(InboundMessage::Json(v)) => {
                        bus.send("msg", v).expect("send to bus");
                    }
                    Ok(InboundMessage::Text(s)) => {
                        bus.send("msg", Value::String(s)).expect("send");
                    }
                    Ok(InboundMessage::Bytes(_)) => {}
                    Err(_) => break, // caller idle / gone: stop forwarding
                }
            }
            bus.close();
            Ok(())
        }
    }

    /// One WS reader that records every message it sees into a shared
    /// collector keyed by its own label. Two of these on one caller prove
    /// inbound is broadcast (both see every message, neither steals).
    struct WsReader {
        label: &'static str,
        seen: Arc<StdMutex<Vec<(&'static str, Value)>>>,
    }
    test_manifest!(WsReader, "WsReader");
    #[async_trait]
    impl Node for WsReader {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
                return Err(weft_core::error::WeftError::NodeExecution("no ws caller".into()));
            };
            // Test scripts inbound before the run; each reader reads history
            // from the window start on its OWN cursor (broadcast: both
            // readers see every message, neither steals).
            let cursor = ws.cursor_from_start();
            loop {
                match cursor.receive().await {
                    Ok(InboundMessage::Json(v)) => {
                        self.seen.lock().unwrap().push((self.label, v));
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            ctx.pulse_downstream(NodeOutput::new().set("done", Value::Bool(true))).await
        }
    }

    /// HTTP responder: streams one chunk, sends the final body, AND pulses
    /// `done` downstream, proving talk-to-caller and graph emission compose.
    struct HttpResponder;
    test_manifest!(HttpResponder, "HttpResponder");
    #[async_trait]
    impl Node for HttpResponder {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let Some(CallerHandle::Http(http)) = ctx.caller() else {
                return Err(weft_core::error::WeftError::NodeExecution("no http caller".into()));
            };
            http.write(OutboundChunk::Json(json!({ "stage": "working" }))).await?;
            http.respond(OutboundChunk::Json(json!({ "stage": "done" }))).await?;
            ctx.pulse_downstream(NodeOutput::new().set("done", Value::Bool(true))).await
        }
    }

    /// Sends one message to the caller. On a caller-tied run a disconnect
    /// makes the send error (cancel); on a survives run it is a no-op and
    /// the node still completes. Distinguishes the two lifetime regimes.
    struct CallerSender;
    test_manifest!(CallerSender, "CallerSender");
    #[async_trait]
    impl Node for CallerSender {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
                return Err(weft_core::error::WeftError::NodeExecution("no ws caller".into()));
            };
            // Propagate the send result: a caller-tied disconnect surfaces
            // as an error (-> cancel); a survives disconnect is Ok (void).
            ws.send(OutboundChunk::Json(json!({ "hi": true }))).await?;
            ctx.pulse_downstream(NodeOutput::new().set("done", Value::Bool(true))).await
        }
    }

    /// Requires a caller and fails loud when there is none. Models a node
    /// wired under a live trigger but run on a caller-less execution.
    struct NeedsCaller;
    test_manifest!(NeedsCaller, "NeedsCaller");
    #[async_trait]
    impl Node for NeedsCaller {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            if ctx.caller().is_none() {
                return Err(weft_core::error::WeftError::NodeExecution(
                    "NeedsCaller ran without a live caller".into(),
                ));
            }
            ctx.pulse_downstream(NodeOutput::new().set("done", Value::Bool(true))).await
        }
    }

    /// One-node catalog over a single boxed node, for the single-node
    /// caller projects.
    struct OneNodeCatalog {
        node: &'static dyn Node,
    }
    impl NodeCatalog for OneNodeCatalog {
        fn lookup(&self, node_type: &str) -> Option<&'static dyn Node> {
            if node_type == self.node.node_type() { Some(self.node) } else { None }
        }
        fn all(&self) -> Vec<&'static str> { vec![self.node.node_type()] }
    }

    // ----- Tests ------------------------------------------------------------

    /// Caller × bus, asserting on the journaled bus stream: the three
    /// inbound caller messages each became a bus message, in order.
    #[tokio::test]
    async fn caller_to_bus_journals_each_message() {
        // CallerToBus forwards onto a bus; a Drainer consumer registers
        // "drain" (releasing CallerToBus's wait_for) and drains so the run
        // reaches quiescence.
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "caller-bus2", "description": null,
            "nodes": [
                { "id": "producer", "nodeType": "CallerToBus", "label": null, "config": null,
                  "position": {"x":0.0,"y":0.0}, "inputs": [],
                  "outputs": [{ "name": "channel", "portType": "Bus", "required": false },
                              { "name": "done", "portType": "Boolean", "required": false }],
                  "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "drain", "nodeType": "Drainer", "label": null, "config": null,
                  "position": {"x":1.0,"y":0.0},
                  "inputs": [{ "name": "channel", "portType": "Bus", "required": true }],
                  "outputs": [], "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
            ],
            "edges": [{ "id": "e", "source": "producer", "target": "drain", "sourceHandle": "channel", "targetHandle": "channel" }],
            "groups": []
        })).expect("project");

        // Drainer registers "drain" (releasing CallerToBus's wait_for) and
        // drains every message so the run reaches quiescence.
        struct Drainer;
        test_manifest!(Drainer, "Drainer");
        #[async_trait]
        impl Node for Drainer {
            async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
                let mut bus = ctx.bus_from_input("channel")?;
                bus.register("drain").expect("register drain");
                let mut cursor = bus.cursor();
                while cursor.next().await.expect("no fellbehind").is_some() {}
                Ok(())
            }
        }
        struct TwoCatalog { a: &'static CallerToBus, b: &'static Drainer }
        impl NodeCatalog for TwoCatalog {
            fn lookup(&self, t: &str) -> Option<&'static dyn Node> {
                match t { "CallerToBus" => Some(self.a), "Drainer" => Some(self.b), _ => None }
            }
            fn all(&self) -> Vec<&'static str> { vec!["CallerToBus", "Drainer"] }
        }
        let catalog: Arc<dyn NodeCatalog> = Arc::new(TwoCatalog {
            a: Box::leak(Box::new(CallerToBus)),
            b: Box::leak(Box::new(Drainer)),
        });

        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "producer").await;
        let fake = FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, false));
        for i in 0..3 { fake.push_inbound(InboundMessage::Json(json!({ "i": i }))); }

        let outcome = run_with_caller(
            project, catalog, journal.clone(), color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");

        let msgs: Vec<i64> = journal.events.lock().unwrap().iter().filter_map(|e| match e {
            ExecEvent::BusMessage { from, payload, .. } if from == "caller_to_bus" =>
                payload.value().and_then(|v| v.get("i")).and_then(|v| v.as_i64()),
            _ => None,
        }).collect();
        assert_eq!(msgs, vec![0, 1, 2], "all three caller messages journaled onto the bus in order");
    }

    /// Broadcast: two reader nodes on ONE caller both see every inbound
    /// message (per-reader cursor), neither steals from the other. This is
    /// the in-process version of the cluster broadcast check.
    #[tokio::test]
    async fn inbound_broadcasts_to_two_nodes() {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        // Two readers wrapped so they have distinct node_type strings; each
        // delegates to a shared `WsReader` body with its own label.
        struct ReaderA(WsReader);
        struct ReaderB(WsReader);
        test_manifest!(ReaderA, "ReaderA");
        #[async_trait]
        impl Node for ReaderA {
            async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> { self.0.run(ctx).await }
        }
        test_manifest!(ReaderB, "ReaderB");
        #[async_trait]
        impl Node for ReaderB {
            async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> { self.0.run(ctx).await }
        }
        struct AB { a: &'static ReaderA, b: &'static ReaderB }
        impl NodeCatalog for AB {
            fn lookup(&self, t: &str) -> Option<&'static dyn Node> {
                match t { "ReaderA" => Some(self.a), "ReaderB" => Some(self.b), _ => None }
            }
            fn all(&self) -> Vec<&'static str> { vec!["ReaderA", "ReaderB"] }
        }
        let catalog: Arc<dyn NodeCatalog> = Arc::new(AB {
            a: Box::leak(Box::new(ReaderA(WsReader { label: "A", seen: seen.clone() }))),
            b: Box::leak(Box::new(ReaderB(WsReader { label: "B", seen: seen.clone() }))),
        });
        // Both readers kicked at start (two entry nodes, no edges).
        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "broadcast", "description": null,
            "nodes": [
                { "id": "ra", "nodeType": "ReaderA", "label": null, "config": null,
                  "position": {"x":0.0,"y":0.0}, "inputs": [],
                  "outputs": [{ "name": "done", "portType": "Boolean", "required": false }],
                  "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] },
                { "id": "rb", "nodeType": "ReaderB", "label": null, "config": null,
                  "position": {"x":1.0,"y":0.0}, "inputs": [],
                  "outputs": [{ "name": "done", "portType": "Boolean", "required": false }],
                  "features": {}, "scope": [], "groupBoundary": null, "requiresInfra": false, "images": [] }
            ],
            "edges": [], "groups": []
        })).expect("broadcast project");

        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        // Kick both reader nodes.
        journal.record_event(&ExecEvent::ExecutionStarted {
            color, project_id: project.id.to_string(), entry_node: "ra".into(),
            phase: weft_core::context::Phase::Fire, definition_hash: "h".into(), at_unix: 0,
        }, None).await.unwrap();
        for n in ["ra", "rb"] {
            journal.record_event(&ExecEvent::NodeKicked {
                color, node_id: n.into(), firing: false, payload: None, port_snapshot: None, at_unix: 0,
            }, None).await.unwrap();
        }
        let fake = FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, false));
        for i in 0..2 { fake.push_inbound(InboundMessage::Json(json!({ "i": i }))); }

        let outcome = run_with_caller(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");

        let got = seen.lock().unwrap().clone();
        let a_count = got.iter().filter(|(l, _)| *l == "A").count();
        let b_count = got.iter().filter(|(l, _)| *l == "B").count();
        assert_eq!(a_count, 2, "reader A saw both messages (broadcast, not stolen): {got:?}");
        assert_eq!(b_count, 2, "reader B saw both messages (broadcast, not stolen): {got:?}");
    }

    /// HTTP respond + downstream pulse in one firing: the node talks to the
    /// caller AND emits `done`, both happen, run completes.
    #[tokio::test]
    async fn http_respond_and_pulse_compose() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(HttpResponder)) });
        let project = single_node_project("HttpResponder");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;
        let fake = FakeCallerConnection::connected(caller_cfg(Protocol::Http, false));

        let outcome = run_with_caller(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");

        // The node both wrote a chunk and terminated with the final body.
        use weft_core::caller::CallerCall;
        let calls = fake.calls();
        assert!(calls.iter().any(|c| matches!(c, CallerCall::SendChunk(_))), "wrote a streaming chunk: {calls:?}");
        assert!(calls.iter().any(|c| matches!(c, CallerCall::Terminate(_))), "sent the final body / terminated: {calls:?}");
    }

    /// Caller-tied (can_suspend = false): a node that sends to a
    /// disconnected caller surfaces an error, which cancels the execution.
    #[tokio::test]
    async fn caller_tied_disconnect_cancels() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(CallerSender)) });
        let project = single_node_project("CallerSender");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;
        // Caller-tied AND already disconnected: the send must error -> cancel.
        let fake = FakeCallerConnection::disconnected(caller_cfg(Protocol::Websocket, false));

        let outcome = run_with_caller(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(
            matches!(outcome, ExecutionOutcome::Cancelled | ExecutionOutcome::Failed { .. }),
            "a caller-tied run whose send hits a gone caller must not Complete; got {outcome:?}"
        );
    }

    /// Survives (can_suspend = true): a send to a disconnected caller is a
    /// no-op into the void; the node still completes normally.
    #[tokio::test]
    async fn survives_disconnect_continues() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(CallerSender)) });
        let project = single_node_project("CallerSender");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;
        let fake = FakeCallerConnection::disconnected(caller_cfg(Protocol::Websocket, true));

        let outcome = run_with_caller(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(
            matches!(outcome, ExecutionOutcome::Completed { .. }),
            "a survives run continues past a gone caller; got {outcome:?}"
        );
    }

    /// No caller (a durable run): `ctx.caller()` is None and a node that
    /// requires one fails loud rather than silently no-op'ing.
    #[tokio::test]
    async fn no_caller_run_fails_loud_when_required() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(NeedsCaller)) });
        let project = single_node_project("NeedsCaller");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;

        // No caller wired (None): the durable-run case.
        let outcome = run_with_caller(project, catalog, journal, color, None).await;
        assert!(
            matches!(outcome, ExecutionOutcome::Failed { .. }),
            "a node requiring a caller must fail loud on a caller-less run; got {outcome:?}"
        );
    }

    // ----- await_signal × caller (hold-then-kill) + non-durability -------

    /// Caller config with an explicit short hold, for the suspension tests
    /// (the caller-tied hold is real wall-time, so keep it ~1s).
    fn caller_cfg_hold(can_suspend: bool, hold_secs: u64) -> CallerRuntimeConfig {
        let mut c = caller_cfg(Protocol::Websocket, can_suspend);
        c.suspend.default_hold_secs = hold_secs;
        c
    }

    /// A node that parks on `await_signal` (a human form), the durable-wait
    /// primitive. Its interaction with a live caller is the whole point: a
    /// caller-tied run must NOT durably suspend here.
    struct AwaiterNode;
    test_manifest!(AwaiterNode, "Awaiter");
    #[async_trait]
    impl Node for AwaiterNode {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let _ = ctx.await_signal(human_form()).await?;
            ctx.pulse_downstream(NodeOutput::new().set("done", Value::Bool(true))).await
        }
    }

    /// Caller-tied (can_suspend = false) run hits a durable `await_signal`
    /// with no resolving signal arriving: it holds the worker warm for the
    /// hold window, then is KILLED (cancelled), because a tied run cannot
    /// degrade into a caller-less background job. This is ALSO the
    /// non-durability proof: a live tied run does not produce a resumable
    /// suspension.
    #[tokio::test]
    async fn caller_tied_run_at_await_signal_is_killed_not_suspended() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(AwaiterNode)) });
        let project = single_node_project("Awaiter");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;
        // Tied + connected + a 1s hold: no fire ever arrives, so it must
        // cancel after the hold (not Stall, not Complete).
        let fake = FakeCallerConnection::connected(caller_cfg_hold(false, 1));

        let outcome = run_with_caller_tasks(
            project, catalog, journal.clone(), color,
            Some(fake.clone() as Arc<dyn CallerConnection>), AwaitTasks::new(),
        ).await;
        assert!(
            matches!(outcome, ExecutionOutcome::Cancelled),
            "a caller-tied run at a durable wait with no fire must be killed, not suspended; got {outcome:?}"
        );
        // Non-durability: the terminal journal event is a cancellation, NOT
        // a clean suspension that a later fire could resume.
        let has_cancel = journal.events.lock().unwrap().iter().any(|e| matches!(
            e, ExecEvent::ExecutionCancelled { color: c, .. } if *c == color));
        assert!(has_cancel, "tied live run must journal a cancellation (non-durable)");
    }

    /// Survivable (can_suspend = true) run hits the SAME durable wait and
    /// cleanly STALLS (suspends): the worker exits, a later fire resumes it
    /// caller-less. The contrast with the tied case above is the lifetime
    /// axis doing its job.
    #[tokio::test]
    async fn survivable_run_at_await_signal_suspends_cleanly() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(AwaiterNode)) });
        let project = single_node_project("Awaiter");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;
        let fake = FakeCallerConnection::connected(caller_cfg_hold(true, 1));

        let outcome = run_with_caller_tasks(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>), AwaitTasks::new(),
        ).await;
        assert!(
            matches!(outcome, ExecutionOutcome::Stalled),
            "a survivable run durably suspends at await_signal; got {outcome:?}"
        );
    }

    // ----- infra × caller ------------------------------------------------

    /// A node that talks to the live caller AND resolves an infra endpoint
    /// in the same firing. With no infra (NoopInfra), endpoint resolution
    /// errors; the node handles it gracefully AFTER the caller talk, proving
    /// the two paths coexist without interfering. The caller send is
    /// recorded regardless of the infra outcome.
    struct CallerPlusEndpoint;
    test_manifest!(CallerPlusEndpoint, "CallerPlusEndpoint");
    #[async_trait]
    impl Node for CallerPlusEndpoint {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
                return Err(weft_core::error::WeftError::NodeExecution("no ws caller".into()));
            };
            // Talk to the caller first.
            ws.send(OutboundChunk::Json(json!({ "hi": true }))).await?;
            // Then attempt to resolve an endpoint; with no infra this errors,
            // which the node tolerates (the point is the two paths compose).
            let _ = ctx.endpoint("api").await; // Err under NoopInfra; ignored.
            ctx.pulse_downstream(NodeOutput::new().set("done", Value::Bool(true))).await
        }
    }

    #[tokio::test]
    async fn caller_and_endpoint_resolution_compose() {
        let catalog: Arc<dyn NodeCatalog> =
            Arc::new(OneNodeCatalog { node: Box::leak(Box::new(CallerPlusEndpoint)) });
        let project = single_node_project("CallerPlusEndpoint");
        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        seed(&journal, color, &project, "entry").await;
        let fake = FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, false));

        let outcome = run_with_caller(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");
        use weft_core::caller::CallerCall;
        assert!(
            fake.calls().iter().any(|c| matches!(c, CallerCall::SendChunk(_))),
            "the caller send happened even though the endpoint path ran too: {:?}", fake.calls()
        );
    }

    // ----- loop × caller -------------------------------------------------

    /// A loop body node that, on each iteration, receives one message from
    /// the live caller and emits it on the gather `out` port. Proves
    /// `ctx.caller()` resolves for a firing INSIDE a loop frame (non-empty
    /// frame stack), and the caller is the SAME one across iterations.
    struct LoopBodyCaller;
    test_manifest!(LoopBodyCaller, "LoopBodyCaller");
    #[async_trait]
    impl Node for LoopBodyCaller {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let Some(CallerHandle::Websocket(ws)) = ctx.caller() else {
                return Err(weft_core::error::WeftError::NodeExecution("no ws caller in loop body".into()));
            };
            // The test scripts inbound before the run; read history from the
            // window start. Each iteration mints an INDEPENDENT cursor from
            // the floor, so every iteration sees the first retained message
            // (broadcast semantics: a cursor is its own reader, not a shared
            // queue) -- the property under test.
            let msg = match ws.cursor_from_start().receive().await {
                Ok(InboundMessage::Json(v)) => v,
                Ok(InboundMessage::Text(s)) => Value::String(s),
                _ => Value::Null,
            };
            ctx.pulse_downstream(NodeOutput::new().set("out", msg)).await
        }
    }

    /// Entry node that emits the two-item list onto `items`, feeding LoopIn
    /// (a loop is never the entry; an upstream node supplies the `over` list).
    struct ListSource;
    test_manifest!(ListSource, "ListSource");
    #[async_trait]
    impl Node for ListSource {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            ctx.pulse_downstream(NodeOutput::new().set("items", json!(["a", "b"]))).await
        }
    }

    /// Sink collecting the loop's gathered `List[String|Null]` output.
    struct GatherSink { got: Arc<StdMutex<Option<Value>>> }
    test_manifest!(GatherSink, "GatherSink");
    #[async_trait]
    impl Node for GatherSink {
        async fn run(&self, ctx: ExecutionContext) -> WeftResult<()> {
            let data = ctx.ports.raw("data").cloned().unwrap_or(Value::Null);
            *self.got.lock().unwrap() = Some(data);
            Ok(())
        }
    }

    #[tokio::test]
    async fn caller_resolves_inside_loop_body_frames() {
        // Sequential loop over two items; the body reads the caller once per
        // iteration. LoopIn/LoopOut are built-in (handled inline by the
        // driver, not the catalog), so the catalog only needs the body+sink.
        let got = Arc::new(StdMutex::new(None));
        struct LoopCat { src: &'static ListSource, body: &'static LoopBodyCaller, sink: &'static GatherSink }
        impl NodeCatalog for LoopCat {
            fn lookup(&self, t: &str) -> Option<&'static dyn Node> {
                match t {
                    "ListSource" => Some(self.src),
                    "LoopBodyCaller" => Some(self.body),
                    "GatherSink" => Some(self.sink),
                    _ => None,
                }
            }
            fn all(&self) -> Vec<&'static str> { vec!["ListSource", "LoopBodyCaller", "GatherSink"] }
        }
        let catalog: Arc<dyn NodeCatalog> = Arc::new(LoopCat {
            src: Box::leak(Box::new(ListSource)),
            body: Box::leak(Box::new(LoopBodyCaller)),
            sink: Box::leak(Box::new(GatherSink { got: got.clone() })),
        });

        let project: ProjectDefinition = serde_json::from_value(json!({
            "id": uuid::Uuid::new_v4(), "name": "loop-caller", "description": null,
            "nodes": [
                { "id": "src", "nodeType": "ListSource", "label": null, "config": null,
                  "position": {"x":-1.0,"y":0.0}, "inputs": [],
                  "outputs": [{ "name": "items", "portType": "List[String]", "required": false }],
                  "scope": [], "groupBoundary": null, "features": {}, "requiresInfra": false, "images": [] },
                { "id": "g__in", "nodeType": "LoopIn", "label": null,
                  "config": { "parentId": "g", "parallel": false, "over": ["items"], "carry": [] },
                  "position": {"x":0.0,"y":0.0},
                  "groupBoundary": { "groupId": "g", "role": "In" },
                  "inputs": [{ "name": "items", "portType": "List[String]", "required": true }],
                  "outputs": [
                      { "name": "items", "portType": "String", "required": false },
                      { "name": "index", "portType": "Number", "required": false }
                  ],
                  "scope": [], "features": {}, "requiresInfra": false, "images": [] },
                { "id": "body", "nodeType": "LoopBodyCaller", "label": null, "config": null,
                  "position": {"x":1.0,"y":0.0},
                  "inputs": [{ "name": "in", "portType": "String", "required": true }],
                  "outputs": [{ "name": "out", "portType": "String", "required": false }],
                  "scope": ["g"], "groupBoundary": null, "features": {}, "requiresInfra": false, "images": [] },
                { "id": "g__out", "nodeType": "LoopOut", "label": null,
                  "config": { "parentId": "g" },
                  "position": {"x":2.0,"y":0.0},
                  "groupBoundary": { "groupId": "g", "role": "Out" },
                  "inputs": [
                      { "name": "results", "portType": "String", "required": false },
                      { "name": "done", "portType": "Boolean", "required": false }
                  ],
                  "outputs": [{ "name": "results", "portType": "List[String | Null]", "required": false }],
                  "scope": [], "features": {}, "requiresInfra": false, "images": [] },
                { "id": "sink", "nodeType": "GatherSink", "label": null, "config": null,
                  "position": {"x":3.0,"y":0.0},
                  "inputs": [{ "name": "data", "portType": "List[String | Null]", "required": true }],
                  "outputs": [], "scope": [], "groupBoundary": null, "features": {}, "requiresInfra": false, "images": [] }
            ],
            "edges": [
                { "id": "e0", "source": "src", "sourceHandle": "items", "target": "g__in", "targetHandle": "items" },
                { "id": "e1", "source": "g__in", "sourceHandle": "items", "target": "body", "targetHandle": "in" },
                { "id": "e2", "source": "body", "sourceHandle": "out", "target": "g__out", "targetHandle": "results" },
                { "id": "e3", "source": "g__out", "sourceHandle": "results", "target": "sink", "targetHandle": "data" }
            ],
            "groups": []
        })).expect("loop-caller project");

        let color = uuid::Uuid::new_v4();
        let journal = Arc::new(MemJournal::default());
        // Kick the entry ListSource; it pulses the two-item list onto LoopIn.
        seed(&journal, color, &project, "src").await;

        let fake = FakeCallerConnection::connected(caller_cfg(Protocol::Websocket, false));
        fake.push_inbound(InboundMessage::Json(json!("from-iter-0")));
        fake.push_inbound(InboundMessage::Json(json!("from-iter-1")));

        let outcome = run_with_caller(
            project, catalog, journal, color,
            Some(fake.clone() as Arc<dyn CallerConnection>),
        ).await;
        assert!(matches!(outcome, ExecutionOutcome::Completed { .. }), "got {outcome:?}");

        // One gathered entry per iteration, and every entry is a real
        // message read from the caller. The point: `ctx.caller()` resolved
        // for a firing INSIDE a loop frame, on every iteration. Note the
        // broadcast semantics: each iteration's `ctx.caller()` is an
        // independent reader with its own cursor from offset 0, so both read
        // the FIRST message ("from-iter-0") rather than consuming
        // sequentially. That is the documented model (a reader sees the whole
        // stream from its start), not a per-iteration queue; sequential
        // draining would need one handle threaded across iterations, which
        // the loop's separate firings do not do.
        let gathered = got.lock().unwrap().clone().expect("sink got the gather");
        let arr = gathered.as_array().expect("gather is a list");
        assert_eq!(arr.len(), 2, "two iterations gathered: {gathered:?}");
        assert!(
            arr.iter().all(|v| v.as_str() == Some("from-iter-0")),
            "every loop iteration resolved the caller and read its first broadcast message \
             (independent cursor per firing); got {gathered:?}"
        );
    }

