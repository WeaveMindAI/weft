//! Layer-3 contract tests for the raw-pipe serving side: the real
//! listener code (registration, the stream engine, framing, the
//! connect dialogue, the fire pattern) wired against hand-rolled
//! fakes of its I/O: a fake broker (the listener-resolve route
//! answering the connection's values), a fake peer (a real
//! in-process TCP server speaking a line dialogue), and a fake task
//! store recording the fires the sink enqueued.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use weft_core::signal::{to_spec, Framing, SocketFrame, StreamListen};
use weft_core::Access;
use weft_listener::kinds::{register_in_registry, RoutingSource, SignalIdentity};
use weft_listener::registry::Registry;
use weft_listener::ListenerConfig;

// ---------- Fakes ----------

/// Records every fire the listener enqueued; no business logic.
struct FakeTasks {
    enqueued: Mutex<Vec<weft_task_store::tasks::NewTask>>,
}

#[async_trait::async_trait]
impl weft_task_store::TaskStoreClient for FakeTasks {
    async fn enqueue_dedup(
        &self,
        spec: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
        let id = uuid::Uuid::new_v4();
        self.enqueued.lock().unwrap().push(spec);
        Ok(weft_task_store::tasks::DedupOutcome::Inserted(id))
    }
    async fn wait_for_terminal(
        &self,
        _: uuid::Uuid,
        _: std::time::Duration,
        _: std::time::Duration,
    ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
        unreachable!("not used by the serving side")
    }
    async fn claim_one(
        &self,
        _: &str,
        _: weft_task_store::tasks::ClaimFilter,
    ) -> anyhow::Result<Option<weft_task_store::tasks::Task>> {
        unreachable!()
    }
    async fn heartbeat(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<bool> {
        unreachable!()
    }
    async fn requeue(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<bool> {
        unreachable!()
    }
    async fn complete(&self, _: uuid::Uuid, _: &str, _: Value) -> anyhow::Result<()> {
        unreachable!()
    }
    async fn fail(&self, _: uuid::Uuid, _: &str, _: String) -> anyhow::Result<()> {
        unreachable!()
    }
}

/// One in-process line-protocol peer: greets on connect, answers the
/// dialogue lines the test expects, records everything received, and
/// pushes test lines down the held pipe on demand.
struct FakePeer {
    /// Lines the LISTENER sent down the pipe.
    received: Arc<Mutex<Vec<String>>>,
    address: String,
    /// Send lines down the held pipe (newline appended).
    downlink: tokio::sync::mpsc::Sender<String>,
}

async fn spawn_peer() -> FakePeer {
    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(16);
    let rx = Arc::new(tokio::sync::Mutex::new(rx));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let received_srv = received.clone();
    tokio::spawn(async move {
        // Serve every connection cycle (the engine reconnects), each
        // draining the shared downlink.
        while let Ok((stream, _)) = listener.accept().await {
            let (read_half, mut write) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            let rx = rx.clone();
            let received = received_srv.clone();
            tokio::spawn(async move {
                // The peer speaks first, like a real IMAP server.
                if write.write_all(b"* OK ready\r\n").await.is_err() {
                    return;
                }
                loop {
                    let mut guard = rx.lock().await;
                    tokio::select! {
                        line = guard.recv() => {
                            drop(guard);
                            let Some(line) = line else { break };
                            if write.write_all(format!("{line}\r\n").as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        up = lines.next_line() => {
                            drop(guard);
                            match up {
                                Ok(Some(line)) => {
                                    // Answer the dialogue like the
                                    // protocol would.
                                    let answer = if line.starts_with("a1 LOGIN") {
                                        Some("a1 OK signed in")
                                    } else if line == "a2 IDLE" {
                                        Some("+ idling")
                                    } else {
                                        None
                                    };
                                    received.lock().unwrap().push(line);
                                    if let Some(answer) = answer {
                                        if write
                                            .write_all(format!("{answer}\r\n").as_bytes())
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                }
                                _ => break,
                            }
                        }
                    }
                }
            });
        }
    });
    FakePeer { received, address: addr.to_string(), downlink: tx }
}

/// The fake broker: only the listener-resolve route, answering the
/// mailbox connection's values.
async fn spawn_broker() -> String {
    use axum::routing::post;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/v1/access/listener-resolve",
        post(|axum::Json(req): axum::Json<Value>| async move {
            assert_eq!(req["service"], "email");
            axum::Json(json!({
                "values": { "user": "q@acme.com", "password": "s3cret" },
                "auth": [],
                "events": {},
                "recipe_values": {}
            }))
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

async fn wait_until(mut check: impl FnMut() -> bool, what: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !check() {
        if std::time::Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

// The whole raw-pipe path: register the signal, watch the listener
// resolve the connection, run the interpolated dialogue against the
// peer, and fire ONLY the lines matching the fire pattern, with the
// dialogue's own chatter staying quiet.
//
// Stress-looped by construction (spawned tasks + channels + a
// multi-thread runtime): each iteration runs a fully isolated
// scenario (its own peer, broker, and token), so concurrent runs
// contend on the scheduler, never on each other's state.
weft_core::stress_test!(
    name: a_stream_signal_serves_end_to_end,
    runs: 6,
    worker_threads: 4,
    async fn body() {
        run_scenario().await;
    }
);

async fn run_scenario() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let sig_token = format!("sig-{run_id}");
    let peer = spawn_peer().await;
    let broker_base = spawn_broker().await;

    let tasks = Arc::new(FakeTasks { enqueued: Mutex::new(Vec::new()) });
    let registry = Arc::new(Registry::new());
    let config = Arc::new(ListenerConfig {
        pod_name: format!("test-pod-{run_id}"),
        http_port: 0,
        broker_url: broker_base.clone(),
    });
    let token_path = std::env::temp_dir().join(format!("weft-test-token-{}", uuid::Uuid::new_v4()));
    std::fs::write(&token_path, "test-token").unwrap();
    let events_broker = weft_broker_client::BrokerEventsClient::new(
        broker_base,
        weft_broker_client::TokenSource::new(token_path),
    );

    let access = Access::new(uuid::Uuid::new_v4().to_string(), "email", None);
    let mut kind = StreamListen::new(
        &peer.address,
        Framing::Delimiter { bytes: "\r\n".into() },
        r"^\* \d+ (EXISTS|RECENT)",
    )
    .with_access(&access)
    .step(
        SocketFrame::Text { body: "a1 LOGIN {user} {password}\r\n".into() },
        "^a1 OK",
    )
    .step(SocketFrame::Text { body: "a2 IDLE\r\n".into() }, r"^\+");
    // The peer is plaintext: an in-process pipe has no certificate.
    kind.tls = false;

    register_in_registry(
        SignalIdentity {
            token: sig_token.clone(),
            tenant_id: "tenant-a".into(),
            node_id: "node-1".into(),
            is_resume: false,
            color: None,
            placement_generation: 7,
            spec: to_spec(kind),
        },
        RoutingSource::Restore {
            routing: weft_core::primitive::SignalRouting {
                surface: weft_core::primitive::SignalSurface::Internal,
                auth: weft_core::primitive::SignalAuth::None,
                auth_config: Value::Null,
            },
            kind_state: json!({}),
            seq: 0,
        },
        registry.clone(),
        weft_listener::fire_sink::FireSignalSink::new(tasks.clone()),
        config,
        events_broker,
    )
    .await
    .expect("registration succeeds");

    // The dialogue ran with the connection's values interpolated.
    wait_until(|| peer.received.lock().unwrap().len() >= 2, "the dialogue").await;
    {
        let received = peer.received.lock().unwrap();
        assert_eq!(received[0], "a1 LOGIN q@acme.com s3cret");
        assert_eq!(received[1], "a2 IDLE");
    }

    // A line matching the fire pattern fires; the greeting and the
    // dialogue answers never did (nothing enqueued yet).
    assert!(
        tasks.enqueued.lock().unwrap().is_empty(),
        "dialogue chatter must not fire"
    );
    peer.downlink.send("* 4 EXISTS".into()).await.unwrap();
    wait_until(|| !tasks.enqueued.lock().unwrap().is_empty(), "the fire").await;
    {
        let fires = tasks.enqueued.lock().unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].payload["payload"], Value::String("* 4 EXISTS".into()));
        assert_eq!(fires[0].tenant_id.as_deref(), Some("tenant-a"));
        assert_eq!(fires[0].payload["token"], Value::String(sig_token.clone()));
        assert_eq!(fires[0].payload["placement_generation"], 7);
    }

    // A steady-state line that does not match stays quiet.
    peer.downlink.send("* OK still here".into()).await.unwrap();
    peer.downlink.send("* 5 RECENT".into()).await.unwrap();
    wait_until(|| tasks.enqueued.lock().unwrap().len() >= 2, "the second fire").await;
    assert_eq!(
        tasks.enqueued.lock().unwrap().len(),
        2,
        "the non-matching line must not have fired"
    );
}
