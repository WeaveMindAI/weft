//! Layer-3 contract tests for the raw-pipe serving side: the real
//! listener code (registration, the stream engine, framing, the
//! connect dialogue, the fire pattern) wired against hand-rolled
//! fakes of its I/O: a fake broker (the listener-resolve and infra-address routes
//! answering the connection's values), a fake peer (a real
//! in-process TCP server speaking a line dialogue), and a fake task
//! store recording the fires the sink enqueued.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use weft_core::signal::{to_spec, Framing, SocketFrame, StreamListen};
use weft_core::Access;
use weft_listener::kinds::bring_up;
use weft_listener::ListenerState;
use weft_listener::ListenerConfig;

// ---------- Fakes ----------

/// The task store: an entry's event never becomes a task, so nothing
/// here is asked to enqueue one.
struct FakeTasks;

#[async_trait::async_trait]
impl weft_task_store::TaskStoreClient for FakeTasks {

    async fn enqueue_dedup(
        &self,
        _: weft_task_store::tasks::NewTask,
    ) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
        unreachable!("an entry's event never becomes a task")
    }
    async fn wait_for_terminal(
        &self,
        _: uuid::Uuid,
        _: std::time::Duration,
    ) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
        unreachable!("not used by the serving side")
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
    spawn_peer_gated(tokio::sync::watch::channel(true).1).await
}

/// [`spawn_peer`], answering the sign-in only once `signed_in` turns
/// true: until then a dialogue that waits on each answer has nothing
/// more to send.
async fn spawn_peer_gated(signed_in: tokio::sync::watch::Receiver<bool>) -> FakePeer {
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
            let mut signed_in = signed_in.clone();
            tokio::spawn(async move {
                // The peer speaks first, like a real IMAP server.
                if write.write_all(b"* OK ready\r\n").await.is_err() {
                    return;
                }
                // A sign-in read but not yet answered: the peer keeps
                // reading (and recording) while it holds the answer back.
                let mut sign_in_held = false;
                loop {
                    let mut guard = rx.lock().await;
                    tokio::select! {
                        open = async { signed_in.wait_for(|open| *open).await.is_ok() }, if sign_in_held => {
                            drop(guard);
                            sign_in_held = false;
                            if !open || write.write_all(b"a1 OK signed in\r\n").await.is_err() {
                                break;
                            }
                        }
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
                                        sign_in_held = true;
                                        None
                                    } else if line == "a2 SELECT INBOX" {
                                        Some("a2 OK [READ-WRITE] SELECT completed")
                                    } else if line.ends_with(" IDLE") {
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

/// The fake broker: the listener-resolve route, answering the mailbox
/// connection's values, and the infra-address route, which knows no
/// infra and hands every address back unchanged.
async fn spawn_broker() -> String {
    spawn_broker_with(json!({ "user": "q@acme.com", "password": "s3cret" })).await
}

/// [`spawn_broker`], answering `values` as what the connection stores.
async fn spawn_broker_with(values: Value) -> String {
    use axum::routing::post;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/v1/access/listener-resolve",
        post(move |axum::Json(req): axum::Json<Value>| {
            let values = values.clone();
            async move {
                assert_eq!(req["service"], "email");
                axum::Json(json!({
                    "values": values,
                    "auth": [],
                    "events": {},
                    "recipe_values": {}
                }))
            }
        }),
    )
    // An entry's event goes straight to its project's worker
    // (`FakeDoors`).
    .route(
            "/v1/signal/fire_target",
            post(|| async { axum::Json(json!({ "project_id": uuid::Uuid::nil(), "is_resume": false, "address": "http://worker.test" })) }),
        );
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
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

/// A listener holding signals here, talking to the broker at
/// `broker_base` and firing into `doors`.
fn listener_state(run_id: &str, broker_base: String, doors: Arc<FakeDoors>) -> ListenerState {
    ListenerState::new(
        ListenerConfig {
            replica: format!("test-listener-{run_id}"),
            holds_here: true,
            holders: weft_listener::Holders::OnThisMachine,
        },
        Arc::new(FakeTasks),
        // The fake broker ignores the bearer.
        weft_broker_client::BrokerLink::new(
            broker_base,
            weft_broker_client::TokenSource::role(
                Arc::new(weft_platform_traits::FixedToken("test-token".into())),
                format!("test-listener-{run_id}"),
                weft_platform_traits::CoreRole::Listener,
            ),
        ),
        Arc::new(weft_platform_traits::FakeAlarm::new()),
        doors,
        weft_listener::HolderPlatform {
            news: Arc::new(weft_platform_traits::FakeHolderNews::default()),
            liveness: Arc::new(weft_platform_traits::OneProcessHolds),
        },
    )
}

/// The mailbox signal on `peer_address`, signing in through an `email`
/// connection, brought up from its row the way a restarted listener
/// does.
async fn bring_up_mailbox(state: &ListenerState, sig_token: &str, peer_address: &str) {
    let access = Access::new(uuid::Uuid::new_v4().to_string(), "email", None);
    let mut kind = StreamListen::new(
        peer_address,
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

    let row = json!({
        "token": sig_token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "node-1",
        "spec_json": serde_json::to_string(&to_spec(kind)).unwrap(), "is_resume": false, "run_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": {}, "kind_state_seq": 1, "holds": true, "serving": null
    });
    bring_up(state, serde_json::from_value(row).unwrap(), weft_core::signal::listener_protocol::StartMode::Restore)
        .await
        .expect("registration succeeds");
}

async fn run_scenario() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let sig_token = format!("sig-{run_id}");
    let peer = spawn_peer().await;
    let broker_base = spawn_broker().await;

    let doors = Arc::new(FakeDoors::default());
    let state = listener_state(&run_id, broker_base, doors.clone());
    bring_up_mailbox(&state, &sig_token, &peer.address).await;

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
        doors.fired.lock().unwrap().is_empty(),
        "dialogue chatter must not fire"
    );
    peer.downlink.send("* 4 EXISTS".into()).await.unwrap();
    wait_until(|| !doors.fired.lock().unwrap().is_empty(), "the fire").await;
    {
        let fires = doors.fired.lock().unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].payload, Value::String("* 4 EXISTS".into()));
        assert_eq!(fires[0].token, Value::String(sig_token.clone()));
    }

    // A steady-state line that does not match stays quiet.
    peer.downlink.send("* OK still here".into()).await.unwrap();
    peer.downlink.send("* 5 RECENT".into()).await.unwrap();
    wait_until(|| doors.fired.lock().unwrap().len() >= 2, "the second fire").await;
    assert_eq!(
        doors.fired.lock().unwrap().len(),
        2,
        "the non-matching line must not have fired"
    );
}

/// The mail trigger's own dialogue, as `ReceiveEmail` declares it
/// (`catalog/email/receive_email/mod.rs`), against an in-process IMAP
/// server: the address comes from the connection's `imapHost` and
/// `imapPort`, the sign-in sends the user and password as IMAP quoted
/// strings, each line waits for the server's answer to the one before
/// (nothing after the LOGIN goes out until the server says OK), the inbox
/// is opened and held in IDLE, and only the server's arrival
/// announcement fires.
#[tokio::test]
async fn the_mail_triggers_imap_dialogue_signs_in_opens_the_inbox_and_fires_on_arrival() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let (sign_in, signed_in) = tokio::sync::watch::channel(false);
    let peer = spawn_peer_gated(signed_in).await;
    let (host, port) = peer.address.rsplit_once(':').unwrap();
    let broker_base = spawn_broker_with(json!({
        "user": "q@acme.com", "password": r#"pa"ss w\rd"#, "imapHost": host, "imapPort": port,
    }))
    .await;
    let doors = Arc::new(FakeDoors::default());
    let state = listener_state(&run_id, broker_base, doors.clone());

    let access = Access::new(uuid::Uuid::new_v4().to_string(), "email", None);
    let mut kind = StreamListen::new(
        "{imapHost}:{imapPort}",
        Framing::Delimiter { bytes: "\r\n".into() },
        r"^\* \d+ (EXISTS|RECENT)",
    )
    .with_access(&access)
    .step(
        SocketFrame::Text { body: "a1 LOGIN {user|quoted} {password|quoted}\r\n".into() },
        "^a1 OK",
    )
    .step(SocketFrame::Text { body: "a2 SELECT INBOX\r\n".into() }, "^a2 OK")
    .step(SocketFrame::Text { body: "a3 IDLE\r\n".into() }, r"^\+")
    .with_heartbeat(SocketFrame::Text { body: "DONE\r\na9 IDLE\r\n".into() }, 25 * 60);
    // The in-process server has no certificate.
    kind.tls = false;
    let token = format!("sig-{run_id}");
    let row = json!({
        "token": token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "recv",
        "spec_json": serde_json::to_string(&to_spec(kind)).unwrap(), "is_resume": false, "run_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": {}, "kind_state_seq": 1, "holds": true, "serving": null
    });
    bring_up(&state, serde_json::from_value(row).unwrap(), weft_core::signal::listener_protocol::StartMode::Restore)
        .await
        .expect("registration succeeds");

    wait_until(|| !peer.received.lock().unwrap().is_empty(), "the sign-in").await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        *peer.received.lock().unwrap(),
        vec![r#"a1 LOGIN "q@acme.com" "pa\"ss w\\rd""#.to_string()],
        "nothing goes out before the server accepts the sign-in"
    );
    sign_in.send(true).unwrap();
    wait_until(|| peer.received.lock().unwrap().len() >= 3, "the rest of the dialogue").await;
    assert_eq!(peer.received.lock().unwrap()[1..], ["a2 SELECT INBOX".to_string(), "a3 IDLE".to_string()]);
    assert!(doors.fired.lock().unwrap().is_empty(), "the dialogue's answers fire nothing");

    peer.downlink.send("* 1 FETCH (FLAGS (\\Seen))".into()).await.unwrap();
    peer.downlink.send("* 7 EXISTS".into()).await.unwrap();
    wait_until(|| !doors.fired.lock().unwrap().is_empty(), "the arrival").await;
    let fires = doors.fired.lock().unwrap();
    assert_eq!(fires.len(), 1, "only the arrival fired");
    assert_eq!(fires[0].payload, Value::String("* 7 EXISTS".into()));
    assert_eq!(fires[0].token, Value::String(token.clone()));
}

/// A broker whose answer about the connection is that only a reconnect
/// brings it back (the access store's 409): resolving again repeats it,
/// so the held signal is given up with why, once, instead of retried on
/// the ladder. Records each resolve and each give-up's reason.
#[derive(Default)]
struct RefusingBroker {
    resolves: std::sync::atomic::AtomicUsize,
    gave_up: Mutex<Vec<String>>,
}

async fn spawn_refusing_broker(broker: Arc<RefusingBroker>) -> String {
    use axum::routing::post;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let resolving = broker.clone();
    let app = axum::Router::new()
        .route(
            "/v1/access/listener-resolve",
            post(move || {
                let broker = resolving.clone();
                async move {
                    broker.resolves.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    (axum::http::StatusCode::CONFLICT, "the 'email' access needs reconnecting: the refresh was refused")
                }
            }),
        )
        .route(
            "/v1/signal/give_up",
            post(move |axum::Json(req): axum::Json<Value>| {
                let broker = broker.clone();
                async move {
                    broker.gave_up.lock().unwrap().push(req["reason"].as_str().unwrap().to_string());
                    axum::Json(json!({}))
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test]
async fn a_connection_needing_a_reconnect_stops_the_held_signal() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let peer = spawn_peer().await;
    let broker = Arc::new(RefusingBroker::default());
    let state = listener_state(&run_id, spawn_refusing_broker(broker.clone()).await, Arc::new(FakeDoors::default()));
    bring_up_mailbox(&state, &format!("sig-{run_id}"), &peer.address).await;

    wait_until(|| !broker.gave_up.lock().unwrap().is_empty(), "the give-up").await;
    let reason = broker.gave_up.lock().unwrap()[0].clone();
    assert!(reason.contains("needs reconnecting"), "says why: {reason}");
    // The ladder's first step is a second; past it, a loop still retrying
    // would have resolved again.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_eq!(broker.resolves.load(std::sync::atomic::Ordering::SeqCst), 1, "never asked again");
    assert_eq!(broker.gave_up.lock().unwrap().len(), 1, "given up once");
    assert!(peer.received.lock().unwrap().is_empty(), "never dialed the peer");
}

/// The project's worker, as the listener hands it events: records each
/// one, and, while `failing`, cannot be reached.
#[derive(Default)]
struct FakeDoors {
    fired: Mutex<Vec<weft_core::door_fire::DoorFire>>,
    failing: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl weft_listener::fire_sink::WorkerDoors for FakeDoors {
    async fn fire(&self, _: uuid::Uuid, address: &str, fire: &weft_core::door_fire::DoorFire) -> anyhow::Result<weft_core::door_fire::Fired> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("no worker answers at {address}");
        }
        self.fired.lock().unwrap().push(fire.clone());
        Ok(weft_core::door_fire::Fired::Started { run_id: weft_core::door_fire::run_of_fire(fire.fire_id) })
    }
}
