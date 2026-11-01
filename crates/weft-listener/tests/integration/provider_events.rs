//! Layer-3 contract tests for the provider_events serving side: the
//! real listener code (registration, transport decision, the shared
//! socket engine, the pre-fire filter) wired against hand-rolled
//! fakes of its I/O: a fake broker (the listener-resolve route), a
//! fake provider (the socket-mint endpoint), a fake gateway (a real
//! in-process websocket server), and a fake task store recording the
//! fires the sink enqueued.

use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};

use weft_core::signal::{to_spec, Predicate, PredicateOp, ProviderEvents};
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

/// One in-process websocket gateway: accepts a connection, sends the
/// frames the test scripts, records what came back up the socket.
struct FakeGateway {
    /// Frames the LISTENER sent up the socket (acks).
    received: Arc<Mutex<Vec<String>>>,
    url: String,
    /// Send test frames down the held socket.
    downlink: tokio::sync::mpsc::Sender<String>,
}

async fn spawn_gateway() -> FakeGateway {
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
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws.split();
            let rx = rx.clone();
            let received = received_srv.clone();
            tokio::spawn(async move {
                loop {
                    let mut guard = rx.lock().await;
                    tokio::select! {
                        frame = guard.recv() => {
                            drop(guard);
                            let Some(frame) = frame else { break };
                            if write
                                .send(tokio_tungstenite::tungstenite::Message::Text(frame.into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        up = read.next() => {
                            drop(guard);
                            match up {
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t))) => {
                                    received.lock().unwrap().push(t.as_str().to_owned());
                                }
                                Some(Ok(_)) => {}
                                _ => break,
                            }
                        }
                    }
                }
            });
        }
    });
    FakeGateway { received, url: format!("ws://{addr}"), downlink: tx }
}

/// The fake broker + provider in one server: the listener-resolve
/// route (answering the connection's values + the events recipe) and
/// the socket-mint endpoint (answering the gateway's address,
/// recording the auth it saw). The mint answers once `mint_open` is
/// true, so a scenario can hold the socket back until every
/// subscription it registers has joined it.
struct FakeBroker {
    mint_auth_seen: Arc<Mutex<Vec<String>>>,
    mint_open: tokio::sync::watch::Sender<bool>,
}

async fn spawn_broker(gateway_url: String, multi_account: bool, mint_open: bool) -> (String, FakeBroker) {
    use axum::extract::State;
    use axum::routing::post;
    let mint_auth_seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (open_tx, open_rx) = tokio::sync::watch::channel(mint_open);
    #[derive(Clone)]
    struct S {
        gateway_url: String,
        mint_auth_seen: Arc<Mutex<Vec<String>>>,
        mint_open: tokio::sync::watch::Receiver<bool>,
    }
    let state = S { gateway_url, mint_auth_seen: mint_auth_seen.clone(), mint_open: open_rx };
    // The mint URL must be known before the recipe is written, and
    // the recipe is answered by this same server: bind first, then
    // build the app around the address.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let mint_url = format!("{base}/mint");
    let app = axum::Router::new()
        // An entry's event goes straight to its project's worker
        // (`FakeDoors`).
        .route(
            "/v1/signal/fire_target",
            post(|| async { axum::Json(json!({ "project_id": uuid::Uuid::nil(), "is_resume": false, "address": "http://worker.test" })) }),
        )
        .route(
            "/v1/access/listener-resolve",
            post(move |State(_s): State<S>, axum::Json(req): axum::Json<Value>| async move {
                assert_eq!(req["service"], "fakechat");
                axum::Json(json!({
                    "values": { "token": "tok-api" },
                    "auth": [{ "kind": "header", "name": "Authorization",
                               "value": "Bearer {token}" }],
                    "recipe_values": { "appToken": "xapp-1" },
                    // The connection's own provider account, what an
                    // account-scoped subscriber is matched against on
                    // a multi-account socket.
                    "provider_account": "T1",
                    "events": {
                        "messages": {
                            "fields": { "type": "type", "text": "text",
                                        "channel": "channel" },
                            "account": { "value": "team", "path": "team_id" },
                            "socket": {
                                "connect": {
                                    "url": mint_url,
                                    "method": "POST",
                                    "auth": [{ "kind": "header", "name": "Authorization",
                                               "value": "Bearer {appToken}" }],
                                    "captures": [{ "name": "url", "path": "url" }]
                                },
                                "replies": [{ "whenField": "envelope_id",
                                              "frame": "{\"envelope_id\":\"{value}\"}" }],
                                "eventPath": "payload.event",
                                "multiAccount": multi_account
                            }
                        }
                    }
                }))
            }),
        )
        .route(
            "/mint",
            post(|State(s): State<S>, headers: axum::http::HeaderMap| async move {
                let auth = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                s.mint_auth_seen.lock().unwrap().push(auth);
                let mut open = s.mint_open.clone();
                open.wait_for(|open| *open).await.expect("the scenario holds the gate");
                axum::Json(json!({ "ok": true, "url": s.gateway_url }))
            }),
        )
        .with_state(state);
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
    (base, FakeBroker { mint_auth_seen, mint_open: open_tx })
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

// The whole dial-out path: register a subscription, watch the
// listener resolve the connection, mint the socket address WITH the
// recipe-named app token, ack the envelope, and fire ONLY the frames
// matching the subscription's filter, as the topic's named fields.
//
// Stress-looped by construction (spawned tasks + channels + a
// multi-thread runtime): each iteration runs a fully isolated
// scenario (its own process name, token, servers), so concurrent runs
// contend on the scheduler, never on each other's state.
weft_core::stress_test!(
    name: a_socket_subscription_serves_end_to_end,
    runs: 6,
    worker_threads: 4,
    async fn body() {
        run_scenario().await;
    }
);

/// One scenario's shared listener-side rig: the fake task store and the
/// listener every registration in the scenario goes through.
struct Rig {
    doors: Arc<FakeDoors>,
    state: ListenerState,
}

fn rig(run_id: &str, broker_base: String) -> Rig {
    let doors = Arc::new(FakeDoors::default());
    let state = ListenerState::new(
        ListenerConfig {
            // Per-run instance: the shared-socket registry keys on it,
            // so parallel iterations never share a socket.
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
        doors.clone(),
        weft_listener::HolderPlatform {
            news: Arc::new(weft_platform_traits::FakeHolderNews::default()),
            liveness: Arc::new(weft_platform_traits::OneProcessHolds),
        },
    );
    Rig { doors, state }
}

/// Register one subscription through the real registration path.
async fn register_subscription(
    rig: &Rig,
    token: &str,
    spec: weft_core::primitive::SignalSpec,
) {
    // Brought up from its row, the way a restarted listener does.
    let row = json!({
        "token": token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "node-1",
        "spec_json": serde_json::to_string(&spec).unwrap(), "is_resume": false, "run_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": {}, "kind_state_seq": 1, "holds": true, "serving": null
    });
    bring_up(&rig.state, serde_json::from_value(row).unwrap(), weft_core::signal::listener_protocol::StartMode::Restore)
        .await
        .expect("registration succeeds");
}

async fn run_scenario() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let sig_token = format!("sig-{run_id}");
    let gateway = spawn_gateway().await;
    let (broker_base, broker) = spawn_broker(gateway.url.clone(), false, true).await;
    let rig = rig(&run_id, broker_base);
    let doors = rig.doors.clone();
    let registry = rig.state.registry.clone();

    let access = Access::new(uuid::Uuid::new_v4().to_string(), "fakechat", None);
    let spec = to_spec(ProviderEvents::new(
        &access,
        "messages",
        vec![Predicate {
            field: "text".into(),
            op: PredicateOp::Contains,
            value: Some("weft".into()),
        }],
    ));
    register_subscription(&rig, &sig_token, spec).await;

    // The listener resolved the connection and minted the address
    // with the RECIPE-named app token, not the API token.
    wait_until(|| !broker.mint_auth_seen.lock().unwrap().is_empty(), "the mint call").await;
    assert_eq!(
        broker.mint_auth_seen.lock().unwrap()[0],
        "Bearer xapp-1",
        "the mint call authenticates with the recipe's app-level token"
    );

    // Send a matching event down the socket, wrapped in the envelope
    // the recipe unwraps, with an envelope_id to ack.
    gateway
        .downlink
        .send(
            json!({
                "envelope_id": "env-1",
                "payload": { "event": {
                    "type": "message", "channel": "C42",
                    "text": "hello weft", "team_id": "T1"
                }}
            })
            .to_string(),
        )
        .await
        .unwrap();

    wait_until(|| !doors.fired.lock().unwrap().is_empty(), "the matching fire").await;
    {
        let fires = doors.fired.lock().unwrap();
        assert_eq!(fires.len(), 1);
        let payload = &fires[0].payload;
        assert_eq!(
            *payload,
            json!({ "type": "message", "channel": "C42", "text": "hello weft" }),
            "the fire carries the topic's NAMED fields, nothing raw"
        );
        assert_eq!(fires[0].token, Value::String(sig_token.clone()));
    }

    // The envelope was acked on the socket, per the recipe's reply
    // rule.
    wait_until(|| !gateway.received.lock().unwrap().is_empty(), "the ack").await;
    assert_eq!(
        gateway.received.lock().unwrap()[0],
        r#"{"envelope_id":"env-1"}"#
    );

    // A NON-matching event (filter says text must contain "weft")
    // fires nothing.
    gateway
        .downlink
        .send(
            json!({
                "payload": { "event": {
                    "type": "message", "channel": "C42",
                    "text": "unrelated", "team_id": "T1"
                }}
            })
            .to_string(),
        )
        .await
        .unwrap();
    // A second MATCHING event proves the pipeline is still alive, so
    // the non-matching one above was genuinely filtered rather than
    // still in flight.
    gateway
        .downlink
        .send(
            json!({
                "payload": { "event": {
                    "type": "message", "channel": "C42",
                    "text": "weft again", "team_id": "T1"
                }}
            })
            .to_string(),
        )
        .await
        .unwrap();
    wait_until(|| doors.fired.lock().unwrap().len() >= 2, "the second matching fire").await;
    {
        let fires = doors.fired.lock().unwrap();
        assert_eq!(fires.len(), 2, "the non-matching frame fired nothing");
        assert_eq!(fires[1].payload["text"], "weft again");
    }

    // Unregistering (registry removal) aborts the per-signal task;
    // its drop guard leaves the socket share, and the LAST subscriber
    // leaving tears the shared engine down.
    registry.remove(&sig_token, weft_listener::registry::Registrations::All);
}

// A multi-account socket fails CLOSED: an account-scoped subscriber
// (its connection's provider account is "T1") hears a frame only when
// the frame PROVES it is T1's; another install's frame and a frame
// naming no account at all deliver nothing to it. An app-wide
// subscriber on the same socket hears everything.
weft_core::stress_test!(
    name: a_multi_account_socket_delivers_only_proven_own_account_frames,
    runs: 4,
    worker_threads: 4,
    async fn body() {
        run_multi_account_scenario().await;
    }
);

async fn run_multi_account_scenario() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let account_token = format!("sig-acct-{run_id}");
    let app_token = format!("sig-app-{run_id}");
    let gateway = spawn_gateway().await;
    let (broker_base, broker) = spawn_broker(gateway.url.clone(), true, false).await;
    let rig = rig(&run_id, broker_base);

    // Two subscriptions on the SAME connection (one shared socket):
    // one scoped to its own account, one app-wide.
    let access = Access::new(uuid::Uuid::new_v4().to_string(), "fakechat", None);
    register_subscription(
        &rig,
        &account_token,
        to_spec(ProviderEvents::new(&access, "messages", Vec::new())),
    )
    .await;
    register_subscription(
        &rig,
        &app_token,
        to_spec(ProviderEvents::new(&access, "messages", Vec::new()).app_wide()),
    )
    .await;
    // A frame reaches the subscriptions on the socket when it arrives, so
    // the socket opens only once both have joined it (each settles its
    // transport right as it joins); else the first could hear a frame
    // before the second is there.
    let joined = |token: &str| {
        rig.state.registry.get(token).is_some_and(|sig| sig.serving.lock().transport.is_some())
    };
    wait_until(|| joined(&account_token) && joined(&app_token), "both subscriptions joining the socket").await;
    broker.mint_open.send(true).unwrap();

    let frame = |team: Option<&str>, text: &str| {
        let mut payload = json!({
            "event": { "type": "message", "channel": "C1", "text": text }
        });
        if let Some(team) = team {
            payload["team_id"] = Value::String(team.to_string());
        }
        json!({ "payload": payload }).to_string()
    };

    // Another install's frame: the app-wide subscriber hears it, the
    // account-scoped one must not.
    gateway.downlink.send(frame(Some("T2"), "other install")).await.unwrap();
    // A frame naming NO account: unknown must fail closed for the
    // account-scoped subscriber (and warn), never deliver.
    gateway.downlink.send(frame(None, "shapeless")).await.unwrap();
    // The subscriber's own account's frame: both hear it.
    gateway.downlink.send(frame(Some("T1"), "own install")).await.unwrap();

    wait_until(
        || rig.doors.fired.lock().unwrap().len() >= 4,
        "the four expected fires",
    )
    .await;
    let fires = rig.doors.fired.lock().unwrap();
    let of = |token: &str| -> Vec<String> {
        fires
            .iter()
            .filter(|f| f.token == Value::String(token.to_string()))
            .map(|f| f.payload["text"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(
        of(&app_token),
        vec!["other install", "shapeless", "own install"],
        "the app-wide subscriber hears every frame"
    );
    assert_eq!(
        of(&account_token),
        vec!["own install"],
        "the account-scoped subscriber hears only frames proven to be its own account's"
    );
    assert_eq!(fires.len(), 4, "nothing else fired");
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

/// A fake broker for a subscription pushed to the install: the
/// listener-resolve route answering a webhook-only topic, and the
/// subscription check answering what `check` says, recording each check.
async fn spawn_webhook_broker(check: Result<(), &'static str>) -> (String, Arc<Mutex<Vec<Value>>>) {
    use axum::response::IntoResponse;
    use axum::routing::post;
    let checked: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = checked.clone();
    let app = axum::Router::new()
        .route(
            "/v1/access/listener-resolve",
            post(|| async {
                axum::Json(json!({
                    "values": { "token": "tok-api" },
                    "auth": [],
                    "recipe_values": {},
                    "events": {
                        "messages": {
                            "fields": { "text": "text" },
                            "account": { "value": "team", "path": "team_id" },
                            "webhook": {
                                "verify": { "kind": "token_echo" },
                                "routeBy": "subscription",
                                "idPath": "header:X-Chan-Id",
                                "tokenPath": "header:X-Chan-Token",
                                "subscribe": {
                                    "subscribe": {
                                        "url": "https://provider.test/watch",
                                        "method": "POST",
                                        "body": { "address": "{receiverUrl}" },
                                        "auth": []
                                    }
                                }
                            }
                        }
                    }
                }))
            }),
        )
        .route(
            "/v1/access/subscription/check",
            post(move |axum::Json(req): axum::Json<Value>| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(req);
                    match check {
                        Ok(()) => (axum::http::StatusCode::OK, axum::Json(Value::Null)).into_response(),
                        Err(why) => (axum::http::StatusCode::BAD_REQUEST, why.to_string()).into_response(),
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
    (base, checked)
}

/// Whether a subscription pushed to the install could come up is asked
/// of the broker, subscribing nothing: its refusal (no address the
/// internet reaches) is what the check answers, naming the fix; a broker
/// that says it could lets it through.
#[tokio::test]
async fn a_pushed_subscription_is_checked_with_the_broker_before_it_comes_up() {
    for (check, refused) in [(Err("this trigger needs 'fakechat' to deliver events TO your weft"), true), (Ok(()), false)] {
        let (base, checked) = spawn_webhook_broker(check).await;
        let rig = rig(&uuid::Uuid::new_v4().simple().to_string(), base);
        let access = Access::new(uuid::Uuid::new_v4().to_string(), "fakechat", None);
        let spec = to_spec(ProviderEvents::new(&access, "messages", Vec::new()));
        let identity = weft_listener::kinds::SignalIdentity {
            token: "sig-1".into(),
            tenant_id: "tenant-a".into(),
            node_id: "inbox".into(),
            is_resume: false,
            run_id: None,
            spec,
        };
        let answer = weft_listener::kinds::check_signal(&rig.state, identity, None, 1_000).await;
        match refused {
            true => {
                let why = format!("{:#}", answer.expect_err("refused"));
                assert!(why.contains("to deliver events TO your weft"), "{why}");
            }
            false => answer.expect("it could come up"),
        }
        let checked = checked.lock().unwrap();
        assert_eq!(checked.len(), 1, "one check, and no subscribe");
        assert_eq!((checked[0]["topic"].as_str(), checked[0]["signal_token"].as_str()), (Some("messages"), Some("sig-1")));
    }
}
