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
                                .send(tokio_tungstenite::tungstenite::Message::Text(frame))
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
                                    received.lock().unwrap().push(t);
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
/// recording the auth it saw).
struct FakeBroker {
    mint_auth_seen: Arc<Mutex<Vec<String>>>,
}

async fn spawn_broker(gateway_url: String, multi_account: bool) -> (String, FakeBroker) {
    use axum::extract::State;
    use axum::routing::post;
    let mint_auth_seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    #[derive(Clone)]
    struct S {
        gateway_url: String,
        mint_auth_seen: Arc<Mutex<Vec<String>>>,
    }
    let state = S { gateway_url, mint_auth_seen: mint_auth_seen.clone() };
    // The mint URL must be known before the recipe is written, and
    // the recipe is answered by this same server: bind first, then
    // build the app around the address.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    let mint_url = format!("{base}/mint");
    let app = axum::Router::new()
        .route(
            "/v1/access/listener-resolve",
            post(move |State(_s): State<S>, axum::Json(req): axum::Json<Value>| async move {
                assert_eq!(req["service"], "fakechat");
                axum::Json(json!({
                    "values": { "token": "tok-api" },
                    "auth": [{ "kind": "header", "name": "Authorization",
                               "value": "Bearer {token}" }],
                    "recipe_values": { "app_token": "xapp-1" },
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
                                               "value": "Bearer {app_token}" }],
                                    "captures": [{ "name": "url", "path": "url" }]
                                },
                                "replies": [{ "when_field": "envelope_id",
                                              "frame": "{\"envelope_id\":\"{value}\"}" }],
                                "event_path": "payload.event",
                                "multi_account": multi_account
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
                axum::Json(json!({ "ok": true, "url": s.gateway_url }))
            }),
        )
        .with_state(state);
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, FakeBroker { mint_auth_seen })
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
// scenario (its own pod name, token, servers), so concurrent runs
// contend on the scheduler, never on each other's state.
weft_core::stress_test!(
    name: a_socket_subscription_serves_end_to_end,
    runs: 6,
    worker_threads: 4,
    async fn body() {
        run_scenario().await;
    }
);

/// One scenario's shared listener-side rig: the fake task store, the
/// registry, and the config every registration in the scenario uses.
struct Rig {
    tasks: Arc<FakeTasks>,
    registry: Arc<Registry>,
    config: Arc<ListenerConfig>,
    broker_base: String,
}

fn rig(run_id: &str, broker_base: String) -> Rig {
    Rig {
        tasks: Arc::new(FakeTasks { enqueued: Mutex::new(Vec::new()) }),
        registry: Arc::new(Registry::new()),
        config: Arc::new(ListenerConfig {
            // Per-run pod name: the shared-socket registry keys on
            // it, so parallel iterations never share a socket.
            pod_name: format!("test-pod-{run_id}"),
            http_port: 0,
            broker_url: broker_base.clone(),
        }),
        broker_base,
    }
}

/// Register one subscription through the real registration path.
async fn register_subscription(
    rig: &Rig,
    token: &str,
    spec: weft_core::primitive::SignalSpec,
) {
    // The events client reads a bearer token from a file; hand it a
    // real one (the fake broker ignores it).
    let token_path =
        std::env::temp_dir().join(format!("weft-test-token-{}", uuid::Uuid::new_v4()));
    std::fs::write(&token_path, "test-token").unwrap();
    let events_broker = weft_broker_client::BrokerEventsClient::new(
        rig.broker_base.clone(),
        weft_broker_client::TokenSource::new(token_path),
    );
    register_in_registry(
        SignalIdentity {
            token: token.to_string(),
            tenant_id: "tenant-a".into(),
            node_id: "node-1".into(),
            is_resume: false,
            color: None,
            placement_generation: 7,
            spec,
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
        rig.registry.clone(),
        weft_listener::fire_sink::FireSignalSink::new(rig.tasks.clone()),
        rig.config.clone(),
        events_broker,
    )
    .await
    .expect("registration succeeds");
}

async fn run_scenario() {
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let sig_token = format!("sig-{run_id}");
    let gateway = spawn_gateway().await;
    let (broker_base, broker) = spawn_broker(gateway.url.clone(), false).await;
    let rig = rig(&run_id, broker_base);
    let tasks = rig.tasks.clone();
    let registry = rig.registry.clone();

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

    wait_until(|| !tasks.enqueued.lock().unwrap().is_empty(), "the matching fire").await;
    {
        let fires = tasks.enqueued.lock().unwrap();
        assert_eq!(fires.len(), 1);
        let payload = &fires[0].payload["payload"];
        assert_eq!(
            *payload,
            json!({ "type": "message", "channel": "C42", "text": "hello weft" }),
            "the fire carries the topic's NAMED fields, nothing raw"
        );
        assert_eq!(fires[0].tenant_id.as_deref(), Some("tenant-a"));
        assert_eq!(fires[0].payload["token"], Value::String(sig_token.clone()));
        assert_eq!(fires[0].payload["placement_generation"], 7);
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
    wait_until(|| tasks.enqueued.lock().unwrap().len() >= 2, "the second matching fire").await;
    {
        let fires = tasks.enqueued.lock().unwrap();
        assert_eq!(fires.len(), 2, "the non-matching frame fired nothing");
        assert_eq!(fires[1].payload["payload"]["text"], "weft again");
    }

    // Unregistering (registry removal) aborts the per-signal task;
    // its drop guard leaves the socket share, and the LAST subscriber
    // leaving tears the shared engine down.
    registry.remove(&sig_token);
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
    let (broker_base, _broker) = spawn_broker(gateway.url.clone(), true).await;
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
        || rig.tasks.enqueued.lock().unwrap().len() >= 4,
        "the four expected fires",
    )
    .await;
    let fires = rig.tasks.enqueued.lock().unwrap();
    let of = |token: &str| -> Vec<String> {
        fires
            .iter()
            .filter(|f| f.payload["token"] == Value::String(token.to_string()))
            .map(|f| f.payload["payload"]["text"].as_str().unwrap().to_string())
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
