//! Layer-3 contract tests for the wake path: the real listener code
//! (registration, arming, `kinds::wake`, the timer's claim) against
//! hand-rolled fakes of its I/O: a fake broker holding signal rows in
//! memory (the held read and the claimed state write, the same
//! rule the SQL applies), a fake alarm recording the wakes set, and a
//! fake task store recording the fires enqueued.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use weft_core::signal::{to_spec, Timer, TimerSpec};
use weft_listener::kinds::{prepare_signal, wake, SignalIdentity, WakeBody};
use weft_listener::{ListenerConfig, ListenerState};
use weft_platform_traits::FakeAlarm;

// ---------- Fakes ----------

/// Records every fire the listener enqueued.
#[derive(Default)]
struct FakeTasks {
    enqueued: Mutex<Vec<weft_task_store::tasks::NewTask>>,
    /// While set, every enqueue fails (the broker is down).
    failing: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl weft_task_store::TaskStoreClient for FakeTasks {
    async fn cancels_asked(
        &self,
        _project_id: uuid::Uuid,
        _execution_ids: Vec<String>,
    ) -> anyhow::Result<Vec<weft_task_store::tasks::CancelAsked>> {
        Ok(Vec::new())
    }
    async fn enqueue_dedup(&self, spec: weft_task_store::tasks::NewTask) -> anyhow::Result<weft_task_store::tasks::DedupOutcome> {
        if self.failing.load(std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("the broker is down");
        }
        self.enqueued.lock().unwrap().push(spec);
        Ok(weft_task_store::tasks::DedupOutcome::Inserted(uuid::Uuid::new_v4()))
    }
    async fn wait_for_terminal(&self, _: uuid::Uuid, _: std::time::Duration) -> anyhow::Result<weft_task_store::tasks::TaskOutcome> {
        unreachable!()
    }
    async fn claim_execution(
        &self,
        _: &str,
        _: uuid::Uuid,
        _: &str,
    ) -> anyhow::Result<Option<weft_task_store::tasks::ClaimedExecution>> {
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

/// Signal rows by token: `(row as the broker hands it back)`. The write
/// applies the same rule as `held_signals::write_kind_state`.
type Rows = Arc<Mutex<HashMap<String, Value>>>;

/// A registration's kind state, written (moving the row one version on)
/// just before the next claim is judged: a registration landing between
/// a wake's read and its claim.
type Landing = Arc<Mutex<Option<Value>>>;

/// How many of the next `get_held` reads answer 500 (the broker
/// hiccuping), and how many `get_held` reads were made.
#[derive(Default)]
struct GetHeld {
    failing: std::sync::atomic::AtomicUsize,
    reads: std::sync::atomic::AtomicUsize,
}

async fn spawn_broker(rows: Rows, landing: Landing, get_held: Arc<GetHeld>) -> String {
    use axum::routing::post;
    use std::sync::atomic::Ordering;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let get_rows = rows.clone();
    let list_rows = rows.clone();
    let hold_rows = rows.clone();
    let let_go_rows = rows.clone();
    let app = axum::Router::new()
        // A holder giving up its claims, as `held_signals::let_go` does.
        .route(
            "/v1/signal/let_go",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = let_go_rows.clone();
                async move {
                    let mut rows = rows.lock().unwrap();
                    for row in rows.values_mut() {
                        if row["held_by"] == req["replica"] {
                            row["held_by"] = Value::Null;
                        }
                    }
                    axum::Json(json!({}))
                }
            }),
        )
        // A holder's look, by the same rule as `held_signals::hold`: it
        // keeps what it still claims, names what is gone as ended, and
        // takes the held rows nobody claims or that it claims without
        // saying it holds them, the wanted ones first.
        .route(
            "/v1/signal/hold",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = hold_rows.clone();
                async move {
                    let mut rows = rows.lock().unwrap();
                    let replica = req["replica"].clone();
                    let holding: Vec<&str> = req["holding"].as_array().unwrap().iter().map(|h| h["token"].as_str().unwrap()).collect();
                    let kept: Vec<&str> = holding
                        .iter()
                        .copied()
                        .filter(|t| rows.get(*t).is_some_and(|r| r["holds"] == json!(true) && r["held_by"] == replica))
                        .collect();
                    let ended: Vec<&str> = holding.iter().copied().filter(|t| !rows.contains_key(*t)).collect();
                    let want: Vec<&str> = req["want"].as_array().unwrap().iter().map(|t| t.as_str().unwrap()).collect();
                    let mut free: Vec<String> = rows
                        .iter()
                        .filter(|(t, r)| {
                            r["holds"] == json!(true)
                                && (r["held_by"].is_null() || r["held_by"] == replica)
                                && !holding.contains(&t.as_str())
                        })
                        .map(|(t, _)| t.clone())
                        .collect();
                    free.sort_by_key(|t| !want.contains(&t.as_str()));
                    let mut taken = Vec::new();
                    for token in free {
                        let row = rows.get_mut(&token).unwrap();
                        row["held_by"] = replica.clone();
                        let mut wire = row.clone();
                        wire.as_object_mut().unwrap().remove("held_by");
                        taken.push(wire);
                    }
                    axum::Json(json!({ "kept": kept, "ended": ended, "taken": taken }))
                }
            }),
        )
        .route(
            "/v1/signal/list_held",
            post(move |axum::Json(_): axum::Json<Value>| {
                let rows = list_rows.clone();
                async move {
                    let rows: Vec<Value> = rows
                        .lock()
                        .unwrap()
                        .values()
                        .cloned()
                        .map(|mut row| {
                            row.as_object_mut().unwrap().remove("held_by");
                            row
                        })
                        .collect();
                    axum::Json(json!({ "rows": rows }))
                }
            }),
        )
        // Every address is one the listener reaches as it is.
        .route(
            "/v1/infra/listener-address",
            post(|axum::Json(req): axum::Json<Value>| async move { axum::Json(json!({ "authority": req["authority"] })) }),
        )
        .route(
            "/v1/signal/get_held",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = get_rows.clone();
                let get_held = get_held.clone();
                async move {
                    get_held.reads.fetch_add(1, Ordering::SeqCst);
                    let failing = get_held.failing.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
                    if failing.is_ok() {
                        return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
                    }
                    let mut row = rows.lock().unwrap().get(req["token"].as_str().unwrap()).cloned();
                    if let Some(row) = row.as_mut() {
                        row.as_object_mut().unwrap().remove("held_by");
                    }
                    Ok(axum::Json(json!({ "row": row })))
                }
            }),
        )
        .route(
            "/v1/signal/write_kind_state",
            post(move |axum::Json(req): axum::Json<Value>| {
                let rows = rows.clone();
                let landing = landing.clone();
                async move {
                    let mut rows = rows.lock().unwrap();
                    let Some(row) = rows.get_mut(req["token"].as_str().unwrap()) else {
                        return axum::Json(json!({ "written": false }));
                    };
                    if let Some(registered) = landing.lock().unwrap().take() {
                        row["kind_state"] = registered;
                        row["kind_state_seq"] = json!(row["kind_state_seq"].as_i64().unwrap() + 1);
                    }
                    let at = row["kind_state_seq"].as_i64().unwrap();
                    let written = req["from_seq"].as_i64().unwrap() == at;
                    if written {
                        row["kind_state"] = req["kind_state"].clone();
                        row["kind_state_seq"] = json!(at + 1);
                    }
                    axum::Json(json!({ "written": written }))
                }
            }),
        );
    tokio::spawn(async move { axum::serve(listener, weft_broker_client::line::server::with_line(app)).await.unwrap() });
    format!("http://{addr}")
}

struct Rig {
    state: ListenerState,
    tasks: Arc<FakeTasks>,
    alarm: Arc<FakeAlarm>,
    rows: Rows,
    landing: Landing,
    get_held: Arc<GetHeld>,
    /// The fake broker's address.
    broker: String,
}

/// The listener's way to the fake broker at `broker`.
fn link(broker: &str) -> weft_broker_client::BrokerLink {
    weft_broker_client::BrokerLink::new(
        broker.to_string(),
        weft_broker_client::TokenSource::role(
            Arc::new(weft_platform_traits::FixedToken("test-token".into())),
            "test-listener",
            weft_platform_traits::CoreRole::Listener,
        ),
    )
}

async fn rig(holds_here: bool) -> Rig {
    let rows: Rows = Arc::default();
    let landing: Landing = Arc::default();
    let get_held: Arc<GetHeld> = Arc::default();
    let broker = spawn_broker(rows.clone(), landing.clone(), get_held.clone()).await;
    let tasks = Arc::new(FakeTasks::default());
    let alarm = Arc::new(FakeAlarm::new());
    let state = ListenerState::new(
        ListenerConfig { replica: "test-listener".into(), holds_here, prefer_push: false },
        tasks.clone(),
        link(&broker),
        alarm.clone(),
    );
    Rig { state, tasks, alarm, rows, landing, get_held, broker }
}

fn identity(token: &str, spec: weft_core::primitive::SignalSpec) -> SignalIdentity {
    SignalIdentity {
        token: token.into(),
        tenant_id: "tenant-a".into(),
        node_id: "tick".into(),
        is_resume: false,
        execution_id: None,
        spec,
    }
}

/// The row the dispatcher would write for a registration.
fn row(token: &str, spec: &weft_core::primitive::SignalSpec, kind_state: Value, seq: i64) -> Value {
    let holds = weft_listener::kinds::lookup(&spec.kind)
        .is_some_and(|h| h.between_fires(spec, &kind_state).expect("a test row's state reads") == weft_listener::kinds::BetweenFires::Holds);
    json!({
        "token": token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "tick",
        "spec_json": serde_json::to_string(spec).unwrap(), "is_resume": false, "execution_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": kind_state, "kind_state_seq": seq, "holds": holds, "serving": null
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Register a signal the way the dispatcher does: prepare it (nothing
/// starts), write its row, then bring it up. Answers the prepared state.
async fn register(rig: &Rig, token: &str, spec: &weft_core::primitive::SignalSpec, asked_at_unix_ms: i64) -> anyhow::Result<Value> {
    let prepared = prepare_signal(&rig.state, identity(token, spec.clone()), None, None, asked_at_unix_ms).await?;
    assert!(rig.alarm.wakes_for(&format!("signal:{token}")).is_empty(), "preparing sets no wake");
    let written = row(token, spec, prepared.kind_state.clone(), 1);
    rig.rows.lock().unwrap().insert(token.into(), written.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written)?, weft_core::signal::listener_protocol::StartMode::New).await?;
    Ok(prepared.kind_state)
}

/// A rehydrate brings up every row it can: a row that cannot come up is
/// named in the answer, and the rows after it still come up.
#[tokio::test]
async fn a_rehydrate_brings_up_every_row_past_a_broken_one() {
    let rig = rig(false).await;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 60_000 } });
    let prepared = prepare_signal(&rig.state, identity("good", spec.clone()), None, None, now_ms()).await.unwrap();
    let mut broken = row("broken", &spec, json!({}), 1);
    broken["spec_json"] = json!("not json");
    {
        let mut rows = rig.rows.lock().unwrap();
        rows.insert("broken".into(), broken);
        rows.insert("good".into(), row("good", &spec, prepared.kind_state, 1));
    }

    let down = weft_listener::registry::rehydrate(&rig.state, Some(uuid::Uuid::nil()), &[]).await.unwrap();
    assert_eq!(down.len(), 1, "{down:?}");
    assert!(down[0].contains("signal broken"), "{down:?}");
    assert_eq!(rig.alarm.wakes_for("signal:good").len(), 1, "the good row came up");
}

/// A timer is armed with its pinned moment, fires once on the wake, and
/// a second delivery of the same wake (an alarm retrying, a second copy
/// of the listener) fires nothing: the claim has one winner.
#[tokio::test]
async fn a_timer_wake_fires_once_however_often_it_is_delivered() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.expect("a timer registers on a serverless listener");
    let armed = rig.alarm.wakes_for("signal:tok");
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].at_unix_ms, asked + 1_000, "the wake is the pinned moment");
    assert_eq!(armed[0].role, weft_platform_traits::CoreRole::Listener);
    let body = || WakeBody { token: "tok".into(), due_at_ms: asked + 1_000 };

    wake(&rig.state, body()).await.unwrap();
    wake(&rig.state, body()).await.unwrap();
    let fires = rig.tasks.enqueued.lock().unwrap();
    assert_eq!(fires.len(), 1, "one tick, whatever the deliveries");
    assert_eq!(fires[0].payload["token"], "tok");
    assert!(fires[0].payload["payload"]["scheduledTime"].is_string());
    let stored = rig.rows.lock().unwrap()["tok"].clone();
    assert_eq!(stored["kind_state"], json!({}), "a one-shot has nothing left");
    assert_eq!(stored["kind_state_seq"], 2);
    assert_eq!(rig.alarm.wakes_for("signal:tok").len(), 1, "and sets no further wake");
}

/// A wake is only ever set once its row is written, so a wake that finds
/// no row is for a signal that is gone: it does nothing and sets nothing.
#[tokio::test]
async fn a_wake_for_a_signal_that_is_gone_does_nothing() {
    let rig = rig(true).await;
    let now = now_ms();
    wake(&rig.state, WakeBody { token: "gone".into(), due_at_ms: now }).await.unwrap();
    assert!(rig.alarm.wakes_for("signal:gone").is_empty());
    assert!(rig.tasks.enqueued.lock().unwrap().is_empty());
}

/// A registration that rewrote the row (a reactivate, a new schedule)
/// moved its version, so a wake that read the row before it cannot claim
/// over it: the new schedule stands.
#[tokio::test]
async fn a_wake_read_before_a_new_registration_cannot_claim_over_it() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    let fresh = json!({ "next_fire_at_unix_ms": asked + 3_600_000 });
    *rig.landing.lock().unwrap() = Some(fresh.clone());

    wake(&rig.state, WakeBody { token: "tok".into(), due_at_ms: asked + 1_000 }).await.unwrap();
    let stored = rig.rows.lock().unwrap()["tok"].clone();
    assert_eq!((stored["kind_state"].clone(), stored["kind_state_seq"].clone()), (fresh, json!(2)), "the new schedule stands");
}

/// A kind that holds a connection open is prepared as held by a listener
/// that scales to zero: its row says so, and a holder takes it. Nothing
/// starts here.
#[tokio::test]
async fn a_serverless_listener_prepares_a_held_kind_for_a_holder() {
    let rig = rig(false).await;
    let spec = to_spec(weft_core::signal::SseSubscribe { url: "https://example.com/feed".into(), event_name: String::new(), filters: Vec::new() });
    let prepared = prepare_signal(&rig.state, identity("sse", spec), None, None, now_ms()).await.expect("prepared");
    assert!(prepared.holds);
    assert!(rig.state.registry.get("sse").is_none());
}

/// A tick that could not be enqueued fails the wake and leaves the moment
/// unclaimed, so the alarm's retry fires it: a one-shot is never lost to
/// a broker that was down for a moment.
#[tokio::test]
async fn a_tick_that_could_not_be_enqueued_is_fired_by_the_retried_wake() {
    let rig = rig(false).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    let body = || WakeBody { token: "tok".into(), due_at_ms: asked + 1_000 };

    rig.tasks.failing.store(true, std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, body()).await.expect_err("a lost tick fails the wake so the alarm retries");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 1, "the moment is not claimed");

    rig.tasks.failing.store(false, std::sync::atomic::Ordering::SeqCst);
    wake(&rig.state, body()).await.unwrap();
    let fires = rig.tasks.enqueued.lock().unwrap();
    assert_eq!(fires.len(), 1, "the retry fires the tick");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 2);
}

/// A far wake a platform delivered early (Cloud Tasks cannot schedule past
/// 30 days) touches nothing and is set again for its real moment.
#[tokio::test]
async fn a_wake_that_arrives_before_its_moment_is_set_again_for_it() {
    let rig = rig(false).await;
    let asked = now_ms();
    let far = asked + 60 * 24 * 3_600_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: (far - asked) as u64 } });
    register(&rig, "tok", &spec, asked).await.unwrap();

    wake(&rig.state, WakeBody { token: "tok".into(), due_at_ms: far }).await.unwrap();
    assert!(rig.tasks.enqueued.lock().unwrap().is_empty(), "nothing fires early");
    assert_eq!(rig.rows.lock().unwrap()["tok"]["kind_state_seq"], 1);
    let wakes = rig.alarm.wakes_for("signal:tok");
    assert_eq!(wakes.len(), 2, "armed, then set again");
    assert_eq!(wakes[1].at_unix_ms, far);
    assert_eq!(wakes[1].body["due_at_ms"], far);
}

/// A feed that answers 500 while `failing` is set, and one page with a
/// single item once it is cleared.
async fn spawn_feed(failing: Arc<std::sync::atomic::AtomicBool>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/feed",
        axum::routing::get(move || {
            let failing = failing.clone();
            async move {
                if failing.load(std::sync::atomic::Ordering::SeqCst) {
                    Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                } else {
                    Ok(axum::Json(json!({ "items": [{ "id": 7 }] })))
                }
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}/feed")
}

/// Another copy of the listener, as a serverless platform starts one per
/// request: nothing in memory, the same broker, task store and alarm.
fn fresh_listener(rig: &Rig) -> ListenerState {
    ListenerState::new((*rig.state.config).clone(), rig.tasks.clone(), link(&rig.broker), rig.alarm.clone())
}

/// What a signal's display says, as one string.
async fn shown(state: &ListenerState, token: &str) -> String {
    let sig = weft_listener::registry::held(state, token).await.unwrap().expect("the row is held");
    let live = weft_listener::kinds::compute_live(&weft_listener::kinds::LiveCtx { sig: &sig, address: None });
    serde_json::to_string(&live).unwrap()
}

/// A poll's failure streak lives on the row, so it grows across wakes
/// that each run on a fresh listener (as serverless copies do), reaches
/// the escalation, and shows on the node's display. A delta poll that
/// never succeeded stays unprimed however often its row was written, so
/// its first good poll primes silently instead of firing history.
#[tokio::test]
async fn a_poll_failure_streak_survives_fresh_listeners_and_priming_is_explicit() {
    let failing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let feed = spawn_feed(failing.clone()).await;
    let spec = to_spec(
        serde_json::from_value::<weft_core::signal::PollEndpoint>(json!({
            "url": feed,
            "interval_secs": 60,
            "delta": { "items": "items", "cursor_field": "id" },
        }))
        .unwrap(),
    );
    let first = rig(false).await;
    register(&first, "poll", &spec, now_ms()).await.unwrap();

    for n in 1..=3 {
        let copy = fresh_listener(&first);
        let now = now_ms();
        wake(&copy, WakeBody { token: "poll".into(), due_at_ms: now }).await.unwrap();
        let stored = first.rows.lock().unwrap()["poll"].clone();
        assert_eq!(stored["kind_state"]["consecutive_failures"], n, "{stored}");
        assert!(stored["kind_state"].get("delta").is_none(), "a failed poll primes nothing");
        assert!(shown(&copy, "poll").await.contains(&format!("failed {n} in a row")));
    }
    assert_eq!(first.rows.lock().unwrap()["poll"]["kind_state_seq"], 4, "the row was written, and is still unprimed");

    failing.store(false, std::sync::atomic::Ordering::SeqCst);
    let copy = fresh_listener(&first);
    let now = now_ms();
    wake(&copy, WakeBody { token: "poll".into(), due_at_ms: now }).await.unwrap();
    assert!(first.tasks.enqueued.lock().unwrap().is_empty(), "the first good poll primes, it fires nothing");
    let stored = first.rows.lock().unwrap()["poll"]["kind_state"].clone();
    assert_eq!(stored, json!({ "delta": { "cursor": 7 } }), "primed, and the streak is cleared");
    assert!(shown(&copy, "poll").await.contains("polling"));
}

// ---------- A run waiting on a poll ----------

/// A job status endpoint answering each GET with the next of `script`
/// (status code, body), then repeating the last; counts the GETs.
async fn spawn_status(script: Vec<(u16, Value)>) -> (String, Arc<std::sync::atomic::AtomicUsize>) {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = hits.clone();
    let script = Arc::new(script);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/status",
        axum::routing::get(move || {
            let hits = counted.clone();
            let script = script.clone();
            async move {
                let n = hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (code, body) = script[n.min(script.len() - 1)].clone();
                (axum::http::StatusCode::from_u16(code).unwrap(), axum::Json(body))
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}/status"), hits)
}

/// A wait on a job: resumes on the first answer whose `status` says done.
fn job_wait(url: &str) -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::PollEndpoint {
        url: url.into(),
        interval_secs: 60,
        filters: vec![weft_core::signal::Predicate::regex("status", "COMPLETED|FAILED")],
        ..Default::default()
    })
}

/// Register a parked run's wait the way the dispatcher does.
async fn register_wait(rig: &Rig, token: &str, spec: &weft_core::primitive::SignalSpec, asked_at_unix_ms: i64) -> anyhow::Result<()> {
    let mut who = identity(token, spec.clone());
    who.is_resume = true;
    who.execution_id = Some("exec-1".into());
    let prepared = prepare_signal(&rig.state, who, None, None, asked_at_unix_ms).await?;
    let mut written = row(token, spec, prepared.kind_state, 1);
    written["is_resume"] = json!(true);
    written["execution_id"] = json!("exec-1");
    rig.rows.lock().unwrap().insert(token.into(), written.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(written)?, StartMode::New).await
}

/// A job that is already done when the run starts waiting resumes it on
/// the first wake, which is pinned at the moment the wait was asked for
/// rather than an interval later. The answer is the response itself, it
/// routes to the parked run, and nothing polls after it.
#[tokio::test]
async fn a_wait_on_a_job_already_done_resumes_at_once() {
    let rig = rig(false).await;
    let (url, hits) = spawn_status(vec![(200, json!({ "status": "COMPLETED", "url": "https://cdn/x.mp4" }))]).await;
    let asked = now_ms();
    register_wait(&rig, "wait", &job_wait(&url), asked).await.unwrap();
    let armed = rig.alarm.wakes_for("signal:wait");
    assert_eq!(armed.len(), 1);
    assert_eq!(armed[0].at_unix_ms, asked, "the first poll is the moment the wait was asked for");

    wake(&rig.state, WakeBody { token: "wait".into(), due_at_ms: asked }).await.unwrap();
    {
        let fires = rig.tasks.enqueued.lock().unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].payload["payload"]["url"], "https://cdn/x.mp4");
    }
    assert_eq!(rig.alarm.wakes_for("signal:wait").len(), 1, "answered: no further poll is set");
    assert!(shown(&rig.state, "wait").await.contains("answered"));

    let outcome = weft_listener::kinds::process(&rig.state, "wait", json!({ "status": "COMPLETED" })).await.unwrap();
    assert!(
        matches!(&outcome.target, weft_core::signal::listener_protocol::ProcessTarget::Resume { execution_id } if execution_id == "exec-1"),
        "the fire resumes the parked run: {:?}",
        outcome.target
    );

    // A wake that was already set when the answer went out polls nothing.
    wake(&rig.state, WakeBody { token: "wait".into(), due_at_ms: now_ms() }).await.unwrap();
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(rig.tasks.enqueued.lock().unwrap().len(), 1);
}

/// A job still running is not an answer: the filter drops it and the wait
/// keeps polling on its interval. A non-2xx answer is a failed poll,
/// counted on the row and retried. The first answer that passes the
/// filter resumes the run, once.
#[tokio::test]
async fn a_wait_polls_until_the_job_is_done_and_resumes_once() {
    let rig = rig(false).await;
    let (url, hits) = spawn_status(vec![
        (200, json!({ "status": "IN_PROGRESS" })),
        (503, json!({ "error": "busy" })),
        (200, json!({ "status": "IN_PROGRESS" })),
        (200, json!({ "status": "FAILED", "error": "nsfw" })),
    ])
    .await;
    let asked = now_ms();
    register_wait(&rig, "wait", &job_wait(&url), asked).await.unwrap();

    for _ in 0..3 {
        wake(&rig.state, WakeBody { token: "wait".into(), due_at_ms: now_ms() }).await.unwrap();
        assert!(rig.tasks.enqueued.lock().unwrap().is_empty(), "not done yet: nothing fires");
    }
    let stored = rig.rows.lock().unwrap()["wait"]["kind_state"].clone();
    assert_eq!(stored, json!({}), "the failure streak cleared on the good poll after it");
    let wakes = rig.alarm.wakes_for("signal:wait");
    assert_eq!(wakes.len(), 4, "armed, then one more per poll");
    assert!(wakes[1].at_unix_ms > asked, "after the first poll, the interval sets the pace");

    wake(&rig.state, WakeBody { token: "wait".into(), due_at_ms: now_ms() }).await.unwrap();
    {
        let fires = rig.tasks.enqueued.lock().unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0].payload["payload"], json!({ "status": "FAILED", "error": "nsfw" }));
    }
    assert_eq!(rig.alarm.wakes_for("signal:wait").len(), 4, "answered: no further poll is set");
    assert_eq!(rig.rows.lock().unwrap()["wait"]["kind_state"], json!({ "resumed": true }));
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 4);
}

/// A non-2xx answer is shown on the node as a failed poll while the
/// wait goes on.
#[tokio::test]
async fn a_wait_whose_status_endpoint_fails_keeps_polling_and_says_so() {
    let rig = rig(false).await;
    let (url, _) = spawn_status(vec![(404, json!({ "error": "no such job" }))]).await;
    register_wait(&rig, "wait", &job_wait(&url), now_ms()).await.unwrap();
    wake(&rig.state, WakeBody { token: "wait".into(), due_at_ms: now_ms() }).await.unwrap();
    assert!(rig.tasks.enqueued.lock().unwrap().is_empty());
    assert!(shown(&rig.state, "wait").await.contains("404"), "{}", shown(&rig.state, "wait").await);
    assert_eq!(rig.alarm.wakes_for("signal:wait").len(), 2, "it polls again");
}

/// Delta mode cannot serve a run's wait (its first poll primes
/// silently), so the registration is refused, naming why.
#[tokio::test]
async fn a_wait_in_delta_mode_is_refused() {
    let rig = rig(false).await;
    let spec = to_spec(
        serde_json::from_value::<weft_core::signal::PollEndpoint>(json!({
            "url": "https://example.com/jobs",
            "delta": { "items": "items", "cursor_field": "id" },
        }))
        .unwrap(),
    );
    let err = register_wait(&rig, "wait", &spec, now_ms()).await.expect_err("refused");
    assert!(format!("{err:#}").contains("delta"), "{err:#}");
}

// ---------- Held connections: one bring-up path ----------

/// A held-connection spec whose loop only ever retries (nothing listens
/// on the port), so bringing it up needs nothing reachable.
fn sse_spec() -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::SseSubscribe {
        url: "http://127.0.0.1:1/events".into(),
        event_name: String::new(),
        filters: vec![],
    })
}

fn has_task(state: &ListenerState, token: &str) -> bool {
    state.registry.get(token).is_some_and(|sig| sig.task.is_some())
}

/// Two bring-ups of a held connection at once (a look racing a
/// registration's take) start it once: the second waits for the first and
/// finds it up, instead of replacing (and so stopping) the task the first
/// just started.
#[tokio::test]
async fn two_bring_ups_of_a_held_connection_start_it_once() {
    let rig = rig(true).await;
    let written = row("sse", &sse_spec(), json!({}), 1);
    rig.rows.lock().unwrap().insert("sse".into(), written.clone());
    let up = || weft_listener::registry::hold(&rig.state, serde_json::from_value(written.clone()).unwrap(), weft_core::signal::listener_protocol::StartMode::Restore);
    let (a, b) = tokio::join!(up(), up());
    a.unwrap();
    b.unwrap();
    let first = rig.state.registry.get("sse").expect("up");
    assert!(has_task(&rig.state, "sse"));
    let again = weft_listener::registry::held(&rig.state, "sse").await.unwrap().unwrap();
    assert!(Arc::ptr_eq(&first.serving, &again.serving), "one running connection answers");
}

/// A holder takes the held signals nobody holds, keeps them while it
/// claims them, and stops one whose claim went (its row was rewritten),
/// taking it up again fresh at its next look.
#[tokio::test]
async fn a_holder_takes_what_nobody_holds_and_stops_what_it_lost() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    let restore = weft_core::signal::listener_protocol::StartMode::Restore;
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    assert!(has_task(&rig.state, "sse"), "taken and up");
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    assert!(has_task(&rig.state, "sse"), "kept");
    rig.rows.lock().unwrap().get_mut("sse").unwrap()["held_by"] = Value::Null;
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    assert!(!has_task(&rig.state, "sse"), "its claim went, so it stopped");
    weft_listener::hold::take_now(&rig.state, &[], restore).await.unwrap();
    assert!(has_task(&rig.state, "sse"), "and was taken again");
}

/// A listener that does not hold never brings a held connection up: it
/// answers from the row, with what its holder last said.
#[tokio::test]
async fn a_listener_that_does_not_hold_answers_a_held_signal_from_its_row() {
    let rig = rig(false).await;
    let mut written = row("sse", &sse_spec(), json!({}), 1);
    written["serving"] = json!({ "status": "listening" });
    rig.rows.lock().unwrap().insert("sse".into(), written);
    let sig = weft_listener::registry::held(&rig.state, "sse").await.unwrap().expect("its row answers");
    assert!(sig.task.is_none());
    assert_eq!(sig.serving.lock().status, "listening");
    assert!(rig.state.registry.get("sse").is_none(), "nothing is brought up here");
    let down = weft_listener::registry::rehydrate(&rig.state, None, &[]).await.unwrap();
    assert!(down.is_empty() && rig.state.registry.get("sse").is_none(), "a rehydrate leaves it to the holders");
}

/// A held connection whose row cannot be read again once it is up keeps
/// running: the row was held when it was listed, and an unregister that
/// comes later still finds it in the registry.
#[tokio::test]
async fn a_held_connection_stays_up_when_its_row_cannot_be_read_again() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    rig.get_held.failing.store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
    let down = weft_listener::registry::rehydrate(&rig.state, None, &[]).await.unwrap();
    assert!(down.is_empty(), "{down:?}");
    assert!(has_task(&rig.state, "sse"), "a broker hiccup does not turn it off");
    assert!(rig.get_held.reads.load(std::sync::atomic::Ordering::SeqCst) > 1, "the re-read was retried");
    assert!(rig.state.registry.down_reason("sse").is_none());
}

/// A held connection that could not come up is down, says so, and comes
/// back by itself once it can: nothing else would ever name it again.
#[tokio::test]
async fn a_down_held_connection_is_shown_and_retried_until_it_comes_up() {
    let rig = rig(true).await;
    let mut unreadable = sse_spec();
    unreadable.config = json!({ "url": 5 });
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &unreadable, json!({}), 1));

    let down = weft_listener::registry::rehydrate(&rig.state, None, &[]).await.unwrap();
    assert_eq!(down.len(), 1, "{down:?}");
    let reason = rig.state.registry.down_reason("sse").expect("it is down");
    assert!(reason.contains("sse_subscribe"), "{reason}");
    let sig = weft_listener::registry::held(&rig.state, "sse").await.unwrap().expect("its row still answers");
    assert!(sig.task.is_none(), "a first use leaves the retrying to its loop");

    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    for _ in 0..100 {
        if has_task(&rig.state, "sse") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(has_task(&rig.state, "sse"), "the retry brought it up");
    assert_eq!(rig.rows.lock().unwrap()["sse"]["held_by"], "test-listener", "under a claim it took");
    assert!(rig.state.registry.down_reason("sse").is_none());
}

use weft_core::signal::listener_protocol::StartMode;

fn sse_at(url: &str) -> weft_core::primitive::SignalSpec {
    to_spec(weft_core::signal::SseSubscribe { url: url.into(), event_name: String::new(), filters: vec![] })
}

fn unreadable_sse() -> weft_core::primitive::SignalSpec {
    let mut spec = sse_spec();
    spec.config = json!({ "url": 5 });
    spec
}

/// A held connection whose row is deleted while it comes up (the
/// unregister passed before its task was in the registry) is let go once
/// the row is read again, instead of running for a row nobody holds.
#[tokio::test]
async fn a_held_connection_whose_row_went_while_it_came_up_is_let_go() {
    let rig = rig(true).await;
    let snapshot = row("sse", &sse_spec(), json!({}), 1);
    weft_listener::registry::hold(&rig.state, serde_json::from_value(snapshot).unwrap(), StartMode::Restore).await.unwrap();
    assert!(rig.state.registry.get("sse").is_none(), "the re-read found no row, so nothing holds it");
    assert!(rig.state.registry.down_reason("sse").is_none());
}

/// A row the dispatcher puts back replaces the replacement running under
/// the same token, so the listener ends up running what the row says.
#[tokio::test]
async fn a_put_back_row_replaces_the_connection_running_under_its_token() {
    let rig = rig(true).await;
    let replacement = row("sse", &sse_at("http://127.0.0.1:1/new"), json!({}), 1);
    rig.rows.lock().unwrap().insert("sse".into(), replacement.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(replacement).unwrap(), StartMode::New).await.unwrap();
    let running = rig.state.registry.get("sse").unwrap();

    let prior = row("sse", &sse_at("http://127.0.0.1:1/old"), json!({}), 2);
    rig.rows.lock().unwrap().insert("sse".into(), prior.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(prior).unwrap(), StartMode::PutBack).await.unwrap();

    let now = rig.state.registry.get("sse").unwrap();
    assert_eq!(now.spec.config["url"], "http://127.0.0.1:1/old", "the put-back row runs");
    assert!(!Arc::ptr_eq(&running.serving, &now.serving), "the replacement's connection was replaced");
}

/// A put-back that cannot come up stops the replacement it was undoing
/// (the row no longer says that spec) and is marked down and retried.
#[tokio::test]
async fn a_put_back_that_cannot_come_up_stops_the_replacement_and_is_retried() {
    let rig = rig(true).await;
    let replacement = row("sse", &sse_spec(), json!({}), 1);
    rig.rows.lock().unwrap().insert("sse".into(), replacement.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(replacement).unwrap(), StartMode::New).await.unwrap();

    let prior = row("sse", &unreadable_sse(), json!({}), 2);
    rig.rows.lock().unwrap().insert("sse".into(), prior.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(prior).unwrap(), StartMode::PutBack)
        .await
        .expect_err("the put-back row cannot come up");
    assert!(rig.state.registry.get("sse").is_none(), "the replacement no longer runs");
    assert!(rig.state.registry.down_reason("sse").is_some(), "the put-back row is down and retried");
    assert_eq!(rig.state.registry.retry_loops_running(), 1);
}

/// A row cleared and marked down again while its retry loop sleeps gets
/// one new loop, and the old one ends at its next turn: never two loops
/// for one token.
#[tokio::test]
async fn a_row_marked_down_again_keeps_one_retry_loop() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &unreadable_sse(), json!({}), 1));
    let bring_up = || async {
        let row = rig.rows.lock().unwrap()["sse"].clone();
        weft_listener::registry::hold(&rig.state, serde_json::from_value(row).unwrap(), StartMode::Restore).await
    };
    bring_up().await.expect_err("down");
    assert_eq!(rig.state.registry.retry_loops_running(), 1);
    rig.state.registry.clear_down("sse");
    bring_up().await.expect_err("down again");

    // Past the first loop's first wait, it has seen the entry is not its own.
    let first_wait = weft_core::time_scale::scaled(std::time::Duration::from_secs(1));
    tokio::time::sleep(first_wait + std::time::Duration::from_millis(500)).await;
    assert_eq!(rig.state.registry.retry_loops_running(), 1, "one loop for the token");
    assert!(rig.state.registry.down_reason("sse").is_some());
}

// ---------- Replacing a held connection ----------

/// A held kind that records, in order, each task it starts (by the spec's
/// `n`) and each outside teardown it is asked for, under the spec's
/// `test`: the tests of this binary share one process under `cargo test`,
/// so one log per test keeps them from reading each other's entries.
struct RecordingHold;

static RECORDED: Mutex<std::collections::BTreeMap<String, Vec<String>>> = Mutex::new(std::collections::BTreeMap::new());

fn recorded(test: &str) -> Vec<String> {
    RECORDED.lock().unwrap().get(test).cloned().unwrap_or_default()
}

fn record(spec: &weft_core::primitive::SignalSpec, entry: String) {
    let test = spec.config["test"].as_str().expect("a recording spec names its test").to_string();
    RECORDED.lock().unwrap().entry(test).or_default().push(entry);
}
const RECORDING_TAG: &str = "test_recording_hold";

#[async_trait::async_trait]
impl weft_listener::kinds::KindHandler for RecordingHold {
    fn tag(&self) -> &'static str {
        RECORDING_TAG
    }

    fn between_fires(&self, _spec: &weft_core::primitive::SignalSpec, _kind_state: &serde_json::Value) -> anyhow::Result<weft_listener::kinds::BetweenFires> {
        Ok(weft_listener::kinds::BetweenFires::Holds)
    }

    fn compute_routing(&self, _spec: &weft_core::primitive::SignalSpec) -> anyhow::Result<weft_core::primitive::SignalRouting> {
        anyhow::bail!("never registered through prepare in this test")
    }

    async fn spawn_task(
        &self,
        spec: &weft_core::primitive::SignalSpec,
        _kind_state: &Value,
        _ctx: weft_listener::kinds::SpawnCtx,
    ) -> anyhow::Result<Option<tokio::task::JoinHandle<()>>> {
        record(spec, format!("spawn {}", spec.config["n"]));
        Ok(Some(tokio::spawn(std::future::pending())))
    }

    fn process_entry(
        &self,
        _sig: &weft_listener::registry::RegisteredSignal,
        payload: Value,
    ) -> weft_core::signal::listener_protocol::ProcessOutcome {
        weft_core::signal::listener_protocol::ProcessOutcome {
            value: payload,
            target: weft_core::signal::listener_protocol::ProcessTarget::Entry,
        }
    }

    fn render(&self, _token: &str, _sig: &weft_listener::registry::RegisteredSignal) -> anyhow::Result<Option<Value>> {
        Ok(None)
    }

    async fn on_unregister(
        &self,
        _token: &str,
        sig: &weft_listener::registry::RegisteredSignal,
        _events_broker: &Arc<weft_broker_client::BrokerEventsClient>,
    ) {
        record(&sig.spec, format!("unregister {}", sig.spec.config["n"]));
    }
}

inventory::submit!(&RecordingHold as &dyn weft_listener::kinds::KindHandler);

fn recording_spec(test: &str, n: u32) -> weft_core::primitive::SignalSpec {
    weft_core::primitive::SignalSpec {
        kind: RECORDING_TAG.into(),
        config: json!({ "n": n, "test": test }),
        consumer_kind: None,
        access: None,
        match_predicates: Vec::new(),
        limits: Default::default(),
        run_class: Default::default(),
    }
}

/// Replacing a held connection tears the displaced one down (a provider
/// subscription of its spec is dropped) before the new one starts, since
/// that teardown is keyed by the token the new one reuses; forgetting it
/// later tears the new one down.
#[tokio::test]
async fn replacing_a_held_connection_tears_the_displaced_one_down_first() {
    let rig = rig(true).await;
    for n in 1..=2 {
        let written = row("rec", &recording_spec("replacing", n), json!({}), n.into());
        rig.rows.lock().unwrap().insert("rec".into(), written.clone());
        weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
            .await
            .unwrap();
    }
    assert_eq!(recorded("replacing"), ["spawn 1", "unregister 1", "spawn 2"]);

    weft_listener::kinds::forget(&rig.state, "rec");
    wait_for(|| recorded("replacing").len() == 4).await;
    assert_eq!(recorded("replacing")[3], "unregister 2");
    assert!(rig.state.registry.get("rec").is_none());
}

/// Forgetting a held connection and bringing the same token straight
/// back up tears the old one down before the new one starts: the
/// detached teardown holds the token's bring-up guard, so it cannot drop
/// the new subscription.
#[tokio::test]
async fn a_bring_up_right_after_forget_waits_for_the_old_teardown() {
    let rig = rig(true).await;
    let first = row("rec", &recording_spec("bring_up", 1), json!({}), 1);
    rig.rows.lock().unwrap().insert("rec".into(), first.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(first).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
        .await
        .unwrap();

    weft_listener::kinds::forget(&rig.state, "rec");
    let second = row("rec", &recording_spec("bring_up", 2), json!({}), 2);
    rig.rows.lock().unwrap().insert("rec".into(), second.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(second).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
        .await
        .unwrap();
    assert_eq!(recorded("bring_up"), ["spawn 1", "unregister 1", "spawn 2"]);
    assert!(rig.state.registry.get("rec").is_some(), "the new connection runs");
}

/// A held signal registered again in a process that already holds it is
/// taken again and comes up as its row now reads, the displaced one torn
/// down first.
#[tokio::test]
async fn a_held_signal_registered_again_here_comes_up_anew() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("rec".into(), row("rec", &recording_spec("again", 1), json!({}), 1));
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert_eq!(recorded("again"), ["spawn 1"]);

    rig.rows.lock().unwrap().get_mut("rec").unwrap()["spec_json"] = json!(serde_json::to_string(&recording_spec("again", 2)).unwrap());
    weft_listener::hold::take_now(&rig.state, &["rec".to_string()], StartMode::New).await.unwrap();
    assert_eq!(recorded("again"), ["spawn 1", "unregister 1", "spawn 2"]);
    assert_eq!(rig.state.registry.get("rec").unwrap().spec.config["n"], 2);
}

/// A held signal that went to another holder stops here and leaves what
/// it arranged outside to that holder; one whose row is gone stops and
/// tears that down.
#[tokio::test]
async fn a_lost_claim_stops_here_and_an_ended_signal_tears_down() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("rec".into(), row("rec", &recording_spec("lost", 1), json!({}), 1));
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();

    rig.rows.lock().unwrap().get_mut("rec").unwrap()["held_by"] = json!("another-holder");
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert!(rig.state.registry.get("rec").is_none(), "stopped here");

    rig.rows.lock().unwrap().get_mut("rec").unwrap()["held_by"] = Value::Null;
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    assert_eq!(recorded("lost"), ["spawn 1", "spawn 1"], "no teardown for a lost claim");

    rig.rows.lock().unwrap().remove("rec");
    weft_listener::hold::take_now(&rig.state, &[], StartMode::Restore).await.unwrap();
    wait_for(|| recorded("lost").len() == 3).await;
    assert_eq!(recorded("lost")[2], "unregister 1");
    assert!(rig.state.registry.get("rec").is_none());
}

/// A holder told to stop finishes its look, stops its connections before
/// it gives up their claims, and leaves what they arranged outside to the
/// next holder.
#[tokio::test]
async fn letting_go_stops_the_connections_first_and_tears_nothing_down() {
    let rig = rig(true).await;
    rig.rows.lock().unwrap().insert("rec".into(), row("rec", &recording_spec("let_go", 1), json!({}), 1));
    weft_listener::hold::run(rig.state.clone(), None, std::future::ready(())).await;
    assert!(rig.state.registry.get("rec").is_none());
    assert!(rig.rows.lock().unwrap()["rec"]["held_by"].is_null(), "the claim is given up");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(recorded("let_go"), ["spawn 1"]);
}

/// A wake left from a row that woke, for a row registered again to be held,
/// ends quietly instead of failing every retry of the alarm.
#[tokio::test]
async fn a_wake_for_a_signal_that_no_longer_wakes_ends_quietly() {
    let rig = rig(false).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    wake(&rig.state, WakeBody { token: "sse".into(), due_at_ms: now_ms() }).await.unwrap();
    assert!(rig.tasks.enqueued.lock().unwrap().is_empty());
    assert!(rig.alarm.wakes_for("signal:sse").is_empty());
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the detached teardown never ran");
}

// ---------- The `/wake` door ----------

/// Serve the listener's real router on a local port; answers its base URL.
async fn serve(state: ListenerState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, weft_listener::router(state)).await.unwrap() });
    format!("http://{addr}")
}

/// Every log line written while the guard lives, as text.
#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A body that can never be a wake is answered 200 (so a retrying queue
/// like Cloud Tasks stops), says it was dropped and why, and is logged
/// as an error carrying the body.
#[tokio::test]
async fn a_wake_body_that_can_never_be_read_is_dropped_with_a_success_and_logged() {
    let logs = Logs::default();
    let writer = logs.clone();
    let subscriber = tracing_subscriber::fmt().with_ansi(false).with_writer(move || writer.clone()).finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let rig = rig(false).await;
    let base = serve(rig.state.clone()).await;
    let http = reqwest::Client::new();
    for body in ["not json at all", r#"{"at_unix_ms": 1, "body": {"tok": "x"}}"#] {
        let answer = http.post(format!("{base}/wake")).body(body).send().await.unwrap();
        assert_eq!(answer.status(), 200, "{body}");
        let said: Value = answer.json().await.unwrap();
        assert_eq!(said["dropped"], true, "{said}");
        assert!(said["reason"].as_str().unwrap().contains("not a wake call"), "{said}");
    }
    let written = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(written.contains("ERROR") && written.contains("not json at all"), "{written}");
    assert!(rig.tasks.enqueued.lock().unwrap().is_empty());
}

/// A readable wake whose processing fails answers 500, so the alarm tries
/// it again.
#[tokio::test]
async fn a_readable_wake_that_fails_answers_500_for_a_retry() {
    let rig = rig(false).await;
    rig.get_held.failing.store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
    let base = serve(rig.state.clone()).await;
    let call = json!({ "at_unix_ms": now_ms(), "body": { "token": "tok", "due_at_ms": now_ms() } });
    let answer = reqwest::Client::new().post(format!("{base}/wake")).json(&call).send().await.unwrap();
    assert_eq!(answer.status(), 500);
}
