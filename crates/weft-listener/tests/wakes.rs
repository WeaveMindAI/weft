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
use weft_platform_traits::{FakeAlarm, Placement};

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
    async fn wait_cancels(
        &self,
        _project_id: uuid::Uuid,
        _execution_ids: Vec<String>,
        _wait: std::time::Duration,
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
    async fn claim_one(
        &self,
        _: &str,
        _: weft_task_store::tasks::ClaimFilter,
        _: std::time::Duration,
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
    let app = axum::Router::new()
        .route(
            "/v1/signal/list_held",
            post(move |axum::Json(_): axum::Json<Value>| {
                let rows = list_rows.clone();
                async move { axum::Json(json!({ "rows": rows.lock().unwrap().values().cloned().collect::<Vec<_>>() })) }
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
                    let row = rows.lock().unwrap().get(req["token"].as_str().unwrap()).cloned();
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
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

struct Rig {
    state: ListenerState,
    tasks: Arc<FakeTasks>,
    alarm: Arc<FakeAlarm>,
    rows: Rows,
    landing: Landing,
    get_held: Arc<GetHeld>,
}

async fn rig(placement: Placement) -> Rig {
    let rows: Rows = Arc::default();
    let landing: Landing = Arc::default();
    let get_held: Arc<GetHeld> = Arc::default();
    let broker = spawn_broker(rows.clone(), landing.clone(), get_held.clone()).await;
    let tasks = Arc::new(FakeTasks::default());
    let alarm = Arc::new(FakeAlarm::new());
    let state = ListenerState::new(
        ListenerConfig { replica: "test-listener".into(), broker_url: broker, placement },
        tasks.clone(),
        weft_broker_client::TokenSource::role(
            Arc::new(weft_platform_traits::FixedToken("test-token".into())),
            "test-listener",
            weft_platform_traits::CoreRole::Listener,
        ),
        alarm.clone(),
    );
    Rig { state, tasks, alarm, rows, landing, get_held }
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
    json!({
        "token": token, "tenant_id": "tenant-a", "for_instance": null, "node_id": "tick",
        "spec_json": serde_json::to_string(spec).unwrap(), "is_resume": false, "execution_id": null,
        "surface_kind": "internal", "mount_path": null, "mount_methods": [], "auth_kind": "none",
        "auth_config": null, "kind_state": kind_state, "kind_state_seq": seq
    })
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Register a signal the way the dispatcher does: prepare it (nothing
/// starts), write its row, then bring it up. Answers the prepared state.
async fn register(rig: &Rig, token: &str, spec: &weft_core::primitive::SignalSpec, asked_at_unix_ms: i64) -> anyhow::Result<Value> {
    let prepared = prepare_signal(&rig.state, identity(token, spec.clone()), None, asked_at_unix_ms)?;
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
    let rig = rig(Placement::Serverless).await;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 60_000 } });
    let prepared = prepare_signal(&rig.state, identity("good", spec.clone()), None, now_ms()).unwrap();
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
    let rig = rig(Placement::Serverless).await;
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
    let rig = rig(Placement::Machine).await;
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
    let rig = rig(Placement::Serverless).await;
    let asked = now_ms() - 5_000;
    let spec = to_spec(Timer { spec: TimerSpec::After { duration_ms: 1_000 } });
    register(&rig, "tok", &spec, asked).await.unwrap();
    let fresh = json!({ "next_fire_at_unix_ms": asked + 3_600_000 });
    *rig.landing.lock().unwrap() = Some(fresh.clone());

    wake(&rig.state, WakeBody { token: "tok".into(), due_at_ms: asked + 1_000 }).await.unwrap();
    let stored = rig.rows.lock().unwrap()["tok"].clone();
    assert_eq!((stored["kind_state"].clone(), stored["kind_state_seq"].clone()), (fresh, json!(2)), "the new schedule stands");
}

/// A kind that holds a connection open is refused by a listener that
/// scales to zero, naming the setting to change.
#[tokio::test]
async fn a_serverless_listener_refuses_a_kind_that_holds_a_connection() {
    let rig = rig(Placement::Serverless).await;
    let spec = to_spec(weft_core::signal::SseSubscribe { url: "https://example.com/feed".into(), event_name: String::new(), filters: Vec::new() });
    let err = prepare_signal(&rig.state, identity("sse", spec), None, now_ms()).expect_err("refused");
    let text = format!("{err:#}");
    assert!(text.contains("roles.listener"), "{text}");
    assert!(rig.state.registry.get("sse").is_none());
}

/// A tick that could not be enqueued fails the wake and leaves the moment
/// unclaimed, so the alarm's retry fires it: a one-shot is never lost to
/// a broker that was down for a moment.
#[tokio::test]
async fn a_tick_that_could_not_be_enqueued_is_fired_by_the_retried_wake() {
    let rig = rig(Placement::Serverless).await;
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
    let rig = rig(Placement::Serverless).await;
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
    ListenerState::new(
        (*rig.state.config).clone(),
        rig.tasks.clone(),
        weft_broker_client::TokenSource::role(
            Arc::new(weft_platform_traits::FixedToken("test-token".into())),
            "test-listener",
            weft_platform_traits::CoreRole::Listener,
        ),
        rig.alarm.clone(),
    )
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
    let first = rig(Placement::Serverless).await;
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

/// Two first uses of a held connection at once start it once: the second
/// waits for the first and finds it up, instead of replacing (and so
/// stopping) the task the first just started.
#[tokio::test]
async fn two_first_uses_of_a_held_connection_start_it_once() {
    let rig = rig(Placement::Machine).await;
    rig.rows.lock().unwrap().insert("sse".into(), row("sse", &sse_spec(), json!({}), 1));
    let (a, b) = tokio::join!(
        weft_listener::registry::held(&rig.state, "sse"),
        weft_listener::registry::held(&rig.state, "sse"),
    );
    let (a, b) = (a.unwrap().unwrap(), b.unwrap().unwrap());
    assert!(Arc::ptr_eq(&a.serving, &b.serving), "both answers are the one running connection");
    assert!(has_task(&rig.state, "sse"));
}

/// A held connection whose row cannot be read again once it is up keeps
/// running: the row was held when it was listed, and an unregister that
/// comes later still finds it in the registry.
#[tokio::test]
async fn a_held_connection_stays_up_when_its_row_cannot_be_read_again() {
    let rig = rig(Placement::Machine).await;
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
    let rig = rig(Placement::Machine).await;
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
    let rig = rig(Placement::Machine).await;
    let snapshot = row("sse", &sse_spec(), json!({}), 1);
    weft_listener::registry::hold(&rig.state, serde_json::from_value(snapshot).unwrap(), StartMode::Restore).await.unwrap();
    assert!(rig.state.registry.get("sse").is_none(), "the re-read found no row, so nothing holds it");
    assert!(rig.state.registry.down_reason("sse").is_none());
}

/// A row the dispatcher puts back replaces the replacement running under
/// the same token, so the listener ends up running what the row says.
#[tokio::test]
async fn a_put_back_row_replaces_the_connection_running_under_its_token() {
    let rig = rig(Placement::Machine).await;
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
    let rig = rig(Placement::Machine).await;
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
    let rig = rig(Placement::Machine).await;
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
/// `n`) and each outside teardown it is asked for.
struct RecordingHold;

static RECORDED: Mutex<Vec<String>> = Mutex::new(Vec::new());
const RECORDING_TAG: &str = "test_recording_hold";

#[async_trait::async_trait]
impl weft_listener::kinds::KindHandler for RecordingHold {
    fn tag(&self) -> &'static str {
        RECORDING_TAG
    }

    fn between_fires(&self) -> weft_listener::kinds::BetweenFires {
        weft_listener::kinds::BetweenFires::Holds
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
        RECORDED.lock().unwrap().push(format!("spawn {}", spec.config["n"]));
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
        RECORDED.lock().unwrap().push(format!("unregister {}", sig.spec.config["n"]));
    }
}

inventory::submit!(&RecordingHold as &dyn weft_listener::kinds::KindHandler);

fn recording_spec(n: u32) -> weft_core::primitive::SignalSpec {
    weft_core::primitive::SignalSpec {
        kind: RECORDING_TAG.into(),
        config: json!({ "n": n }),
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
    let rig = rig(Placement::Machine).await;
    for n in 1..=2 {
        let written = row("rec", &recording_spec(n), json!({}), n.into());
        rig.rows.lock().unwrap().insert("rec".into(), written.clone());
        weft_listener::registry::hold(&rig.state, serde_json::from_value(written).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
            .await
            .unwrap();
    }
    assert_eq!(*RECORDED.lock().unwrap(), ["spawn 1", "unregister 1", "spawn 2"]);

    weft_listener::kinds::forget(&rig.state, "rec");
    wait_for(|| RECORDED.lock().unwrap().len() == 4).await;
    assert_eq!(RECORDED.lock().unwrap()[3], "unregister 2");
    assert!(rig.state.registry.get("rec").is_none());
}

/// Forgetting a held connection and bringing the same token straight
/// back up tears the old one down before the new one starts: the
/// detached teardown holds the token's bring-up guard, so it cannot drop
/// the new subscription.
#[tokio::test]
async fn a_bring_up_right_after_forget_waits_for_the_old_teardown() {
    let rig = rig(Placement::Machine).await;
    let first = row("rec", &recording_spec(1), json!({}), 1);
    rig.rows.lock().unwrap().insert("rec".into(), first.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(first).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
        .await
        .unwrap();

    weft_listener::kinds::forget(&rig.state, "rec");
    let second = row("rec", &recording_spec(2), json!({}), 2);
    rig.rows.lock().unwrap().insert("rec".into(), second.clone());
    weft_listener::registry::hold(&rig.state, serde_json::from_value(second).unwrap(), weft_core::signal::listener_protocol::StartMode::New)
        .await
        .unwrap();
    assert_eq!(*RECORDED.lock().unwrap(), ["spawn 1", "unregister 1", "spawn 2"]);
    assert!(rig.state.registry.get("rec").is_some(), "the new connection runs");
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

    let rig = rig(Placement::Serverless).await;
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
    let rig = rig(Placement::Serverless).await;
    rig.get_held.failing.store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
    let base = serve(rig.state.clone()).await;
    let call = json!({ "at_unix_ms": now_ms(), "body": { "token": "tok", "due_at_ms": now_ms() } });
    let answer = reqwest::Client::new().post(format!("{base}/wake")).json(&call).send().await.unwrap();
    assert_eq!(answer.status(), 500);
}
