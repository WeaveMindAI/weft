//! Integration test: run the listener + a mock dispatcher on
//! ephemeral ports, register a webhook + a 100ms timer, verify
//! both relay to the mock dispatcher.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use serde_json::Value;
use tokio::sync::Mutex;
use weft_core::primitive::{
    TimerSpec, WakeSignalKind, WakeSignalSpec, WebhookAuth,
};
use weft_listener::{router, ListenerConfig, ListenerState};

#[derive(Clone, Default)]
struct MockDispatcher {
    fires: Arc<Mutex<Vec<Value>>>,
    relay_token: Arc<String>,
}

async fn signal_fired(
    State(state): State<MockDispatcher>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    let bearer: &str = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    if bearer != state.relay_token.as_str() {
        return StatusCode::UNAUTHORIZED;
    }
    state.fires.lock().await.push(body);
    StatusCode::ACCEPTED
}

async fn spawn_mock_dispatcher(relay_token: String) -> (MockDispatcher, String) {
    let state = MockDispatcher {
        fires: Arc::new(Mutex::new(Vec::new())),
        relay_token: Arc::new(relay_token),
    };
    let app = Router::new()
        .route("/signal-fired", post(signal_fired))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (state, format!("http://{addr}"))
}

async fn spawn_listener(dispatcher_url: String) -> (ListenerState, String, String) {
    let admin_token = "admin".to_string();
    let relay_token = "relay".to_string();
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let public_base = format!("http://{addr}");
    let config = ListenerConfig {
        project_id: "test-project".into(),
        http_port: addr.port(),
        public_base_url: public_base.clone(),
        dispatcher_url,
        relay_token: relay_token.clone(),
        admin_token: admin_token.clone(),
    };
    let state = ListenerState::new(config);
    let app = router(state.clone());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (state, public_base, admin_token)
}

#[tokio::test]
async fn webhook_fire_relays_to_dispatcher() {
    let (mock, dispatcher_url) = spawn_mock_dispatcher("relay".into()).await;
    let (_state, listener_base, admin_token) = spawn_listener(dispatcher_url).await;

    // Register a webhook signal.
    let client = reqwest::Client::new();
    let token = "tok-webhook".to_string();
    let spec = WakeSignalSpec {
        kind: WakeSignalKind::Webhook {
            path: "".into(),
            auth: WebhookAuth::None,
        },
        is_resume: false,
    };
    let reg = client
        .post(format!("{listener_base}/register"))
        .bearer_auth(&admin_token)
        .json(&serde_json::json!({
            "token": token,
            "spec": spec,
            "node_id": "node-1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg.status(), StatusCode::OK);
    let reg_body: serde_json::Value = reg.json().await.unwrap();
    let url = reg_body.get("user_url").and_then(|v| v.as_str()).unwrap();
    assert!(url.contains("/signal/tok-webhook"));

    // Fire it.
    let fire = client
        .post(url)
        .json(&serde_json::json!({"hello": "world"}))
        .send()
        .await
        .unwrap();
    assert_eq!(fire.status(), StatusCode::ACCEPTED);

    // Relay landed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let fires = mock.fires.lock().await;
    assert_eq!(fires.len(), 1);
    assert_eq!(fires[0].get("token").and_then(|v| v.as_str()), Some("tok-webhook"));
    assert_eq!(
        fires[0].get("payload").and_then(|v| v.get("hello")).and_then(|v| v.as_str()),
        Some("world")
    );
}

#[tokio::test]
async fn timer_after_fires_once() {
    let (mock, dispatcher_url) = spawn_mock_dispatcher("relay".into()).await;
    let (_state, listener_base, admin_token) = spawn_listener(dispatcher_url).await;

    let client = reqwest::Client::new();
    let spec = WakeSignalSpec {
        kind: WakeSignalKind::Timer {
            spec: TimerSpec::After { duration_ms: 100 },
        },
        is_resume: false,
    };
    let reg = client
        .post(format!("{listener_base}/register"))
        .bearer_auth(&admin_token)
        .json(&serde_json::json!({
            "token": "tok-timer",
            "spec": spec,
            "node_id": "node-1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg.status(), StatusCode::OK);
    let reg_body: serde_json::Value = reg.json().await.unwrap();
    assert!(reg_body.get("user_url").and_then(|v| v.as_str()).is_none());

    tokio::time::sleep(Duration::from_millis(400)).await;
    let fires = mock.fires.lock().await;
    assert_eq!(fires.len(), 1, "After(100ms) should fire exactly once");
    assert_eq!(
        fires[0].get("token").and_then(|v| v.as_str()),
        Some("tok-timer")
    );
}

#[tokio::test]
async fn unregister_aborts_timer() {
    let (mock, dispatcher_url) = spawn_mock_dispatcher("relay".into()).await;
    let (_state, listener_base, admin_token) = spawn_listener(dispatcher_url).await;

    let client = reqwest::Client::new();
    let spec = WakeSignalSpec {
        kind: WakeSignalKind::Timer {
            spec: TimerSpec::After { duration_ms: 300 },
        },
        is_resume: false,
    };
    client
        .post(format!("{listener_base}/register"))
        .bearer_auth(&admin_token)
        .json(&serde_json::json!({
            "token": "tok-abort",
            "spec": spec,
            "node_id": "node-1",
        }))
        .send()
        .await
        .unwrap();

    // Unregister before the 300ms elapses.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let un = client
        .post(format!("{listener_base}/unregister"))
        .bearer_auth(&admin_token)
        .json(&serde_json::json!({"token": "tok-abort"}))
        .send()
        .await
        .unwrap();
    assert_eq!(un.status(), StatusCode::NO_CONTENT);

    // Wait past the original fire time; should be no relay.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let fires = mock.fires.lock().await;
    assert!(fires.is_empty(), "unregistered timer must not fire");
}

#[tokio::test]
async fn admin_routes_require_token() {
    let (_mock, dispatcher_url) = spawn_mock_dispatcher("relay".into()).await;
    let (_state, listener_base, _admin_token) = spawn_listener(dispatcher_url).await;

    let client = reqwest::Client::new();
    let spec = WakeSignalSpec {
        kind: WakeSignalKind::Webhook {
            path: "".into(),
            auth: WebhookAuth::None,
        },
        is_resume: false,
    };
    let reg = client
        .post(format!("{listener_base}/register"))
        // No bearer auth.
        .json(&serde_json::json!({
            "token": "tok",
            "spec": spec,
            "node_id": "node-1",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(reg.status(), StatusCode::UNAUTHORIZED);
}
