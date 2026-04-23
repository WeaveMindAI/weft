//! Integration test: spin up a dispatcher in-process, register a
//! project with a webhook, activate it, fire a POST to the minted
//! URL, verify a worker spawned. This exercises the full CLI-free
//! lifecycle end to end.
//!
//! Note: the subprocess worker backend shells out to `weft-runner`,
//! which is awkward in a cargo test. We use a stub worker backend
//! that captures spawn calls without actually running the binary.
//! That's enough to verify the dispatcher's routing is correct.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tokio::sync::Mutex;
use tower::ServiceExt;

use weft_dispatcher::api::router;
use weft_dispatcher::backend::{
    EventStream, InfraBackend, InfraHandle, InfraSpec, WakeContext, WorkerBackend, WorkerHandle,
};
use weft_dispatcher::journal::sqlite::SqliteJournal;
use weft_dispatcher::{DispatcherConfig, DispatcherState, ProjectStore};

// ----- Test-only backends --------------------------------------------

struct RecordingWorkerBackend {
    spawned: Arc<Mutex<Vec<WakeContext>>>,
}

#[async_trait]
impl WorkerBackend for RecordingWorkerBackend {
    async fn spawn_worker(
        &self,
        _binary_path: &std::path::Path,
        wake: WakeContext,
    ) -> anyhow::Result<WorkerHandle> {
        self.spawned.lock().await.push(wake);
        Ok(WorkerHandle { id: "stub".into() })
    }
    async fn kill_worker(&self, _h: WorkerHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct StubInfraBackend;

#[async_trait]
impl InfraBackend for StubInfraBackend {
    async fn provision(&self, _spec: InfraSpec) -> anyhow::Result<InfraHandle> {
        anyhow::bail!("not used in this test")
    }
    async fn deprovision(&self, _h: InfraHandle) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stream_events(&self, _h: InfraHandle) -> anyhow::Result<EventStream> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}

// ----- Helpers -------------------------------------------------------

fn hello_project() -> (uuid::Uuid, serde_json::Value) {
    // ApiPost -> Debug. The webhook URL targets `receive`; when
    // `curl` fires it, the runner wakes `receive`, pulses through to
    // `print`, which logs the body.
    //
    // The dispatcher compiles + enriches source on register, so the
    // test just hands it the raw weft + a stable id.
    let source = r#"
# Project: Hello Webhook

receive = ApiPost
print = Debug { label: "got" }

print.value = receive.body
"#;
    let id = uuid::Uuid::new_v4();
    let payload = serde_json::json!({
        "id": id,
        "name": "Hello Webhook",
        "source": source,
    });
    (id, payload)
}

async fn build_state(tmp: &tempfile::TempDir) -> (DispatcherState, Arc<Mutex<Vec<WakeContext>>>) {
    let data_dir = tmp.path().to_path_buf();
    let projects_dir = data_dir.join("projects");
    let projects = ProjectStore::new(projects_dir).unwrap();
    let journal = SqliteJournal::open(&data_dir.join("journal.sqlite"))
        .await
        .unwrap();
    let spawned = Arc::new(Mutex::new(Vec::new()));
    let config = DispatcherConfig {
        http_port: 0,
        data_dir,
        worker_backend: "stub".into(),
        infra_backend: "stub".into(),
    };
    let state = DispatcherState {
        config: Arc::new(config),
        journal: Arc::new(journal),
        workers: Arc::new(RecordingWorkerBackend { spawned: spawned.clone() }),
        infra: Arc::new(StubInfraBackend),
        projects,
        events: weft_dispatcher::EventBus::new(),
        slots: weft_dispatcher::slots::Slots::new(),
        scheduler: weft_dispatcher::scheduler::Scheduler::new(),
    };
    (state, spawned)
}

fn req(method: &str, uri: &str, body: Option<serde_json::Value>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    }
}

// ----- The test -----------------------------------------------------

#[tokio::test]
async fn webhook_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let (state, spawned) = build_state(&tmp).await;
    let app = router(state);

    // 1. Register the project.
    let (project_id, project_json) = hello_project();
    let resp = app
        .clone()
        .oneshot(req("POST", "/projects", Some(project_json)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Activate to mint webhook URLs.
    let resp = app
        .clone()
        .oneshot(req("POST", &format!("/projects/{project_id}/activate"), None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let activate_body: serde_json::Value =
        serde_json::from_slice(&axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    let urls = activate_body.get("urls").and_then(|v| v.as_array()).unwrap().clone();
    assert_eq!(urls.len(), 1, "expected one webhook URL");
    let url = urls[0].get("url").and_then(|v| v.as_str()).unwrap();
    // Extract token from /w/{token} suffix.
    let after = url.split("/w/").nth(1).unwrap();
    let token = after.split('/').next().unwrap();

    // 3. Fire the webhook with a JSON body.
    let resp = app
        .clone()
        .oneshot(req(
            "POST",
            &format!("/w/{token}"),
            Some(serde_json::json!({"hello": "world"})),
        ))
        .await
        .unwrap();
    let status = resp.status();
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "expected 202 from /w/{token}, got {status}: {}",
        String::from_utf8_lossy(&body_bytes)
    );

    // 4. Confirm a worker spawn was recorded. Under Slice 3 the
    // wake payload lives in the slot queue (delivered over WS once
    // the worker connects), not on WakeContext; only `project_id`
    // and `color` sit on the spawn-time handoff. The WS round-trip
    // is covered by its own integration test.
    let spawned = spawned.lock().await;
    assert_eq!(spawned.len(), 1, "exactly one worker should have spawned");
    let wake = &spawned[0];
    assert_eq!(wake.project_id, project_id.to_string());
}
