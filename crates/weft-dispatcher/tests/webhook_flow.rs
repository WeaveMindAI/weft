//! Integration: register a signal directly with a live in-process
//! listener, simulate the listener firing by POSTing
//! `/signal-fired` on the dispatcher, verify the dispatcher spawns
//! a worker. Bypasses the full activate flow (which would need a
//! real worker to run the TriggerSetup sub-execution); activation
//! gets its own higher-level coverage in the slice's end-to-end
//! story.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tokio::sync::Mutex;
use tower::util::ServiceExt;
use uuid::Uuid;

use weft_dispatcher::{
    api::router,
    backend::{EventStream, InfraBackend, InfraHandle, InfraSpec, WakeContext, WorkerBackend, WorkerHandle},
    config::DispatcherConfig,
    journal::sqlite::SqliteJournal,
    listener::{
        register_signal, ListenerBackend, ListenerHandle, ListenerRegistry, RegisteredSignalMeta,
        SignalTracker,
    },
    project_store::ProjectStore,
    slots::Slots,
    DispatcherState,
};

// ----- Stub backends ------------------------------------------------

struct RecordingWorkerBackend {
    spawned: Arc<Mutex<Vec<WakeContext>>>,
}

#[async_trait]
impl WorkerBackend for RecordingWorkerBackend {
    async fn spawn_worker(&self, wake: WakeContext) -> anyhow::Result<WorkerHandle> {
        self.spawned.lock().await.push(wake);
        Ok(WorkerHandle {
            id: uuid::Uuid::new_v4().to_string(),
        })
    }
    async fn kill_worker(&self, _handle: WorkerHandle) -> anyhow::Result<()> {
        Ok(())
    }
}

struct StubInfraBackend;

#[async_trait]
impl InfraBackend for StubInfraBackend {
    async fn provision(&self, _spec: InfraSpec) -> anyhow::Result<InfraHandle> {
        anyhow::bail!("not used in this test")
    }
    async fn scale_to_zero(&self, _h: &InfraHandle) -> anyhow::Result<()> {
        Ok(())
    }
    async fn scale_up(&self, _h: &InfraHandle) -> anyhow::Result<()> {
        Ok(())
    }
    async fn delete(&self, _h: InfraHandle) -> anyhow::Result<()> {
        Ok(())
    }
    async fn stream_events(&self, _h: InfraHandle) -> anyhow::Result<EventStream> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(rx)
    }
}

/// In-process listener: runs the real weft-listener code as a tokio
/// server on an ephemeral port. No subprocess needed.
struct InProcessListenerBackend;

#[async_trait]
impl ListenerBackend for InProcessListenerBackend {
    async fn spawn(
        &self,
        project_id: &str,
        dispatcher_url: &str,
    ) -> anyhow::Result<ListenerHandle> {
        use weft_listener::{router as listener_router, ListenerConfig, ListenerState};
        let tcp =
            tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = tcp.local_addr()?;
        let admin_url = format!("http://{addr}");
        let admin_token = Uuid::new_v4().to_string();
        let relay_token = Uuid::new_v4().to_string();
        let cfg = ListenerConfig {
            project_id: project_id.to_string(),
            http_port: addr.port(),
            public_base_url: admin_url.clone(),
            dispatcher_url: dispatcher_url.to_string(),
            relay_token: relay_token.clone(),
            admin_token: admin_token.clone(),
        };
        let state = ListenerState::new(cfg);
        let app = listener_router(state);
        tokio::spawn(async move {
            let _ = axum::serve(tcp, app).await;
        });
        Ok(ListenerHandle {
            admin_url: admin_url.clone(),
            public_base_url: admin_url,
            admin_token,
            relay_token,
        })
    }
    async fn stop(&self, _project_id: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

// ----- Helpers -------------------------------------------------------

fn hello_project() -> (uuid::Uuid, serde_json::Value) {
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
        slots: Slots::new(),
        listener_backend: Arc::new(InProcessListenerBackend),
        listeners: ListenerRegistry::new(),
        signal_tracker: SignalTracker::new(),
        infra_registry: weft_dispatcher::infra::InfraRegistry::new(),
    };
    (state, spawned)
}

fn req(method: &str, uri: &str, body: Option<serde_json::Value>) -> Request<Body> {
    let b = Request::builder().method(method).uri(uri);
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
async fn signal_fired_spawns_worker() {
    let tmp = tempfile::tempdir().unwrap();
    let (state, spawned) = build_state(&tmp).await;
    let signal_tracker = state.signal_tracker.clone();
    let listeners = state.listeners.clone();
    let listener_backend = state.listener_backend.clone();
    let journal = state.journal.clone();
    let project_store = state.projects.clone();

    let app = router(state);

    // 1. Register the project.
    let (project_id, project_json) = hello_project();
    let resp = app
        .clone()
        .oneshot(req("POST", "/projects", Some(project_json)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Journal an ExecutionStarted so /signal-fired's resume
    //    lookup (if any) has a project-for-color binding, then
    //    manually set up a listener + register a signal with it.
    //    Skip the full activate path (which needs a live worker).
    let handle = listener_backend
        .spawn(&project_id.to_string(), "http://127.0.0.1:65535")
        .await
        .unwrap();
    listeners.insert(project_id.to_string(), handle.clone());

    // Register a webhook signal.
    let token = "tok-webhook".to_string();
    let spec = weft_core::primitive::WakeSignalSpec {
        kind: weft_core::primitive::WakeSignalKind::Webhook {
            path: "".into(),
            auth: weft_core::primitive::WebhookAuth::None,
        },
        is_resume: false,
    };
    let user_url = register_signal(&handle, &token, &spec, "receive")
        .await
        .expect("listener register");
    assert!(user_url.is_some());

    // Put a tracker entry so /signal-fired routes correctly.
    signal_tracker.insert(
        token.clone(),
        RegisteredSignalMeta {
            project_id: project_id.to_string(),
            token: token.clone(),
            node_id: "receive".to_string(),
            is_resume: false,
            user_url: user_url.clone(),
            kind: "webhook".to_string(),
        },
    );

    // Sanity: the project store has this project registered.
    assert!(project_store.get(project_id).await.is_some());
    // Silence the journal import if not otherwise referenced.
    let _ = &journal;

    // 3. Simulate the listener firing by POSTing /signal-fired.
    let fire_body = serde_json::json!({
        "project_id": project_id.to_string(),
        "token": token,
        "payload": { "hello": "world" },
    });
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/signal-fired")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {}", handle.relay_token))
                .body(Body::from(serde_json::to_vec(&fire_body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 4. Confirm a worker was spawned.
    let spawned = spawned.lock().await;
    assert_eq!(spawned.len(), 1, "exactly one worker should have spawned");
    assert_eq!(spawned[0].project_id, project_id.to_string());
}
