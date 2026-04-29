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
    journal::MockJournal,
    listener::{register_signal, ListenerBackend, ListenerHandle, ListenerPool},
    journal::SignalRegistration,
    project_store::MockProjectStore,
    slots::Slots,
    tenant::{self, TenantId},
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
        tenant: &TenantId,
        _namespace: &str,
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
            tenant_id: tenant.to_string(),
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
    async fn stop(&self, _tenant: &TenantId, _namespace: &str) -> anyhow::Result<()> {
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

async fn build_state(_tmp: &tempfile::TempDir) -> (DispatcherState, Arc<Mutex<Vec<WakeContext>>>) {
    let projects: weft_dispatcher::ProjectStore = Arc::new(MockProjectStore::new());
    let journal = MockJournal::new();
    let spawned = Arc::new(Mutex::new(Vec::new()));
    let config = DispatcherConfig {
        http_port: 0,
        worker_backend: "stub".into(),
        infra_backend: "stub".into(),
        dispatcher_callback_url: "http://127.0.0.1:0".into(),
        internal_url_template: "http://{pod}.test:9999".into(),
        internal_secret: "test-secret".into(),
    };
    // The test never touches leases so a lazy pool that never
    // opens a real connection is safe. `connect_lazy` returns
    // immediately and only attempts I/O on first query.
    let pg_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://test:test@127.0.0.1:1/test")
        .expect("lazy pool");

    let state = DispatcherState {
        config: Arc::new(config),
        pod_id: weft_dispatcher::state::PodId("test-pod".into()),
        journal: Arc::new(journal),
        pg_pool,
        workers: Arc::new(RecordingWorkerBackend { spawned: spawned.clone() }),
        infra: Arc::new(StubInfraBackend),
        projects,
        events: weft_dispatcher::EventBus::new(),
        slots: Slots::new(),
        listener_backend: Arc::new(InProcessListenerBackend),
        listeners: ListenerPool::new(),
        infra_registry: weft_dispatcher::infra::InfraRegistry::new(),
        tenant_router: tenant::local_router(),
        namespace_mapper: tenant::local_namespace_mapper(),
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
    let tenant = TenantId::local();
    let handle = listener_backend
        .spawn(&tenant, "wm-local", "http://127.0.0.1:65535")
        .await
        .unwrap();
    listeners.insert(tenant.to_string(), handle.clone());

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

    // Persist a signal row so /signal-fired routes correctly.
    journal
        .signal_insert(&SignalRegistration {
            token: token.clone(),
            tenant_id: tenant.to_string(),
            project_id: project_id.to_string(),
            color: None,
            node_id: "receive".to_string(),
            is_resume: false,
            user_url: user_url.clone(),
            kind: "webhook".to_string(),
            spec_json: serde_json::to_string(&spec).unwrap(),
        })
        .await
        .unwrap();

    // Sanity: the project store has this project registered.
    assert!(project_store.get(project_id).await.is_some());
    // Silence the journal import if not otherwise referenced.
    let _ = &journal;

    // 3. Simulate the listener firing by POSTing /signal-fired.
    let fire_body = serde_json::json!({
        "tenant_id": tenant.to_string(),
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
