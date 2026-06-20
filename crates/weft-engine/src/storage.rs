//! Worker-side storage data path: the lazily-ensured box endpoint +
//! the ensure-then-retry policy around `weft_storage`'s HTTP client.
//!
//! The worker never assumes the box exists. Before the first call
//! (and after any box-unreachable error, since the scale-to-zero
//! reaper may have torn the box down mid-execution) it asks the
//! dispatcher to ensure the box via the task queue and caches the
//! returned endpoint. From then on bytes flow worker<->box directly.
//!
//! Retry policy: an UNREACHABLE error means the request never
//! reached the box (no side effect happened), so every verb retries
//! after a forced re-ensure, EXCEPT `put`: its body stream is
//! consumed by the first attempt and cannot be replayed; it
//! re-ensures (so the caller's retry is hot) and fails loudly.
//!
//! The re-ensure waits for the box Deployment rollout, but a box pod
//! RESTART (a `grow`/`shrink` disk op restarts the single pod) leaves
//! a short window where the pod is Ready yet the Service endpoint /
//! cluster DNS has not finished re-propagating, so the very next
//! request still connection-fails. A single immediate retry races that
//! window; the replayable verbs therefore retry a few times with a
//! short backoff (a free intra-cluster call, no paid API), which is
//! enough to cover endpoint propagation after the pod is back.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use weft_core::error::{WeftError, WeftResult};
use weft_core::storage::{ByteRange, ByteStream, KeepTtl, StorageScope, StoredFileMeta};
use weft_core::Color;
use weft_storage::client::{BoxClient, ClientIdentity, StorageClientError, StorageOps};
use weft_task_store::traits::TaskStoreClient;
use weft_task_store::TaskKind;

/// Color-parameterized storage surface the `ContextHandle` storage
/// methods delegate to. One impl per worker process; Layer-3 tests
/// inject a fake.
#[async_trait]
pub trait WorkerStorageOps: Send + Sync {
    async fn put(
        &self,
        color: Color,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        data: ByteStream,
    ) -> WeftResult<Value>;
    async fn get(
        &self,
        color: Color,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)>;
    async fn delete(&self, color: Color, key: &str) -> WeftResult<()>;
    async fn list(&self, color: Color, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>>;
    async fn keep(&self, color: Color, key: &str, ttl: KeepTtl) -> WeftResult<()>;
    async fn presign(&self, color: Color, key: &str, ttl_secs: Option<u64>) -> WeftResult<String>;

    /// Eager terminate sweep: delete the color's un-kept exec files
    /// on clean finish. Best-effort BY CONTRACT (errors log, never
    /// fail the terminal path): the dispatcher's durable sweep is
    /// the guarantee; this only reclaims space a few seconds sooner.
    /// A no-op when this worker process never touched storage (no
    /// cached endpoint): nothing was stored, and ensuring a box just
    /// to sweep nothing would be provisioning churn.
    async fn eager_sweep(&self, color: Color);
}

fn to_weft(e: StorageClientError) -> WeftError {
    match e {
        StorageClientError::Denied(m) => WeftError::NodeExecution(format!("storage denied: {m}")),
        StorageClientError::NotFound(m) => {
            WeftError::NodeExecution(format!("storage file not found: {m}"))
        }
        StorageClientError::Unreachable(m) => WeftError::NodeExecution(format!(
            "storage box unreachable: {m}. The box has been re-provisioned; retry the operation"
        )),
        StorageClientError::Other(m) => WeftError::NodeExecution(format!("storage: {m}")),
    }
}

/// Per-request identity: the worker's projected SA token (re-read
/// every call so kubelet rotation propagates) + the execution color.
struct WorkerIdentity {
    token_path: std::path::PathBuf,
    color: String,
}

#[async_trait]
impl ClientIdentity for WorkerIdentity {
    async fn bearer(&self) -> anyhow::Result<String> {
        let bytes = tokio::fs::read(&self.token_path).await.map_err(|e| {
            anyhow::anyhow!("read SA token at {}: {e}", self.token_path.display())
        })?;
        Ok(String::from_utf8(bytes)
            .map_err(|_| anyhow::anyhow!("SA token not utf8"))?
            .trim()
            .to_string())
    }

    fn color(&self) -> Option<String> {
        Some(self.color.clone())
    }
}

pub struct WorkerStorage {
    tasks: Arc<dyn TaskStoreClient>,
    tenant_id: String,
    token_path: std::path::PathBuf,
    /// Shared connection pool across every per-color client.
    http: reqwest::Client,
    /// Cached box endpoint; None until first ensure. A forced
    /// re-ensure replaces it (the dispatcher may have re-provisioned
    /// the box at the same address; the call is what guarantees the
    /// box EXISTS again).
    endpoint: tokio::sync::RwLock<Option<String>>,
}

impl WorkerStorage {
    pub fn new(
        tasks: Arc<dyn TaskStoreClient>,
        tenant_id: String,
        token_path: std::path::PathBuf,
    ) -> Arc<Self> {
        Arc::new(Self {
            tasks,
            tenant_id,
            token_path,
            http: reqwest::Client::new(),
            endpoint: tokio::sync::RwLock::new(None),
        })
    }

    /// Resolve the box endpoint, asking the dispatcher to ensure the
    /// box when not cached (or when `force` after an unreachable).
    async fn ensure_endpoint(&self, force: bool) -> WeftResult<String> {
        if !force {
            if let Some(url) = self.endpoint.read().await.clone() {
                return Ok(url);
            }
        }
        // Dedup on the tenant id: concurrent ensures (two nodes
        // hitting storage at once) collapse to one task. The dedup
        // index covers only LIVE rows (pending/claimed), so a
        // COMPLETED ensure never blocks a fresh "the box is gone,
        // bring it back" request after a teardown; that re-enqueues
        // cleanly. ensure_box is idempotent server-side, so even a
        // collapse onto an in-flight task returns the right endpoint.
        let id = self
            .tasks
            .enqueue_dedup(weft_task_store::tasks::NewTask {
                kind: TaskKind::EnsureStorageBox,
                target: weft_task_store::tasks::TaskTarget::Dispatcher,
                project_id: None,
                dedup_key: Some(format!("ensure_storage_box:{}", self.tenant_id)),
                color: None,
                tenant_id: Some(self.tenant_id.clone()),
                target_pod_name: None,
                payload: serde_json::json!({}),
            })
            .await
            .map_err(|e| WeftError::NodeExecution(format!("ensure storage box: {e}")))?
            .id()
            // Only the broker-backed FireSignal path can fence (placement
            // generation); this ensure-storage-box enqueue never does.
            .expect("ensure-storage-box enqueue is never fenced");
        let outcome = self
            .tasks
            .wait_for_terminal(id, crate::context::TASK_WAIT_TIMEOUT, crate::context::TASK_POLL_INTERVAL)
            .await
            .map_err(|e| WeftError::NodeExecution(format!("ensure storage box: {e}")))?;
        let url = match outcome.status {
            weft_task_store::tasks::TaskStatus::Complete => outcome
                .result
                .as_ref()
                .and_then(|r| r.get("box_url"))
                .and_then(|v| v.as_str())
                .map(String::from)
                .ok_or_else(|| {
                    WeftError::NodeExecution("ensure storage box returned no box_url".into())
                })?,
            weft_task_store::tasks::TaskStatus::Failed => {
                return Err(WeftError::NodeExecution(format!(
                    "ensure storage box failed: {}",
                    outcome.error.unwrap_or_else(|| "(no error)".into())
                )))
            }
            other => {
                return Err(WeftError::NodeExecution(format!(
                    "ensure storage box status: {other:?}"
                )))
            }
        };
        *self.endpoint.write().await = Some(url.clone());
        Ok(url)
    }

    async fn client(&self, color: Color, force_ensure: bool) -> WeftResult<BoxClient> {
        let url = self.ensure_endpoint(force_ensure).await?;
        Ok(BoxClient::new_with_http(
            url,
            Arc::new(WorkerIdentity {
                token_path: self.token_path.clone(),
                color: color.to_string(),
            }),
            self.http.clone(),
        ))
    }
}

// ---------- fake (tests) ----------

/// Adapter exposing `weft_storage`'s in-memory `FakeStorageOps` as
/// the color-parameterized worker surface. The per-call color is
/// ignored: the fake's seeded identity already pins it, and the wall
/// checks inside the fake run against that identity.
#[cfg(any(test, feature = "test-helpers"))]
pub struct FakeWorkerStorage {
    pub inner: Arc<weft_storage::client::FakeStorageOps>,
}

#[cfg(any(test, feature = "test-helpers"))]
impl FakeWorkerStorage {
    /// A fake bound to (tenant t1, project p1, color c1).
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: weft_storage::client::FakeStorageOps::new(
                weft_storage::key::CallerAuth::Worker {
                    tenant: "t1".into(),
                    project_id: "p1".into(),
                    color: Some("c1".into()),
                },
            ),
        })
    }
}

#[cfg(any(test, feature = "test-helpers"))]
#[async_trait]
impl WorkerStorageOps for FakeWorkerStorage {
    async fn put(
        &self,
        _color: Color,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        data: ByteStream,
    ) -> WeftResult<Value> {
        self.inner
            .put(scope, mime_type, filename, keep, data)
            .await
            .map(|m| m.to_value())
            .map_err(to_weft)
    }

    async fn get(
        &self,
        _color: Color,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)> {
        self.inner.get(key, range).await.map_err(to_weft)
    }

    async fn delete(&self, _color: Color, key: &str) -> WeftResult<()> {
        self.inner.delete(key).await.map_err(to_weft)
    }

    async fn list(&self, _color: Color, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
        self.inner.list(scope).await.map_err(to_weft)
    }

    async fn keep(&self, _color: Color, key: &str, ttl: KeepTtl) -> WeftResult<()> {
        self.inner.keep(key, ttl).await.map(|_| ()).map_err(to_weft)
    }

    async fn presign(
        &self,
        _color: Color,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> WeftResult<String> {
        self.inner.presign(key, ttl_secs).await.map_err(to_weft)
    }

    async fn eager_sweep(&self, _color: Color) {
        // Mirrors the real impl's contract over the in-memory fake.
        if let Ok(files) = self.inner.list(&StorageScope::Execution).await {
            for f in files.iter().filter(|f| !f.keep) {
                let _ = self.inner.delete(&f.key).await;
            }
        }
    }
}

/// Bounded retry-on-unreachable for replayable verbs. First attempt on
/// the cached endpoint; on UNREACHABLE, force a re-ensure (which waits
/// for the box rollout) and retry, then retry a few more times with a
/// short backoff to ride out Service-endpoint / DNS re-propagation
/// after a box pod restart. Only UNREACHABLE is retried (the request
/// never reached the box, so no side effect to double); any other error
/// fails immediately. The final UNREACHABLE (all retries exhausted) is
/// surfaced loudly via `to_weft`.
macro_rules! with_retry {
    ($self:ident, $color:ident, |$client:ident| $call:expr) => {{
        // Backoff schedule for the retries AFTER the forced re-ensure.
        // Total added wait ~3.5s, enough for endpoint propagation; a box
        // that is still unreachable after this is genuinely down, which
        // must fail loud, not hang.
        const BACKOFF_MS: [u64; 4] = [250, 500, 1000, 1500];
        let $client = $self.client($color, false).await?;
        match $call {
            Err(StorageClientError::Unreachable(_)) => {
                // Force a re-ensure (waits for rollout), then retry with
                // backoff to cover post-restart endpoint propagation.
                let mut last = StorageClientError::Unreachable(
                    "box unreachable before re-ensure".into(),
                );
                for (i, delay) in BACKOFF_MS.iter().enumerate() {
                    // Sleep BEFORE each retry (not after a failure), so a
                    // genuinely-down box fails as soon as the attempts are
                    // exhausted instead of waiting out a final backoff. The
                    // first attempt forces a re-ensure (waits for rollout);
                    // later ones reuse the freshly-ensured endpoint.
                    tokio::time::sleep(std::time::Duration::from_millis(*delay)).await;
                    let $client = match $self.client($color, i == 0).await {
                        Ok(c) => c,
                        Err(e) => return Err(e),
                    };
                    match $call {
                        Ok(v) => return Ok(v),
                        Err(StorageClientError::Unreachable(m)) => {
                            last = StorageClientError::Unreachable(m);
                        }
                        Err(e) => return Err(to_weft(e)),
                    }
                }
                Err(to_weft(last))
            }
            other => other.map_err(to_weft),
        }
    }};
}

#[async_trait]
impl WorkerStorageOps for WorkerStorage {
    async fn put(
        &self,
        color: Color,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        data: ByteStream,
    ) -> WeftResult<Value> {
        let client = self.client(color, false).await?;
        match client.put(scope, mime_type, filename, keep, data).await {
            Ok(file) => Ok(file.to_value()),
            Err(StorageClientError::Unreachable(m)) => {
                // The body stream is consumed; we cannot replay it.
                // Re-ensure so the box is back for the caller's
                // retry, then fail loud.
                let _ = self.ensure_endpoint(true).await;
                Err(to_weft(StorageClientError::Unreachable(m)))
            }
            Err(e) => Err(to_weft(e)),
        }
    }

    async fn get(
        &self,
        color: Color,
        key: &str,
        range: Option<ByteRange>,
    ) -> WeftResult<(StoredFileMeta, ByteStream)> {
        with_retry!(self, color, |client| client.get(key, range).await)
    }

    async fn delete(&self, color: Color, key: &str) -> WeftResult<()> {
        with_retry!(self, color, |client| client.delete(key).await)
    }

    async fn list(&self, color: Color, scope: &StorageScope) -> WeftResult<Vec<StoredFileMeta>> {
        with_retry!(self, color, |client| client.list(scope).await)
    }

    async fn keep(&self, color: Color, key: &str, ttl: KeepTtl) -> WeftResult<()> {
        with_retry!(self, color, |client| client.keep(key, ttl).await.map(|_| ()))
    }

    async fn presign(
        &self,
        color: Color,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> WeftResult<String> {
        with_retry!(self, color, |client| client.presign(key, ttl_secs).await)
    }

    async fn eager_sweep(&self, color: Color) {
        if self.endpoint.read().await.is_none() {
            return;
        }
        let result: WeftResult<()> = async {
            let client = self.client(color, false).await?;
            let files = client
                .list(&StorageScope::Execution)
                .await
                .map_err(to_weft)?;
            for f in files.iter().filter(|f| !f.keep) {
                client.delete(&f.key).await.map_err(to_weft)?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(
                target: "weft_engine::storage",
                color = %color,
                error = %e,
                "eager storage sweep failed; the dispatcher's durable sweep will catch up"
            );
        }
    }
}
