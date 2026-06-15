//! Consumer-side clients of the storage box.
//!
//! `StorageOps` is the worker data path (the engine implements the
//! `ContextHandle` storage methods on top of it). `StorageAdminOps`
//! is the dispatcher control path (mint, sweeps, usage, wipes).
//! Both have a real HTTP impl and a hand-rolled fake behind
//! `test-helpers`.

use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::TryStreamExt;
use weft_core::storage::{
    ByteRange, ByteStream, KeepTtl, StorageScope, StoredFileMeta, StoredFile,
};

use crate::protocol::*;

/// Client-side error, shaped so the engine can tell "the box is not
/// there" (re-ensure via the dispatcher, then retry once) from a
/// real denial/failure (surface to the node).
#[derive(Debug, thiserror::Error)]
pub enum StorageClientError {
    #[error("storage box unreachable: {0}")]
    Unreachable(String),
    #[error("denied: {0}")]
    Denied(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Other(String),
}

type ClientResult<T> = Result<T, StorageClientError>;

#[async_trait]
pub trait StorageOps: Send + Sync {
    async fn put(
        &self,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        data: ByteStream,
    ) -> ClientResult<StoredFile>;

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> ClientResult<(StoredFileMeta, ByteStream)>;

    async fn delete(&self, key: &str) -> ClientResult<()>;

    async fn list(&self, scope: &StorageScope) -> ClientResult<Vec<StoredFileMeta>>;

    async fn keep(&self, key: &str, ttl: KeepTtl) -> ClientResult<StoredFileMeta>;

    async fn presign(&self, key: &str, ttl_secs: Option<u64>) -> ClientResult<String>;
}

#[async_trait]
pub trait StorageAdminOps: Send + Sync {
    async fn mint(&self, box_url: &str, key: &str, ttl_secs: Option<u64>)
        -> ClientResult<MintResponse>;
    async fn sweep_exec(&self, box_url: &str, color: &str) -> ClientResult<u32>;
    async fn wipe_prefix(&self, box_url: &str, prefix: &str) -> ClientResult<u32>;
    async fn delete_key(&self, box_url: &str, key: &str) -> ClientResult<()>;
    async fn usage(&self, box_url: &str) -> ClientResult<Usage>;
    async fn list_all(&self, box_url: &str) -> ClientResult<Vec<StoredFileMeta>>;
}

// ---------- shared HTTP plumbing ----------

fn classify(e: reqwest::Error) -> StorageClientError {
    if e.is_connect() || e.is_timeout() {
        StorageClientError::Unreachable(e.to_string())
    } else {
        StorageClientError::Other(e.to_string())
    }
}

async fn classify_status(resp: reqwest::Response) -> ClientResult<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_default();
    Err(match status.as_u16() {
        401 | 403 => StorageClientError::Denied(body),
        404 => StorageClientError::NotFound(body),
        _ => StorageClientError::Other(format!("{status}: {body}")),
    })
}

/// How a client obtains its bearer + color claim per request. The
/// worker reads its projected SA token from disk every call (kubelet
/// rotation); tests inject fixed strings.
#[async_trait]
pub trait ClientIdentity: Send + Sync {
    async fn bearer(&self) -> anyhow::Result<String>;
    fn color(&self) -> Option<String>;
}

pub struct TokenFileIdentity {
    pub token_path: std::path::PathBuf,
    pub color: Option<String>,
}

#[async_trait]
impl ClientIdentity for TokenFileIdentity {
    async fn bearer(&self) -> anyhow::Result<String> {
        let bytes = tokio::fs::read(&self.token_path)
            .await
            .with_context(|| format!("read SA token at {}", self.token_path.display()))?;
        Ok(String::from_utf8(bytes).context("SA token not utf8")?.trim().to_string())
    }

    fn color(&self) -> Option<String> {
        self.color.clone()
    }
}

/// The real worker->box client.
pub struct BoxClient {
    base_url: String,
    identity: Arc<dyn ClientIdentity>,
    http: reqwest::Client,
}

impl BoxClient {
    pub fn new(base_url: String, identity: Arc<dyn ClientIdentity>) -> Self {
        Self { base_url, identity, http: reqwest::Client::new() }
    }

    /// Share an existing `reqwest::Client` (its connection pool)
    /// across many per-color/per-endpoint `BoxClient`s.
    pub fn new_with_http(
        base_url: String,
        identity: Arc<dyn ClientIdentity>,
        http: reqwest::Client,
    ) -> Self {
        Self { base_url, identity, http }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }

    async fn auth_headers(&self, req: reqwest::RequestBuilder) -> ClientResult<reqwest::RequestBuilder> {
        let bearer = self
            .identity
            .bearer()
            .await
            .map_err(|e| StorageClientError::Other(format!("{e:#}")))?;
        let mut req = req.bearer_auth(bearer);
        if let Some(color) = self.identity.color() {
            req = req.header(HDR_COLOR, color);
        }
        Ok(req)
    }
}

#[async_trait]
impl StorageOps for BoxClient {
    async fn put(
        &self,
        scope: &StorageScope,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        data: ByteStream,
    ) -> ClientResult<StoredFile> {
        let mut req = self
            .auth_headers(self.http.put(self.url("/v1/files")))
            .await?
            .header(HDR_SCOPE, serde_json::to_string(scope).expect("scope serializes"))
            .header(HDR_MIME, mime_type)
            .header(HDR_FILENAME, filename);
        if let Some(keep) = &keep {
            req = req.header(HDR_KEEP, serde_json::to_string(keep).expect("keep serializes"));
        }
        let resp = req
            .body(reqwest::Body::wrap_stream(data))
            .send()
            .await
            .map_err(classify)?;
        let resp = classify_status(resp).await?;
        let value: serde_json::Value =
            resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))?;
        StoredFile::from_value(&value)
            .map_err(|e| StorageClientError::Other(format!("bad put response: {e}")))
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> ClientResult<(StoredFileMeta, ByteStream)> {
        let mut req = self
            .auth_headers(self.http.get(self.url(&format!("/v1/files/{key}"))))
            .await?;
        if let Some(r) = range {
            // Exclusive end -> inclusive HTTP Range.
            let header = match r.end {
                Some(e) if e > r.start => format!("bytes={}-{}", r.start, e - 1),
                Some(_) => format!("bytes={}-{}", r.start, r.start),
                None => format!("bytes={}-", r.start),
            };
            req = req.header("range", header);
        }
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        let meta: StoredFileMeta = serde_json::from_str(
            resp.headers()
                .get("x-weft-meta")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    StorageClientError::Other("box response missing x-weft-meta".into())
                })?,
        )
        .map_err(|e| StorageClientError::Other(format!("bad x-weft-meta: {e}")))?;
        let stream: ByteStream = Box::pin(
            resp.bytes_stream()
                .map_err(|e| std::io::Error::other(format!("download stream: {e}"))),
        );
        Ok((meta, stream))
    }

    async fn delete(&self, key: &str) -> ClientResult<()> {
        let req = self
            .auth_headers(self.http.delete(self.url(&format!("/v1/files/{key}"))))
            .await?;
        classify_status(req.send().await.map_err(classify)?).await?;
        Ok(())
    }

    async fn list(&self, scope: &StorageScope) -> ClientResult<Vec<StoredFileMeta>> {
        let req = self
            .auth_headers(self.http.get(self.url("/v1/list")))
            .await?
            .query(&[("scope", serde_json::to_string(scope).expect("scope serializes"))]);
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        let list: ListResponse =
            resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))?;
        Ok(list.files)
    }

    async fn keep(&self, key: &str, ttl: KeepTtl) -> ClientResult<StoredFileMeta> {
        let req = self
            .auth_headers(self.http.post(self.url("/v1/keep")))
            .await?
            .json(&KeepRequest { key: key.to_string(), ttl });
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))
    }

    async fn presign(&self, key: &str, ttl_secs: Option<u64>) -> ClientResult<String> {
        let req = self
            .auth_headers(self.http.post(self.url("/v1/presign")))
            .await?
            .json(&PresignRequest { key: key.to_string(), ttl_secs });
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        let out: PresignResponse =
            resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))?;
        Ok(out.url)
    }
}

/// The real dispatcher->box admin client. `box_url` is per call:
/// one dispatcher serves many tenants' boxes.
pub struct BoxAdminClient {
    identity: Arc<dyn ClientIdentity>,
    http: reqwest::Client,
}

impl BoxAdminClient {
    pub fn new(identity: Arc<dyn ClientIdentity>) -> Self {
        Self { identity, http: reqwest::Client::new() }
    }

    async fn authed(&self, req: reqwest::RequestBuilder) -> ClientResult<reqwest::RequestBuilder> {
        let bearer = self
            .identity
            .bearer()
            .await
            .map_err(|e| StorageClientError::Other(format!("{e:#}")))?;
        Ok(req.bearer_auth(bearer))
    }
}

fn admin_url(box_url: &str, path: &str) -> String {
    format!("{}{}", box_url.trim_end_matches('/'), path)
}

#[async_trait]
impl StorageAdminOps for BoxAdminClient {
    async fn mint(
        &self,
        box_url: &str,
        key: &str,
        ttl_secs: Option<u64>,
    ) -> ClientResult<MintResponse> {
        let req = self
            .authed(self.http.post(admin_url(box_url, "/admin/mint")))
            .await?
            .json(&MintRequest { key: key.to_string(), ttl_secs });
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))
    }

    async fn sweep_exec(&self, box_url: &str, color: &str) -> ClientResult<u32> {
        let req = self
            .authed(self.http.post(admin_url(box_url, "/admin/sweep-exec")))
            .await?
            .json(&SweepExecRequest { color: color.to_string() });
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        let out: SweepExecResponse =
            resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))?;
        Ok(out.swept)
    }

    async fn wipe_prefix(&self, box_url: &str, prefix: &str) -> ClientResult<u32> {
        let req = self
            .authed(self.http.post(admin_url(box_url, "/admin/wipe-prefix")))
            .await?
            .json(&WipePrefixRequest { prefix: prefix.to_string() });
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        let out: WipePrefixResponse =
            resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))?;
        Ok(out.wiped)
    }

    async fn delete_key(&self, box_url: &str, key: &str) -> ClientResult<()> {
        let req = self
            .authed(self.http.delete(admin_url(box_url, &format!("/admin/files/{key}"))))
            .await?;
        classify_status(req.send().await.map_err(classify)?).await?;
        Ok(())
    }

    async fn usage(&self, box_url: &str) -> ClientResult<Usage> {
        let req = self.authed(self.http.get(admin_url(box_url, "/admin/usage"))).await?;
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))
    }

    async fn list_all(&self, box_url: &str) -> ClientResult<Vec<StoredFileMeta>> {
        let req = self.authed(self.http.get(admin_url(box_url, "/admin/list-all"))).await?;
        let resp = classify_status(req.send().await.map_err(classify)?).await?;
        let out: ListResponse =
            resp.json().await.map_err(|e| StorageClientError::Other(e.to_string()))?;
        Ok(out.files)
    }
}

// ---------- fake (test-helpers) ----------

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::FakeStorageOps;

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    #[derive(Debug, Clone, PartialEq)]
    pub enum StorageCall {
        Put { key: String },
        Get { key: String, range: Option<ByteRange> },
        Delete { key: String },
        List,
        Keep { key: String },
        Presign { key: String },
    }

    #[derive(Default)]
    struct Inner {
        files: BTreeMap<String, (StoredFileMeta, bytes::Bytes)>,
        calls: Vec<StorageCall>,
        /// When set, every call fails Unreachable (simulates a
        /// torn-down box; the engine's ensure-then-retry path).
        unreachable: bool,
    }

    /// Dumb in-memory `StorageOps`. Keys are minted under the seeded
    /// identity the way the box would (`exec/<color>/<n>` etc).
    pub struct FakeStorageOps {
        identity: super::super::key::CallerAuth,
        inner: Mutex<Inner>,
    }

    impl FakeStorageOps {
        pub fn new(identity: crate::key::CallerAuth) -> Arc<Self> {
            Arc::new(Self { identity, inner: Mutex::new(Inner::default()) })
        }

        pub fn calls(&self) -> Vec<StorageCall> {
            self.inner.lock().calls.clone()
        }

        pub fn set_unreachable(&self, v: bool) {
            self.inner.lock().unreachable = v;
        }

        pub fn file_bytes(&self, key: &str) -> Option<bytes::Bytes> {
            self.inner.lock().files.get(key).map(|(_, b)| b.clone())
        }

        pub fn seed_file(&self, key: &str, meta: StoredFileMeta, bytes: bytes::Bytes) {
            self.inner.lock().files.insert(key.to_string(), (meta, bytes));
        }

        fn check_up(&self) -> ClientResult<()> {
            if self.inner.lock().unreachable {
                Err(StorageClientError::Unreachable("fake box down".into()))
            } else {
                Ok(())
            }
        }

        /// Mirror the box's wall for a key-addressed verb: parse the key
        /// and check this caller may touch its scope. The REAL box runs
        /// this on every get/delete/keep/presign (via the service's
        /// `check_access_and_grant`), so the fake must too, or layer-3
        /// tests would pass cross-scope access the real box denies.
        fn enforce_wall(&self, key: &str) -> ClientResult<()> {
            let parsed = crate::key::parse_key(key).map_err(StorageClientError::Other)?;
            crate::key::check_key_access(&self.identity, &parsed.scope)
                .map_err(StorageClientError::Denied)
        }
    }

    #[async_trait]
    impl StorageOps for FakeStorageOps {
        async fn put(
            &self,
            scope: &StorageScope,
            mime_type: &str,
            filename: &str,
            keep: Option<KeepTtl>,
            data: ByteStream,
        ) -> ClientResult<StoredFile> {
            self.check_up()?;
            let id = {
                let inner = self.inner.lock();
                format!("f{}", inner.files.len())
            };
            let key = crate::key::key_for_put(&self.identity, scope, &id)
                .map_err(StorageClientError::Denied)?
                .to_key();
            let bytes = weft_core::storage::collect_stream(data)
                .await
                .map_err(|e| StorageClientError::Other(e.to_string()))?;
            let meta = StoredFileMeta {
                key: key.clone(),
                mime_type: mime_type.to_string(),
                size_bytes: bytes.len() as u64,
                filename: filename.to_string(),
                keep: keep.is_some(),
                expires_at_unix: None,
                keep_ttl_secs: None,
                created_at_unix: 0,
            };
            let file = StoredFile {
                key: key.clone(),
                mime_type: meta.mime_type.clone(),
                size_bytes: meta.size_bytes,
                filename: meta.filename.clone(),
            };
            let mut inner = self.inner.lock();
            inner.files.insert(key.clone(), (meta, bytes));
            inner.calls.push(StorageCall::Put { key });
            Ok(file)
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> ClientResult<(StoredFileMeta, ByteStream)> {
            self.check_up()?;
            self.enforce_wall(key)?;
            let mut inner = self.inner.lock();
            inner.calls.push(StorageCall::Get { key: key.to_string(), range });
            let (meta, bytes) = inner
                .files
                .get(key)
                .cloned()
                .ok_or_else(|| StorageClientError::NotFound(key.to_string()))?;
            let bytes = match range {
                None => bytes,
                Some(r) => {
                    let end = r.end.unwrap_or(bytes.len() as u64).min(bytes.len() as u64);
                    bytes.slice(r.start as usize..end as usize)
                }
            };
            Ok((meta, weft_core::storage::bytes_stream(bytes)))
        }

        async fn delete(&self, key: &str) -> ClientResult<()> {
            self.check_up()?;
            self.enforce_wall(key)?;
            let mut inner = self.inner.lock();
            inner.calls.push(StorageCall::Delete { key: key.to_string() });
            inner
                .files
                .remove(key)
                .map(|_| ())
                .ok_or_else(|| StorageClientError::NotFound(key.to_string()))
        }

        async fn list(&self, scope: &StorageScope) -> ClientResult<Vec<StoredFileMeta>> {
            self.check_up()?;
            let prefix = crate::key::prefix_for_list(&self.identity, scope)
                .map_err(StorageClientError::Denied)?;
            let mut inner = self.inner.lock();
            inner.calls.push(StorageCall::List);
            Ok(inner
                .files
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, (m, _))| m.clone())
                .collect())
        }

        async fn keep(&self, key: &str, ttl: KeepTtl) -> ClientResult<StoredFileMeta> {
            self.check_up()?;
            self.enforce_wall(key)?;
            let mut inner = self.inner.lock();
            inner.calls.push(StorageCall::Keep { key: key.to_string() });
            let (meta, _) = inner
                .files
                .get_mut(key)
                .ok_or_else(|| StorageClientError::NotFound(key.to_string()))?;
            meta.keep = true;
            meta.keep_ttl_secs = match ttl {
                KeepTtl::Never => None,
                KeepTtl::Default => Some(crate::config::DEFAULT_KEEP_TTL.as_secs()),
                KeepTtl::Secs { secs } => Some(secs),
            };
            Ok(meta.clone())
        }

        async fn presign(&self, key: &str, _ttl_secs: Option<u64>) -> ClientResult<String> {
            self.check_up()?;
            self.enforce_wall(key)?;
            let mut inner = self.inner.lock();
            inner.calls.push(StorageCall::Presign { key: key.to_string() });
            if !inner.files.contains_key(key) {
                return Err(StorageClientError::NotFound(key.to_string()));
            }
            Ok(format!("https://fake-box/public/get?cap=fake-{key}"))
        }
    }
}
