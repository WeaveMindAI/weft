//! The disk pool: per-disk blob I/O + usage + grow/shrink requests,
//! behind the `DiskPoolOps` trait. Lives here (not in
//! weft-platform-traits) because the placement layer and the resize
//! watcher in this crate are its only consumers.
//!
//! Real impl: each backing PVC is mounted as a plain directory under
//! a common root (`/disks/<name>`); per-disk I/O goes through
//! `object_store::LocalFileSystem`, free/total via statvfs, and
//! grow/shrink requests are HTTP calls to the dispatcher (which owns
//! PVC provisioning and the pod spec). Fake: in-memory disks with
//! byte capacities, behind `test-helpers`.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use weft_core::storage::ByteStream;

/// Live view of one mounted backing disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskInfo {
    /// Mount-directory name (e.g. `disk-0`). Stable across restarts;
    /// the dispatcher names PVCs/mounts deterministically.
    pub name: String,
    pub free_bytes: u64,
    pub total_bytes: u64,
}

#[async_trait]
pub trait DiskPoolOps: Send + Sync {
    /// Every currently-mounted disk with live usage numbers.
    async fn disks(&self) -> Result<Vec<DiskInfo>>;

    /// Stream `data` into `path` on `disk`. Atomic visibility (write
    /// to a temp location, rename into place) so a crash never
    /// leaves a half-file AT the final path. Returns bytes written.
    async fn write_file(&self, disk: &str, path: &str, data: ByteStream) -> Result<u64>;

    /// Stream `path` back, optionally only `[start, end)` of it.
    /// `Ok(None)` means the file is not on this disk: the caller
    /// (a get stream racing an evacuation that moved this chunk to
    /// another disk and deleted the source) re-resolves the chunk's
    /// home from the live index and retries. `Err` is a real I/O
    /// fault. An open read survives a concurrent delete of the same
    /// path (POSIX unlink semantics); the fake snapshots content at
    /// call time to mirror that.
    async fn read_file(
        &self,
        disk: &str,
        path: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<ByteStream>>;

    /// Fully read a SMALL file (metadata, state). `None` if absent.
    async fn read_small(&self, disk: &str, path: &str) -> Result<Option<Bytes>>;

    async fn delete_file(&self, disk: &str, path: &str) -> Result<()>;

    /// Recursive listing under `prefix`: `(path, size_bytes)` pairs.
    async fn list_files(&self, disk: &str, prefix: &str) -> Result<Vec<(String, u64)>>;

    /// Ask the dispatcher to provision one more backing disk (PVC +
    /// pod spec update; the pod restarts with the new mount).
    async fn request_disk_add(&self) -> Result<()>;

    /// Ask the dispatcher to release `disk` (must already be fully
    /// evacuated; the dispatcher removes the mount + deletes the PVC).
    async fn request_disk_remove(&self, disk: &str) -> Result<()>;
}

// ---------- production impl ----------

/// One `LocalFileSystem` per mounted backing PVC, discovered by
/// scanning the disks root for subdirectories at construction. The
/// mount set only changes via pod restart (k8s pods cannot hot-mount
/// volumes), so a boot-time scan is exact for the pod's lifetime.
pub struct LocalDiskPool {
    root: std::path::PathBuf,
    stores: BTreeMap<String, Arc<object_store::local::LocalFileSystem>>,
    http: reqwest::Client,
    /// Dispatcher endpoints for grow/shrink. The box authenticates
    /// with its projected SA token on every request.
    dispatcher_url: String,
    tenant_id: String,
    token_path: std::path::PathBuf,
}

impl LocalDiskPool {
    pub fn new(
        root: impl Into<std::path::PathBuf>,
        dispatcher_url: String,
        tenant_id: String,
        token_path: std::path::PathBuf,
    ) -> Result<Self> {
        let root = root.into();
        let mut stores = BTreeMap::new();
        for entry in std::fs::read_dir(&root)
            .with_context(|| format!("read disks root {}", root.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            // Kubernetes mounts a `lost+found` on ext4 volumes; only
            // `disk-*` directories are backing disks.
            if !name.starts_with("disk-") {
                continue;
            }
            let store = object_store::local::LocalFileSystem::new_with_prefix(entry.path())
                .with_context(|| format!("open disk mount {}", entry.path().display()))?;
            stores.insert(name, Arc::new(store.with_automatic_cleanup(true)));
        }
        if stores.is_empty() {
            return Err(anyhow!(
                "no backing disks mounted under {} (expected disk-* directories); the \
                 dispatcher provisions at least one PVC before starting the box",
                root.display()
            ));
        }
        Ok(Self {
            root,
            stores,
            http: reqwest::Client::new(),
            dispatcher_url,
            tenant_id,
            token_path,
        })
    }

    fn store(&self, disk: &str) -> Result<&Arc<object_store::local::LocalFileSystem>> {
        self.stores
            .get(disk)
            .ok_or_else(|| anyhow!("unknown disk '{disk}' (mounted: {:?})", self.stores.keys()))
    }

    async fn bearer(&self) -> Result<String> {
        let bytes = tokio::fs::read(&self.token_path)
            .await
            .with_context(|| format!("read SA token at {}", self.token_path.display()))?;
        Ok(String::from_utf8(bytes).context("SA token not utf8")?.trim().to_string())
    }

    async fn post_dispatcher(&self, path: &str, body: serde_json::Value) -> Result<()> {
        let url = format!("{}{}", self.dispatcher_url.trim_end_matches('/'), path);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(self.bearer().await?)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("POST {url} returned {status}: {body}"));
        }
        Ok(())
    }
}

#[async_trait]
impl DiskPoolOps for LocalDiskPool {
    async fn disks(&self) -> Result<Vec<DiskInfo>> {
        let mut out = Vec::with_capacity(self.stores.len());
        for name in self.stores.keys() {
            let path = self.root.join(name);
            let stat = nix::sys::statvfs::statvfs(&path)
                .with_context(|| format!("statvfs {}", path.display()))?;
            let frsize = stat.fragment_size() as u64;
            out.push(DiskInfo {
                name: name.clone(),
                // blocks available to unprivileged callers, which is
                // what our writes actually get.
                free_bytes: stat.blocks_available() as u64 * frsize,
                total_bytes: stat.blocks() as u64 * frsize,
            });
        }
        Ok(out)
    }

    async fn write_file(&self, disk: &str, path: &str, mut data: ByteStream) -> Result<u64> {
        use object_store::ObjectStore;
        let store = self.store(disk)?.clone();
        let location = object_store::path::Path::from(path);
        // Multipart streams to a temp file and completes with a
        // rename, which is the atomic-visibility property we need.
        let upload = store.put_multipart(&location).await?;
        let mut writer = object_store::WriteMultipart::new(upload);
        let mut written = 0u64;
        while let Some(chunk) = data.next().await {
            let chunk = chunk.context("read body stream")?;
            written += chunk.len() as u64;
            writer.put(chunk);
        }
        writer.finish().await?;
        Ok(written)
    }

    async fn read_file(
        &self,
        disk: &str,
        path: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<ByteStream>> {
        use object_store::ObjectStore;
        let store = self.store(disk)?.clone();
        let location = object_store::path::Path::from(path);
        let opts = object_store::GetOptions {
            range: range.map(|(s, e)| object_store::GetRange::Bounded(s..e)),
            ..Default::default()
        };
        match store.get_opts(&location, opts).await {
            Ok(result) => Ok(Some(Box::pin(result.into_stream().map(|r| {
                r.map_err(|e| std::io::Error::other(format!("disk read: {e}")))
            })))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn read_small(&self, disk: &str, path: &str) -> Result<Option<Bytes>> {
        use object_store::ObjectStore;
        let store = self.store(disk)?;
        let location = object_store::path::Path::from(path);
        match store.get(&location).await {
            Ok(r) => Ok(Some(r.bytes().await?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn delete_file(&self, disk: &str, path: &str) -> Result<()> {
        use object_store::ObjectStore;
        let store = self.store(disk)?;
        match store.delete(&object_store::path::Path::from(path)).await {
            // Idempotent: deleting an already-gone file is the
            // desired end state (crash-resume paths re-delete).
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_files(&self, disk: &str, prefix: &str) -> Result<Vec<(String, u64)>> {
        use object_store::ObjectStore;
        let store = self.store(disk)?;
        let prefix_path = object_store::path::Path::from(prefix);
        let mut listing = store.list(Some(&prefix_path));
        let mut out = Vec::new();
        while let Some(meta) = listing.next().await {
            let meta = meta?;
            out.push((meta.location.to_string(), meta.size));
        }
        Ok(out)
    }

    async fn request_disk_add(&self) -> Result<()> {
        self.post_dispatcher(
            &format!("/internal/storage/{}/disks/add", self.tenant_id),
            serde_json::json!({}),
        )
        .await
    }

    async fn request_disk_remove(&self, disk: &str) -> Result<()> {
        self.post_dispatcher(
            &format!("/internal/storage/{}/disks/remove", self.tenant_id),
            serde_json::json!({ "disk": disk }),
        )
        .await
    }
}

// ---------- fake (test-helpers) ----------

#[cfg(any(test, feature = "test-helpers"))]
pub use fake::{FakeDiskPool, PoolCall};

#[cfg(any(test, feature = "test-helpers"))]
mod fake {
    use super::*;
    use parking_lot::Mutex;

    /// What the pool was asked to do, append-only.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum PoolCall {
        DiskAddRequested,
        DiskRemoveRequested { disk: String },
    }

    #[derive(Default)]
    struct FakeDisk {
        capacity: u64,
        files: BTreeMap<String, Bytes>,
    }

    impl FakeDisk {
        fn used(&self) -> u64 {
            self.files.values().map(|b| b.len() as u64).sum()
        }
    }

    #[derive(Default)]
    struct Inner {
        disks: BTreeMap<String, FakeDisk>,
        calls: Vec<PoolCall>,
        /// Fault injection: when `Some(n)`, the write_file call after
        /// n more successes fails once (interrupted-evacuation tests).
        fail_write_after: Option<u32>,
    }

    /// Dumb in-memory pool: plain maps + an append-only call log.
    #[derive(Default)]
    pub struct FakeDiskPool {
        inner: Mutex<Inner>,
    }

    impl FakeDiskPool {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn add_disk(&self, name: &str, capacity: u64) {
            self.inner
                .lock()
                .disks
                .insert(name.to_string(), FakeDisk { capacity, files: BTreeMap::new() });
        }

        /// Simulate the dispatcher honoring a remove: drop the disk
        /// and everything on it.
        pub fn remove_disk(&self, name: &str) {
            self.inner.lock().disks.remove(name);
        }

        pub fn calls(&self) -> Vec<PoolCall> {
            self.inner.lock().calls.clone()
        }

        pub fn file(&self, disk: &str, path: &str) -> Option<Bytes> {
            self.inner.lock().disks.get(disk)?.files.get(path).cloned()
        }

        pub fn file_count(&self, disk: &str) -> usize {
            self.inner.lock().disks.get(disk).map(|d| d.files.len()).unwrap_or(0)
        }

        /// Let `n` more writes succeed, then fail the next one once.
        pub fn fail_write_after(&self, n: u32) {
            self.inner.lock().fail_write_after = Some(n);
        }

        /// Test sabotage: silently drop a file behind the store's
        /// back (corruption simulation).
        pub fn remove_file_for_test(&self, disk: &str, path: &str) {
            if let Some(d) = self.inner.lock().disks.get_mut(disk) {
                d.files.remove(path);
            }
        }
    }

    #[async_trait]
    impl DiskPoolOps for FakeDiskPool {
        async fn disks(&self) -> Result<Vec<DiskInfo>> {
            Ok(self
                .inner
                .lock()
                .disks
                .iter()
                .map(|(name, d)| DiskInfo {
                    name: name.clone(),
                    free_bytes: d.capacity.saturating_sub(d.used()),
                    total_bytes: d.capacity,
                })
                .collect())
        }

        async fn write_file(&self, disk: &str, path: &str, mut data: ByteStream) -> Result<u64> {
            // Collect first: an atomic write is all-or-nothing, so
            // the fake never exposes a partial file either.
            let mut buf = Vec::new();
            while let Some(chunk) = data.next().await {
                buf.extend_from_slice(&chunk?);
            }
            let mut inner = self.inner.lock();
            match inner.fail_write_after {
                Some(0) => {
                    inner.fail_write_after = None;
                    return Err(anyhow!("injected write failure"));
                }
                Some(n) => inner.fail_write_after = Some(n - 1),
                None => {}
            }
            let d = inner
                .disks
                .get_mut(disk)
                .ok_or_else(|| anyhow!("unknown disk '{disk}'"))?;
            if d.used() + buf.len() as u64 > d.capacity {
                return Err(anyhow!("no space left on fake disk '{disk}'"));
            }
            let len = buf.len() as u64;
            d.files.insert(path.to_string(), Bytes::from(buf));
            Ok(len)
        }

        async fn read_file(
            &self,
            disk: &str,
            path: &str,
            range: Option<(u64, u64)>,
        ) -> Result<Option<ByteStream>> {
            // Not on this disk -> Ok(None) (the evacuation-race signal),
            // exactly like the real LocalFileSystem's NotFound.
            let Some(bytes) = self.file(disk, path) else {
                return Ok(None);
            };
            let bytes = match range {
                None => bytes,
                Some((s, e)) => {
                    if e > bytes.len() as u64 || s > e {
                        return Err(anyhow!(
                            "range {s}..{e} out of bounds for {} bytes",
                            bytes.len()
                        ));
                    }
                    bytes.slice(s as usize..e as usize)
                }
            };
            Ok(Some(weft_core::storage::bytes_stream(bytes)))
        }

        async fn read_small(&self, disk: &str, path: &str) -> Result<Option<Bytes>> {
            let inner = self.inner.lock();
            let Some(d) = inner.disks.get(disk) else {
                return Err(anyhow!("unknown disk '{disk}'"));
            };
            Ok(d.files.get(path).cloned())
        }

        async fn delete_file(&self, disk: &str, path: &str) -> Result<()> {
            let mut inner = self.inner.lock();
            let d = inner
                .disks
                .get_mut(disk)
                .ok_or_else(|| anyhow!("unknown disk '{disk}'"))?;
            d.files.remove(path);
            Ok(())
        }

        async fn list_files(&self, disk: &str, prefix: &str) -> Result<Vec<(String, u64)>> {
            let inner = self.inner.lock();
            let d = inner
                .disks
                .get(disk)
                .ok_or_else(|| anyhow!("unknown disk '{disk}'"))?;
            Ok(d.files
                .iter()
                .filter(|(p, _)| p.starts_with(prefix))
                .map(|(p, b)| (p.clone(), b.len() as u64))
                .collect())
        }

        async fn request_disk_add(&self) -> Result<()> {
            self.inner.lock().calls.push(PoolCall::DiskAddRequested);
            Ok(())
        }

        async fn request_disk_remove(&self, disk: &str) -> Result<()> {
            self.inner
                .lock()
                .calls
                .push(PoolCall::DiskRemoveRequested { disk: disk.to_string() });
            Ok(())
        }
    }
}
