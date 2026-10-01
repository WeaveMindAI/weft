//! The version files this replica fetched before, kept on its own disk
//! under their tenant and content hash. A blob's name IS its bytes' sha256,
//! so an entry can never be stale: it is either the right bytes or absent.
//! It is only a shortcut past the store; a replica with an empty cache (a
//! fresh one, a sibling) fetches from the store and fills its own.
//!
//! Kept per tenant: a hit hands over bytes without asking the store, so a
//! tenant only ever hits what its own builds fetched from its own assets.
//! One tenant naming a hash another tenant stored never gets those bytes.
//!
//! Bounded: after each fill the least recently used blobs (by mtime, which
//! a hit refreshes) go until the cache fits under its cap.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};

/// The default ceiling on the cache's bytes.
pub const DEFAULT_CAP_BYTES: u64 = 1 << 30;

#[derive(Debug, Clone)]
pub struct BlobCache {
    dir: PathBuf,
    cap_bytes: u64,
}

impl BlobCache {
    pub fn new(dir: PathBuf, cap_bytes: u64) -> Self {
        Self { dir, cap_bytes }
    }

    /// Next to the build working directories (the system temp dir).
    pub fn in_temp_dir() -> Self {
        Self::new(std::env::temp_dir().join("weft-blob-cache"), DEFAULT_CAP_BYTES)
    }

    fn tenant_dir(&self, tenant: &str) -> Result<PathBuf> {
        anyhow::ensure!(
            weft_core::storage::key::valid_segment(tenant),
            "'{tenant}' is not a tenant id the blob cache can file under"
        );
        Ok(self.dir.join(tenant))
    }

    /// The bytes `tenant` stored under `hash`, or `None` when this replica
    /// has not kept them for that tenant (never fetched, or evicted). A hit
    /// counts as a use. `hash` must already be checked as a content hash.
    pub fn get(&self, tenant: &str, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.tenant_dir(tenant)?.join(hash);
        match std::fs::read(&path) {
            Ok(bytes) => {
                touch(&path);
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read cached blob {}", path.display())),
        }
    }

    /// Keep `bytes`, fetched from `tenant`'s assets and already verified to
    /// hash to `hash`. Written to a private file (a dot name, which eviction
    /// leaves alone while it is fresh) and renamed into place, so a reader
    /// never sees half a blob.
    pub fn put(&self, tenant: &str, hash: &str, bytes: &[u8]) -> Result<()> {
        let dir = self.tenant_dir(tenant)?;
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let tmp = dir.join(format!("{TEMP_PREFIX}{hash}.{}", uuid::Uuid::new_v4().simple()));
        let blob = dir.join(hash);
        std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
        match std::fs::rename(&tmp, &blob) {
            Ok(()) => Ok(()),
            // Our private file is gone (only a stale-temp sweep removes
            // one), but the blob is there: some other fill of the same
            // bytes landed, which is all a put promises.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && blob.is_file() => Ok(()),
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e).with_context(|| format!("move {} into the blob cache", tmp.display()))
            }
        }
    }

    /// Drop the least recently used blobs, across every tenant, until the
    /// cache fits under its cap. A blob another build is reading at that
    /// moment may go; that build then sees a miss and fetches it again.
    /// A put's in-progress file is never counted or touched; one older than
    /// [`STALE_TEMP_AGE`] was left by a put that died, and goes. A regular
    /// file at the top is a blob from before the cache was kept per tenant,
    /// which no lookup reaches any more, and goes too.
    pub fn evict(&self) -> Result<()> {
        let now = SystemTime::now();
        let mut blobs: Vec<(SystemTime, u64, PathBuf)> = Vec::new();
        let mut total = 0u64;
        for top in entries_of(&self.dir)? {
            let Some(meta) = stat(&top)? else { continue };
            if !meta.is_dir() {
                remove(&top)?;
                continue;
            }
            for blob in entries_of(&top)? {
                let Some(meta) = stat(&blob)? else { continue };
                let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
                if is_temp(&blob) {
                    let age = now.duration_since(modified).unwrap_or_default();
                    if age > STALE_TEMP_AGE {
                        remove(&blob)?;
                    }
                    continue;
                }
                total += meta.len();
                blobs.push((modified, meta.len(), blob));
            }
        }
        if total <= self.cap_bytes {
            return Ok(());
        }
        blobs.sort();
        for (_, len, path) in blobs {
            if total <= self.cap_bytes {
                break;
            }
            remove(&path)?;
            total -= len;
        }
        Ok(())
    }
}

/// The name prefix of a put's in-progress file.
const TEMP_PREFIX: &str = ".";

/// How old an in-progress file must be before eviction treats it as left
/// behind by a put that died. Far longer than any write of one blob.
pub const STALE_TEMP_AGE: Duration = Duration::from_secs(60 * 60);

fn is_temp(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with(TEMP_PREFIX))
}

/// `path`'s metadata, none when it is already gone.
fn stat(path: &Path) -> Result<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => Ok(Some(meta)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("stat {}", path.display())),
    }
}

/// Remove one file; one already gone is removed.
fn remove(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("evict {}", path.display())),
    }
}

/// The entries of `dir`, none when it does not exist (yet, or any more).
fn entries_of(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("list {}", dir.display())),
    };
    entries
        .map(|entry| entry.map(|e| e.path()).with_context(|| format!("list {}", dir.display())))
        .collect()
}

/// Mark a blob as just used. Best effort by design: a blob whose mtime
/// could not move is only evicted earlier than it would have been.
fn touch(path: &Path) {
    if let Ok(file) = std::fs::File::options().write(true).open(path) {
        let _ = file.set_modified(SystemTime::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kept_blob_comes_back_and_the_cap_evicts_the_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(dir.path().join("c"), 10);
        assert_eq!(cache.get("t", "a").unwrap(), None);
        cache.put("t", "a", b"123456").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        cache.put("t", "b", b"7890").unwrap();
        cache.evict().unwrap();
        assert_eq!(cache.get("t", "a").unwrap().as_deref(), Some(&b"123456"[..]));
        // "a" was just used, so "b" is the oldest once "c" overflows.
        std::thread::sleep(std::time::Duration::from_millis(20));
        cache.put("t", "c", b"xy").unwrap();
        cache.evict().unwrap();
        assert_eq!(cache.get("t", "b").unwrap(), None);
        assert!(cache.get("t", "a").unwrap().is_some());
        assert!(cache.get("t", "c").unwrap().is_some());
    }

    /// A blob one tenant's build fetched is never another tenant's hit, even
    /// for the same content hash.
    #[test]
    fn a_kept_blob_is_its_tenants_only() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(dir.path().join("c"), u64::MAX);
        cache.put("alice", "h", b"bytes").unwrap();
        assert_eq!(cache.get("alice", "h").unwrap().as_deref(), Some(&b"bytes"[..]));
        assert_eq!(cache.get("bob", "h").unwrap(), None);
        assert!(cache.get("../alice", "h").is_err());
    }

    /// A put's in-progress file is neither counted nor removed while fresh,
    /// however full the cache; a stale one and a pre-per-tenant top-level
    /// blob are swept.
    #[test]
    fn eviction_spares_fresh_in_progress_files_and_sweeps_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("c");
        let cache = BlobCache::new(root.clone(), 0);
        cache.put("t", "a", b"123").unwrap();
        let fresh = root.join("t").join(".b.1");
        std::fs::write(&fresh, b"half").unwrap();
        let stale = root.join("t").join(".b.2");
        std::fs::write(&stale, b"dead").unwrap();
        let old = SystemTime::now() - STALE_TEMP_AGE - Duration::from_secs(60);
        std::fs::File::options().write(true).open(&stale).unwrap().set_modified(old).unwrap();
        let legacy = root.join("oldhash");
        std::fs::write(&legacy, b"legacy").unwrap();
        cache.evict().unwrap();
        assert!(fresh.exists());
        assert!(!stale.exists());
        assert!(!legacy.exists());
        assert_eq!(cache.get("t", "a").unwrap(), None);
    }

    /// A second put of the same bytes after the first landed still succeeds.
    #[test]
    fn a_repeated_put_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(dir.path().join("c"), u64::MAX);
        cache.put("t", "h", b"x").unwrap();
        cache.put("t", "h", b"x").unwrap();
        assert_eq!(cache.get("t", "h").unwrap().as_deref(), Some(&b"x"[..]));
    }
}
