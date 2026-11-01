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
//! a hit refreshes) go until the cache fits under its cap, and a blob over
//! a tenth of the cap is never kept ([`BlobCache::largest_kept`]).

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};

/// The ceiling on the cache's bytes on a disk of the replica's own.
pub const DEFAULT_CAP_BYTES: u64 = 1 << 30;
/// The ceiling where the replica's files take its memory
/// (`Placement::files_take_memory`): a cache of [`DEFAULT_CAP_BYTES`] there
/// would outgrow the whole replica, which runs out of memory on a build
/// like any other allocation.
pub const IN_MEMORY_CAP_BYTES: u64 = 64 << 20;
// At most a quarter of the least memory a role is given (512Mi).
const _: () = assert!(IN_MEMORY_CAP_BYTES * 4 <= 512 << 20);

#[derive(Debug, Clone)]
pub struct BlobCache {
    dir: PathBuf,
    cap_bytes: u64,
}

impl BlobCache {
    pub fn new(dir: PathBuf, cap_bytes: u64) -> Self {
        Self { dir, cap_bytes }
    }

    /// Next to the build working directories (the system temp dir), sized
    /// for where the replica runs: [`IN_MEMORY_CAP_BYTES`] when its files
    /// take its memory, else [`DEFAULT_CAP_BYTES`].
    pub fn in_temp_dir(files_take_memory: bool) -> Self {
        let cap = if files_take_memory { IN_MEMORY_CAP_BYTES } else { DEFAULT_CAP_BYTES };
        Self::new(std::env::temp_dir().join("weft-blob-cache"), cap)
    }

    fn tenant_dir(&self, tenant: &str) -> Result<PathBuf> {
        anyhow::ensure!(
            weft_core::storage::key::valid_segment(tenant),
            "'{tenant}' is not a tenant id the blob cache can file under"
        );
        Ok(self.dir.join(tenant))
    }

    /// The largest blob this cache keeps: a tenth of its cap. Keeping one
    /// near the cap would evict most of what is kept for a single file, so
    /// a big one is fetched from the store each time it is needed instead.
    pub fn largest_kept(&self) -> u64 {
        self.cap_bytes / 10
    }

    /// Copy the blob `tenant` stored under `hash` to a new file at `dest`:
    /// true on a hit (which counts as a use), false when this replica has
    /// not kept it for that tenant (never fetched, too big to keep, or
    /// evicted). `hash` must already be checked as a content hash. A copy
    /// that fails midway removes what it wrote.
    pub fn get_into(&self, tenant: &str, hash: &str, dest: &Path) -> Result<bool> {
        let path = self.tenant_dir(tenant)?.join(hash);
        // Open before copying: once open, an eviction meanwhile cannot
        // take the bytes from under the copy.
        let mut blob = match std::fs::File::open(&path) {
            Ok(blob) => blob,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e).with_context(|| format!("read cached blob {}", path.display())),
        };
        let copied = std::fs::File::create_new(dest)
            .with_context(|| format!("create {}", dest.display()))
            .and_then(|mut out| {
                std::io::copy(&mut blob, &mut out).with_context(|| format!("copy cached blob {} to {}", path.display(), dest.display()))
            });
        if let Err(e) = copied {
            let _ = std::fs::remove_file(dest);
            return Err(e);
        }
        touch(&path);
        Ok(true)
    }

    /// Drop the blob `tenant` stored under `hash`: one whose bytes did not
    /// hash to its name when it was read. Nothing kept is no error.
    pub fn forget(&self, tenant: &str, hash: &str) -> Result<()> {
        let path = self.tenant_dir(tenant)?.join(hash);
        match std::fs::remove_file(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e).with_context(|| format!("remove cached blob {}", path.display())),
            _ => Ok(()),
        }
    }

    /// Keep a copy of the file at `src`, fetched from `tenant`'s assets and
    /// already verified to hash to `hash`, unless it is larger than
    /// [`Self::largest_kept`]. Written to a private file (a dot name, which
    /// eviction leaves alone while it is fresh) and renamed into place, so
    /// a reader never sees half a blob.
    pub fn put_from(&self, tenant: &str, hash: &str, src: &Path) -> Result<()> {
        let dir = self.tenant_dir(tenant)?;
        let len = std::fs::metadata(src).with_context(|| format!("stat {}", src.display()))?.len();
        if len > self.largest_kept() {
            return Ok(());
        }
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let tmp = dir.join(format!("{TEMP_PREFIX}{hash}.{}", uuid::Uuid::new_v4().simple()));
        let blob = dir.join(hash);
        if let Err(e) = std::fs::copy(src, &tmp) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e).with_context(|| format!("copy {} to {}", src.display(), tmp.display()));
        }
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

    /// A file holding `bytes` under `dir`, to keep from.
    fn file(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.join(format!("src.{name}"));
        std::fs::write(&path, bytes).unwrap();
        path
    }

    /// What the cache hands back for `hash`, read from the copy it made.
    fn got(cache: &BlobCache, tenant: &str, hash: &str) -> Option<Vec<u8>> {
        let out = tempfile::tempdir().unwrap();
        let dest = out.path().join("out");
        cache.get_into(tenant, hash, &dest).unwrap().then(|| std::fs::read(&dest).unwrap())
    }

    /// Kept through a cache whose cap keeps everything, evicted through a
    /// view of the same directory capped at 10 bytes.
    #[test]
    fn a_kept_blob_comes_back_and_the_cap_evicts_the_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let keep = BlobCache::new(dir.path().join("c"), u64::MAX);
        let tight = BlobCache::new(dir.path().join("c"), 10);
        assert_eq!(got(&keep, "t", "a"), None);
        keep.put_from("t", "a", &file(dir.path(), "a", b"123456")).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        keep.put_from("t", "b", &file(dir.path(), "b", b"7890")).unwrap();
        tight.evict().unwrap();
        assert_eq!(got(&keep, "t", "a").as_deref(), Some(&b"123456"[..]));
        // "a" was just used, so "b" is the oldest once "c" overflows.
        std::thread::sleep(std::time::Duration::from_millis(20));
        keep.put_from("t", "c", &file(dir.path(), "c", b"xy")).unwrap();
        tight.evict().unwrap();
        assert_eq!(got(&keep, "t", "b"), None);
        assert!(got(&keep, "t", "a").is_some());
        assert!(got(&keep, "t", "c").is_some());
    }

    /// A replica whose files take its memory keeps a cache a small share of
    /// any role's memory, never the gigabyte a disk of its own takes.
    #[test]
    fn a_replica_whose_files_take_its_memory_keeps_a_small_cache() {
        assert_eq!(BlobCache::in_temp_dir(true).largest_kept(), IN_MEMORY_CAP_BYTES / 10);
        assert_eq!(BlobCache::in_temp_dir(false).largest_kept(), DEFAULT_CAP_BYTES / 10);
    }

    /// A blob over a tenth of the cap is never kept, so one big file
    /// cannot flush everything else the cache holds.
    #[test]
    fn a_blob_over_a_tenth_of_the_cap_is_not_kept() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(dir.path().join("c"), 100);
        cache.put_from("t", "small", &file(dir.path(), "small", &[1; 10])).unwrap();
        cache.put_from("t", "big", &file(dir.path(), "big", &[2; 11])).unwrap();
        cache.evict().unwrap();
        assert_eq!(got(&cache, "t", "big"), None);
        assert_eq!(got(&cache, "t", "small"), Some(vec![1; 10]));
    }

    /// A blob one tenant's build fetched is never another tenant's hit, even
    /// for the same content hash.
    #[test]
    fn a_kept_blob_is_its_tenants_only() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(dir.path().join("c"), u64::MAX);
        cache.put_from("alice", "h", &file(dir.path(), "h", b"bytes")).unwrap();
        assert_eq!(got(&cache, "alice", "h").as_deref(), Some(&b"bytes"[..]));
        assert_eq!(got(&cache, "bob", "h"), None);
        assert!(cache.get_into("../alice", "h", &dir.path().join("x")).is_err());
    }

    /// A put's in-progress file is neither counted nor removed while fresh,
    /// however full the cache; a stale one and a pre-per-tenant top-level
    /// blob are swept.
    #[test]
    fn eviction_spares_fresh_in_progress_files_and_sweeps_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("c");
        BlobCache::new(root.clone(), u64::MAX).put_from("t", "a", &file(dir.path(), "a", b"123")).unwrap();
        let cache = BlobCache::new(root.clone(), 0);
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
        assert_eq!(got(&cache, "t", "a"), None);
    }

    /// A second put of the same bytes after the first landed still succeeds.
    #[test]
    fn a_repeated_put_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BlobCache::new(dir.path().join("c"), u64::MAX);
        let src = file(dir.path(), "h", b"x");
        cache.put_from("t", "h", &src).unwrap();
        cache.put_from("t", "h", &src).unwrap();
        assert_eq!(got(&cache, "t", "h").as_deref(), Some(&b"x"[..]));
    }
}
