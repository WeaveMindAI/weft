//! The placement engine: chunked writes spilling across disks,
//! ordered-chunk reads (streaming + ranges), in-place delete,
//! evacuate-one-disk, the terminate/TTL sweeps, and the usage view.
//! Everything operates over `DiskPoolOps` so Layer-3 tests run the
//! REAL code against fake disks, including the shrink path.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use weft_core::storage::{ByteRange, ByteStream, KeepTtl, StoredFileMeta};
use weft_platform_traits::Clock;

use crate::boxstate::{self, BoxState};
use crate::config::{CHUNK_SIZE_BYTES, DEFAULT_KEEP_TTL};
use crate::key::ParsedKey;
use crate::disk::{DiskInfo, DiskPoolOps};
use crate::index::{
    chunk_path, merge_scans, meta_path, parse_meta_path, plan_range, ChunkLoc, DiskScan,
    FileEntry, Index, MetaFile, DRAINING_MARKER,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("file not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    /// Caller supplied a malformed argument (e.g. an unanchored wipe
    /// prefix). A client error, not a server fault: maps to 400.
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("storage corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// The usage view the dispatcher's reaper + `weft files usage` read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    /// Total stored bytes, INCLUDING un-kept exec scratch: a box
    /// holding any byte (even scratch of a parked execution) is not
    /// empty and must not be torn down.
    #[serde(rename = "storedBytes")]
    pub stored_bytes: u64,
    #[serde(rename = "fileCount")]
    pub file_count: u64,
    #[serde(rename = "lastActivityUnix")]
    pub last_activity_unix: i64,
    pub disks: Vec<UsageDisk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageDisk {
    pub name: String,
    #[serde(rename = "freeBytes")]
    pub free_bytes: u64,
    #[serde(rename = "totalBytes")]
    pub total_bytes: u64,
    pub draining: bool,
}

/// Pick the disk for the next `need` bytes: non-draining, enough
/// free space, most free first (balances fill). Pure.
fn choose_disk(disks: &[DiskInfo], draining: &BTreeSet<String>, need: u64) -> Option<String> {
    disks
        .iter()
        .filter(|d| !draining.contains(&d.name) && d.free_bytes >= need)
        .max_by_key(|d| d.free_bytes)
        .map(|d| d.name.clone())
}

pub struct Store {
    pool: Arc<dyn DiskPoolOps>,
    clock: Arc<dyn Clock>,
    index: RwLock<Index>,
    state: Mutex<BoxState>,
    draining: RwLock<BTreeSet<String>>,
    /// Release barrier between disk WRITES and an evacuation's final
    /// release. A writer (chunk write, meta write) holds the READ side
    /// across its choose-disk-then-write span; evacuate holds the WRITE
    /// side across its emptiness-check -> re-replicate -> release window.
    /// This makes "a writer lands bytes on a disk evacuate is releasing"
    /// impossible: while evacuate verifies-empty-and-releases, no write is
    /// in flight, and a writer that resumes afterward re-reads the disk
    /// list (the released disk is gone) before choosing a target. It does
    /// NOT serialize the bulk drain (only the brief release window), so
    /// concurrent puts are never blocked on the multi-chunk move, only an
    /// in-flight write delays the background release until it finishes.
    release_barrier: RwLock<()>,
    last_activity_unix: AtomicI64,
}

impl Store {
    /// Boot: scan every disk, rebuild the index, collect crash
    /// garbage, load (or mint) the box state. Corruption the scan
    /// can't explain fails the boot loudly; never guesses.
    pub async fn open(
        pool: Arc<dyn DiskPoolOps>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, StoreError> {
        let mut scans = Vec::new();
        let mut draining = BTreeSet::new();
        for disk in pool.disks().await? {
            let is_draining = pool.read_small(&disk.name, DRAINING_MARKER).await?.is_some();
            if is_draining {
                draining.insert(disk.name.clone());
            }
            let chunk_files = pool.list_files(&disk.name, "chunks/").await?;
            let mut meta_files = Vec::new();
            for (path, _) in pool.list_files(&disk.name, "meta/").await? {
                let bytes = pool
                    .read_small(&disk.name, &path)
                    .await?
                    .ok_or_else(|| StoreError::Corrupt(format!(
                        "meta file '{path}' on '{}' vanished between list and read",
                        disk.name
                    )))?;
                let meta: MetaFile = serde_json::from_slice(&bytes).map_err(|e| {
                    StoreError::Corrupt(format!(
                        "unparseable meta file '{path}' on '{}': {e}",
                        disk.name
                    ))
                })?;
                meta_files.push((path, meta));
            }
            scans.push(DiskScan { disk: disk.name, draining: is_draining, chunk_files, meta_files });
        }
        let outcome = merge_scans(scans).map_err(StoreError::Corrupt)?;
        for (disk, path) in &outcome.garbage {
            tracing::warn!(
                target: "weft_storage::scan",
                disk = %disk, path = %path,
                "collecting scan garbage"
            );
            pool.delete_file(disk, path).await?;
        }
        let state = boxstate::load_or_init(&pool).await?;
        let now = clock.now_unix();
        Ok(Self {
            pool,
            clock,
            index: RwLock::new(outcome.index),
            state: Mutex::new(state),
            draining: RwLock::new(draining),
            release_barrier: RwLock::new(()),
            last_activity_unix: AtomicI64::new(now),
        })
    }

    fn touch(&self) {
        self.last_activity_unix.store(self.clock.now_unix(), Ordering::Relaxed);
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    pub fn pool(&self) -> &Arc<dyn DiskPoolOps> {
        &self.pool
    }

    pub async fn capability_secret(&self) -> Result<Vec<u8>, StoreError> {
        Ok(self.state.lock().await.secret()?)
    }

    /// Record a `(project, shared_name)` grant on first use. A no-op
    /// when already granted (no state write).
    pub async fn record_grant(&self, project_id: &str, name: &str) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        if state.grants.insert((project_id.to_string(), name.to_string())) {
            state.version += 1;
            // Skip draining disks: a state write must never land ONLY
            // on a disk about to be released (it would be lost), the
            // same rule evacuate's re-replicate relies on.
            let skip = self.draining.read().await.clone();
            boxstate::persist(&self.pool, &state, &skip).await?;
        }
        Ok(())
    }

    pub async fn grants(&self) -> Vec<(String, String)> {
        self.state.lock().await.grants.iter().cloned().collect()
    }

    pub async fn last_resize_at_unix(&self) -> Option<i64> {
        self.state.lock().await.last_resize_at_unix
    }

    pub async fn stamp_resize(&self) -> Result<(), StoreError> {
        let mut state = self.state.lock().await;
        state.last_resize_at_unix = Some(self.clock.now_unix());
        state.version += 1;
        // Skip draining disks (see record_grant): a state write must
        // never land only on a disk about to be released.
        let skip = self.draining.read().await.clone();
        boxstate::persist(&self.pool, &state, &skip).await?;
        Ok(())
    }

    fn resolve_keep(&self, keep: Option<KeepTtl>, now: i64) -> (bool, Option<i64>, Option<u64>) {
        match keep {
            None => (false, None, None),
            Some(KeepTtl::Never) => (true, None, None),
            Some(KeepTtl::Default) => {
                let ttl = DEFAULT_KEEP_TTL.as_secs();
                (true, Some(now + ttl as i64), Some(ttl))
            }
            Some(KeepTtl::Secs { secs }) => (true, Some(now + secs as i64), Some(secs)),
        }
    }

    // ---------- put ----------

    /// Store `data` under `key` (which the service already built and
    /// authorized). Streams in bounded memory: at most one chunk
    /// (`CHUNK_SIZE_BYTES`) is buffered while its disk is chosen by
    /// EXACT size; chunks spill to whichever disk has the most room.
    /// On any failure every written piece is deleted before the
    /// error returns (no junk a crashed put didn't already label;
    /// boot-scan collects those).
    pub async fn put(
        &self,
        key: &ParsedKey,
        mime_type: &str,
        filename: &str,
        keep: Option<KeepTtl>,
        mut data: ByteStream,
    ) -> Result<StoredFileMeta, StoreError> {
        self.touch();
        let key = &key.to_key();
        if self.index.read().await.files.contains_key(key) {
            return Err(StoreError::Conflict(format!("key '{key}' already exists")));
        }

        // Hold the release barrier (read) for the whole put: chunk writes,
        // the commit meta write, and the index insert. An evacuation's
        // final verify-empty-and-release takes the WRITE side, so it waits
        // for in-flight puts to finish before releasing a disk (it never
        // releases a disk a put is still writing to, and the put's chunks
        // are not yet in the index for the drain loop to move). The bulk
        // chunk MOVE during a drain runs without this guard, so concurrent
        // puts are not blocked by the move, only the brief release waits.
        let _barrier = self.release_barrier.read().await;

        let mut written: Vec<ChunkLoc> = Vec::new();
        let mut carry: Option<Bytes> = None;
        let mut stream_done = false;
        let result: Result<(), StoreError> = async {
            while !stream_done {
                // Fill one chunk's worth of pieces. The carry-over
                // from the previous boundary goes through the SAME
                // slicing as fresh pieces (it can itself exceed a
                // whole chunk when the source yields huge buffers).
                let mut pieces: Vec<Bytes> = Vec::new();
                let mut total = 0u64;
                while total < CHUNK_SIZE_BYTES {
                    let piece = match carry.take() {
                        Some(c) => c,
                        None => match data.next().await {
                            None => {
                                stream_done = true;
                                break;
                            }
                            Some(piece) => piece.context("read upload stream")?,
                        },
                    };
                    let room = CHUNK_SIZE_BYTES - total;
                    if (piece.len() as u64) > room {
                        carry = Some(piece.slice(room as usize..));
                        total += room;
                        pieces.push(piece.slice(..room as usize));
                    } else {
                        total += piece.len() as u64;
                        pieces.push(piece);
                    }
                }
                if total == 0 {
                    break;
                }
                let disks = self.pool.disks().await?;
                let draining = self.draining.read().await.clone();
                let disk = choose_disk(&disks, &draining, total).ok_or_else(|| {
                    StoreError::Other(anyhow::anyhow!(
                        "no backing disk has {total} free bytes for the next chunk; the pool \
                         is full (the resize watcher grows it; retry after it does, or free \
                         space with `weft files rm`)"
                    ))
                })?;
                let ordinal = written.len() as u32;
                let path = chunk_path(key, ordinal);
                let stream: ByteStream = Box::pin(futures::stream::iter(
                    pieces.into_iter().map(Ok::<_, std::io::Error>),
                ));
                let len = self.pool.write_file(&disk, &path, stream).await?;
                written.push(ChunkLoc { disk, len });
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            // Cleanup: a failed put leaves nothing behind.
            for (ordinal, loc) in written.iter().enumerate() {
                if let Err(del) =
                    self.pool.delete_file(&loc.disk, &chunk_path(key, ordinal as u32)).await
                {
                    tracing::error!(
                        target: "weft_storage::put",
                        key, disk = %loc.disk, ordinal, error = %del,
                        "failed-put cleanup could not delete a chunk; boot-scan will collect it"
                    );
                }
            }
            return Err(e);
        }

        let now = self.clock.now_unix();
        let (keep_flag, expires_at_unix, keep_ttl_secs) = self.resolve_keep(keep, now);
        let meta = StoredFileMeta {
            key: key.to_string(),
            mime_type: mime_type.to_string(),
            size_bytes: written.iter().map(|c| c.len).sum(),
            filename: filename.to_string(),
            keep: keep_flag,
            expires_at_unix,
            keep_ttl_secs,
            created_at_unix: now,
        };
        // The meta write is the COMMIT POINT. Place it next to chunk
        // 0 (or the roomiest disk for an empty file).
        let meta_disk = match written.first() {
            Some(c) => c.disk.clone(),
            None => {
                let disks = self.pool.disks().await?;
                let draining = self.draining.read().await.clone();
                choose_disk(&disks, &draining, 4096).ok_or_else(|| {
                    StoreError::Other(anyhow::anyhow!("no disk with room for a metadata file"))
                })?
            }
        };
        let meta_bytes = Bytes::from(serde_json::to_vec(&MetaFile::from_meta(&meta)).expect("meta serializes"));
        self.pool
            .write_file(&meta_disk, &meta_path(key), weft_core::storage::bytes_stream(meta_bytes))
            .await?;

        self.index.write().await.files.insert(
            key.to_string(),
            FileEntry { meta: meta.clone(), chunks: written, meta_disk },
        );
        Ok(meta)
    }

    // ---------- get ----------

    /// Metadata without bytes and without an access bump.
    pub async fn meta(&self, key: &ParsedKey) -> Option<StoredFileMeta> {
        let key = key.to_key();
        self.index.read().await.files.get(&key).map(|e| e.meta.clone())
    }

    /// Stream a file (optionally a sub-range). Validates the chunk
    /// lengths against the metadata's size and fails LOUDLY on
    /// mismatch. Counts as access: bumps a kept file's expiry.
    /// Chunk locations are re-resolved from the live index as the
    /// stream advances, so a concurrent evacuation redirects the
    /// reader to a chunk's new home instead of tearing the read
    /// (an in-flight read of the exact chunk being moved survives
    /// the source delete via POSIX unlink semantics).
    /// Takes `Arc<Self>` because the returned stream is 'static and
    /// owns its handle on the store.
    pub async fn get(
        self: &Arc<Self>,
        key: &ParsedKey,
        range: Option<ByteRange>,
    ) -> Result<(StoredFileMeta, ByteStream), StoreError> {
        self.touch();
        let key = key.to_key();
        let (meta, chunk_lens) = {
            let index = self.index.read().await;
            let entry = index
                .files
                .get(&key)
                .ok_or_else(|| StoreError::NotFound(key.clone()))?;
            if entry.chunk_total() != entry.meta.size_bytes {
                return Err(StoreError::Corrupt(format!(
                    "file '{key}': metadata says {} bytes but chunks hold {}; refusing a \
                     best-effort read. Rebuild happens at boot; if this persists the disks \
                     lost data",
                    entry.meta.size_bytes,
                    entry.chunk_total()
                )));
            }
            (entry.meta.clone(), entry.chunks.iter().map(|c| c.len).collect::<Vec<_>>())
        };
        self.bump_if_kept(&key).await?;

        let (start, end) = match range {
            None => (0, meta.size_bytes),
            Some(r) => {
                let end = r.end.unwrap_or(meta.size_bytes);
                if r.start > end || end > meta.size_bytes {
                    return Err(StoreError::Other(anyhow::anyhow!(
                        "range {}..{} out of bounds for {} bytes",
                        r.start,
                        end,
                        meta.size_bytes
                    )));
                }
                (r.start, end)
            }
        };
        let parts = plan_range(&chunk_lens, start, end);
        let store = self.clone();
        let stream: ByteStream = Box::pin(async_stream::try_stream(store, key, parts));
        Ok((meta, stream))
    }

    // ---------- delete / keep / list ----------

    /// Delete: meta first (the file stops existing atomically), then
    /// chunks (space reclaimed in place), then the index entry. A
    /// crash mid-way leaves only self-labeled chunks the boot scan
    /// collects.
    pub async fn delete(&self, key: &ParsedKey) -> Result<(), StoreError> {
        self.touch();
        let key = key.to_key();
        // Unconditional remove: a user delete always takes the file.
        match self.remove_and_purge(&key, |_| true).await? {
            Some(()) => Ok(()),
            // `remove_if` only returns None when the predicate spared the
            // entry; with an always-true predicate the sole None cause is
            // an absent key.
            None => Err(StoreError::NotFound(key)),
        }
    }

    /// Remove `key` from the index and purge its bytes, but ONLY if
    /// `should_remove(entry)` holds when evaluated UNDER THE WRITE LOCK.
    /// The predicate is the commit gate: the sweeps pass a still-a-victim
    /// check so a file a concurrent `keep`/access-bump renewed between
    /// victim-collection and here is spared (returns `Ok(None)`), instead
    /// of being deleted off a stale list. `delete` passes always-true.
    /// Returns `Ok(Some(()))` when removed, `Ok(None)` when the key was
    /// absent or the predicate spared it.
    async fn remove_and_purge(
        &self,
        key: &str,
        should_remove: impl FnOnce(&FileEntry) -> bool,
    ) -> Result<Option<()>, StoreError> {
        // REMOVE the entry from the index FIRST, under the write lock,
        // so a concurrent keep / access-bump cannot find it and rewrite
        // its meta file mid-delete (which would resurrect the file at
        // the next boot scan). The predicate is checked in the SAME
        // critical section as the removal, so a renew that lands just
        // before us is seen and spares the file. From here the file is
        // logically gone.
        let entry = {
            let mut index = self.index.write().await;
            match index.files.get(key) {
                Some(e) if should_remove(e) => index.files.remove(key).expect("just checked"),
                // Absent, or the predicate spared it (e.g. a sweep victim
                // that got kept/renewed since collection). Nothing to do.
                _ => return Ok(None),
            }
        };
        // Delete the meta on disk (the boot scan keys off meta; chunks
        // without a meta are collected as garbage). If THIS fails the
        // file is still fully present on disk while the index says it
        // is gone, so a boot scan would resurrect it: ROLL BACK the
        // index removal to keep index and disk consistent, then fail.
        if let Err(e) = self.pool.delete_file(&entry.meta_disk, &meta_path(key)).await {
            self.index.write().await.files.insert(key.to_string(), entry);
            return Err(e.into());
        }
        // Meta is gone, the file is logically deleted. A chunk-delete
        // error below only leaves orphan chunks the boot scan collects,
        // never a half-indexed file, so no rollback is needed here.
        for (ordinal, loc) in entry.chunks.iter().enumerate() {
            self.pool.delete_file(&loc.disk, &chunk_path(key, ordinal as u32)).await?;
        }
        Ok(Some(()))
    }

    /// Flag an exec-scoped file as kept (additive only; there is no
    /// un-keep). Project/shared files are persistent already; the
    /// service rejects keep on them before calling here.
    pub async fn keep(&self, key: &ParsedKey, ttl: KeepTtl) -> Result<StoredFileMeta, StoreError> {
        self.touch();
        let key = key.to_key();
        let now = self.clock.now_unix();
        let (keep_flag, expires_at_unix, keep_ttl_secs) = self.resolve_keep(Some(ttl), now);
        debug_assert!(keep_flag);
        let meta = {
            let mut index = self.index.write().await;
            let entry = index
                .files
                .get_mut(&key)
                .ok_or_else(|| StoreError::NotFound(key.clone()))?;
            entry.meta.keep = true;
            entry.meta.expires_at_unix = expires_at_unix;
            entry.meta.keep_ttl_secs = keep_ttl_secs;
            entry.meta.clone()
        };
        self.rewrite_meta(&meta).await?;
        Ok(meta)
    }

    /// Explicit access for non-get verbs that still count as use
    /// (presign, the user-download mint): bumps a kept file's TTL.
    /// Errors NotFound when the key doesn't exist.
    pub async fn touch_access(&self, key: &ParsedKey) -> Result<(), StoreError> {
        self.touch();
        let key = key.to_key();
        if !self.index.read().await.files.contains_key(&key) {
            return Err(StoreError::NotFound(key));
        }
        self.bump_if_kept(&key).await
    }

    /// Access bump: a kept file with a TTL gets `expires_at = now +
    /// ttl`. Failing the bump fails the access (a disk that cannot
    /// rewrite a metadata file is broken; silently serving would let
    /// the file expire despite use).
    async fn bump_if_kept(&self, key: &str) -> Result<(), StoreError> {
        let bumped = {
            let mut index = self.index.write().await;
            let Some(entry) = index.files.get_mut(key) else {
                return Ok(());
            };
            let Some(ttl) = entry.meta.keep_ttl_secs else {
                return Ok(());
            };
            entry.meta.expires_at_unix = Some(self.clock.now_unix() + ttl as i64);
            entry.meta.clone()
        };
        self.rewrite_meta(&bumped).await
    }

    async fn rewrite_meta(&self, meta: &StoredFileMeta) -> Result<(), StoreError> {
        // Hold the release barrier (read) across the meta_disk read and
        // the write, so an evacuation cannot move this file's meta off a
        // draining disk and release it while we write a fresh copy back
        // onto that same disk (which would strand the only meta replica on
        // a disk about to be reclaimed). Same guard puts take.
        let _barrier = self.release_barrier.read().await;
        let meta_disk = {
            let index = self.index.read().await;
            index
                .files
                .get(&meta.key)
                .ok_or_else(|| StoreError::NotFound(meta.key.clone()))?
                .meta_disk
                .clone()
        };
        let bytes = Bytes::from(serde_json::to_vec(&MetaFile::from_meta(meta)).expect("meta serializes"));
        self.pool
            .write_file(&meta_disk, &meta_path(&meta.key), weft_core::storage::bytes_stream(bytes))
            .await?;
        Ok(())
    }

    pub async fn list(&self, prefix: &str) -> Vec<StoredFileMeta> {
        self.touch();
        self.index
            .read()
            .await
            .files
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(_, e)| e.meta.clone())
            .collect()
    }

    pub async fn list_all(&self) -> Vec<StoredFileMeta> {
        self.index.read().await.files.values().map(|e| e.meta.clone()).collect()
    }

    // ---------- sweeps ----------

    /// Terminate sweep: delete the UN-KEPT files under
    /// `exec/<color>/`. Kept survivors stay in place. Idempotent.
    pub async fn sweep_exec(&self, color: &str) -> Result<u32, StoreError> {
        let prefix = format!("exec/{color}/");
        let victims: Vec<String> = self
            .index
            .read()
            .await
            .files
            .range(prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .filter(|(_, e)| !e.meta.keep)
            .map(|(k, _)| k.clone())
            .collect();
        let mut swept = 0;
        for key in victims {
            // Re-check un-kept UNDER the write lock at delete time: a
            // `keep` that landed between collection and here flips the
            // file to kept and must spare it (the stale victim list still
            // names it). `remove_and_purge` returns None when spared.
            match self.remove_and_purge(&key, |e| !e.meta.keep).await {
                Ok(Some(())) => swept += 1,
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(swept)
    }

    /// TTL expiry sweep: delete kept files past their (access-bumped)
    /// expiry. `KeepTtl::Never` files have no expiry and never match.
    pub async fn expiry_sweep(&self) -> Result<u32, StoreError> {
        let now = self.clock.now_unix();
        let victims: Vec<String> = self
            .index
            .read()
            .await
            .files
            .iter()
            .filter(|(_, e)| e.meta.expires_at_unix.map(|t| t <= now).unwrap_or(false))
            .map(|(k, _)| k.clone())
            .collect();
        let mut swept = 0;
        for key in victims {
            // Re-check expiry UNDER the write lock at delete time: an
            // access-bump (get/presign/mint) that renewed this file's TTL
            // between collection and here pushes `expires_at_unix` into
            // the future and must spare it. Re-read `now` so a long sweep
            // does not delete a file that became live mid-sweep.
            let still_expired =
                |e: &FileEntry| e.meta.expires_at_unix.map(|t| t <= self.clock.now_unix()).unwrap_or(false);
            match self.remove_and_purge(&key, still_expired).await {
                Ok(Some(())) => {
                    tracing::info!(target: "weft_storage::expiry", key = %key, "kept file expired");
                    swept += 1;
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(swept)
    }

    /// Wipe every file under `prefix` (kept or not). The `weft rm` /
    /// `weft clean` surface. `prefix` MUST be a scope-anchored owner
    /// boundary (`<scope>/<owner>/`); an unanchored or empty prefix is
    /// rejected loudly so a raw `starts_with` can never over-delete
    /// across the whole box or across owner boundaries.
    pub async fn wipe_prefix(&self, prefix: &str) -> Result<u32, StoreError> {
        crate::key::validate_wipe_prefix(prefix).map_err(StoreError::Invalid)?;
        let victims: Vec<String> = self
            .index
            .read()
            .await
            .files
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect();
        let mut wiped = 0;
        for key in victims {
            // The victims came from the index, so they are already valid
            // keys; purge unconditionally via the internal path (the
            // `&ParsedKey` public `delete` would force a needless re-parse
            // of a key the index already vouches for). A concurrent delete
            // racing us returns None (already gone), which the sweep wants.
            match self.remove_and_purge(&key, |_| true).await {
                Ok(Some(())) => wiped += 1,
                // A concurrent delete got there first; the end state
                // (gone) is what wipe wants, just not counted as ours.
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(wiped)
    }

    // ---------- usage ----------

    pub async fn usage(&self) -> Result<Usage, StoreError> {
        let (stored_bytes, file_count) = {
            let index = self.index.read().await;
            (
                index.files.values().map(|e| e.meta.size_bytes).sum(),
                index.files.len() as u64,
            )
        };
        let draining = self.draining.read().await.clone();
        let disks = self
            .pool
            .disks()
            .await?
            .into_iter()
            .map(|d| UsageDisk {
                draining: draining.contains(&d.name),
                name: d.name,
                free_bytes: d.free_bytes,
                total_bytes: d.total_bytes,
            })
            .collect();
        Ok(Usage {
            stored_bytes,
            file_count,
            last_activity_unix: self.last_activity_unix.load(Ordering::Relaxed),
            disks,
        })
    }

    pub async fn draining_disks(&self) -> BTreeSet<String> {
        self.draining.read().await.clone()
    }

    // ---------- evacuation (shrink) ----------

    /// Evacuate `disk` then ask the dispatcher to release it. Chunk
    /// by chunk: copy to another disk, flip the index entry AFTER the
    /// copy fully lands, delete the source after the flip. Trivially
    /// resumable: an interruption leaves either no copy (redo) or a
    /// sanctioned duplicate the boot scan resolves toward the
    /// non-draining home. Every file stays fully readable throughout.
    pub async fn evacuate(&self, disk: &str) -> Result<(), StoreError> {
        // Persist the draining marker FIRST: placement stops
        // targeting the disk, and a restart resumes the evacuation.
        self.pool
            .write_file(disk, DRAINING_MARKER, weft_core::storage::bytes_stream(Bytes::from_static(b"1")))
            .await?;
        self.draining.write().await.insert(disk.to_string());

        // Move chunks. Snapshot the work list, then re-verify each
        // item against the live index right before acting (a
        // concurrent delete may have removed it).
        let work: Vec<(String, usize)> = {
            let index = self.index.read().await;
            index
                .files
                .iter()
                .flat_map(|(key, e)| {
                    e.chunks
                        .iter()
                        .enumerate()
                        .filter(|(_, c)| c.disk == disk)
                        .map(|(i, _)| (key.clone(), i))
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        for (key, ordinal) in work {
            let still_there = {
                let index = self.index.read().await;
                index
                    .files
                    .get(&key)
                    .and_then(|e| e.chunks.get(ordinal))
                    .map(|c| c.disk == disk)
                    .unwrap_or(false)
            };
            if !still_there {
                continue;
            }
            let path = chunk_path(&key, ordinal as u32);
            let len = {
                let index = self.index.read().await;
                index.files[&key].chunks[ordinal].len
            };
            let disks = self.pool.disks().await?;
            let draining = self.draining.read().await.clone();
            let target = choose_disk(&disks, &draining, len).ok_or_else(|| {
                StoreError::Other(anyhow::anyhow!(
                    "evacuation of '{disk}' stalled: no other disk has {len} free bytes. \
                     Aborting; every file remains readable from its current home"
                ))
            })?;
            let Some(data) = self.pool.read_file(disk, &path, None).await? else {
                // The chunk's file vanished between the still-there
                // check and the read: a concurrent delete removed the
                // whole file. Nothing to move; the next item (or the
                // post-evacuation emptiness check) covers it.
                continue;
            };
            self.pool.write_file(&target, &path, data).await?;
            // Flip AFTER the copy landed; re-check the entry exists
            // (delete may have raced the copy).
            let flipped = {
                let mut index = self.index.write().await;
                match index.files.get_mut(&key).and_then(|e| e.chunks.get_mut(ordinal)) {
                    Some(loc) if loc.disk == disk => {
                        loc.disk = target.clone();
                        true
                    }
                    _ => false,
                }
            };
            self.pool.delete_file(disk, &path).await?;
            if !flipped {
                // The file was deleted mid-copy; the copy is junk.
                self.pool.delete_file(&target, &path).await?;
            }
        }

        // Move meta files the same way.
        let metas: Vec<String> = self
            .pool
            .list_files(disk, "meta/")
            .await?
            .into_iter()
            .filter_map(|(p, _)| parse_meta_path(&p))
            .collect();
        for key in metas {
            let path = meta_path(&key);
            let Some(bytes) = self.pool.read_small(disk, &path).await? else {
                continue;
            };
            let exists = self.index.read().await.files.contains_key(&key);
            if exists {
                let disks = self.pool.disks().await?;
                let draining = self.draining.read().await.clone();
                let target = choose_disk(&disks, &draining, bytes.len() as u64).ok_or_else(|| {
                    StoreError::Other(anyhow::anyhow!(
                        "evacuation of '{disk}' stalled moving metadata: no room elsewhere"
                    ))
                })?;
                self.pool
                    .write_file(&target, &path, weft_core::storage::bytes_stream(bytes))
                    .await?;
                let mut index = self.index.write().await;
                if let Some(e) = index.files.get_mut(&key) {
                    e.meta_disk = target;
                }
            }
            self.pool.delete_file(disk, &path).await?;
        }

        // Take the release barrier (WRITE) for the verify-empty -> release
        // window: it waits for every in-flight put / meta rewrite (which
        // hold the READ side) to finish, so no write is landing on `disk`
        // while we confirm it empty and release it, and any writer that
        // resumes afterward re-reads the disk list (this disk gone) before
        // choosing a target. Held only for this brief window, never the
        // bulk drain above.
        let _release = self.release_barrier.write().await;

        // Verify empty, then hand the disk back.
        let leftover_chunks = self.pool.list_files(disk, "chunks/").await?;
        let leftover_meta = self.pool.list_files(disk, "meta/").await?;
        if !leftover_chunks.is_empty() || !leftover_meta.is_empty() {
            return Err(StoreError::Corrupt(format!(
                "evacuation of '{disk}' finished its work list but {} chunk / {} meta files \
                 remain; refusing to release the disk",
                leftover_chunks.len(),
                leftover_meta.len()
            )));
        }
        // Re-replicate the live boxstate to the SURVIVING disks BEFORE
        // releasing this one. The chunk/meta move loop above ignores
        // BOXSTATE_PATH, so if the newest replica happened to live only
        // on the disk we are about to drop (e.g. after a prior partial
        // persist), it would be lost and the box would silently revert
        // to an older grants table / resize cooldown on the next boot.
        // Skip the draining disk so the write must land on a surviving
        // disk (writing the dying disk and counting it would re-create
        // the very loss this guards against). Lock order matches
        // record_grant / stamp_resize (state then draining).
        let state = self.state.lock().await;
        let skip = self.draining.read().await.clone();
        boxstate::persist(&self.pool, &state, &skip).await?;
        drop(state);
        self.pool.request_disk_remove(disk).await?;
        Ok(())
    }
}

/// The get stream: walks the planned chunk sub-ranges, resolving
/// each chunk's disk from the live index right before its read
/// starts. Hand-rolled state machine (no async-stream macro dep):
/// `unfold` over (next part index, the in-progress chunk stream).
mod async_stream {
    use super::*;

    enum State {
        NextPart(usize),
        Streaming(usize, ByteStream),
        Done,
    }

    pub fn try_stream(
        store: Arc<Store>,
        key: String,
        parts: Vec<(usize, u64, u64)>,
    ) -> impl futures::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
        futures::stream::unfold(
            (store, key, parts, State::NextPart(0)),
            |(store, key, parts, state)| async move {
                let mut state = state;
                loop {
                    match state {
                        State::Done => return None,
                        State::NextPart(i) => {
                            let Some(&(ordinal, s, e)) = parts.get(i) else {
                                return None;
                            };
                            let path = chunk_path(&key, ordinal as u32);
                            // Resolve the chunk's disk from the LIVE
                            // index and read it. If the read returns
                            // not-found, a concurrent evacuation moved
                            // this chunk and deleted the source between
                            // our index read and the open; re-resolve
                            // and retry. An evacuation moves a chunk at
                            // most once (and there is one evacuation at
                            // a time), so the bound is tiny; a generous
                            // cap stops a pathological loop from hanging
                            // a download.
                            let mut attempts = 0u32;
                            let opened = loop {
                                attempts += 1;
                                if attempts > 64 {
                                    break Err(std::io::Error::other(format!(
                                        "chunk {ordinal} of '{key}' kept moving during the \
                                         download; giving up after {attempts} re-resolves"
                                    )));
                                }
                                let loc = {
                                    let index = store.index.read().await;
                                    index
                                        .files
                                        .get(&key)
                                        .and_then(|entry| entry.chunks.get(ordinal))
                                        .map(|c| c.disk.clone())
                                };
                                let Some(disk) = loc else {
                                    break Err(std::io::Error::other(format!(
                                        "file '{key}' disappeared mid-download (deleted or swept)"
                                    )));
                                };
                                match store.pool.read_file(&disk, &path, Some((s, e))).await {
                                    Ok(Some(chunk_stream)) => break Ok(chunk_stream),
                                    // Not on this disk: re-resolve (the
                                    // chunk was just evacuated).
                                    Ok(None) => {
                                        tokio::task::yield_now().await;
                                        continue;
                                    }
                                    Err(err) => {
                                        break Err(std::io::Error::other(format!(
                                            "read chunk {ordinal} of '{key}' on '{disk}': {err}"
                                        )))
                                    }
                                }
                            };
                            match opened {
                                Ok(chunk_stream) => state = State::Streaming(i, chunk_stream),
                                Err(err) => {
                                    return Some((Err(err), (store, key, parts, State::Done)))
                                }
                            }
                        }
                        State::Streaming(i, mut chunk_stream) => {
                            match chunk_stream.next().await {
                                Some(Ok(bytes)) => {
                                    return Some((
                                        Ok(bytes),
                                        (store, key, parts, State::Streaming(i, chunk_stream)),
                                    ));
                                }
                                Some(Err(err)) => {
                                    return Some((Err(err), (store, key, parts, State::Done)));
                                }
                                None => {
                                    state = State::NextPart(i + 1);
                                }
                            }
                        }
                    }
                }
            },
        )
    }
}
