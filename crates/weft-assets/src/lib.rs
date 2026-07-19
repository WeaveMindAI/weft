//! The pre-build asset sync: make the project's ASSET storage plane mirror
//! exactly what the code references, and hand the compiler the resolved
//! values.
//!
//! An `@asset("assets/pic.png", Image)` ref names a file living with the
//! project. Right before a build, the build driver runs [`sync_assets`]:
//!
//!   1. hash every referenced file (streamed; the content hash IS the asset's
//!      identity and its storage id),
//!   2. diff against the project's existing `asset/` keys,
//!   3. upload the missing content, delete the no-longer-referenced content,
//!   4. return the `path -> stored-file value` map the compiler substitutes
//!      (see `weft_compiler::file_reader::AssetMode::Resolve`).
//!
//! Storage is DERIVED STATE: it always converges to "the assets the code
//! references right now". The compiler never sees bytes; workers read them
//! from the bucket at run time. URL refs never reach this module (they
//! resolve inline to url-form values).
//!
//! One sync per project at a time: the plane converges to the LAST sync's
//! referenced set (step 4 deletes what that sync's source no longer
//! references), so the driver serializes builds of a project. Concurrent
//! syncs of one project would race their deletes against each other's
//! uploads; a build whose source was superseded mid-flight is stale by
//! construction either way.
//!
//! I/O is behind two traits so the sync's orchestration is contract-testable
//! with fakes: [`AssetSource`] (where the project's files live) and
//! [`AssetStore`] (the project's asset plane in runtime storage).

use std::collections::BTreeMap;
use std::io::Read;

use anyhow::{bail, Context, Result};
use sha2::Digest;
use weft_core::project::FileRef;
use weft_core::storage::StoredFile;

/// Where the project's files live. `open` returns a streaming reader so a
/// multi-gigabyte asset is hashed and uploaded in bounded memory.
pub trait AssetSource: Send + Sync {
    /// A reader over the file at `path` (as written in the `@asset` ref:
    /// project-relative, or absolute/outside where the source allows it).
    /// Errors name the path ("asset not found" is the loud build error).
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>>;
}

/// The project's asset plane in runtime storage.
#[async_trait::async_trait]
pub trait AssetStore: Send + Sync {
    /// Every existing asset of the project: `content hash -> full storage key`.
    async fn list(&self) -> Result<BTreeMap<String, String>>;
    /// Upload one asset's bytes under its content hash; returns the stored
    /// key. MUST be idempotent for an already-ACTIVE identical hash (the
    /// begin verb answers `already_stored` with the existing key; same
    /// content = same asset), and MUST error for anything else.
    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String>;
    /// Delete one asset by its full storage key.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// Stream-hash a reader: `(sha256 hex, total bytes)` without ever holding the
/// whole content. The hash is the asset's identity everywhere (storage id,
/// diff key, upload dedup).
pub fn hash_reader(mut r: impl Read) -> Result<(String, u64)> {
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    loop {
        let n = r.read(&mut buf).context("read for hashing")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((format!("{:x}", hasher.finalize()), total))
}

/// Make the store mirror `refs` and return the compiler's resolution map
/// (`ref path -> stored-file value`).
///
/// Every missing/unreadable file is collected and reported in ONE loud error
/// (so a build with three broken refs names all three, not the first). Upload
/// and delete failures abort loudly; a partial sync leaves only content that
/// is either referenced (kept next run) or unreferenced-but-listed (deleted
/// next run), so no state is ever stranded.
pub async fn sync_assets(
    refs: &[FileRef],
    source: &dyn AssetSource,
    store: &dyn AssetStore,
) -> Result<BTreeMap<String, serde_json::Value>> {
    // 1. Hash every referenced file. Two refs to the same path were deduped by
    //    the collector; two paths with identical bytes share one stored asset.
    let mut hashed: Vec<(&FileRef, String, u64)> = Vec::with_capacity(refs.len());
    let mut broken: Vec<String> = Vec::new();
    for r in refs {
        match source.open(&r.path).and_then(hash_reader) {
            Ok((hash, size)) => hashed.push((r, hash, size)),
            Err(e) => broken.push(format!("  {}: {e:#}", r.path)),
        }
    }
    if !broken.is_empty() {
        bail!(
            "{} referenced asset(s) could not be read:\n{}",
            broken.len(),
            broken.join("\n")
        );
    }

    // 2. Diff against what the store already holds.
    let existing = store.list().await.context("list existing assets")?;

    // 3. Upload the missing content (a second reader pass streams the bytes;
    //    hashing buffered them nowhere). Record every asset's key.
    let mut keys: BTreeMap<&str, String> = BTreeMap::new();
    for (r, hash, size) in &hashed {
        if let Some(key) = existing.get(hash.as_str()) {
            keys.insert(hash, key.clone());
            continue;
        }
        if keys.contains_key(hash.as_str()) {
            continue; // two paths, identical bytes: already uploaded this pass
        }
        let mime = weft_core::storage::mime_from_filename(&r.path);
        let mut reader = source
            .open(&r.path)
            .with_context(|| format!("re-open asset {} for upload", r.path))?;
        let key = store
            .upload(hash, mime, &r.path, *size, reader.as_mut())
            .await
            .with_context(|| format!("upload asset {}", r.path))?;
        keys.insert(hash, key);
    }

    // 4. Delete content no ref uses anymore (the derived-state guarantee: the
    //    plane converges to exactly the referenced set).
    let referenced: std::collections::BTreeSet<&str> =
        hashed.iter().map(|(_, h, _)| h.as_str()).collect();
    for (hash, key) in &existing {
        if !referenced.contains(hash.as_str()) {
            store
                .delete(key)
                .await
                .with_context(|| format!("delete unreferenced asset {key}"))?;
        }
    }

    // 5. The compiler's map: path -> stored-file value, marker kind picked by
    //    the DECLARED type (an `Image` ref is `__weft_image__` whatever the
    //    extension guesses).
    let mut map = BTreeMap::new();
    for (r, hash, size) in &hashed {
        let key = keys.get(hash.as_str()).expect("every hashed ref got a key");
        let file = StoredFile {
            key: key.clone(),
            mime_type: weft_core::storage::mime_from_filename(&r.path).to_string(),
            size_bytes: *size,
            filename: r.path.clone(),
        };
        map.insert(r.path.clone(), weft_core::storage::typed_file_value(&file, &r.ty));
    }
    Ok(map)
}
