//! Upload the assets a build needs and hand the compiler their resolved values.
//!
//! An `@asset("assets/pic.png", Image)` ref names a file living with the
//! project. Right before a build, the build driver runs [`sync_assets`]:
//!
//!   1. hash every referenced file (streamed; the content hash IS the asset's
//!      identity and its storage id),
//!   2. diff against the project's existing `asset/` keys,
//!   3. upload the missing content without deleting older versions,
//!   4. return the `path -> stored-file value` map the compiler substitutes
//!      (see `weft_compiler::file_reader::AssetMode::Resolve`).
//!
//! The compiler never sees bytes; workers read them
//! from the bucket at run time. URL refs never reach this module (they
//! resolve inline to url-form values).
//!
//! After resolving ALL file references, the driver publishes the complete
//! set of [`referenced_asset_keys`] to storage. Current assets do not expire;
//! removed ones receive the usual access-renewed TTL. Old executions keep
//! their original keys and may read those files until they expire.
//!
//! I/O is behind two traits so the sync's orchestration is contract-testable
//! with fakes: [`AssetSource`] (where the project's files live) and
//! [`AssetStore`] (the project's asset plane in runtime storage).

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

use anyhow::{bail, Context, Result};
use sha2::Digest;
use weft_core::project::{FileRef, ProjectDefinition};
use weft_core::storage::{FileHandle, StoredFile};
use weft_core::storage::key::{parse_key, KeyScope};
use weft_core::storage::media::{classify_media_slot, media_slots, MediaSlotContent};

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
    /// Remove one stored asset by key. The sync's own undo: an upload whose
    /// bytes turned out not to match the hash they were stored under (the
    /// file changed between the hashing read and the upload read) must not
    /// stay in the store, where a later diff would take the hash for the
    /// real content.
    async fn delete(&self, key: &str) -> Result<()>;
    /// Every existing asset of the project: `content hash -> full storage key`.
    async fn list(&self) -> Result<BTreeMap<String, String>>;
    /// Upload one asset's bytes under its content hash. MUST be idempotent
    /// for an already-ACTIVE identical hash (same content = same asset),
    /// and MUST error for anything else.
    ///
    /// The answer says which of the two happened, because they are not the
    /// same thing to the caller: only [`Uploaded::Stored`] read `bytes`, so
    /// only there does the sync have a streamed hash to hold the upload to.
    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<Uploaded>;
}

/// What an upload did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Uploaded {
    /// The bytes were read and stored under this key.
    Stored(String),
    /// The store already held this exact content under this key, so
    /// nothing was read and nothing was written.
    AlreadyStored(String),
}

impl Uploaded {
    /// The stored key, whichever way the upload went.
    pub fn key(&self) -> &str {
        match self {
            Uploaded::Stored(k) | Uploaded::AlreadyStored(k) => k,
        }
    }
}

/// This project's uploaded files used by the resolved definition, including
/// nested file values and files selected by stored key instead of disk path.
/// Execution/project/shared files keep their own lifetime rules.
pub fn referenced_asset_keys(project: &ProjectDefinition) -> Result<Vec<String>> {
    let mut keys = BTreeSet::new();
    let project_id = project.id.to_string();
    for node in &project.nodes {
        for port in &node.inputs {
            let Some(value) = node.written_value(&port.name).or(port.default.as_ref()) else { continue };
            for slot in media_slots(value, &port.port_type) {
                if let MediaSlotContent::Stored { handle: FileHandle::Key(key), .. } =
                    classify_media_slot(&slot).map_err(anyhow::Error::msg)?
                {
                    let parsed = parse_key(&key).map_err(anyhow::Error::msg)?;
                    if matches!(parsed.scope, KeyScope::Asset { project_id: owner } if owner == project_id) {
                        keys.insert(key);
                    }
                }
            }
        }
    }
    Ok(keys.into_iter().collect())
}

/// Stream-hash a reader: `(sha256 hex, total bytes)` without ever holding the
/// whole content. The hash is the asset's identity everywhere (storage id,
/// diff key, upload dedup).
pub fn hash_reader(r: impl Read) -> Result<(String, u64)> {
    let (hash, size, _) = hash_reader_peeking(r)?;
    Ok((hash, size))
}

/// How many leading bytes the sync keeps aside to name a file's media type
/// and check its kind. Binary signatures need a dozen; an SVG's root
/// element can sit behind a prolog, a doctype and a generator comment.
const SIGNATURE_HEAD: usize = 1024;

/// [`hash_reader`], also handing back the file's first bytes (the
/// signature the declared kind is checked against), so the kind check
/// costs no second read of the file.
pub fn hash_reader_peeking(mut r: impl Read) -> Result<(String, u64, Vec<u8>)> {
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total: u64 = 0;
    let mut head: Vec<u8> = Vec::with_capacity(SIGNATURE_HEAD);
    loop {
        let n = r.read(&mut buf).context("read for hashing")?;
        if n == 0 {
            break;
        }
        if head.len() < SIGNATURE_HEAD {
            let take = (SIGNATURE_HEAD - head.len()).min(n);
            head.extend_from_slice(&buf[..take]);
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((format!("{:x}", hasher.finalize()), total, head))
}

/// A reader that hashes what passes through it, so the upload read can be
/// held to the hash the first read produced.
struct HashingReader<R> {
    inner: R,
    hasher: sha2::Sha256,
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
}

/// Ensure `refs` are uploaded and return the compiler's resolution map
/// (`FileRef::resolution_key -> stored-file value`).
///
/// Every missing/unreadable file, and every file whose bytes are not the
/// kind its `@asset` declared (an `Image` over an mp3), is collected and
/// reported in ONE loud error (so a build with three broken refs names all
/// three, not the first). Upload failures abort loudly. No existing file is
/// deleted: a paused execution may still need an earlier version.
pub async fn sync_assets(
    refs: &[FileRef],
    source: &dyn AssetSource,
    store: &dyn AssetStore,
) -> Result<BTreeMap<String, serde_json::Value>> {
    // 1. Hash every referenced file, name its media type from its bytes
    //    (the filename only when the bytes carry no signature), and hold
    //    those bytes to the kind the ref declared. The collector deduped
    //    refs by path AND declared type, so one path under two types is
    //    checked once per type; identical bytes share one stored asset
    //    whatever they are declared.
    let mut hashed: Vec<(&FileRef, String, u64, &'static str)> = Vec::with_capacity(refs.len());
    let mut broken: Vec<String> = Vec::new();
    for r in refs {
        match source.open(&r.path).and_then(hash_reader_peeking) {
            Ok((hash, size, head)) => {
                match weft_core::storage::check_declared_kind(&r.ty, &head, &r.path) {
                    Ok(()) => {
                        let mime = weft_core::storage::sniff_mime(&head)
                            .unwrap_or_else(|| weft_core::storage::mime_from_filename(&r.path));
                        hashed.push((r, hash, size, mime));
                    }
                    Err(e) => broken.push(format!("  {e}")),
                }
            }
            Err(e) => broken.push(format!("  {}: {e:#}", r.path)),
        }
    }
    if !broken.is_empty() {
        bail!(
            "{} referenced asset(s) could not be used:\n{}",
            broken.len(),
            broken.join("\n")
        );
    }

    // 2. Diff against what the store already holds.
    let existing = store.list().await.context("list existing assets")?;

    // 3. Upload the missing content (a second reader pass streams the bytes;
    //    hashing buffered them nowhere). The second pass is hashed too and
    //    held to the first: a file edited between the two reads would
    //    otherwise be stored under another content's hash, and every later
    //    build would take it for that content. Record every asset's key.
    let mut keys: BTreeMap<&str, String> = BTreeMap::new();
    for (r, hash, size, mime) in &hashed {
        if let Some(key) = existing.get(hash.as_str()) {
            keys.insert(hash, key.clone());
            continue;
        }
        if keys.contains_key(hash.as_str()) {
            continue; // two paths, identical bytes: already uploaded this pass
        }
        let reader = source
            .open(&r.path)
            .with_context(|| format!("re-open asset {} for upload", r.path))?;
        let mut reader = HashingReader { inner: reader, hasher: sha2::Sha256::new() };
        let uploaded = store
            .upload(hash, mime, &r.path, *size, &mut reader)
            .await
            .with_context(|| format!("upload asset {}", r.path))?;
        // Only a real transfer has a streamed hash to check. An
        // `AlreadyStored` answer read nothing (the store already held this
        // content), so hashing what came off the reader would compare the
        // file against the empty digest and throw away a healthy asset.
        if let Uploaded::Stored(key) = &uploaded {
            let streamed = format!("{:x}", reader.hasher.finalize());
            if streamed != *hash {
                store
                    .delete(key)
                    .await
                    .with_context(|| format!("discard the changed asset {} (stored as {key})", r.path))?;
                bail!(
                    "asset {} changed while it was being synced (it hashed {hash} when the build \
                     read it and {streamed} when it was uploaded); rerun the build",
                    r.path
                );
            }
        }
        keys.insert(hash, uploaded.key().to_string());
    }

    // 4. The compiler's map: path -> stored-file value, marker kind picked by
    //    the DECLARED type (an `Image` ref is `__weft_image__` whatever the
    //    extension guesses).
    let mut map = BTreeMap::new();
    for (r, hash, size, mime) in &hashed {
        let key = keys.get(hash.as_str()).expect("every hashed ref got a key");
        let file = StoredFile {
            key: key.clone(),
            mime_type: mime.to_string(),
            size_bytes: *size,
            filename: r.path.clone(),
        };
        map.insert(r.resolution_key(), weft_core::storage::typed_file_value(&file, &r.ty));
    }
    Ok(map)
}
