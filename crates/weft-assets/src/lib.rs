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
//!
//! The hash-diff-upload core is [`publish_hashed`], and it is not only
//! the asset sync's: a version snapshot (`weft checkpoint`, every `weft
//! run`) publishes the project's files through [`publish_files`] into
//! the same content-addressed plane, so a file identical to one any
//! earlier version held costs nothing to record again.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek};

use anyhow::{bail, Context, Result};
use sha2::Digest;
use weft_core::project::{FileRef, ProjectDefinition};
use weft_core::storage::{FileHandle, StoredFile};
use weft_core::storage::key::{parse_key, KeyScope};
use weft_core::storage::media::{classify_media_slot, media_slots, MediaSlotContent};

pub trait AssetReader: Read + Seek + Send {}
impl<T: Read + Seek + Send> AssetReader for T {}

/// Where the project's files live. Paths are as written in an `@asset`
/// ref: project-relative, or absolute/outside where the source allows
/// it. Errors name the path ("asset not found" is the loud build error).
pub trait AssetSource: Send + Sync {
    /// A streaming reader over the file: one forward pass, bounded
    /// memory, no promise about what a second open sees. Hashing reads
    /// this way.
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>>;
    /// A reader over bytes that cannot change under the caller, which
    /// [`publish_hashed`] verifies against the recorded hash and then
    /// rewinds and uploads. Only the files that are about to be stored
    /// are opened this way, because a disk source pays for it with a
    /// private copy of the file: a new asset is read once to hash it, once
    /// to copy it, once to verify the copy and once to upload it. An asset
    /// the store already holds is only ever read once.
    fn snapshot(&self, path: &str) -> Result<Box<dyn AssetReader>>;
}

/// The project's asset plane in runtime storage.
#[async_trait::async_trait]
pub trait AssetStore: Send + Sync {
    /// Every existing asset of the project: `content hash -> full storage key`.
    async fn list(&self) -> Result<BTreeMap<String, String>>;
    /// Upload one asset's bytes under its content hash. MUST be idempotent
    /// for an already-ACTIVE identical hash (same content = same asset),
    /// and MUST error for anything else.
    ///
    /// The bytes are a verified snapshot. Return their stored key whether
    /// this call uploads them or another publisher already completed them.
    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String>;
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

/// One file hashed and ready to publish: what [`publish_hashed`] needs
/// to know about it. Built by [`hash_files`] or, for an `@asset` ref, by
/// [`sync_assets`] after its kind check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashedFile {
    /// The path as the source opens it.
    pub path: String,
    /// The content hash: the file's identity everywhere.
    pub hash: String,
    pub size: u64,
    /// The media type the store records for it.
    pub mime: String,
}

/// Where a published file lives in the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Published {
    pub hash: String,
    pub key: String,
}

/// Hash every file in `paths` through `source`, naming each one's media
/// type from its first bytes (the filename when the bytes carry no
/// signature). Every path that cannot be read is collected and reported
/// in ONE loud error, so a snapshot with three missing files names all
/// three.
pub fn hash_files(paths: &[String], source: &dyn AssetSource) -> Result<Vec<HashedFile>> {
    let mut hashed = Vec::with_capacity(paths.len());
    let mut broken: Vec<String> = Vec::new();
    for path in paths {
        match source.open(path).and_then(hash_reader_peeking) {
            Ok((hash, size, head)) => {
                let mime = weft_core::storage::sniff_mime(&head)
                    .unwrap_or_else(|| weft_core::storage::mime_from_filename(path));
                hashed.push(HashedFile { path: path.clone(), hash, size, mime: mime.to_string() });
            }
            Err(e) => broken.push(format!("  {path}: {e:#}")),
        }
    }
    if !broken.is_empty() {
        bail!("{} file(s) could not be read:\n{}", broken.len(), broken.join("\n"));
    }
    Ok(hashed)
}

/// Make sure the store holds every hashed file, and answer each one's
/// key: `content hash -> full storage key`. The content-addressed core
/// every publisher shares (an `@asset` sync, a version snapshot): diff
/// against what the store already holds, upload only the missing
/// content, never delete an existing file (a paused execution may still
/// need an earlier version). Each missing file is uploaded from a
/// snapshot verified against its recorded hash first: parts are stored
/// by number under a key that is the content's hash, so bytes that do
/// not match must never reach a part another publisher of the same
/// content may be writing too.
pub async fn publish_hashed(
    hashed: &[HashedFile],
    source: &dyn AssetSource,
    store: &dyn AssetStore,
) -> Result<BTreeMap<String, String>> {
    // 1. Diff against what the store already holds.
    let existing = store.list().await.context("list existing assets")?;

    // 2. Verify each missing file's snapshot against the earlier hash,
    //    then rewind and upload those exact bytes. Record every key.
    let mut keys: BTreeMap<String, String> = BTreeMap::new();
    for HashedFile { path, hash, size, mime } in hashed {
        if let Some(key) = existing.get(hash.as_str()) {
            keys.insert(hash.clone(), key.clone());
            continue;
        }
        if keys.contains_key(hash.as_str()) {
            continue; // two paths, identical bytes: already uploaded this pass
        }
        let mut reader = source
            .snapshot(path)
            .with_context(|| format!("snapshot {path} for upload"))?;
        let (actual_hash, actual_size) = hash_reader(&mut reader)?;
        anyhow::ensure!(actual_hash == *hash && actual_size == *size,
            "{path} changed while it was being published; rerun the command");
        reader.rewind().with_context(|| format!("rewind verified snapshot of {path}"))?;
        let uploaded = store
            .upload(hash, mime, path, *size, reader.as_mut())
            .await
            .with_context(|| format!("upload {path}"))?;
        keys.insert(hash.clone(), uploaded);
    }
    Ok(keys)
}

/// Publish every file in `paths` and answer where each one landed:
/// `path -> Published { hash, key }`. [`hash_files`] then
/// [`publish_hashed`]; a version snapshot is exactly this over the
/// files a version covers.
pub async fn publish_files(
    paths: &[String],
    source: &dyn AssetSource,
    store: &dyn AssetStore,
) -> Result<BTreeMap<String, Published>> {
    let hashed = hash_files(paths, source)?;
    let keys = publish_hashed(&hashed, source, store).await?;
    Ok(hashed
        .into_iter()
        .map(|h| {
            let key = keys.get(&h.hash).expect("every hashed file got a key").clone();
            (h.path, Published { hash: h.hash, key })
        })
        .collect())
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
    let mut hashed: Vec<(&FileRef, HashedFile)> = Vec::with_capacity(refs.len());
    let mut broken: Vec<String> = Vec::new();
    for r in refs {
        match source.open(&r.path).and_then(hash_reader_peeking) {
            Ok((hash, size, head)) => {
                // The check reports only what is wrong with the bytes;
                // the `@asset(...)` that declared them is this caller's
                // to name, since this is the one place it is the source.
                match weft_core::storage::check_declared_kind(&r.ty, &head)
                    .map_err(|why| format!("@asset({:?}, {}): {why}", r.path, r.ty))
                {
                    Ok(()) => {
                        let mime = weft_core::storage::sniff_mime(&head)
                            .unwrap_or_else(|| weft_core::storage::mime_from_filename(&r.path));
                        hashed.push((
                            r,
                            HashedFile { path: r.path.clone(), hash, size, mime: mime.to_string() },
                        ));
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

    // 2. The shared content-addressed core verifies and uploads missing content.
    let files: Vec<HashedFile> = hashed.iter().map(|(_, h)| h.clone()).collect();
    let keys = publish_hashed(&files, source, store).await?;

    // 3. The compiler's map: path -> stored-file value, marker kind picked by
    //    the DECLARED type (an `Image` ref is `__weft_image__` whatever the
    //    extension guesses).
    let mut map = BTreeMap::new();
    for (r, h) in &hashed {
        let key = keys.get(h.hash.as_str()).expect("every hashed ref got a key");
        let file = StoredFile {
            key: key.clone(),
            mime_type: h.mime.clone(),
            size_bytes: h.size,
            filename: r.path.clone(),
        };
        map.insert(r.resolution_key(), weft_core::storage::typed_file_value(&file, &r.ty));
    }
    Ok(map)
}
