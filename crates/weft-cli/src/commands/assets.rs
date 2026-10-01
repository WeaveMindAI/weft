//! The CLI's asset-sync driver: right before a build, make the project's
//! asset storage contain what the code references and resolve the `@asset`
//! refs in the compiled definition (see `weft-assets` for the sync itself
//! and `docs` for the model). The project's files live on disk (paths
//! outside the project are legal: the ref names wherever the file already
//! is), and the store is the dispatcher's storage surface.

use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use weft_assets::{AssetSource, AssetStore};

use crate::client::DispatcherClient;

/// What the author's machine resolves of a definition's `@asset` refs
/// before a build: every file ref's stored-file value (the files synced to
/// the tenant's assets first), every stored-key ref's, and every text
/// ref's fetched-and-cast value, by resolution key. The build sends the map
/// with the version; the install re-reads each stored file it names.
pub struct AssetResolutions {
    pub map: BTreeMap<String, serde_json::Value>,
    /// Stored-key refs whose SOURCE file a future build still needs, even
    /// when this build inlined its content (a text-typed stored asset).
    source_refs: Vec<weft_core::project::FileRef>,
}

/// Resolve the definition's `@asset` refs to the map a build sends
/// (see [`AssetResolutions`]). With `publish`, a referenced file the asset
/// plane lacks is uploaded; without, it is an error naming `weft bake`.
pub async fn asset_resolutions(
    client: &DispatcherClient,
    project_root: &std::path::Path,
    definition: &weft_core::project::ProjectDefinition,
    publish: bool,
) -> Result<AssetResolutions> {
    let (map, key_refs, text_refs) = resolve_asset_map(client, project_root, definition, publish).await?;
    let source_refs = key_refs
        .into_iter()
        .chain(text_refs.into_iter().filter(weft_compiler::file_ref::is_runtime_key_ref))
        .collect();
    Ok(AssetResolutions { map, source_refs })
}

/// Tell the store which assets the project's current source references,
/// so a file no source uses any more starts expiring. `definition` is the
/// RESOLVED program (its stored-file values name what it uses); `sources`
/// adds the version's own blobs. Answers the store's warnings (a version
/// whose stored file is gone), for the caller to show.
pub async fn publish_references(
    client: &DispatcherClient,
    definition: &weft_core::project::ProjectDefinition,
    resolutions: &AssetResolutions,
    sources: Option<&weft_core::project::hash::Manifest>,
) -> Result<Vec<String>> {
    let mut references = asset_references(definition, resolutions.source_refs.iter())?;
    if let Some(sources) = sources {
        let scope = weft_core::storage::key::KeyScope::Asset;
        for hash in sources.values() {
            references.keys.push(weft_core::storage::key::scope_key(&scope, hash).map_err(anyhow::Error::msg)?);
        }
    }
    let (status, text) = client.post_json_status("/storage/assets/references", &serde_json::to_value(references)?)
        .await.context("update project asset lifetimes")?;
    if !(200..300).contains(&status) {
        // The build's own file is what is missing here (a version's is a
        // warning, never a refusal): the upload just made did not land.
        bail!("update project asset lifetimes: {}\nRun the command again; if it repeats, `weft files ls` shows what storage holds for your account",
            if text.trim().is_empty() { format!("dispatcher returned {status}") } else { text.trim().to_string() });
    }
    let published: weft_core::storage::AssetsPublished = serde_json::from_str(&text).context("parse the publish answer")?;
    Ok(published.warnings)
}

/// Resolve the `@file` and `@asset` markers in a run's handed values
/// (`--emit`, `--from`, `--group`, `--fire`, or a saved example) exactly
/// as a build resolves the same markers written in source: a `@file`
/// read from the project and cast to its type, an `@asset` uploaded into
/// the tenant's assets (or fetched, or looked up) and replaced by
/// the value its declared type stands for. An uploaded file is not added
/// to the project's published assets, so it lives on the store's own
/// countdown, which every read of it pushes back.
pub async fn resolve_run_values(
    client: &DispatcherClient,
    project_root: &std::path::Path,
    spec: &mut weft_core::run_spec::RunSpec,
) -> Result<()> {
    let fs = weft_compiler::CompileFs::disk(project_root);
    weft_compiler::file_ref::resolve_file_markers(spec, &fs)
        .map_err(|errs| anyhow::anyhow!("a run value cannot be read:\n  {}", errs.join("\n  ")))?;
    let (map, _, _) = resolve_asset_map(client, project_root, &*spec, true).await?;
    weft_compiler::file_ref::apply_asset_resolutions(spec, &map)
        .map_err(|errs| anyhow::anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))?;
    Ok(())
}

/// The asset step both callers share: sync the file-kind `@asset`s on
/// disk, look up the stored-key ones, fetch and cast the text ones. Hands
/// back every resolved value by resolution key, and the stored-key and
/// text refs, which a build's publish names.
async fn resolve_asset_map(
    client: &DispatcherClient,
    project_root: &std::path::Path,
    target: &impl weft_compiler::file_ref::MarkedValues,
    publish: bool,
) -> Result<(BTreeMap<String, serde_json::Value>, Vec<weft_core::project::FileRef>, Vec<weft_core::project::FileRef>)> {
    let refs = weft_compiler::file_ref::collect_asset_refs(target);
    let mut map = if refs.is_empty() {
        BTreeMap::new()
    } else {
        let source = DiskSource::new(project_root.to_path_buf());
        let mut store = DispatcherStore::new(client);
        store.publish = publish;
        weft_assets::sync_assets(&refs, &source, &store).await.context("sync project assets")?
    };
    // Refs whose source is a RUNTIME STORAGE KEY (a stored file picked in the
    // editor): nothing to sync, resolve them against the tenant's file
    // listing (the `weft files` door). The match itself is the compiler's
    // shared step so every build driver resolves identically.
    let key_refs = weft_compiler::file_ref::collect_runtime_key_refs(target);
    if !key_refs.is_empty() {
        let listing: weft_core::storage::ListFilesResponse = serde_json::from_value(
            client.get_json("/storage/files").await.context("list stored files")?,
        )
        .context("parse stored-file listing")?;
        weft_compiler::file_ref::resolve_runtime_key_refs(&key_refs, &listing.files, &mut map)
            .map_err(|errs| anyhow::anyhow!("stored files of the wrong kind:\n  {}", errs.join("\n  ")))?;
    }
    // Text values: read once per build (a URL fetched, a stored file
    // downloaded, a disk path read from wherever it points, the project
    // root or anywhere on the machine), cast, and substituted like every
    // other deferred ref. Nothing is uploaded for a text asset: its value
    // is inlined into the build, so no stored copy would be referenced.
    let text_refs = weft_compiler::file_ref::collect_text_refs(target);
    if !text_refs.is_empty() {
        let http = reqwest::Client::new();
        let mut failed: Vec<String> = Vec::new();
        for r in &text_refs {
            let fetched: Result<Vec<u8>> = if weft_compiler::file_ref::is_url_ref(r) {
                fetch_url_bytes(&http, &r.path).await
            } else if weft_compiler::file_ref::is_runtime_key_ref(r) {
                crate::commands::files::download_bytes(client, &r.path).await
            } else {
                let path = std::path::Path::new(&r.path);
                let full = if path.is_absolute() { path.to_path_buf() } else { project_root.join(path) };
                std::fs::read(&full).with_context(|| format!("read {}", full.display()))
            };
            match fetched.and_then(|bytes| {
                weft_compiler::file_ref::resolve_text_bytes(r, &bytes).map_err(anyhow::Error::msg)
            }) {
                Ok(value) => {
                    map.insert(r.resolution_key(), value);
                }
                Err(e) => failed.push(format!("{}: {e:#}", r.path)),
            }
        }
        if !failed.is_empty() {
            bail!("text assets could not be fetched:\n  {}", failed.join("\n  "));
        }
    }
    Ok((map, key_refs, text_refs))
}

/// Resolve the definition's `@asset` refs in place (for a caller that
/// hashes or reads the resolved program itself: `weft status`'s drift,
/// the editor's preview), and with `publish`, record the references.
/// Hands back the publish's warnings; empty without `publish`.
pub async fn resolve_project_assets(
    client: &DispatcherClient,
    project_root: &std::path::Path,
    definition: &mut weft_core::project::ProjectDefinition,
    publish: bool,
) -> Result<Vec<String>> {
    let resolutions = asset_resolutions(client, project_root, definition, publish).await?;
    weft_compiler::file_ref::apply_asset_resolutions(definition, &resolutions.map)
        .map_err(|errs| anyhow::anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))?;
    if !publish {
        return Ok(Vec::new());
    }
    // Published only after every reference resolved.
    publish_references(client, definition, &resolutions, None).await
}

fn asset_references<'a>(
    definition: &weft_core::project::ProjectDefinition,
    source_refs: impl Iterator<Item = &'a weft_core::project::FileRef>,
) -> Result<weft_core::storage::AssetReferencesRequest> {
    let mut references = weft_core::storage::AssetReferencesRequest {
        project: definition.id.to_string(),
        keys: weft_assets::referenced_asset_keys(definition)?,
        kept: Vec::new(),
    };
    // A text-typed stored asset is read at build time and becomes plain text
    // in the definition. Its SOURCE still needs the file for future builds.
    // Keep those source keys too; the dispatcher adds the authenticated tenant.
    for reference in source_refs {
        if reference.path.starts_with("asset/")
            && weft_core::storage::key::is_scope_key(&reference.path)
        {
            references.keys.push(reference.path.clone());
        }
    }
    Ok(references)
}

/// One GET of a text asset's URL, whole body in memory (a prompt, a JSON
/// shape: small by contract). A non-2xx answer is the loud build error.
async fn fetch_url_bytes(http: &reqwest::Client, url: &str) -> Result<Vec<u8>> {
    let resp = http.get(url).send().await.with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        bail!("GET {url} answered {}", resp.status());
    }
    Ok(resp.bytes().await.with_context(|| format!("read {url}"))?.to_vec())
}

/// Local project files: paths resolve against the project root; an absolute
/// path is used as-is (a local ref may point anywhere on the machine).
pub(crate) struct DiskSource {
    root: PathBuf,
}

impl DiskSource {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

impl DiskSource {
    fn file(&self, path: &str) -> Result<(PathBuf, std::fs::File)> {
        let p = std::path::Path::new(path);
        let full = if p.is_absolute() { p.to_path_buf() } else { self.root.join(p) };
        let file = std::fs::File::open(&full)
            .with_context(|| format!("asset not found at {}", full.display()))?;
        Ok((full, file))
    }
}

impl AssetSource for DiskSource {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        Ok(Box::new(self.file(path)?.1))
    }

    /// A private copy under the project's own `.weft/`: a real disk with
    /// the project's own free space, where the system temp dir is often
    /// memory-backed and an asset can be gigabytes. (An asset referenced by
    /// an absolute path from another disk is copied here too.) The file is
    /// anonymous, so nothing can see it by name, and it goes with the handle.
    fn snapshot(&self, path: &str) -> Result<Box<dyn weft_assets::AssetReader>> {
        let (full, mut file) = self.file(path)?;
        let scratch = self.root.join(".weft");
        std::fs::create_dir_all(&scratch).with_context(|| format!("create {}", scratch.display()))?;
        let mut snapshot = tempfile::tempfile_in(&scratch)
            .with_context(|| format!("create a snapshot of {} under {}", full.display(), scratch.display()))?;
        std::io::copy(&mut file, &mut snapshot).with_context(|| format!("snapshot {}", full.display()))?;
        snapshot.rewind().with_context(|| format!("rewind the snapshot of {}", full.display()))?;
        Ok(Box::new(snapshot))
    }
}

/// The dispatcher-backed assets of the caller's tenant: control calls go to
/// the dispatcher's storage surface, bytes go straight to the bucket on the
/// presigned part URLs it returns (the same contract the editor's upload
/// field drives).
pub(crate) struct DispatcherStore<'a> {
    publish: bool,
    client: &'a DispatcherClient,
    /// For the presigned part PUTs (bucket-direct; not dispatcher traffic).
    http: reqwest::Client,
}

impl<'a> DispatcherStore<'a> {
    pub(crate) fn new(client: &'a DispatcherClient) -> Self {
        Self { client, http: reqwest::Client::new(), publish: true }
    }
}

#[async_trait::async_trait]
impl AssetStore for DispatcherStore<'_> {

    async fn held(&self, hashes: &[String]) -> Result<BTreeMap<String, String>> {
        let resp = self
            .client
            .post_json(
                "/storage/assets/held",
                &serde_json::to_value(weft_core::storage::AssetsHeldRequest { hashes: hashes.to_vec() })?,
            )
            .await
            .context("ask which contents are already stored")?;
        let held: weft_core::storage::AssetsHeldResponse =
            serde_json::from_value(resp).context("parse the stored-contents answer")?;
        // Each key must be the asset its hash names. Parsed through the one
        // key grammar and failed loud on anything else: an entry taken on
        // faith would skip uploading content the store does not hold.
        for (hash, key) in &held.keys {
            let parsed = weft_core::storage::key::parse_key(key)
                .map_err(|e| anyhow::anyhow!("the stored-contents answer holds a malformed key: {e}"))?;
            if parsed.scope != weft_core::storage::key::KeyScope::Asset || &parsed.id != hash {
                bail!("the stored-contents answer names '{key}' for {hash}, which is not that content's asset");
            }
        }
        Ok(held.keys)
    }

    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String> {
        anyhow::ensure!(self.publish, "asset '{filename}' is not stored in your account's assets yet; run `weft bake` to publish the current files and capture trigger settings");
        // NOT wrapped in a Ctrl-C handler, and that is deliberate.
        //
        // Cancelling the upload on an interrupt looks right and costs too
        // much: `tokio::signal::ctrl_c` installs a PROCESS-GLOBAL handler
        // that permanently replaces the default die-on-SIGINT, and it is
        // not lifted when the future is dropped. An upload happens early
        // in `weft run` and `weft build`, long before the waits whose
        // documented recovery IS Ctrl-C (the infra wait, a build, a live
        // connection), so taking the signal here left those unkillable.
        //
        // The interrupted upload is handled where it lands instead: the
        // store keeps what arrived, and the next publish of the same
        // content resumes it through the upload protocol. Nothing is lost
        // and nothing is blocked, so there is no reason to own the signal.
        self.transfer(hash, mime, filename, size_bytes, bytes).await
    }
}

impl DispatcherStore<'_> {
    /// PUT one reserved part's bytes and report its etag.
    async fn put_part(
        &self,
        key: &str,
        part: &weft_core::storage::PresignedPart,
        body: &[u8],
    ) -> Result<()> {
        let resp = self
            .http
            .put(&part.url)
            .body(body.to_vec())
            .send()
            .await
            .context("PUT asset part to bucket")?;
        if !resp.status().is_success() {
            bail!("asset part PUT failed: HTTP {}", resp.status());
        }
        let etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .context("bucket returned no ETag for asset part")?
            .to_string();
        self.client
            .post_with_body(
                "/storage/upload/part-done",
                &serde_json::to_value(weft_core::storage::PartDoneRequest {
                    key: key.to_string(),
                    part_number: part.part_number,
                    etag,
                })?,
            )
            .await
            .context("report asset part")?;
        Ok(())
    }

    async fn transfer(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String> {
        let mut upload_key = None;
        let transferred: Result<String> = async {
            let (status, body) = self
                .client
                .post_json_status(
                    "/storage/upload/begin",
                    &serde_json::to_value(weft_core::storage::AssetUploadBeginRequest {
                        mime_type: mime.to_string(),
                        filename: filename.to_string(),
                        declared_size: Some(size_bytes),
                        content_hash: hash.to_string(),
                    })?,
                )
                .await
                .context("begin asset upload")?;
            // Three answers, all from the begin itself, so this never has to
            // guess the key: the store mints it (it is tenant-anchored) and
            // says it in every one of them.
            let (key, part_size, mut reserved, carved) = if (200..300).contains(&status) {
                let begin: weft_core::storage::UploadBeginResponse =
                    serde_json::from_str(&body).context("parse upload/begin")?;
                upload_key = Some(begin.key.clone());
                // Already stored under this content's key: the upload's
                // idempotent success. Content-addressed, so the same bytes are
                // the same asset, and the begin answers the existing key with
                // nothing to transfer.
                if begin.already_stored {
                    return Ok(begin.key);
                }
                if begin.resume {
                    // The same content is already part way up: an earlier
                    // publish that stopped (a Ctrl-C, a dropped connection), or
                    // another publish of the same asset running right now.
                    // Carrying on with it is safe either way, because a part is
                    // reserved by NUMBER and both writers put identical bytes in
                    // it. The first to call complete claims the upload; from
                    // then on the store refuses the other's changes as
                    // "completing", and that one waits for the file to land
                    // (see the error path below).
                    let (rstatus, rbody) = self
                        .client
                        .post_json_status(
                            "/storage/upload/resume",
                            &serde_json::to_value(weft_core::storage::UploadResumeRequest { key: begin.key.clone() })?,
                        )
                        .await
                        .context("resume the earlier upload of this content")?;
                    if (200..300).contains(&rstatus) {
                        let resumed: weft_core::storage::UploadResumeResponse =
                            serde_json::from_str(&rbody).context("parse upload/resume")?;
                        (begin.key, resumed.part_size as usize, resumed.missing, resumed.reserved_bytes)
                    } else {
                        // It was swept between the begin and this call, so its
                        // key frees itself and the next attempt starts fresh.
                        bail!(
                            "the unfinished upload of {filename} was cleared from the store while \
                             this was picking it up (the store said: {}). Nothing is lost: run the \
                             same command again.",
                            rbody.trim()
                        );
                    }
                } else {
                    (begin.key, begin.part_size as usize, Vec::new(), 0u64)
                }
            } else {
                bail!("begin asset upload for {filename}: HTTP {status}: {}", body.trim());
            };

            // Stream: read one part-sized chunk at a time, reserve + PUT + report.
            let mut buf = vec![0u8; part_size];
            // How far into the file the reader has got. The file is read once,
            // forwards, because it arrives as a stream and cannot be seeked.
            let mut at = 0u64;
            // The parts a resume named are already RESERVED, with their own
            // numbers, sizes and places in the file, so they are sent as they
            // are before anything new is reserved: `complete` refuses while a
            // reserved part has no etag, so skipping them would leave the
            // upload unfinishable.
            //
            // In offset order, and each one written at the offset the STORE
            // stated. A resume can name a part from the middle of the file
            // (any part whose PUT failed while a later one succeeded), and
            // working the offsets out here instead meant assuming the parts
            // handed back are one run at the end of what has landed.
            reserved.sort_by_key(|p| p.offset_bytes);
            // The sizes and offsets came off the wire, like the part URLs, so
            // they are checked before they index anything: a part claiming
            // more than one part's worth, or a place past the end of the
            // file, would have panicked on the slice rather than failed.
            if let Some(bad) = reserved.iter().find(|p| {
                p.size_bytes > part_size as u64 || p.offset_bytes.saturating_add(p.size_bytes) > size_bytes
            }) {
                bail!(
                    "the store placed part {} of the unfinished upload of {filename} at byte {} with \
                     {} bytes, which does not fit a {size_bytes} byte file in parts of up to \
                     {part_size} bytes; cancel that upload and publish again",
                    bad.part_number,
                    bad.offset_bytes,
                    bad.size_bytes
                );
            }
            for part in reserved {
                // Forward to this part's place, reading and dropping what the
                // store already holds.
                while at < part.offset_bytes {
                    let want = (buf.len() as u64).min(part.offset_bytes - at) as usize;
                    let mut filled = 0;
                    while filled < want {
                        let n = bytes.read(&mut buf[filled..want]).context("skip already-uploaded bytes")?;
                        if n == 0 {
                            bail!(
                                "asset {filename} is shorter than the {} bytes already uploaded under \
                                 its own content hash; cancel that upload and publish again",
                                part.offset_bytes
                            );
                        }
                        filled += n;
                    }
                    at += want as u64;
                }
                let want = part.size_bytes as usize;
                let mut filled = 0;
                while filled < want {
                    let n = bytes.read(&mut buf[filled..want]).context("read asset chunk")?;
                    if n == 0 {
                        bail!(
                            "asset {filename} shrank while uploading (read {at} + {filled} of \
                             {size_bytes} bytes); rerun the command"
                        );
                    }
                    filled += n;
                }
                self.put_part(&key, &part, &buf[..want]).await?;
                at += want as u64;
            }
            // New parts begin where the store stopped carving, which is not
            // where the last resent part ended whenever the missing part was
            // in the middle. Forward the reader there.
            while at < carved {
                let want = (buf.len() as u64).min(carved - at) as usize;
                let mut filled = 0;
                while filled < want {
                    let n = bytes.read(&mut buf[filled..want]).context("skip already-uploaded bytes")?;
                    if n == 0 {
                        bail!(
                            "asset {filename} is shorter than the {carved} bytes already carved into \
                             parts under its own content hash; cancel that upload and publish again"
                        );
                    }
                    filled += n;
                }
                at += want as u64;
            }
            let mut sent = at;
            while sent < size_bytes {
                let want = part_size.min((size_bytes - sent) as usize);
                let mut filled = 0;
                while filled < want {
                    let n = bytes.read(&mut buf[filled..want]).context("read asset chunk")?;
                    if n == 0 {
                        bail!(
                            "asset {filename} shrank while uploading (read {sent} + {filled} of \
                             {size_bytes} bytes); rerun the build"
                        );
                    }
                    filled += n;
                }
                // The part NUMBER is the position, so this says which part of
                // the file it is about to send rather than asking for "the next
                // one". Two publishes of the same content (the key is its hash,
                // so the bytes are identical) then name the same part instead of
                // each extending the reservation past the end of the file.
                let part_number = (sent / part_size as u64) as i32 + 1;
                let parts = self
                    .client
                    .post_json(
                        "/storage/upload/parts",
                        &serde_json::to_value(weft_core::storage::UploadPartsRequest {
                            key: key.clone(),
                            parts: vec![weft_core::storage::PartAsk { part_number, size_bytes: want as u64 }],
                        })?,
                    )
                    .await
                    .context("reserve asset part")?;
                let parts: weft_core::storage::UploadPartsResponse =
                    serde_json::from_value(parts).context("parse the reserved part")?;
                let part = parts.parts.into_iter().next().context("upload/parts returned no part")?;
                self.put_part(&key, &part, &buf[..want]).await?;
                sent += want as u64;
            }

            self.client
                .post_json(
                    "/storage/upload/complete",
                    &serde_json::to_value(weft_core::storage::UploadCompleteRequest { key: key.clone() })?,
                )
                .await
                .context("complete asset upload")?;
            Ok(key)
        }.await;
        let error = match transferred {
            Ok(uploaded) => return Ok(uploaded),
            Err(error) => error,
        };
        // Any transfer step can lose to another publisher finishing these
        // same content-addressed bytes. When the store said so outright (a
        // "completing" refusal), the file is about to land: wait for it with
        // the same bounded backoff the engine gives a completion. Otherwise
        // look once, without retrying the upload or interpreting error text.
        let Some(key) = upload_key else { return Err(error) };
        let completing = error.chain().any(|cause| cause.is::<crate::client::StoreCompleting>());
        let mut attempt = 0;
        loop {
            let stored = self.client.get_json_if_found(&format!("/storage/files/meta/{key}")).await
                .with_context(|| format!("{error:#}; could not determine whether another publisher completed this asset"))?;
            if let Some(stored) = stored {
                let meta: weft_core::storage::StoredFileMeta = serde_json::from_value(stored)
                    .with_context(|| format!("{error:#}; the store's answer about {key} did not parse"))?;
                // The key is the content hash the store was told at begin,
                // and the store checks only the assembled size, so this
                // confirms a finished upload under this key, not the bytes
                // themselves.
                anyhow::ensure!(
                    meta.key == key && meta.size_bytes == size_bytes,
                    "{error:#}; the store holds {} under {key} at {} bytes where {filename} is {size_bytes} bytes, \
                     so that upload is not this content",
                    meta.key, meta.size_bytes
                );
                return Ok(key);
            }
            if !completing {
                return Err(error);
            }
            let Some(delay) = completing_wait(attempt) else {
                bail!(
                    "{error:#}; another publish of {filename} was completing it as {key}, and the file \
                     had not landed after waiting. The store finishes or ends that upload on its own; \
                     run the command again"
                );
            };
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

/// How long to wait before looking again for a file another caller is
/// completing, after `attempt` looks found nothing; `None` once the wait is
/// over. 1s doubling, capped at 30s, twelve times: about four minutes, near
/// the store's completion lease, after which its sweep finishes or ends the
/// upload itself.
// SYNC: completion backoff <-> crates/weft-engine/src/storage.rs COMPLETE_ATTEMPTS / complete_until_landed
fn completing_wait(attempt: u32) -> Option<std::time::Duration> {
    const ATTEMPTS: u32 = 12;
    (attempt < ATTEMPTS).then(|| std::time::Duration::from_secs(1u64 << attempt.min(5)).min(std::time::Duration::from_secs(30)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::{StatusCode, Uri}, response::IntoResponse, Json, Router};
    use weft_core::project::{FileMarker, FileRef, ProjectDefinition};
    use weft_core::weft_type::WeftType;

    #[test]
    fn an_asset_snapshot_does_not_change_with_the_source_file() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("asset"), "original").unwrap();
        let source = DiskSource::new(root.path().to_path_buf());
        let mut reader = source.snapshot("asset").unwrap();
        std::fs::write(root.path().join("asset"), "changed").unwrap();
        let mut content = String::new();
        reader.read_to_string(&mut content).unwrap();
        assert_eq!(content, "original");
        reader.rewind().unwrap();
        assert_eq!(weft_assets::hash_reader(reader).unwrap().1, 8);
    }

    /// The wait for a file another caller is completing has the engine's
    /// shape: 1s doubling, capped at 30s, then over.
    #[test]
    fn the_completing_wait_doubles_caps_and_ends() {
        let waits: Vec<u64> = (0..).map_while(completing_wait).map(|d| d.as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30, 30, 30, 30, 30, 30]);
    }

    /// Two publishes of the same new content: the other one claimed the
    /// completion, so this one's complete is refused as "completing" and the
    /// file is not there yet on the first look. It waits and gets the file.
    #[tokio::test]
    async fn a_completing_refusal_waits_for_the_other_publish_to_land() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let part_url = format!("{base}/part");
        let looks = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let app = Router::new().fallback(move |uri: Uri| {
            let part_url = part_url.clone();
            let looks = looks.clone();
            async move {
                let value = match uri.path() {
                    "/storage/upload/begin" => serde_json::json!({"key":"asset/hash", "part_size":4, "resume":false}),
                    "/storage/upload/parts" => serde_json::json!({"parts":[{"part_number":1,"size_bytes":4,"offset_bytes":0,"url":part_url}]}),
                    "/part" => return (StatusCode::OK, [("etag", "part-1")], "").into_response(),
                    "/storage/upload/part-done" => serde_json::json!({}),
                    "/storage/upload/complete" => {
                        return (StatusCode::CONFLICT, [(weft_core::storage::COMPLETING_HEADER, "retry")], "completing").into_response();
                    }
                    "/storage/files/meta/asset/hash" => {
                        if looks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                            return (StatusCode::NOT_FOUND, [("x-weft-not-found", "file")], "missing file").into_response();
                        }
                        serde_json::json!({"key":"asset/hash","mimeType":"text/plain","sizeBytes":4,"filename":"test","keep":false,"createdAtUnix":0,"version":1})
                    }
                    _ => return (StatusCode::NOT_FOUND, "unexpected request").into_response(),
                };
                Json(value).into_response()
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let client = DispatcherClient::new(base, None);
        let store = DispatcherStore::new(&client);
        let result = store.transfer("hash", "text/plain", "test", 4, &mut &b"data"[..]).await;
        server.abort();
        assert_eq!(result.unwrap(), "asset/hash");
    }

    #[tokio::test]
    async fn another_publisher_can_finish_at_any_upload_step() {
      for completed_elsewhere in [true, false] {
        for failed_path in ["/storage/upload/resume", "/storage/upload/parts", "/part", "/storage/upload/part-done", "/storage/upload/complete"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let part_url = format!("{base}/part");
            let app = Router::new().fallback(move |uri: Uri| {
                let part_url = part_url.clone();
                async move {
                    if uri.path() == failed_path { return (StatusCode::CONFLICT, "upload no longer pending").into_response(); }
                    if uri.path() == "/storage/files/meta/asset/hash" && !completed_elsewhere {
                        return (StatusCode::NOT_FOUND, [("x-weft-not-found", "file")], "missing file").into_response();
                    }
                    let value = match uri.path() {
                        "/storage/upload/begin" => serde_json::json!({"key":"asset/hash", "part_size":4, "resume":failed_path.ends_with("resume")}),
                        "/storage/upload/parts" => serde_json::json!({"parts":[{"part_number":1,"size_bytes":4,"offset_bytes":0,"url":part_url}]}),
                        "/part" => return (StatusCode::OK, [("etag", "part-1")], "").into_response(),
                        "/storage/upload/part-done" => serde_json::json!({}),
                        "/storage/files/meta/asset/hash" => serde_json::json!({"key":"asset/hash","mimeType":"text/plain","sizeBytes":4,"filename":"test","keep":false,"createdAtUnix":0,"version":1}),
                        _ => return (StatusCode::NOT_FOUND, "unexpected request").into_response(),
                    };
                    Json(value).into_response()
                }
            });
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
            let client = DispatcherClient::new(base, None);
            let store = DispatcherStore::new(&client);
            let result = store.transfer("hash", "text/plain", "test", 4, &mut &b"data"[..]).await;
            server.abort();
            if completed_elsewhere {
                assert_eq!(result.unwrap(), "asset/hash", "failure at {failed_path}");
            } else {
                assert!(result.is_err(), "failure at {failed_path} cannot succeed without stored content");
            }
        }
      }
    }

    #[test]
    fn asset_references_keep_text_sources_and_publish_the_empty_set() {
        let project: ProjectDefinition = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-0000-0000-000000000001", "nodes": [], "edges": []
        })).unwrap();
        let make_ref = |path: String| FileRef {
            path, marker: FileMarker::Asset, ty: WeftType::parse("String").unwrap(),
        };
        let own = format!("asset/{}", "a".repeat(64));
        let refs = [
            make_ref(own.clone()),
            make_ref("asset/logo.png".into()),
            make_ref("exec/c/generated".into()),
        ];
        assert_eq!(asset_references(&project, refs.iter()).unwrap().keys, vec![own]);
        let empty = asset_references(&project, std::iter::empty()).unwrap();
        assert_eq!(serde_json::to_value(empty).unwrap(), serde_json::json!({
            "project": project.id.to_string(), "keys": [], "kept": []
        }));
    }
}
