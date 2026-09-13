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

/// Collect, sync, and resolve the definition's `@asset` refs, in place.
/// Even an empty reference set is published, so removing the last uploaded
/// file starts its expiry countdown. A text-typed `@asset` from a URL or a stored
/// key is fetched here, at build, and cast to its declared type (the same
/// cast a project-file `@file` gets at parse).
pub async fn resolve_project_assets(
    client: &DispatcherClient,
    project_root: &std::path::Path,
    definition: &mut weft_core::project::ProjectDefinition,
    sources: Option<&weft_core::project::hash::Manifest>,
    publish: bool,
) -> Result<()> {
    let refs = weft_compiler::file_ref::collect_asset_refs(definition);
    let mut map = if refs.is_empty() {
        BTreeMap::new()
    } else {
        let source = DiskSource::new(project_root.to_path_buf());
        let mut store = DispatcherStore::new(client, definition.id.to_string());
        store.publish = publish;
        weft_assets::sync_assets(&refs, &source, &store).await.context("sync project assets")?
    };
    // Refs whose source is a RUNTIME STORAGE KEY (a stored file picked in the
    // editor): nothing to sync, resolve them against the tenant's file
    // listing (the `weft files` door). The match itself is the compiler's
    // shared step so every build driver resolves identically.
    let key_refs = weft_compiler::file_ref::collect_runtime_key_refs(definition);
    if !key_refs.is_empty() {
        let listing: weft_core::storage::ListFilesResponse = serde_json::from_value(
            client.get_json("/storage/files").await.context("list stored files")?,
        )
        .context("parse stored-file listing")?;
        weft_compiler::file_ref::resolve_runtime_key_refs(&key_refs, &listing.files, &mut map)
            .map_err(|errs| anyhow::anyhow!("stored files of the wrong kind:\n  {}", errs.join("\n  ")))?;
    }
    // Text values that live somewhere the parse could not read: fetched
    // once per build, cast, and substituted like every other deferred ref.
    let text_refs = weft_compiler::file_ref::collect_remote_text_refs(definition);
    if !text_refs.is_empty() {
        let project = Some(definition.id.to_string());
        let http = reqwest::Client::new();
        let mut failed: Vec<String> = Vec::new();
        for r in &text_refs {
            let fetched: Result<Vec<u8>> = if weft_compiler::file_ref::is_url_ref(r) {
                fetch_url_bytes(&http, &r.path).await
            } else {
                crate::commands::files::download_bytes(client, &r.path, &project).await
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
    weft_compiler::file_ref::apply_asset_resolutions(definition, &map)
        .map_err(|errs| anyhow::anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))?;
    if !publish { return Ok(()); }
    // Publish only after every reference resolved successfully. This includes
    // uploaded files selected by stored key, not just this build's disk refs.
    let mut references = asset_references(definition, key_refs.iter().chain(text_refs.iter()))?;
    if let Some(sources) = sources {
        let scope = weft_core::storage::key::KeyScope::Asset { project_id: definition.id.to_string() };
        for hash in sources.values().filter(|hash| !hash.is_empty()) {
            references.keys.push(weft_core::storage::key::scope_key(&scope, hash).map_err(anyhow::Error::msg)?);
        }
    }
    client.post_with_body("/storage/assets/references", &serde_json::to_value(references)?)
        .await.context("update project asset lifetimes")
}

fn asset_references<'a>(
    definition: &weft_core::project::ProjectDefinition,
    source_refs: impl Iterator<Item = &'a weft_core::project::FileRef>,
) -> Result<weft_core::storage::AssetReferencesRequest> {
    let mut references = weft_core::storage::AssetReferencesRequest {
        project: definition.id.to_string(),
        keys: weft_assets::referenced_asset_keys(definition)?,
    };
    // A text-typed stored asset is read at build time and becomes plain text
    // in the definition. Its SOURCE still needs the file for future builds.
    // Keep those source keys too; the dispatcher adds the authenticated tenant.
    let asset_prefix = format!("asset/{}/", definition.id);
    for reference in source_refs {
        if reference.path.starts_with(&asset_prefix)
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

/// The dispatcher-backed asset plane: control calls go to the dispatcher's
/// storage surface, bytes go straight to the bucket on the presigned part
/// URLs it returns (the same contract the editor's upload field drives).
pub(crate) struct DispatcherStore<'a> {
    publish: bool,
    client: &'a DispatcherClient,
    project: String,
    /// For the presigned part PUTs (bucket-direct; not dispatcher traffic).
    http: reqwest::Client,
}

impl<'a> DispatcherStore<'a> {
    pub(crate) fn new(client: &'a DispatcherClient, project: String) -> Self {
        Self { client, project, http: reqwest::Client::new(), publish: true }
    }
}

#[async_trait::async_trait]
impl AssetStore for DispatcherStore<'_> {

    async fn list(&self) -> Result<BTreeMap<String, String>> {
        let resp = self
            .client
            .post_json("/storage/assets/list", &serde_json::json!({ "project": self.project }))
            .await
            .context("list project assets")?;
        let listing: weft_core::storage::ListFilesResponse =
            serde_json::from_value(resp).context("parse project asset listing")?;
        let mut out = BTreeMap::new();
        for file in listing.files {
            let key = &file.key;
            // The asset key's id segment IS the content hash. Parse through
            // the one key grammar and fail loud on anything else: a
            // malformed entry silently registered as a "hash" would corrupt
            // the sync's diff and re-upload real content.
            let parsed = weft_core::storage::key::parse_key(key)
                .map_err(|e| anyhow::anyhow!("asset listing returned a malformed key: {e}"))?;
            if !weft_core::storage::is_content_hash(&parsed.id) {
                bail!("asset listing returned a non-content-hash id in key '{key}'");
            }
            out.insert(parsed.id, key.to_string());
        }
        Ok(out)
    }

    async fn upload(
        &self,
        hash: &str,
        mime: &str,
        filename: &str,
        size_bytes: u64,
        bytes: &mut (dyn Read + Send),
    ) -> Result<String> {
        anyhow::ensure!(self.publish, "asset '{filename}' is not stored for this project; run `weft bake` to publish the current files and capture trigger settings");
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
                &serde_json::json!({ "key": key, "part_number": part.part_number, "etag": etag }),
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
                    // SYNC: begin body <-> crates/weft-dispatcher/src/api/storage.rs EditorUploadBeginRequest
                    &serde_json::json!({
                        "project": self.project,
                        "mime_type": mime,
                        "filename": filename,
                        "declared_size": size_bytes,
                        "content_hash": hash,
                    }),
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
                    // it, so whichever finishes first is the answer for both.
                    let (rstatus, rbody) = self
                        .client
                        .post_json_status(
                            "/storage/upload/resume",
                            &serde_json::json!({ "key": begin.key }),
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
                        &serde_json::json!({
                            "key": key,
                            "parts": [{ "part_number": part_number, "size_bytes": want }],
                        }),
                    )
                    .await
                    .context("reserve asset part")?;
                let part = parts
                    .get("parts")
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.first())
                    .context("upload/parts returned no part")?;
                let part: weft_core::storage::PresignedPart =
                    serde_json::from_value(part.clone()).context("parse the reserved part")?;
                self.put_part(&key, &part, &buf[..want]).await?;
                sent += want as u64;
            }

            self.client
                .post_json("/storage/upload/complete", &serde_json::json!({ "key": key }))
                .await
                .context("complete asset upload")?;
            Ok(key)
        }.await;
        match transferred {
            Ok(uploaded) => Ok(uploaded),
            Err(error) => {
                // Any transfer step can lose to another publisher finishing
                // these same content-addressed bytes. Confirm the final state
                // once, without retrying the upload or interpreting error text.
                if let Some(key) = upload_key {
                    let stored = self.client.get_json_if_found(&format!("/storage/files/meta/{key}")).await
                        .with_context(|| format!("{error:#}; could not determine whether another publisher completed this asset"))?;
                    if let Some(stored) = stored {
                        let meta: weft_core::storage::StoredFileMeta = serde_json::from_value(stored)
                            .with_context(|| format!("{error:#}; the store's answer about {key} did not parse"))?;
                        // The key is the content hash the store was told at
                        // begin, and the store checks only the assembled
                        // size, so this confirms a finished upload under
                        // this key, not the bytes themselves.
                        anyhow::ensure!(
                            meta.key == key && meta.size_bytes == size_bytes,
                            "{error:#}; the store holds {} under {key} at {} bytes where {filename} is {size_bytes} bytes, \
                             so that upload is not this content",
                            meta.key, meta.size_bytes
                        );
                        return Ok(key);
                    }
                }
                Err(error)
            }
        }
    }
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
                    if uri.path() == "/storage/files/meta/asset/p/hash" && !completed_elsewhere {
                        return (StatusCode::NOT_FOUND, [("x-weft-not-found", "file")], "missing file").into_response();
                    }
                    let value = match uri.path() {
                        "/storage/upload/begin" => serde_json::json!({"key":"asset/p/hash", "part_size":4, "resume":failed_path.ends_with("resume")}),
                        "/storage/upload/parts" => serde_json::json!({"parts":[{"part_number":1,"size_bytes":4,"offset_bytes":0,"url":part_url}]}),
                        "/part" => return (StatusCode::OK, [("etag", "part-1")], "").into_response(),
                        "/storage/upload/part-done" => serde_json::json!({}),
                        "/storage/files/meta/asset/p/hash" => serde_json::json!({"key":"asset/p/hash","mimeType":"text/plain","sizeBytes":4,"filename":"test","keep":false,"createdAtUnix":0}),
                        _ => return (StatusCode::NOT_FOUND, "unexpected request").into_response(),
                    };
                    Json(value).into_response()
                }
            });
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
            let client = DispatcherClient::new(base);
            let store = DispatcherStore::new(&client, "p".into());
            let result = store.transfer("hash", "text/plain", "test", 4, &mut &b"data"[..]).await;
            server.abort();
            if completed_elsewhere {
                assert_eq!(result.unwrap(), "asset/p/hash", "failure at {failed_path}");
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
        let own = format!("asset/{}/{}", project.id, "a".repeat(64));
        let refs = [
            make_ref(own.clone()),
            make_ref(format!("asset/other-project/{}", "b".repeat(64))),
            make_ref("exec/c/generated".into()),
        ];
        assert_eq!(asset_references(&project, refs.iter()).unwrap().keys, vec![own]);
        let empty = asset_references(&project, std::iter::empty()).unwrap();
        assert_eq!(serde_json::to_value(empty).unwrap(), serde_json::json!({
            "project": project.id.to_string(), "keys": []
        }));
    }
}
