//! The CLI's asset-sync driver: right before a build, make the project's
//! asset storage contain what the code references and resolve the `@asset`
//! refs in the compiled definition (see `weft-assets` for the sync itself
//! and `docs` for the model). The project's files live on disk (paths
//! outside the project are legal: the ref names wherever the file already
//! is), and the store is the dispatcher's storage surface.

use std::collections::BTreeMap;
use std::io::Read;
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
) -> Result<()> {
    let refs = weft_compiler::file_ref::collect_asset_refs(definition);
    let mut map = if refs.is_empty() {
        BTreeMap::new()
    } else {
        let source = DiskSource { root: project_root.to_path_buf() };
        let store = DispatcherStore {
            client,
            project: definition.id.to_string(),
            http: reqwest::Client::new(),
        };
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
    // Publish only after every reference resolved successfully. This includes
    // uploaded files selected by stored key, not just this build's disk refs.
    let references = asset_references(definition, key_refs.iter().chain(text_refs.iter()))?;
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
struct DiskSource {
    root: PathBuf,
}

impl AssetSource for DiskSource {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        let p = std::path::Path::new(path);
        let full = if p.is_absolute() { p.to_path_buf() } else { self.root.join(p) };
        let file = std::fs::File::open(&full)
            .with_context(|| format!("asset not found at {}", full.display()))?;
        Ok(Box::new(file))
    }
}

/// The dispatcher-backed asset plane: control calls go to the dispatcher's
/// storage surface, bytes go straight to the bucket on the presigned part
/// URLs it returns (the same contract the editor's upload field drives).
struct DispatcherStore<'a> {
    client: &'a DispatcherClient,
    project: String,
    /// For the presigned part PUTs (bucket-direct; not dispatcher traffic).
    http: reqwest::Client,
}

#[async_trait::async_trait]
impl AssetStore for DispatcherStore<'_> {
    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_with_body(
                // SYNC: delete body <-> crates/weft-dispatcher/src/api/storage.rs RemoveRequest
                "/storage/files",
                &serde_json::json!({ "key": key }),
            )
            .await
            .with_context(|| format!("delete asset {key}"))?;
        Ok(())
    }

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
    ) -> Result<weft_assets::Uploaded> {
        let begin = self
            .client
            .post_json(
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
            .await;
        let begin = begin.context("begin asset upload")?;
        let key = begin
            .get("key")
            .and_then(|v| v.as_str())
            .context("upload/begin missing `key`")?
            .to_string();
        // An already-ACTIVE identical hash is this upload's idempotent
        // success: content-addressed, same bytes = same asset. It only
        // happens on a race (another build completed this content between
        // our list and begin); the begin answers the existing key with
        // nothing to transfer. A PENDING collision (another build mid-upload)
        // stays a loud error: rerun once it settles.
        if begin.get("already_stored").and_then(|v| v.as_bool()) == Some(true) {
            return Ok(weft_assets::Uploaded::AlreadyStored(key));
        }
        let part_size = begin
            .get("part_size")
            .and_then(|v| v.as_u64())
            .context("upload/begin missing `part_size`")? as usize;

        // Stream: read one part-sized chunk at a time, reserve + PUT + report.
        let mut sent: u64 = 0;
        let mut buf = vec![0u8; part_size];
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
            let parts = self
                .client
                .post_json(
                    "/storage/upload/parts",
                    &serde_json::json!({ "key": key, "sizes": [want] }),
                )
                .await
                .context("reserve asset part")?;
            let part = parts
                .get("parts")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .context("upload/parts returned no part")?;
            let url = part.get("url").and_then(|v| v.as_str()).context("part missing url")?;
            let part_number =
                part.get("part_number").and_then(|v| v.as_i64()).context("part missing number")?;
            let resp = self
                .http
                .put(url)
                .body(buf[..want].to_vec())
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
                    &serde_json::json!({ "key": key, "part_number": part_number, "etag": etag }),
                )
                .await
                .context("report asset part")?;
            sent += want as u64;
        }

        self.client
            .post_json("/storage/upload/complete", &serde_json::json!({ "key": key }))
            .await
            .context("complete asset upload")?;
        Ok(weft_assets::Uploaded::Stored(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weft_core::project::{FileMarker, FileRef, ProjectDefinition};
    use weft_core::weft_type::WeftType;

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
