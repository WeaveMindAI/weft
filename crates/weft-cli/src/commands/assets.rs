//! The tenant's assets as this machine reaches them: the dispatcher-backed
//! store every upload of the CLI goes through ([`DispatcherStore`], over
//! `weft_assets::upload`), and the read-only resolution of `@asset` and
//! `@file` markers for the commands that judge a program here without
//! running it (`weft run --check`, `weft status`). A build resolves its
//! own on the install. The project's files live on disk (paths outside the
//! project are legal: the ref names wherever the file already is).

use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use weft_assets::upload::{ContentStore, UploadPlane};
use weft_assets::{AssetSource, AssetStore};

use crate::client::DispatcherClient;

/// Resolve the `@file` and `@asset` markers in a run's handed values
/// (`--emit`, `--from`, `--group`, `--fire`, or a saved example) the way
/// the install resolves them when the run starts: a `@file` read from the
/// project at `root` and cast to its type, an `@asset` replaced by the
/// value its declared type stands for. Nothing is written to the install:
/// a file not stored yet stands for the value its upload would give it
/// (`weft run --check`, which runs nothing).
pub async fn resolve_run_values(client: &DispatcherClient, root: &std::path::Path, spec: &mut weft_core::run_spec::RunSpec) -> Result<()> {
    let fs = weft_compiler::CompileFs::disk(root);
    weft_compiler::file_ref::resolve_file_markers(spec, &fs)
        .map_err(|errs| anyhow::anyhow!("a run value cannot be read:\n  {}", errs.join("\n  ")))?;
    let map = resolve_asset_map(client, &DiskSource::new(root.to_path_buf()), &*spec).await?;
    weft_compiler::file_ref::apply_asset_resolutions(spec, &map)
        .map_err(|errs| anyhow::anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))?;
    Ok(())
}

/// The asset step both callers share: name the stored file each file-kind
/// `@asset` on disk is (or would be, uploaded), look up the stored-key
/// ones, fetch and cast the text ones. Hands back every resolved value by
/// resolution key. Nothing is uploaded.
///
/// `files` is where a path a ref names is read: project-relative, or
/// anywhere on the machine when absolute.
async fn resolve_asset_map(
    client: &DispatcherClient,
    files: &dyn weft_assets::AssetSource,
    target: &impl weft_compiler::file_ref::MarkedValues,
) -> Result<weft_core::project::AssetResolutions> {
    let refs = weft_compiler::file_ref::collect_asset_refs(target);
    let mut map = if refs.is_empty() {
        BTreeMap::new()
    } else {
        let store = DispatcherStore { missing: Missing::Assume, ..DispatcherStore::new(client) };
        weft_assets::sync_assets(&refs, files, &store).await.context("sync project assets")?
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
        let http = weft_core::net::http_client().build()?;
        let mut failed: Vec<String> = Vec::new();
        for r in &text_refs {
            let fetched: Result<Vec<u8>> = if weft_compiler::file_ref::is_url_ref(r) {
                fetch_url_bytes(&http, &r.path).await
            } else if weft_compiler::file_ref::is_runtime_key_ref(r) {
                crate::commands::files::download_bytes(client, &r.path).await
            } else {
                files.open(&r.path).and_then(|mut file| {
                    let mut bytes = Vec::new();
                    file.read_to_end(&mut bytes).with_context(|| format!("read {}", r.path))?;
                    Ok(bytes)
                })
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
    Ok(map)
}

/// Resolve the definition's `@asset` refs in place, for a caller that
/// hashes the resolved program itself (`weft status`'s drift), the way
/// [`resolve_run_values`] does: nothing is written to the install.
pub async fn resolve_project_assets(
    client: &DispatcherClient,
    project_root: &std::path::Path,
    definition: &mut weft_core::project::ProjectDefinition,
) -> Result<()> {
    let map = resolve_asset_map(client, &DiskSource::new(project_root.to_path_buf()), &*definition).await?;
    weft_compiler::file_ref::apply_asset_resolutions(definition, &map)
        .map_err(|errs| anyhow::anyhow!("unresolved assets:\n  {}", errs.join("\n  ")))
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

impl AssetSource for DiskSource {
    fn open(&self, path: &str) -> Result<Box<dyn Read + Send>> {
        let p = std::path::Path::new(path);
        let full = if p.is_absolute() { p.to_path_buf() } else { self.root.join(p) };
        let file = std::fs::File::open(&full).with_context(|| format!("asset not found at {}", full.display()))?;
        Ok(Box::new(file))
    }
}

/// The caller's tenant's assets as the dispatcher's storage surface
/// serves them: control calls go to its `/storage/*` routes, bytes go
/// straight to the bucket on the presigned part URLs it returns (the same
/// contract the editor's upload field drives).
pub(crate) struct DispatcherPlane<'a>(pub(crate) &'a DispatcherClient);

#[async_trait::async_trait]
impl UploadPlane for DispatcherPlane<'_> {
    async fn held(&self, hashes: &[String]) -> Result<weft_core::storage::AssetsHeldResponse> {
        let request = weft_core::storage::AssetsHeldRequest { hashes: hashes.to_vec() };
        let resp = self.0.post_json("/storage/assets/held", &serde_json::to_value(request)?).await?;
        serde_json::from_value(resp).context("parse the stored-contents answer")
    }

    async fn begin(&self, request: weft_assets::upload::Begin) -> Result<weft_core::storage::UploadBeginResponse> {
        let resp = match request {
            weft_assets::upload::Begin::Asset(request) => self.0.post_json("/storage/upload/begin", &serde_json::to_value(request)?).await?,
            weft_assets::upload::Begin::Copy(request) => self.0.post_json("/storage/import/begin", &serde_json::to_value(request)?).await?,
        };
        serde_json::from_value(resp).context("parse the upload's begin")
    }

    async fn resume(&self, key: &str) -> Result<weft_core::storage::UploadResumeResponse> {
        let request = weft_core::storage::UploadResumeRequest { key: key.to_string(), sender: weft_core::storage::PartSender::Caller };
        let resp = self.0.post_json("/storage/upload/resume", &serde_json::to_value(request)?).await?;
        serde_json::from_value(resp).context("parse upload/resume")
    }

    async fn parts(&self, key: &str, parts: Vec<weft_core::storage::PartAsk>) -> Result<Vec<weft_core::storage::PresignedPart>> {
        let request = weft_core::storage::UploadPartsRequest { key: key.to_string(), parts, sender: weft_core::storage::PartSender::Caller };
        let resp = self.0.post_json("/storage/upload/parts", &serde_json::to_value(request)?).await?;
        let parts: weft_core::storage::UploadPartsResponse = serde_json::from_value(resp).context("parse the reserved part")?;
        Ok(parts.parts)
    }

    async fn part_done(&self, request: weft_core::storage::PartDoneRequest) -> Result<()> {
        self.0.post_with_body("/storage/upload/part-done", &serde_json::to_value(request)?).await
    }

    async fn complete(&self, key: &str) -> Result<()> {
        let request = weft_core::storage::UploadCompleteRequest { key: key.to_string() };
        // The store puts the file together from its parts, however large.
        self.0.waiting_on_its_work().post_json("/storage/upload/complete", &serde_json::to_value(request)?).await?;
        Ok(())
    }

    async fn abort(&self, key: &str) -> Result<()> {
        let request = weft_core::storage::UploadAbortRequest { key: key.to_string() };
        self.0.post_with_body("/storage/upload/abort", &serde_json::to_value(request)?).await
    }

    async fn meta_if_found(&self, key: &str) -> Result<Option<weft_core::storage::StoredFileMeta>> {
        match self.0.get_json_if_found(&format!("/storage/files/meta/{key}")).await? {
            Some(stored) => Ok(Some(serde_json::from_value(stored).with_context(|| format!("parse the store's answer about {key}"))?)),
            None => Ok(None),
        }
    }

    fn completing(&self, error: &anyhow::Error) -> bool {
        crate::client::refusal(error).is_some_and(|refused| refused.marked() == Some(crate::client::Marked::StoreCompleting))
    }

    fn interrupted(&self, error: &anyhow::Error) -> bool {
        crate::client::connection_failed(error)
    }
}

/// The dispatcher-backed assets of the caller's tenant, with what a
/// command does about content the store does not hold ([`Missing`]).
pub(crate) struct DispatcherStore<'a> {
    missing: Missing,
    /// What the store answered an upload of each content it does not hold
    /// is stored under, for [`Missing::Assume`].
    unheld: std::sync::Mutex<BTreeMap<String, String>>,
    content: ContentStore<DispatcherPlane<'a>>,
}

/// What a [`DispatcherStore`] does with content the store does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Missing {
    /// Upload it.
    Upload,
    /// Answer the key its upload would store it under (the store says it,
    /// `AssetsHeldResponse::unheld`), uploading nothing: a check that runs
    /// nothing reads the value a run would.
    Assume,
}

impl<'a> DispatcherStore<'a> {
    pub(crate) fn new(client: &'a DispatcherClient) -> Self {
        Self::over(ContentStore::new(DispatcherPlane(client)), Missing::Upload)
    }

    /// [`Self::new`], saying on stderr how far its uploads have got.
    pub(crate) fn telling(client: &'a DispatcherClient) -> Self {
        Self::over(ContentStore::telling(DispatcherPlane(client)), Missing::Upload)
    }

    fn over(content: ContentStore<DispatcherPlane<'a>>, missing: Missing) -> Self {
        Self { missing, unheld: Default::default(), content }
    }
}

#[async_trait::async_trait]
impl AssetStore for DispatcherStore<'_> {
    async fn held(&self, hashes: &[String]) -> Result<BTreeMap<String, String>> {
        let held = self.content.held_answer(hashes).await?;
        self.unheld.lock().expect("unheld keys").extend(held.unheld);
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
        match self.missing {
            Missing::Upload => self.content.upload(hash, mime, filename, size_bytes, bytes).await,
            Missing::Assume => self.unheld.lock().expect("unheld keys").get(hash).cloned().ok_or_else(|| {
                anyhow::anyhow!("the store named no key for '{filename}' ({hash}), which it neither holds nor would store")
            }),
        }
    }

    fn interrupted(&self, error: &anyhow::Error) -> bool {
        self.content.interrupted(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{http::{StatusCode, Uri}, response::IntoResponse, Json, Router};

    /// The dispatcher's routes, as the CLI plane reads them: a
    /// "completing" refusal (its marker header) is the other publish
    /// finishing, so this one waits and gets the file; the meta route's
    /// marked 404 is "not there yet".
    #[tokio::test]
    async fn a_completing_refusal_from_the_dispatcher_waits_for_the_other_publish_to_land() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let part_url = format!("{base}/part");
        let hash = weft_assets::hash_reader(&b"data"[..]).unwrap().0;
        let key = format!("t/asset/{hash}");
        let meta_path = format!("/storage/files/meta/{key}");
        let looks = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let app = Router::new().fallback({
            let key = key.clone();
            move |uri: Uri| {
                let (part_url, looks, key, meta_path) = (part_url.clone(), looks.clone(), key.clone(), meta_path.clone());
                async move {
                    let value = match uri.path() {
                        "/storage/upload/begin" => serde_json::json!({"key": key, "partSize": 4, "resume": false}),
                        "/storage/upload/parts" => serde_json::json!({"parts":[{"partNumber":1,"sizeBytes":4,"offsetBytes":0,"url":part_url}]}),
                        "/part" => return (StatusCode::OK, [("etag", "part-1")], "").into_response(),
                        "/storage/upload/part-done" => return StatusCode::NO_CONTENT.into_response(),
                        "/storage/upload/complete" => {
                            return (StatusCode::CONFLICT, [(weft_core::storage::COMPLETING_HEADER, "retry")], "completing").into_response();
                        }
                        path if path == meta_path => {
                            if looks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                                return (StatusCode::NOT_FOUND, [("x-weft-not-found", "file")], "missing file").into_response();
                            }
                            serde_json::json!({"key": key,"mimeType":"text/plain","sizeBytes":4,"filename":"test","keep":false,"createdAtUnix":0,"version":1})
                        }
                        _ => return (StatusCode::NOT_FOUND, "unexpected request").into_response(),
                    };
                    Json(value).into_response()
                }
            }
        });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        let client = DispatcherClient::new(base, None);
        let store = DispatcherStore::new(&client);
        let result = store.upload(&hash, "text/plain", "test", 4, &mut &b"data"[..]).await;
        server.abort();
        assert_eq!(result.unwrap(), key);
    }
}
