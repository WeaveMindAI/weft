//! `weft files ...`: browse and manage the tenant's stored files.
//!
//! A download asks the install for a link: it checks the caller, then
//! answers a short-lived presigned link straight to the object store, and
//! the bytes stream from the store, never through the install. The store
//! answers `Range` requests, so a dropped transfer resumes where it
//! stopped.

use std::collections::BTreeMap;

use anyhow::Context;

use super::Ctx;

/// Every file in the caller's tenant (the dispatcher takes the tenant
/// from the credential, so this works outside a project folder too).
pub(crate) async fn list_files(client: &crate::client::DispatcherClient) -> anyhow::Result<Vec<weft_core::storage::StoredFileMeta>> {
    let listing: weft_core::storage::ListFilesResponse =
        serde_json::from_value(client.get_json("/storage/files").await?).context("parse files listing")?;
    Ok(listing.files)
}

/// The SCOPE portion of a wire key: the bucket is shared and keys
/// are tenant-anchored (`<tenant>/<scope>/<owner>/<id>`), but the user
/// thinks + types in the scope portion (`<scope>/<owner>/<id>`). Strip
/// the leading tenant segment for display, matching, and prefix filters.
/// The dispatcher re-adds the caller's tenant on the way back in, so the
/// user never sees or types it.
pub(crate) fn scope_key(wire_key: &str) -> &str {
    wire_key.split_once('/').map(|(_tenant, rest)| rest).unwrap_or(wire_key)
}

pub(crate) fn fmt_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// Short human duration for a remaining lifetime: "45s", "4m", "3h", "12d".
fn fmt_remaining(secs: i64) -> String {
    match secs {
        s if s < 60 => format!("{}s", s.max(1)),
        s if s < 3600 => format!("{}m", (s + 59) / 60),
        s if s < 86400 => format!("{}h", (s + 3599) / 3600),
        s => format!("{}d", (s + 86399) / 86400),
    }
}

/// The space a file sits in (`<scope>/<owner>`, tenant stripped, the
/// prefix `ls` groups by and `rm` takes with a trailing `/`), and how `ls`
/// says what it is. Read off the key's own grammar
/// ([`weft_core::storage::key::parse_key`]), so every scope is named by what it
/// is and a key of no scope weft knows is said to be one.
fn space_of(wire_key: &str) -> (String, String) {
    use weft_core::storage::key::KeyScope;
    let scoped = scope_key(wire_key);
    let Ok(parsed) = weft_core::storage::key::parse_key(wire_key) else {
        let space = scoped.rsplit_once('/').map(|(s, _)| s).unwrap_or(scoped).to_string();
        return (space, "not a key weft makes".to_string());
    };
    let space = scoped.strip_suffix(&format!("/{}", parsed.id)).unwrap_or(scoped).to_string();
    let label = match parsed.scope {
        KeyScope::Run { .. } => "one run's files, removed soon after it ends unless kept".to_string(),
        KeyScope::Project { .. } => "the project's files".to_string(),
        KeyScope::Instance { instance, .. } => format!("instance {instance}'s files in the project"),
        KeyScope::Shared { name } => format!("the shared space '{name}'"),
        KeyScope::Asset => "assets: the files your projects' @asset references hold, one per content".to_string(),
    };
    (space, label)
}

/// `weft files ls [PREFIX]`: list, grouped by space (run, project,
/// instance, shared and asset).
pub async fn ls(ctx: Ctx, prefix: Option<String>) -> anyhow::Result<()> {
    let files: Vec<_> = list_files(&ctx.client()?)
        .await?
        .into_iter()
        .filter(|f| prefix.as_deref().map(|p| scope_key(&f.key).starts_with(p)).unwrap_or(true))
        .collect();
    if ctx.json_out(&files)? {
        return Ok(());
    }
    if files.is_empty() {
        println!("(no stored files)");
        return Ok(());
    }
    let mut groups: BTreeMap<(String, String), Vec<&weft_core::storage::StoredFileMeta>> = BTreeMap::new();
    for f in &files {
        groups.entry(space_of(&f.key)).or_default().push(f);
    }
    for ((space, label), entries) in groups {
        println!("{space}/  ({label})");
        for f in entries {
            let keep = if f.keep { " keep" } else { "" };
            // Remaining lifetime, not a raw timestamp: a kept file's TTL, or an
            // ended run's un-kept output in its post-run linger window.
            let expiry = match f.expires_at_unix {
                Some(t) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(t);
                    format!(" deleted in {}", fmt_remaining(t - now))
                }
                None => String::new(),
            };
            println!(
                "  {:<40} {:>10}  {}{keep}{expiry}",
                f.key.rsplit('/').next().unwrap_or(&f.key),
                fmt_size(f.size_bytes),
                f.filename,
            );
        }
    }
    Ok(())
}

/// `weft files inspect <KEY>`.
pub async fn inspect(ctx: Ctx, key: String) -> anyhow::Result<()> {
    let files = list_files(&ctx.client()?).await?;
    // The user types the scope key; match it against each wire key's
    // scope portion (tenant stripped).
    let Some(meta) = files.into_iter().find(|f| scope_key(&f.key) == key) else {
        anyhow::bail!("no stored file with key '{key}' (see `{}`)", ctx.weft("files ls"));
    };
    println!("{}", serde_json::to_string_pretty(&meta)?);
    Ok(())
}

/// Max number of CONSECUTIVE zero-progress resumes before giving up
/// (a resume that advances the byte count resets the counter). A
/// download of any size and any duration survives blips: each resume
/// asks for a fresh link (the last one may have expired meanwhile) and
/// continues from the current offset with a `Range` request, so only a
/// transfer that is genuinely stuck (the same byte offset failing over
/// and over) is abandoned.
const DOWNLOAD_RESUME_ATTEMPTS: u32 = 20;

/// The whole content of one stored file, in memory: the build's read of a
/// text-typed `@asset` that names a stored key. Small files by contract
/// (a prompt, a JSON shape), so no streaming, no resume; the size the
/// install answered with the link is checked so a cut transfer is never
/// cast as a value.
pub(crate) async fn download_bytes(client: &crate::client::DispatcherClient, key: &str) -> anyhow::Result<Vec<u8>> {
    let link = mint_download_url(client, key).await?;
    let resp = weft_core::net::http_client()
        .build()?
        .get(&link.url)
        .send()
        .await
        .with_context(|| format!("GET stored file {key}"))?;
    if !resp.status().is_success() {
        let status = resp.status();
        anyhow::bail!("download of '{key}' failed ({status})");
    }
    let bytes = resp.bytes().await.with_context(|| format!("read stored file {key}"))?;
    if bytes.len() as u64 != link.size_bytes {
        anyhow::bail!(
            "download of '{key}' was cut short ({} of {} bytes)",
            bytes.len(),
            link.size_bytes
        );
    }
    Ok(bytes.to_vec())
}

/// Stream the stored file `key` into `dest` (created, or emptied when it
/// is there), resuming where a dropped transfer stopped, and answer how
/// many bytes it holds: a file of any size never passes through memory
/// whole. `dest` holds part of the file when this fails; the caller owns
/// it and throws it away.
pub(crate) async fn download_to(client: &crate::client::DispatcherClient, key: &str, dest: &std::path::Path) -> anyhow::Result<u64> {
    let link = mint_download_url(client, key).await?;
    download_to_tmp(&weft_core::net::http_client().build()?, client, key, &link, dest).await
}

/// Ask the install for a fresh link to one file: it checks the caller,
/// then answers a short-lived presigned link straight to the object store,
/// with the file's name and size. The store answers that link with the
/// bytes alone, so the name (the default output path) and the size (the
/// completeness check) come from this answer.
async fn mint_download_url(
    client: &crate::client::DispatcherClient,
    key: &str,
) -> anyhow::Result<weft_core::storage::PresignResult> {
    let body = weft_core::storage::DownloadRequest { key: key.to_string(), ttl_secs: None };
    serde_json::from_value(client.post_json("/storage/files/download", &serde_json::to_value(&body)?).await?)
        .context("read the install's answer to the download request")
}

pub async fn download(ctx: Ctx, key: String, output: Option<String>) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let http = weft_core::net::http_client().build()?;

    // The first link, with the file's name (the default output path) and
    // its size (the completeness check), which the install knows and the
    // store's answer does not carry.
    let link = mint_download_url(&client, &key).await?;
    let out_path = output.unwrap_or_else(|| {
        Some(link.filename.clone())
            .filter(|f| !f.is_empty())
            .unwrap_or_else(|| key.rsplit('/').next().unwrap_or("download.bin").to_string())
    });
    // Download to a sibling temp file, then atomically rename onto `out_path` only
    // after the transfer completes and passes the size check. This means (a) a
    // failed/partial download never leaves a plausible-looking half-file at
    // `out_path`, and (b) an existing `out_path` is untouched until success (the
    // old code truncated it up front with `File::create`, so a mid-transfer failure
    // destroyed the user's previous copy). The temp lives in the same directory so
    // the rename is a same-filesystem atomic move. On any error we remove it.
    let tmp_path = format!("{out_path}.{}.weft-partial", uuid::Uuid::new_v4().simple());
    let result = download_to_tmp(&http, &client, &key, &link, std::path::Path::new(&tmp_path)).await;
    match result {
        Ok(written) => {
            tokio::fs::rename(&tmp_path, &out_path)
                .await
                .with_context(|| format!("finalize download: rename {tmp_path} -> {out_path}"))?;
            println!("downloaded {} ({}) -> {out_path}", key, fmt_size(written));
            Ok(())
        }
        Err(e) => {
            // Best-effort cleanup of the partial temp file; the transfer error is
            // the one the user sees. A leftover temp (if even the remove fails) is
            // named `.weft-partial` so it is obviously disposable.
            if let Err(rm) = tokio::fs::remove_file(&tmp_path).await {
                if rm.kind() != std::io::ErrorKind::NotFound {
                    eprintln!("warning: could not remove partial download {tmp_path}: {rm}");
                }
            }
            Err(e)
        }
    }
}

/// Stream the download into `tmp_path`, resuming on network drops, and return the
/// total bytes written on success. `first` is the install's first answer: every
/// resume holds the file to the version and size it named. All failure surfaces
/// (network stall, bad resume, the file changing meanwhile, local write error,
/// truncated result) return `Err` and leave the temp file for the caller to clean
/// up.
async fn download_to_tmp(
    http: &reqwest::Client,
    client: &crate::client::DispatcherClient,
    key: &str,
    first: &weft_core::storage::PresignResult,
    tmp_path: &std::path::Path,
) -> anyhow::Result<u64> {
    let total = first.size_bytes;
    // The initial full-body GET (byte 0 onward). Not a HEAD: it streams the file.
    let response = http.get(&first.url).send().await.with_context(|| format!("GET {}", first.url))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("download failed ({status}): {body}");
    }
    let mut file = tokio::fs::File::create(tmp_path)
        .await
        .with_context(|| format!("create {}", tmp_path.display()))?;

    // Stream the first response, then resume on any drop: ask for a fresh
    // link (the last one may have expired meanwhile) and continue from the
    // byte offset we already have with a Range request. A resume that could
    // not be sent (the link request or the ranged GET failing on its way)
    // is one more resume that made no progress.
    let mut written = 0u64;
    let mut next: anyhow::Result<reqwest::Response> = Ok(response);
    // Counts CONSECUTIVE resumes that made zero progress. Any resume
    // that advances `written` resets it, so a large file over a flaky
    // link survives unboundedly many drops as long as it keeps moving;
    // only a truly stuck transfer (the same offset failing over and
    // over) hits the cap. This matches the "any size, any duration
    // survives blips" promise.
    let mut stalls = 0u32;
    loop {
        let before = written;
        let cause = match next {
            Ok(mut response) => match drain_into(&mut response, &mut file, &mut written).await {
                Ok(()) => break, // stream ended cleanly
                // A local write error is fatal: a fresh link cannot fix a full or
                // read-only disk, and looping would spin against an unfixable condition.
                // Surface it immediately with a disk-oriented message.
                Err(DrainError::Write(e)) => {
                    return Err(e).with_context(|| {
                        format!(
                            "download of '{key}' could not write to disk at {} of {} \
                             (out of space or the path is not writable)",
                            fmt_size(written),
                            fmt_size(total),
                        )
                    });
                }
                // A network drop is resumable: fall through to the resume below.
                Err(DrainError::Body(e)) => anyhow::Error::new(e),
            },
            Err(e) => e,
        };

        // Done already? (A clean EOF can surface as the body
        // ending exactly at `total`; treat that as success.)
        if total == written {
            break;
        }
        if written > before {
            stalls = 0; // forward progress; reset the stall counter
        } else {
            stalls += 1;
        }
        if stalls > DOWNLOAD_RESUME_ATTEMPTS {
            return Err(cause).with_context(|| {
                format!(
                    "download of '{key}' stalled; gave up after \
                     {DOWNLOAD_RESUME_ATTEMPTS} consecutive resumes made no progress, \
                     stuck at {} of {}",
                    fmt_size(written),
                    fmt_size(total),
                )
            });
        }
        eprintln!("download interrupted at {} ({cause:#}); resuming...", fmt_size(written));
        next = match resume_at(http, client, key, first, written).await {
            // Refused (a status, the file changed, the range ignored): no
            // resume can fix it.
            Err(e) if !crate::client::connection_failed(&e) => return Err(e),
            sent => sent,
        };
    }

    tokio::io::AsyncWriteExt::flush(&mut file).await?;
    if written != total {
        anyhow::bail!(
            "download of '{key}' ended at {} but the file is {}; the result is truncated",
            fmt_size(written),
            fmt_size(total),
        );
    }
    Ok(written)
}

/// The rest of the file from byte `offset`, on a fresh link held to the
/// content `first` named. The install answers each link with the file's
/// version and size as they are now; a file written again meanwhile would
/// splice two contents into one, so a different version or size is
/// refused. The answer MUST be 206 Partial Content starting exactly at
/// `offset`: a 200 (range ignored, full body from byte 0) would append a
/// second copy onto what is already written.
async fn resume_at(
    http: &reqwest::Client,
    client: &crate::client::DispatcherClient,
    key: &str,
    first: &weft_core::storage::PresignResult,
    offset: u64,
) -> anyhow::Result<reqwest::Response> {
    let link = mint_download_url(client, key).await?;
    if (link.version, link.size_bytes) != (first.version, first.size_bytes) {
        anyhow::bail!(
            "the file '{key}' changed while it downloaded (version {} of {}, now version {} of {}); \
             run the download again",
            first.version,
            fmt_size(first.size_bytes),
            link.version,
            fmt_size(link.size_bytes),
        );
    }
    let resumed = http
        .get(&link.url)
        .header("range", format!("bytes={offset}-"))
        .send()
        .await
        .with_context(|| format!("resume GET {}", link.url))?;
    let status = resumed.status();
    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        let body = resumed.text().await.unwrap_or_default();
        anyhow::bail!(
            "resume of '{key}' did not return partial content (got {status}); \
             the server ignored the Range request, refusing to corrupt the file: {body}"
        );
    }
    let range_start = resumed
        .headers()
        .get("content-range")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_content_range_start);
    if range_start != Some(offset) {
        anyhow::bail!(
            "resume of '{key}' returned the wrong byte offset \
             (asked for {offset}, server reported {range_start:?}); \
             refusing to corrupt the file"
        );
    }
    Ok(resumed)
}

/// Why `drain_into` stopped short of a clean body end. The two kinds are handled
/// very differently: a `Body` error (the network dropped mid-stream) is RESUMABLE
/// (a fresh link + Range from where we are); a `Write` error (the local disk is
/// full / read-only) is FATAL (no amount of resuming fixes it, and resuming
/// would loop forever against an unfixable local condition).
enum DrainError {
    Body(reqwest::Error),
    Write(std::io::Error),
}

/// Pipe a response body into `file`, advancing `written` by bytes ACTUALLY WRITTEN
/// (never by bytes merely received), so a partial write can't desync the counter
/// from the file and send a resume to the wrong offset. Returns `Ok` when the body
/// ends cleanly, a `Body` error on a network drop (resumable), or a `Write` error
/// on a local write failure (fatal).
async fn drain_into(
    response: &mut reqwest::Response,
    file: &mut tokio::fs::File,
    written: &mut u64,
) -> Result<(), DrainError> {
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                tokio::io::AsyncWriteExt::write_all(file, &chunk)
                    .await
                    .map_err(DrainError::Write)?;
                // Only count bytes that reached the file: write_all either writes
                // the whole chunk or returns Err (which we propagated above), so on
                // reaching here every byte of `chunk` is on disk.
                *written += chunk.len() as u64;
            }
            Ok(None) => return Ok(()),
            Err(e) => return Err(DrainError::Body(e)),
        }
    }
}

/// Start offset of a `Content-Range: bytes <start>-<last>/<total>`
/// header. None if the header is absent or not in that exact shape;
/// the caller then refuses the resume rather than guessing.
fn parse_content_range_start(header: &str) -> Option<u64> {
    header
        .strip_prefix("bytes ")?
        .split('-')
        .next()?
        .trim()
        .parse()
        .ok()
}

/// `weft files rm <KEY-OR-SPACE>`: a full key removes one file; a
/// trailing `/` removes the whole space (prefix).
pub async fn rm(ctx: Ctx, target: String, yes: bool) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let is_prefix = target.ends_with('/');

    // Confirm before deleting: a prefix wipe removes a whole space,
    // INCLUDING kept files the user deliberately persisted, so this is
    // the most destructive verb in the surface. Show the blast radius and
    // require an explicit yes. `--yes` skips the prompt (and is REQUIRED
    // when stdin is not a terminal, so a piped/scripted run can never
    // silently wipe). Same convention as the interactive prompts in
    // `weft deactivate` / `weft ensure`.
    if !yes {
        if is_prefix {
            // List what falls under the prefix so the user sees exactly
            // what is about to go (count + kept-file count).
            let files = list_files(&client).await?;
            // `target` is a scope prefix/key; match each wire key's scope
            // portion (tenant stripped) so the preview shows the real
            // blast radius the dispatcher will wipe.
            let victims: Vec<_> =
                files.iter().filter(|f| scope_key(&f.key).starts_with(&target)).collect();
            if victims.is_empty() {
                println!("nothing under '{target}' to remove");
                return Ok(());
            }
            let kept = victims.iter().filter(|f| f.keep).count();
            println!(
                "About to remove the whole space '{target}': {} file(s){}.",
                victims.len(),
                if kept > 0 { format!(", {kept} of them KEPT (persisted on purpose)") } else { String::new() }
            );
        } else {
            println!("About to remove '{target}'.");
        }
        if !crate::prompt::confirm("Type 'yes' to confirm: ", "--yes")? {
            println!("aborted");
            return Ok(());
        }
    }

    let body = if is_prefix {
        weft_core::storage::RemoveFilesRequest { key: None, prefix: Some(target) }
    } else {
        weft_core::storage::RemoveFilesRequest { key: Some(target), prefix: None }
    };
    let removed: weft_core::storage::FilesRemoved =
        // A prefix takes as long as removing every file under it.
        serde_json::from_value(client.waiting_on_its_work().delete_with_body("/storage/files", &serde_json::to_value(&body)?).await?)
            .context("read the removal answer")?;
    println!("removed {} file(s)", removed.removed);
    Ok(())
}

/// `weft files usage`.
pub async fn usage(ctx: Ctx) -> anyhow::Result<()> {
    let client = ctx.client()?;
    let resp = client.get_json("/storage/usage").await?;
    if ctx.json_out(&resp)? {
        return Ok(());
    }
    let usage: weft_core::storage::TenantUsage = serde_json::from_value(resp).context("read the storage usage")?;
    if usage.file_count == 0 {
        println!("nothing stored");
    } else {
        println!("stored: {} across {} file(s)", fmt_size(usage.stored_bytes), usage.file_count);
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::{download_to_tmp, mint_download_url, parse_content_range_start, scope_key, space_of};
    use crate::client::DispatcherClient;
    use axum::{body::Body, http::{HeaderMap, StatusCode}, response::IntoResponse, routing::{get, post}, Json, Router};
    use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

    const CONTENT: &[u8] = b"the whole content of one stored file, cut midway by the network";
    const CUT_AT: usize = 20;

    /// How the fake store answers the resume, and what the fake install
    /// says about the file on the second link.
    #[derive(Clone, Copy)]
    enum Resume {
        /// 206 from the asked offset, the file unchanged.
        Partial,
        /// 200 with the whole body again: the range ignored.
        Whole,
        /// The file was written again since the first link.
        NewVersion,
    }

    /// A fake install answering download links and a fake store whose
    /// first answer is cut after [`CUT_AT`] bytes, then the download into a
    /// temp file. Answers what the download returned and the file's bytes.
    async fn download_cut_then(resume: Resume) -> (anyhow::Result<u64>, Vec<u8>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let blob = format!("{base}/blob");
        let links = Arc::new(AtomicU64::new(0));
        let app = Router::new()
            .route(
                "/storage/files/download",
                post(move || {
                    let n = links.fetch_add(1, Ordering::SeqCst);
                    let version = if n > 0 && matches!(resume, Resume::NewVersion) { 2 } else { 1 };
                    let url = blob.clone();
                    async move {
                        Json(serde_json::json!({
                            "url": url, "filename": "f.txt", "sizeBytes": CONTENT.len(), "version": version, "public": false,
                        }))
                    }
                }),
            )
            .route(
                "/blob",
                get(move |headers: HeaderMap| async move {
                    let Some(range) = headers.get("range") else {
                        // The first answer: the head of the file, then the
                        // connection dies.
                        let head = futures::stream::once(async { Ok::<_, std::io::Error>(axum::body::Bytes::from_static(&CONTENT[..CUT_AT])) });
                        let cut = futures::stream::once(async {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            Err(std::io::Error::other("the network dropped"))
                        });
                        let body = Body::from_stream(futures::StreamExt::chain(head, cut));
                        return (StatusCode::OK, [("content-length", CONTENT.len().to_string())], body).into_response();
                    };
                    let start: usize = range.to_str().unwrap().strip_prefix("bytes=").unwrap().trim_end_matches('-').parse().unwrap();
                    match resume {
                        Resume::Whole => (StatusCode::OK, CONTENT).into_response(),
                        Resume::Partial | Resume::NewVersion => {
                            let content_range = format!("bytes {start}-{}/{}", CONTENT.len() - 1, CONTENT.len());
                            (StatusCode::PARTIAL_CONTENT, [("content-range", content_range)], &CONTENT[start..]).into_response()
                        }
                    }
                }),
            );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = DispatcherClient::new(base, None);
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("f.txt.partial");
        let first = mint_download_url(&client, "project/p1/f0").await.unwrap();
        let result = download_to_tmp(&weft_core::net::http_client().build().unwrap(), &client, "project/p1/f0", &first, &tmp).await;
        server.abort();
        (result, std::fs::read(&tmp).unwrap_or_default())
    }

    /// A body cut midway carries on from where it stopped, on a fresh
    /// link, and the bytes come out whole.
    #[tokio::test]
    async fn a_cut_download_resumes_where_it_stopped() {
        let (result, bytes) = download_cut_then(Resume::Partial).await;
        assert_eq!(result.unwrap(), CONTENT.len() as u64);
        assert_eq!(bytes, CONTENT);
    }

    /// A resume answered with the whole body again would append a second
    /// copy: refused.
    #[tokio::test]
    async fn a_resume_that_ignores_the_range_is_refused() {
        let e = download_cut_then(Resume::Whole).await.0.unwrap_err().to_string();
        assert!(e.contains("did not return partial content"), "{e}");
    }

    /// A file written again while it downloaded would splice two contents
    /// into one: refused, asking for the download to be run again.
    #[tokio::test]
    async fn a_file_that_changed_meanwhile_is_refused() {
        let e = download_cut_then(Resume::NewVersion).await.0.unwrap_err().to_string();
        assert!(e.contains("changed while it downloaded") && e.contains("run the download again"), "{e}");
    }

    /// Each scope is grouped under its own space and named by what it
    /// is: the instance scope under its project AND instance, the asset
    /// scope as one space, a shared space by its name, and a key of no
    /// scope weft knows as that.
    #[test]
    fn a_file_is_grouped_and_named_by_its_scope() {
        let hash = "a".repeat(64);
        let space = |key: &str| space_of(key).0;
        let label = |key: &str| space_of(key).1;
        assert_eq!(space("t/run/r1/f0"), "run/r1");
        assert!(label("t/run/r1/f0").starts_with("one run's files"));
        assert_eq!((space("t/project/p1/f0"), label("t/project/p1/f0")), ("project/p1".into(), "the project's files".into()));
        assert_eq!(space("t/instance/p1/alice/f0"), "instance/p1/alice");
        assert_eq!(label("t/instance/p1/alice/f0"), "instance alice's files in the project");
        assert_eq!((space("t/shared/team/f0"), label("t/shared/team/f0")), ("shared/team".into(), "the shared space 'team'".into()));
        assert_eq!(space(&format!("t/asset/{hash}")), "asset");
        assert!(label(&format!("t/asset/{hash}")).starts_with("assets"));
        assert_eq!(label("t/bogus/x/f0"), "not a key weft makes", "never filed as a shared space");
        assert_eq!(space("t/bogus/x/f0"), "bogus/x");
    }

    #[test]
    fn content_range_start_parses_or_rejects() {
        assert_eq!(parse_content_range_start("bytes 100-199/2000"), Some(100));
        assert_eq!(parse_content_range_start("bytes 0-0/1"), Some(0));
        // Wrong shape / missing pieces are rejected (resume refuses).
        assert_eq!(parse_content_range_start("100-199/2000"), None);
        assert_eq!(parse_content_range_start("bytes */2000"), None);
        assert_eq!(parse_content_range_start(""), None);
    }

    #[test]
    fn scope_key_strips_the_leading_tenant_segment() {
        // A wire key is `<tenant>/<scope>/<owner>/<id>`; the CLI shows the
        // tenant-less `<scope>/<owner>/<id>`.
        assert_eq!(scope_key("alice/run/c1/f0"), "run/c1/f0");
        assert_eq!(scope_key("alice/project/p1/f0"), "project/p1/f0");
        // No slash: nothing to strip, returned as-is.
        assert_eq!(scope_key("bare"), "bare");
        // Only the FIRST segment is stripped (split_once), not every one.
        assert_eq!(scope_key("t/a/b"), "a/b");
    }
}
