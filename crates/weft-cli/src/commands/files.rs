//! `weft files ...`: browse and manage the tenant's stored files.
//!
//! Download is the brokered-handshake-then-direct path: the dispatcher
//! authenticates + returns a short-lived PRESIGNED bucket URL; the bytes then
//! stream STRAIGHT from the storage bucket (never through the dispatcher).

use std::collections::BTreeMap;

use anyhow::Context;

use super::Ctx;

fn project_query(ctx: &Ctx) -> String {
    // Best-effort project context (tenant resolution server-side);
    // `weft files` outside a project dir still works on the caller's
    // tenant.
    match ctx.project() {
        Ok(p) => format!("?project={}", p.id()),
        Err(_) => String::new(),
    }
}

/// The SCOPE portion of a wire key: the bucket is shared and keys
/// are tenant-anchored (`<tenant>/<scope>/<owner>/<id>`), but the user
/// thinks + types in the scope portion (`<scope>/<owner>/<id>`). Strip
/// the leading tenant segment for display, matching, and prefix filters.
/// The dispatcher re-adds the caller's tenant on the way back in, so the
/// user never sees or types it.
fn scope_key(wire_key: &str) -> &str {
    wire_key.split_once('/').map(|(_tenant, rest)| rest).unwrap_or(wire_key)
}

fn fmt_size(bytes: u64) -> String {
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

/// `weft files ls [PREFIX]`: list, organized by scope (project
/// spaces / shared spaces / past-execution survivors + scratch).
pub async fn ls(ctx: Ctx, prefix: Option<String>) -> anyhow::Result<()> {
    let client = ctx.client();
    let resp = client
        .get_json(&format!("/storage/files{}", project_query(&ctx)))
        .await?;
    let files: Vec<weft_core::storage::StoredFileMeta> =
        serde_json::from_value(resp.get("files").cloned().unwrap_or_default())
            .context("parse files listing")?;
    let files: Vec<_> = files
        .into_iter()
        .filter(|f| prefix.as_deref().map(|p| scope_key(&f.key).starts_with(p)).unwrap_or(true))
        .collect();
    if ctx.json() {
        println!("{}", serde_json::to_string(&files)?);
        return Ok(());
    }
    if files.is_empty() {
        println!("(no stored files)");
        return Ok(());
    }
    // Group by the SCOPE space `<scope>/<owner>/` (tenant stripped).
    let mut groups: BTreeMap<String, Vec<&weft_core::storage::StoredFileMeta>> = BTreeMap::new();
    for f in &files {
        let scope = scope_key(&f.key);
        let space = scope
            .rsplit_once('/')
            .map(|(s, _)| s.to_string())
            .unwrap_or_else(|| scope.to_string());
        groups.entry(space).or_default().push(f);
    }
    for (space, entries) in groups {
        let label = if space.starts_with("exec/") {
            format!("{space}/  (execution scratch / survivors)")
        } else if space.starts_with("project/") {
            format!("{space}/  (project files)")
        } else {
            format!("{space}/  (shared space)")
        };
        println!("{label}");
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
    let client = ctx.client();
    let resp = client
        .get_json(&format!("/storage/files{}", project_query(&ctx)))
        .await?;
    let files: Vec<weft_core::storage::StoredFileMeta> =
        serde_json::from_value(resp.get("files").cloned().unwrap_or_default())?;
    // The user types the scope key; match it against each wire key's
    // scope portion (tenant stripped).
    let Some(meta) = files.into_iter().find(|f| scope_key(&f.key) == key) else {
        anyhow::bail!("no stored file with key '{key}' (see `weft files ls`)");
    };
    println!("{}", serde_json::to_string_pretty(&meta)?);
    Ok(())
}

/// `weft files download <KEY> [-o OUT]`: handshake with the
/// dispatcher for a presigned URL, then stream the bytes DIRECTLY from the
/// storage bucket.
/// Max number of CONSECUTIVE zero-progress resumes before giving up
/// (a resume that advances the byte count resets the counter). A
/// download of any size and any duration survives blips: each resume
/// mints a fresh, short-lived pass and continues from the current
/// offset, so only a transfer that is genuinely stuck (the same byte
/// offset failing over and over) is abandoned.
const DOWNLOAD_RESUME_ATTEMPTS: u32 = 20;

/// The download handshake result: the presigned bucket URL + the file's name +
/// total size. A presigned S3 GET carries no `x-weft-meta`, so the name (default
/// output path) and size (completeness check) come from the handshake, not the
/// byte stream's headers.
struct DownloadHandshake {
    url: String,
    filename: String,
    size_bytes: u64,
}

/// Ask the dispatcher for a fresh download pass (the brokered handshake): the
/// dispatcher authenticates, then returns a short-lived presigned bucket URL for
/// the single file plus its name + size. The bytes stream DIRECTLY from the
/// storage bucket at that URL; the dispatcher is not in the byte path.
async fn mint_download_url(
    client: &crate::client::DispatcherClient,
    key: &str,
    project: &Option<String>,
) -> anyhow::Result<DownloadHandshake> {
    let resp = client
        .post_json(
            "/storage/files/download",
            &serde_json::json!({ "key": key, "project": project }),
        )
        .await?;
    let url = resp
        .get("url")
        .and_then(|v| v.as_str())
        .map(String::from)
        .context("dispatcher returned no download url")?;
    let filename = resp.get("filename").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    let size_bytes = resp.get("sizeBytes").and_then(|v| v.as_u64()).context(
        "download handshake returned no sizeBytes; cannot verify completeness",
    )?;
    Ok(DownloadHandshake { url, filename, size_bytes })
}

pub async fn download(ctx: Ctx, key: String, output: Option<String>) -> anyhow::Result<()> {
    let client = ctx.client();
    let project = ctx.project().ok().map(|p| p.id().to_string());
    let http = reqwest::Client::new();

    // First handshake: returns the presigned URL + the file's name (default
    // output path) and total size (the completeness signal). The presigned
    // bucket GET carries no per-file metadata header, so the size comes from the
    // handshake, a trusted control-plane source, not the byte stream.
    let handshake = mint_download_url(&client, &key, &project).await?;
    let url = handshake.url;
    let total = handshake.size_bytes;
    let out_path = output.unwrap_or_else(|| {
        Some(handshake.filename)
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
    let result = download_to_tmp(&http, &client, &key, &project, url, total, &tmp_path).await;
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
/// total bytes written on success. All failure surfaces (network stall, bad
/// resume, local write error, truncated result) return `Err` and leave the temp
/// file for the caller to clean up.
#[allow(clippy::too_many_arguments)]
async fn download_to_tmp(
    http: &reqwest::Client,
    client: &crate::client::DispatcherClient,
    key: &str,
    project: &Option<String>,
    first_url: String,
    total: u64,
    tmp_path: &str,
) -> anyhow::Result<u64> {
    let mut url = first_url;
    // The initial full-body GET (byte 0 onward). Not a HEAD: it streams the file.
    let first = http.get(&url).send().await.with_context(|| format!("GET {url}"))?;
    if !first.status().is_success() {
        let status = first.status();
        let body = first.text().await.unwrap_or_default();
        anyhow::bail!("download failed ({status}): {body}");
    }
    let mut file = tokio::fs::File::create(tmp_path)
        .await
        .with_context(|| format!("create {tmp_path}"))?;

    // Stream the first response, then resume on any mid-stream drop:
    // re-handshake (fresh pass) and continue from the byte offset we
    // already have via a Range request. A drop after a long quiet
    // period would otherwise fail on an expired pass; the
    // re-handshake makes blips invisible.
    let mut written = 0u64;
    let mut response = first;
    // Counts CONSECUTIVE resumes that made zero progress. Any resume
    // that advances `written` resets it, so a large file over a flaky
    // link survives unboundedly many drops as long as it keeps moving;
    // only a truly stuck transfer (the same offset failing over and
    // over) hits the cap. This matches the "any size, any duration
    // survives blips" promise.
    let mut stalls = 0u32;
    loop {
        let before = written;
        let drained = drain_into(&mut response, &mut file, &mut written).await;
        let body_err = match drained {
            Ok(()) => break, // stream ended cleanly
            // A local write error is fatal: re-handshaking cannot fix a full or
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
            // A network drop is resumable: fall through to the re-handshake below.
            Err(DrainError::Body(e)) => e,
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
            return Err(body_err).with_context(|| {
                format!(
                    "download of '{key}' stalled; gave up after \
                     {DOWNLOAD_RESUME_ATTEMPTS} consecutive resumes made no progress, \
                     stuck at {} of {}",
                    fmt_size(written),
                    fmt_size(total),
                )
            });
        }
        eprintln!(
            "download interrupted at {} ({}); resuming...",
            fmt_size(written),
            body_err
        );
        url = mint_download_url(client, key, project).await?.url;
        let resumed = http
            .get(&url)
            .header("range", format!("bytes={written}-"))
            .send()
            .await
            .with_context(|| format!("resume GET {url}"))?;
        let status = resumed.status();
        // A resume MUST come back as 206 Partial Content starting exactly where we
        // stopped. A 200 (range ignored, full body from byte 0) would append a
        // second copy onto what we already wrote, silently corrupting the file.
        // Reject anything but a 206 at the right offset, loudly.
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
        if range_start != Some(written) {
            anyhow::bail!(
                "resume of '{key}' returned the wrong byte offset \
                 (asked for {written}, server reported {range_start:?}); \
                 refusing to corrupt the file"
            );
        }
        response = resumed;
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

/// Why `drain_into` stopped short of a clean body end. The two kinds are handled
/// very differently: a `Body` error (the network dropped mid-stream) is RESUMABLE
/// (re-handshake + Range from where we are); a `Write` error (the local disk is
/// full / read-only) is FATAL (no amount of resuming fixes it, and re-handshaking
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
    let client = ctx.client();
    let project = ctx.project().ok().map(|p| p.id().to_string());
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
            let resp = client
                .get_json(&format!("/storage/files{}", project_query(&ctx)))
                .await?;
            let files: Vec<weft_core::storage::StoredFileMeta> =
                serde_json::from_value(resp.get("files").cloned().unwrap_or_default())
                    .context("parse files listing")?;
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
        serde_json::json!({ "prefix": target, "project": project })
    } else {
        serde_json::json!({ "key": target, "project": project })
    };
    let resp = client.delete_with_body("/storage/files", &body).await?;
    let removed = resp.get("removed").and_then(|v| v.as_u64()).unwrap_or(0);
    println!("removed {removed} file(s)");
    Ok(())
}

/// `weft files usage`.
pub async fn usage(ctx: Ctx) -> anyhow::Result<()> {
    let client = ctx.client();
    let resp = client
        .get_json(&format!("/storage/usage{}", project_query(&ctx)))
        .await?;
    if ctx.json() {
        println!("{resp}");
        return Ok(());
    }
    let stored = resp.get("storedBytes").and_then(|v| v.as_u64()).unwrap_or(0);
    let count = resp.get("fileCount").and_then(|v| v.as_u64()).unwrap_or(0);
    if count == 0 {
        println!("nothing stored");
    } else {
        println!("stored: {} across {count} file(s)", fmt_size(stored));
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::{parse_content_range_start, scope_key};

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
        assert_eq!(scope_key("alice/exec/c1/f0"), "exec/c1/f0");
        assert_eq!(scope_key("alice/project/p1/f0"), "project/p1/f0");
        // No slash: nothing to strip, returned as-is.
        assert_eq!(scope_key("bare"), "bare");
        // Only the FIRST segment is stripped (split_once), not every one.
        assert_eq!(scope_key("t/a/b"), "a/b");
    }
}
