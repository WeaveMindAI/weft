//! `weft files ...` + `weft storage ...`: browse and manage the
//! tenant's stored files, and configure the backing-disk profile.
//!
//! Download is the brokered-handshake-then-direct path: the
//! dispatcher authenticates + returns the box's public URL with a
//! short-lived capability; the bytes then stream STRAIGHT from the
//! box (never through the dispatcher).

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
        .filter(|f| prefix.as_deref().map(|p| f.key.starts_with(p)).unwrap_or(true))
        .collect();
    if ctx.json() {
        println!("{}", serde_json::to_string(&files)?);
        return Ok(());
    }
    if files.is_empty() {
        println!("(no stored files)");
        return Ok(());
    }
    // Group by `<scope>/<owner>/`.
    let mut groups: BTreeMap<String, Vec<&weft_core::storage::StoredFileMeta>> = BTreeMap::new();
    for f in &files {
        let space = f
            .key
            .rsplit_once('/')
            .map(|(s, _)| s.to_string())
            .unwrap_or_else(|| f.key.clone());
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
            let expiry = match f.expires_at_unix {
                Some(t) => format!(" expires={t}"),
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
    let Some(meta) = files.into_iter().find(|f| f.key == key) else {
        anyhow::bail!("no stored file with key '{key}' (see `weft files ls`)");
    };
    println!("{}", serde_json::to_string_pretty(&meta)?);
    Ok(())
}

/// `weft files download <KEY> [-o OUT]`: handshake with the
/// dispatcher, then stream the bytes DIRECTLY from the box.
/// Max number of CONSECUTIVE zero-progress resumes before giving up
/// (a resume that advances the byte count resets the counter). A
/// download of any size and any duration survives blips: each resume
/// mints a fresh, short-lived pass and continues from the current
/// offset, so only a transfer that is genuinely stuck (the same byte
/// offset failing over and over) is abandoned.
const DOWNLOAD_RESUME_ATTEMPTS: u32 = 20;

/// Ask the dispatcher for a fresh download pass (the brokered
/// handshake): the dispatcher authenticates, then returns the box's
/// public URL carrying a short-lived single-file capability. The
/// bytes stream DIRECTLY from the box at that URL; the dispatcher is
/// not in the byte path.
async fn mint_download_url(
    client: &crate::client::DispatcherClient,
    key: &str,
    project: &Option<String>,
) -> anyhow::Result<String> {
    let resp = client
        .post_json(
            "/storage/files/download",
            &serde_json::json!({ "key": key, "project": project }),
        )
        .await?;
    resp.get("url")
        .and_then(|v| v.as_str())
        .map(String::from)
        .context("dispatcher returned no download url")
}

pub async fn download(ctx: Ctx, key: String, output: Option<String>) -> anyhow::Result<()> {
    let client = ctx.client();
    let project = ctx.project().ok().map(|p| p.id().to_string());
    let http = reqwest::Client::new();

    // First handshake: also tells us the filename (for the default
    // output path) and the total size (so we know when we are done).
    let mut url = mint_download_url(&client, &key, &project).await?;
    let head = http.get(&url).send().await.with_context(|| format!("GET {url}"))?;
    if !head.status().is_success() {
        let status = head.status();
        let body = head.text().await.unwrap_or_default();
        anyhow::bail!("download failed ({status}): {body}");
    }
    // `x-weft-meta` carries the file's total size, which is the ONLY
    // signal that the download is complete. The box sets it on every
    // response, so a missing/unparseable header is a hard error: never
    // downgrade to "no verification" (that would let a truncated body
    // report success). Fail loud instead.
    let meta: weft_core::storage::StoredFileMeta = head
        .headers()
        .get("x-weft-meta")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| serde_json::from_str(s).ok())
        .context(
            "download response missing a valid x-weft-meta header; cannot verify completeness",
        )?;
    let total = meta.size_bytes;
    let out_path = output.unwrap_or_else(|| {
        Some(meta.filename.clone())
            .filter(|f| !f.is_empty())
            .unwrap_or_else(|| key.rsplit('/').next().unwrap_or("download.bin").to_string())
    });
    let mut file = tokio::fs::File::create(&out_path)
        .await
        .with_context(|| format!("create {out_path}"))?;

    // Stream the first response, then resume on any mid-stream drop:
    // re-handshake (fresh pass) and continue from the byte offset we
    // already have via a Range request. A drop after a long quiet
    // period would otherwise fail on an expired pass; the
    // re-handshake makes blips invisible.
    let mut written = 0u64;
    let mut response = head;
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
        match drained {
            Ok(()) => break, // stream ended cleanly
            Err(e) => {
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
                    return Err(e).with_context(|| {
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
                    e
                );
                url = mint_download_url(&client, &key, &project).await?;
                let resumed = http
                    .get(&url)
                    .header("range", format!("bytes={written}-"))
                    .send()
                    .await
                    .with_context(|| format!("resume GET {url}"))?;
                let status = resumed.status();
                // A resume MUST come back as 206 Partial Content
                // starting exactly where we stopped. A 200 (range
                // ignored, full body from byte 0) would append a second
                // copy onto what we already wrote, silently corrupting
                // the file. Reject anything but a 206 at the right
                // offset, loudly.
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
        }
    }

    tokio::io::AsyncWriteExt::flush(&mut file).await?;
    if written != total {
        anyhow::bail!(
            "download of '{key}' ended at {} but the file is {}; the result is truncated",
            fmt_size(written),
            fmt_size(total),
        );
    }
    println!("downloaded {} ({}) -> {out_path}", key, fmt_size(written));
    Ok(())
}

/// Pipe a response body into `file`, advancing `written`. Returns the
/// stream error on a mid-transfer drop (the caller resumes), or `Ok`
/// when the body ends.
async fn drain_into(
    response: &mut reqwest::Response,
    file: &mut tokio::fs::File,
    written: &mut u64,
) -> anyhow::Result<()> {
    while let Some(chunk) = response.chunk().await.context("download stream")? {
        *written += chunk.len() as u64;
        tokio::io::AsyncWriteExt::write_all(file, &chunk).await?;
    }
    Ok(())
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
            let victims: Vec<_> = files.iter().filter(|f| f.key.starts_with(&target)).collect();
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
    if !resp.get("provisioned").and_then(|v| v.as_bool()).unwrap_or(false) {
        println!("no storage box provisioned (nothing stored)");
        return Ok(());
    }
    let stored = resp.get("storedBytes").and_then(|v| v.as_u64()).unwrap_or(0);
    let count = resp.get("fileCount").and_then(|v| v.as_u64()).unwrap_or(0);
    println!("stored: {} across {count} file(s)", fmt_size(stored));
    if let Some(disks) = resp.get("disks").and_then(|v| v.as_array()) {
        for d in disks {
            let name = d.get("name").and_then(|v| v.as_str()).unwrap_or("?");
            let free = d.get("freeBytes").and_then(|v| v.as_u64()).unwrap_or(0);
            let total = d.get("totalBytes").and_then(|v| v.as_u64()).unwrap_or(0);
            let draining = d.get("draining").and_then(|v| v.as_bool()).unwrap_or(false);
            let tag = if draining { "  (draining)" } else { "" };
            println!("  {name}: {} free of {}{tag}", fmt_size(free), fmt_size(total));
        }
    }
    Ok(())
}

/// `weft storage config [--class C] [--disk-gib N]`: view or set the
/// per-tenant backing-disk profile (StorageClass + unit size).
/// Applies to disks provisioned from now on.
pub async fn config(
    ctx: Ctx,
    storage_class: Option<String>,
    disk_gib: Option<u64>,
) -> anyhow::Result<()> {
    let client = ctx.client();
    let q = project_query(&ctx);
    if storage_class.is_none() && disk_gib.is_none() {
        let resp = client.get_json(&format!("/storage/profile{q}")).await?;
        println!("{}", serde_json::to_string_pretty(&resp)?);
        return Ok(());
    }
    // Partial update: read current, overlay the provided knobs.
    let current = client.get_json(&format!("/storage/profile{q}")).await?;
    let class = match &storage_class {
        // `--class default` clears the override (cluster default).
        Some(c) if c == "default" => None,
        Some(c) => Some(c.clone()),
        None => current
            .get("storage_class")
            .and_then(|v| v.as_str())
            .map(String::from),
    };
    let unit = disk_gib
        .map(|g| (g as i64) << 30)
        .unwrap_or_else(|| current.get("disk_unit_bytes").and_then(|v| v.as_i64()).unwrap_or(10 << 30));
    let resp = client
        .put_json(
            &format!("/storage/profile{q}"),
            &serde_json::json!({ "storage_class": class, "disk_unit_bytes": unit }),
        )
        .await?;
    println!("profile updated: {}", serde_json::to_string(&resp)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_content_range_start;

    #[test]
    fn content_range_start_parses_or_rejects() {
        assert_eq!(parse_content_range_start("bytes 100-199/2000"), Some(100));
        assert_eq!(parse_content_range_start("bytes 0-0/1"), Some(0));
        // Wrong shape / missing pieces are rejected (resume refuses).
        assert_eq!(parse_content_range_start("100-199/2000"), None);
        assert_eq!(parse_content_range_start("bytes */2000"), None);
        assert_eq!(parse_content_range_start(""), None);
    }
}
